// Package executor drives the durable CatSync run lifecycle: it validates
// the configuration, locks the destination, discovers the source, collects
// the remote manifest in one batched SSH call, plans disjoint backend sets,
// persists the run before any transfer, executes the native parallel-rsync
// small-file stage, mscp, and rsync stages sequentially (so no destination
// path ever has concurrent writers), verifies, and only then marks tasks
// completed.
//
// Resume loads a persisted run, recovers interrupted state, reacquires the
// destination lock, revalidates source metadata, and continues pending and
// failed tasks with the run's recorded immutable configuration: small-file
// batches are rebuilt deterministically from pending tasks (legacy
// fpsync-run fields are ignored and retired on completion), mscp resumes
// from its retained checkpoint with the recorded working directory, and
// rsync resumes through its partial directory.
//
// State transitions are persisted atomically before and after every
// meaningful operation: a task is marked running before its subprocess
// launches, a subprocess failure marks it failed (never completed),
// cancellation returns running tasks to pending with an interruption note,
// and mscp checkpoints are removed only after the verification gate passes.
package executor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/frankruan/catsync/internal/backend"
	"github.com/frankruan/catsync/internal/discovery"
	"github.com/frankruan/catsync/internal/planner"
	"github.com/frankruan/catsync/internal/process"
	"github.com/frankruan/catsync/internal/state"
	"github.com/frankruan/catsync/internal/target"
)

// Tuning constants of the MVP orchestration.
const (
	// DefaultFilesPerPartition is the maximum number of files in one
	// native small-file batch.
	DefaultFilesPerPartition = 2000

	// MaxSSHConnections bounds the estimated global SSH connection count,
	// computed as Jobs × Streams (each transfer job may hold Streams
	// connections, and auxiliary phases use at most one). Configurations
	// exceeding the cap are rejected so a typo cannot open hundreds of
	// connections to a host.
	MaxSSHConnections = 64

	// mscpInflight is the fixed -a value (in-flight SFTP requests per
	// connection), mscp's own default.
	mscpInflight = 32
	// mscpMinChunk is the fixed -s value (minimum chunk size), mscp's own
	// default of 16 MiB.
	mscpMinChunk = 16 << 20
)

// Sentinel errors; test them with errors.Is.
var (
	// ErrInvalidConfig rejects an inconsistent executor configuration;
	// the CLI maps it to the usage exit status.
	ErrInvalidConfig = errors.New("executor: invalid configuration")
	// ErrVerifyNotImplemented rejects the sha256 verification mode: this
	// MVP implements none and size only and refuses to pretend otherwise.
	ErrVerifyNotImplemented = errors.New("executor: verify mode sha256 is not implemented in this MVP (use none or size)")
	// ErrNotResumable reports a run with no work left to resume.
	ErrNotResumable = errors.New("executor: run is not resumable")
)

// Programs names the external executables. An empty field uses the default
// program name resolved through PATH.
type Programs struct {
	SSH   string
	Rsync string
	MSCP  string
}

func (p Programs) ssh() string { return orDefault(p.SSH, "ssh") }

// sshProgram returns the ssh override exactly as configured: empty means
// the transport and probe default to "ssh".
func (p Programs) sshProgram() string { return p.SSH }

func (p Programs) rsync() string { return orDefault(p.Rsync, "rsync") }
func (p Programs) mscp() string  { return orDefault(p.MSCP, "mscp") }

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

// Config describes one executor invocation. Sync uses every field; Resume
// uses only StateDir, Programs, Env, Stdout, and Stderr — the run's
// recorded configuration is authoritative for everything else.
type Config struct {
	// SourceDir is the absolute local source directory.
	SourceDir string
	// Dest is the validated remote destination.
	Dest target.Destination
	// Backend is the requested backend selection (auto or concrete).
	Backend state.Backend
	// DryRun plans and persists the run without transferring data.
	DryRun bool
	// IPv6 restricts backend tools and auxiliary SSH calls to IPv6.
	IPv6 bool
	// Verify is the completion verification mode: none or size.
	Verify string
	// Jobs caps concurrent transfer processes.
	Jobs int
	// Streams caps mscp SSH connections per transfer process.
	Streams int
	// LargeThreshold is the size in bytes at which a file counts as large.
	LargeThreshold uint64
	// StateDir holds the durable run state.
	StateDir string
	// Programs overrides the external executables (tests, diagnostics).
	Programs Programs
	// Env lists environment additions for every child process (see
	// process.Spec.Env); nil inherits the environment unchanged.
	Env []string
	// Stdout and Stderr receive progress and diagnostics; nil discards.
	Stdout io.Writer
	Stderr io.Writer
}

// validateFull checks a Sync configuration.
func (c *Config) validateFull() error {
	if !c.Backend.Valid() {
		return fmt.Errorf("%w: backend %q", ErrInvalidConfig, c.Backend)
	}
	switch c.Verify {
	case state.VerifyNone, state.VerifySize:
	case state.VerifySHA256:
		return ErrVerifyNotImplemented
	default:
		return fmt.Errorf("%w: verify mode %q", ErrInvalidConfig, c.Verify)
	}
	if c.Jobs < 1 {
		return fmt.Errorf("%w: jobs must be at least 1, got %d", ErrInvalidConfig, c.Jobs)
	}
	if c.Streams < 1 {
		return fmt.Errorf("%w: streams must be at least 1, got %d", ErrInvalidConfig, c.Streams)
	}
	if c.Jobs*c.Streams > MaxSSHConnections {
		return fmt.Errorf("%w: jobs × streams = %d exceeds the SSH connection budget %d", ErrInvalidConfig, c.Jobs*c.Streams, MaxSSHConnections)
	}
	if strings.TrimSpace(c.StateDir) == "" {
		return fmt.Errorf("%w: state directory is empty", ErrInvalidConfig)
	}
	if c.SourceDir == "" || !filepath.IsAbs(c.SourceDir) {
		return fmt.Errorf("%w: source directory %q is not absolute", ErrInvalidConfig, c.SourceDir)
	}
	if err := backend.ValidateDestination(c.Dest); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	// The ssh override reaches rsync's -e transport, which rsync splits on
	// whitespace; refuse overrides it cannot represent instead of silently
	// running a different command.
	if err := (backend.SSH{Program: c.Programs.SSH}).ValidateTransport(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	for _, p := range []struct{ name, value string }{
		{"CATSYNC_SSH", c.Programs.SSH},
		{"CATSYNC_RSYNC", c.Programs.Rsync},
		{"CATSYNC_MSCP", c.Programs.MSCP},
	} {
		if strings.ContainsRune(p.value, 0) {
			return fmt.Errorf("%w: %s contains a NUL byte", ErrInvalidConfig, p.name)
		}
	}
	return nil
}

// validateResume checks the fields Resume consumes.
func (c *Config) validateResume() error {
	if strings.TrimSpace(c.StateDir) == "" {
		return fmt.Errorf("%w: state directory is empty", ErrInvalidConfig)
	}
	return nil
}

// Executor carries one run's shared machinery.
type Executor struct {
	cfg    Config
	progs  Programs
	store  *state.Store
	stdout io.Writer
	stderr io.Writer

	// run is the active run document. runMu serializes task transitions
	// and saves so concurrent mscp workers never corrupt the document.
	run   *state.Run
	runMu sync.Mutex

	// saveFn persists the run; it defaults to the store and is replaced in
	// tests to inject persistence failures. A failed save is always
	// propagated to the caller and never silently swallowed when it could
	// affect correctness or resumability.
	saveFn func(*state.Run) error
}

func newExecutor(cfg Config) (*Executor, error) {
	store, err := state.NewStore(cfg.StateDir)
	if err != nil {
		return nil, err
	}
	e := &Executor{
		cfg:    cfg,
		progs:  cfg.Programs,
		store:  store,
		stdout: cfg.Stdout,
		stderr: cfg.Stderr,
	}
	e.saveFn = store.Save
	if e.stdout == nil {
		e.stdout = io.Discard
	}
	if e.stderr == nil {
		e.stderr = io.Discard
	}
	return e, nil
}

func (e *Executor) logf(format string, args ...any) {
	fmt.Fprintf(e.stdout, format+"\n", args...)
}

func (e *Executor) warnf(format string, args ...any) {
	fmt.Fprintf(e.stderr, "catsync: "+format+"\n", args...)
}

// detectTools probes the configured programs for availability.
func (e *Executor) detectTools() planner.Tools {
	found := func(name string) bool {
		_, err := exec.LookPath(name)
		return err == nil
	}
	return planner.Tools{
		Rsync: found(e.progs.rsync()),
		MSCP:  found(e.progs.mscp()),
	}
}

// requireSSH fails when the ssh program is missing: every remote operation
// (manifest, mkdir, and the rsync transport) depends on it.
func (e *Executor) requireSSH() error {
	if _, err := exec.LookPath(e.progs.ssh()); err != nil {
		return fmt.Errorf("executor: ssh is required but %q was not found in PATH", e.progs.ssh())
	}
	return nil
}

// lockDestination acquires the destination-scoped lock for dest.
func (e *Executor) lockDestination(dest state.Destination) (*state.Lock, error) {
	lock, err := e.store.LockDestination(dest)
	if err != nil {
		return nil, err
	}
	return lock, nil
}

// saveRun persists the run under the run mutex. Callers must treat an
// error as fatal for the current operation: a transfer whose intent or
// outcome could not be recorded must not start or be reported as finished.
func (e *Executor) saveRun() error {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	if err := e.saveFn(e.run); err != nil {
		return fmt.Errorf("executor: save run %s: %w", e.run.ID, err)
	}
	return nil
}

// collectManifest runs one batched SSH manifest collection. The boolean
// reports whether the destination state is authoritative: true with a
// populated or legitimately empty map (the destination directory exists or
// was proven absent), false on any collection failure.
func (e *Executor) collectManifest(ctx context.Context, dest target.Destination, ipv6 bool) (map[string]discovery.RemoteEntry, bool, error) {
	argv, err := discovery.ManifestCommandProg(e.progs.ssh(), dest, ipv6)
	if err != nil {
		return nil, false, err
	}
	var stdout, stderr strings.Builder
	err = process.Run(ctx, process.Spec{
		Name:   argv[0],
		Args:   argv[1:],
		Env:    e.cfg.Env,
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if err != nil {
		var exitErr *process.ExitError
		if errors.As(err, &exitErr) && exitErr.Code == discovery.ManifestExitNoSuchDir {
			// The destination directory does not exist: every source
			// file is authoritatively new.
			return map[string]discovery.RemoteEntry{}, true, nil
		}
		detail := strings.TrimSpace(stderr.String())
		if detail != "" {
			return nil, false, fmt.Errorf("executor: remote manifest collection failed: %w (%s)", err, firstLine(detail))
		}
		return nil, false, fmt.Errorf("executor: remote manifest collection failed: %w", err)
	}
	m, err := discovery.CollectManifest(strings.NewReader(stdout.String()))
	if err != nil {
		return nil, false, fmt.Errorf("executor: remote manifest collection failed: %w", err)
	}
	return m, true, nil
}

// mkdirRemoteParents creates the given absolute remote directories with one
// batched ssh mkdir -p call.
func (e *Executor) mkdirRemoteParents(ctx context.Context, dest target.Destination, ipv6 bool, dirs []string) error {
	if len(dirs) == 0 {
		return nil
	}
	argv, err := discovery.MkdirCommandProg(e.progs.ssh(), dest, ipv6, dirs)
	if err != nil {
		return err
	}
	var stderr strings.Builder
	if err := process.Run(ctx, process.Spec{
		Name:   argv[0],
		Args:   argv[1:],
		Env:    e.cfg.Env,
		Stderr: &stderr,
	}); err != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail != "" {
			return fmt.Errorf("executor: create remote directories: %w (%s)", err, firstLine(detail))
		}
		return fmt.Errorf("executor: create remote directories: %w", err)
	}
	return nil
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	return s
}

// PlanOutcome is the result of a plan-only invocation.
type PlanOutcome struct {
	// Result is the planner assignment.
	Result *planner.Result
	// ManifestKnown reports whether the destination metadata is
	// authoritative; false means the conservative all-rsync fallback was
	// planned.
	ManifestKnown bool
	// ManifestErr explains why the destination metadata is unknown.
	ManifestErr error
	// Tools reports detected tool availability.
	Tools planner.Tools
	// LogicalBytes sums the sizes of all pending decisions: the data
	// volume a real sync would read from the source. It is not a network
	// traffic estimate.
	LogicalBytes int64
}

// Plan discovers the source, collects the remote manifest (falling back to
// unknown destination state), and computes the disjoint backend assignment
// without persisting or transferring anything.
func Plan(ctx context.Context, cfg Config) (*PlanOutcome, error) {
	if err := cfg.validateFull(); err != nil {
		return nil, err
	}
	e, err := newExecutor(cfg)
	if err != nil {
		return nil, err
	}
	return e.planPhase(ctx)
}

// planPhase performs discovery, manifest collection, and planning.
func (e *Executor) planPhase(ctx context.Context) (*PlanOutcome, error) {
	if err := e.requireSSH(); err != nil {
		return nil, err
	}
	tools := e.detectTools()
	var entries []discovery.Entry
	if err := discovery.Walk(e.cfg.SourceDir, func(en discovery.Entry) error {
		entries = append(entries, en)
		return nil
	}); err != nil {
		return nil, err
	}
	var dest map[string]discovery.RemoteEntry
	known := false
	m, mok, merr := e.collectManifest(ctx, e.cfg.Dest, e.cfg.IPv6)
	switch {
	case merr != nil && e.cfg.Backend == state.BackendMSCP:
		// Forced mscp must never write a file whose destination state is
		// unknown.
		return nil, fmt.Errorf("executor: backend mscp requires destination metadata: %w", merr)
	case merr != nil:
		e.warnf("remote manifest unavailable; planning conservatively (all files via rsync): %v", merr)
	default:
		dest, known = m, mok
	}
	result, err := planner.Plan(entries, dest, planner.Config{
		Backend:   e.cfg.Backend,
		Threshold: e.cfg.LargeThreshold,
		Tools:     tools,
	})
	if err != nil {
		return nil, err
	}
	out := &PlanOutcome{Result: result, ManifestKnown: known, ManifestErr: merr, Tools: tools}
	for _, d := range result.Decisions {
		if d.Status == planner.StatusPending {
			out.LogicalBytes += d.Size
		}
	}
	return out, nil
}

// buildRun creates the persisted run document from a plan.
func (e *Executor) buildRun(out *PlanOutcome) (*state.Run, error) {
	id, err := state.NewID()
	if err != nil {
		return nil, err
	}
	srcAbs, err := filepath.Abs(e.cfg.SourceDir)
	if err != nil {
		return nil, fmt.Errorf("executor: resolve source path: %w", err)
	}
	run := &state.Run{
		ID: id,
		Source: state.Source{
			Path: srcAbs,
		},
		Destination: state.Destination{
			User: e.cfg.Dest.User,
			Host: e.cfg.Dest.Host,
			Path: e.cfg.Dest.Path,
		},
		WorkDir: srcAbs,
		Config: state.Config{
			Backend:        e.cfg.Backend,
			DryRun:         e.cfg.DryRun,
			IPv6:           e.cfg.IPv6,
			Verify:         e.cfg.Verify,
			Jobs:           e.cfg.Jobs,
			Streams:        e.cfg.Streams,
			LargeThreshold: e.cfg.LargeThreshold,
		},
		Status:    state.StatusPending,
		CreatedAt: time.Now().UTC(),
	}
	for _, d := range out.Result.Decisions {
		t := state.Task{
			Path:     d.Path,
			Size:     d.Size,
			ModTime:  d.ModTime,
			DestSize: -1,
			Backend:  d.Backend,
		}
		switch d.Status {
		case planner.StatusPending:
			t.Status = state.StatusPending
		case planner.StatusSkipped:
			t.Status = state.StatusSkipped
		default: // unsupported
			// Keep unsupported distinct from skipped: the entry needs
			// transfer but the MVP cannot do it.
			t.Status = state.StatusUnsupported
			t.Error = d.Reason
		}
		run.Tasks = append(run.Tasks, t)
	}
	return run, nil
}

// Sync plans and executes a fresh run.
func Sync(ctx context.Context, cfg Config) (*state.Run, error) {
	return syncWith(ctx, cfg, nil)
}

// syncWith implements Sync and lets tests prepare the executor (for example
// to inject persistence failures) before any state is written.
func syncWith(ctx context.Context, cfg Config, prepare func(*Executor)) (*state.Run, error) {
	if err := cfg.validateFull(); err != nil {
		return nil, err
	}
	e, err := newExecutor(cfg)
	if err != nil {
		return nil, err
	}
	if prepare != nil {
		prepare(e)
	}
	lock, err := e.lockDestination(state.Destination{User: cfg.Dest.User, Host: cfg.Dest.Host, Path: cfg.Dest.Path})
	if err != nil {
		return nil, err
	}
	defer func() { _ = lock.Unlock() }()

	out, err := e.planPhase(ctx)
	if err != nil {
		return nil, err
	}
	run, err := e.buildRun(out)
	if err != nil {
		return nil, err
	}
	e.run = run
	run.Status = state.StatusRunning
	if err := e.saveRun(); err != nil {
		return run, err
	}
	e.logf("run %s: planned %d task(s)", run.ID, len(run.Tasks))
	if cfg.DryRun {
		now := time.Now().UTC()
		run.Status = state.StatusCompleted
		run.FinishedAt = &now
		if err := e.saveRun(); err != nil {
			return run, err
		}
		e.logf("run %s: dry run complete; no data was transferred", run.ID)
		return run, nil
	}
	err = e.execute(ctx)
	return run, err
}

// Resume continues a persisted run (the given ID, or the latest resumable
// run when id is empty) with the run's recorded immutable configuration.
func Resume(ctx context.Context, cfg Config, id string) (*state.Run, error) {
	return resumeWith(ctx, cfg, id, nil)
}

// resumeWith implements Resume with a test preparation hook.
func resumeWith(ctx context.Context, cfg Config, id string, prepare func(*Executor)) (*state.Run, error) {
	if err := cfg.validateResume(); err != nil {
		return nil, err
	}
	e, err := newExecutor(cfg)
	if err != nil {
		return nil, err
	}
	if prepare != nil {
		prepare(e)
	}
	var run *state.Run
	if id == "" {
		run, err = e.latestResumable()
	} else {
		run, _, err = e.store.LoadAndRecover(id)
	}
	if err != nil {
		return nil, err
	}
	if !run.Resumable() {
		return run, fmt.Errorf("%w: %s (status %s, no pending or failed tasks)", ErrNotResumable, run.ID, run.Status)
	}
	// The recorded configuration is authoritative for the transfer
	// strategy, but the current invocation's program overrides are what
	// Resume will actually execute, so they must pass the same validation
	// a fresh sync applies (notably the rsync -e transport check).
	rc := Config{
		SourceDir:      run.Source.Path,
		Dest:           target.Destination{User: run.Destination.User, Host: run.Destination.Host, Path: run.Destination.Path},
		Backend:        run.Config.Backend,
		Verify:         run.Config.Verify,
		Jobs:           run.Config.Jobs,
		Streams:        run.Config.Streams,
		LargeThreshold: run.Config.LargeThreshold,
		StateDir:       cfg.StateDir,
		Programs:       cfg.Programs,
		Env:            cfg.Env,
	}
	if err := rc.validateFull(); err != nil {
		return run, fmt.Errorf("executor: run %s configuration no longer valid: %w", run.ID, err)
	}
	if _, err := target.ValidateSource(run.Source.Path); err != nil {
		return run, fmt.Errorf("executor: run %s source: %w", run.ID, err)
	}
	e.run = run
	lock, err := e.lockDestination(run.Destination)
	if err != nil {
		return run, err
	}
	defer func() { _ = lock.Unlock() }()

	// A crash between persisting completion and checkpoint cleanup leaves a
	// completed task referencing a file. Retire those now; this cannot lose
	// progress because completed is already durable.
	if err := e.retireCompletedCheckpoints(); err != nil {
		return run, err
	}

	run.Status = state.StatusRunning
	run.Error = ""
	run.FinishedAt = nil
	if err := e.saveRun(); err != nil {
		return run, err
	}
	e.logf("run %s: resuming", run.ID)
	err = e.execute(ctx)
	return run, err
}

// latestResumable returns the most recently updated run that still has
// work, recovering interrupted state as it goes. It distinguishes an empty
// state directory (state.ErrNoRuns) from runs that merely have nothing to
// resume (ErrNotResumable).
func (e *Executor) latestResumable() (*state.Run, error) {
	runs, err := e.store.List()
	if err != nil {
		return nil, err
	}
	if len(runs) == 0 {
		return nil, state.ErrNoRuns
	}
	var best *state.Run
	for _, r := range runs {
		loaded, _, err := e.store.LoadAndRecover(r.ID)
		if err != nil {
			return nil, err
		}
		if !loaded.Resumable() {
			continue
		}
		if best == nil || loaded.UpdatedAt.After(best.UpdatedAt) ||
			(loaded.UpdatedAt.Equal(best.UpdatedAt) && loaded.ID > best.ID) {
			best = loaded
		}
	}
	if best == nil {
		return nil, fmt.Errorf("%w: no run has pending or failed tasks", ErrNotResumable)
	}
	return best, nil
}

// execute runs the transfer stages sequentially, then verification, then
// finalization. Stages are sequential so a destination path never has
// concurrent writers.
func (e *Executor) execute(ctx context.Context) error {
	var errs []string
	stages := []struct {
		name string
		fn   func(context.Context) error
	}{
		{"parallel-rsync", e.smallFileStage},
		{"mscp", e.mscpStage},
		{"rsync", e.rsyncStage},
	}
	for _, st := range stages {
		if err := st.fn(ctx); err != nil {
			if ctx.Err() != nil {
				ctxErr := fmt.Errorf("executor: %s stage: %w", st.name, ctx.Err())
				if ierr := e.interrupt(); ierr != nil {
					return fmt.Errorf("%w; %v", ctxErr, ierr)
				}
				return ctxErr
			}
			errs = append(errs, fmt.Sprintf("%s stage: %v", st.name, err))
			e.warnf("%s", errs[len(errs)-1])
		}
	}
	if err := e.verifyStage(ctx); err != nil {
		if ctx.Err() != nil {
			ctxErr := fmt.Errorf("executor: verification: %w", ctx.Err())
			if ierr := e.interrupt(); ierr != nil {
				return fmt.Errorf("%w; %v", ctxErr, ierr)
			}
			return ctxErr
		}
		errs = append(errs, fmt.Sprintf("verification: %v", err))
		e.warnf("%s", errs[len(errs)-1])
	}
	return e.finalize(errs)
}

// interrupt converts the run to its resumable interrupted form and saves.
// A save failure here is returned: an interrupted run whose recovery state
// was not recorded may look running on disk, which the next load repairs,
// but the caller must still see the persistence failure.
func (e *Executor) interrupt() error {
	e.runMu.Lock()
	changed := e.run.RecoverInterrupted()
	e.runMu.Unlock()
	if changed {
		if err := e.saveRun(); err != nil {
			return fmt.Errorf("executor: record interruption of run %s: %w", e.run.ID, err)
		}
	}
	e.warnf("interrupted; run %s is safe to resume", e.run.ID)
	return nil
}

// finalize sets the terminal run status from the accumulated stage errors
// and per-task outcomes and saves.
func (e *Executor) finalize(errs []string) error {
	e.runMu.Lock()
	failed := 0
	for _, t := range e.run.Tasks {
		if t.Status == state.StatusFailed {
			failed++
		}
	}
	e.runMu.Unlock()

	if len(errs) == 0 && failed == 0 {
		unsupported := 0
		e.runMu.Lock()
		now := time.Now().UTC()
		e.run.Status = state.StatusCompleted
		e.run.FinishedAt = &now
		e.run.Error = ""
		// Retire legacy external-fpsync artifacts in the same atomic save
		// as the completion: resume never reads them, and the fields stay
		// set on a failed or interrupted run for diagnostics.
		e.run.FPSyncRunID = ""
		e.run.FPSyncStateDir = ""
		e.run.FPSyncOutput = ""
		for _, t := range e.run.Tasks {
			if t.Status == state.StatusUnsupported {
				unsupported++
			}
		}
		e.runMu.Unlock()
		if err := e.saveRun(); err != nil {
			return err
		}
		// The completion gate passed and is persisted; only now are any
		// remaining checkpoint artifacts for this run retired.
		e.removeRunCheckpointDir()
		if unsupported > 0 {
			e.logf("run %s: completed with %d unsupported entrie(s) NOT transferred", e.run.ID, unsupported)
			e.warnf("run %s: %d source entrie(s) are unsupported (symlinks, special files, or scan errors) and were NOT transferred", e.run.ID, unsupported)
		} else {
			e.logf("run %s: completed", e.run.ID)
		}
		return nil
	}
	summary := fmt.Sprintf("%d task(s) failed", failed)
	if len(errs) > 0 {
		summary = strings.Join(errs, "; ") + "; " + summary
	}
	e.runMu.Lock()
	now := time.Now().UTC()
	e.run.Status = state.StatusFailed
	e.run.FinishedAt = &now
	e.run.Error = summary
	e.runMu.Unlock()
	if err := e.saveRun(); err != nil {
		return err
	}
	e.logf("run %s: failed: %s", e.run.ID, summary)
	return fmt.Errorf("executor: run %s: %s", e.run.ID, summary)
}

// statSource stats one task's source file with Lstat, requiring a regular
// file.
func (e *Executor) statSource(rel string) (os.FileInfo, error) {
	p := filepath.Join(e.run.Source.Path, filepath.FromSlash(rel))
	fi, err := os.Lstat(p)
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("no longer a regular file")
	}
	return fi, nil
}

// recheckBefore validates a task's source metadata against the plan
// immediately before transfer. On a mismatch the task is failed and the
// transfer must not run. A persistence error is returned so the stage stops
// instead of starting work whose intent is not on disk.
func (e *Executor) recheckBefore(t *state.Task) (bool, error) {
	fi, err := e.statSource(t.Path)
	if err != nil {
		if ferr := e.failTask(t, fmt.Sprintf("source revalidation failed: %v", err)); ferr != nil {
			return false, ferr
		}
		return false, nil
	}
	if fi.Size() != t.Size || !fi.ModTime().Equal(t.ModTime) {
		if ferr := e.failTask(t, "source changed since planning; start a new run"); ferr != nil {
			return false, ferr
		}
		return false, nil
	}
	return true, nil
}

// recheckAfter validates that the source stayed unchanged across the
// transfer. A changed source is a failed task, not a successful transfer.
func (e *Executor) recheckAfter(t *state.Task) (bool, error) {
	fi, err := e.statSource(t.Path)
	if err != nil {
		if ferr := e.failTask(t, fmt.Sprintf("source post-check failed: %v", err)); ferr != nil {
			return false, ferr
		}
		return false, nil
	}
	if fi.Size() != t.Size || !fi.ModTime().Equal(t.ModTime) {
		if ferr := e.failTask(t, "source changed during transfer; not marked completed"); ferr != nil {
			return false, ferr
		}
		return false, nil
	}
	return true, nil
}

// failTask marks a task failed and persists the run, returning any
// persistence error.
func (e *Executor) failTask(t *state.Task, msg string) error {
	e.runMu.Lock()
	prevStatus, prevErr := t.Status, t.Error
	t.Status = state.StatusFailed
	t.Error = msg
	e.runMu.Unlock()
	if err := e.saveRun(); err != nil {
		// Keep memory consistent with the disk document so later logic and
		// finalization do not build on an unrecorded transition.
		e.runMu.Lock()
		t.Status, t.Error = prevStatus, prevErr
		e.runMu.Unlock()
		return err
	}
	return nil
}

// pendingTasks returns the indexes of tasks that are pending or failed
// (retryable) and owned by backend b, in task order.
func (e *Executor) pendingTasks(b state.Backend) []int {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	var out []int
	for i := range e.run.Tasks {
		t := &e.run.Tasks[i]
		if t.Backend == b && (t.Status == state.StatusPending || t.Status == state.StatusFailed) {
			out = append(out, i)
		}
	}
	return out
}

// markRunning marks a task running, increments its attempt count, and
// persists the run. It MUST succeed before the subprocess launches: a
// transfer whose running state could not be recorded would be invisible and
// unrecoverable after a crash. On failure the in-memory transition is
// rolled back and the error propagated to the stage, which must not start
// the subprocess.
func (e *Executor) markRunning(t *state.Task) error {
	e.runMu.Lock()
	prevStatus, prevAttempts, prevErr := t.Status, t.Attempts, t.Error
	t.Status = state.StatusRunning
	t.Attempts++
	t.Error = ""
	e.runMu.Unlock()
	if err := e.saveRun(); err != nil {
		e.runMu.Lock()
		t.Status, t.Attempts, t.Error = prevStatus, prevAttempts, prevErr
		e.runMu.Unlock()
		return err
	}
	return nil
}

// retireCheckpoint clears a completed task's checkpoint reference and then
// removes the file, in crash-safe order:
//
//  1. The completed status was already persisted by completeTask, whose
//     save failure keeps the checkpoint intact.
//  2. The cleared reference is persisted before the file is removed. A
//     crash between the two steps leaves only a harmless orphan file; a
//     crash before the reference save leaves a completed task whose
//     checkpoint is still referenced, which resume's cleanup pass retires.
//  3. File removal is best-effort: an orphan checkpoint is harmless, while
//     a missing one for an unfinished task would force a full restart.
func (e *Executor) retireCheckpoint(t *state.Task) error {
	e.runMu.Lock()
	cp := t.Checkpoint
	if cp == "" {
		e.runMu.Unlock()
		return nil
	}
	t.Checkpoint = ""
	e.runMu.Unlock()
	if err := e.saveRun(); err != nil {
		e.runMu.Lock()
		t.Checkpoint = cp
		e.runMu.Unlock()
		return fmt.Errorf("executor: clear checkpoint reference for %q: %w", t.Path, err)
	}
	if err := os.Remove(cp); err != nil && !errors.Is(err, os.ErrNotExist) {
		e.warnf("remove checkpoint %s: %v", cp, err)
	}
	return nil
}

// retireCompletedCheckpoints is the resume-time recovery pass for completed
// tasks that still reference a checkpoint (a crash between completion and
// reference cleanup). The completed status is already durable, so clearing
// the reference and removing the file cannot lose transfer progress.
func (e *Executor) retireCompletedCheckpoints() error {
	var refs []*state.Task
	e.runMu.Lock()
	for i := range e.run.Tasks {
		t := &e.run.Tasks[i]
		if t.Status == state.StatusCompleted && t.Checkpoint != "" {
			refs = append(refs, t)
		}
	}
	e.runMu.Unlock()
	if len(refs) == 0 {
		return nil
	}
	// Persist all cleared references first, then remove the files.
	e.runMu.Lock()
	paths := make([]string, len(refs))
	for i, t := range refs {
		paths[i] = t.Checkpoint
		t.Checkpoint = ""
	}
	e.runMu.Unlock()
	if err := e.saveRun(); err != nil {
		e.runMu.Lock()
		for i, t := range refs {
			t.Checkpoint = paths[i]
		}
		e.runMu.Unlock()
		return fmt.Errorf("executor: clear completed checkpoint references: %w", err)
	}
	for _, cp := range paths {
		if err := os.Remove(cp); err != nil && !errors.Is(err, os.ErrNotExist) {
			e.warnf("remove leftover checkpoint %s: %v", cp, err)
		}
	}
	return nil
}

// removeRunCheckpointDir removes the run's own checkpoint directory after a
// fully successful run, clearing any harmless leftovers from an earlier
// crash. It acts only on CatSync's per-run state directory.
func (e *Executor) removeRunCheckpointDir() {
	abs, err := filepath.Abs(e.cfg.StateDir)
	if err != nil {
		return
	}
	dir := filepath.Join(abs, "checkpoints", e.run.ID)
	if err := os.RemoveAll(dir); err != nil {
		e.warnf("remove checkpoint directory %s: %v", dir, err)
	}
}
