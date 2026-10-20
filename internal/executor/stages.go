package executor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/frankruan/catsync/internal/backend"
	"github.com/frankruan/catsync/internal/backend/fpsync"
	"github.com/frankruan/catsync/internal/backend/mscp"
	"github.com/frankruan/catsync/internal/backend/rsync"
	"github.com/frankruan/catsync/internal/state"
	"github.com/frankruan/catsync/internal/target"
)

// destTarget converts the run's recorded destination for backend use.
func (e *Executor) destTarget() target.Destination {
	return target.Destination{
		User: e.run.Destination.User,
		Host: e.run.Destination.Host,
		Path: e.run.Destination.Path,
	}
}

// stageVerifyNone reports whether tasks complete immediately after their
// stage (verify mode none) or stay running until the batched verification
// stage (verify mode size).
func (e *Executor) stageVerifyNone() bool {
	return e.run.Config.Verify == state.VerifyNone
}

// fpsyncStage runs the parallel small-file stage. fpsync always sees the
// whole source tree; files at or above the size threshold are skipped by
// fpart's -S and reported as S records, and each skipped path must be owned
// by another backend (or need no transfer), otherwise the stage fails
// loudly instead of silently dropping data.
func (e *Executor) fpsyncStage(ctx context.Context) error {
	idxs := e.pendingTasks(state.BackendFPSync)
	if len(idxs) == 0 {
		return nil
	}
	if !e.detectTools().FPSyncCompatible() {
		for _, i := range idxs {
			if err := e.failTask(&e.run.Tasks[i], "fpsync or fpart is no longer available; start a new run to replan"); err != nil {
				return err
			}
		}
		return errors.New("fpsync or fpart unavailable")
	}
	// The fpart partition limit is shifted by one so fpart's skip-big
	// boundary is exactly "size >= large threshold". Thresholds below 2
	// cannot express that boundary in fpart; the planner never assigns
	// fpsync tasks for them, but a resumed run recorded by an older build
	// (or a corrupted plan) must fail loudly instead of letting fpsync
	// copy files that another backend owns.
	partitionLimit, err := fpsync.LimitForThreshold(e.run.Config.LargeThreshold)
	if err != nil {
		for _, i := range idxs {
			if ferr := e.failTask(&e.run.Tasks[i], fmt.Sprintf("%v; start a new run to replan", err)); ferr != nil {
				return ferr
			}
		}
		return err
	}

	// Revalidate every small file immediately before the stage.
	var active []*state.Task
	for _, i := range idxs {
		t := &e.run.Tasks[i]
		ok, rerr := e.recheckBefore(t)
		if rerr != nil {
			return rerr
		}
		if ok {
			active = append(active, t)
		}
	}
	if len(active) == 0 {
		return errors.New("all fpsync tasks failed source revalidation")
	}

	stateDir, err := e.fpsyncStateDir()
	if err != nil {
		return err
	}
	cfg := fpsync.Config{
		Program:           e.progs.fpsync(),
		SourceDir:         e.run.Source.Path,
		Dest:              e.destTarget(),
		Jobs:              e.run.Config.Jobs,
		FilesPerPartition: DefaultFilesPerPartition,
		// fpart -S skips size > limit, so limit = threshold-1 keeps
		// at-or-above-threshold files out of this stage's writes.
		Threshold: partitionLimit,
		StateDir:  stateDir,
		Env:       e.cfg.Env,
		Stderr:    e.stderr,
	}

	// Persist every task's running intent before launching. If any save
	// fails, no subprocess is started; tasks already marked running are
	// recovered as pending by interruption handling.
	for _, t := range active {
		if err := e.markRunning(t); err != nil {
			return err
		}
	}
	e.logf("stage fpsync: %d file(s) below %d bytes (fpart -s %d)", len(active), e.run.Config.LargeThreshold, partitionLimit)

	// Resume only when the recorded run metadata and its on-disk state
	// directory are both still valid; otherwise start a fresh fpsync run.
	e.runMu.Lock()
	prevOutput := e.run.FPSyncOutput
	e.runMu.Unlock()
	var res *fpsync.Result
	if e.run.FPSyncRunID != "" && dirExists(stateDir) {
		res, err = cfg.Resume(ctx, e.run.FPSyncRunID)
	} else {
		if e.run.FPSyncRunID != "" {
			e.warnf("fpsync state directory %s is gone; starting a fresh fpsync run", stateDir)
		}
		res, err = cfg.Run(ctx)
	}
	// Persist the resume artifacts before anything else, success or
	// failure: an interrupted run needs them. A failed save is reported but
	// does not hide the transfer error below.
	var artifactErr error
	if res != nil {
		e.runMu.Lock()
		if res.RunID != "" {
			e.run.FPSyncRunID = res.RunID
		}
		e.run.FPSyncStateDir = stateDir
		if res.OutputPath != "" {
			e.run.FPSyncOutput = res.OutputPath
		}
		e.runMu.Unlock()
		if serr := e.saveRun(); serr != nil {
			artifactErr = serr
			e.warnf("save fpsync run artifacts: %v", serr)
		}
	}
	if err != nil {
		if ctx.Err() == nil {
			for _, t := range active {
				if ferr := e.failTask(t, fmt.Sprintf("fpsync failed: %v", err)); ferr != nil {
					return ferr
				}
			}
		}
		if artifactErr != nil {
			return fmt.Errorf("%w (also failed to record fpsync run artifacts: %v)", err, artifactErr)
		}
		return err
	}
	if artifactErr != nil {
		return artifactErr
	}

	// On resume fpsync reports no S records again; re-verify the original
	// run's preserved output instead.
	skipped := res.Skipped
	if len(skipped) == 0 && prevOutput != "" && prevOutput != res.OutputPath {
		if data, rerr := os.ReadFile(prevOutput); rerr == nil {
			_, skipped = fpsync.ParseOutput(string(data))
		}
	}
	var orphans []string
	for _, s := range skipped {
		ot := e.taskByPath(s.Path)
		switch {
		case ot == nil:
			// A file that appeared after planning and is too large for
			// this stage: it has no owner, so it must not pass silently.
			orphans = append(orphans, s.Path)
		case ot.Status == state.StatusSkipped || ot.Status == state.StatusUnsupported:
			// Unchanged or untransferable: needs no other owner.
		case ot.Backend == state.BackendFPSync:
			if ferr := e.failTask(ot, "file grew to or past the large threshold during the run; start a new run"); ferr != nil {
				return ferr
			}
		default:
			// Owned by mscp or rsync as planned.
		}
	}

	for _, t := range active {
		e.runMu.Lock()
		still := t.Status == state.StatusRunning
		e.runMu.Unlock()
		if !still {
			continue
		}
		ok, rerr := e.recheckAfter(t)
		if rerr != nil {
			return rerr
		}
		if !ok {
			continue
		}
		if e.stageVerifyNone() {
			if cerr := e.completeTask(t); cerr != nil {
				return cerr
			}
		}
	}
	if len(orphans) > 0 {
		return fmt.Errorf("fpsync skipped %d large file(s) with no owning backend (new files appeared mid-run?): %s",
			len(orphans), strings.Join(quoteAll(orphans, 5), ", "))
	}
	return nil
}

// fpsyncStateDir returns the persistent fpsync -t directory: the recorded
// one on resume, otherwise a fresh per-state-dir location.
func (e *Executor) fpsyncStateDir() (string, error) {
	if e.run.FPSyncStateDir != "" {
		return e.run.FPSyncStateDir, nil
	}
	abs, err := filepath.Abs(e.cfg.StateDir)
	if err != nil {
		return "", fmt.Errorf("executor: resolve state directory: %w", err)
	}
	return filepath.Join(abs, "fpsync"), nil
}

func dirExists(p string) bool {
	fi, err := os.Stat(p)
	return err == nil && fi.IsDir()
}

func quoteAll(paths []string, max int) []string {
	shown := paths
	if len(shown) > max {
		shown = shown[:max]
	}
	out := make([]string, len(shown))
	for i, p := range shown {
		out[i] = fmt.Sprintf("%q", p)
	}
	if rest := len(paths) - len(shown); rest > 0 {
		out = append(out, fmt.Sprintf("and %d more", rest))
	}
	return out
}

// completeTask records a completed task after its post-checks passed, then
// retires any mscp checkpoint. Ordering is crash-safe:
//
//  1. The completed status is persisted while the checkpoint reference is
//     still present, so a crash cannot lose track of the checkpoint before
//     completion is durable. If this save fails, the in-memory transition is
//     rolled back and the error is returned; the checkpoint is untouched.
//  2. Only then is the checkpoint reference cleared (its own save) and the
//     file removed (best-effort, harmless if orphaned).
func (e *Executor) completeTask(t *state.Task) error {
	e.runMu.Lock()
	prevStatus, prevErr := t.Status, t.Error
	t.Status = state.StatusCompleted
	t.Error = ""
	e.runMu.Unlock()
	if err := e.saveRun(); err != nil {
		e.runMu.Lock()
		t.Status, t.Error = prevStatus, prevErr
		e.runMu.Unlock()
		return err
	}
	return e.retireCheckpoint(t)
}

// mscpStage runs the confirmed-new large-file tasks, one mscp process per
// file, bounded by the jobs semaphore. mscp follows scp semantics and does
// not create missing remote parent directories, so the stage first issues
// one batched remote mkdir.
func (e *Executor) mscpStage(ctx context.Context) error {
	idxs := e.pendingTasks(state.BackendMSCP)
	if len(idxs) == 0 {
		return nil
	}
	if !e.detectTools().MSCP {
		for _, i := range idxs {
			if err := e.failTask(&e.run.Tasks[i], "mscp is no longer available; start a new run to replan"); err != nil {
				return err
			}
		}
		return errors.New("mscp unavailable")
	}

	// One batched mkdir -p for every parent directory the stage needs.
	parents := map[string]struct{}{}
	for _, i := range idxs {
		rel := e.run.Tasks[i].Path
		if slash := strings.LastIndexByte(rel, '/'); slash >= 0 {
			parents[targetRemoteJoin(e.run.Destination.Path, rel[:slash])] = struct{}{}
		}
	}
	if len(parents) > 0 {
		dirs := make([]string, 0, len(parents))
		for d := range parents {
			dirs = append(dirs, d)
		}
		sort.Strings(dirs)
		if err := e.mkdirRemoteParents(ctx, e.destTarget(), e.run.Config.IPv6, dirs); err != nil {
			for _, i := range idxs {
				if ferr := e.failTask(&e.run.Tasks[i], fmt.Sprintf("create remote parent directories: %v", err)); ferr != nil {
					return ferr
				}
			}
			return err
		}
	}

	e.logf("stage mscp: %d large file(s), up to %d process(es) × %d connection(s)",
		len(idxs), e.run.Config.Jobs, e.run.Config.Streams)

	ckptDir, err := e.checkpointDir()
	if err != nil {
		return err
	}
	sem := make(chan struct{}, e.run.Config.Jobs)
	var wg sync.WaitGroup
	var errs []string
	var errsMu sync.Mutex
	for _, i := range idxs {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		}
		wg.Add(1)
		go func(t *state.Task) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := e.mscpTask(ctx, t, ckptDir); err != nil && ctx.Err() == nil {
				errsMu.Lock()
				errs = append(errs, err.Error())
				errsMu.Unlock()
			}
		}(&e.run.Tasks[i])
	}
	wg.Wait()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

// mscpTask transfers one large file, resuming from its retained checkpoint
// when one exists.
func (e *Executor) mscpTask(ctx context.Context, t *state.Task, ckptDir string) error {
	ok, err := e.recheckBefore(t)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	e.runMu.Lock()
	if t.Checkpoint == "" {
		sum := sha256.Sum256([]byte(t.Path))
		t.Checkpoint = filepath.Join(ckptDir, hex.EncodeToString(sum[:])+".mscpckpt")
	}
	checkpoint := t.Checkpoint
	e.runMu.Unlock()
	if err := os.MkdirAll(filepath.Dir(checkpoint), 0o700); err != nil {
		return e.failTask(t, fmt.Sprintf("create checkpoint directory: %v", err))
	}

	resume := fileExists(checkpoint)
	cfg := mscp.Config{
		Program:     e.progs.mscp(),
		WorkDir:     e.run.WorkDir,
		Path:        t.Path,
		Dest:        e.destTarget(),
		Connections: e.run.Config.Streams,
		Inflight:    mscpInflight,
		MinChunk:    mscpMinChunk,
		Checkpoint:  checkpoint,
		SSH:         sshOpts(e.run.Config.IPv6),
		Env:         e.cfg.Env,
		Stdout:      e.stdout,
		Stderr:      e.stderr,
	}

	// The running intent must be durable before mscp starts.
	if err := e.markRunning(t); err != nil {
		return err
	}
	if resume {
		err = cfg.Resume(ctx)
	} else {
		err = cfg.Run(ctx)
	}
	if err != nil {
		if ctx.Err() == nil {
			return e.failTask(t, fmt.Sprintf("mscp failed: %v", err))
		}
		return err
	}
	ok, rerr := e.recheckAfter(t)
	if rerr != nil {
		return rerr
	}
	if !ok {
		return fmt.Errorf("%s: source changed during transfer", t.Path)
	}
	if e.stageVerifyNone() {
		return e.completeTask(t)
	}
	return nil
}

// checkpointDir returns the per-run checkpoint directory.
func (e *Executor) checkpointDir() (string, error) {
	abs, err := filepath.Abs(e.cfg.StateDir)
	if err != nil {
		return "", fmt.Errorf("executor: resolve state directory: %w", err)
	}
	return filepath.Join(abs, "checkpoints", e.run.ID), nil
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// rsyncStage runs the conservative stage: one rsync invocation over the
// disjoint set of pending rsync-owned paths.
func (e *Executor) rsyncStage(ctx context.Context) error {
	idxs := e.pendingTasks(state.BackendRsync)
	if len(idxs) == 0 {
		return nil
	}
	if !e.detectTools().Rsync {
		for _, i := range idxs {
			if err := e.failTask(&e.run.Tasks[i], "rsync is no longer available"); err != nil {
				return err
			}
		}
		return errors.New("rsync unavailable")
	}

	var active []*state.Task
	for _, i := range idxs {
		t := &e.run.Tasks[i]
		ok, rerr := e.recheckBefore(t)
		if rerr != nil {
			return rerr
		}
		if ok {
			active = append(active, t)
		}
	}
	if len(active) == 0 {
		return errors.New("all rsync tasks failed source revalidation")
	}
	paths := make([]string, 0, len(active))
	for _, t := range active {
		paths = append(paths, t.Path)
	}
	for _, t := range active {
		if err := e.markRunning(t); err != nil {
			return err
		}
	}
	e.logf("stage rsync: %d file(s)", len(active))
	cfg := rsync.Config{
		Program:   e.progs.rsync(),
		SourceDir: e.run.Source.Path,
		Dest:      e.destTarget(),
		SSH:       sshOpts(e.run.Config.IPv6),
		Paths:     paths,
		Env:       e.cfg.Env,
		Stdout:    e.stdout,
		Stderr:    e.stderr,
	}
	if err := cfg.Run(ctx); err != nil {
		if ctx.Err() == nil {
			for _, t := range active {
				if ferr := e.failTask(t, fmt.Sprintf("rsync failed: %v", err)); ferr != nil {
					return ferr
				}
			}
		}
		return err
	}
	for _, t := range active {
		ok, rerr := e.recheckAfter(t)
		if rerr != nil {
			return rerr
		}
		if !ok {
			continue
		}
		if e.stageVerifyNone() {
			if cerr := e.completeTask(t); cerr != nil {
				return cerr
			}
		}
	}
	return nil
}

// verifyStage performs the requested completion verification for every task
// that finished its stage. Verification is batched: one remote manifest
// collection covers all tasks, never one SSH connection per file.
func (e *Executor) verifyStage(ctx context.Context) error {
	if e.run.Config.Verify == state.VerifyNone {
		return nil
	}
	e.runMu.Lock()
	var candidates []*state.Task
	for i := range e.run.Tasks {
		if e.run.Tasks[i].Status == state.StatusRunning {
			candidates = append(candidates, &e.run.Tasks[i])
		}
	}
	e.runMu.Unlock()
	if len(candidates) == 0 {
		return nil
	}
	e.logf("verify: size check of %d file(s) (size equality is not content proof)", len(candidates))
	manifest, known, err := e.collectManifest(ctx, e.destTarget(), e.run.Config.IPv6)
	if err != nil || !known {
		if err == nil {
			err = errors.New("destination state unavailable")
		}
		for _, t := range candidates {
			if ferr := e.failTask(t, fmt.Sprintf("size verification failed: %v", err)); ferr != nil {
				return ferr
			}
		}
		return err
	}
	var failures []string
	for _, t := range candidates {
		entry, ok := manifest[t.Path]
		switch {
		case !ok:
			if ferr := e.failTask(t, "size verification failed: file missing at the destination after transfer"); ferr != nil {
				return ferr
			}
			failures = append(failures, t.Path)
		case entry.Size != t.Size:
			if ferr := e.failTask(t, fmt.Sprintf("size verification failed: source %d bytes, destination %d bytes", t.Size, entry.Size)); ferr != nil {
				return ferr
			}
			failures = append(failures, t.Path)
		default:
			if cerr := e.completeTask(t); cerr != nil {
				return cerr
			}
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("size verification failed for %d file(s): %s", len(failures), strings.Join(quoteAll(failures, 5), ", "))
	}
	return nil
}

// targetRemoteJoin joins the remote destination root with a slash-separated
// relative directory.
func targetRemoteJoin(base, rel string) string {
	return strings.TrimSuffix(base, "/") + "/" + rel
}

// sshOpts renders the backend SSH options for a run.
func sshOpts(ipv6 bool) backend.SSH {
	return backend.SSH{IPv6: ipv6}
}
