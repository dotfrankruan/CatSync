package executor

import (
	"bytes"
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
	"github.com/frankruan/catsync/internal/backend/mscp"
	"github.com/frankruan/catsync/internal/backend/rsync"
	"github.com/frankruan/catsync/internal/partition"
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

// batchOutcome is one batch's result in the native small-file stage. The
// results slice is indexed by batch, so aggregation and logging follow
// batch order, never goroutine completion order.
type batchOutcome struct {
	started   bool
	cancelled bool
	stderr    string
	err       error
}

// smallFileStage runs the native parallel-rsync small-file stage: the
// pending BackendFPSync tasks are packed into deterministic disjoint
// batches, and up to Config.Jobs rsync processes run concurrently, each
// transferring one batch via --files-from over NUL-delimited stdin. No
// external fpsync or fpart process is used; "fpsync" is kept only as the
// persisted backend name for compatibility with existing runs.
func (e *Executor) smallFileStage(ctx context.Context) error {
	idxs := e.pendingTasks(state.BackendFPSync)
	if len(idxs) == 0 {
		return nil
	}
	if err := e.requireSSH(); err != nil {
		for _, i := range idxs {
			if ferr := e.failTask(&e.run.Tasks[i], fmt.Sprintf("ssh is not available: %v", err)); ferr != nil {
				return ferr
			}
		}
		return err
	}
	if !e.detectTools().Rsync {
		for _, i := range idxs {
			if err := e.failTask(&e.run.Tasks[i], "rsync is not available"); err != nil {
				return err
			}
		}
		return errors.New("rsync unavailable")
	}

	// Pack deterministically from the recorded plan order and sizes; the
	// same inputs always produce the same batches, so a resumed run
	// rebuilds equivalent batches from its pending tasks.
	byPath := make(map[string]*state.Task, len(idxs))
	var items []partition.Item
	for _, i := range idxs {
		t := &e.run.Tasks[i]
		byPath[t.Path] = t
		items = append(items, partition.Item{Path: t.Path, Size: t.Size})
	}
	batches, err := partition.Pack(items, partition.Config{
		MaxFiles: DefaultFilesPerPartition,
		MaxBytes: e.run.Config.LargeThreshold,
	})
	if err != nil {
		for _, i := range idxs {
			if ferr := e.failTask(&e.run.Tasks[i], fmt.Sprintf("partition small files: %v", err)); ferr != nil {
				return ferr
			}
		}
		return fmt.Errorf("executor: partition small files: %w", err)
	}

	// Create every remote parent directory once, before any batch starts:
	// batches share parents, and --no-implied-dirs assumes they exist.
	parents := map[string]struct{}{}
	for _, it := range items {
		if slash := strings.LastIndexByte(it.Path, '/'); slash >= 0 {
			parents[targetRemoteJoin(e.run.Destination.Path, it.Path[:slash])] = struct{}{}
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

	jobs := e.run.Config.Jobs
	if jobs > len(batches) {
		jobs = len(batches)
	}
	e.logf("stage parallel-rsync: %d small file(s), %d batch(es), up to %d concurrent rsync process(es)",
		len(items), len(batches), jobs)

	// stageCtx lets the first unrecoverable batch failure cancel the
	// remaining in-flight batches and stop new launches.
	stageCtx, cancelStage := context.WithCancel(ctx)
	defer cancelStage()
	var cancelOnce sync.Once

	results := make([]batchOutcome, len(batches))

	sem := make(chan struct{}, jobs)
	var wg sync.WaitGroup
launch:
	for bi := range batches {
		select {
		case sem <- struct{}{}:
		case <-stageCtx.Done():
			break launch
		}
		wg.Add(1)
		go func(bi int) {
			defer wg.Done()
			defer func() { <-sem }()
			results[bi] = e.runSmallBatch(ctx, stageCtx, batches[bi], byPath)
			if results[bi].err != nil {
				// First failure wins: cancel the rest of the stage.
				cancelOnce.Do(cancelStage)
			}
		}(bi)
	}
	wg.Wait()

	// Deterministic aggregation: report batch diagnostics in batch order,
	// never in goroutine completion order.
	var errs []string
	for bi, r := range results {
		if !r.started {
			continue
		}
		if r.stderr != "" {
			e.warnf("parallel-rsync batch %d/%d stderr:\n%s", bi+1, len(results), r.stderr)
		}
		if r.err != nil {
			errs = append(errs, fmt.Sprintf("batch %d/%d: %v", bi+1, len(results), r.err))
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

// runSmallBatch executes one batch of the native small-file stage: per-task
// source revalidation, durable running intent, one rsync invocation, then
// per-task post-checks. ctx is the caller's context; stageCtx is cancelled
// when the stage is torn down early.
func (e *Executor) runSmallBatch(ctx, stageCtx context.Context, batch partition.Batch, byPath map[string]*state.Task) (out batchOutcome) {
	out.started = true

	// 1. Revalidate each source file immediately before this batch starts,
	// catching changes that happened while the batch waited for a slot.
	active := make([]*state.Task, 0, len(batch.Items))
	for _, it := range batch.Items {
		t := byPath[it.Path]
		ok, err := e.recheckBefore(t)
		if err != nil {
			out.err = err
			return out
		}
		if ok {
			active = append(active, t)
		}
	}
	if len(active) == 0 {
		// Every task in this batch failed its pre-check (already recorded
		// as failed); no subprocess runs.
		return out
	}

	// 2. Persist the running intent of every task in the batch before the
	// subprocess launches. A save failure aborts the batch.
	for _, t := range active {
		if err := e.markRunning(t); err != nil {
			out.err = err
			return out
		}
	}

	// 3. One rsync invocation with the batch's NUL-delimited file list,
	// capturing output per batch (never into a shared buffer).
	paths := make([]string, 0, len(active))
	for _, t := range active {
		paths = append(paths, t.Path)
	}
	var stdout, stderr bytes.Buffer
	cfg := rsync.Config{
		Program:       e.progs.rsync(),
		SourceDir:     e.run.Source.Path,
		Dest:          e.destTarget(),
		SSH:           e.sshOpts(e.run.Config.IPv6),
		Paths:         paths,
		NoImpliedDirs: true,
		Env:           e.cfg.Env,
		Stdout:        &stdout,
		Stderr:        &stderr,
	}
	if err := cfg.Run(stageCtx); err != nil {
		out.stderr = stderr.String()
		if stageCtx.Err() != nil {
			// Cancelled by the caller or by a sibling batch failure: the
			// tasks were not transferred, so they return to pending.
			msg := "interrupted: catsync stopped before finishing; safe to resume"
			if ctx.Err() == nil {
				msg = "cancelled: another small-file batch failed; safe to resume"
			}
			if rerr := e.revertToPending(active, msg); rerr != nil {
				out.err = rerr
				return out
			}
			out.cancelled = true
			return out
		}
		for _, t := range active {
			if ferr := e.failTask(t, fmt.Sprintf("rsync batch failed: %v", err)); ferr != nil {
				out.err = ferr
				return out
			}
		}
		out.err = fmt.Errorf("rsync batch failed: %w", err)
		return out
	}
	out.stderr = stderr.String()

	// 4. Post-transfer source revalidation per task; verification=none
	// completes the task, otherwise it stays running for the batched
	// verification stage.
	for _, t := range active {
		ok, rerr := e.recheckAfter(t)
		if rerr != nil {
			out.err = rerr
			return out
		}
		if !ok {
			continue
		}
		if e.stageVerifyNone() {
			if cerr := e.completeTask(t); cerr != nil {
				out.err = cerr
				return out
			}
		}
	}
	return out
}

// revertToPending returns running tasks to pending with a cancellation
// note, persisting the transition once for the whole batch.
func (e *Executor) revertToPending(tasks []*state.Task, msg string) error {
	e.runMu.Lock()
	for _, t := range tasks {
		if t.Status == state.StatusRunning {
			t.Status = state.StatusPending
			t.Error = msg
		}
	}
	e.runMu.Unlock()
	return e.saveRun()
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
		SSH:         e.sshOpts(e.run.Config.IPv6),
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
		SSH:       e.sshOpts(e.run.Config.IPv6),
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

// sshOpts renders the backend SSH options for a run. The configured ssh
// program override (CATSYNC_SSH) reaches rsync's -e transport as well as
// the manifest/mkdir invocations, so diagnostics and tests control the
// whole ssh path CatSync itself starts. fpsync's internal rsync/ssh and
// mscp's libssh always use their own resolution and are not affected.
func (e *Executor) sshOpts(ipv6 bool) backend.SSH {
	return backend.SSH{IPv6: ipv6, Program: e.progs.sshProgram()}
}
