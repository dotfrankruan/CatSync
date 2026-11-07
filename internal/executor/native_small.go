package executor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/dotfrankruan/CatSync/internal/backend"
	"github.com/dotfrankruan/CatSync/internal/backend/multistream"
	"github.com/dotfrankruan/CatSync/internal/state"
)

// nativeSmallStage moves the small new files the planner assigned to the
// native SFTP backend: one point-to-point transfer per file over a single
// SFTP session, with the same checkpoint, durable-completion ordering, and
// no-clobber publish as the large-file backend.
//
// Auto mode assigns this backend only when rsync is unavailable, because
// batched rsync is considerably faster for many small files; the stage exists
// so a tree of new files can be copied on a machine that has nothing but ssh.
func (e *Executor) nativeSmallStage(ctx context.Context) error {
	idxs := e.pendingTasks(state.BackendSFTP)
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
	ckptDir, err := e.checkpointDir()
	if err != nil {
		return err
	}
	upload := e.run.Config.Direction.Effective() != state.DirectionRemoteToLocal

	if upload {
		// One batched mkdir -p for every parent directory the stage needs:
		// the temp object lives next to the final path.
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
		// Publishing new files without overwriting needs the hardlink
		// extension; without it the files fall back to rsync when it is
		// available, and fail closed otherwise.
		hardlinkOK, probeErr := e.probeHardlink(ctx)
		if probeErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return e.failStageWithProbeError(idxs, probeErr)
		}
		if !hardlinkOK {
			return e.routeSmallWithoutHardlink(idxs, ckptDir)
		}
	}

	tasks := e.tasksAt(idxs)
	if !upload {
		active, rerr := e.recheckRemoteSource(ctx, tasks, "before native transfer")
		if rerr != nil {
			return rerr
		}
		if len(active) == 0 {
			return errors.New("all native transfer tasks failed source revalidation")
		}
		if err := ensureLocalParents(e.run.Destination.Path, active); err != nil {
			if ferr := e.failBatch(active, fmt.Sprintf("prepare local destination parents: %v", err)); ferr != nil {
				return ferr
			}
			return err
		}
		tasks = active
	}

	e.logf("stage native-small: %d new file(s), up to %d concurrent transfer(s), one SFTP session each",
		len(tasks), e.run.Config.Jobs)

	sem := make(chan struct{}, e.run.Config.Jobs)
	var wg sync.WaitGroup
	var errs []string
	var errsMu sync.Mutex
	for _, t := range tasks {
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
			if err := e.nativeSmallTask(ctx, t, ckptDir, upload); err != nil && ctx.Err() == nil {
				errsMu.Lock()
				errs = append(errs, err.Error())
				errsMu.Unlock()
			}
		}(t)
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

// nativeSmallTask transfers or resumes one small new file natively.
func (e *Executor) nativeSmallTask(ctx context.Context, t *state.Task, ckptDir string, upload bool) error {
	if !upload {
		// Downloads revalidated their remote source in one batch already.
	} else {
		ok, err := e.recheckBefore(t)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
	}
	if strings.HasSuffix(t.Checkpoint, ".mscpckpt") || fileExists(legacyCheckpointName(ckptDir, t.Path)) {
		return e.failTask(t, legacyMSCPMessage)
	}
	checkpoint := t.Checkpoint
	if checkpoint == "" {
		e.runMu.Lock()
		if t.Checkpoint == "" {
			t.Checkpoint = nativeCheckpointName(ckptDir, t.Path)
		}
		checkpoint = t.Checkpoint
		e.runMu.Unlock()
	}
	if err := os.MkdirAll(filepath.Dir(checkpoint), 0o700); err != nil {
		return e.failTask(t, fmt.Sprintf("create checkpoint directory: %v", err))
	}

	opts := multistream.TransferOptions{
		SFTP:           e.sftpConfig(),
		Streams:        1, // one file: extra sessions only add overhead
		Workers:        1,
		ChunkSize:      nativeChunkSize,
		CheckpointPath: checkpoint,
		OnProgress:     e.addProgress,
	}
	if !upload {
		opts.ExpectedRemote = &multistream.RemoteIdentity{Size: uint64(t.Size), ModTime: t.ModTime}
	}
	if err := e.markRunning(t); err != nil {
		return err
	}

	var result *multistream.TransferResult
	var err error
	if upload {
		sourceAbs := filepath.Join(e.run.Source.Path, filepath.FromSlash(t.Path))
		remoteFinal := backend.RemotePath(e.run.Destination.Path, t.Path)
		result, err = e.nativeUpload(ctx, sourceAbs, t.Path, remoteFinal, checkpoint, opts)
	} else {
		remoteSource := backend.RemotePath(e.run.Source.Path, t.Path)
		localFinal := filepath.Join(e.run.Destination.Path, filepath.FromSlash(t.Path))
		result, err = e.nativeDownload(ctx, remoteSource, t.Path, localFinal, checkpoint, opts)
	}
	if err != nil {
		if ctx.Err() == nil {
			return e.failTask(t, fmt.Sprintf("native transfer failed: %v", err))
		}
		return err
	}
	if e.stageVerifyNone() {
		if err := e.completeTask(t); err != nil {
			return err
		}
		e.removeNativeTemp(ctx, result.TempPath)
	}
	return nil
}

// nativeUpload runs a fresh or resumed upload, restarting when the checkpoint
// exists but its temporary object never appeared.
func (e *Executor) nativeUpload(ctx context.Context, sourceAbs, sourceRel, remoteFinal, checkpoint string, opts multistream.TransferOptions) (*multistream.TransferResult, error) {
	if !fileExists(checkpoint) {
		return multistream.Transfer(ctx, sourceAbs, sourceRel, remoteFinal, opts)
	}
	ck, err := multistream.LoadCheckpoint(checkpoint)
	if err != nil {
		return nil, fmt.Errorf("load native checkpoint: %w", err)
	}
	if ck.Source != sourceRel {
		return nil, fmt.Errorf("native checkpoint is bound to source %q, not %q; refusing to resume", ck.Source, sourceRel)
	}
	result, err := multistream.Resume(ctx, sourceAbs, sourceRel, checkpoint, remoteFinal, opts)
	if err != nil && len(ck.Completed) == 0 && isMissingTemp(err) {
		if rmErr := os.Remove(checkpoint); rmErr != nil {
			return nil, fmt.Errorf("restart after missing temp: remove checkpoint: %w", rmErr)
		}
		return multistream.Transfer(ctx, sourceAbs, sourceRel, remoteFinal, opts)
	}
	return result, err
}

// nativeDownload is the download counterpart of nativeUpload.
func (e *Executor) nativeDownload(ctx context.Context, remoteSource, sourceRel, localFinal, checkpoint string, opts multistream.TransferOptions) (*multistream.TransferResult, error) {
	if !fileExists(checkpoint) {
		return multistream.Download(ctx, remoteSource, sourceRel, localFinal, opts)
	}
	ck, err := multistream.LoadCheckpoint(checkpoint)
	if err != nil {
		return nil, fmt.Errorf("load native checkpoint: %w", err)
	}
	if ck.Source != sourceRel {
		return nil, fmt.Errorf("native checkpoint is bound to source %q, not %q; refusing to resume", ck.Source, sourceRel)
	}
	result, err := multistream.ResumeDownload(ctx, remoteSource, sourceRel, checkpoint, localFinal, opts)
	if err != nil && len(ck.Completed) == 0 && isMissingTemp(err) {
		if rmErr := os.Remove(checkpoint); rmErr != nil {
			return nil, fmt.Errorf("restart after missing temp: remove checkpoint: %w", rmErr)
		}
		return multistream.Download(ctx, remoteSource, sourceRel, localFinal, opts)
	}
	return result, err
}

// isMissingTemp reports whether a resume failed because its temporary object
// vanished, which is safe to restart only when no chunk was ever completed.
func isMissingTemp(err error) bool {
	return errors.Is(err, multistream.ErrRemoteTempMissing) ||
		errors.Is(err, multistream.ErrLocalTempMissing) ||
		errors.Is(err, multistream.ErrRemoteTempExists) ||
		errors.Is(err, multistream.ErrRemoteTempMismatch)
}

// routeSmallWithoutHardlink handles an upload server without the hardlink
// extension: the files cannot be published without clobbering, so never-started
// tasks fall back to rsync batches when rsync is available, and fail closed
// otherwise. Started tasks always fail closed and keep their state.
func (e *Executor) routeSmallWithoutHardlink(idxs []int, ckptDir string) error {
	auto := e.run.Config.Backend == state.BackendAuto
	rsyncOK := e.detectTools().Rsync
	var failures []string
	rerouted := 0
	for _, i := range idxs {
		t := &e.run.Tasks[i]
		kind, reason := e.classifyNativeStart(t, ckptDir)
		switch kind {
		case nativeStartedLegacy:
			if err := e.failTask(t, legacyMSCPMessage); err != nil {
				return err
			}
			failures = append(failures, t.Path)
			continue
		case nativeStarted, nativeStateUnknown:
			msg := fmt.Sprintf("cannot publish without the hardlink@openssh.com extension and this transfer has already started (%s); refusing to fall back", reason)
			if err := e.failTask(t, msg); err != nil {
				return err
			}
			failures = append(failures, t.Path)
			continue
		}
		if !rsyncOK {
			msg := "server lacks the hardlink@openssh.com extension required for no-clobber publish, and rsync is not installed to fall back to"
			if err := e.failTask(t, msg); err != nil {
				return err
			}
			failures = append(failures, t.Path)
			continue
		}
		if !auto {
			if err := e.failTask(t, "server lacks the hardlink@openssh.com extension required for no-clobber publish"); err != nil {
				return err
			}
			failures = append(failures, t.Path)
			continue
		}
		e.runMu.Lock()
		prev := t.Backend
		t.Backend = state.BackendFPSync
		e.runMu.Unlock()
		if err := e.saveRun(); err != nil {
			e.runMu.Lock()
			t.Backend = prev
			e.runMu.Unlock()
			return err
		}
		rerouted++
	}
	if rerouted > 0 {
		e.logf("native-small: server lacks hardlink@openssh.com; %d new file(s) handed to the native parallel-rsync stage", rerouted)
	}
	if len(failures) > 0 {
		return fmt.Errorf("native-small: %d task(s) failed: hardlink@openssh.com unsupported", len(failures))
	}
	return nil
}
