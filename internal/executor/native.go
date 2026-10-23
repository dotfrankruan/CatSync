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

	"github.com/pkg/sftp"

	"github.com/dotfrankruan/CatSync/internal/backend"
	"github.com/dotfrankruan/CatSync/internal/backend/multistream"
	"github.com/dotfrankruan/CatSync/internal/state"
)

// nativeChunkSize is the fixed chunk size for native multistream transfers.
const nativeChunkSize = 16 << 20 // 16 MiB

// legacyMSCPMessage is the fail-closed error for a checkpoint produced by
// the old external-mscp backend. Its binary/relative-path format is not a
// native checkpoint; the file may have been partially transferred, so the
// destination is never touched and the user must recover with an older
// CatSync release or inspect the destination manually.
const legacyMSCPMessage = "legacy external-mscp checkpoint found; this build cannot resume it — " +
	"recover with an older catsync release or delete the checkpoint after manually verifying the destination (the final file may be partially written and is left untouched)"

// nativeCheckpointName builds the deterministic local checkpoint path for
// one relative path.
func nativeCheckpointName(ckptDir, rel string) string {
	sum := sha256.Sum256([]byte(rel))
	return filepath.Join(ckptDir, hex.EncodeToString(sum[:])+".nativeckpt")
}

// legacyCheckpointName is the name the external-mscp backend used.
func legacyCheckpointName(ckptDir, rel string) string {
	sum := sha256.Sum256([]byte(rel))
	return filepath.Join(ckptDir, hex.EncodeToString(sum[:])+".mscpckpt")
}

// sftpConfig builds the SFTP session configuration for this run.
func (e *Executor) sftpConfig() multistream.SFTPConfig {
	return multistream.SFTPConfig{
		Program: e.progs.sshProgram(),
		Dest:    e.destTarget(),
		IPv6:    e.run.Config.IPv6,
		Env:     e.cfg.Env,
		Stderr:  e.stderr,
	}
}

// nativeLargeStage runs the confirmed-new large-file tasks through the
// native system-SSH/SFTP multistream backend, one transfer per file,
// bounded by the jobs semaphore. The persisted backend value stays "mscp"
// for compatibility; no external mscp is ever executed.
func (e *Executor) nativeLargeStage(ctx context.Context) error {
	idxs := e.pendingTasks(state.BackendMSCP)
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

	// One batched mkdir -p for every parent directory the stage needs: the
	// temp object lives next to the final path.
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

	e.logf("stage native-large: %d large file(s), up to %d job(s) × %d SFTP session(s)",
		len(idxs), e.run.Config.Jobs, e.run.Config.Streams)

	ckptDir, err := e.checkpointDir()
	if err != nil {
		return err
	}

	// Probe the hardlink capability once, before any temp write.
	hardlinkOK, probeErr := e.probeHardlink(ctx)
	if probeErr != nil {
		if ctx.Err() != nil {
			// Cancelled during the probe: never rewrite task ownership
			// while the run is being torn down.
			return ctx.Err()
		}
		// A probe error is not "the extension is unsupported": we cannot
		// know, so nothing may fall back. Every task fails with the real
		// cause and keeps its checkpoint and remote objects.
		return e.failStageWithProbeError(idxs, probeErr)
	}
	if !hardlinkOK {
		return e.routeWithoutHardlink(idxs, ckptDir)
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
			if err := e.nativeLargeTask(ctx, t, ckptDir); err != nil && ctx.Err() == nil {
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

// probeHardlink checks, via one short SFTP session, whether the server
// supports the hardlink@openssh.com extension required for no-clobber
// publish.
func (e *Executor) probeHardlink(ctx context.Context) (bool, error) {
	cfg := e.sftpConfig()
	ok := false
	err := cfg.WithClient(ctx, func(c *sftp.Client) error {
		_, ok = c.HasExtension("hardlink@openssh.com")
		return nil
	})
	if err != nil {
		return false, err
	}
	return ok, nil
}

// nativeStartState classifies whether a task may still be treated as never
// started (and therefore safe to hand to the conservative rsync fallback).
type nativeStartState int

const (
	// nativeNotStarted: no checkpoint reference and neither checkpoint file
	// exists — the transfer has never begun.
	nativeNotStarted nativeStartState = iota
	// nativeStarted: this task already has native transfer state.
	nativeStarted
	// nativeStartedLegacy: a legacy external-mscp checkpoint exists.
	nativeStartedLegacy
	// nativeStateUnknown: the checkpoint state cannot be determined
	// reliably, which must be treated as started.
	nativeStateUnknown
)

// classifyNativeStart decides whether the task has already entered the
// native transfer (or cannot be proven not to have).
func (e *Executor) classifyNativeStart(t *state.Task, ckptDir string) (nativeStartState, string) {
	if t.Checkpoint != "" {
		return nativeStarted, "task already references a checkpoint"
	}
	if exists, err := fileExistsChecked(nativeCheckpointName(ckptDir, t.Path)); err != nil {
		return nativeStateUnknown, fmt.Sprintf("cannot determine native checkpoint state: %v", err)
	} else if exists {
		return nativeStarted, "a native checkpoint exists"
	}
	if exists, err := fileExistsChecked(legacyCheckpointName(ckptDir, t.Path)); err != nil {
		return nativeStateUnknown, fmt.Sprintf("cannot determine legacy checkpoint state: %v", err)
	} else if exists {
		return nativeStartedLegacy, "a legacy external-mscp checkpoint exists"
	}
	return nativeNotStarted, ""
}

// fileExistsChecked reports whether the path exists, distinguishing "does
// not exist" from an unreadable state that must not be guessed.
func fileExistsChecked(p string) (bool, error) {
	_, err := os.Stat(p)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

// failStageWithProbeError fails every task of the stage with the real probe
// cause. A failed capability probe says nothing about the server's
// extensions, so no task — fresh or resumed — may fall back to rsync.
func (e *Executor) failStageWithProbeError(idxs []int, probeErr error) error {
	for _, i := range idxs {
		t := &e.run.Tasks[i]
		if err := e.failTask(t, fmt.Sprintf("native multistream: cannot verify hardlink publish support: %v (checkpoint and remote state preserved)", probeErr)); err != nil {
			return err
		}
	}
	return fmt.Errorf("native multistream: hardlink capability probe failed: %w", probeErr)
}

// routeWithoutHardlink handles a server that cleanly reports the missing
// hardlink@openssh.com extension. Only tasks proven never started may fall
// back to the conservative rsync path in auto mode. A task that already has
// any native or legacy checkpoint — or whose state cannot be proven clean —
// fails closed: rsync would bypass the no-clobber publish and could write
// over a final created meanwhile, so the backend, checkpoint, temp, and
// final are all left untouched.
func (e *Executor) routeWithoutHardlink(idxs []int, ckptDir string) error {
	auto := e.run.Config.Backend == state.BackendAuto
	failed := 0
	for _, i := range idxs {
		t := &e.run.Tasks[i]
		stateKind, reason := e.classifyNativeStart(t, ckptDir)
		switch stateKind {
		case nativeStartedLegacy:
			// The legacy checkpoint is an actionable, distinct failure.
			if err := e.failTask(t, legacyMSCPMessage); err != nil {
				return err
			}
			failed++
			continue
		case nativeStarted, nativeStateUnknown:
			msg := fmt.Sprintf("native multistream: cannot publish safely without the hardlink@openssh.com extension and this transfer has already started (%s); refusing to fall back to rsync — resolve the server's hardlink support, or inspect the destination manually before removing the checkpoint", reason)
			if err := e.failTask(t, msg); err != nil {
				return err
			}
			failed++
			continue
		}
		if !auto {
			if err := e.failTask(t, "native multistream: server lacks the hardlink@openssh.com extension required for no-clobber publish"); err != nil {
				return err
			}
			failed++
			continue
		}
		// Proven never started: safe to hand to the conservative stage.
		e.runMu.Lock()
		prev := t.Backend
		t.Backend = state.BackendRsync
		e.runMu.Unlock()
		if err := e.saveRun(); err != nil {
			e.runMu.Lock()
			t.Backend = prev
			e.runMu.Unlock()
			return err
		}
		e.logf("native-large: %s: server lacks hardlink@openssh.com; falling back to conservative rsync (transfer never started)", t.Path)
	}
	if failed > 0 {
		return fmt.Errorf("native multistream: %d task(s) failed: hardlink@openssh.com unsupported", failed)
	}
	return nil
}

// nativeLargeTask transfers or resumes one large file natively.
func (e *Executor) nativeLargeTask(ctx context.Context, t *state.Task, ckptDir string) error {
	ok, err := e.recheckBefore(t)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	// Fail closed on a legacy external-mscp checkpoint: its format is not a
	// native checkpoint and the destination may hold a partial file.
	if strings.HasSuffix(t.Checkpoint, ".mscpckpt") || fileExists(legacyCheckpointName(ckptDir, t.Path)) {
		return e.failTask(t, legacyMSCPMessage)
	}

	e.runMu.Lock()
	if t.Checkpoint == "" {
		t.Checkpoint = nativeCheckpointName(ckptDir, t.Path)
	}
	checkpoint := t.Checkpoint
	e.runMu.Unlock()
	if err := os.MkdirAll(filepath.Dir(checkpoint), 0o700); err != nil {
		return e.failTask(t, fmt.Sprintf("create checkpoint directory: %v", err))
	}

	sourceAbs := filepath.Join(e.run.Source.Path, filepath.FromSlash(t.Path))
	remoteFinal := backend.RemotePath(e.run.Destination.Path, t.Path)
	opts := multistream.TransferOptions{
		SFTP:           e.sftpConfig(),
		Streams:        e.run.Config.Streams,
		Workers:        e.run.Config.Streams,
		ChunkSize:      nativeChunkSize,
		CheckpointPath: checkpoint,
	}

	// The running intent must be durable before any remote write.
	if err := e.markRunning(t); err != nil {
		return err
	}

	var result *multistream.TransferResult
	if fileExists(checkpoint) {
		// The checkpoint is a contract: its bound source relative path must
		// be this task's path, or a stale/foreign checkpoint is refused.
		ck, lerr := multistream.LoadCheckpoint(checkpoint)
		if lerr != nil {
			return e.failTask(t, fmt.Sprintf("load native checkpoint: %v", lerr))
		}
		if ck.Source != t.Path {
			return e.failTask(t, fmt.Sprintf("native checkpoint is bound to source %q, not %q; refusing to resume", ck.Source, t.Path))
		}
		result, err = multistream.Resume(ctx, sourceAbs, checkpoint, remoteFinal, opts)
		if errors.Is(err, multistream.ErrRemoteTempMissing) {
			// The recorded transfer never created its temp (it failed
			// before the first remote write). Nothing was transferred, so
			// a fresh start with the same checkpoint file is safe.
			if ck, lerr := multistream.LoadCheckpoint(checkpoint); lerr == nil && len(ck.Completed) == 0 {
				if rmErr := os.Remove(checkpoint); rmErr != nil {
					return e.failTask(t, fmt.Sprintf("restart after missing temp: remove checkpoint: %v", rmErr))
				}
				result, err = multistream.Transfer(ctx, sourceAbs, t.Path, remoteFinal, opts)
			}
		}
	} else {
		result, err = multistream.Transfer(ctx, sourceAbs, t.Path, remoteFinal, opts)
	}
	if err != nil {
		if ctx.Err() == nil {
			return e.failTask(t, fmt.Sprintf("native large transfer failed: %v", err))
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
		// Ordering: publish+verify (done inside Transfer/Resume) → persist
		// completed → retire the local checkpoint → remove the remote temp.
		if err := e.completeTask(t); err != nil {
			return err
		}
		e.removeRemoteTemp(ctx, result.TempPath)
		return nil
	}
	// verify-size mode: the task stays running until the verification stage
	// completes it; the checkpoint is retired and the temp removed there.
	return nil
}

// removeRemoteTemp deletes the published transfer's temporary object. It is
// best-effort cleanup: a failure never rolls back a published final and is
// logged for a later cleanup pass.
func (e *Executor) removeRemoteTemp(ctx context.Context, tempPath string) {
	if tempPath == "" {
		return
	}
	cfg := e.sftpConfig()
	err := cfg.WithClient(ctx, func(c *sftp.Client) error {
		return c.Remove(tempPath)
	})
	if err != nil {
		e.warnf("native-large: remove remote temp %s: %v (the published file is unaffected; clean up manually or rerun)", tempPath, err)
	}
}

// nativeTempPath reads a completed native task's temp path from its
// checkpoint before the checkpoint is retired.
func (e *Executor) nativeTempPath(t *state.Task) string {
	if t.Checkpoint == "" || !strings.HasSuffix(t.Checkpoint, ".nativeckpt") {
		return ""
	}
	ck, err := multistream.LoadCheckpoint(t.Checkpoint)
	if err != nil {
		e.warnf("native-large: read checkpoint for %q: %v", t.Path, err)
		return ""
	}
	return ck.TempPath
}
