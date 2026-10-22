//go:build unix

package process

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// run starts cmd as the leader of a new process group, waits for it, and
// guarantees the group is gone before returning.
func run(ctx context.Context, cmd *exec.Cmd, spec Spec) error {
	grace := spec.GracePeriod
	if grace <= 0 {
		grace = DefaultGracePeriod
	}
	// Setpgid puts the child in its own process group led by its PID, so
	// a group signal reaches every descendant (rsync, ssh) without
	// touching catsync or unrelated processes. WaitDelay bounds the time
	// Wait may spend on stdout/stderr pipes held by survivors.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = grace
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("process: start %s: %w", spec.Name, err)
	}
	pgid := cmd.Process.Pid

	exited := make(chan struct{})
	// cancelObserved is closed only when the watcher takes the ctx.Done()
	// branch and actually performs the teardown. A late cancellation after
	// the child exited normally leaves it closed, so the completed command
	// is not reclassified as interrupted.
	cancelObserved := make(chan struct{})
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-ctx.Done():
			close(cancelObserved)
		case <-exited:
			return
		}
		terminateGroup(pgid, gracefulSignal(ctx), grace)
	}()

	waitErr := cmd.Wait()
	close(exited)
	// The direct child is now waited for. If the context was cancelled,
	// let the watcher finish its bounded teardown before checking the
	// group, so Run never returns while a descendant is still being
	// signalled.
	<-watcherDone

	reapErr := reapGroup(pgid, grace)

	// Only a watcher-observed cancellation is treated as a cancellation.
	// ctx.Err() is read here because the watcher observed Done, so it is
	// non-nil; reading it through ctx keeps the returned error wrapping
	// the caller's context cause (Canceled or DeadlineExceeded).
	var ctxErr error
	select {
	case <-cancelObserved:
		ctxErr = ctx.Err()
	default:
	}
	return classifyRunResult(spec.Name, waitErr, ctxErr, reapErr)
}

// gracefulSignal picks the signal forwarded to the child group: the one
// recorded in the context's CancelSignal (SIGINT or SIGTERM), else SIGTERM.
func gracefulSignal(ctx context.Context) syscall.Signal {
	if slot, ok := CancelSignalFromContext(ctx); ok {
		if sig, ok := slot.Get(); ok {
			if s, isUnix := sig.(syscall.Signal); isUnix {
				switch s {
				case syscall.SIGINT, syscall.SIGTERM:
					return s
				}
			}
		}
	}
	return syscall.SIGTERM
}

// terminateGroup forwards sig to the process group led by pgid, waits up to
// grace for it to disappear, and SIGKILLs whatever remains. Both the group
// and the direct child are signalled, covering the case where the child
// left the group.
func terminateGroup(pgid int, sig syscall.Signal, grace time.Duration) {
	_ = syscall.Kill(-pgid, sig)
	_ = syscall.Kill(pgid, sig)
	if waitGroupGone(pgid, grace) {
		return
	}
	_ = syscall.Kill(-pgid, syscall.SIGKILL)
	_ = syscall.Kill(pgid, syscall.SIGKILL)
	// Bounded best-effort wait; reapGroup reports survivors.
	_ = waitGroupGone(pgid, grace)
}

// reapGroup force-kills any process still alive in the group after the
// direct child was waited for, and verifies the group is empty. A group
// member left behind by a tool that exited successfully is a bug in the
// tool but still must not outlive Catsync, so it is killed here too.
func reapGroup(pgid int, grace time.Duration) error {
	if !groupAlive(pgid) {
		return nil
	}
	_ = syscall.Kill(-pgid, syscall.SIGKILL)
	if waitGroupGone(pgid, grace) {
		return nil
	}
	return fmt.Errorf("%w: process group %d still has live processes", ErrDescendantsAlive, pgid)
}

// groupAlive reports whether any process remains in the group led by pgid.
func groupAlive(pgid int) bool {
	err := syscall.Kill(-pgid, 0)
	if err == nil {
		return true
	}
	// EPERM means at least one process exists but cannot be signalled.
	return errors.Is(err, syscall.EPERM)
}

// waitGroupGone polls until the group is empty or the deadline passes.
func waitGroupGone(pgid int, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for {
		if !groupAlive(pgid) {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// exitSignal extracts the terminating signal from an *exec.ExitError. The
// second result is false when the process exited normally (non-zero status)
// or the platform does not expose the signal.
func exitSignal(err *exec.ExitError) (os.Signal, bool) {
	if err == nil || err.ProcessState == nil {
		return nil, false
	}
	ws, ok := err.Sys().(syscall.WaitStatus)
	if !ok || !ws.Signaled() {
		return nil, false
	}
	return ws.Signal(), true
}
