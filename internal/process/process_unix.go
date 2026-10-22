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

// Handle is a running interactive subprocess in its own process group.
// The caller drives its stdio while it runs and must call Wait exactly
// once, after the session is over. Wait reaps the direct child, verifies
// the process group is empty, and reports the result with the same
// classification as Run.
type Handle struct {
	name           string
	ctx            context.Context
	cmd            *exec.Cmd
	pgid           int
	grace          time.Duration
	exited         chan struct{} // closed once the direct child is reaped
	cancelObserved chan struct{} // closed when the watcher saw cancellation
	watcherDone    chan struct{} // closed when the watcher is finished
	done           chan struct{} // closed when cleanup is complete
	waitErr        error
	reapErr        error
}

// startInteractive starts cmd in its own process group and returns a
// Handle. Cancellation of ctx forwards the graceful signal to the whole
// group, then SIGKILLs survivors after the grace period.
func startInteractive(ctx context.Context, cmd *exec.Cmd, spec Spec) (*Handle, error) {
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
		return nil, fmt.Errorf("process: start %s: %w", spec.Name, err)
	}
	h := &Handle{
		name:           spec.Name,
		ctx:            ctx,
		cmd:            cmd,
		pgid:           cmd.Process.Pid,
		grace:          grace,
		exited:         make(chan struct{}),
		cancelObserved: make(chan struct{}),
		watcherDone:    make(chan struct{}),
		done:           make(chan struct{}),
	}
	go h.watch()
	go h.wait()
	return h, nil
}

// watch forwards cancellation to the process group.
func (h *Handle) watch() {
	defer close(h.watcherDone)
	select {
	case <-h.ctx.Done():
		close(h.cancelObserved)
	case <-h.exited:
		return
	}
	terminateGroup(h.pgid, gracefulSignal(h.ctx), h.grace)
}

// wait reaps the direct child, cleans the group, and joins the watcher.
func (h *Handle) wait() {
	h.waitErr = h.cmd.Wait()
	close(h.exited)
	h.reapErr = reapGroup(h.pgid, h.grace)
	<-h.watcherDone
	close(h.done)
}

// Exited returns a channel closed once the direct child has exited (but
// possibly before group cleanup has finished).
func (h *Handle) Exited() <-chan struct{} { return h.exited }

// Wait blocks until the process and its group are cleaned up, then returns
// the classified result.
func (h *Handle) Wait() error {
	<-h.done
	var ctxErr error
	select {
	case <-h.cancelObserved:
		ctxErr = h.ctx.Err()
	default:
	}
	return classifyRunResult(h.name, h.waitErr, ctxErr, h.reapErr)
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
