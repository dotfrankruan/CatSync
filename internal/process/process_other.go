//go:build !unix

package process

import (
	"context"
	"fmt"
	"os"
	"os/exec"
)

// Handle is a running interactive subprocess. The caller drives its stdio
// while it runs and must call Wait exactly once, after the session is
// over. Non-Unix platforms do not expose process groups to this package,
// so descendants cannot be enumerated or signalled; the direct child is
// still waited for.
type Handle struct {
	name           string
	ctx            context.Context
	exited         chan struct{}
	cancelObserved chan struct{}
	watcherDone    chan struct{}
	done           chan struct{}
	waitErr        error
}

// startInteractive starts cmd and kills the direct child when ctx ends.
func startInteractive(ctx context.Context, cmd *exec.Cmd, spec Spec) (*Handle, error) {
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("process: start %s: %w", spec.Name, err)
	}
	h := &Handle{
		name:           spec.Name,
		ctx:            ctx,
		exited:         make(chan struct{}),
		cancelObserved: make(chan struct{}),
		watcherDone:    make(chan struct{}),
		done:           make(chan struct{}),
	}
	go func() {
		defer close(h.watcherDone)
		select {
		case <-ctx.Done():
			close(h.cancelObserved)
			_ = cmd.Process.Kill()
		case <-h.exited:
		}
	}()
	go func() {
		h.waitErr = cmd.Wait()
		close(h.exited)
		<-h.watcherDone
		close(h.done)
	}()
	return h, nil
}

// Exited returns a channel closed once the direct child has exited.
func (h *Handle) Exited() <-chan struct{} { return h.exited }

// Wait blocks until the process has been waited for and returns the
// classified result.
func (h *Handle) Wait() error {
	<-h.done
	var ctxErr error
	select {
	case <-h.cancelObserved:
		ctxErr = h.ctx.Err()
	default:
	}
	return classifyRunResult(h.name, h.waitErr, ctxErr, nil)
}

// exitSignal always reports false on non-Unix platforms, where the
// terminating signal is not exposed through syscall.WaitStatus.
func exitSignal(err *exec.ExitError) (os.Signal, bool) {
	return nil, false
}
