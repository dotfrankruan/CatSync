//go:build !unix

package process

import (
	"context"
	"fmt"
	"os"
	"os/exec"
)

// run starts cmd and kills the direct child when ctx ends. Non-Unix
// platforms do not expose process groups to this package, so descendants
// cannot be enumerated or signalled; the direct child is still waited for.
// As on Unix, a cancellation is reported only when the watcher actually
// observed it.
func run(ctx context.Context, cmd *exec.Cmd, spec Spec) error {
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("process: start %s: %w", spec.Name, err)
	}
	exited := make(chan struct{})
	cancelObserved := make(chan struct{})
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-ctx.Done():
			close(cancelObserved)
			_ = cmd.Process.Kill()
		case <-exited:
		}
	}()
	waitErr := cmd.Wait()
	close(exited)
	<-watcherDone
	var ctxErr error
	select {
	case <-cancelObserved:
		ctxErr = ctx.Err()
	default:
	}
	return classifyRunResult(spec.Name, waitErr, ctxErr, nil)
}

// exitSignal always reports false on non-Unix platforms, where the
// terminating signal is not exposed through syscall.WaitStatus.
func exitSignal(err *exec.ExitError) (os.Signal, bool) {
	return nil, false
}
