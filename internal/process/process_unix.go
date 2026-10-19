//go:build unix

package process

import (
	"os"
	"os/exec"
	"syscall"
)

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
