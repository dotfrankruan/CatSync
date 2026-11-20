//go:build !unix

package daemon

import (
	"os/exec"
	"time"
)

// configureChild is a no-op on platforms without Unix process groups.
func configureChild(cmd *exec.Cmd) {}

// terminateChild kills the child; there is no process group to signal.
func terminateChild(cmd *exec.Cmd, grace time.Duration) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Kill()
}
