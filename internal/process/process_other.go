//go:build !unix

package process

import (
	"os"
	"os/exec"
)

// exitSignal always reports false on non-Unix platforms, where the
// terminating signal is not exposed through syscall.WaitStatus.
func exitSignal(err *exec.ExitError) (os.Signal, bool) {
	return nil, false
}
