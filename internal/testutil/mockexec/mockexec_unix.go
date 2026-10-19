//go:build unix

package mockexec

import (
	"fmt"
	"syscall"
)

// ProcessAlive reports whether pid refers to an existing process (including
// a zombie, which is exactly what orphan checks must catch). The result is
// nil when the process exists.
func ProcessAlive(pid int) error {
	return syscall.Kill(pid, 0)
}

// raiseSignal sends the named signal to the current process.
func raiseSignal(name string) error {
	signals := map[string]syscall.Signal{
		"TERM": syscall.SIGTERM,
		"KILL": syscall.SIGKILL,
		"INT":  syscall.SIGINT,
		"HUP":  syscall.SIGHUP,
		"QUIT": syscall.SIGQUIT,
		"USR1": syscall.SIGUSR1,
		"USR2": syscall.SIGUSR2,
	}
	sig, ok := signals[name]
	if !ok {
		return fmt.Errorf("unknown signal %q", name)
	}
	return syscall.Kill(syscall.Getpid(), sig)
}
