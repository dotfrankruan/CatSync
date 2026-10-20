//go:build unix

package mockexec

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
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

// ignoreTerm makes the helper ignore SIGTERM, forcing the process runner's
// SIGKILL escalation path.
func ignoreTerm() {
	signal.Ignore(syscall.SIGTERM)
}

// installCatchSignal records the first SIGINT/SIGTERM the helper receives
// to <dir>/signal and exits 42, proving which signal was forwarded.
func installCatchSignal(dir string) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-ch
		if dir != "" {
			_ = os.WriteFile(filepath.Join(dir, "signal"), []byte(sig.String()), 0600)
		}
		os.Exit(42)
	}()
}
