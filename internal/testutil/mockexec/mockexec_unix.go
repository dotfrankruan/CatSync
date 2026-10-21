//go:build unix

package mockexec

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

// ProcessAlive reports whether pid refers to an existing process (including
// a zombie, which is exactly what orphan checks must catch). The result is
// nil when the process exists.
func ProcessAlive(pid int) error {
	return syscall.Kill(pid, 0)
}

// signalByName maps a mock signal name to its Unix signal.
func signalByName(name string) (syscall.Signal, bool) {
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
	return sig, ok
}

// raiseSignal sends the named signal to the current process.
func raiseSignal(name string) error {
	sig, ok := signalByName(name)
	if !ok {
		return fmt.Errorf("unknown signal %q", name)
	}
	return syscall.Kill(syscall.Getpid(), sig)
}

// installZeroExitSignal catches name (TERM or INT), records it to path
// when set, and exits 0: a tool that treats a forwarded cancellation
// signal as a graceful shutdown.
func installZeroExitSignal(name, path string) error {
	sig, ok := signalByName(name)
	if !ok {
		return fmt.Errorf("unknown zero-exit signal %q", name)
	}
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, sig)
	go func() {
		s := <-ch
		if path != "" {
			_ = os.WriteFile(path, []byte(s.String()), 0600)
		}
		os.Exit(0)
	}()
	return nil
}

// ignoreTerm makes the helper ignore SIGTERM, forcing the process runner's
// SIGKILL escalation path.
func ignoreTerm() {
	signal.Ignore(syscall.SIGTERM)
}

// installCatchSignal records the first SIGINT/SIGTERM the helper receives
// to the signal path and exits 42, proving which signal was forwarded.
func installCatchSignal(path string) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-ch
		if path != "" {
			_ = os.WriteFile(path, []byte(sig.String()), 0600)
		}
		os.Exit(42)
	}()
}
