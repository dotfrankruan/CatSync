//go:build !unix

package mockexec

import "errors"

// ProcessAlive always reports an error on non-Unix platforms, where the
// mock-executable orphan checks are not supported.
func ProcessAlive(pid int) error {
	return errors.New("mockexec: ProcessAlive unsupported on this platform")
}

// raiseSignal always fails on non-Unix platforms.
func raiseSignal(name string) error {
	return errors.New("mockexec: raiseSignal unsupported on this platform")
}

// ignoreTerm is a no-op on non-Unix platforms.
func ignoreTerm() {}

// installCatchSignal is a no-op on non-Unix platforms.
func installCatchSignal(dir string) {}

// installZeroExitSignal is a no-op on non-Unix platforms.
func installZeroExitSignal(name, dir string) error { return nil }

// sshEmulation is unavailable on non-Unix platforms: the mock ssh never
// emulates a remote end there.
func sshEmulation(args []string) (int, bool) { return 0, false }

// SFTPServerAvailable always reports false on non-Unix platforms.
func SFTPServerAvailable() bool { return false }
