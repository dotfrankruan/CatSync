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
