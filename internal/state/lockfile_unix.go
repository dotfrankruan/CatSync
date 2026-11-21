//go:build unix

package state

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// tryLockFile attempts a nonblocking exclusive advisory lock. It reports
// false when another open file description holds the lock, and a descriptive
// error when the filesystem cannot provide reliable advisory locking.
func tryLockFile(f *os.File) (bool, error) {
	err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, unix.EWOULDBLOCK), errors.Is(err, unix.EAGAIN):
		return false, nil
	case errors.Is(err, unix.ENOLCK), errors.Is(err, unix.ENOTSUP), errors.Is(err, unix.EOPNOTSUPP):
		return false, fmt.Errorf("filesystem does not support reliable advisory locks: %w", err)
	default:
		return false, err
	}
}

func unlockFile(f *os.File) error {
	return unix.Flock(int(f.Fd()), unix.LOCK_UN)
}
