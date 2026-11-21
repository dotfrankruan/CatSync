//go:build !unix

package state

import (
	"errors"
	"os"
)

func tryLockFile(*os.File) (bool, error) {
	return false, errors.New("advisory file locks are not supported on this platform")
}

func unlockFile(*os.File) error { return nil }
