package multistream

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var (
	ErrInvalidLocalTemp  = errors.New("multistream: invalid local temporary spec")
	ErrLocalFinalExists  = errors.New("multistream: local final destination already exists")
	ErrLocalTempExists   = errors.New("multistream: local temporary file already exists")
	ErrLocalTempMissing  = errors.New("multistream: local temporary file does not exist")
	ErrLocalTempMismatch = errors.New("multistream: local temporary file does not match the plan")
)

type LocalTempSpec struct {
	FinalPath string
	TempPath  string
	Size      uint64
}

func (s LocalTempSpec) validate() error {
	for _, field := range []struct{ name, value string }{{"final", s.FinalPath}, {"temporary", s.TempPath}} {
		if field.value == "" || strings.ContainsRune(field.value, 0) || !filepath.IsAbs(field.value) || filepath.Clean(field.value) != field.value {
			return fmt.Errorf("%w: %s path %q must be clean and absolute", ErrInvalidLocalTemp, field.name, field.value)
		}
	}
	if s.FinalPath == s.TempPath || filepath.Dir(s.FinalPath) != filepath.Dir(s.TempPath) {
		return fmt.Errorf("%w: final and temp must be distinct files in one directory", ErrInvalidLocalTemp)
	}
	if s.Size > math.MaxInt64 {
		return fmt.Errorf("%w: size %d exceeds int64", ErrInvalidLocalTemp, s.Size)
	}
	return nil
}

func randomLocalTempPath(final string) (string, error) {
	var token [12]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", fmt.Errorf("multistream: generate local temp name: %w", err)
	}
	return filepath.Join(filepath.Dir(final), ".catsync-tmp-"+filepath.Base(final)+"-"+hex.EncodeToString(token[:])), nil
}

// PrepareFreshLocalTemp creates and durably sizes a same-directory temp.
// O_EXCL is authoritative; neither an existing final nor temp is modified.
func PrepareFreshLocalTemp(spec LocalTempSpec) (*os.File, error) {
	if err := spec.validate(); err != nil {
		return nil, err
	}
	if _, err := os.Lstat(spec.FinalPath); err == nil {
		return nil, fmt.Errorf("%w: %s", ErrLocalFinalExists, spec.FinalPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("multistream: lstat local final: %w", err)
	}
	f, err := os.OpenFile(spec.TempPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if _, lerr := os.Lstat(spec.TempPath); lerr == nil {
			return nil, fmt.Errorf("%w: %s", ErrLocalTempExists, spec.TempPath)
		}
		return nil, fmt.Errorf("multistream: create local temp: %w", err)
	}
	fail := func(cause error) (*os.File, error) {
		_ = f.Close()
		if rmErr := os.Remove(spec.TempPath); rmErr != nil && !errors.Is(rmErr, os.ErrNotExist) {
			return nil, errors.Join(cause, fmt.Errorf("remove created local temp: %w", rmErr))
		}
		return nil, cause
	}
	if err := f.Chmod(0o600); err != nil {
		return fail(fmt.Errorf("multistream: chmod local temp: %w", err))
	}
	if err := f.Truncate(int64(spec.Size)); err != nil {
		return fail(fmt.Errorf("multistream: truncate local temp: %w", err))
	}
	if err := f.Sync(); err != nil {
		return fail(fmt.Errorf("multistream: sync local temp: %w", err))
	}
	return f, nil
}

// OpenResumeLocalTemp opens an existing exact-size regular temp without
// creating or truncating it. The open handle is checked against the path's
// pre-open identity to narrow replacement races.
func OpenResumeLocalTemp(spec LocalTempSpec) (*os.File, error) {
	if err := spec.validate(); err != nil {
		return nil, err
	}
	if _, err := os.Lstat(spec.FinalPath); err == nil {
		return nil, fmt.Errorf("%w: %s", ErrLocalFinalExists, spec.FinalPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	before, err := os.Lstat(spec.TempPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s", ErrLocalTempMissing, spec.TempPath)
	}
	if err != nil || !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 || before.Size() != int64(spec.Size) {
		return nil, fmt.Errorf("%w: %s", ErrLocalTempMismatch, spec.TempPath)
	}
	f, err := os.OpenFile(spec.TempPath, os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("multistream: open local temp: %w", err)
	}
	after, err := f.Stat()
	if err != nil || !after.Mode().IsRegular() || after.Size() != int64(spec.Size) || !os.SameFile(before, after) {
		_ = f.Close()
		return nil, fmt.Errorf("%w: local temp changed while opening", ErrLocalTempMismatch)
	}
	return f, nil
}

// SetLocalTempMeta applies metadata while the file is still unpublished.
func SetLocalTempMeta(f *os.File, spec LocalTempSpec, mode os.FileMode, mtime time.Time) error {
	if f == nil {
		return fmt.Errorf("%w: nil temp file", ErrInvalidLocalTemp)
	}
	if err := f.Chmod(mode.Perm()); err != nil {
		return fmt.Errorf("multistream: chmod local temp: %w", err)
	}
	if err := os.Chtimes(spec.TempPath, mtime, mtime); err != nil {
		return fmt.Errorf("multistream: set local temp mtime: %w", err)
	}
	throughHandle, err := f.Stat()
	if err != nil {
		return err
	}
	throughPath, err := os.Lstat(spec.TempPath)
	if err != nil || !os.SameFile(throughHandle, throughPath) {
		return fmt.Errorf("%w: local temp changed while setting metadata", ErrLocalTempMismatch)
	}
	return f.Sync()
}

// PublishLocalFinal uses a same-filesystem hardlink for atomic no-clobber
// publication and verifies exact size and mtime afterwards.
func PublishLocalFinal(spec LocalTempSpec, mtime time.Time) error {
	if err := spec.validate(); err != nil {
		return err
	}
	if err := os.Link(spec.TempPath, spec.FinalPath); err != nil {
		if _, lerr := os.Lstat(spec.FinalPath); lerr == nil {
			return fmt.Errorf("%w: %s", ErrLocalFinalExists, spec.FinalPath)
		}
		return fmt.Errorf("multistream: publish local hardlink: %w", err)
	}
	fi, err := os.Lstat(spec.FinalPath)
	if err != nil || !fi.Mode().IsRegular() || fi.Size() != int64(spec.Size) || !fi.ModTime().Equal(mtime) {
		if err == nil {
			err = fmt.Errorf("size or mtime mismatch")
		}
		return fmt.Errorf("%w: verify published local final: %v", ErrLocalTempMismatch, err)
	}
	return nil
}

// ProbeLocalPublishState recovers the link-succeeded/state-not-saved crash
// window only when final and temp are provably the same filesystem object.
func ProbeLocalPublishState(spec LocalTempSpec) (bool, error) {
	if err := spec.validate(); err != nil {
		return false, err
	}
	final, err := os.Lstat(spec.FinalPath)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	temp, err := os.Lstat(spec.TempPath)
	if err != nil || !os.SameFile(final, temp) {
		return false, fmt.Errorf("%w: local final exists and is not the transfer temp", ErrPublishConflict)
	}
	return true, nil
}
