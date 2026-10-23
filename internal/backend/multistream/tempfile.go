package multistream

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"strings"

	"github.com/pkg/sftp"
)

// Sentinel errors of the remote temporary-file layer; test with errors.Is.
var (
	// ErrRemoteFinalExists rejects writing when the final destination path
	// already exists as a file, directory, or symlink.
	ErrRemoteFinalExists = errors.New("multistream: final destination already exists")
	// ErrRemoteTempExists rejects fresh preparation when the temporary path
	// already exists; the exclusive create never overwrites it.
	ErrRemoteTempExists = errors.New("multistream: temporary file already exists")
	// ErrRemoteTempMissing rejects resume when the temporary path is absent.
	ErrRemoteTempMissing = errors.New("multistream: temporary file does not exist")
	// ErrRemoteTempMismatch rejects resume when the temporary file is not a
	// regular file of exactly the planned size.
	ErrRemoteTempMismatch = errors.New("multistream: temporary file does not match the plan")
	// ErrInvalidRemoteTemp rejects a spec before any remote call: empty,
	// NUL-bearing, relative, unclean, root, identical, or cross-directory
	// paths, or a size that cannot be addressed.
	ErrInvalidRemoteTemp = errors.New("multistream: invalid remote temporary spec")
)

// RemoteTempSpec describes one large file's remote temporary object.
type RemoteTempSpec struct {
	// FinalPath is the absolute POSIX path the file will eventually land at.
	FinalPath string
	// TempPath is the absolute POSIX path of the temporary object. It must
	// share its parent directory with FinalPath, so a later
	// hardlink/no-clobber publish can never cross filesystems.
	TempPath string
	// Size is the exact size the temporary object must hold.
	Size uint64
}

// validate checks the spec before any remote call.
func (s RemoteTempSpec) validate() error {
	for _, f := range []struct{ name, v string }{
		{"final path", s.FinalPath},
		{"temporary path", s.TempPath},
	} {
		if f.v == "" {
			return fmt.Errorf("%w: %s is empty", ErrInvalidRemoteTemp, f.name)
		}
		if strings.ContainsRune(f.v, 0) {
			return fmt.Errorf("%w: %s contains a NUL byte", ErrInvalidRemoteTemp, f.name)
		}
		if !path.IsAbs(f.v) {
			return fmt.Errorf("%w: %s %q is not an absolute POSIX path", ErrInvalidRemoteTemp, f.name, f.v)
		}
		if path.Clean(f.v) != f.v {
			return fmt.Errorf("%w: %s %q is not clean", ErrInvalidRemoteTemp, f.name, f.v)
		}
		if f.v == "/" {
			return fmt.Errorf("%w: %s must not be the root", ErrInvalidRemoteTemp, f.name)
		}
	}
	if s.FinalPath == s.TempPath {
		return fmt.Errorf("%w: final and temporary paths are identical", ErrInvalidRemoteTemp)
	}
	if path.Dir(s.FinalPath) != path.Dir(s.TempPath) {
		return fmt.Errorf("%w: final %q and temporary %q must share a parent directory",
			ErrInvalidRemoteTemp, s.FinalPath, s.TempPath)
	}
	if s.Size > math.MaxInt64 {
		return fmt.Errorf("%w: size %d exceeds the int64 address range", ErrInvalidRemoteTemp, s.Size)
	}
	return nil
}

// remoteFile is the narrow per-handle surface the temp layer uses.
type remoteFile interface {
	io.WriterAt
	// Chmod sets the object's permission bits through the open handle,
	// never via a path that could have been replaced.
	Chmod(mode os.FileMode) error
	// Truncate resizes the object.
	Truncate(size int64) error
	// Sync flushes the object to remote storage.
	Sync() error
	// Stat stats the object through the open handle. It re-verifies the
	// opened object's reported attributes; it cannot prove inode identity
	// across a replace-with-same-attributes attack — that is why the layer
	// instead relies on checkpoint binding, unpredictable run-specific
	// names, and the final no-clobber publish.
	Stat() (os.FileInfo, error)
	Close() error
}

// remoteOps is the narrow client surface the temp layer uses; *sftp.Client
// satisfies it, and tests inject failures through small fakes.
type remoteOps interface {
	Lstat(path string) (os.FileInfo, error)
	OpenFile(path string, flags int) (remoteFile, error)
	Remove(path string) error
}

// clientOps adapts *sftp.Client to remoteOps.
type clientOps struct{ c *sftp.Client }

func (o clientOps) Lstat(p string) (os.FileInfo, error) { return o.c.Lstat(p) }
func (o clientOps) OpenFile(p string, flags int) (remoteFile, error) {
	return o.c.OpenFile(p, flags)
}
func (o clientOps) Remove(p string) error { return o.c.Remove(p) }

// RemoteTempWriter is a write-only random-access handle on the remote
// temporary object. It deliberately exposes WriterAt, Sync, and Close only:
// no seek-based Write, no reads.
type RemoteTempWriter struct {
	f    remoteFile
	path string
}

// WriteAt writes p at the absolute offset off within the temporary object.
func (w *RemoteTempWriter) WriteAt(p []byte, off int64) (int, error) { return w.f.WriteAt(p, off) }

// Sync flushes the object to remote storage.
func (w *RemoteTempWriter) Sync() error { return w.f.Sync() }

// Close closes the handle.
func (w *RemoteTempWriter) Close() error { return w.f.Close() }

// PrepareFreshTemp creates the remote temporary object for a fresh
// transfer. The final destination must not exist in any form (file,
// directory, or symlink). The temporary object is created with
// O_WRONLY|O_CREATE|O_EXCL — an existing temporary object is never
// overwritten or truncated — and its permissions tightened to 0600 through
// the open handle, never through a path that could have been replaced.
// Parent directories are never created; the caller must prepare them
// explicitly beforehand.
//
// Safety against overwriting comes from the remote exclusive create, not
// from a check-then-create sequence.
func PrepareFreshTemp(client *sftp.Client, spec RemoteTempSpec) error {
	if client == nil {
		return fmt.Errorf("%w: nil sftp client", ErrInvalidRemoteTemp)
	}
	return prepareFreshTemp(clientOps{client}, spec)
}

func prepareFreshTemp(ops remoteOps, spec RemoteTempSpec) error {
	if err := spec.validate(); err != nil {
		return err
	}
	if _, err := ops.Lstat(spec.FinalPath); err == nil {
		return fmt.Errorf("%w: %s", ErrRemoteFinalExists, spec.FinalPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("multistream: lstat final path: %w", err)
	}
	f, err := ops.OpenFile(spec.TempPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL)
	if err != nil {
		// The exclusive create is authoritative; Lstat only reclassifies
		// the error when something is actually there.
		if _, lerr := ops.Lstat(spec.TempPath); lerr == nil {
			return fmt.Errorf("%w: %s", ErrRemoteTempExists, spec.TempPath)
		}
		return fmt.Errorf("multistream: create temp: %w", err)
	}
	// The SFTP OPEN packet carries no permission attributes, so the server
	// applied its umask default; tighten to 0600 through the open handle,
	// never via the path.
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		rmErr := ops.Remove(spec.TempPath)
		if rmErr != nil {
			return errors.Join(fmt.Errorf("multistream: chmod temp: %w", err),
				fmt.Errorf("multistream: remove temp after failure: %w", rmErr))
		}
		return fmt.Errorf("multistream: chmod temp: %w", err)
	}
	// From here on, any failure removes exactly the file this call created.
	fail := func(cause error) error {
		_ = f.Close()
		rmErr := ops.Remove(spec.TempPath)
		if rmErr != nil {
			return errors.Join(cause, fmt.Errorf("multistream: remove temp after failure: %w", rmErr))
		}
		return cause
	}
	if err := f.Truncate(int64(spec.Size)); err != nil {
		return fail(fmt.Errorf("multistream: truncate temp: %w", err))
	}
	if err := f.Sync(); err != nil {
		return fail(fmt.Errorf("multistream: sync temp: %w", err))
	}
	if err := f.Close(); err != nil {
		return fail(fmt.Errorf("multistream: close temp: %w", err))
	}
	return nil
}

// OpenResumeTemp reopens a temporary object prepared by an earlier run.
// The final destination must still not exist; the temporary object must
// exist, must be a regular file (never a symlink), and must hold exactly
// spec.Size bytes — checked by Lstat and then again through the open handle
// so a replaced file cannot slip through. The handle is opened O_WRONLY
// only: no O_CREATE, no O_TRUNC, no O_APPEND.
func OpenResumeTemp(client *sftp.Client, spec RemoteTempSpec) (*RemoteTempWriter, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: nil sftp client", ErrInvalidRemoteTemp)
	}
	return openResumeTemp(clientOps{client}, spec)
}

func openResumeTemp(ops remoteOps, spec RemoteTempSpec) (*RemoteTempWriter, error) {
	if err := spec.validate(); err != nil {
		return nil, err
	}
	if _, err := ops.Lstat(spec.FinalPath); err == nil {
		return nil, fmt.Errorf("%w: %s", ErrRemoteFinalExists, spec.FinalPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("multistream: lstat final path: %w", err)
	}
	fi, err := ops.Lstat(spec.TempPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", ErrRemoteTempMissing, spec.TempPath)
		}
		return nil, fmt.Errorf("multistream: lstat temp: %w", err)
	}
	if !fi.Mode().IsRegular() || fi.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%w: %s is not a regular file", ErrRemoteTempMismatch, spec.TempPath)
	}
	if fi.Size() != int64(spec.Size) {
		return nil, fmt.Errorf("%w: %s is %d bytes, want %d", ErrRemoteTempMismatch, spec.TempPath, fi.Size(), spec.Size)
	}
	f, err := ops.OpenFile(spec.TempPath, os.O_WRONLY)
	if err != nil {
		return nil, fmt.Errorf("multistream: open temp: %w", err)
	}
	// Close the check-then-replace window: verify again through the open
	// handle.
	fi2, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("multistream: stat temp after open: %w", err)
	}
	if !fi2.Mode().IsRegular() || fi2.Mode()&os.ModeSymlink != 0 {
		f.Close()
		return nil, fmt.Errorf("%w: %s replaced by a non-regular file", ErrRemoteTempMismatch, spec.TempPath)
	}
	if fi2.Size() != int64(spec.Size) {
		f.Close()
		return nil, fmt.Errorf("%w: %s size changed to %d after open", ErrRemoteTempMismatch, spec.TempPath, fi2.Size())
	}
	return &RemoteTempWriter{f: f, path: spec.TempPath}, nil
}
