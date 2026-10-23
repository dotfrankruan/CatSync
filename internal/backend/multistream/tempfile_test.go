//go:build unix

package multistream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pkg/sftp"
)

// fakeInfo is a minimal os.FileInfo for fake remote ops.
type fakeInfo struct {
	name string
	size int64
	mode os.FileMode
}

func (f fakeInfo) Name() string       { return f.name }
func (f fakeInfo) Size() int64        { return f.size }
func (f fakeInfo) Mode() os.FileMode  { return f.mode }
func (f fakeInfo) ModTime() time.Time { return time.Unix(0, 0) }
func (f fakeInfo) IsDir() bool        { return f.mode.IsDir() }
func (f fakeInfo) Sys() any           { return nil }

// fakeOps is a package-internal remoteOps for failure injection; every
// called op is recorded in order.
type fakeOps struct {
	mu         sync.Mutex
	calls      []string
	lstatFn    func(path string) (os.FileInfo, error)
	openFileFn func(path string, flags int) (remoteFile, error)
	chmodFn    func(path string, mode os.FileMode) error
	removeFn   func(path string) error
}

func (o *fakeOps) record(format string, args ...any) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.calls = append(o.calls, fmt.Sprintf(format, args...))
}

func (o *fakeOps) Lstat(p string) (os.FileInfo, error) {
	o.record("lstat %s", p)
	return o.lstatFn(p)
}

func (o *fakeOps) OpenFile(p string, flags int) (remoteFile, error) {
	o.record("openfile %s flags=%#x", p, flags)
	return o.openFileFn(p, flags)
}

func (o *fakeOps) Chmod(p string, mode os.FileMode) error {
	o.record("chmod %s %o", p, mode)
	return o.chmodFn(p, mode)
}

func (o *fakeOps) Remove(p string) error {
	o.record("remove %s", p)
	return o.removeFn(p)
}

func (o *fakeOps) callList() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string{}, o.calls...)
}

// fakeFile is a remoteFile whose behavior comes from function fields.
type fakeFile struct {
	writeAtFn  func(p []byte, off int64) (int, error)
	truncateFn func(size int64) error
	syncFn     func() error
	statFn     func() (os.FileInfo, error)
	closeFn    func() error
}

func (f *fakeFile) WriteAt(p []byte, off int64) (int, error) {
	if f.writeAtFn != nil {
		return f.writeAtFn(p, off)
	}
	return len(p), nil
}
func (f *fakeFile) Truncate(size int64) error {
	if f.truncateFn != nil {
		return f.truncateFn(size)
	}
	return nil
}
func (f *fakeFile) Sync() error {
	if f.syncFn != nil {
		return f.syncFn()
	}
	return nil
}
func (f *fakeFile) Stat() (os.FileInfo, error) {
	if f.statFn != nil {
		return f.statFn()
	}
	return fakeInfo{name: "t", size: 0, mode: 0}, nil
}
func (f *fakeFile) Close() error {
	if f.closeFn != nil {
		return f.closeFn()
	}
	return nil
}

func tempSpec() RemoteTempSpec {
	return RemoteTempSpec{
		FinalPath: "/data/big.bin",
		TempPath:  "/data/.catsync-tmp-big.bin",
		Size:      256,
	}
}

func TestRemoteTempValidationBeforeRemoteCalls(t *testing.T) {
	bad := []struct {
		name string
		spec RemoteTempSpec
	}{
		{"empty final", RemoteTempSpec{FinalPath: "", TempPath: "/data/.t", Size: 1}},
		{"NUL final", RemoteTempSpec{FinalPath: "/data/a\x00b", TempPath: "/data/.t", Size: 1}},
		{"relative final", RemoteTempSpec{FinalPath: "data/a", TempPath: "/data/.t", Size: 1}},
		{"unclean final", RemoteTempSpec{FinalPath: "/data//a", TempPath: "/data/.t", Size: 1}},
		{"root final", RemoteTempSpec{FinalPath: "/", TempPath: "/.t", Size: 1}},
		{"identical paths", RemoteTempSpec{FinalPath: "/data/a", TempPath: "/data/a", Size: 1}},
		{"different parents", RemoteTempSpec{FinalPath: "/data/a", TempPath: "/other/.t", Size: 1}},
		{"size beyond int64", RemoteTempSpec{FinalPath: "/data/a", TempPath: "/data/.t", Size: math.MaxInt64 + 1}},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			ops := &fakeOps{
				lstatFn: func(string) (os.FileInfo, error) {
					return nil, errors.New("lstat must not be called")
				},
			}
			if err := prepareFreshTemp(ops, tc.spec); !errors.Is(err, ErrInvalidRemoteTemp) {
				t.Errorf("prepareFreshTemp = %v, want ErrInvalidRemoteTemp", err)
			}
			if _, err := openResumeTemp(ops, tc.spec); !errors.Is(err, ErrInvalidRemoteTemp) {
				t.Errorf("openResumeTemp = %v, want ErrInvalidRemoteTemp", err)
			}
			if len(ops.callList()) != 0 {
				t.Errorf("remote ops called before validation failed: %v", ops.callList())
			}
		})
	}
}

// TestPrepareFreshTempFakeOps verifies the fresh flow's call order and
// flags through the fake ops: exclusive create, chmod 0600, truncate, sync,
// close.
func TestPrepareFreshTempFakeOps(t *testing.T) {
	spec := tempSpec()
	f := &fakeFile{}
	ops := &fakeOps{
		lstatFn: func(string) (os.FileInfo, error) { return nil, os.ErrNotExist },
		openFileFn: func(p string, flags int) (remoteFile, error) {
			// Portable flag semantics: exclusive create, write-only.
			if flags&os.O_WRONLY == 0 || flags&os.O_CREATE == 0 || flags&os.O_EXCL == 0 {
				t.Errorf("OpenFile flags = %#x, want O_WRONLY|O_CREATE|O_EXCL", flags)
			}
			if p != spec.TempPath {
				t.Errorf("OpenFile path = %q, want %q", p, spec.TempPath)
			}
			return f, nil
		},
		chmodFn: func(p string, mode os.FileMode) error {
			if mode != 0o600 {
				t.Errorf("chmod mode = %o, want 600", mode)
			}
			return nil
		},
		removeFn: func(string) error { return nil },
	}
	if err := prepareFreshTemp(ops, spec); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"lstat /data/big.bin",
		fmt.Sprintf("openfile /data/.catsync-tmp-big.bin flags=%#x", os.O_WRONLY|os.O_CREATE|os.O_EXCL),
		"chmod /data/.catsync-tmp-big.bin 600",
	}
	got := ops.callList()
	if strings.Join(got, ";") != strings.Join(want, ";") {
		t.Errorf("call order = %v, want %v", got, want)
	}
}

// TestPrepareFreshTempFailureCleansTemp: a truncate/sync/close failure
// removes exactly the created temp, and the first error is preserved; a
// failing remove joins instead of masking.
func TestPrepareFreshTempFailureCleansTemp(t *testing.T) {
	spec := tempSpec()
	boom := errors.New("truncate boom")
	mk := func(removeErr error) *fakeOps {
		f := &fakeFile{truncateFn: func(int64) error { return boom }}
		return &fakeOps{
			lstatFn:    func(string) (os.FileInfo, error) { return nil, os.ErrNotExist },
			openFileFn: func(string, int) (remoteFile, error) { return f, nil },
			chmodFn:    func(string, os.FileMode) error { return nil },
			removeFn:   func(string) error { return removeErr },
		}
	}
	ops := mk(nil)
	err := prepareFreshTemp(ops, spec)
	if !errors.Is(err, boom) {
		t.Fatalf("prepareFreshTemp = %v, want it to wrap the truncate error", err)
	}
	got := ops.callList()
	if n := len(got); n == 0 || got[len(got)-1] != "remove /data/.catsync-tmp-big.bin" {
		t.Errorf("calls = %v, want a trailing remove of the created temp", got)
	}
	// Cleanup failure joins, never masks.
	ops2 := mk(errors.New("remove boom"))
	err = prepareFreshTemp(ops2, spec)
	if !errors.Is(err, boom) || !strings.Contains(err.Error(), "remove boom") {
		t.Errorf("joined error = %v, want both the cause and the cleanup failure", err)
	}
}

func TestPrepareFreshTempRejectsExistingFinal(t *testing.T) {
	spec := tempSpec()
	ops := &fakeOps{
		lstatFn: func(p string) (os.FileInfo, error) {
			if p == spec.FinalPath {
				return fakeInfo{name: "big.bin", mode: 0}, nil // exists
			}
			return nil, os.ErrNotExist
		},
		openFileFn: func(string, int) (remoteFile, error) {
			return nil, errors.New("OpenFile must not be called")
		},
	}
	if err := prepareFreshTemp(ops, spec); !errors.Is(err, ErrRemoteFinalExists) {
		t.Fatalf("prepareFreshTemp = %v, want ErrRemoteFinalExists", err)
	}
}

func TestPrepareFreshTempRejectsExistingTemp(t *testing.T) {
	spec := tempSpec()
	ops := &fakeOps{
		lstatFn: func(p string) (os.FileInfo, error) {
			if p == spec.TempPath {
				return fakeInfo{name: "tmp", mode: 0}, nil
			}
			return nil, os.ErrNotExist
		},
		// The exclusive create fails as the wire reports it; Lstat then
		// confirms the object exists.
		openFileFn: func(string, int) (remoteFile, error) { return nil, errors.New("failure") },
	}
	if err := prepareFreshTemp(ops, spec); !errors.Is(err, ErrRemoteTempExists) {
		t.Fatalf("prepareFreshTemp = %v, want ErrRemoteTempExists", err)
	}
}

func TestOpenResumeTempFakeOps(t *testing.T) {
	spec := tempSpec()
	regular := fakeInfo{name: "tmp", size: 256, mode: 0o600}
	f := &fakeFile{statFn: func() (os.FileInfo, error) { return regular, nil }}
	ops := &fakeOps{
		lstatFn: func(p string) (os.FileInfo, error) {
			if p == spec.TempPath {
				return regular, nil
			}
			return nil, os.ErrNotExist
		},
		openFileFn: func(p string, flags int) (remoteFile, error) {
			if flags != os.O_WRONLY {
				t.Errorf("resume OpenFile flags = %#x, want O_WRONLY only", flags)
			}
			return f, nil
		},
	}
	w, err := openResumeTemp(ops, spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenResumeTempFailures(t *testing.T) {
	spec := tempSpec()
	cases := []struct {
		name     string
		tempInfo os.FileInfo
		lstatErr error
		statFn   func() (os.FileInfo, error)
		want     error
	}{
		{"missing", nil, os.ErrNotExist, nil, ErrRemoteTempMissing},
		{"directory", fakeInfo{name: "tmp", mode: os.ModeDir}, nil, nil, ErrRemoteTempMismatch},
		{"symlink", fakeInfo{name: "tmp", mode: os.ModeSymlink}, nil, nil, ErrRemoteTempMismatch},
		{"wrong size", fakeInfo{name: "tmp", size: 255, mode: 0o600}, nil, nil, ErrRemoteTempMismatch},
		// Replaced between Lstat and open: the handle's Stat disagrees.
		{"replaced after open", fakeInfo{name: "tmp", size: 256, mode: 0o600}, nil,
			func() (os.FileInfo, error) { return fakeInfo{name: "tmp", size: 7, mode: 0o600}, nil }, ErrRemoteTempMismatch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ops := &fakeOps{
				lstatFn: func(p string) (os.FileInfo, error) {
					if p == spec.TempPath {
						return tc.tempInfo, tc.lstatErr
					}
					return nil, os.ErrNotExist
				},
				openFileFn: func(string, int) (remoteFile, error) {
					if tc.name == "missing" {
						return nil, errors.New("OpenFile must not be called")
					}
					return &fakeFile{statFn: tc.statFn}, nil
				},
			}
			_, err := openResumeTemp(ops, spec)
			if !errors.Is(err, tc.want) {
				t.Errorf("openResumeTemp = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestOpenResumeTempRejectsExistingFinal(t *testing.T) {
	spec := tempSpec()
	ops := &fakeOps{
		lstatFn: func(p string) (os.FileInfo, error) {
			if p == spec.FinalPath {
				return fakeInfo{name: "big.bin", mode: 0}, nil
			}
			return nil, os.ErrNotExist
		},
	}
	if _, err := openResumeTemp(ops, spec); !errors.Is(err, ErrRemoteFinalExists) {
		t.Fatalf("openResumeTemp = %v, want ErrRemoteFinalExists", err)
	}
}

// withRealSFTPServer runs fn against the real OpenSSH sftp-server (which
// supports fsync@openssh.com), skipping when it is unavailable.
func withRealSFTPServer(t *testing.T, fn func(*sftp.Client) error) {
	t.Helper()
	if !realServerAvailable() {
		t.Skipf("real sftp-server %s not available; skipping real-server test", sftpServerBin)
	}
	cfg := helperConfig(t, t.TempDir(), envRealServer+"=1")
	if err := cfg.WithClient(context.Background(), fn); err != nil {
		t.Fatalf("WithClient: %v", err)
	}
}

func TestRemoteTempFreshRealServer(t *testing.T) {
	dir := t.TempDir()
	final := path.Join(dir, "big.bin")
	tmp := path.Join(dir, ".catsync-tmp-big.bin")
	withRealSFTPServer(t, func(client *sftp.Client) error {
		spec := RemoteTempSpec{FinalPath: final, TempPath: tmp, Size: 4096}
		if err := PrepareFreshTemp(client, spec); err != nil {
			return err
		}
		fi, err := client.Lstat(tmp)
		if err != nil {
			return err
		}
		if fi.Size() != 4096 {
			return fmt.Errorf("temp size = %d, want 4096", fi.Size())
		}
		if got := fi.Mode().Perm(); got != 0o600 {
			return fmt.Errorf("temp mode = %o, want 600", got)
		}
		if _, err := client.Lstat(final); !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("final path must not exist: %v", err)
		}
		return nil
	})
}

func TestRemoteTempZeroSizeRealServer(t *testing.T) {
	dir := t.TempDir()
	final := path.Join(dir, "empty.bin")
	tmp := path.Join(dir, ".catsync-tmp-empty.bin")
	withRealSFTPServer(t, func(client *sftp.Client) error {
		spec := RemoteTempSpec{FinalPath: final, TempPath: tmp, Size: 0}
		if err := PrepareFreshTemp(client, spec); err != nil {
			return err
		}
		fi, err := client.Lstat(tmp)
		if err != nil {
			return err
		}
		if fi.Size() != 0 {
			return fmt.Errorf("zero temp size = %d, want 0", fi.Size())
		}
		return nil
	})
}

func TestRemoteTempFreshExistingFinalRealServer(t *testing.T) {
	dir := t.TempDir()
	final := path.Join(dir, "big.bin")
	tmp := path.Join(dir, ".catsync-tmp-big.bin")
	withRealSFTPServer(t, func(client *sftp.Client) error {
		// The final file already exists.
		f, err := client.OpenFile(final, os.O_WRONLY|os.O_CREATE)
		if err != nil {
			return err
		}
		if _, err := f.Write([]byte("existing")); err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		err = PrepareFreshTemp(client, RemoteTempSpec{FinalPath: final, TempPath: tmp, Size: 1})
		if !errors.Is(err, ErrRemoteFinalExists) {
			return fmt.Errorf("fresh with existing final = %v, want ErrRemoteFinalExists", err)
		}
		if _, lerr := client.Lstat(tmp); !errors.Is(lerr, os.ErrNotExist) {
			return fmt.Errorf("temp must not be created when final exists: %v", lerr)
		}
		// The final's content is untouched.
		got, err := readAllRemote(client, final)
		if err != nil {
			return err
		}
		if string(got) != "existing" {
			return fmt.Errorf("final content = %q, want untouched %q", got, "existing")
		}
		return nil
	})
}

func TestRemoteTempFreshFinalSymlinkRealServer(t *testing.T) {
	dir := t.TempDir()
	final := path.Join(dir, "big.bin")
	tmp := path.Join(dir, ".catsync-tmp-big.bin")
	withRealSFTPServer(t, func(client *sftp.Client) error {
		if err := client.Symlink("/nonexistent-target", final); err != nil {
			return err
		}
		err := PrepareFreshTemp(client, RemoteTempSpec{FinalPath: final, TempPath: tmp, Size: 1})
		if !errors.Is(err, ErrRemoteFinalExists) {
			return fmt.Errorf("fresh with symlink final = %v, want ErrRemoteFinalExists", err)
		}
		return nil
	})
}

func TestRemoteTempFreshExistingTempRealServer(t *testing.T) {
	dir := t.TempDir()
	final := path.Join(dir, "big.bin")
	tmp := path.Join(dir, ".catsync-tmp-big.bin")
	withRealSFTPServer(t, func(client *sftp.Client) error {
		f, err := client.OpenFile(tmp, os.O_WRONLY|os.O_CREATE)
		if err != nil {
			return err
		}
		if _, err := f.Write([]byte("PREEXISTING")); err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		err = PrepareFreshTemp(client, RemoteTempSpec{FinalPath: final, TempPath: tmp, Size: 11})
		if !errors.Is(err, ErrRemoteTempExists) {
			return fmt.Errorf("fresh with existing temp = %v, want ErrRemoteTempExists", err)
		}
		got, err := readAllRemote(client, tmp)
		if err != nil {
			return err
		}
		if string(got) != "PREEXISTING" {
			return fmt.Errorf("temp content = %q, want untouched (no overwrite/truncate)", got)
		}
		return nil
	})
}

func TestRemoteTempFreshTempIsDirOrSymlinkRealServer(t *testing.T) {
	dir := t.TempDir()
	final := path.Join(dir, "big.bin")
	tmpDir := path.Join(dir, ".catsync-tmp-dir")
	tmpLink := path.Join(dir, ".catsync-tmp-link")
	withRealSFTPServer(t, func(client *sftp.Client) error {
		if err := client.Mkdir(tmpDir); err != nil {
			return err
		}
		if err := client.Symlink("/nonexistent-target", tmpLink); err != nil {
			return err
		}
		for _, tmp := range []string{tmpDir, tmpLink} {
			err := PrepareFreshTemp(client, RemoteTempSpec{FinalPath: final, TempPath: tmp, Size: 1})
			if !errors.Is(err, ErrRemoteTempExists) {
				return fmt.Errorf("fresh with temp %s = %v, want ErrRemoteTempExists", tmp, err)
			}
		}
		return nil
	})
}

func TestRemoteTempResumeRealServer(t *testing.T) {
	dir := t.TempDir()
	final := path.Join(dir, "big.bin")
	tmp := path.Join(dir, ".catsync-tmp-big.bin")
	withRealSFTPServer(t, func(client *sftp.Client) error {
		// Prepare a 16-byte temp of zeros.
		if err := PrepareFreshTemp(client, RemoteTempSpec{FinalPath: final, TempPath: tmp, Size: 16}); err != nil {
			return err
		}
		// Missing temp: resume is rejected.
		if _, err := OpenResumeTemp(client, RemoteTempSpec{FinalPath: final, TempPath: tmp + ".nope", Size: 16}); !errors.Is(err, ErrRemoteTempMissing) {
			return fmt.Errorf("resume missing = %v, want ErrRemoteTempMissing", err)
		}
		// Wrong size: rejected.
		if _, err := OpenResumeTemp(client, RemoteTempSpec{FinalPath: final, TempPath: tmp, Size: 15}); !errors.Is(err, ErrRemoteTempMismatch) {
			return fmt.Errorf("resume wrong size = %v, want ErrRemoteTempMismatch", err)
		}
		// Success path: resume, write at a non-zero offset, sync, close.
		w, err := OpenResumeTemp(client, RemoteTempSpec{FinalPath: final, TempPath: tmp, Size: 16})
		if err != nil {
			return err
		}
		if _, err := w.WriteAt([]byte("AB"), 5); err != nil {
			return fmt.Errorf("WriteAt: %w", err)
		}
		if err := w.Sync(); err != nil {
			return fmt.Errorf("Sync: %w", err)
		}
		if err := w.Close(); err != nil {
			return fmt.Errorf("Close: %w", err)
		}
		// Writing after close must fail.
		if _, err := w.WriteAt([]byte("X"), 0); err == nil {
			return errors.New("WriteAt after Close = nil, want error")
		}
		got, err := readAllRemote(client, tmp)
		if err != nil {
			return err
		}
		want := append([]byte{}, make([]byte, 5)...)
		want = append(want, 'A', 'B')
		want = append(want, make([]byte, 9)...)
		if string(got) != string(want) {
			return fmt.Errorf("temp content = %q, want %q (offset write)", got, want)
		}
		if _, err := client.Lstat(final); !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("final must not exist: %v", err)
		}
		return nil
	})
}

func TestRemoteTempResumeNonRegularRealServer(t *testing.T) {
	dir := t.TempDir()
	final := path.Join(dir, "big.bin")
	tmpDir := path.Join(dir, ".catsync-tmp-dir")
	tmpLink := path.Join(dir, ".catsync-tmp-link")
	withRealSFTPServer(t, func(client *sftp.Client) error {
		if err := client.Mkdir(tmpDir); err != nil {
			return err
		}
		if _, err := OpenResumeTemp(client, RemoteTempSpec{FinalPath: final, TempPath: tmpDir, Size: 0}); !errors.Is(err, ErrRemoteTempMismatch) {
			return fmt.Errorf("resume dir = %v, want ErrRemoteTempMismatch", err)
		}
		if err := client.Symlink("/nonexistent-target", tmpLink); err != nil {
			return err
		}
		if _, err := OpenResumeTemp(client, RemoteTempSpec{FinalPath: final, TempPath: tmpLink, Size: 0}); !errors.Is(err, ErrRemoteTempMismatch) {
			return fmt.Errorf("resume symlink = %v, want ErrRemoteTempMismatch", err)
		}
		return nil
	})
}

// readAllRemote reads a remote file fully.
func readAllRemote(client *sftp.Client, path string) ([]byte, error) {
	f, err := client.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}
