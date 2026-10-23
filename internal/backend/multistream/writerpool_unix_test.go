//go:build unix

package multistream

import (
	"context"
	"errors"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pkg/sftp"
)

// sharedFakeFile is one remote temp object shared by every fake session's
// handle — the fake-FS analogue of N SFTP sessions writing one file.
type sharedFakeFile struct {
	mu      sync.Mutex
	content []byte
}

func newSharedFakeFile(size int64) *sharedFakeFile {
	return &sharedFakeFile{content: make([]byte, size)}
}

func (f *sharedFakeFile) writeAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	copy(f.content[off:off+int64(len(p))], p)
	return len(p), nil
}

func (f *sharedFakeFile) statInfo() os.FileInfo {
	return fakeInfo{name: "t", size: int64(len(f.content)), mode: 0o600}
}

func TestWriterPoolOpensIndependentHandles(t *testing.T) {
	spec := tempSpec()
	var mu sync.Mutex
	var opens []int // flags seen per open call
	ops := make([]remoteOps, 4)
	for i := range ops {
		ops[i] = &fakeOps{
			lstatFn: defaultTempLstat,
			openFileFn: func(p string, flags int) (remoteFile, error) {
				mu.Lock()
				opens = append(opens, flags)
				mu.Unlock()
				return &fakeFile{statFn: func() (os.FileInfo, error) {
					return fakeInfo{name: "t", size: 256, mode: 0o600}, nil
				}}, nil
			},
		}
	}
	pool, err := openRemoteWriterPool(spec, ops)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.CloseAll()
	if len(opens) != 4 {
		t.Fatalf("opens = %d, want 4 (one per session)", len(opens))
	}
	for _, flags := range opens {
		if flags != os.O_WRONLY {
			t.Errorf("pool open flags = %#x, want exactly O_WRONLY (no create/trunc/append)", flags)
		}
	}
}

func TestWriterPoolOpenFailureClosesPrevious(t *testing.T) {
	spec := tempSpec()
	var closed int
	ops := make([]remoteOps, 3)
	for i := range ops {
		i := i
		ops[i] = &fakeOps{
			lstatFn: defaultTempLstat,
			openFileFn: func(p string, flags int) (remoteFile, error) {
				if i == 2 {
					return nil, errors.New("open boom")
				}
				return &fakeFile{
					statFn: func() (os.FileInfo, error) {
						return fakeInfo{name: "t", size: 256, mode: 0o600}, nil
					},
					closeFn: func() error {
						mu := &sync.Mutex{}
						_ = mu
						closed++
						return nil
					},
				}, nil
			},
		}
	}
	_, err := openRemoteWriterPool(spec, ops)
	if err == nil || !strings.Contains(err.Error(), "session 2") {
		t.Fatalf("openRemoteWriterPool = %v, want a session-2 error", err)
	}
	if closed != 2 {
		t.Errorf("closed = %d, want 2 previously opened handles closed", closed)
	}
}

func TestWriterPoolHandleStatMismatch(t *testing.T) {
	spec := tempSpec()
	ops := make([]remoteOps, 2)
	for i := range ops {
		i := i
		ops[i] = &fakeOps{
			lstatFn: defaultTempLstat,
			openFileFn: func(p string, flags int) (remoteFile, error) {
				size := int64(256)
				if i == 1 {
					size = 255 // wrong
				}
				return &fakeFile{statFn: func() (os.FileInfo, error) {
					return fakeInfo{name: "t", size: size, mode: 0o600}, nil
				}}, nil
			},
		}
	}
	_, err := openRemoteWriterPool(spec, ops)
	if !errors.Is(err, ErrRemoteTempMismatch) {
		t.Fatalf("openRemoteWriterPool = %v, want ErrRemoteTempMismatch", err)
	}
}

func TestWriterPoolShortWriteRetryAndZeroProgress(t *testing.T) {
	spec := tempSpec()
	var mu sync.Mutex
	var calls int
	var wrote []byte
	ops := []remoteOps{&fakeOps{
		lstatFn: defaultTempLstat,
		openFileFn: func(string, int) (remoteFile, error) {
			return &fakeFile{
				statFn: func() (os.FileInfo, error) { return fakeInfo{name: "t", size: 256, mode: 0o600}, nil },
				writeAtFn: func(p []byte, off int64) (int, error) {
					mu.Lock()
					defer mu.Unlock()
					calls++
					// Force short writes: accept at most half the payload.
					n := len(p) / 2
					if n == 0 {
						n = len(p)
					}
					wrote = append(wrote, p[:n]...)
					return n, nil
				},
			}, nil
		},
	}}
	pool, err := openRemoteWriterPool(spec, ops)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.CloseAll()
	if _, err := pool.WriteAt([]byte("abcdef"), 0); err != nil {
		t.Fatal(err)
	}
	if string(wrote) != "abcdef" {
		t.Errorf("wrote = %q, want %q (short writes retried in order)", wrote, "abcdef")
	}
	if calls < 2 {
		t.Errorf("calls = %d, want at least 2 (short-write retry)", calls)
	}

	// Zero progress without error is its own error, not an infinite loop.
	zeroOps := []remoteOps{&fakeOps{
		lstatFn: defaultTempLstat,
		openFileFn: func(string, int) (remoteFile, error) {
			return &fakeFile{
				statFn:    func() (os.FileInfo, error) { return fakeInfo{name: "t", size: 256, mode: 0o600}, nil },
				writeAtFn: func(p []byte, off int64) (int, error) { return 0, nil },
			}, nil
		},
	}}
	zeroPool, err := openRemoteWriterPool(spec, zeroOps)
	if err != nil {
		t.Fatal(err)
	}
	defer zeroPool.CloseAll()
	if _, err := zeroPool.WriteAt([]byte("x"), 0); !errors.Is(err, ErrZeroProgressWrite) {
		t.Fatalf("zero-progress WriteAt = %v, want ErrZeroProgressWrite", err)
	}
}

// TestWriterPoolCopySpreadsSessions proves deterministically that Copy over
// the pool fans chunks over multiple distinct SFTP sessions into one shared
// temp object: the write barrier only opens once all four sessions have
// been borrowed, and the shared buffer must end up byte-exact.
func TestWriterPoolCopySpreadsSessions(t *testing.T) {
	spec := RemoteTempSpec{FinalPath: "/data/big.bin", TempPath: "/data/.catsync-tmp-big.bin", Size: 64}
	shared := newSharedFakeFile(64)
	arrived := make(chan int, 4)
	release := make(chan struct{})

	ops := make([]remoteOps, 4)
	for i := range ops {
		idx := i
		ops[i] = &fakeOps{
			lstatFn: func(p string) (os.FileInfo, error) {
				if p == spec.TempPath {
					return shared.statInfo(), nil
				}
				return nil, os.ErrNotExist
			},
			openFileFn: func(p string, flags int) (remoteFile, error) {
				if flags != os.O_WRONLY {
					t.Errorf("pool open flags = %#x, want O_WRONLY", flags)
				}
				return &fakeFile{
					statFn: func() (os.FileInfo, error) { return shared.statInfo(), nil },
					writeAtFn: func(p []byte, off int64) (int, error) {
						arrived <- idx
						<-release
						return shared.writeAt(p, off)
					},
				}, nil
			},
		}
	}
	pool, err := openRemoteWriterPool(spec, ops)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.CloseAll()

	size := uint64(64)
	chunks, err := Plan(size, 16) // 4 chunks
	if err != nil {
		t.Fatal(err)
	}
	src := &testReader{data: patternBytes(size)}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	copyErr := make(chan error, 1)
	go func() {
		copyErr <- Copy(ctx, src, pool, size, chunks, CopyOptions{Workers: 4})
	}()

	// Wait until all four sessions are concurrently in flight.
	seen := map[int]bool{}
	for len(seen) < 4 {
		select {
		case idx := <-arrived:
			seen[idx] = true
		case <-ctx.Done():
			t.Fatal("timed out waiting for four concurrent sessions")
		}
	}
	close(release)
	if err := <-copyErr; err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if got := string(shared.content); got != string(src.data) {
		t.Error("shared temp content mismatch after multi-session Copy")
	}
	if len(pool.UsedSessions()) != 4 {
		t.Errorf("used sessions = %v, want all 4", pool.UsedSessions())
	}
}

func TestWriterPoolSyncAllCloseAllOrder(t *testing.T) {
	spec := tempSpec()
	var mu sync.Mutex
	synced := 0
	closed := 0
	syncErr := errors.New("sync boom")
	ops := make([]remoteOps, 3)
	for i := range ops {
		i := i
		ops[i] = &fakeOps{
			lstatFn: defaultTempLstat,
			openFileFn: func(string, int) (remoteFile, error) {
				return &fakeFile{
					statFn: func() (os.FileInfo, error) { return fakeInfo{name: "t", size: 256, mode: 0o600}, nil },
					syncFn: func() error {
						mu.Lock()
						synced++
						mu.Unlock()
						if i == 1 {
							return syncErr
						}
						return nil
					},
					closeFn: func() error {
						mu.Lock()
						closed++
						mu.Unlock()
						return nil
					},
				}, nil
			},
		}
	}
	pool, err := openRemoteWriterPool(spec, ops)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.SyncAll(); !errors.Is(err, syncErr) {
		t.Fatalf("SyncAll = %v, want the sync error", err)
	}
	if synced != 3 {
		t.Errorf("synced = %d, want 3", synced)
	}
	if err := pool.CloseAll(); err != nil {
		t.Fatalf("CloseAll: %v", err)
	}
	if closed != 3 {
		t.Errorf("closed = %d, want 3", closed)
	}
	// After CloseAll, writes fail.
	if _, err := pool.WriteAt([]byte("x"), 0); !errors.Is(err, ErrWriterPoolClosed) {
		t.Errorf("WriteAt after CloseAll = %v, want ErrWriterPoolClosed", err)
	}
	// CloseAll is idempotent.
	if err := pool.CloseAll(); err != nil {
		t.Errorf("second CloseAll = %v, want nil", err)
	}
}

// TestWriterPoolConcurrencyCap: with 2 sessions, at most 2 writes run
// concurrently; no busy-wait (the blocked calls park on a channel).
func TestWriterPoolConcurrencyCap(t *testing.T) {
	spec := tempSpec()
	gate := make(chan struct{})
	started := make(chan int, 16)
	var inFlight int
	var inFlightMu sync.Mutex
	ops := make([]remoteOps, 2)
	for i := range ops {
		ops[i] = &fakeOps{
			lstatFn: defaultTempLstat,
			openFileFn: func(string, int) (remoteFile, error) {
				return &fakeFile{
					statFn: func() (os.FileInfo, error) { return fakeInfo{name: "t", size: 256, mode: 0o600}, nil },
					writeAtFn: func(p []byte, off int64) (int, error) {
						inFlightMu.Lock()
						inFlight++
						if inFlight > 2 {
							t.Errorf("in-flight writes = %d, want at most 2", inFlight)
						}
						inFlightMu.Unlock()
						started <- 1
						<-gate
						inFlightMu.Lock()
						inFlight--
						inFlightMu.Unlock()
						return len(p), nil
					},
				}, nil
			},
		}
	}
	pool, err := openRemoteWriterPool(spec, ops)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.CloseAll()

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = pool.WriteAt([]byte("x"), 0)
		}()
	}
	// Exactly two can start.
	for i := 0; i < 2; i++ {
		<-started
	}
	// No third may have started while two are parked.
	select {
	case <-started:
		t.Fatal("a third write started while both handles were busy")
	default:
	}
	close(gate)
	wg.Wait()
}

// TestWriterPoolRealServerCopy: four real sftp-server sessions write one
// shared temp file through the pool; the content must be byte-exact.
func TestWriterPoolRealServerCopy(t *testing.T) {
	dir := t.TempDir()
	final := path.Join(dir, "big.bin")
	tmp := path.Join(dir, ".catsync-tmp-big.bin")
	size := uint64(64 << 10)
	withRealSFTPServer(t, func(client *sftp.Client) error { return nil }) // ensures helper mode works
	cfg := helperConfig(t, t.TempDir(), envRealServer+"=1")
	err := cfg.WithClients(context.Background(), 4, func(clients []*sftp.Client) error {
		spec := RemoteTempSpec{FinalPath: final, TempPath: tmp, Size: size}
		if err := PrepareFreshTemp(clients[0], spec); err != nil {
			return err
		}
		pool, err := OpenRemoteWriterPool(spec, clients)
		if err != nil {
			return err
		}
		chunks, err := Plan(size, 4096)
		if err != nil {
			return err
		}
		src := &testReader{data: patternBytes(size)}
		if err := Copy(context.Background(), src, pool, size, chunks, CopyOptions{Workers: 4}); err != nil {
			return err
		}
		if err := pool.SyncAll(); err != nil {
			return err
		}
		if err := pool.CloseAll(); err != nil {
			return err
		}
		got, err := readAllRemote(clients[0], tmp)
		if err != nil {
			return err
		}
		if string(got) != string(src.data) {
			return errors.New("temp content mismatch after multi-session Copy")
		}
		if len(pool.UsedSessions()) < 1 {
			return errors.New("no session was used")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("multi-session Copy: %v", err)
	}
}

func TestWriterPoolNilClient(t *testing.T) {
	spec := tempSpec()
	if _, err := OpenRemoteWriterPool(spec, []*sftp.Client{nil}); !errors.Is(err, ErrInvalidRemoteTemp) {
		t.Fatalf("OpenRemoteWriterPool(nil client) = %v, want ErrInvalidRemoteTemp", err)
	}
}

// defaultTempLstat answers the resume-target validation done by the pool:
// final is absent, temp exists as a regular file of the requested size. The
// spread test's spec has size 64; the others 256 — the pool only checks
// consistency against the spec, so return the size that spec wants by
// reading the path: the spread test uses the same temp path with size 64.
func defaultTempLstat(p string) (os.FileInfo, error) {
	if p == "/data/.catsync-tmp-big.bin" {
		return fakeInfo{name: "t", size: 256, mode: 0o600}, nil
	}
	return nil, os.ErrNotExist
}
