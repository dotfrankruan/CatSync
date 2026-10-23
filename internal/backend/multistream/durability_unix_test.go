//go:build unix

package multistream

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestCheckpointOnlyAfterRemoteSync: with the durability barrier, a chunk's
// index must not appear in the on-disk checkpoint until after the remote
// fsync succeeded. The fake file's Sync blocks on a gate, so the checkpoint
// state is observable before and after the barrier.
func TestCheckpointOnlyAfterRemoteSync(t *testing.T) {
	spec := RemoteTempSpec{FinalPath: "/data/big.bin", TempPath: "/data/.catsync-tmp-big.bin", Size: 64}
	shared := newSharedFakeFile(64)
	syncGate := make(chan struct{})
	syncCalled := make(chan struct{}, 8)

	ops := make([]remoteOps, 2)
	for i := range ops {
		ops[i] = &fakeOps{
			lstatFn: func(p string) (os.FileInfo, error) {
				if p == spec.TempPath {
					return shared.statInfo(), nil
				}
				return nil, os.ErrNotExist
			},
			openFileFn: func(p string, flags int) (remoteFile, error) {
				return &fakeFile{
					statFn: func() (os.FileInfo, error) { return shared.statInfo(), nil },
					writeAtFn: func(b []byte, off int64) (int, error) {
						return shared.writeAt(b, off)
					},
					syncFn: func() error {
						syncCalled <- struct{}{}
						<-syncGate
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
	defer pool.CloseAll()

	ckptPath := filepath.Join(t.TempDir(), "ckpt.json")
	size := spec.Size
	chunks, err := Plan(size, 16) // 4 chunks
	if err != nil {
		t.Fatal(err)
	}
	src := &testReader{data: patternBytes(size)}

	ckpt, err := NewCheckpoint("big.bin", size, time.Unix(1, 0), "h:/data", spec.TempPath, 16)
	if err != nil {
		t.Fatal(err)
	}

	copyDone := make(chan error, 1)
	go func() {
		copyDone <- Copy(context.Background(), src, pool, size, chunks, CopyOptions{
			Workers: 2,
			OnComplete: func(chunk Chunk) error {
				// The durability barrier: sync, then record.
				if err := pool.SyncAny(); err != nil {
					return err
				}
				if err := ckpt.MarkComplete(chunk.Index); err != nil {
					return err
				}
				return ckpt.Save(ckptPath)
			},
		})
	}()

	// Wait for the first sync attempt: a chunk's write is done but the
	// barrier is held.
	<-syncCalled
	// The on-disk checkpoint must not yet contain any chunk: the sync has
	// not returned, so no completion has been recorded.
	loaded, err := LoadCheckpoint(ckptPath)
	if err != nil && !errors.Is(err, ErrCheckpointNotFound) {
		t.Fatal(err)
	}
	if err == nil && len(loaded.Completed) != 0 {
		t.Fatalf("checkpoint claims %v before any sync completed", loaded.Completed)
	}

	// Open the barrier: syncs complete, chunks are recorded.
	close(syncGate)
	if err := <-copyDone; err != nil {
		t.Fatalf("Copy: %v", err)
	}
	loaded, err = LoadCheckpoint(ckptPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Completed) != 4 {
		t.Errorf("completed = %v, want all 4 after sync", loaded.Completed)
	}
}

// TestSyncFailureNeverCheckpointsChunk: when the remote fsync fails, the
// chunk must not enter the checkpoint, the copy must stop, and everything
// must be torn down.
func TestSyncFailureNeverCheckpointsChunk(t *testing.T) {
	spec := RemoteTempSpec{FinalPath: "/data/big.bin", TempPath: "/data/.catsync-tmp-big.bin", Size: 64}
	shared := newSharedFakeFile(64)
	syncBoom := errors.New("fsync failed")

	ops := make([]remoteOps, 2)
	for i := range ops {
		ops[i] = &fakeOps{
			lstatFn: func(p string) (os.FileInfo, error) {
				if p == spec.TempPath {
					return shared.statInfo(), nil
				}
				return nil, os.ErrNotExist
			},
			openFileFn: func(p string, flags int) (remoteFile, error) {
				return &fakeFile{
					statFn: func() (os.FileInfo, error) { return shared.statInfo(), nil },
					writeAtFn: func(b []byte, off int64) (int, error) {
						return shared.writeAt(b, off)
					},
					syncFn: func() error { return syncBoom },
				}, nil
			},
		}
	}
	pool, err := openRemoteWriterPool(spec, ops)
	if err != nil {
		t.Fatal(err)
	}

	ckptPath := filepath.Join(t.TempDir(), "ckpt.json")
	size := spec.Size
	chunks, err := Plan(size, 16)
	if err != nil {
		t.Fatal(err)
	}
	src := &testReader{data: patternBytes(size)}
	ckpt, err := NewCheckpoint("big.bin", size, time.Unix(1, 0), "h:/data", spec.TempPath, 16)
	if err != nil {
		t.Fatal(err)
	}

	err = Copy(context.Background(), src, pool, size, chunks, CopyOptions{
		Workers: 2,
		OnComplete: func(chunk Chunk) error {
			if err := pool.SyncAny(); err != nil {
				return err
			}
			if err := ckpt.MarkComplete(chunk.Index); err != nil {
				return err
			}
			return ckpt.Save(ckptPath)
		},
	})
	if err == nil || !strings.Contains(err.Error(), "fsync failed") {
		t.Fatalf("Copy = %v, want the fsync failure", err)
	}
	// The checkpoint must be empty (or not exist): nothing was durably
	// confirmed.
	loaded, lerr := LoadCheckpoint(ckptPath)
	if lerr != nil && !errors.Is(lerr, ErrCheckpointNotFound) {
		t.Fatal(lerr)
	}
	if lerr == nil && len(loaded.Completed) != 0 {
		t.Errorf("checkpoint claims %v despite the sync failure", loaded.Completed)
	}
	// The pool must be cleanly closable; no goroutine or handle may leak.
	if err := pool.CloseAll(); err != nil {
		t.Errorf("CloseAll: %v", err)
	}
}

// TestDurabilityBarrierNeverAhead runs many multi-worker, multi-chunk
// copies with per-chunk durability assertions: every recorded chunk must
// have a successful sync AFTER its write. Repeated under race.
func TestDurabilityBarrierNeverAhead(t *testing.T) {
	spec := RemoteTempSpec{FinalPath: "/data/big.bin", TempPath: "/data/.catsync-tmp-big.bin", Size: 256}

	for run := 0; run < 20; run++ {
		shared := newSharedFakeFile(256)
		var mu sync.Mutex
		var writeDoneAt []time.Time // per chunk index
		var syncAt []time.Time
		size := spec.Size
		chunks, err := Plan(size, 16) // 16 chunks
		if err != nil {
			t.Fatal(err)
		}
		writeDoneAt = make([]time.Time, len(chunks))

		ops := make([]remoteOps, 4)
		for i := range ops {
			i := i
			ops[i] = &fakeOps{
				lstatFn: func(p string) (os.FileInfo, error) {
					if p == spec.TempPath {
						return shared.statInfo(), nil
					}
					return nil, os.ErrNotExist
				},
				openFileFn: func(p string, flags int) (remoteFile, error) {
					return &fakeFile{
						statFn: func() (os.FileInfo, error) { return shared.statInfo(), nil },
						writeAtFn: func(b []byte, off int64) (int, error) {
							n, err := shared.writeAt(b, off)
							mu.Lock()
							chunkIdx := int(off / 16)
							if int64(len(b))+off == int64((chunkIdx+1)*16) || off+int64(len(b)) == int64(size) {
								writeDoneAt[chunkIdx] = time.Now()
							}
							mu.Unlock()
							return n, err
						},
						syncFn: func() error {
							mu.Lock()
							syncAt = append(syncAt, time.Now())
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

		var recorded []int
		var recMu sync.Mutex
		src := &testReader{data: patternBytes(size)}
		err = Copy(context.Background(), src, pool, size, chunks, CopyOptions{
			Workers: 4,
			OnComplete: func(chunk Chunk) error {
				if err := pool.SyncAny(); err != nil {
					return err
				}
				recMu.Lock()
				recorded = append(recorded, chunk.Index)
				recMu.Unlock()
				return nil
			},
		})
		pool.CloseAll()
		if err != nil {
			t.Fatalf("run %d: Copy: %v", run, err)
		}
		if len(recorded) != 16 {
			t.Fatalf("run %d: recorded %d chunks, want 16", run, len(recorded))
		}
		// Every recorded chunk had its final write before some successful sync.
		mu.Lock()
		for _, idx := range recorded {
			ok := false
			for _, st := range syncAt {
				if !st.Before(writeDoneAt[idx]) {
					ok = true
					break
				}
			}
			if !ok {
				t.Errorf("run %d: chunk %d recorded without a later sync", run, idx)
			}
		}
		mu.Unlock()
	}
}
