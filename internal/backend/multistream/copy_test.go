package multistream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"testing"
)

// pattern returns a deterministic content byte for an absolute offset.
func pattern(off uint64) byte { return byte(off*31 + 7) }

func patternBytes(size uint64) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = pattern(uint64(i))
	}
	return b
}

type readOp struct {
	off uint64
	n   int
}

// testReader serves reads from data, or from readFn when set. Every call is
// recorded.
type testReader struct {
	mu     sync.Mutex
	data   []byte
	reads  []readOp
	readFn func(off uint64, p []byte) (int, error)
}

func (r *testReader) ReadAt(p []byte, off int64) (int, error) {
	r.mu.Lock()
	r.reads = append(r.reads, readOp{off: uint64(off), n: len(p)})
	r.mu.Unlock()
	if r.readFn != nil {
		return r.readFn(uint64(off), p)
	}
	if off < 0 || uint64(off) >= uint64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p, r.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (r *testReader) readCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.reads)
}

type writeOp struct {
	off uint64
	n   int
}

// testWriter applies writes into data (when large enough) and records every
// call. writeFn, when set, controls the (n, err) result — for short writes,
// injected errors, gates, and zero progress — and the recorded/applied
// bytes always reflect the returned n.
type testWriter struct {
	mu      sync.Mutex
	data    []byte
	writes  []writeOp
	writeFn func(off uint64, p []byte) (int, error)
}

func newWriter(size uint64) *testWriter {
	return &testWriter{data: make([]byte, size)}
}

func (w *testWriter) WriteAt(p []byte, off int64) (int, error) {
	n, err := len(p), error(nil)
	if w.writeFn != nil {
		n, err = w.writeFn(uint64(off), p)
	}
	w.mu.Lock()
	if n > 0 {
		end := uint64(off) + uint64(n)
		if end <= uint64(len(w.data)) {
			copy(w.data[uint64(off):end], p[:n])
		}
		w.writes = append(w.writes, writeOp{off: uint64(off), n: n})
	}
	w.mu.Unlock()
	return n, err
}

func (w *testWriter) snapshot() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]byte{}, w.data...)
}

func (w *testWriter) writeCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.writes)
}

func (w *testWriter) ops() []writeOp {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]writeOp{}, w.writes...)
}

// chunkCovering returns the chunk containing offset off.
func chunkCovering(chunks []Chunk, off uint64) (Chunk, bool) {
	for _, c := range chunks {
		if off >= c.Offset && off < c.End() {
			return c, true
		}
	}
	return Chunk{}, false
}

// checkWritesWithinChunks fails the test if any recorded write strays
// outside the chunk its offset belongs to.
func checkWritesWithinChunks(t *testing.T, chunks []Chunk, writes []writeOp) {
	t.Helper()
	for _, w := range writes {
		c, ok := chunkCovering(chunks, w.off)
		if !ok {
			t.Fatalf("write at offset %d is outside every chunk", w.off)
		}
		if uint64(w.n) > c.End()-w.off {
			t.Fatalf("write at %d of %d bytes crosses chunk %d end %d", w.off, w.n, c.Index, c.End())
		}
	}
}

func mustPlan(t *testing.T, size, chunkSize uint64) []Chunk {
	t.Helper()
	chunks, err := Plan(size, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	return chunks
}

func TestCopySingleChunkExact(t *testing.T) {
	size := uint64(200)
	chunks := mustPlan(t, size, 200)
	src := &testReader{data: patternBytes(size)}
	dst := newWriter(size)
	var callbacks []Chunk
	err := Copy(context.Background(), src, dst, size, chunks, CopyOptions{
		Workers: 1,
		OnComplete: func(c Chunk) error {
			callbacks = append(callbacks, c)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if !bytes.Equal(dst.snapshot(), src.data) {
		t.Fatal("destination content differs from source")
	}
	if len(callbacks) != 1 || callbacks[0].Index != 0 {
		t.Fatalf("callbacks = %+v, want exactly chunk 0", callbacks)
	}
	checkWritesWithinChunks(t, chunks, dst.ops())
}

func TestCopyMultiChunkContent(t *testing.T) {
	size := uint64(384)
	chunks := mustPlan(t, size, 64)
	src := &testReader{data: patternBytes(size)}
	dst := newWriter(size)
	var mu sync.Mutex
	callbacks := map[int]int{}
	err := Copy(context.Background(), src, dst, size, chunks, CopyOptions{
		Workers: 3,
		OnComplete: func(c Chunk) error {
			mu.Lock()
			callbacks[c.Index]++
			mu.Unlock()
			return nil
		},
	})
	if err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if !bytes.Equal(dst.snapshot(), src.data) {
		t.Fatal("destination content differs from source")
	}
	if len(callbacks) != len(chunks) {
		t.Fatalf("callbacks for %d chunks, want %d", len(callbacks), len(chunks))
	}
	for i, n := range callbacks {
		if n != 1 {
			t.Errorf("chunk %d callback count = %d, want exactly 1", i, n)
		}
	}
	checkWritesWithinChunks(t, chunks, dst.ops())
}

func TestCopySkipsCompletedChunks(t *testing.T) {
	size := uint64(256)
	chunks := mustPlan(t, size, 64)
	src := &testReader{data: patternBytes(size)}
	dst := newWriter(size)
	// Pre-fill the completed chunks' destination ranges with markers.
	for i := uint64(64); i < 192; i++ {
		dst.data[i] = 0xAA
	}
	completed := map[int]struct{}{1: {}, 2: {}}
	// Fail loudly if a completed chunk's range is read or written.
	src.readFn = func(off uint64, p []byte) (int, error) {
		for _, idx := range []int{1, 2} {
			c := chunks[idx]
			if off >= c.Offset && off < c.End() {
				return 0, fmt.Errorf("read of completed chunk %d range", idx)
			}
		}
		n := copy(p, src.data[off:])
		if n < len(p) {
			return n, io.EOF
		}
		return n, nil
	}
	dst.writeFn = func(off uint64, p []byte) (int, error) {
		for _, idx := range []int{1, 2} {
			c := chunks[idx]
			if off >= c.Offset && off < c.End() {
				return 0, fmt.Errorf("write into completed chunk %d range", idx)
			}
		}
		return len(p), nil
	}
	var mu sync.Mutex
	var callbacks []int
	err := Copy(context.Background(), src, dst, size, chunks, CopyOptions{
		Workers:   2,
		Completed: completed,
		OnComplete: func(c Chunk) error {
			mu.Lock()
			callbacks = append(callbacks, c.Index)
			mu.Unlock()
			return nil
		},
	})
	if err != nil {
		t.Fatalf("Copy: %v", err)
	}
	// Only chunks 0 and 3 were transferred and reported.
	if len(callbacks) != 2 {
		t.Fatalf("callbacks = %v, want exactly chunks 0 and 3", callbacks)
	}
	for _, idx := range callbacks {
		if idx == 1 || idx == 2 {
			t.Errorf("completed chunk %d got a callback", idx)
		}
	}
	for i := uint64(64); i < 192; i++ {
		if dst.data[i] != 0xAA {
			t.Fatalf("completed range byte %d was overwritten", i)
		}
	}
	if !bytes.Equal(dst.data[:64], src.data[:64]) || !bytes.Equal(dst.data[192:], src.data[192:]) {
		t.Fatal("pending chunk content mismatch")
	}
	checkWritesWithinChunks(t, chunks, dst.ops())
}

// TestCopyWorkerConcurrencyCap uses gates, not sleeps: exactly Workers
// chunks may be in flight; a fourth start before release is a failure.
func TestCopyWorkerConcurrencyCap(t *testing.T) {
	size := uint64(384)
	chunks := mustPlan(t, size, 64) // 6 chunks
	src := &testReader{data: patternBytes(size)}
	dst := newWriter(size)
	started := make(chan int, 16)
	release := make(chan struct{})
	dst.writeFn = func(off uint64, p []byte) (int, error) {
		c, _ := chunkCovering(chunks, off)
		started <- c.Index
		<-release
		return len(p), nil
	}
	done := make(chan error, 1)
	go func() {
		done <- Copy(context.Background(), src, dst, size, chunks, CopyOptions{Workers: 3})
	}()
	for i := 0; i < 3; i++ {
		<-started
	}
	select {
	case idx := <-started:
		t.Fatalf("chunk %d started while 3 workers were already busy (cap 3)", idx)
	default:
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if !bytes.Equal(dst.snapshot(), src.data) {
		t.Fatal("content mismatch after gated concurrency")
	}
}

// TestCopyCallbackAfterFullWrite proves OnComplete fires only after the
// chunk's full range is written: the callback reads the writer's per-chunk
// byte accounting and requires the whole length.
func TestCopyCallbackAfterFullWrite(t *testing.T) {
	size := uint64(256)
	chunks := mustPlan(t, size, 64)
	src := &testReader{data: patternBytes(size)}
	dst := newWriter(size)
	var mu sync.Mutex
	writtenPerChunk := map[int]uint64{}
	dst.writeFn = func(off uint64, p []byte) (int, error) {
		c, _ := chunkCovering(chunks, off)
		mu.Lock()
		writtenPerChunk[c.Index] += uint64(len(p))
		mu.Unlock()
		return len(p), nil
	}
	err := Copy(context.Background(), src, dst, size, chunks, CopyOptions{
		Workers: 2,
		OnComplete: func(c Chunk) error {
			mu.Lock()
			got := writtenPerChunk[c.Index]
			mu.Unlock()
			if got != c.Length {
				return fmt.Errorf("chunk %d reported complete with %d of %d bytes written", c.Index, got, c.Length)
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if !bytes.Equal(dst.snapshot(), src.data) {
		t.Fatal("content mismatch")
	}
}

// TestCopyCallbackFailureStopsCopy: a completed chunk's callback fails;
// no new chunk is claimed after the failure, in-flight workers exit after
// their current window, and nothing happens after Copy returns.
//
// Deterministic synchronization without sleeps: chunk 1's write waits on a
// gate that the failing callback itself closes, so the sibling chunk is
// guaranteed in-flight when the failure lands; the dispatcher has no free
// worker to receive another chunk before cancellation takes effect.
func TestCopyCallbackFailureStopsCopy(t *testing.T) {
	size := uint64(256)
	chunks := mustPlan(t, size, 64) // 4 chunks
	src := &testReader{data: patternBytes(size)}
	dst := newWriter(size)

	gate1 := make(chan struct{})
	var cbMu sync.Mutex
	callbacks := map[int]int{}
	var startMu sync.Mutex
	started := map[int]bool{}
	dst.writeFn = func(off uint64, p []byte) (int, error) {
		c, _ := chunkCovering(chunks, off)
		startMu.Lock()
		started[c.Index] = true
		startMu.Unlock()
		if c.Index == 1 {
			<-gate1
		}
		return len(p), nil
	}
	// One worker copies chunk 0 (its callback fails); the other is inside
	// chunk 1's write, released by the failing callback.
	err := Copy(context.Background(), src, dst, size, chunks, CopyOptions{
		Workers: 2,
		OnComplete: func(c Chunk) error {
			cbMu.Lock()
			callbacks[c.Index]++
			cbMu.Unlock()
			if c.Index == 0 {
				close(gate1) // let the in-flight sibling finish its window
				return errors.New("injected callback failure")
			}
			return nil
		},
	})
	if err == nil {
		t.Fatal("Copy = nil, want the callback failure")
	}
	if !strings.Contains(err.Error(), "chunk 0") || !strings.Contains(err.Error(), "completion callback") {
		t.Errorf("error = %v, want chunk 0 callback failure with range", err)
	}
	startMu.Lock()
	for idx := range started {
		if idx != 0 && idx != 1 {
			t.Errorf("chunk %d started after the callback failure", idx)
		}
	}
	startMu.Unlock()
	cbMu.Lock()
	defer cbMu.Unlock()
	if callbacks[0] != 1 {
		t.Errorf("chunk 0 callbacks = %d, want exactly 1 (the failing call)", callbacks[0])
	}
	for _, idx := range []int{1, 2, 3} {
		if callbacks[idx] != 0 {
			t.Errorf("chunk %d got %d callbacks after cancellation", idx, callbacks[idx])
		}
	}
}
func TestCopyReadError(t *testing.T) {
	size := uint64(192)
	chunks := mustPlan(t, size, 64)
	src := &testReader{data: patternBytes(size)}
	boom := errors.New("boom read")
	src.readFn = func(off uint64, p []byte) (int, error) {
		c, _ := chunkCovering(chunks, off)
		if c.Index == 1 {
			return 0, boom
		}
		n := copy(p, src.data[off:])
		if n < len(p) {
			return n, io.EOF
		}
		return n, nil
	}
	dst := newWriter(size)
	err := Copy(context.Background(), src, dst, size, chunks, CopyOptions{Workers: 2})
	if !errors.Is(err, boom) {
		t.Fatalf("Copy = %v, want it to wrap the read error", err)
	}
	if !strings.Contains(err.Error(), "chunk 1") {
		t.Errorf("error = %v, want chunk index", err)
	}
}

func TestCopyWriteError(t *testing.T) {
	size := uint64(192)
	chunks := mustPlan(t, size, 64)
	src := &testReader{data: patternBytes(size)}
	dst := newWriter(size)
	boom := errors.New("boom write")
	dst.writeFn = func(off uint64, p []byte) (int, error) {
		c, _ := chunkCovering(chunks, off)
		if c.Index == 2 {
			return 0, boom
		}
		return len(p), nil
	}
	err := Copy(context.Background(), src, dst, size, chunks, CopyOptions{Workers: 2})
	if !errors.Is(err, boom) {
		t.Fatalf("Copy = %v, want it to wrap the write error", err)
	}
	if !strings.Contains(err.Error(), "chunk 2") {
		t.Errorf("error = %v, want chunk index", err)
	}
}

// TestCopyShortReadsRetried: a reader that serves at most 8 bytes per call
// must not break the copy.
func TestCopyShortReadsRetried(t *testing.T) {
	size := uint64(192)
	chunks := mustPlan(t, size, 64)
	src := &testReader{data: patternBytes(size)}
	src.readFn = func(off uint64, p []byte) (int, error) {
		max := 8
		if len(p) < max {
			max = len(p)
		}
		n := copy(p[:max], src.data[off:])
		if n < len(p) {
			return n, nil // short read without error: must be retried
		}
		return n, nil
	}
	dst := newWriter(size)
	if err := Copy(context.Background(), src, dst, size, chunks, CopyOptions{Workers: 2}); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if !bytes.Equal(dst.snapshot(), src.data) {
		t.Fatal("content mismatch after short reads")
	}
}

// TestCopyReadEOFWithFinalBytes covers the legal "n > 0 with io.EOF" shape
// at the end of the readable range.
func TestCopyReadEOFWithFinalBytes(t *testing.T) {
	size := uint64(128)
	chunks := mustPlan(t, size, 64)
	src := &testReader{data: patternBytes(size)}
	src.readFn = func(off uint64, p []byte) (int, error) {
		if off >= size {
			return 0, io.EOF
		}
		n := copy(p, src.data[off:])
		if uint64(off)+uint64(n) >= size {
			return n, io.EOF // bytes delivered AND EOF reported
		}
		return n, nil
	}
	dst := newWriter(size)
	if err := Copy(context.Background(), src, dst, size, chunks, CopyOptions{Workers: 2}); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if !bytes.Equal(dst.snapshot(), src.data) {
		t.Fatal("content mismatch with n>0+io.EOF reads")
	}
}

// TestCopyShortReadEOF: the source ends before the plan's range — a hard
// error, never a silent short copy.
func TestCopyShortReadEOF(t *testing.T) {
	size := uint64(128)
	chunks := mustPlan(t, size, 64)
	src := &testReader{data: patternBytes(32)} // shorter than the plan
	dst := newWriter(size)
	err := Copy(context.Background(), src, dst, size, chunks, CopyOptions{Workers: 1})
	if !errors.Is(err, ErrShortRead) {
		t.Fatalf("Copy = %v, want ErrShortRead", err)
	}
}

func TestCopyZeroProgressRead(t *testing.T) {
	size := uint64(64)
	chunks := mustPlan(t, size, 64)
	src := &testReader{data: patternBytes(size)}
	src.readFn = func(off uint64, p []byte) (int, error) {
		return 0, nil // zero progress without an error
	}
	dst := newWriter(size)
	err := Copy(context.Background(), src, dst, size, chunks, CopyOptions{Workers: 1})
	if !errors.Is(err, ErrZeroProgress) {
		t.Fatalf("Copy = %v, want ErrZeroProgress", err)
	}
}

func TestCopyShortWritesRetried(t *testing.T) {
	size := uint64(192)
	chunks := mustPlan(t, size, 64)
	src := &testReader{data: patternBytes(size)}
	dst := newWriter(size)
	dst.writeFn = func(off uint64, p []byte) (int, error) {
		if len(p) > 8 {
			return 8, nil // short write without error: must be retried
		}
		return len(p), nil
	}
	if err := Copy(context.Background(), src, dst, size, chunks, CopyOptions{Workers: 2}); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if !bytes.Equal(dst.snapshot(), src.data) {
		t.Fatal("content mismatch after short writes")
	}
	checkWritesWithinChunks(t, chunks, dst.ops())
}

func TestCopyZeroProgressWrite(t *testing.T) {
	size := uint64(64)
	chunks := mustPlan(t, size, 64)
	src := &testReader{data: patternBytes(size)}
	dst := newWriter(size)
	dst.writeFn = func(off uint64, p []byte) (int, error) {
		return 0, nil
	}
	err := Copy(context.Background(), src, dst, size, chunks, CopyOptions{Workers: 1})
	if !errors.Is(err, ErrZeroProgress) {
		t.Fatalf("Copy = %v, want ErrZeroProgress", err)
	}
}

func TestCopyPreCancelledDoesNothing(t *testing.T) {
	size := uint64(192)
	chunks := mustPlan(t, size, 64)
	src := &testReader{data: patternBytes(size)}
	src.readFn = func(off uint64, p []byte) (int, error) {
		return 0, errors.New("read must not be called")
	}
	dst := newWriter(size)
	dst.writeFn = func(off uint64, p []byte) (int, error) {
		return 0, errors.New("write must not be called")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := Copy(ctx, src, dst, size, chunks, CopyOptions{
		Workers: 2,
		OnComplete: func(Chunk) error {
			return errors.New("callback must not be called")
		},
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Copy = %v, want wrapped context.Canceled", err)
	}
	if src.readCount() != 0 {
		t.Errorf("reads = %d, want 0", src.readCount())
	}
	if dst.writeCount() != 0 {
		t.Errorf("writes = %d, want 0", dst.writeCount())
	}
}

// TestCopyMidCancelStopsDispatch: two workers are blocked inside in-flight
// chunk writes; cancellation must stop dispatch of the remaining chunks and
// return a wrapped context.Canceled; after Copy returns, nothing more may
// happen (structural: Copy only returns after all workers exited).
func TestCopyMidCancelStopsDispatch(t *testing.T) {
	size := uint64(256)
	chunks := mustPlan(t, size, 64) // 4 chunks
	src := &testReader{data: patternBytes(size)}
	dst := newWriter(size)
	block := make(chan struct{})
	startedCh := make(chan struct{}, 4)
	var mu sync.Mutex
	started := 0
	dst.writeFn = func(off uint64, p []byte) (int, error) {
		mu.Lock()
		started++
		mu.Unlock()
		startedCh <- struct{}{}
		<-block
		return len(p), nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Copy(ctx, src, dst, size, chunks, CopyOptions{Workers: 2}) }()
	// Both workers are inside their first chunk's write — deterministic,
	// channel-driven, no spin.
	<-startedCh
	<-startedCh
	cancel()
	close(block)
	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Copy = %v, want wrapped context.Canceled", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if started != 2 {
		t.Errorf("chunks started = %d, want exactly 2 (no dispatch after cancel)", started)
	}
}

func TestCopyWorkersOneAndExcess(t *testing.T) {
	size := uint64(192)
	chunks := mustPlan(t, size, 64)
	src := &testReader{data: patternBytes(size)}
	dst := newWriter(size)
	if err := Copy(context.Background(), src, dst, size, chunks, CopyOptions{Workers: 1}); err != nil {
		t.Fatalf("workers=1: %v", err)
	}
	if !bytes.Equal(dst.snapshot(), src.data) {
		t.Fatal("workers=1: content mismatch")
	}
	dst2 := newWriter(size)
	if err := Copy(context.Background(), src, dst2, size, chunks, CopyOptions{Workers: 10}); err != nil {
		t.Fatalf("workers=10 over 3 chunks: %v", err)
	}
	if !bytes.Equal(dst2.snapshot(), src.data) {
		t.Fatal("workers>chunks: content mismatch")
	}
}

func TestCopyZeroSizeNoIO(t *testing.T) {
	src := &testReader{readFn: func(off uint64, p []byte) (int, error) {
		return 0, errors.New("read must not be called")
	}}
	dst := newWriter(0)
	dst.writeFn = func(off uint64, p []byte) (int, error) {
		return 0, errors.New("write must not be called")
	}
	chunks := mustPlan(t, 0, 64)
	if len(chunks) != 0 {
		t.Fatalf("zero-size plan = %d chunks, want 0", len(chunks))
	}
	if err := Copy(context.Background(), src, dst, 0, chunks, CopyOptions{Workers: 2}); err != nil {
		t.Fatalf("Copy of zero-size file: %v", err)
	}
}

func TestCopyInvalidPlanFailsBeforeIO(t *testing.T) {
	good := mustPlan(t, 192, 64)
	cases := []struct {
		name   string
		chunks []Chunk
		size   uint64
	}{
		{"duplicate index", []Chunk{{Index: 0, Offset: 0, Length: 64}, {Index: 0, Offset: 64, Length: 64}}, 128},
		{"gap", []Chunk{{Index: 0, Offset: 0, Length: 64}, {Index: 1, Offset: 128, Length: 64}}, 192},
		{"wrong total", good, 999},
		{"zero length", []Chunk{{Index: 0, Offset: 0, Length: 0}}, 0},
		{"overflow end", []Chunk{{Index: 0, Offset: math.MaxUint64 - 8, Length: 16}}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := &testReader{readFn: func(off uint64, p []byte) (int, error) {
				return 0, errors.New("read must not be called")
			}}
			dst := newWriter(tc.size)
			dst.writeFn = func(off uint64, p []byte) (int, error) {
				return 0, errors.New("write must not be called")
			}
			err := Copy(context.Background(), src, dst, tc.size, tc.chunks, CopyOptions{Workers: 1})
			if !errors.Is(err, ErrInvalidPlan) {
				t.Fatalf("Copy = %v, want ErrInvalidPlan", err)
			}
			if src.readCount() != 0 || dst.writeCount() != 0 {
				t.Errorf("I/O happened for an invalid plan: reads=%d writes=%d", src.readCount(), dst.writeCount())
			}
		})
	}
}

func TestCopyConfigValidation(t *testing.T) {
	size := uint64(64)
	chunks := mustPlan(t, size, 64)
	src := &testReader{data: patternBytes(size)}
	dst := newWriter(size)

	if err := Copy(context.Background(), src, dst, size, chunks, CopyOptions{Workers: 0}); !errors.Is(err, ErrInvalidCopy) {
		t.Errorf("Workers=0 = %v, want ErrInvalidCopy", err)
	}
	if err := Copy(context.Background(), nil, dst, size, chunks, CopyOptions{Workers: 1}); !errors.Is(err, ErrInvalidCopy) {
		t.Errorf("nil reader = %v, want ErrInvalidCopy", err)
	}
	if err := Copy(context.Background(), src, nil, size, chunks, CopyOptions{Workers: 1}); !errors.Is(err, ErrInvalidCopy) {
		t.Errorf("nil writer = %v, want ErrInvalidCopy", err)
	}
	if err := Copy(context.Background(), src, dst, size, chunks, CopyOptions{
		Workers:   1,
		Completed: map[int]struct{}{7: {}},
	}); !errors.Is(err, ErrInvalidCopy) {
		t.Errorf("completed index out of range = %v, want ErrInvalidCopy", err)
	}
	if err := Copy(context.Background(), src, dst, math.MaxInt64+1, nil, CopyOptions{Workers: 1}); !errors.Is(err, ErrInvalidCopy) {
		t.Errorf("size beyond int64 = %v, want ErrInvalidCopy", err)
	}
	// Note: a structurally valid plan over an unaddressable size cannot be
	// tested, because such a plan cannot be materialized in memory (a
	// 64 MiB-chunk plan over 2^63 bytes has 2^37 entries).
}

// TestCopyLargeOffsetsRecordedOnly exercises huge file offsets without
// large allocations: the reader synthesizes pattern bytes and the writer
// only records offsets. A 1 TiB plan with 3 pending chunks must copy
// exactly those ranges.
func TestCopyLargeOffsetsRecordedOnly(t *testing.T) {
	size := uint64(1) << 40       // 1 TiB
	chunkSize := uint64(64) << 20 // 64 MiB
	chunks := mustPlan(t, size, chunkSize)
	if len(chunks) < 16384 {
		t.Fatalf("plan has %d chunks, want at least 16384", len(chunks))
	}
	completed := map[int]struct{}{}
	pendingIdx := []int{0, 8000, len(chunks) - 1}
	for i := range chunks {
		completed[i] = struct{}{}
	}
	for _, idx := range pendingIdx {
		delete(completed, idx)
	}

	var readBytes uint64
	var readMu sync.Mutex
	src := &testReader{readFn: func(off uint64, p []byte) (int, error) {
		for i := range p {
			p[i] = pattern(off + uint64(i))
		}
		readMu.Lock()
		readBytes += uint64(len(p))
		readMu.Unlock()
		return len(p), nil
	}}
	dst := &testWriter{writeFn: func(off uint64, p []byte) (int, error) {
		for i, b := range p {
			if b != pattern(off+uint64(i)) {
				return 0, fmt.Errorf("content mismatch at %d", off+uint64(i))
			}
		}
		return len(p), nil
	}}
	var mu sync.Mutex
	done := map[int]bool{}
	err := Copy(context.Background(), src, dst, size, chunks, CopyOptions{
		Workers:   4,
		Completed: completed,
		OnComplete: func(c Chunk) error {
			mu.Lock()
			done[c.Index] = true
			mu.Unlock()
			return nil
		},
	})
	if err != nil {
		t.Fatalf("Copy: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(done) != 3 {
		t.Fatalf("completed %d chunks, want 3", len(done))
	}
	for _, idx := range pendingIdx {
		if !done[idx] {
			t.Errorf("chunk %d not completed", idx)
		}
	}
	readMu.Lock()
	defer readMu.Unlock()
	if want := chunkSize*2 + chunks[len(chunks)-1].Length; readBytes != want {
		t.Errorf("read bytes = %d, want %d", readBytes, want)
	}
	checkWritesWithinChunks(t, chunks, dst.ops())
}
