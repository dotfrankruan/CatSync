package multistream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
)

// copyBufSize bounds per-worker scratch memory. Total scratch is
// O(Workers × copyBufSize), independent of file and chunk sizes.
const copyBufSize = 256 << 10

// Sentinel errors reported by the copy engine; test them with errors.Is.
var (
	// ErrInvalidCopy rejects unusable copy configuration: nil endpoints, a
	// non-positive worker count, a completed index outside the plan, or a
	// file size the io.ReaderAt/io.WriterAt int64 offsets cannot address.
	ErrInvalidCopy = errors.New("multistream: invalid copy configuration")
	// ErrShortRead reports a source that ended before the planned chunk
	// range was read (the file shrank or the plan is wrong).
	ErrShortRead = errors.New("multistream: short read")
	// ErrZeroProgress reports a ReaderAt/WriterAt that returned 0 bytes
	// without an error, which would otherwise spin forever.
	ErrZeroProgress = errors.New("multistream: zero-progress I/O")
)

// CopyOptions configures one Copy call.
type CopyOptions struct {
	// Workers is the number of concurrent chunk copiers; at least 1.
	// Active chunk copies never exceed Workers.
	Workers int
	// Completed lists chunk indexes already transferred durably (from a
	// checkpoint). Those chunks are never read, written, or reported.
	Completed map[int]struct{}
	// OnComplete is called exactly once per newly completed chunk, after
	// its full range has been written. A callback error stops the copy;
	// because the error means the completion was not recorded, a later
	// resume may safely rewrite the chunk. Nil is allowed.
	OnComplete func(Chunk) error
}

// Copy copies every chunk of the plan that is not in opts.Completed from
// src to dst, using a fixed pool of opts.Workers goroutines. It is the
// transport-agnostic range-copy core of the native large-file backend.
//
// Guarantees:
//
//   - The plan is validated before any read or write happens.
//   - Each pending chunk is copied exactly once, every destination write
//     stays strictly inside that chunk's [Offset, Offset+Length) range, and
//     OnComplete fires only after the chunk's full range is written.
//   - On the first read, write, validation, or callback error, no new
//     chunks are claimed, in-flight workers observe cancellation at their
//     next window boundary, and Copy returns only after every worker has
//     exited — no read, write, or callback can happen after return.
//   - If the caller's context is cancelled — before or during the copy —
//     the returned error wraps ctx.Err(); a pre-cancelled copy performs no
//     I/O and calls no callbacks at all.
//
// What Copy deliberately does NOT do (the future transport's job): create,
// truncate, fsync, or rename the destination; manage remote metadata;
// persist checkpoints. It reports completed chunks via OnComplete and
// leaves durability to the caller.
func Copy(ctx context.Context, src io.ReaderAt, dst io.WriterAt, size uint64, chunks []Chunk, opts CopyOptions) error {
	if src == nil {
		return fmt.Errorf("%w: source reader is nil", ErrInvalidCopy)
	}
	if dst == nil {
		return fmt.Errorf("%w: destination writer is nil", ErrInvalidCopy)
	}
	if opts.Workers < 1 {
		return fmt.Errorf("%w: workers must be at least 1, got %d", ErrInvalidCopy, opts.Workers)
	}
	for idx := range opts.Completed {
		if idx < 0 || idx >= len(chunks) {
			return fmt.Errorf("%w: completed chunk index %d is outside the plan of %d chunks", ErrInvalidCopy, idx, len(chunks))
		}
	}
	// io.ReaderAt/io.WriterAt take int64 offsets; the plan covers
	// [0, size), so a size beyond MaxInt64 could never be addressed
	// correctly. Reject instead of overflowing. This check runs before
	// plan validation: materializing a plan for an unaddressable size is
	// impossible anyway (a correct plan would not fit in memory).
	if size > math.MaxInt64 {
		return fmt.Errorf("%w: file size %d exceeds the int64 address range of io.ReaderAt", ErrInvalidCopy, size)
	}
	// Plan validation is pure computation and happens before any I/O.
	if err := ValidatePlan(chunks, size); err != nil {
		return err
	}
	// A pre-cancelled context performs no reads, writes, or callbacks.
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("multistream: copy interrupted before starting: %w", err)
	}

	var pending []Chunk
	for _, c := range chunks {
		if _, ok := opts.Completed[c.Index]; ok {
			continue
		}
		pending = append(pending, c)
	}
	if len(pending) == 0 {
		return nil
	}

	workers := opts.Workers
	if workers > len(pending) {
		workers = len(pending)
	}

	// workCtx cancels on the first real failure; ctx stays the caller's.
	workCtx, cancelWork := context.WithCancel(ctx)
	defer cancelWork()

	type job struct {
		pendingIdx int
		chunk      Chunk
	}
	jobs := make(chan job)
	// failErr remembers the first error that triggered cancellation — the
	// cause, never a teardown artifact.
	var failOnce sync.Once
	var failErr error
	var wg sync.WaitGroup
	// cbMu serializes OnComplete calls so the callback never runs
	// concurrently with itself.
	var cbMu sync.Mutex
	// recordFailure remembers the first real failure (never a
	// context-teardown one) as the cause, and cancels the work context.
	recordFailure := func(err error) {
		failOnce.Do(func() { failErr = err })
		cancelWork()
	}

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, copyBufSize)
			for {
				// Check before claiming work: never claim on a dead context.
				if workCtx.Err() != nil {
					return
				}
				select {
				case j, ok := <-jobs:
					if !ok {
						return
					}
					// The claim may have won a race against cancellation;
					// check again so a claimed chunk is not copied or
					// reported after cancellation.
					if workCtx.Err() != nil {
						return
					}
					if err := copyChunkRange(workCtx, src, dst, j.chunk, buf); err != nil {
						// A worker torn down by a sibling's failure or by
						// caller cancellation reports no error; the real
						// cause is recorded by its originator or by the
						// caller's context below.
						if !(workCtx.Err() != nil && errors.Is(err, context.Canceled)) {
							recordFailure(err)
						}
						return
					}
					// The whole chunk is written. A cancelled copy must not
					// report completion: the checkpoint would claim bytes
					// whose write path was interrupted.
					if workCtx.Err() != nil {
						return
					}
					if opts.OnComplete != nil {
						cbMu.Lock()
						cbErr := opts.OnComplete(j.chunk)
						cbMu.Unlock()
						if cbErr != nil {
							recordFailure(fmt.Errorf("multistream: chunk %d (%d+%d): completion callback: %w",
								j.chunk.Index, j.chunk.Offset, j.chunk.Length, cbErr))
							return
						}
					}
				case <-workCtx.Done():
					return
				}
			}
		}()
	}

	// Dispatcher: hand out pending chunks in plan order, stopping on
	// cancellation. It never blocks workers' exit because it closes the
	// channel when done or cancelled.
	go func() {
		defer close(jobs)
		for i := range pending {
			if workCtx.Err() != nil {
				return
			}
			select {
			case jobs <- job{pendingIdx: i, chunk: pending[i]}:
			case <-workCtx.Done():
				return
			}
		}
	}()

	wg.Wait()

	// Caller cancellation is the outcome when the caller cancelled: the
	// error wraps the caller's context cause (Canceled or DeadlineExceeded).
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("multistream: copy interrupted: %w", err)
	}
	// Otherwise report the error that triggered cancellation — the first
	// real failure, which is never a teardown artifact of a sibling.
	if failErr != nil {
		return failErr
	}
	return nil
}

// copyChunkRange copies exactly c.Length bytes from src at c.Offset to dst
// at c.Offset, using buf as scratch. Every destination write stays inside
// [c.Offset, c.Offset+c.Length). Short reads and short writes are retried;
// zero progress without an error and premature EOF are failures.
func copyChunkRange(ctx context.Context, src io.ReaderAt, dst io.WriterAt, c Chunk, buf []byte) error {
	off := c.Offset
	remaining := c.Length
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		n := uint64(len(buf))
		if remaining < n {
			n = remaining
		}
		window := buf[:n]

		read := 0
		for read < len(window) {
			rn, rerr := src.ReadAt(window[read:], int64(off)+int64(read))
			if rn < 0 {
				return fmt.Errorf("multistream: chunk %d: negative read count %d at %d", c.Index, rn, off+uint64(read))
			}
			read += rn
			if rerr != nil {
				if errors.Is(rerr, io.EOF) {
					if read == len(window) {
						// n > 0 bytes with io.EOF: legal, window complete.
						break
					}
					return fmt.Errorf("%w: chunk %d: source ended at %d of %d planned bytes (offset %d)",
						ErrShortRead, c.Index, read, len(window), off)
				}
				return fmt.Errorf("multistream: chunk %d: read at %d: %w", c.Index, off+uint64(read), rerr)
			}
			if rn == 0 {
				return fmt.Errorf("%w: chunk %d: read at offset %d", ErrZeroProgress, c.Index, off+uint64(read))
			}
		}

		written := 0
		for written < len(window) {
			wn, werr := dst.WriteAt(window[written:], int64(off)+int64(written))
			if wn < 0 {
				return fmt.Errorf("multistream: chunk %d: negative write count %d at %d", c.Index, wn, off+uint64(written))
			}
			written += wn
			if werr != nil {
				return fmt.Errorf("multistream: chunk %d: write at %d: %w", c.Index, off+uint64(written), werr)
			}
			if wn == 0 {
				return fmt.Errorf("%w: chunk %d: write at offset %d", ErrZeroProgress, c.Index, off+uint64(written))
			}
		}

		off += n
		remaining -= n
	}
	return nil
}
