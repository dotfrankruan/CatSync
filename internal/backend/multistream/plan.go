// Package multistream implements CatSync's native multi-stream large-file
// backend for uploads and downloads over system-SSH SFTP sessions.
//
// Its plan and copy engine stay transport-agnostic; the surrounding package
// supplies resumable checkpoints, SFTP reader/writer pools, receiving-side
// temporary objects, and no-clobber publication.
//
// A large file is split into a deterministic plan of fixed-size chunks
// (Plan). The same file size and chunk size always produce the same plan,
// which is what makes interruption and resume deterministic: a checkpoint
// only needs to record which chunk indexes are complete.
package multistream

import (
	"errors"
	"fmt"
	"math"
)

// Chunk is one fixed-size piece of a large file. Chunks produced by Plan
// are ordered, contiguous, and non-overlapping; only the final chunk of a
// plan may be shorter than the plan's chunk size.
type Chunk struct {
	// Index is the chunk's stable position in the plan, starting at 0. It
	// never changes for a given file size and chunk size, so a checkpoint
	// can record progress as a set of indexes.
	Index int
	// Offset is the chunk's absolute byte offset in the file.
	Offset uint64
	// Length is the chunk's length in bytes; it is always positive.
	Length uint64
}

// End returns the exclusive end offset of the chunk. It is only meaningful
// for chunks from a validated plan, where Offset+Length cannot overflow.
func (c Chunk) End() uint64 { return c.Offset + c.Length }

// Sentinel errors reported by this package; test them with errors.Is.
var (
	// ErrZeroChunkSize rejects a plan requested with a zero chunk size.
	ErrZeroChunkSize = errors.New("multistream: chunk size must be positive")
	// ErrPlanOverflow rejects plans whose chunk count exceeds what an int
	// can index, because the plan could never be materialized in memory.
	ErrPlanOverflow = errors.New("multistream: chunk count overflows int")
	// ErrInvalidPlan rejects malformed chunk plans: duplicate or
	// out-of-order indexes, gaps, overlaps, zero-length chunks, ranges
	// that overflow or exceed the file size, or a wrong total size.
	ErrInvalidPlan = errors.New("multistream: invalid chunk plan")
)

// Plan computes the deterministic chunk layout for a file of size bytes
// using fixed-size chunks of chunkSize bytes. The chunks are ordered,
// contiguous, and non-overlapping, and cover [0, size) exactly once; the
// final chunk may be shorter. A zero-byte file yields zero chunks. A zero
// chunk size is rejected with ErrZeroChunkSize, and a chunk count that
// cannot be represented as an int is rejected with ErrPlanOverflow rather
// than wrapping.
func Plan(size, chunkSize uint64) ([]Chunk, error) {
	if chunkSize == 0 {
		return nil, ErrZeroChunkSize
	}
	n := chunkCount(size, chunkSize)
	if n > uint64(math.MaxInt) {
		return nil, fmt.Errorf("%w: %d chunks of %d bytes for a %d-byte file", ErrPlanOverflow, n, chunkSize, size)
	}
	if n == 0 {
		return nil, nil
	}
	chunks := make([]Chunk, 0, int(n))
	var off uint64
	for i := 0; i < int(n); i++ {
		l := chunkSize
		if rem := size - off; rem < l {
			l = rem
		}
		chunks = append(chunks, Chunk{Index: i, Offset: off, Length: l})
		off += l
	}
	return chunks, nil
}

// chunkCount returns ceil(size/chunkSize) without overflow. chunkSize must
// be positive; callers guarantee it.
func chunkCount(size, chunkSize uint64) uint64 {
	n := size / chunkSize
	if size%chunkSize != 0 {
		n++
	}
	return n
}

// ValidatePlan strictly checks that chunks form the exact deterministic
// plan for a size-byte file: indexes are 0..n-1 in order (no duplicates,
// no out-of-order entries), ranges are contiguous and non-overlapping (no
// gaps), no chunk is zero-length, no range overflows, and the plan covers
// [0, size) exactly once. Transfer and checkpoint code uses it to refuse
// corrupt or ambiguous plans instead of trusting them.
func ValidatePlan(chunks []Chunk, size uint64) error {
	total, err := validateLayout(chunks)
	if err != nil {
		return err
	}
	if total != size {
		return fmt.Errorf("%w: plan covers %d bytes, file is %d bytes", ErrInvalidPlan, total, size)
	}
	return nil
}

// validateLayout checks the structural invariants of a plan — ordered
// indexes, contiguity, positive lengths, no overflow — and returns the
// total number of bytes it covers.
func validateLayout(chunks []Chunk) (uint64, error) {
	var off uint64
	for i, c := range chunks {
		if c.Index != i {
			return 0, fmt.Errorf("%w: chunk at position %d has index %d (duplicate or out of order)", ErrInvalidPlan, i, c.Index)
		}
		if c.Length == 0 {
			return 0, fmt.Errorf("%w: chunk %d has zero length", ErrInvalidPlan, i)
		}
		if c.Offset != off {
			return 0, fmt.Errorf("%w: chunk %d starts at offset %d, want %d (gap or overlap)", ErrInvalidPlan, i, c.Offset, off)
		}
		// c.Offset == off is covered so far; rejecting a length that would
		// push the end past MaxUint64 keeps End() overflow-free.
		if c.Length > math.MaxUint64-off {
			return 0, fmt.Errorf("%w: chunk %d at offset %d with length %d overflows the byte range", ErrInvalidPlan, i, c.Offset, c.Length)
		}
		off += c.Length
	}
	return off, nil
}
