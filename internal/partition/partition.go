// Package partition packs small-file items into deterministic, disjoint
// batches for CatSync's native parallel-rsync backend.
//
// Packing is pure: it never touches the filesystem, starts processes, or
// interprets paths in any way, so item names may contain spaces, newlines,
// quotes, Unicode, and leading dashes without affecting the result. Items
// keep their input order, every item appears in exactly one batch, and the
// same input always produces the same batch layout, which keeps resume,
// logging, and tests deterministic.
//
// The algorithm is stable next-fit: an item joins the current batch while
// the batch's file-count and logical-byte limits both allow it, otherwise
// it starts a new batch. This is deliberately simple; it is not an optimal
// bin-packing heuristic and does not need to be.
package partition

import (
	"errors"
	"fmt"
)

// Item is one file to batch: a slash- or OS-agnostic path relative to the
// transfer root and the size recorded at plan time.
type Item struct {
	// Path is the file path as recorded by discovery and the planner. It is
	// treated as opaque text.
	Path string
	// Size is the file size in bytes; it must not be negative.
	Size int64
}

// Batch is one disjoint group of items that one rsync invocation can
// transfer. Bytes is the sum of the items' sizes, using the planner's
// recorded sizes (the logical batch volume).
type Batch struct {
	// Items are the batch members, in input order.
	Items []Item
	// Bytes is the sum of Items' sizes as uint64.
	Bytes uint64
}

// Config bounds a batch. Both limits must be at least 1.
type Config struct {
	// MaxFiles is the maximum number of items in one batch.
	MaxFiles int
	// MaxBytes is the maximum logical byte volume of one batch. An
	// individual item larger than this is an error: the limit is never
	// silently exceeded.
	MaxBytes uint64
}

// ErrInvalidConfig reports unusable batch limits.
var ErrInvalidConfig = errors.New("partition: invalid configuration")

// Pack splits items into batches honoring cfg. It returns an error for
// invalid configuration, an empty path, a negative size, or an item larger
// than MaxBytes; on error no partial result is returned.
func Pack(items []Item, cfg Config) ([]Batch, error) {
	if cfg.MaxFiles < 1 {
		return nil, fmt.Errorf("%w: MaxFiles must be at least 1, got %d", ErrInvalidConfig, cfg.MaxFiles)
	}
	if cfg.MaxBytes < 1 {
		return nil, fmt.Errorf("%w: MaxBytes must be at least 1, got %d", ErrInvalidConfig, cfg.MaxBytes)
	}
	if len(items) == 0 {
		return nil, nil
	}
	var batches []Batch
	var cur Batch
	for _, it := range items {
		if it.Path == "" {
			return nil, errors.New("partition: item path is empty")
		}
		if it.Size < 0 {
			return nil, fmt.Errorf("partition: item %q has negative size %d", it.Path, it.Size)
		}
		size := uint64(it.Size)
		if size > cfg.MaxBytes {
			return nil, fmt.Errorf("partition: item %q is %d bytes, larger than the %d-byte batch limit", it.Path, it.Size, cfg.MaxBytes)
		}
		// Overflow-safe fit check: cur.Bytes never exceeds MaxBytes, so
		// MaxBytes-cur.Bytes cannot underflow, and size <= MaxBytes.
		if len(cur.Items) > 0 && (len(cur.Items) >= cfg.MaxFiles || size > cfg.MaxBytes-cur.Bytes) {
			batches = append(batches, cur)
			cur = Batch{}
		}
		cur.Items = append(cur.Items, it)
		cur.Bytes += size
	}
	if len(cur.Items) > 0 {
		batches = append(batches, cur)
	}
	return batches, nil
}

// Paths returns the batch's item paths in order.
func (b Batch) Paths() []string {
	out := make([]string, len(b.Items))
	for i, it := range b.Items {
		out[i] = it.Path
	}
	return out
}
