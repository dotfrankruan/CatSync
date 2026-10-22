package multistream

import (
	"errors"
	"math"
	"reflect"
	"testing"
)

func TestPlanZeroSize(t *testing.T) {
	chunks, err := Plan(0, 1<<20)
	if err != nil {
		t.Fatalf("Plan(0, 1MiB): %v", err)
	}
	if len(chunks) != 0 {
		t.Fatalf("Plan(0, 1MiB) = %d chunks, want 0", len(chunks))
	}
	if err := ValidatePlan(chunks, 0); err != nil {
		t.Fatalf("ValidatePlan(empty, 0): %v", err)
	}
}

func TestPlanZeroChunkSize(t *testing.T) {
	for _, size := range []uint64{0, 1, 1 << 20, math.MaxUint64} {
		if _, err := Plan(size, 0); !errors.Is(err, ErrZeroChunkSize) {
			t.Fatalf("Plan(%d, 0) = %v, want ErrZeroChunkSize", size, err)
		}
	}
}

func TestPlanTable(t *testing.T) {
	tests := []struct {
		name        string
		size, chunk uint64
		wantCount   int
		wantLastLen uint64
	}{
		{"size below chunk", 100, 1 << 20, 1, 100},
		{"single exact chunk", 1 << 20, 1 << 20, 1, 1 << 20},
		{"exact boundary", 4 << 20, 1 << 20, 4, 1 << 20},
		{"boundary plus one", (4 << 20) + 1, 1 << 20, 5, 1},
		{"boundary minus one", (4 << 20) - 1, 1 << 20, 4, (1 << 20) - 1},
		{"chunk size one", 7, 1, 7, 1},
		{"uneven split", 10, 3, 4, 1},
		{"chunk size near max", 10, math.MaxUint64 - 3, 1, 10},
		{"max size huge chunk", math.MaxUint64, 1 << 62, 4, (1 << 62) - 1},
		{"max size exact divisor", math.MaxUint64, math.MaxUint64, 1, math.MaxUint64},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunks, err := Plan(tt.size, tt.chunk)
			if err != nil {
				t.Fatalf("Plan(%d, %d): %v", tt.size, tt.chunk, err)
			}
			if len(chunks) != tt.wantCount {
				t.Fatalf("Plan(%d, %d) = %d chunks, want %d", tt.size, tt.chunk, len(chunks), tt.wantCount)
			}
			if got := chunks[len(chunks)-1].Length; got != tt.wantLastLen {
				t.Fatalf("last chunk length = %d, want %d", got, tt.wantLastLen)
			}
			if err := ValidatePlan(chunks, tt.size); err != nil {
				t.Fatalf("ValidatePlan(Plan(%d, %d)): %v", tt.size, tt.chunk, err)
			}
		})
	}
}

// TestPlanProperties checks the plan invariants across a grid of sizes and
// chunk sizes: count, ordering, contiguity, full coverage, and that only
// the final chunk may be short.
func TestPlanProperties(t *testing.T) {
	sizes := []uint64{0, 1, 2, 255, 256, 257, 1 << 20, (1 << 20) + 1, 1<<32 + 12345}
	chunksizes := []uint64{1, 2, 3, 255, 256, 1 << 20, 1<<20 + 7}
	for _, size := range sizes {
		for _, cs := range chunksizes {
			wantCount := size / cs
			if size%cs != 0 {
				wantCount++
			}
			if wantCount > 1<<20 {
				// Too many chunks to materialize in a test; the
				// arithmetic is covered by TestPlanOverflow.
				continue
			}
			chunks, err := Plan(size, cs)
			if err != nil {
				t.Fatalf("Plan(%d, %d): %v", size, cs, err)
			}
			if uint64(len(chunks)) != wantCount {
				t.Fatalf("Plan(%d, %d) = %d chunks, want %d", size, cs, len(chunks), wantCount)
			}
			var off uint64
			for i, c := range chunks {
				if c.Index != i || c.Offset != off {
					t.Fatalf("Plan(%d, %d) chunk %d = %+v, want index %d offset %d", size, cs, i, c, i, off)
				}
				if c.Length == 0 || c.Length > cs {
					t.Fatalf("Plan(%d, %d) chunk %d length %d, want 1..%d", size, cs, i, c.Length, cs)
				}
				if i < len(chunks)-1 && c.Length != cs {
					t.Fatalf("Plan(%d, %d) chunk %d is short (%d) but not final", size, cs, i, c.Length)
				}
				off += c.Length
			}
			if off != size {
				t.Fatalf("Plan(%d, %d) covers %d bytes, want %d", size, cs, off, size)
			}
		}
	}
}

func TestPlanOverflow(t *testing.T) {
	// ceil(MaxUint64 / 2) = 2^63 chunks, which no int can index.
	if _, err := Plan(math.MaxUint64, 2); !errors.Is(err, ErrPlanOverflow) {
		t.Fatalf("Plan(MaxUint64, 2) = %v, want ErrPlanOverflow", err)
	}
	if _, err := Plan(math.MaxUint64, 1); !errors.Is(err, ErrPlanOverflow) {
		t.Fatalf("Plan(MaxUint64, 1) = %v, want ErrPlanOverflow", err)
	}
	// Exactly MaxInt chunks is the largest plannable count; exercise the
	// arithmetic through chunkCount rather than materializing the plan.
	if got := chunkCount(uint64(math.MaxInt), 1); got != uint64(math.MaxInt) {
		t.Fatalf("chunkCount(MaxInt, 1) = %d, want MaxInt", got)
	}
	if got := chunkCount(math.MaxUint64, math.MaxUint64); got != 1 {
		t.Fatalf("chunkCount(MaxUint64, MaxUint64) = %d, want 1", got)
	}
	if got := chunkCount(math.MaxUint64, 1<<62); got != 4 {
		t.Fatalf("chunkCount(MaxUint64, 2^62) = %d, want 4", got)
	}
}

func TestPlanDeterministic(t *testing.T) {
	a, err := Plan((1<<32)+12345, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Plan((1<<32)+12345, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatal("Plan is not deterministic")
	}
}

func TestValidatePlanMalformed(t *testing.T) {
	valid, err := Plan(10, 4) // [0,4) [4,8) [8,10)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidatePlan(valid, 10); err != nil {
		t.Fatalf("valid plan rejected: %v", err)
	}

	tests := []struct {
		name   string
		chunks []Chunk
		size   uint64
	}{
		{"wrong total size", valid, 11},
		{"plan short of size", valid[:2], 10},
		{"gap", []Chunk{{0, 0, 4}, {1, 5, 3}, {2, 8, 2}}, 10},
		{"overlap", []Chunk{{0, 0, 5}, {1, 4, 4}, {2, 8, 2}}, 11},
		{"duplicate index", []Chunk{{0, 0, 4}, {0, 4, 4}, {2, 8, 2}}, 10},
		{"out of order index", []Chunk{{0, 0, 4}, {2, 4, 4}, {1, 8, 2}}, 10},
		{"nonzero first index", []Chunk{{1, 0, 10}}, 10},
		{"zero length chunk", []Chunk{{0, 0, 4}, {1, 4, 0}, {2, 4, 6}}, 10},
		{"range exceeds size", []Chunk{{0, 0, 4}, {1, 4, 4}, {2, 8, 4}}, 10},
		{"offset/length overflow", []Chunk{{0, 0, 4}, {1, 4, math.MaxUint64 - 3}}, math.MaxUint64},
		{"empty plan for nonempty file", nil, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidatePlan(tt.chunks, tt.size); !errors.Is(err, ErrInvalidPlan) {
				t.Fatalf("ValidatePlan = %v, want ErrInvalidPlan", err)
			}
		})
	}
}

func TestValidatePlanEmpty(t *testing.T) {
	if err := ValidatePlan(nil, 0); err != nil {
		t.Fatalf("ValidatePlan(nil, 0): %v", err)
	}
	if err := ValidatePlan([]Chunk{}, 0); err != nil {
		t.Fatalf("ValidatePlan([], 0): %v", err)
	}
}
