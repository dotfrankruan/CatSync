package partition

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
)

func item(path string, size int64) Item { return Item{Path: path, Size: size} }

func TestPackEmpty(t *testing.T) {
	batches, err := Pack(nil, Config{MaxFiles: 10, MaxBytes: 100})
	if err != nil {
		t.Fatalf("Pack(nil): %v", err)
	}
	if len(batches) != 0 {
		t.Fatalf("Pack(nil) = %d batches, want 0", len(batches))
	}
}

func TestPackSingleBatch(t *testing.T) {
	got, err := Pack([]Item{item("a", 10), item("b", 0), item("c", 5)}, Config{MaxFiles: 10, MaxBytes: 100})
	if err != nil {
		t.Fatal(err)
	}
	want := []Batch{{Items: []Item{item("a", 10), item("b", 0), item("c", 5)}, Bytes: 15}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Pack = %#v, want %#v", got, want)
	}
}

func TestPackSplitsByFileCount(t *testing.T) {
	items := []Item{item("a", 0), item("b", 0), item("c", 0), item("d", 0), item("e", 0)}
	got, err := Pack(items, Config{MaxFiles: 2, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	var sizes []int
	for _, b := range got {
		sizes = append(sizes, len(b.Items))
	}
	if want := []int{2, 2, 1}; !reflect.DeepEqual(sizes, want) {
		t.Fatalf("batch sizes = %v, want %v", sizes, want)
	}
	if !reflect.DeepEqual(got[0].Items, items[0:2]) || !reflect.DeepEqual(got[2].Items, items[4:]) {
		t.Fatalf("batches do not preserve input order: %#v", got)
	}
}

func TestPackSplitsByBytes(t *testing.T) {
	items := []Item{item("a", 6), item("b", 6), item("c", 6)}
	got, err := Pack(items, Config{MaxFiles: 100, MaxBytes: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("batches = %d, want 3 (each 6-byte item exceeds a 10-byte pair)", len(got))
	}
	for i, b := range got {
		if b.Bytes != 6 || len(b.Items) != 1 || b.Items[0] != items[i] {
			t.Errorf("batch %d = %#v", i, b)
		}
	}
}

func TestPackFileAndByteLimitsTogether(t *testing.T) {
	// Three 4-byte items: max two files (8 bytes) per batch; the third
	// starts a new batch even though it would fit in bytes.
	items := []Item{item("a", 4), item("b", 4), item("c", 4)}
	got, err := Pack(items, Config{MaxFiles: 2, MaxBytes: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || len(got[0].Items) != 2 || len(got[1].Items) != 1 {
		t.Fatalf("batches = %#v, want [2,1]", got)
	}
	if got[0].Bytes != 8 || got[1].Bytes != 4 {
		t.Fatalf("batch bytes = %d/%d, want 8/4", got[0].Bytes, got[1].Bytes)
	}
}

func TestPackExactByteLimit(t *testing.T) {
	// Two 5-byte items exactly fill a 10-byte batch.
	got, err := Pack([]Item{item("a", 5), item("b", 5)}, Config{MaxFiles: 10, MaxBytes: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Bytes != 10 || len(got[0].Items) != 2 {
		t.Fatalf("Pack = %#v, want one exact 10-byte batch", got)
	}
	// A single item equal to the limit is accepted exactly.
	got, err = Pack([]Item{item("a", 10)}, Config{MaxFiles: 1, MaxBytes: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Bytes != 10 {
		t.Fatalf("exact-size item batch = %#v", got)
	}
}

func TestPackZeroByteFiles(t *testing.T) {
	var items []Item
	for i := 0; i < 5; i++ {
		items = append(items, item(strings.Repeat("z", i+1), 0))
	}
	got, err := Pack(items, Config{MaxFiles: 2, MaxBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("batches = %d, want 3 (file-count limited)", len(got))
	}
	seen := 0
	for _, b := range got {
		if b.Bytes != 0 {
			t.Errorf("zero-byte batch has Bytes = %d", b.Bytes)
		}
		seen += len(b.Items)
	}
	if seen != 5 {
		t.Fatalf("items seen = %d, want 5", seen)
	}
	// A zero-byte file is a legal small file even when MaxBytes is 1.
	got, err = Pack([]Item{item("empty", 0)}, Config{MaxFiles: 1, MaxBytes: 1})
	if err != nil || len(got) != 1 {
		t.Fatalf("zero-byte single item = %#v, %v", got, err)
	}
}

func TestPackRejectsInvalidInput(t *testing.T) {
	cfg := Config{MaxFiles: 10, MaxBytes: 100}
	cases := []struct {
		name  string
		items []Item
		cfg   Config
		want  string
	}{
		{"negative size", []Item{item("a", -1)}, cfg, "negative size"},
		{"empty path", []Item{item("", 1)}, cfg, "path is empty"},
		{"item over byte limit", []Item{item("big", 101)}, cfg, "larger than"},
		{"item over limit after valid items", []Item{item("ok", 1), item("big", 101)}, cfg, "larger than"},
		{"zero MaxFiles", []Item{item("a", 1)}, Config{MaxFiles: 0, MaxBytes: 100}, "MaxFiles"},
		{"zero MaxBytes", []Item{item("a", 1)}, Config{MaxFiles: 1, MaxBytes: 0}, "MaxBytes"},
		{"no items but zero MaxFiles", nil, Config{MaxFiles: 0, MaxBytes: 1}, "MaxFiles"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			batches, err := Pack(tc.items, tc.cfg)
			if err == nil {
				t.Fatalf("Pack = %#v, want error", batches)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want it to mention %q", err, tc.want)
			}
			if batches != nil {
				t.Errorf("error result = %#v, want nil", batches)
			}
		})
	}
	if !errors.Is(func() error {
		_, err := Pack([]Item{item("a", 1)}, Config{MaxFiles: 0, MaxBytes: 1})
		return err
	}(), ErrInvalidConfig) {
		t.Error("config error does not wrap ErrInvalidConfig")
	}
}

func TestPackEachPathExactlyOnceAndDisjoint(t *testing.T) {
	var items []Item
	for i := 0; i < 50; i++ {
		items = append(items, item(fmt.Sprintf("dir/file-%03d.txt", i), int64(i)))
	}
	got, err := Pack(items, Config{MaxFiles: 7, MaxBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	for _, b := range got {
		for _, it := range b.Items {
			seen[it.Path]++
		}
		if len(b.Items) > 7 {
			t.Errorf("batch has %d items, over MaxFiles 7", len(b.Items))
		}
		if b.Bytes > 64 {
			t.Errorf("batch has %d bytes, over MaxBytes 64", b.Bytes)
		}
	}
	if len(seen) != len(items) {
		t.Fatalf("distinct paths = %d, want %d", len(seen), len(items))
	}
	for p, n := range seen {
		if n != 1 {
			t.Errorf("path %q appears %d times, want exactly once", p, n)
		}
	}
}

func TestPackDeterministic(t *testing.T) {
	var items []Item
	for i := 0; i < 40; i++ {
		// Unique paths with small sizes that fit the 40-byte limit.
		items = append(items, item(fmt.Sprintf("p%02d", i), int64(i%9)))
	}
	cfg := Config{MaxFiles: 5, MaxBytes: 40}
	first, err := Pack(items, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for run := 0; run < 5; run++ {
		again, err := Pack(items, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(first, again) {
			t.Fatalf("run %d differs:\n first %#v\n again %#v", run, first, again)
		}
	}
}

func TestPackNoOverflowAtUint64Boundaries(t *testing.T) {
	// MaxInt64-sized items pack without overflowing the byte counter.
	max := int64(math.MaxInt64)
	items := []Item{item("a", max), item("b", max), item("c", 0)}
	got, err := Pack(items, Config{MaxFiles: 10, MaxBytes: math.MaxUint64})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("batches = %d, want 1", len(got))
	}
	if want := uint64(math.MaxInt64) * 2; got[0].Bytes != want {
		t.Fatalf("Bytes = %d, want %d", got[0].Bytes, want)
	}
	// MaxBytes == MaxInt64 rejects a single MaxInt64 item only when it is
	// strictly larger; equality is accepted.
	if _, err := Pack([]Item{item("big", max)}, Config{MaxFiles: 1, MaxBytes: math.MaxUint64}); err != nil {
		t.Fatalf("MaxInt64 item with MaxUint64 limit rejected: %v", err)
	}
	if _, err := Pack([]Item{item("big", max)}, Config{MaxFiles: 1, MaxBytes: uint64(max) - 1}); err == nil {
		t.Fatal("MaxInt64 item accepted under a smaller limit")
	}
}

func TestPackOpaqueSpecialPaths(t *testing.T) {
	paths := []string{
		"with space.txt",
		"ünïcødé-文件.txt",
		"new\nline.txt",
		"quote'and\"double.txt",
		"-leading-dash.txt",
		"tab\tname.txt",
	}
	var items []Item
	for i, p := range paths {
		items = append(items, item(p, int64(i)))
	}
	got, err := Pack(items, Config{MaxFiles: 100, MaxBytes: 100})
	if err != nil {
		t.Fatal(err)
	}
	var gotPaths []string
	for _, b := range got {
		gotPaths = append(gotPaths, b.Paths()...)
	}
	if !reflect.DeepEqual(gotPaths, paths) {
		t.Fatalf("paths = %#v, want verbatim %#v", gotPaths, paths)
	}
	if got, want := got[0].Items[1].Path, "ünïcødé-文件.txt"; got != want {
		t.Fatalf("unicode path = %q, want %q", got, want)
	}
}
