package multistream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func validCheckpoint(t *testing.T) *Checkpoint {
	t.Helper()
	c, err := NewCheckpoint("dir/big.bin", 256, time.Unix(1700000000, 123456789), "user@example.com:/data", "/data/.catsync-tmp/big.bin", 64)
	if err != nil {
		t.Fatalf("NewCheckpoint: %v", err)
	}
	return c
}

func TestCheckpointRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ckpt.json")
	mt := time.Unix(1700000000, 123456789).UTC()
	c, err := NewCheckpoint("dir/big.bin", 256, mt, "user@example.com:/data", "/data/.tmp/big.bin", 64)
	if err != nil {
		t.Fatal(err)
	}
	for _, idx := range []int{2, 0} {
		if err := c.MarkComplete(idx); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Save(path); err != nil {
		t.Fatal(err)
	}
	got, err := LoadCheckpoint(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Source != "dir/big.bin" || got.Size != 256 || got.ChunkSize != 64 {
		t.Errorf("identity fields = %+v", got)
	}
	if !got.ModTime.Equal(mt) {
		t.Errorf("modtime = %v, want %v", got.ModTime, mt)
	}
	if got.Destination != "user@example.com:/data" || got.TempPath != "/data/.tmp/big.bin" {
		t.Errorf("destination/temp = %q %q", got.Destination, got.TempPath)
	}
	if fmt.Sprint(got.Completed) != "[0 2]" {
		t.Errorf("completed = %v, want ascending [0 2]", got.Completed)
	}
}

func TestCheckpointCompletedSavedAscending(t *testing.T) {
	c := validCheckpoint(t)
	for _, idx := range []int{3, 0, 2, 1} {
		if err := c.MarkComplete(idx); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(t.TempDir(), "ckpt.json")
	if err := c.Save(path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"completed":[0,1,2,3]`) {
		t.Errorf("saved completed list not ascending: %s", data)
	}
}

func TestCheckpointNanosecondModTime(t *testing.T) {
	c := validCheckpoint(t)
	path := filepath.Join(t.TempDir(), "ns.json")
	if err := c.Save(path); err != nil {
		t.Fatal(err)
	}
	got, err := LoadCheckpoint(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.ModTime.Nanosecond() != 123456789 {
		t.Errorf("nanoseconds lost: %v", got.ModTime)
	}
}

func TestCheckpointPermissions(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sub", "dir")
	c := validCheckpoint(t)
	path := filepath.Join(dir, "ckpt.json")
	if err := c.Save(path); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("checkpoint mode = %o, want 600", fi.Mode().Perm())
	}
	di, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if di.Mode().Perm() != 0o700 {
		t.Errorf("directory mode = %o, want 700", di.Mode().Perm())
	}
}

func TestCheckpointNotFound(t *testing.T) {
	_, err := LoadCheckpoint(filepath.Join(t.TempDir(), "nope.json"))
	if !errors.Is(err, ErrCheckpointNotFound) {
		t.Fatalf("Load = %v, want ErrCheckpointNotFound", err)
	}
}

func TestCheckpointMalformedJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ckpt.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCheckpoint(path); !errors.Is(err, ErrCheckpointCorrupt) {
		t.Fatalf("Load = %v, want ErrCheckpointCorrupt", err)
	}
}

func TestCheckpointUnknownField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ckpt.json")
	body := `{"version":1,"source":"a","size":1,"mod_time":"2023-11-14T22:13:20.123456789Z","destination":"h:/d","temp_path":"/d/.t/a","chunk_size":64,"completed":[],"secret":"x"}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCheckpoint(path); !errors.Is(err, ErrCheckpointCorrupt) {
		t.Fatalf("Load = %v, want ErrCheckpointCorrupt", err)
	}
}

func TestCheckpointTrailingJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ckpt.json")
	c := validCheckpoint(t)
	if err := c.Save(path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, []byte(" {}\n")...)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCheckpoint(path); !errors.Is(err, ErrCheckpointCorrupt) {
		t.Fatalf("Load = %v, want ErrCheckpointCorrupt", err)
	}
}

func TestCheckpointUnsupportedVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ckpt.json")
	c := validCheckpoint(t)
	if err := c.Save(path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data = bytes.Replace(data, []byte(`"version":1`), []byte(`"version":99`), 1)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCheckpoint(path); !errors.Is(err, ErrCheckpointUnsupportedVersion) {
		t.Fatalf("Load = %v, want ErrCheckpointUnsupportedVersion", err)
	}
}

func TestCheckpointRejectsBadIdentity(t *testing.T) {
	mt := time.Unix(1700000000, 0).UTC()
	cases := []struct {
		name               string
		source, dest, temp string
		size, chunk        uint64
	}{
		{"source absolute", "/a", "h:/d", "/d/.t/a", 10, 64},
		{"source parent escape", "../a", "h:/d", "/d/.t/a", 10, 64},
		{"source NUL", "a\x00b", "h:/d", "/d/.t/a", 10, 64},
		{"destination empty", "a", "", "/d/.t/a", 10, 64},
		{"destination NUL", "a", "h\x00:/d", "/d/.t/a", 10, 64},
		{"temp path relative", "a", "h:/d", "d/.t/a", 10, 64},
		{"temp path NUL", "a", "h:/d", "/d/.t/a\x00", 10, 64},
		{"temp path empty", "a", "h:/d", "", 10, 64},
		{"chunk size zero", "a", "h:/d", "/d/.t/a", 10, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewCheckpoint(tc.source, tc.size, mt, tc.dest, tc.temp, tc.chunk); err == nil {
				t.Error("NewCheckpoint accepted invalid identity")
			}
		})
	}
}

func TestCheckpointMarkCompleteRules(t *testing.T) {
	c := validCheckpoint(t) // size 256, chunk 64 → 4 chunks, indexes 0..3
	if err := c.MarkComplete(-1); err == nil {
		t.Error("negative index accepted")
	}
	if err := c.MarkComplete(4); err == nil {
		t.Error("index outside plan accepted")
	}
	if err := c.MarkComplete(2); err != nil {
		t.Fatal(err)
	}
	if err := c.MarkComplete(2); err != nil {
		t.Fatal(err)
	}
	if len(c.Completed) != 1 {
		t.Errorf("completed = %v, want exactly one entry", c.Completed)
	}
	// Out-of-range indexes in a loaded document are corruption.
	path := filepath.Join(t.TempDir(), "ckpt.json")
	if err := c.Save(path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data = bytes.Replace(data, []byte(`"completed":[2]`), []byte(`"completed":[2,2]`), 1)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCheckpoint(path); !errors.Is(err, ErrCheckpointCorrupt) {
		t.Fatalf("duplicate completed index: Load = %v, want ErrCheckpointCorrupt", err)
	}
}

func TestCheckpointSaveFailureKeepsPrevious(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ckpt.json")
	c := validCheckpoint(t)
	if err := c.Save(path); err != nil {
		t.Fatal(err)
	}
	// Corrupt the in-memory checkpoint, then save: the failure must leave
	// the previous valid checkpoint untouched.
	c.Source = "../bad"
	if err := c.Save(path); err == nil {
		t.Fatal("Save of invalid checkpoint = nil, want error")
	}
	got, err := LoadCheckpoint(path)
	if err != nil {
		t.Fatalf("previous checkpoint lost after failed save: %v", err)
	}
	if got.Source != "dir/big.bin" {
		t.Errorf("previous checkpoint source = %q, want dir/big.bin", got.Source)
	}
}

// TestCheckpointCompletedSetFeedsCopy proves the loaded checkpoint's
// completed set drives Copy's resume behavior: only the pending chunks are
// transferred.
func TestCheckpointCompletedSetFeedsCopy(t *testing.T) {
	size := uint64(256)
	chunks, err := Plan(size, 64)
	if err != nil {
		t.Fatal(err)
	}
	c := validCheckpoint(t) // size 256, chunk 64 → 4 chunks
	if err := c.MarkComplete(1); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "ckpt.json")
	if err := c.Save(path); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadCheckpoint(path)
	if err != nil {
		t.Fatal(err)
	}

	src := &testReader{data: patternBytes(size)}
	dst := newWriter(size)
	// Chunk 1 was durably transferred earlier; its destination range
	// already holds the right bytes.
	copy(dst.data[chunks[1].Offset:chunks[1].End()], src.data[chunks[1].Offset:chunks[1].End()])
	var mu sync.Mutex
	var callbacks []int
	if err := Copy(context.Background(), src, dst, size, chunks, CopyOptions{
		Workers:   2,
		Completed: loaded.CompletedSet(),
		OnComplete: func(ch Chunk) error {
			mu.Lock()
			callbacks = append(callbacks, ch.Index)
			mu.Unlock()
			return nil
		},
	}); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if len(callbacks) != 3 {
		t.Errorf("callbacks = %v, want exactly the 3 pending chunks", callbacks)
	}
	for _, idx := range callbacks {
		if idx == 1 {
			t.Error("completed chunk 1 got a callback")
		}
	}
	if !bytes.Equal(dst.snapshot(), src.data) {
		t.Error("content mismatch after checkpoint-driven resume")
	}
}
