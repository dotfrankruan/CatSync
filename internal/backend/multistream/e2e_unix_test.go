//go:build unix

package multistream

import (
	"context"
	"errors"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"
)

// TestNativeTransferEndToEnd is the full real-protocol lifecycle against the
// real OpenSSH sftp-server on a local directory: a multi-MiB deterministic
// file, four SFTP sessions, a deterministic mid-transfer cancellation with a
// partial checkpoint, a resume, byte-exact publication with exact mode and
// nanosecond mtime, and the documented "published but not retired" cleanup
// contract.
func TestNativeTransferEndToEnd(t *testing.T) {
	if !realServerAvailable() {
		t.Skipf("real sftp-server %s not available", sftpServerBin)
	}
	base := t.TempDir()
	src := filepath.Join(base, "src", "big.bin")
	if err := os.MkdirAll(filepath.Dir(src), 0o755); err != nil {
		t.Fatal(err)
	}
	const size = uint64(8 << 20) // 8 MiB
	data := patternBytes(size)
	if err := os.WriteFile(src, data, 0o640); err != nil {
		t.Fatal(err)
	}
	// A stable, non-whole-second mtime exercises nanosecond metadata.
	mtime := time.Unix(1_700_000_000, 987654321)
	if err := os.Chtimes(src, mtime, mtime); err != nil {
		t.Fatal(err)
	}
	remoteDir := filepath.Join(base, "remote")
	if err := os.MkdirAll(remoteDir, 0o755); err != nil {
		t.Fatal(err)
	}
	final := path.Join(remoteDir, "big.bin")
	ckptPath := filepath.Join(base, "state", "ckpt.json")
	cfg := helperConfig(t, t.TempDir(), envRealServer+"=1")
	opts := TransferOptions{
		SFTP:           cfg,
		Streams:        4,
		Workers:        4,
		ChunkSize:      256 << 10, // 32 chunks
		CheckpointPath: ckptPath,
	}

	// 1. Fresh transfer, deterministically cancelled after three chunks.
	ctx, cancel := context.WithCancel(context.Background())
	opts.OnChunkComplete = func(done, total int) {
		if done == 3 {
			cancel()
		}
	}
	_, err := Transfer(ctx, src, "src/big.bin", final, opts)
	cancel()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Transfer = %v, want wrapped context.Canceled", err)
	}
	// 2. The checkpoint records partial progress and the final is invisible.
	ckpt, err := LoadCheckpoint(ckptPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(ckpt.Completed) == 0 || len(ckpt.Completed) >= 32 {
		t.Fatalf("completed = %v, want a partial set", ckpt.Completed)
	}
	if _, err := os.Lstat(final); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("final visible before publication: %v", err)
	}
	tempPath := ckpt.TempPath
	if fi, err := os.Lstat(tempPath); err != nil || fi.Size() != int64(size) {
		t.Fatalf("temp must exist at full size after the partial copy: %v", err)
	}
	partial := len(ckpt.Completed)

	// 3. Resume: same temp, only pending chunks, byte-exact publish.
	opts.OnChunkComplete = nil
	res, err := Resume(context.Background(), src, "src/big.bin", ckptPath, final, opts)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if !res.Published || res.Recovered {
		t.Errorf("result = %+v, want a fresh publish", res)
	}
	got, err := os.ReadFile(final)
	if err != nil {
		t.Fatalf("published final missing: %v", err)
	}
	if len(got) != len(data) {
		t.Fatalf("final size = %d, want %d", len(got), len(data))
	}
	for i := range data {
		if got[i] != data[i] {
			t.Fatalf("final content differs at byte %d", i)
		}
	}
	fi, err := os.Lstat(final)
	if err != nil {
		t.Fatal(err)
	}
	if got := fi.Mode().Perm(); got != 0o640 {
		t.Errorf("final mode = %o, want source mode 640", got)
	}
	if !fi.ModTime().Equal(mtime) {
		t.Errorf("final mtime = %v, want %v (nanosecond exact)", fi.ModTime(), mtime)
	}
	// 4. A second Resume against the already-published state is the
	// crash-window recovery: it recognizes final and temp as the same
	// inode, reports Recovered, and copies nothing. It never overwrites.
	again, err := Resume(context.Background(), src, "src/big.bin", ckptPath, final, opts)
	if err != nil {
		t.Fatalf("recovery resume: %v", err)
	}
	if !again.Published || !again.Recovered {
		t.Errorf("second resume = %+v, want a recovered publish without recopy", again)
	}
	got2, err := os.ReadFile(final)
	if err != nil {
		t.Fatal(err)
	}
	if len(got2) != len(data) || got2[len(got2)-1] != data[len(data)-1] {
		t.Error("recovery must not rewrite the published final")
	}

	// 5. Cleanup contract: the library leaves the checkpoint and temp for
	// the caller; the caller retires them after recording completion.
	if _, err := os.Stat(ckptPath); err != nil {
		t.Errorf("checkpoint must survive Resume for the caller: %v", err)
	}
	if _, err := os.Stat(tempPath); err != nil {
		t.Errorf("temp must survive Resume for the caller: %v", err)
	}
	if err := os.Remove(tempPath); err != nil {
		t.Errorf("remove temp: %v", err)
	}
	if err := os.Remove(ckptPath); err != nil {
		t.Errorf("remove checkpoint: %v", err)
	}
	// The published final survives the cleanup.
	if _, err := os.Stat(final); err != nil {
		t.Errorf("final must survive cleanup: %v", err)
	}
	_ = partial
}
