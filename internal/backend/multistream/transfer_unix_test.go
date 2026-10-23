//go:build unix

package multistream

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/pkg/sftp"
)

// transferEnv bundles a transfer test's local source, remote dir, and
// checkpoint path.
type transferEnv struct {
	sourceAbs string
	sourceRel string
	remoteDir string
	ckptPath  string
	opts      TransferOptions
}

func newTransferEnv(t *testing.T, size uint64, chunkSize uint64) *transferEnv {
	t.Helper()
	if !realServerAvailable() {
		t.Skipf("real sftp-server %s not available; skipping real-server test", sftpServerBin)
	}
	base := t.TempDir()
	src := filepath.Join(base, "src", "big.bin")
	if err := os.MkdirAll(filepath.Dir(src), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(src, patternBytes(size), 0o644); err != nil {
		t.Fatal(err)
	}
	remoteDir := filepath.Join(base, "remote")
	if err := os.MkdirAll(remoteDir, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := helperConfig(t, t.TempDir(), envRealServer+"=1")
	return &transferEnv{
		sourceAbs: src,
		sourceRel: "src/big.bin",
		remoteDir: remoteDir,
		ckptPath:  filepath.Join(base, "state", "ckpt.json"),
		opts: TransferOptions{
			SFTP:      cfg,
			Streams:   2,
			Workers:   2,
			ChunkSize: chunkSize,
		},
	}
}

func (e *transferEnv) final() string { return path.Join(e.remoteDir, "big.bin") }

// TestTransferFreshRealServer: a fresh transfer writes the temp through
// multiple sessions, checkpoints every chunk, and never publishes the
// final path.
func TestTransferFreshRealServer(t *testing.T) {
	env := newTransferEnv(t, 40<<10, 8<<10) // 5 chunks
	var progresses [][2]int
	env.opts.OnChunkComplete = func(done, total int) {
		progresses = append(progresses, [2]int{done, total})
	}
	env.opts.CheckpointPath = env.ckptPath
	if err := Transfer(context.Background(), env.sourceAbs, env.sourceRel, env.final(), env.opts); err != nil {
		t.Fatalf("Transfer: %v", err)
	}
	// Checkpoint: all 5 chunks completed.
	ckpt, err := LoadCheckpoint(env.ckptPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(ckpt.Completed) != 5 {
		t.Errorf("completed = %v, want 5 chunks", ckpt.Completed)
	}
	if got := len(progresses); got != 5 {
		t.Errorf("progress calls = %d, want 5", got)
	}
	// Temp content is exact; final was never published.
	cfg := helperConfig(t, t.TempDir(), envRealServer+"=1")
	err = cfg.WithClient(context.Background(), func(client *sftp.Client) error {
		got, err := readAllRemote(client, ckpt.TempPath)
		if err != nil {
			return err
		}
		if string(got) != string(patternBytes(40<<10)) {
			return errors.New("temp content mismatch")
		}
		if _, err := client.Lstat(env.final()); !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("final published too early: %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTransferCheckpointConflict(t *testing.T) {
	env := newTransferEnv(t, 8<<10, 8<<10)
	if err := os.MkdirAll(filepath.Dir(env.ckptPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(env.ckptPath, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	env.opts.CheckpointPath = env.ckptPath
	if err := Transfer(context.Background(), env.sourceAbs, env.sourceRel, env.final(), env.opts); !errors.Is(err, ErrCheckpointConflict) {
		t.Fatalf("Transfer = %v, want ErrCheckpointConflict", err)
	}
}

// TestTransferCancelAndResume: cancel mid-transfer deterministically after
// the second chunk's checkpoint save; resume must finish the pending chunks
// only and produce a byte-exact temp.
func TestTransferCancelAndResume(t *testing.T) {
	env := newTransferEnv(t, 64<<10, 8<<10) // 8 chunks
	env.opts.CheckpointPath = env.ckptPath
	ctx, cancel := context.WithCancel(context.Background())
	env.opts.OnChunkComplete = func(done, total int) {
		if done == 2 {
			cancel()
		}
	}
	err := Transfer(ctx, env.sourceAbs, env.sourceRel, env.final(), env.opts)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Transfer = %v, want wrapped context.Canceled", err)
	}
	ckpt, err := LoadCheckpoint(env.ckptPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(ckpt.Completed) == 0 || len(ckpt.Completed) >= 8 {
		t.Fatalf("completed = %v, want a partial set", ckpt.Completed)
	}
	partial := len(ckpt.Completed)

	// Resume completes the pending chunks.
	env.opts.OnChunkComplete = nil
	if err := Resume(context.Background(), env.sourceAbs, env.ckptPath, env.final(), env.opts); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	ckpt, err = LoadCheckpoint(env.ckptPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(ckpt.Completed) != 8 {
		t.Errorf("completed after resume = %v, want all 8", ckpt.Completed)
	}
	cfg := helperConfig(t, t.TempDir(), envRealServer+"=1")
	err = cfg.WithClient(context.Background(), func(client *sftp.Client) error {
		got, err := readAllRemote(client, ckpt.TempPath)
		if err != nil {
			return err
		}
		if string(got) != string(patternBytes(64<<10)) {
			return fmt.Errorf("temp content mismatch after resume from %d chunks", partial)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestTransferCheckpointSaveFailureStopsCopy: after the first chunk's save,
// the checkpoint directory becomes unwritable; the next save must stop the
// copy and the on-disk checkpoint must hold exactly the first chunk.
func TestTransferCheckpointSaveFailureStopsCopy(t *testing.T) {
	env := newTransferEnv(t, 64<<10, 8<<10) // 8 chunks
	env.opts.CheckpointPath = env.ckptPath
	env.opts.Workers = 1 // deterministic: one chunk at a time
	env.opts.Streams = 1
	dir := filepath.Dir(env.ckptPath)
	locked := make(chan struct{})
	env.opts.OnChunkComplete = func(done, total int) {
		if done == 1 {
			if err := os.Chmod(dir, 0o500); err != nil {
				t.Errorf("chmod dir: %v", err)
			}
			close(locked)
		}
	}
	err := Transfer(context.Background(), env.sourceAbs, env.sourceRel, env.final(), env.opts)
	<-locked
	defer os.Chmod(dir, 0o700)
	if err == nil {
		t.Fatal("Transfer = nil, want the checkpoint save failure")
	}
	ckpt, lerr := LoadCheckpoint(env.ckptPath)
	if lerr != nil {
		t.Fatal(lerr)
	}
	if len(ckpt.Completed) != 1 {
		t.Errorf("completed = %v, want exactly 1 chunk (save failure stopped the copy)", ckpt.Completed)
	}
}

func TestResumeDestinationMismatch(t *testing.T) {
	env := newTransferEnv(t, 16<<10, 8<<10)
	env.opts.CheckpointPath = env.ckptPath
	ctx, cancel := context.WithCancel(context.Background())
	env.opts.OnChunkComplete = func(done, total int) { cancel() }
	if err := Transfer(ctx, env.sourceAbs, env.sourceRel, env.final(), env.opts); err == nil {
		t.Fatal("Transfer = nil, want cancellation")
	}
	env.opts.OnChunkComplete = nil
	other := path.Join(env.remoteDir, "other.bin")
	if err := Resume(context.Background(), env.sourceAbs, env.ckptPath, other, env.opts); !errors.Is(err, ErrDestinationMismatch) {
		t.Fatalf("Resume with wrong destination = %v, want ErrDestinationMismatch", err)
	}
}

func TestResumeSourceMismatch(t *testing.T) {
	env := newTransferEnv(t, 16<<10, 8<<10)
	env.opts.CheckpointPath = env.ckptPath
	ctx, cancel := context.WithCancel(context.Background())
	env.opts.OnChunkComplete = func(done, total int) { cancel() }
	if err := Transfer(ctx, env.sourceAbs, env.sourceRel, env.final(), env.opts); err == nil {
		t.Fatal("Transfer = nil, want cancellation")
	}
	// Change the source's size.
	if err := os.WriteFile(env.sourceAbs, patternBytes(12<<10), 0o644); err != nil {
		t.Fatal(err)
	}
	env.opts.OnChunkComplete = nil
	if err := Resume(context.Background(), env.sourceAbs, env.ckptPath, env.final(), env.opts); !errors.Is(err, ErrSourceMismatch) {
		t.Fatalf("Resume with changed size = %v, want ErrSourceMismatch", err)
	}
}

// TestTransferSourceMtimeChanged: an mtime-only source change mid-transfer
// is caught by the final source verification.
func TestTransferSourceMtimeChanged(t *testing.T) {
	env := newTransferEnv(t, 16<<10, 8<<10)
	env.opts.CheckpointPath = env.ckptPath
	env.opts.Workers = 1
	env.opts.Streams = 1
	env.opts.OnChunkComplete = func(done, total int) {
		if done == 1 {
			if err := os.Chtimes(env.sourceAbs, time.Unix(1_700_000_000, 0), time.Unix(1_700_000_000, 0)); err != nil {
				t.Errorf("chtimes: %v", err)
			}
		}
	}
	err := Transfer(context.Background(), env.sourceAbs, env.sourceRel, env.final(), env.opts)
	if !errors.Is(err, ErrSourceChanged) {
		t.Fatalf("Transfer = %v, want ErrSourceChanged", err)
	}
}
