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
// multiple sessions, checkpoints every chunk, and publishes the final path
// via no-clobber hardlink with exact content, mode, and mtime.
func TestTransferFreshRealServer(t *testing.T) {
	env := newTransferEnv(t, 40<<10, 8<<10) // 5 chunks
	var progresses [][2]int
	env.opts.OnChunkComplete = func(done, total int) {
		progresses = append(progresses, [2]int{done, total})
	}
	env.opts.CheckpointPath = env.ckptPath
	result, err := Transfer(context.Background(), env.sourceAbs, env.sourceRel, env.final(), env.opts)
	if err != nil {
		t.Fatalf("Transfer: %v", err)
	}
	if !result.Published || result.Recovered {
		t.Errorf("result = %+v, want published and not recovered", result)
	}
	if result.TempPath == "" {
		t.Error("result must record the temp path for later cleanup")
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
	// The final was published by hardlink: exact content, source mode, and
	// nanosecond mtime. The temp still exists (cleanup is the caller's job
	// after state is persisted).
	finalData, err := os.ReadFile(env.final())
	if err != nil {
		t.Fatal(err)
	}
	if string(finalData) != string(patternBytes(40<<10)) {
		t.Error("final content mismatch")
	}
	fiFinal, err := os.Lstat(env.final())
	if err != nil {
		t.Fatal(err)
	}
	fiSrc, err := os.Lstat(env.sourceAbs)
	if err != nil {
		t.Fatal(err)
	}
	if fiFinal.Mode().Perm() != fiSrc.Mode().Perm() {
		t.Errorf("final mode = %o, want source mode %o", fiFinal.Mode().Perm(), fiSrc.Mode().Perm())
	}
	if !fiFinal.ModTime().Equal(fiSrc.ModTime()) {
		t.Errorf("final mtime = %v, want source mtime %v", fiFinal.ModTime(), fiSrc.ModTime())
	}
	// Final and temp are the same object (hardlink): same inode.
	if !os.SameFile(mustStat(t, env.final()), mustStat(t, ckpt.TempPath)) {
		t.Error("final and temp are not the same object after hardlink publish")
	}
}

func mustStat(t *testing.T, path string) os.FileInfo {
	t.Helper()
	fi, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi
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
	if _, err := Transfer(context.Background(), env.sourceAbs, env.sourceRel, env.final(), env.opts); !errors.Is(err, ErrCheckpointConflict) {
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
	_, err := Transfer(ctx, env.sourceAbs, env.sourceRel, env.final(), env.opts)
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
	if _, err := Resume(context.Background(), env.sourceAbs, env.sourceRel, env.ckptPath, env.final(), env.opts); err != nil {
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
	_, err := Transfer(context.Background(), env.sourceAbs, env.sourceRel, env.final(), env.opts)
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
	if _, err := Transfer(ctx, env.sourceAbs, env.sourceRel, env.final(), env.opts); err == nil {
		t.Fatal("Transfer = nil, want cancellation")
	}
	env.opts.OnChunkComplete = nil
	other := path.Join(env.remoteDir, "other.bin")
	if _, err := Resume(context.Background(), env.sourceAbs, env.sourceRel, env.ckptPath, other, env.opts); !errors.Is(err, ErrDestinationMismatch) {
		t.Fatalf("Resume with wrong destination = %v, want ErrDestinationMismatch", err)
	}
}

func TestResumeSourceMismatch(t *testing.T) {
	env := newTransferEnv(t, 16<<10, 8<<10)
	env.opts.CheckpointPath = env.ckptPath
	ctx, cancel := context.WithCancel(context.Background())
	env.opts.OnChunkComplete = func(done, total int) { cancel() }
	if _, err := Transfer(ctx, env.sourceAbs, env.sourceRel, env.final(), env.opts); err == nil {
		t.Fatal("Transfer = nil, want cancellation")
	}
	// Change the source's size.
	if err := os.WriteFile(env.sourceAbs, patternBytes(12<<10), 0o644); err != nil {
		t.Fatal(err)
	}
	env.opts.OnChunkComplete = nil
	if _, err := Resume(context.Background(), env.sourceAbs, env.sourceRel, env.ckptPath, env.final(), env.opts); !errors.Is(err, ErrSourceMismatch) {
		t.Fatalf("Resume with changed size = %v, want ErrSourceMismatch", err)
	}
}

// TestResumeSourceBindingBeforeSSH proves the Resume API itself validates
// the expected relative source path and its checkpoint binding before it can
// start the configured ssh program. The executor's duplicate check is only
// defense in depth, not part of the library's correctness.
func TestResumeSourceBindingBeforeSSH(t *testing.T) {
	env := newTransferEnv(t, 16<<10, 8<<10)
	env.opts.CheckpointPath = env.ckptPath
	fi, err := os.Stat(env.sourceAbs)
	if err != nil {
		t.Fatal(err)
	}
	tempPath := path.Join(env.remoteDir, ".catsync-tmp-binding")
	ckpt, err := NewCheckpoint(env.sourceRel, uint64(fi.Size()), fi.ModTime(),
		destinationIdentity(env.opts.SFTP.Dest, env.final()), tempPath, 8<<10)
	if err != nil {
		t.Fatal(err)
	}
	if err := ckpt.Save(env.ckptPath); err != nil {
		t.Fatal(err)
	}
	// If validation regresses and SSH is attempted, this nonexistent program
	// produces an unrelated start error and the ErrSourceMismatch assertion
	// below fails.
	env.opts.SFTP.Program = filepath.Join(t.TempDir(), "ssh-must-not-start")

	for _, sourceRel := range []string{"../escape.bin", "other.bin"} {
		_, err := Resume(context.Background(), env.sourceAbs, sourceRel, env.ckptPath, env.final(), env.opts)
		if !errors.Is(err, ErrSourceMismatch) {
			t.Errorf("Resume source %q = %v, want ErrSourceMismatch before SSH", sourceRel, err)
		}
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
	_, err := Transfer(context.Background(), env.sourceAbs, env.sourceRel, env.final(), env.opts)
	if !errors.Is(err, ErrSourceChanged) {
		t.Fatalf("Transfer = %v, want ErrSourceChanged", err)
	}
}
