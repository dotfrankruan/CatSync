//go:build unix

package multistream

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type downloadEnv struct {
	remoteSource string
	sourceRel    string
	localFinal   string
	ckptPath     string
	opts         TransferOptions
}

func newDownloadEnv(t *testing.T, size, chunkSize uint64) *downloadEnv {
	t.Helper()
	if !realServerAvailable() {
		t.Skipf("real sftp-server %s not available; skipping real-server test", sftpServerBin)
	}
	base := t.TempDir()
	remote := filepath.Join(base, "remote", "source with space.bin")
	if err := os.MkdirAll(filepath.Dir(remote), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(remote, patternBytes(size), 0o640); err != nil {
		t.Fatal(err)
	}
	mtime := time.Unix(1_700_000_123, 456789123)
	if err := os.Chtimes(remote, mtime, mtime); err != nil {
		t.Fatal(err)
	}
	localDir := filepath.Join(base, "local")
	if err := os.MkdirAll(localDir, 0o755); err != nil {
		t.Fatal(err)
	}
	return &downloadEnv{
		remoteSource: remote,
		sourceRel:    "nested/source with space.bin",
		localFinal:   filepath.Join(localDir, "downloaded.bin"),
		ckptPath:     filepath.Join(base, "state", "download.json"),
		opts: TransferOptions{
			SFTP:      helperConfig(t, t.TempDir(), envRealServer+"=1"),
			Streams:   2,
			Workers:   2,
			ChunkSize: chunkSize,
		},
	}
}

func TestDownloadFreshRealServer(t *testing.T) {
	env := newDownloadEnv(t, 40<<10, 8<<10)
	env.opts.CheckpointPath = env.ckptPath
	result, err := Download(context.Background(), env.remoteSource, env.sourceRel, env.localFinal, env.opts)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	if !result.Published || result.Recovered {
		t.Fatalf("result = %+v", result)
	}
	got, err := os.ReadFile(env.localFinal)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(patternBytes(40<<10)) {
		t.Fatal("downloaded content mismatch")
	}
	remoteInfo := mustStat(t, env.remoteSource)
	localInfo := mustStat(t, env.localFinal)
	if localInfo.Mode().Perm() != remoteInfo.Mode().Perm() || !localInfo.ModTime().Equal(remoteInfo.ModTime()) {
		t.Fatalf("local metadata = %o %v, want %o %v", localInfo.Mode().Perm(), localInfo.ModTime(), remoteInfo.Mode().Perm(), remoteInfo.ModTime())
	}
	ckpt, err := LoadCheckpoint(env.ckptPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(ckpt.Completed) != 5 || !os.SameFile(localInfo, mustStat(t, ckpt.TempPath)) {
		t.Fatalf("checkpoint = %+v; final/temp are not the completed hardlink pair", ckpt)
	}
}

func TestDownloadCancelAndResume(t *testing.T) {
	env := newDownloadEnv(t, 64<<10, 8<<10)
	env.opts.CheckpointPath = env.ckptPath
	env.opts.Streams = 1
	env.opts.Workers = 1
	ctx, cancel := context.WithCancel(context.Background())
	env.opts.OnChunkComplete = func(done, total int) {
		if done == 2 {
			cancel()
		}
	}
	_, err := Download(ctx, env.remoteSource, env.sourceRel, env.localFinal, env.opts)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Download = %v, want context.Canceled", err)
	}
	ckpt, err := LoadCheckpoint(env.ckptPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(ckpt.Completed) != 2 {
		t.Fatalf("completed = %v, want exactly first two chunks", ckpt.Completed)
	}

	var resumed int
	env.opts.OnChunkComplete = func(done, total int) { resumed++ }
	if _, err := ResumeDownload(context.Background(), env.remoteSource, env.sourceRel, env.ckptPath, env.localFinal, env.opts); err != nil {
		t.Fatalf("ResumeDownload: %v", err)
	}
	if resumed != 6 {
		t.Fatalf("resume copied %d chunks, want 6 pending chunks", resumed)
	}
	got, err := os.ReadFile(env.localFinal)
	if err != nil || string(got) != string(patternBytes(64<<10)) {
		t.Fatalf("resumed content mismatch: %v", err)
	}
}

func TestDownloadDetectsRemoteSourceChange(t *testing.T) {
	env := newDownloadEnv(t, 32<<10, 8<<10)
	env.opts.CheckpointPath = env.ckptPath
	env.opts.Streams = 1
	env.opts.Workers = 1
	env.opts.OnChunkComplete = func(done, total int) {
		if done == 1 {
			changed := time.Unix(1_700_000_999, 123)
			if err := os.Chtimes(env.remoteSource, changed, changed); err != nil {
				t.Errorf("change remote mtime: %v", err)
			}
		}
	}
	_, err := Download(context.Background(), env.remoteSource, env.sourceRel, env.localFinal, env.opts)
	if !errors.Is(err, ErrSourceChanged) {
		t.Fatalf("Download = %v, want ErrSourceChanged", err)
	}
	if _, err := os.Lstat(env.localFinal); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("final exists after source change: %v", err)
	}
}

func TestResumeDownloadBindingsBeforeSSH(t *testing.T) {
	env := newDownloadEnv(t, 8<<10, 8<<10)
	fi := mustStat(t, env.remoteSource)
	temp := filepath.Join(filepath.Dir(env.localFinal), ".temp")
	ckpt, err := NewCheckpoint(env.sourceRel, uint64(fi.Size()), fi.ModTime(), downloadDestinationIdentity(env.localFinal), temp, 8<<10)
	if err != nil {
		t.Fatal(err)
	}
	if err := ckpt.Save(env.ckptPath); err != nil {
		t.Fatal(err)
	}
	env.opts.SFTP.Program = filepath.Join(t.TempDir(), "ssh-must-not-start")
	for _, tc := range []struct {
		rel, final string
		want       error
	}{
		{"other.bin", env.localFinal, ErrSourceMismatch},
		{env.sourceRel, filepath.Join(filepath.Dir(env.localFinal), "other.bin"), ErrDestinationMismatch},
	} {
		_, err := ResumeDownload(context.Background(), env.remoteSource, tc.rel, env.ckptPath, tc.final, env.opts)
		if !errors.Is(err, tc.want) {
			t.Errorf("ResumeDownload(%q, %q) = %v, want %v", tc.rel, tc.final, err, tc.want)
		}
	}
}

func TestResumeDownloadRecoversPublishedCrashWindow(t *testing.T) {
	env := newDownloadEnv(t, 8<<10, 8<<10)
	env.opts.CheckpointPath = env.ckptPath
	if _, err := Download(context.Background(), env.remoteSource, env.sourceRel, env.localFinal, env.opts); err != nil {
		t.Fatal(err)
	}
	result, err := ResumeDownload(context.Background(), env.remoteSource, env.sourceRel, env.ckptPath, env.localFinal, env.opts)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Published || !result.Recovered {
		t.Fatalf("result = %+v, want recovered publish", result)
	}
}
