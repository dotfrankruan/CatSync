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

	"github.com/pkg/sftp"
)

// TestPublishFinalRealServer: full publish flow on the real server —
// metadata, hardlink, verification, same-inode.
func TestPublishFinalRealServer(t *testing.T) {
	dir := t.TempDir()
	final := path.Join(dir, "big.bin")
	tmp := path.Join(dir, ".catsync-tmp-big.bin")
	mtime := time.Unix(1700000000, 123456789).UTC()
	withRealSFTPServer(t, func(client *sftp.Client) error {
		spec := RemoteTempSpec{FinalPath: final, TempPath: tmp, Size: 5}
		if err := PrepareFreshTemp(client, spec); err != nil {
			return err
		}
		w, err := OpenResumeTemp(client, spec)
		if err != nil {
			return err
		}
		if _, err := w.WriteAt([]byte("hello"), 0); err != nil {
			return err
		}
		if err := w.Sync(); err != nil {
			return err
		}
		if err := w.Close(); err != nil {
			return err
		}
		cfg := helperConfig(t, t.TempDir(), envRealServer+"=1")
		if err := SetRemoteMeta(context.Background(), cfg, client, tmp, 0o640, mtime); err != nil {
			return err
		}
		if err := PublishFinal(context.Background(), cfg, client, spec, mtime); err != nil {
			return err
		}
		// Final exists, exact content, same inode as temp.
		got, err := readAllRemote(client, final)
		if err != nil {
			return err
		}
		if string(got) != "hello" {
			return errors.New("final content mismatch")
		}
		if !os.SameFile(mustStat2(t, final), mustStat2(t, tmp)) {
			return errors.New("final and temp are not the same object")
		}
		fi, err := os.Lstat(final)
		if err != nil {
			return err
		}
		if fi.Mode().Perm() != 0o640 {
			return errors.New("final mode mismatch")
		}
		if !fi.ModTime().Equal(mtime) {
			return errors.New("final mtime mismatch")
		}
		return nil
	})
}

func mustStat2(t *testing.T, p string) os.FileInfo {
	t.Helper()
	fi, err := os.Lstat(p)
	if err != nil {
		t.Fatal(err)
	}
	return fi
}

// TestPublishFinalRefusesExistingFinal: an existing final is never
// overwritten; the error is ErrRemoteFinalExists and the content is intact.
func TestPublishFinalRefusesExistingFinal(t *testing.T) {
	dir := t.TempDir()
	final := path.Join(dir, "big.bin")
	tmp := path.Join(dir, ".catsync-tmp-big.bin")
	if err := os.WriteFile(final, []byte("KEEP ME"), 0o644); err != nil {
		t.Fatal(err)
	}
	withRealSFTPServer(t, func(client *sftp.Client) error {
		// Build the temp directly (PrepareFreshTemp would rightly refuse an
		// existing final; here we want to test PublishFinal's own refusal).
		spec := RemoteTempSpec{FinalPath: final, TempPath: tmp, Size: 3}
		f, err := client.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL)
		if err != nil {
			return err
		}
		if err := f.Truncate(3); err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		cfg := helperConfig(t, t.TempDir(), envRealServer+"=1")
		err = PublishFinal(context.Background(), cfg, client, spec, time.Unix(1_700_000_000, 0))
		if !errors.Is(err, ErrRemoteFinalExists) {
			return errors.New("expected ErrRemoteFinalExists, got " + fmtSprint(err))
		}
		got, err := os.ReadFile(final)
		if err != nil {
			return err
		}
		if string(got) != "KEEP ME" {
			return errors.New("final content was clobbered")
		}
		return nil
	})
}

func fmtSprint(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}

// TestProbePublishStateRealServer covers the crash-window recovery probe:
// final missing → continue; final==temp same inode → published; final is a
// different object → conflict; temp missing + final exists → conflict.
func TestProbePublishStateRealServer(t *testing.T) {
	dir := t.TempDir()
	final := path.Join(dir, "big.bin")
	tmp := path.Join(dir, ".catsync-tmp-big.bin")
	cfg := helperConfig(t, t.TempDir(), envRealServer+"=1")
	withRealSFTPServer(t, func(client *sftp.Client) error {
		spec := RemoteTempSpec{FinalPath: final, TempPath: tmp, Size: 0}
		if err := PrepareFreshTemp(client, spec); err != nil {
			return err
		}
		// Final missing → not published.
		published, err := ProbePublishState(context.Background(), cfg, client, spec)
		if err != nil || published {
			return errors.New("final missing should report unpublished")
		}
		// Publish via hardlink, then probe: same object → published.
		if err := client.Link(tmp, final); err != nil {
			return err
		}
		published, err = ProbePublishState(context.Background(), cfg, client, spec)
		if err != nil || !published {
			return errors.New("same-inode final+temp should report published")
		}
		// A different final object → conflict.
		if err := client.Remove(final); err != nil {
			return err
		}
		if err := os.WriteFile(final, []byte("other"), 0o644); err != nil {
			return err
		}
		if _, err := ProbePublishState(context.Background(), cfg, client, spec); !errors.Is(err, ErrPublishConflict) {
			return errors.New("different final should report conflict, got " + fmtSprint(err))
		}
		// No temp to prove identity → conflict.
		if err := client.Remove(final); err != nil {
			return err
		}
		if err := client.Remove(tmp); err != nil {
			return err
		}
		if err := os.WriteFile(final, []byte("other"), 0o644); err != nil {
			return err
		}
		if _, err := ProbePublishState(context.Background(), cfg, client, spec); !errors.Is(err, ErrPublishConflict) {
			return errors.New("missing temp with existing final should report conflict, got " + fmtSprint(err))
		}
		return nil
	})
}

// TestTransferHardlinkUnsupported: a server without the extension makes
// Transfer fail before any remote write, preserving the checkpoint.
func TestTransferHardlinkUnsupported(t *testing.T) {
	env := newTransferEnv(t, 8<<10, 8<<10)
	// The in-mem server advertises no extensions with this knob.
	env.opts.SFTP.Env = []string{"CATSYNC_TEST_SFTP_HELPER=1", "CATSYNC_TEST_SFTP_NOEXT=1"}
	env.opts.CheckpointPath = env.ckptPath
	_, err := Transfer(context.Background(), env.sourceAbs, env.sourceRel, env.final(), env.opts)
	if !errors.Is(err, ErrHardlinkUnsupported) {
		t.Fatalf("Transfer = %v, want ErrHardlinkUnsupported", err)
	}
	if _, err := os.Stat(env.ckptPath); err != nil {
		t.Errorf("checkpoint must be preserved: %v", err)
	}
	// No temp may have been created in the remote dir.
	entries, err := os.ReadDir(env.remoteDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		t.Errorf("unexpected remote entry after failed capability probe: %s", e.Name())
	}
}

// TestTransferCrashWindowRecovery: after a completed publish (state not
// yet retired), a re-Resume recognizes the same-inode final and recovers
// without copying a single chunk.
func TestTransferCrashWindowRecovery(t *testing.T) {
	env := newTransferEnv(t, 16<<10, 8<<10)
	env.opts.CheckpointPath = env.ckptPath
	result, err := Transfer(context.Background(), env.sourceAbs, env.sourceRel, env.final(), env.opts)
	if err != nil {
		t.Fatalf("Transfer: %v", err)
	}
	// The crash window: checkpoint exists, temp+final published, state not
	// retired. Resume must recover without copying.
	copies := 0
	env.opts.OnChunkComplete = func(done, total int) { copies++ }
	res2, err := Resume(context.Background(), env.sourceAbs, env.sourceRel, env.ckptPath, env.final(), env.opts)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if !res2.Recovered {
		t.Errorf("result = %+v, want a recovered publish", res2)
	}
	if copies != 0 {
		t.Errorf("resume re-copied %d chunks after a completed publish", copies)
	}
	if result.TempPath != res2.TempPath {
		t.Errorf("temp path mismatch: %q vs %q", result.TempPath, res2.TempPath)
	}
}

// TestTransferPreEpochNanosecondMtime: a pre-epoch nanosecond mtime must
// survive the decimal timestamp round trip exactly.
func TestTransferPreEpochNanosecondMtime(t *testing.T) {
	env := newTransferEnv(t, 8<<10, 8<<10)
	env.opts.CheckpointPath = env.ckptPath
	preEpoch := time.Date(1950, 3, 4, 5, 6, 7, 890123456, time.UTC)
	if err := os.Chtimes(env.sourceAbs, preEpoch, preEpoch); err != nil {
		t.Fatal(err)
	}
	if _, err := Transfer(context.Background(), env.sourceAbs, env.sourceRel, env.final(), env.opts); err != nil {
		t.Fatalf("Transfer: %v", err)
	}
	fi, err := os.Lstat(env.final())
	if err != nil {
		t.Fatal(err)
	}
	if !fi.ModTime().Equal(preEpoch) {
		t.Errorf("final mtime = %v, want %v (pre-epoch nanosecond exact)", fi.ModTime(), preEpoch)
	}
}

// TestTransferShellInjectionFilenames: hostile filenames flow through the
// touch/test command construction as pure data.
func TestTransferShellInjectionFilenames(t *testing.T) {
	if !realServerAvailable() {
		t.Skipf("real sftp-server %s not available", sftpServerBin)
	}
	base := t.TempDir()
	sentinel := filepath.Join(base, "sentinel-must-survive")
	if err := os.WriteFile(sentinel, []byte("safe"), 0o644); err != nil {
		t.Fatal(err)
	}
	names := []string{
		"with space.bin",
		"uni-λ-文件.bin",
		"with\nnewline.bin",
		"with'quote.bin",
		"-leading-dash.bin",
		"semi;colon $(echo hi) `echo hi`.bin",
	}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			src := filepath.Join(base, "src-"+name)
			if err := os.WriteFile(src, patternBytes(4096), 0o644); err != nil {
				t.Fatal(err)
			}
			remoteDir := filepath.Join(base, "remote-"+name)
			if err := os.MkdirAll(remoteDir, 0o755); err != nil {
				t.Fatal(err)
			}
			final := path.Join(remoteDir, name)
			cfg := helperConfig(t, t.TempDir(), envRealServer+"=1")
			opts := TransferOptions{
				SFTP:           cfg,
				Streams:        1,
				Workers:        1,
				ChunkSize:      2048,
				CheckpointPath: filepath.Join(base, "state-"+name, "ckpt.json"),
			}
			if _, err := Transfer(context.Background(), src, "src-"+name, final, opts); err != nil {
				t.Fatalf("Transfer: %v", err)
			}
			got, err := os.ReadFile(final)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != string(patternBytes(4096)) {
				t.Error("content mismatch for hostile filename")
			}
			data, err := os.ReadFile(sentinel)
			if err != nil || string(data) != "safe" {
				t.Errorf("sentinel harmed: %v %q", err, data)
			}
		})
	}
}
