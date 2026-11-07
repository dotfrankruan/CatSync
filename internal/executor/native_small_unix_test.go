//go:build unix

package executor

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dotfrankruan/CatSync/internal/state"
	"github.com/dotfrankruan/CatSync/internal/target"
	"github.com/dotfrankruan/CatSync/internal/testutil/mockexec"
)

// noRsyncConfig returns the native harness config with rsync made unavailable,
// the situation the native small-file stage exists for.
func noRsyncConfig(h *harness) Config {
	cfg := h.configNative()
	cfg.Programs.Rsync = filepath.Join("/nonexistent-catsync-test", "rsync")
	return cfg
}

// TestNativeSmallUploadWithoutRsync: a tree of new small files is copied over
// native SFTP when rsync is not installed, with exact metadata and no rsync
// process.
func TestNativeSmallUploadWithoutRsync(t *testing.T) {
	if !mockexec.SFTPServerAvailable() {
		t.Skip("real sftp-server is unavailable")
	}
	h := newHarness(t)
	h.writeSrc("a.txt", 100)
	h.writeSrc("nested/b.bin", 700)
	h.writeSrc("nested/deeper/c.dat", 900)
	post := h.writeManifest("post", manifestContent(
		h.newFileRecord("a.txt"), h.newFileRecord("nested/b.bin"), h.newFileRecord("nested/deeper/c.dat")))
	h.scriptSSHFreshDest(post)

	cfg := noRsyncConfig(h)
	cfg.Verify = state.VerifySize
	run, err := Sync(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Sync without rsync: %v", err)
	}
	if run.Status != state.StatusCompleted {
		t.Fatalf("run = %s (%q), want completed", run.Status, run.Error)
	}
	for _, task := range run.Tasks {
		if task.Backend != state.BackendSFTP {
			t.Errorf("task %s backend = %s, want sftp", task.Path, task.Backend)
		}
		if task.Status != state.StatusCompleted {
			t.Errorf("task %s = %s (%q)", task.Path, task.Status, task.Error)
		}
	}
	for rel, size := range map[string]int{"a.txt": 100, "nested/b.bin": 700, "nested/deeper/c.dat": 900} {
		got, rerr := os.ReadFile(filepath.Join(h.remoteDir, filepath.FromSlash(rel)))
		if rerr != nil {
			t.Fatalf("%s missing: %v", rel, rerr)
		}
		if len(got) != size {
			t.Errorf("%s size = %d, want %d", rel, len(got), size)
		}
		want, _ := os.ReadFile(filepath.Join(h.src, filepath.FromSlash(rel)))
		if !bytes.Equal(got, want) {
			t.Errorf("%s content mismatch", rel)
		}
		gi, _ := os.Stat(filepath.Join(h.remoteDir, filepath.FromSlash(rel)))
		si, _ := os.Stat(filepath.Join(h.src, filepath.FromSlash(rel)))
		if !gi.ModTime().Equal(si.ModTime()) {
			t.Errorf("%s mtime = %v, want %v", rel, gi.ModTime(), si.ModTime())
		}
	}
	if got := len(toolInvocations(h.invocations(), "rsync")); got != 0 {
		t.Errorf("rsync invocations = %d, want 0", got)
	}
}

// TestNativeSmallDownloadWithoutRsync: the same for a remote-to-local copy.
func TestNativeSmallDownloadWithoutRsync(t *testing.T) {
	if !mockexec.SFTPServerAvailable() {
		t.Skip("real sftp-server is unavailable")
	}
	h := newHarness(t)
	recs := ""
	names := []string{"one.txt", "sub/two.bin"}
	for _, name := range names {
		p := filepath.Join(h.remoteDir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, bytes.Repeat([]byte(name), 200), 0o640); err != nil {
			t.Fatal(err)
		}
		mt := time.Unix(1_700_900_000, 123456789)
		if err := os.Chtimes(p, mt, mt); err != nil {
			t.Fatal(err)
		}
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		recs += sourceManifestRecord(name, fi.Size(), fi.ModTime())
	}
	manifest := h.writeManifest("remote-source", "D\x00"+recs)
	h.writeScript("ssh", "match=if [ -d\nstdout_file="+manifest+"\n")

	cfg := h.configNative()
	cfg.Programs.Rsync = filepath.Join("/nonexistent-catsync-test", "rsync")
	cfg.Direction = target.RemoteToLocal
	cfg.SourceDir = filepath.Join(t.TempDir(), "download")
	cfg.Dest.Path = h.remoteDir
	cfg.Backend = state.BackendAuto
	cfg.Verify = state.VerifyNone

	run, err := Sync(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Sync download without rsync: %v", err)
	}
	if run.Status != state.StatusCompleted {
		t.Fatalf("run = %s (%q), want completed", run.Status, run.Error)
	}
	for _, name := range names {
		got, rerr := os.ReadFile(filepath.Join(cfg.SourceDir, filepath.FromSlash(name)))
		if rerr != nil {
			t.Fatalf("%s missing: %v", name, rerr)
		}
		if len(got) != 200*len(name) {
			t.Errorf("%s size = %d", name, len(got))
		}
		fi, _ := os.Stat(filepath.Join(cfg.SourceDir, filepath.FromSlash(name)))
		if fi.Mode().Perm() != 0o640 {
			t.Errorf("%s mode = %o, want 640", name, fi.Mode().Perm())
		}
		if want := time.Unix(1_700_900_000, 123456789); !fi.ModTime().Equal(want) {
			t.Errorf("%s mtime = %v, want %v (nanosecond exact)", name, fi.ModTime(), want)
		}
	}
	if got := len(toolInvocations(h.invocations(), "rsync")); got != 0 {
		t.Errorf("rsync invocations = %d, want 0", got)
	}
}

// TestNativeSmallResumeWithoutRsync: an interrupted small transfer resumes from
// its checkpoint when the same job runs again.
func TestNativeSmallResumeWithoutRsync(t *testing.T) {
	if !mockexec.SFTPServerAvailable() {
		t.Skip("real sftp-server is unavailable")
	}
	h := newHarness(t)
	h.writeSrc("resume.txt", 800)
	// The destination stays empty: the resume must transfer, not reconcile.
	h.writeScript("ssh", "match=if [ -d\nexit=42\n")

	cfg := noRsyncConfig(h)
	cfg.Verify = state.VerifyNone
	// The first transfer session fails, leaving a checkpoint.
	cfg.Env = h.nativeEnv("MOCKEXEC_SFTP_FAILNTH=1")
	first, ferr := Sync(context.Background(), cfg)
	if ferr == nil {
		t.Fatal("first Sync = nil error, want the failed session")
	}
	if !first.Resumable() {
		t.Fatalf("first run = %s, want resumable", first.Status)
	}
	if first.Tasks[0].Backend != state.BackendSFTP {
		t.Fatalf("backend = %s, want sftp", first.Tasks[0].Backend)
	}

	cfg.Env = h.nativeEnv()
	second, err := Sync(context.Background(), cfg)
	if err != nil {
		t.Fatalf("resume Sync: %v", err)
	}
	if second.ID != first.ID {
		t.Errorf("run = %s, want the resumed %s", second.ID, first.ID)
	}
	if second.Status != state.StatusCompleted {
		t.Fatalf("resumed run = %s (%q)", second.Status, second.Error)
	}
	got, err := os.ReadFile(filepath.Join(h.remoteDir, "resume.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 800 {
		t.Errorf("published size = %d, want 800", len(got))
	}
}

// TestNativeSmallFallsBackToRsync: without the hardlink extension new small
// files cannot be published natively; with rsync available they are handed to
// the rsync batch stage instead of failing.
func TestNativeSmallFallsBackToRsync(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("a.txt", 100)
	h.writeSrc("b.txt", 200)
	h.scriptSSHFreshDest(h.writeManifest("post", manifestContent()))
	h.writeScript("rsync", "exit=0")

	cfg := h.configNative()
	cfg.Backend = state.BackendAuto
	cfg.Verify = state.VerifyNone
	cfg.Env = h.nativeEnv(mockexec.EnvSFTPNoExt + "=1")

	run, err := Sync(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Sync with fallback: %v", err)
	}
	if run.Status != state.StatusCompleted {
		t.Fatalf("run = %s (%q), want completed", run.Status, run.Error)
	}
	for _, task := range run.Tasks {
		if task.Backend != state.BackendFPSync {
			t.Errorf("task %s backend = %s, want rerouted fpsync", task.Path, task.Backend)
		}
	}
	if got := len(toolInvocations(h.invocations(), "rsync")); got == 0 {
		t.Error("rsync never ran after the hardlink fallback")
	}
}

// TestNativeSmallFailsClosedWithoutHardlinkOrRsync: with neither the extension
// nor rsync, the tasks fail with an actionable message and nothing is published.
func TestNativeSmallFailsClosedWithoutHardlinkOrRsync(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("a.txt", 100)
	h.scriptSSHFreshDest(h.writeManifest("post", manifestContent()))

	cfg := noRsyncConfig(h)
	cfg.Verify = state.VerifyNone
	cfg.Env = h.nativeEnv(mockexec.EnvSFTPNoExt + "=1")

	run, ferr := Sync(context.Background(), cfg)
	if ferr == nil {
		t.Fatal("Sync = nil error, want a fail-closed failure")
	}
	for _, task := range run.Tasks {
		if task.Status != state.StatusFailed {
			t.Errorf("task %s = %s, want failed", task.Path, task.Status)
		}
		if !strings.Contains(task.Error, "rsync is not installed") {
			t.Errorf("task error = %q, want the actionable message", task.Error)
		}
	}
	if _, err := os.Lstat(filepath.Join(h.remoteDir, "a.txt")); !os.IsNotExist(err) {
		t.Errorf("a final was published despite the failure: %v", err)
	}
}

// TestPlannerRoutesNewSmallFilesToSFTPWithoutRsync: planning decides the
// backend from the available tools.
func TestPlannerRoutesNewSmallFilesToSFTPWithoutRsync(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("small.txt", 100)
	h.writeSrc("big.bin", 4096)
	h.scriptSSHFreshDest(h.writeManifest("post", manifestContent()))

	cfg := noRsyncConfig(h)
	cfg.Verify = state.VerifyNone
	run, err := Sync(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	for _, task := range run.Tasks {
		if task.Path == "small.txt" && task.Backend != state.BackendSFTP {
			t.Errorf("small new file backend = %s, want sftp", task.Backend)
		}
		if task.Path == "big.bin" && task.Backend != state.BackendMSCP {
			t.Errorf("large new file backend = %s, want mscp", task.Backend)
		}
	}
}
