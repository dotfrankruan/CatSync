//go:build unix

package rsync

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/frankruan/catsync/internal/process"
	"github.com/frankruan/catsync/internal/testutil/mockexec"
)

func TestMain(m *testing.M) {
	if mockexec.HelperActive() {
		os.Exit(mockexec.RunHelper())
	}
	os.Exit(m.Run())
}

// mockConfig returns a Config executing the test binary as a mock rsync
// that records into dir, configured by cfg.
func mockConfig(t *testing.T, dir string, cfg map[string]string) Config {
	t.Helper()
	prog, err := mockexec.Program()
	if err != nil {
		t.Fatal(err)
	}
	c := validConfig()
	c.Program = prog
	c.Env = mockexec.Env(dir, cfg)
	return c
}

func TestRunTransmitsArgvAndStdin(t *testing.T) {
	dir := t.TempDir()
	cfg := mockConfig(t, dir, nil)
	cfg.Paths = []string{
		"a/b.txt",
		"dir with space/file.txt",
		"new\nline.txt",
		"ünïcødé-文件.txt",
		"-leading-dash.txt",
	}
	if err := cfg.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	rec, err := mockexec.ReadRecord(dir)
	if err != nil {
		t.Fatal(err)
	}
	wantArgv, wantStdin, err := cfg.Command()
	if err != nil {
		t.Fatal(err)
	}
	if !equalStrings(rec.Args, wantArgv[1:]) {
		t.Errorf("argv mismatch:\n got %#v\nwant %#v", rec.Args, wantArgv[1:])
	}
	if string(rec.Stdin) != string(wantStdin) {
		t.Errorf("stdin = %q, want %q", rec.Stdin, wantStdin)
	}
}

func TestRunFailureExitPropagation(t *testing.T) {
	dir := t.TempDir()
	cfg := mockConfig(t, dir, map[string]string{mockexec.EnvExit: "23"})
	err := cfg.Run(context.Background())
	if err == nil {
		t.Fatal("Run with exit 23 = nil, want error")
	}
	var exitErr *process.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("error %T = %v, want *process.ExitError", err, err)
	}
	if exitErr.Code != 23 {
		t.Errorf("exit code = %d, want 23", exitErr.Code)
	}
}

func TestRunCancellation(t *testing.T) {
	dir := t.TempDir()
	cfg := mockConfig(t, dir, map[string]string{mockexec.EnvSleepMS: "30000"})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	err := cfg.Run(ctx)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Run with cancelled context = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Errorf("cancellation took %s, want prompt termination", elapsed)
	}
	rec, rerr := mockexec.ReadRecord(dir)
	if rerr != nil {
		t.Fatal(rerr)
	}
	assertGone(t, rec.PID)
}

func TestRunEmptyPathSetDoesNotExecute(t *testing.T) {
	// A nonexistent program proves no exec happens for an empty set.
	cfg := validConfig()
	cfg.Program = "/nonexistent/rsync"
	cfg.Paths = nil
	if err := cfg.Run(context.Background()); err != nil {
		t.Fatalf("Run with empty path set = %v, want nil", err)
	}
}

func TestRunValidationErrorDoesNotExecute(t *testing.T) {
	dir := t.TempDir()
	cfg := mockConfig(t, dir, nil)
	cfg.Paths = []string{"../escape"}
	if err := cfg.Run(context.Background()); err == nil {
		t.Fatal("Run with unsafe path = nil, want error")
	}
	if _, err := mockexec.ReadRecord(dir); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("mock executed despite invalid config (record err = %v)", err)
	}
}

// equalStrings compares two string slices element-wise.
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// assertGone fails when pid still refers to a live or zombie process.
func assertGone(t *testing.T, pid int) {
	t.Helper()
	if err := mockexec.ProcessAlive(pid); err == nil {
		t.Errorf("process %d still exists (orphaned or zombie)", pid)
	}
}

// writeSourceFile creates rel under root with content and a deterministic
// modification time.
func writeSourceFile(t *testing.T, root, rel string, content []byte) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, content, 0o640); err != nil {
		t.Fatal(err)
	}
	mtime := time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC)
	if err := os.Chtimes(p, mtime, mtime); err != nil {
		t.Fatal(err)
	}
}

// TestLocalRealRsyncTransfer runs the built command with a real rsync
// binary between two local directories (no SSH): the exact argv and
// NUL-delimited stdin the backend produces must transfer the tree,
// including names with spaces, Unicode, newlines, quotes, and a leading
// dash, preserving contents and modification times. A second run after a
// local modification must update the destination (delta behavior).
func TestLocalRealRsyncTransfer(t *testing.T) {
	prog, err := exec.LookPath("rsync")
	if err != nil {
		t.Skip("rsync not installed; skipping real local transfer test")
	}
	src := t.TempDir()
	dst := t.TempDir()
	paths := []string{
		"plain.txt",
		"dir with space/file.txt",
		"new\nline.txt",
		"ünïcødé-文件.txt",
		"-leading-dash.txt",
		"quote'and\"double.txt",
	}
	for _, rel := range paths {
		writeSourceFile(t, src, rel, []byte("content of "+rel))
	}

	cfg := validConfig()
	cfg.Program = prog
	cfg.SourceDir = src
	cfg.Paths = paths

	runLocal := func() {
		t.Helper()
		argv, stdin, err := cfg.Command()
		if err != nil {
			t.Fatalf("Command: %v", err)
		}
		// Swap the remote destination operand for a local directory: the
		// rest of the command line stays exactly as built.
		argv[len(argv)-1] = dst + "/"
		var stdout, stderr bytes.Buffer
		if err := process.Run(context.Background(), process.Spec{
			Name:   argv[0],
			Args:   argv[1:],
			Stdin:  bytes.NewReader(stdin),
			Stdout: &stdout,
			Stderr: &stderr,
		}); err != nil {
			t.Fatalf("local rsync: %v (stderr %q)", err, stderr.String())
		}
	}

	runLocal()
	for _, rel := range paths {
		got, err := os.ReadFile(filepath.Join(dst, filepath.FromSlash(rel)))
		if err != nil {
			t.Errorf("read destination %q: %v", rel, err)
			continue
		}
		if want := "content of " + rel; string(got) != want {
			t.Errorf("destination %q = %q, want %q", rel, got, want)
		}
		si, _ := os.Stat(filepath.Join(src, filepath.FromSlash(rel)))
		di, _ := os.Stat(filepath.Join(dst, filepath.FromSlash(rel)))
		if si != nil && di != nil && !si.ModTime().Equal(di.ModTime()) {
			t.Errorf("destination %q mtime = %v, want archive-preserved %v", rel, di.ModTime(), si.ModTime())
		}
	}
	// A successful run leaves no partial files behind.
	if _, err := os.Stat(filepath.Join(dst, DefaultPartialDir)); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("partial dir remains after successful transfer: %v", err)
	}

	// Delta update: modify one file in place; a rerun must refresh it.
	writeSourceFile(t, src, "plain.txt", []byte("content of plain.txt -- updated, longer"))
	runLocal()
	got, err := os.ReadFile(filepath.Join(dst, "plain.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if want := "content of plain.txt -- updated, longer"; string(got) != want {
		t.Errorf("after delta update destination = %q, want %q", got, want)
	}
}
