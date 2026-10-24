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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dotfrankruan/CatSync/internal/process"
	"github.com/dotfrankruan/CatSync/internal/testutil/mockexec"
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

func TestLocalRealRsyncMirrorDeletesAfterTransferAndPreservesTree(t *testing.T) {
	prog, err := exec.LookPath("rsync")
	if err != nil {
		t.Skip("rsync not installed; skipping real local mirror test")
	}
	src := t.TempDir()
	dst := t.TempDir()
	writeSourceFile(t, src, "kept/file.txt", []byte("new content"))
	writeSourceFile(t, src, "checksum.txt", []byte("ABCD"))
	if err := os.Mkdir(filepath.Join(src, "empty"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("kept/file.txt", filepath.Join(src, "link")); err != nil {
		t.Fatal(err)
	}
	writeSourceFile(t, dst, "stale.txt", []byte("delete me"))
	writeSourceFile(t, dst, DefaultPartialDir+"/stale.part", []byte("delete internal residue"))
	writeSourceFile(t, dst, "kept/file.txt", []byte("old"))
	// Same size and mtime but different content: only --checksum can detect
	// and repair this case.
	writeSourceFile(t, dst, "checksum.txt", []byte("WXYZ"))

	cfg := MirrorConfig{Program: prog, SourceDir: src, Dest: testDest()}
	argv, err := cfg.Command()
	if err != nil {
		t.Fatal(err)
	}
	argv[len(argv)-1] = dst + "/"
	var stdout, stderr bytes.Buffer
	if err := process.Run(context.Background(), process.Spec{
		Name: argv[0], Args: argv[1:], Stdout: &stdout, Stderr: &stderr,
	}); err != nil {
		t.Fatalf("local mirror: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
	if _, err := os.Lstat(filepath.Join(dst, "stale.txt")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("destination-only file remains: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(dst, DefaultPartialDir)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("protected partial residue remains: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dst, "kept/file.txt"))
	if err != nil || string(got) != "new content" {
		t.Fatalf("mirrored file = %q, %v", got, err)
	}
	if fi, err := os.Stat(filepath.Join(dst, "empty")); err != nil || !fi.IsDir() {
		t.Fatalf("empty directory not mirrored: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(dst, "checksum.txt")); err != nil || string(got) != "ABCD" {
		t.Fatalf("checksum-only mismatch was not repaired: %q, %v", got, err)
	}
	if target, err := os.Readlink(filepath.Join(dst, "link")); err != nil || target != "kept/file.txt" {
		t.Fatalf("symlink = %q, %v", target, err)
	}
}

// runLocalRsync executes cfg against a local destination directory: the
// argv is built exactly as the backend builds it and only the remote
// destination operand is swapped for the local path. It returns the argv
// and stdin payload for assertions.
func runLocalRsync(t *testing.T, cfg Config, dst string) ([]string, []byte) {
	t.Helper()
	argv, stdin, err := cfg.Command()
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	argv[len(argv)-1] = dst + "/"
	var stdout, stderr bytes.Buffer
	if err := process.Run(context.Background(), process.Spec{
		Name:   argv[0],
		Args:   argv[1:],
		Stdin:  bytes.NewReader(stdin),
		Stdout: &stdout,
		Stderr: &stderr,
	}); err != nil {
		t.Fatalf("local rsync: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
	return argv, stdin
}

// mkdirDestParents pre-creates the destination parent directories for rel
// paths, the way CatSync's batched remote mkdir does before launching
// parallel batches.
func mkdirDestParents(t *testing.T, dst string, rels []string) {
	t.Helper()
	for _, rel := range rels {
		if slash := strings.LastIndexByte(rel, '/'); slash >= 0 {
			p := filepath.Join(dst, filepath.FromSlash(rel[:slash]))
			if err := os.MkdirAll(p, 0o755); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// TestLocalRealRsyncNoImpliedDirsNested proves the parallel-rsync stage's
// command shape with a real rsync and a local destination: pre-created
// parent directories plus --relative --no-implied-dirs copy nested files
// correctly (including empty files and names with spaces, Unicode, newlines,
// quotes, and leading dashes), preserve file mtimes, never touch shared
// parent directory metadata, and keep the deletion/whole-file/inplace flags
// out of argv.
func TestLocalRealRsyncNoImpliedDirsNested(t *testing.T) {
	prog, err := exec.LookPath("rsync")
	if err != nil {
		t.Skip("rsync not installed; skipping real local transfer test")
	}
	src := t.TempDir()
	dst := t.TempDir()
	paths := []string{
		"root.txt",
		"dir with space/nested/file one.txt",
		"ünïcødé/文件.txt",
		"new\nline/file.txt",
		"quotes/quote'and\"double.txt",
		"-leading-dash/-file.txt",
		"empty/zero.bin",
	}
	for _, rel := range paths {
		content := []byte("content of " + rel)
		if rel == "empty/zero.bin" {
			content = nil
		}
		writeSourceFile(t, src, rel, content)
	}
	// Source directory attributes differ from the destination's so an
	// implied-dir update would be visible.
	srcDirTime := time.Date(2021, 5, 5, 5, 5, 5, 0, time.UTC)
	for _, rel := range paths {
		if slash := strings.LastIndexByte(rel, '/'); slash >= 0 {
			if err := os.Chtimes(filepath.Join(src, filepath.FromSlash(rel[:slash])), srcDirTime, srcDirTime); err != nil {
				t.Fatal(err)
			}
		}
	}

	mkdirDestParents(t, dst, paths)
	// Give every pre-created destination parent a distinct mtime so an
	// implied-directory attribute update would be unmistakable.
	destDirTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	parents := map[string]bool{}
	for _, rel := range paths {
		if slash := strings.LastIndexByte(rel, '/'); slash >= 0 {
			parents[filepath.Join(dst, filepath.FromSlash(rel[:slash]))] = true
		}
	}
	for p := range parents {
		if err := os.Chtimes(p, destDirTime, destDirTime); err != nil {
			t.Fatal(err)
		}
	}

	cfg := validConfig()
	cfg.Program = prog
	cfg.SourceDir = src
	cfg.Paths = paths
	cfg.NoImpliedDirs = true
	argv, stdin := runLocalRsync(t, cfg, dst)

	if want := strings.Join(paths, "\x00") + "\x00"; string(stdin) != want {
		t.Errorf("stdin = %q, want the NUL-delimited path list", stdin)
	}
	foundFlag := false
	for _, a := range argv {
		if a == "--no-implied-dirs" {
			foundFlag = true
		}
		for _, bad := range forbiddenFlags {
			if a == bad {
				t.Errorf("argv contains forbidden flag %q: %#v", bad, argv)
			}
		}
	}
	if !foundFlag {
		t.Error("argv missing --no-implied-dirs")
	}
	for _, rel := range paths {
		dp := filepath.Join(dst, filepath.FromSlash(rel))
		got, rerr := os.ReadFile(dp)
		if rerr != nil {
			t.Errorf("read destination %q: %v", rel, rerr)
			continue
		}
		want := "content of " + rel
		if rel == "empty/zero.bin" {
			want = ""
		}
		if string(got) != want {
			t.Errorf("destination %q = %q, want %q", rel, got, want)
		}
		si, serr := os.Stat(filepath.Join(src, filepath.FromSlash(rel)))
		di, derr := os.Stat(dp)
		if serr == nil && derr == nil && !si.ModTime().Equal(di.ModTime()) {
			t.Errorf("destination %q mtime = %v, want archive-preserved %v", rel, di.ModTime(), si.ModTime())
		}
	}
	// The decisive property: source directory attributes are NOT applied to
	// the implied destination directories. (Their mtime may still change
	// because files are created inside them; only the source-attribute
	// update is the concurrency hazard.)
	for p := range parents {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatalf("stat parent %s: %v", p, err)
		}
		if fi.ModTime().Equal(srcDirTime) {
			t.Errorf("parent %s mtime = source directory time %v: implied-dir attributes were applied", p, srcDirTime)
		}
	}
	// Control: without --no-implied-dirs the same transfer DOES copy the
	// source directory mtime onto the destination parent, which is exactly
	// the shared-parent race the flag avoids for parallel batches.
	controlDst := t.TempDir()
	mkdirDestParents(t, controlDst, paths)
	for _, rel := range paths {
		if slash := strings.LastIndexByte(rel, '/'); slash >= 0 {
			p := filepath.Join(controlDst, filepath.FromSlash(rel[:slash]))
			if err := os.Chtimes(p, destDirTime, destDirTime); err != nil {
				t.Fatal(err)
			}
		}
	}
	control := validConfig()
	control.Program = prog
	control.SourceDir = src
	control.Paths = paths
	runLocalRsync(t, control, controlDst)
	checkedControl := false
	for _, rel := range paths {
		slash := strings.LastIndexByte(rel, '/')
		if slash < 0 {
			continue
		}
		p := filepath.Join(controlDst, filepath.FromSlash(rel[:slash]))
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatalf("stat control parent %s: %v", p, err)
		}
		if fi.ModTime().Equal(srcDirTime) {
			checkedControl = true
		}
	}
	if !checkedControl {
		t.Error("control run without --no-implied-dirs did not apply source directory times; test assumption broken")
	}
	// Successful runs leave no partial directories behind.
	if _, err := os.Stat(filepath.Join(dst, DefaultPartialDir)); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("partial dir remains after successful transfer: %v", err)
	}
}

// TestLocalRealRsyncConcurrentBatchesSharedParents runs two batches
// concurrently against the same local destination, sharing nested parent
// directories: with pre-created parents and --no-implied-dirs both must
// succeed without corrupting each other or rewriting parent metadata.
func TestLocalRealRsyncConcurrentBatchesSharedParents(t *testing.T) {
	prog, err := exec.LookPath("rsync")
	if err != nil {
		t.Skip("rsync not installed; skipping real local concurrent transfer test")
	}
	src := t.TempDir()
	dst := t.TempDir()
	paths := []string{
		"shared/deep/one.txt",
		"shared/deep/two.txt",
		"shared/deep/ünïcødé three.txt",
		"shared/other/four.txt",
	}
	for _, rel := range paths {
		writeSourceFile(t, src, rel, []byte("payload "+rel))
	}
	mkdirDestParents(t, dst, paths)
	destDirTime := time.Date(2020, 2, 2, 2, 2, 2, 0, time.UTC)
	srcDirTime := time.Date(2021, 6, 6, 6, 6, 6, 0, time.UTC)
	for _, dir := range []string{"shared", "shared/deep", "shared/other"} {
		if err := os.Chtimes(filepath.Join(dst, filepath.FromSlash(dir)), destDirTime, destDirTime); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(filepath.Join(src, filepath.FromSlash(dir)), srcDirTime, srcDirTime); err != nil {
			t.Fatal(err)
		}
	}

	// Two disjoint batches sharing the "shared/deep" parent.
	batches := [][]string{
		{"shared/deep/one.txt", "shared/deep/ünïcødé three.txt"},
		{"shared/deep/two.txt", "shared/other/four.txt"},
	}
	errs := make(chan error, len(batches))
	var wg sync.WaitGroup
	for _, batch := range batches {
		wg.Add(1)
		go func(batch []string) {
			defer wg.Done()
			cfg := validConfig()
			cfg.Program = prog
			cfg.SourceDir = src
			cfg.Paths = batch
			cfg.NoImpliedDirs = true
			argv, stdin, err := cfg.Command()
			if err != nil {
				errs <- err
				return
			}
			argv[len(argv)-1] = dst + "/"
			var stdout, stderr bytes.Buffer
			errs <- process.Run(context.Background(), process.Spec{
				Name:   argv[0],
				Args:   argv[1:],
				Stdin:  bytes.NewReader(stdin),
				Stdout: &stdout,
				Stderr: &stderr,
			})
		}(batch)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent local rsync: %v", err)
		}
	}
	for _, rel := range paths {
		got, rerr := os.ReadFile(filepath.Join(dst, filepath.FromSlash(rel)))
		if rerr != nil {
			t.Errorf("read destination %q: %v", rel, rerr)
			continue
		}
		if want := "payload " + rel; string(got) != want {
			t.Errorf("destination %q = %q, want %q", rel, got, want)
		}
	}
	for _, dir := range []string{"shared", "shared/deep", "shared/other"} {
		fi, err := os.Stat(filepath.Join(dst, filepath.FromSlash(dir)))
		if err != nil {
			t.Fatalf("stat %s: %v", dir, err)
		}
		if fi.ModTime().Equal(srcDirTime) {
			t.Errorf("shared parent %s mtime = source directory time %v: implied-dir attribute race", dir, srcDirTime)
		}
	}
}

// TestLocalRealRsyncSharedRelativePartialDirIsPerFile proves the shared
// relative --partial-dir cannot mix batches: a stale partial file for one
// path does not disturb a concurrent/neighboring batch transferring a
// different file in the same destination parent.
func TestLocalRealRsyncSharedRelativePartialDirIsPerFile(t *testing.T) {
	prog, err := exec.LookPath("rsync")
	if err != nil {
		t.Skip("rsync not installed; skipping real local partial test")
	}
	src := t.TempDir()
	dst := t.TempDir()
	writeSourceFile(t, src, "shared/a.txt", []byte("hello from a"))
	writeSourceFile(t, src, "shared/b.txt", []byte("hello from b"))
	mkdirDestParents(t, dst, []string{"shared/a.txt", "shared/b.txt"})

	partialDir := filepath.Join(dst, "shared", DefaultPartialDir)
	if err := os.MkdirAll(partialDir, 0o755); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(partialDir, "a.txt")
	if err := os.WriteFile(stale, []byte("stale partial bytes"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := validConfig()
	cfg.Program = prog
	cfg.SourceDir = src
	cfg.Paths = []string{"shared/b.txt"}
	cfg.NoImpliedDirs = true
	runLocalRsync(t, cfg, dst)

	if got, rerr := os.ReadFile(filepath.Join(dst, "shared", "b.txt")); rerr != nil || string(got) != "hello from b" {
		t.Fatalf("b.txt = %q, %v", got, rerr)
	}
	// The stale partial for the other file is untouched by the b batch.
	if got, rerr := os.ReadFile(stale); rerr != nil || string(got) != "stale partial bytes" {
		t.Errorf("stale partial for a.txt was disturbed: %q, %v", got, rerr)
	}
	// Transferring a.txt then consumes/replaces its own partial and leaves
	// the file correct.
	cfg.Paths = []string{"shared/a.txt"}
	runLocalRsync(t, cfg, dst)
	if got, rerr := os.ReadFile(filepath.Join(dst, "shared", "a.txt")); rerr != nil || string(got) != "hello from a" {
		t.Fatalf("a.txt = %q, %v", got, rerr)
	}
}
