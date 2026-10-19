//go:build unix

package mscp

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
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

// mockConfig returns a Config executing the test binary as a mock mscp that
// records into dir, configured by mockCfg. WorkDir is a fresh temp dir and
// the checkpoint lives in another, mimicking the run layout.
func mockConfig(t *testing.T, dir string, mockCfg map[string]string) Config {
	t.Helper()
	prog, err := mockexec.Program()
	if err != nil {
		t.Fatal(err)
	}
	c := validConfig()
	c.Program = prog
	c.WorkDir = t.TempDir()
	c.Checkpoint = filepath.Join(t.TempDir(), "checkpoint.bin")
	c.Env = mockexec.Env(dir, mockCfg)
	return c
}

func TestRunArgvAndWorkingDirectory(t *testing.T) {
	dir := t.TempDir()
	cfg := mockConfig(t, dir, nil)
	cfg.Path = "dir with space/ünïcødé-文件.bin"
	if err := cfg.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	rec, err := mockexec.ReadRecord(dir)
	if err != nil {
		t.Fatal(err)
	}
	argv, err := cfg.Command()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rec.Args, argv[1:]) {
		t.Errorf("argv mismatch:\n got %#v\nwant %#v", rec.Args, argv[1:])
	}
	// The kernel may resolve symlinks in the working directory (macOS
	// /var -> /private/var), so compare resolved paths.
	wantDir, err := filepath.EvalSymlinks(cfg.WorkDir)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Dir != wantDir {
		t.Errorf("cwd = %q, want %q (the original source working directory)", rec.Dir, wantDir)
	}
}

func TestRunLeadingDashPath(t *testing.T) {
	dir := t.TempDir()
	cfg := mockConfig(t, dir, nil)
	cfg.Path = "-big.bin"
	if err := cfg.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	rec, err := mockexec.ReadRecord(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := rec.Args[len(rec.Args)-2]; got != "./-big.bin" {
		t.Errorf("source operand = %q, want ./-big.bin", got)
	}
}

func TestRunWritesCheckpoint(t *testing.T) {
	dir := t.TempDir()
	cfg := mockConfig(t, dir, nil)
	cfg.Env = mockexec.Env(dir, map[string]string{mockexec.EnvTouch: cfg.Checkpoint})
	if err := cfg.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, err := os.Stat(cfg.Checkpoint); err != nil {
		t.Errorf("checkpoint not written: %v", err)
	}
}

func TestResumePreservesCheckpoint(t *testing.T) {
	dir := t.TempDir()
	cfg := mockConfig(t, dir, nil)
	content := []byte("mscp checkpoint state\n")
	if err := os.WriteFile(cfg.Checkpoint, content, 0600); err != nil {
		t.Fatal(err)
	}
	if err := cfg.Resume(context.Background()); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	// The checkpoint is preserved for CatSync's verification gate.
	got, err := os.ReadFile(cfg.Checkpoint)
	if err != nil {
		t.Fatalf("checkpoint after resume: %v", err)
	}
	if !reflect.DeepEqual(got, content) {
		t.Errorf("checkpoint = %q, want unchanged %q", got, content)
	}
	rec, err := mockexec.ReadRecord(dir)
	if err != nil {
		t.Fatal(err)
	}
	argv, err := cfg.ResumeCommand()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rec.Args, argv[1:]) {
		t.Errorf("resume argv mismatch:\n got %#v\nwant %#v", rec.Args, argv[1:])
	}
	wantDir, err := filepath.EvalSymlinks(cfg.WorkDir)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Dir != wantDir {
		t.Errorf("resume cwd = %q, want %q (same as the failed run)", rec.Dir, wantDir)
	}
}

func TestRunFailureExitPropagation(t *testing.T) {
	dir := t.TempDir()
	cfg := mockConfig(t, dir, map[string]string{mockexec.EnvExit: "1"})
	err := cfg.Run(context.Background())
	if err == nil {
		t.Fatal("Run with exit 1 = nil, want error")
	}
	var exitErr *process.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("error %T = %v, want *process.ExitError", err, err)
	}
	if exitErr.Code != 1 {
		t.Errorf("exit code = %d, want 1", exitErr.Code)
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
	if err := mockexec.ProcessAlive(rec.PID); err == nil {
		t.Errorf("process %d still exists (orphaned or zombie)", rec.PID)
	}
}

func TestResumeCancellation(t *testing.T) {
	dir := t.TempDir()
	cfg := mockConfig(t, dir, map[string]string{mockexec.EnvSleepMS: "30000"})
	if err := os.WriteFile(cfg.Checkpoint, []byte("state\n"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := cfg.Resume(ctx)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Resume past deadline = %v, want context.DeadlineExceeded", err)
	}
	// The checkpoint survives a cancelled resume untouched.
	got, rerr := os.ReadFile(cfg.Checkpoint)
	if rerr != nil {
		t.Fatalf("checkpoint after cancelled resume: %v", rerr)
	}
	if string(got) != "state\n" {
		t.Errorf("checkpoint = %q, want unchanged", got)
	}
}

func TestRunValidationErrorDoesNotExecute(t *testing.T) {
	dir := t.TempDir()
	cfg := mockConfig(t, dir, nil)
	cfg.Path = "../escape"
	if err := cfg.Run(context.Background()); err == nil {
		t.Fatal("Run with unsafe path = nil, want error")
	}
	if _, err := mockexec.ReadRecord(dir); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("mock executed despite invalid config (record err = %v)", err)
	}
}

// TestInstalledMSCPHelpSyntax validates the flags the backend emits against
// the installed mscp's own help text: -n, -a, -s, -L, -6, -W, and -R must
// all be documented there. No transfer is attempted; this pins the command
// shape to the real tool's syntax.
func TestInstalledMSCPHelpSyntax(t *testing.T) {
	prog, err := exec.LookPath("mscp")
	if err != nil {
		t.Skip("mscp not installed; skipping syntax validation")
	}
	out, err := exec.Command(prog, "-h").CombinedOutput()
	if err != nil {
		t.Fatalf("mscp -h: %v (%s)", err, out)
	}
	help := string(out)
	for _, flag := range []string{"-n ", "-a ", "-s ", "-L ", "-6", "-W ", "-R "} {
		if !strings.Contains(help, flag) {
			t.Errorf("installed mscp help does not document %q:\n%s", flag, help)
		}
	}
}
