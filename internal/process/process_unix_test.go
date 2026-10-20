//go:build unix

package process

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/frankruan/catsync/internal/testutil/mockexec"
)

func TestMain(m *testing.M) {
	if mockexec.HelperActive() {
		os.Exit(mockexec.RunHelper())
	}
	os.Exit(m.Run())
}

// mockSpec returns a Spec executing the test binary as a mock command that
// records into dir, configured by cfg (see the mockexec.EnvXxx constants).
func mockSpec(t *testing.T, dir string, cfg map[string]string) Spec {
	t.Helper()
	prog, err := mockexec.Program()
	if err != nil {
		t.Fatal(err)
	}
	return Spec{Name: prog, Env: mockexec.Env(dir, cfg)}
}

func TestRunRecordsArgvCwdStdin(t *testing.T) {
	dir := t.TempDir()
	work := t.TempDir()
	spec := mockSpec(t, dir, nil)
	spec.Args = []string{
		"--archive", "--files-from=-",
		"plain",
		"with space",
		"with\nnewline",
		"ünïcødé-文件",
		"-leading-dash",
		"quote'and\"double",
		"", // empty argument must survive verbatim
	}
	spec.Dir = work
	spec.Stdin = strings.NewReader("a\x00b\x00with newline\npayload")
	if err := Run(context.Background(), spec); err != nil {
		t.Fatalf("Run: %v", err)
	}
	rec, err := mockexec.ReadRecord(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rec.Args, spec.Args) {
		t.Errorf("argv mismatch:\n got %#v\nwant %#v", rec.Args, spec.Args)
	}
	// The kernel may resolve symlinks in the working directory (macOS
	// /var -> /private/var), so compare resolved paths.
	wantDir, err := filepath.EvalSymlinks(work)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Dir != wantDir {
		t.Errorf("cwd = %q, want %q", rec.Dir, wantDir)
	}
	if got, want := string(rec.Stdin), "a\x00b\x00with newline\npayload"; got != want {
		t.Errorf("stdin = %q, want %q", got, want)
	}
}

// TestRunArgumentsNeverShellExpanded proves that arguments a shell would
// rewrite — variable expansions, globs, command substitutions, tilde —
// reach the child byte-for-byte, because no shell is involved.
func TestRunArgumentsNeverShellExpanded(t *testing.T) {
	dir := t.TempDir()
	spec := mockSpec(t, dir, nil)
	spec.Args = []string{
		"$HOME", "${HOME}", "$(id)", "`id`", "~", "*", "*.txt",
		"a;b", "a|b", "a&b", "a>b", "a<b", "a\\b",
	}
	if err := Run(context.Background(), spec); err != nil {
		t.Fatalf("Run: %v", err)
	}
	rec, err := mockexec.ReadRecord(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rec.Args, spec.Args) {
		t.Errorf("argv mismatch (shell expansion suspected):\n got %#v\nwant %#v", rec.Args, spec.Args)
	}
}

func TestRunCapturesOutputAndEnv(t *testing.T) {
	dir := t.TempDir()
	stdoutContent := []byte("S\t4096\t./big.bin\nInfo: Run ID: 1735689600-12345\n")
	stdoutFile := filepath.Join(dir, "stdout-content")
	if err := os.WriteFile(stdoutFile, stdoutContent, 0600); err != nil {
		t.Fatal(err)
	}
	stderrContent := []byte("warning: something on stderr\n")
	stderrFile := filepath.Join(dir, "stderr-content")
	if err := os.WriteFile(stderrFile, stderrContent, 0600); err != nil {
		t.Fatal(err)
	}
	spec := mockSpec(t, dir, map[string]string{
		mockexec.EnvStdoutFile: stdoutFile,
		mockexec.EnvStderrFile: stderrFile,
		mockexec.EnvEchoEnv:    "MOCKEXEC_TESTVAR",
	})
	spec.Env = append(spec.Env, "MOCKEXEC_TESTVAR=propagated-value")
	var stdout, stderr bytes.Buffer
	spec.Stdout = &stdout
	spec.Stderr = &stderr
	if err := Run(context.Background(), spec); err != nil {
		t.Fatalf("Run: %v", err)
	}
	want := string(stdoutContent) + "propagated-value\n"
	if stdout.String() != want {
		t.Errorf("stdout = %q, want %q", stdout.String(), want)
	}
	if stderr.String() != string(stderrContent) {
		t.Errorf("stderr = %q, want %q", stderr.String(), stderrContent)
	}
}

// TestRunEnvAdditions verifies the environment-addition contract: the child
// inherits the parent's environment, sees additions, and an addition with
// an existing key wins over the inherited value.
func TestRunEnvAdditions(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("MOCKEXEC_INHERITED", "original")
	spec := mockSpec(t, dir, map[string]string{mockexec.EnvEchoEnv: "MOCKEXEC_INHERITED"})
	var stdout bytes.Buffer
	spec.Stdout = &stdout
	if err := Run(context.Background(), spec); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got, want := strings.TrimSpace(stdout.String()), "original"; got != want {
		t.Errorf("inherited var = %q, want %q", got, want)
	}

	dir2 := t.TempDir()
	spec2 := mockSpec(t, dir2, map[string]string{mockexec.EnvEchoEnv: "MOCKEXEC_INHERITED"})
	spec2.Env = append(spec2.Env, "MOCKEXEC_INHERITED=overridden")
	var stdout2 bytes.Buffer
	spec2.Stdout = &stdout2
	if err := Run(context.Background(), spec2); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got, want := strings.TrimSpace(stdout2.String()), "overridden"; got != want {
		t.Errorf("overridden var = %q, want %q", got, want)
	}
}

func TestRunExitFailurePropagation(t *testing.T) {
	dir := t.TempDir()
	spec := mockSpec(t, dir, map[string]string{mockexec.EnvExit: "23"})
	err := Run(context.Background(), spec)
	if err == nil {
		t.Fatal("Run with exit 23 = nil, want error")
	}
	var exitErr *ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("error %T = %v, want *ExitError", err, err)
	}
	if exitErr.Code != 23 {
		t.Errorf("exit code = %d, want 23", exitErr.Code)
	}
	var execErr *exec.ExitError
	if !errors.As(err, &execErr) {
		t.Error("error does not wrap *exec.ExitError")
	}
}

// TestRunSignalDeath verifies a subprocess killed by a signal (not by the
// context) surfaces as an *ExitError with code -1 and signal information.
func TestRunSignalDeath(t *testing.T) {
	dir := t.TempDir()
	spec := mockSpec(t, dir, map[string]string{mockexec.EnvSignal: "TERM"})
	err := Run(context.Background(), spec)
	if err == nil {
		t.Fatal("Run with signal death = nil, want error")
	}
	var exitErr *ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("error %T = %v, want *ExitError", err, err)
	}
	if exitErr.Code != -1 {
		t.Errorf("exit code = %d, want -1 for signal death", exitErr.Code)
	}
	if !strings.Contains(exitErr.Error(), "signal") {
		t.Errorf("error %q does not mention the signal", exitErr)
	}
}

// assertProcessGone fails when pid still refers to a live or zombie process.
func assertProcessGone(t *testing.T, pid int) {
	t.Helper()
	if err := mockexec.ProcessAlive(pid); err == nil {
		t.Errorf("process %d still exists (orphaned or zombie)", pid)
	}
}

func TestRunCancellationTerminatesAndReaps(t *testing.T) {
	dir := t.TempDir()
	spec := mockSpec(t, dir, map[string]string{mockexec.EnvSleepMS: "30000"})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	err := Run(ctx, spec)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("Run with cancelled context = nil, want error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want it to wrap context.Canceled", err)
	}
	if elapsed > 10*time.Second {
		t.Errorf("cancellation took %s, want prompt termination", elapsed)
	}
	rec, rerr := mockexec.ReadRecord(dir)
	if rerr != nil {
		t.Fatal(rerr)
	}
	assertProcessGone(t, rec.PID)
}

func TestRunDeadlineExceeded(t *testing.T) {
	dir := t.TempDir()
	spec := mockSpec(t, dir, map[string]string{mockexec.EnvSleepMS: "30000"})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := Run(ctx, spec)
	if err == nil {
		t.Fatal("Run past deadline = nil, want error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error = %v, want it to wrap context.DeadlineExceeded", err)
	}
	rec, rerr := mockexec.ReadRecord(dir)
	if rerr != nil {
		t.Fatal(rerr)
	}
	assertProcessGone(t, rec.PID)
}

func TestRunPreCancelledContext(t *testing.T) {
	dir := t.TempDir()
	spec := mockSpec(t, dir, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := Run(ctx, spec)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Run with pre-cancelled context = %v, want context.Canceled", err)
	}
}

func TestRunSuccessAfterZeroExit(t *testing.T) {
	dir := t.TempDir()
	spec := mockSpec(t, dir, map[string]string{mockexec.EnvExit: "0"})
	if err := Run(context.Background(), spec); err != nil {
		t.Fatalf("Run exit 0 = %v, want nil", err)
	}
}

// waitGone polls until pid no longer exists (a killed process may linger as
// a zombie briefly while its parent is torn down).
func waitGone(t *testing.T, pid int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		if err := mockexec.ProcessAlive(pid); err != nil {
			return
		}
		if time.Now().After(deadline) {
			t.Errorf("process %d still exists after %s (orphaned, zombie, or leaked descendant)", pid, within)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// childPids waits for n grandchild PID files in dir and returns their PIDs.
func childPids(t *testing.T, dir string, n int) []int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		matches, _ := filepath.Glob(filepath.Join(dir, "child-*.pid"))
		if len(matches) >= n {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected %d grandchild pid files in %s, found %d", n, dir, len(matches))
		}
		time.Sleep(10 * time.Millisecond)
	}
	matches, err := filepath.Glob(filepath.Join(dir, "child-*.pid"))
	if err != nil {
		t.Fatal(err)
	}
	var pids []int
	for _, m := range matches {
		data, err := os.ReadFile(m)
		if err != nil {
			t.Fatal(err)
		}
		var pid int
		if _, err := fmt.Sscanf(strings.TrimSpace(string(data)), "%d", &pid); err != nil {
			t.Fatalf("parse pid file %s: %v", m, err)
		}
		pids = append(pids, pid)
	}
	return pids
}

// TestRunCancellationKillsProcessTree proves a cancelled Run terminates the
// direct child and the grandchildren it spawned, and does not return until
// they are gone.
func TestRunCancellationKillsProcessTree(t *testing.T) {
	dir := t.TempDir()
	childDir := t.TempDir()
	spec := mockSpec(t, dir, map[string]string{
		mockexec.EnvSleepMS:       "30000",
		mockexec.EnvSpawnChildren: "1",
		mockexec.EnvChildPidDir:   childDir,
		mockexec.EnvChildSleepMS:  "30000",
	})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	err := Run(ctx, spec)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Errorf("cancellation took %s, want prompt termination", elapsed)
	}
	rec, rerr := mockexec.ReadRecord(dir)
	if rerr != nil {
		t.Fatal(rerr)
	}
	waitGone(t, rec.PID, 5*time.Second)
	for _, pid := range childPids(t, childDir, 1) {
		waitGone(t, pid, 5*time.Second)
	}
}

// TestRunCancellationKillsMultipleChildren models an fpsync-style process
// tree: one direct child with several concurrent worker descendants. All
// must be gone when Run returns.
func TestRunCancellationKillsMultipleChildren(t *testing.T) {
	dir := t.TempDir()
	childDir := t.TempDir()
	spec := mockSpec(t, dir, map[string]string{
		mockexec.EnvSleepMS:       "30000",
		mockexec.EnvSpawnChildren: "3",
		mockexec.EnvChildPidDir:   childDir,
		mockexec.EnvChildSleepMS:  "30000",
	})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(300 * time.Millisecond)
		cancel()
	}()
	if err := Run(ctx, spec); err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	rec, rerr := mockexec.ReadRecord(dir)
	if rerr != nil {
		t.Fatal(rerr)
	}
	waitGone(t, rec.PID, 5*time.Second)
	pids := childPids(t, childDir, 3)
	if len(pids) != 3 {
		t.Fatalf("got %d child pids, want 3", len(pids))
	}
	for _, pid := range pids {
		waitGone(t, pid, 5*time.Second)
	}
}

// TestRunNormalExitCleansStragglers proves descendants cannot outlive a
// successfully exited tool: Run returns only after the leftover children
// are killed.
func TestRunNormalExitCleansStragglers(t *testing.T) {
	dir := t.TempDir()
	childDir := t.TempDir()
	spec := mockSpec(t, dir, map[string]string{
		mockexec.EnvSleepMS:       "500", // let the grandchildren start first
		mockexec.EnvSpawnChildren: "2",
		mockexec.EnvChildPidDir:   childDir,
		mockexec.EnvChildSleepMS:  "30000",
	})
	if err := Run(context.Background(), spec); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, pid := range childPids(t, childDir, 2) {
		waitGone(t, pid, 5*time.Second)
	}
}

// TestRunForwardsRecordedSignal proves the cancellation signal identity is
// forwarded to the child group (SIGINT stays SIGINT).
func TestRunForwardsRecordedSignal(t *testing.T) {
	dir := t.TempDir()
	spec := mockSpec(t, dir, map[string]string{
		mockexec.EnvSleepMS:     "30000",
		mockexec.EnvCatchSignal: "1",
	})
	base, cancel := context.WithCancel(context.Background())
	slot := NewCancelSignal()
	ctx := WithCancelSignal(base, slot)
	go func() {
		time.Sleep(200 * time.Millisecond)
		slot.Set(syscall.SIGINT)
		cancel()
	}()
	if err := Run(ctx, spec); err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "signal"))
	if err != nil {
		t.Fatalf("helper did not record a signal: %v", err)
	}
	if got := strings.TrimSpace(string(data)); got != "interrupt" {
		t.Errorf("forwarded signal = %q, want interrupt (SIGINT)", got)
	}
}

// TestRunForwardsTermByDefault checks SIGTERM is the fallback when no
// cancel-signal identity is recorded.
func TestRunForwardsTermByDefault(t *testing.T) {
	dir := t.TempDir()
	spec := mockSpec(t, dir, map[string]string{
		mockexec.EnvSleepMS:     "30000",
		mockexec.EnvCatchSignal: "1",
	})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()
	if err := Run(ctx, spec); err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "signal"))
	if err != nil {
		t.Fatalf("helper did not record a signal: %v", err)
	}
	if got := strings.TrimSpace(string(data)); got != "terminated" {
		t.Errorf("forwarded signal = %q, want terminated (SIGTERM)", got)
	}
}

// TestRunGracePeriodForcesKillAndUnblocksPipes covers the hard case: the
// child and its grandchild ignore SIGTERM and the grandchild holds the
// stdout pipe. After the grace period both must be SIGKILLed, the pipes
// must close, and Run must return rather than block forever.
func TestRunGracePeriodForcesKillAndUnblocksPipes(t *testing.T) {
	dir := t.TempDir()
	childDir := t.TempDir()
	spec := mockSpec(t, dir, map[string]string{
		mockexec.EnvSleepMS:         "30000",
		mockexec.EnvIgnoreTerm:      "1",
		mockexec.EnvSpawnChildren:   "1",
		mockexec.EnvChildPidDir:     childDir,
		mockexec.EnvChildSleepMS:    "30000",
		mockexec.EnvChildIgnoreTerm: "1",
	})
	spec.GracePeriod = 300 * time.Millisecond
	var stdout, stderr bytes.Buffer
	spec.Stdout = &stdout
	spec.Stderr = &stderr
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(300 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	err := Run(ctx, spec)
	elapsed := time.Since(start)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if elapsed > 10*time.Second {
		t.Errorf("Run blocked for %s on pipes after cancellation", elapsed)
	}
	rec, rerr := mockexec.ReadRecord(dir)
	if rerr != nil {
		t.Fatal(rerr)
	}
	waitGone(t, rec.PID, 5*time.Second)
	for _, pid := range childPids(t, childDir, 1) {
		waitGone(t, pid, 5*time.Second)
	}
}
