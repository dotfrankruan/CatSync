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

	"github.com/dotfrankruan/CatSync/internal/testutil/mockexec"
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

// waitForFile polls until path exists, failing the test on timeout.
func waitForFile(t *testing.T, path string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not appear within %s", path, within)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestRunCancelledChildExitsZeroStillReturnsContextError is the regression
// test for graceful-shutdown classification: a child that catches the
// forwarded SIGTERM and exits 0 during cancellation did not finish on its
// own, so Run must report the cancellation, not success.
func TestRunCancelledChildExitsZeroStillReturnsContextError(t *testing.T) {
	dir := t.TempDir()
	childDir := t.TempDir()
	ready := filepath.Join(t.TempDir(), "ready")
	spec := mockSpec(t, dir, map[string]string{
		mockexec.EnvSleepMS:        "30000",
		mockexec.EnvZeroExitSignal: "TERM",
		mockexec.EnvReadyFile:      ready,
		mockexec.EnvSpawnChildren:  "1",
		mockexec.EnvChildPidDir:    childDir,
		mockexec.EnvChildSleepMS:   "30000",
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		// The helper writes the ready marker only after its signal handler
		// is installed and its grandchild has reported in.
		deadline := time.Now().Add(10 * time.Second)
		for {
			if _, err := os.Stat(ready); err == nil {
				break
			}
			if time.Now().After(deadline) {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		cancel()
	}()

	err := Run(ctx, spec)
	if err == nil {
		t.Fatal("Run = nil for a cancelled zero-exit child, want context.Canceled")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want it to wrap context.Canceled", err)
	}
	// The helper exited 0 after handling TERM, and the whole tree is gone.
	sig, sigErr := os.ReadFile(filepath.Join(dir, "signal"))
	if sigErr != nil {
		t.Fatalf("helper did not record the forwarded signal: %v", sigErr)
	}
	if got := strings.TrimSpace(string(sig)); got != "terminated" {
		t.Errorf("forwarded signal = %q, want terminated (SIGTERM)", got)
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

// TestRunCancelledChildExitsZeroOnSIGINT is the SIGINT variant of the
// graceful-shutdown regression: the recorded cancel signal is forwarded,
// the child exits 0 after handling it, and Run still reports the
// cancellation.
func TestRunCancelledChildExitsZeroOnSIGINT(t *testing.T) {
	dir := t.TempDir()
	ready := filepath.Join(t.TempDir(), "ready")
	spec := mockSpec(t, dir, map[string]string{
		mockexec.EnvSleepMS:        "30000",
		mockexec.EnvZeroExitSignal: "INT",
		mockexec.EnvReadyFile:      ready,
	})
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	slot := NewCancelSignal()
	ctx := WithCancelSignal(base, slot)
	go func() {
		deadline := time.Now().Add(10 * time.Second)
		for {
			if _, err := os.Stat(ready); err == nil {
				break
			}
			if time.Now().After(deadline) {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		slot.Set(syscall.SIGINT)
		cancel()
	}()

	err := Run(ctx, spec)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want it to wrap context.Canceled", err)
	}
	sig, sigErr := os.ReadFile(filepath.Join(dir, "signal"))
	if sigErr != nil {
		t.Fatalf("helper did not record the forwarded signal: %v", sigErr)
	}
	if got := strings.TrimSpace(string(sig)); got != "interrupt" {
		t.Errorf("forwarded signal = %q, want interrupt (SIGINT)", got)
	}
}

// TestRunDeadlineChildExitsZero checks a child that handles SIGTERM and
// exits 0 when the context deadline expires: the error must wrap
// context.DeadlineExceeded, not classify as success.
func TestRunDeadlineChildExitsZero(t *testing.T) {
	dir := t.TempDir()
	ready := filepath.Join(t.TempDir(), "ready")
	spec := mockSpec(t, dir, map[string]string{
		mockexec.EnvSleepMS:        "30000",
		mockexec.EnvZeroExitSignal: "TERM",
		mockexec.EnvReadyFile:      ready,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	err := Run(ctx, spec)
	if err == nil {
		t.Fatal("Run = nil for a deadline-expired zero-exit child, want DeadlineExceeded")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want it to wrap context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Errorf("deadline path took %s", elapsed)
	}
	// The helper reached its handler (ready file) and recorded the signal,
	// proving the zero-exit graceful path was exercised.
	waitForFile(t, ready, time.Second)
	if _, sigErr := os.ReadFile(filepath.Join(dir, "signal")); sigErr != nil {
		t.Errorf("helper did not handle the forwarded signal: %v", sigErr)
	}
}

// TestRunContextCancelAfterCompletionReturnsNil is the reverse-race
// protection: once Run has returned because the child finished normally, a
// later cancellation must not change the successful result.
func TestRunContextCancelAfterCompletionReturnsNil(t *testing.T) {
	dir := t.TempDir()
	spec := mockSpec(t, dir, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type result struct{ err error }
	done := make(chan result, 1)
	go func() { done <- result{err: Run(ctx, spec)} }()
	r := <-done
	if r.err != nil {
		t.Fatalf("Run = %v, want nil for a normally completed child", r.err)
	}
	// Cancel only after Run returned: no watcher observed it.
	cancel()
	// The returned value is already fixed; this documents the contract.
	if r.err != nil {
		t.Fatalf("late cancellation changed the result to %v", r.err)
	}
}

// TestClassifyRunResultKeepsBothCauses covers the classification seams
// directly: a watcher-observed cancellation wins over a zero wait result,
// and cleanup/WaitDelay diagnostics remain inspectable alongside it.
func TestClassifyRunResultKeepsBothCauses(t *testing.T) {
	// Graceful zero-exit cancellation.
	if err := classifyRunResult("tool", nil, context.Canceled, nil); !errors.Is(err, context.Canceled) {
		t.Errorf("zero-exit cancellation = %v, want context.Canceled", err)
	}
	// Deadline variant.
	if err := classifyRunResult("tool", nil, context.DeadlineExceeded, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("zero-exit deadline = %v, want DeadlineExceeded", err)
	}
	// Cancellation plus cleanup failure: both must be errors.Is-reachable.
	cleanup := fmt.Errorf("process: group survived: %w", ErrDescendantsAlive)
	err := classifyRunResult("tool", nil, context.Canceled, cleanup)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrDescendantsAlive) {
		t.Errorf("joined cancellation+cleanup = %v, want both causes", err)
	}
	// Cancellation plus WaitDelay: keep the diagnostic cause.
	err = classifyRunResult("tool", exec.ErrWaitDelay, context.DeadlineExceeded, nil)
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, exec.ErrWaitDelay) {
		t.Errorf("joined deadline+WaitDelay = %v, want both causes", err)
	}
	// No observed cancellation: a zero wait result is success.
	if err := classifyRunResult("tool", nil, nil, nil); err != nil {
		t.Errorf("plain success = %v, want nil", err)
	}
	// No observed cancellation with a non-zero exit: ExitError, never ctx.
	err = classifyRunResult("tool", &exec.ExitError{}, nil, nil)
	var exitErr *ExitError
	if !errors.As(err, &exitErr) {
		t.Errorf("non-zero exit = %v, want *ExitError", err)
	}
}
