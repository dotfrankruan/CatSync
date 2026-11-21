package state_test

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/dotfrankruan/CatSync/internal/state"
)

// TestMain doubles as the cross-process lock helper: when the environment
// marker is set the test binary acquires a destination lock, announces
// readiness, and blocks until killed. No test selection is involved.
func TestMain(m *testing.M) {
	if mode := os.Getenv("CATSYNC_LOCK_HELPER"); mode != "" {
		os.Exit(runLockHelper(mode))
	}
	os.Exit(m.Run())
}

func runLockHelper(mode string) int {
	store, err := state.NewStore(os.Getenv("CATSYNC_LOCK_STATE"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	lock, err := store.LockDestination(state.Destination{
		User: "bob", Host: "example.com", Path: "/data",
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 3
	}
	defer lock.Unlock()
	if err := os.WriteFile(os.Getenv("CATSYNC_LOCK_READY"), []byte(fmt.Sprint(os.Getpid())), 0o600); err != nil {
		return 4
	}
	switch mode {
	case "sleep":
		time.Sleep(2 * time.Minute)
	case "exit":
		return 0
	}
	return 0
}

// startLockHelper starts one test-binary child holding destA's lock.
func startLockHelper(t *testing.T, stateDir string) *exec.Cmd {
	t.Helper()
	ready := filepath.Join(t.TempDir(), "ready")
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(),
		"CATSYNC_LOCK_HELPER=sleep",
		"CATSYNC_LOCK_STATE="+stateDir,
		"CATSYNC_LOCK_READY="+ready,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start lock helper: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(ready); err == nil && len(data) > 0 {
			return cmd
		}
		time.Sleep(10 * time.Millisecond)
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	t.Fatal("lock helper did not become ready")
	return nil
}

func TestLockCrossProcessContentionAndKill(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	store, err := state.NewStore(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	helper := startLockHelper(t, stateDir)
	defer func() {
		_ = helper.Process.Kill()
		_ = helper.Wait()
	}()
	if _, err := store.LockDestination(destA()); !errors.Is(err, state.ErrLocked) {
		t.Fatalf("cross-process lock = %v, want ErrLocked", err)
	}
	// SIGKILL: the kernel releases the advisory lock; no file removal is
	// needed and the next run acquires immediately.
	if err := helper.Process.Kill(); err != nil {
		t.Fatalf("kill helper: %v", err)
	}
	_ = helper.Wait()
	lock, err := store.LockDestination(destA())
	if err != nil {
		t.Fatalf("lock after SIGKILL: %v (stale lock file must not block)", err)
	}
	if err := lock.Unlock(); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	// A second process may now start again.
	helper2 := startLockHelper(t, stateDir)
	defer func() { _ = helper2.Process.Kill(); _ = helper2.Wait() }()
	if _, err := store.LockDestination(destA()); !errors.Is(err, state.ErrLocked) {
		t.Fatalf("second cross-process lock = %v, want ErrLocked", err)
	}
}

func TestLockSIGTERMAndOldFormatCompatibility(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	store, err := state.NewStore(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	// Old-format lock file: exists, has JSON, no advisory lock. It must not
	// block, and the PID-looking content must not be trusted.
	lock, err := store.LockDestination(destA())
	if err != nil {
		t.Fatal(err)
	}
	if err := lock.Unlock(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(stateDir, "locks"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("locks = %v, %v", entries, err)
	}
	oldPath := filepath.Join(stateDir, "locks", entries[0].Name())
	old := fmt.Sprintf(`{"destination":"%s","pid":%d,"token":"old","acquired_at":"2020-01-01T00:00:00Z"}`, destA().String(), os.Getpid())
	if err := os.WriteFile(oldPath, []byte(old), 0o600); err != nil {
		t.Fatal(err)
	}
	lock2, err := store.LockDestination(destA())
	if err != nil {
		t.Fatalf("old-format lock file blocked acquisition: %v", err)
	}
	if err := lock2.Unlock(); err != nil {
		t.Fatal(err)
	}
	// SIGTERM: start a helper and terminate it; lock must be free.
	helper := startLockHelper(t, stateDir)
	if err := helper.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("signal helper: %v", err)
	}
	_ = helper.Wait()
	lock3, err := store.LockDestination(destA())
	if err != nil {
		t.Fatalf("lock after SIGTERM: %v", err)
	}
	if err := lock3.Unlock(); err != nil {
		t.Fatal(err)
	}
	// Normal helper exit: the lock is released without touching the file.
	ready := filepath.Join(t.TempDir(), "ready")
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(), "CATSYNC_LOCK_HELPER=exit",
		"CATSYNC_LOCK_STATE="+stateDir, "CATSYNC_LOCK_READY="+ready)
	if err := cmd.Run(); err != nil {
		t.Fatalf("helper normal exit: %v", err)
	}
	lock4, err := store.LockDestination(destA())
	if err != nil {
		t.Fatalf("lock after normal exit: %v", err)
	}
	if err := lock4.Unlock(); err != nil {
		t.Fatal(err)
	}
}
