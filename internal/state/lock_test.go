package state_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/dotfrankruan/CatSync/internal/state"
)

func destA() state.Destination {
	return state.Destination{User: "bob", Host: "example.com", Path: "/data"}
}

func TestLockContentionAndRelease(t *testing.T) {
	s := newStore(t)
	lock, err := s.LockDestination(destA())
	if err != nil {
		t.Fatalf("LockDestination: %v", err)
	}
	// A second lock on the same destination fails without blocking.
	if _, err := s.LockDestination(destA()); !errors.Is(err, state.ErrLocked) {
		t.Fatalf("second lock = %v, want ErrLocked", err)
	}
	// Equal destinations spelled differently still contend: the lock key is
	// the canonical [user@]host:path form.
	v6a := state.Destination{Host: "2001:db8::1", Path: "/data"}
	v6b := state.Destination{Host: "[2001:db8::1]", Path: "/data"}
	vlock, err := s.LockDestination(v6a)
	if err != nil {
		t.Fatalf("LockDestination IPv6: %v", err)
	}
	if _, err := s.LockDestination(v6b); !errors.Is(err, state.ErrLocked) {
		t.Errorf("IPv6 respelling lock = %v, want ErrLocked", err)
	}
	// A different destination can be locked in parallel.
	other, err := s.LockDestination(state.Destination{Host: "example.com", Path: "/elsewhere"})
	if err != nil {
		t.Fatalf("LockDestination other: %v", err)
	}
	if err := other.Unlock(); err != nil {
		t.Fatalf("Unlock other: %v", err)
	}
	if err := vlock.Unlock(); err != nil {
		t.Fatalf("Unlock IPv6: %v", err)
	}
	// Releasing frees the destination again.
	if err := lock.Unlock(); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	rel, err := s.LockDestination(destA())
	if err != nil {
		t.Fatalf("relock after Unlock: %v", err)
	}
	if err := rel.Unlock(); err != nil {
		t.Fatalf("Unlock relock: %v", err)
	}
	// Unlock is idempotent.
	if err := rel.Unlock(); err != nil {
		t.Fatalf("second Unlock: %v", err)
	}
	// Lock files are kept (stable inode) but must not block a future run.
	again, err := s.LockDestination(destA())
	if err != nil {
		t.Fatalf("relock with retained lock file: %v", err)
	}
	if err := again.Unlock(); err != nil {
		t.Fatalf("Unlock again: %v", err)
	}
}

func TestLockErrorNamesHolder(t *testing.T) {
	s := newStore(t)
	lock, err := s.LockDestination(destA())
	if err != nil {
		t.Fatalf("LockDestination: %v", err)
	}
	defer lock.Unlock()
	_, err = s.LockDestination(destA())
	if !errors.Is(err, state.ErrLocked) {
		t.Fatalf("second lock = %v, want ErrLocked", err)
	}
	if !strings.Contains(err.Error(), "bob@example.com:/data") {
		t.Errorf("lock error %q does not name the destination", err)
	}
}

func TestLockFileExistenceDoesNotBlock(t *testing.T) {
	// A lock file left behind by an older release (or a crashed process whose
	// flock the kernel already released) must not block a new run. The file
	// and its recorded PID are diagnostic only.
	s := newStore(t)
	if err := os.MkdirAll(filepath.Join(s.Dir(), "locks"), 0o700); err != nil {
		t.Fatal(err)
	}
	lock, err := s.LockDestination(destA())
	if err != nil {
		t.Fatalf("LockDestination: %v", err)
	}
	if err := lock.Unlock(); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	// The file remains; acquisition must still succeed using the advisory
	// lock rather than treating existence as ownership.
	again, err := s.LockDestination(destA())
	if err != nil {
		t.Fatalf("LockDestination with retained lock file: %v", err)
	}
	if err := again.Unlock(); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
}

func TestLockIncompleteDestination(t *testing.T) {
	s := newStore(t)
	for _, d := range []state.Destination{
		{},
		{Host: "example.com"},
		{Path: "relative"},
		{User: "alice", Path: "/data"},
	} {
		if _, err := s.LockDestination(d); err == nil {
			t.Errorf("LockDestination(%+v) = nil, want error", d)
		}
	}
}

func TestLockLocalDestination(t *testing.T) {
	s := newStore(t)
	dest := state.Destination{Path: filepath.Join(t.TempDir(), "download")}
	lock, err := s.LockDestination(dest)
	if err != nil {
		t.Fatalf("LockDestination(local): %v", err)
	}
	defer lock.Unlock()
	if _, err := s.LockDestination(dest); !errors.Is(err, state.ErrLocked) {
		t.Fatalf("second local lock = %v, want ErrLocked", err)
	}
}

func TestUnlockAfterExternalFileRemoval(t *testing.T) {
	s := newStore(t)
	lock, err := s.LockDestination(destA())
	if err != nil {
		t.Fatalf("LockDestination: %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(s.Dir(), "locks"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("locks = %v, %v", entries, err)
	}
	// External tampering (manual removal) must not make Unlock panic or
	// corrupt other state; the advisory lock is released by the handle.
	if err := os.Remove(filepath.Join(s.Dir(), "locks", entries[0].Name())); err != nil {
		t.Fatal(err)
	}
	if err := lock.Unlock(); err != nil {
		t.Errorf("Unlock after external removal: %v", err)
	}
}

func TestLockConcurrentContention(t *testing.T) {
	s := newStore(t)
	const n = 16
	var acquired atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lock, err := s.LockDestination(destA())
			if errors.Is(err, state.ErrLocked) {
				return
			}
			if err != nil {
				t.Errorf("LockDestination: %v", err)
				return
			}
			acquired.Add(1)
			if err := lock.Unlock(); err != nil {
				t.Errorf("Unlock: %v", err)
			}
		}()
	}
	wg.Wait()
	if got := acquired.Load(); got < 1 {
		t.Errorf("no goroutine acquired the lock")
	} else {
		t.Logf("%d goroutine(s) acquired the lock sequentially", got)
	}
}
