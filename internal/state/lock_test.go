package state_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/frankruan/catsync/internal/state"
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
	// All lock files are gone; nothing lingers.
	entries, err := os.ReadDir(filepath.Join(s.Dir(), "locks"))
	if err != nil {
		t.Fatalf("ReadDir locks: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("%d lock files left behind", len(entries))
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

func TestLockStaleNotReclaimed(t *testing.T) {
	// A lock left behind by a process that never unlocked (crash) blocks new
	// runs instead of being silently reclaimed: overlapping writes to one
	// destination are worse than a manual cleanup step.
	s := newStore(t)
	if _, err := s.LockDestination(destA()); err != nil {
		t.Fatalf("LockDestination: %v", err)
	}
	// No Unlock: simulate the holder disappearing.
	if _, err := s.LockDestination(destA()); !errors.Is(err, state.ErrLocked) {
		t.Fatalf("stale lock = %v, want ErrLocked", err)
	}
}

func TestLockIncompleteDestination(t *testing.T) {
	s := newStore(t)
	for _, d := range []state.Destination{
		{},
		{Host: "example.com"},
		{Path: "/data"},
	} {
		if _, err := s.LockDestination(d); err == nil {
			t.Errorf("LockDestination(%+v) = nil, want error", d)
		}
	}
}

func TestUnlockRefusesForeignLock(t *testing.T) {
	s := newStore(t)
	lock, err := s.LockDestination(destA())
	if err != nil {
		t.Fatalf("LockDestination: %v", err)
	}
	// Another holder replaces the file: simulate by removing ours and
	// acquiring a fresh lock through the store.
	entries, err := os.ReadDir(filepath.Join(s.Dir(), "locks"))
	if err != nil {
		t.Fatalf("ReadDir locks: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("want 1 lock file, got %d", len(entries))
	}
	path := filepath.Join(s.Dir(), "locks", entries[0].Name())
	if err := os.Remove(path); err != nil {
		t.Fatalf("Remove lock file: %v", err)
	}
	foreign, err := s.LockDestination(destA())
	if err != nil {
		t.Fatalf("foreign LockDestination: %v", err)
	}
	// The stale handle must refuse to delete the new holder's file.
	if err := lock.Unlock(); err == nil {
		t.Error("Unlock of stale handle succeeded, want refusal")
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("foreign lock file was removed: %v", err)
	}
	// The real holder can still release it.
	if err := foreign.Unlock(); err != nil {
		t.Errorf("foreign Unlock: %v", err)
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
