package state

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// locksDirName is the subdirectory of the state directory holding lock
// files, keeping them out of List's way.
const locksDirName = "locks"

// Lock is an exclusive, destination-scoped advisory lock held by the
// existence of one lock file. Locks are nonblocking: a second holder fails
// immediately with ErrLocked instead of waiting. A lock only ever creates
// and removes its own file; run state and user data are never touched.
type Lock struct {
	path  string
	dest  string
	token string

	mu       sync.Mutex
	released bool
}

// lockInfo is the JSON body of a lock file, recorded so a human can identify
// the holder of a stale lock.
type lockInfo struct {
	Destination string    `json:"destination"`
	PID         int       `json:"pid"`
	Token       string    `json:"token"`
	AcquiredAt  time.Time `json:"acquired_at"`
}

// Destination returns the canonical destination string this lock covers.
func (l *Lock) Destination() string { return l.dest }

// lockPath derives the lock file name for a destination key. The SHA-256
// digest keeps the name short and free of characters that are unsafe in
// file names, whatever the destination looks like; the human-readable
// destination lives inside the file.
func lockPath(locksDir, key string) string {
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(locksDir, hex.EncodeToString(sum[:])+".lock")
}

// LockDestination acquires the exclusive nonblocking lock for dest. Two runs
// writing the same remote tree can corrupt each other's data, so a second
// caller fails immediately with an error wrapping ErrLocked that names the
// recorded holder. The lock is deliberately not reclaimed when a process
// dies holding it: a stale lock file is reported, and a human can remove it
// once the holder is confirmed gone.
func (s *Store) LockDestination(dest Destination) (*Lock, error) {
	if dest.Host == "" || dest.Path == "" {
		return nil, errors.New("state: cannot lock an incomplete destination")
	}
	key := dest.String()
	locksDir := filepath.Join(s.dir, locksDirName)
	if err := ensurePrivateDir(locksDir); err != nil {
		return nil, fmt.Errorf("state: create locks directory: %w", err)
	}
	var rnd [8]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		return nil, fmt.Errorf("state: generate lock token: %w", err)
	}
	info := lockInfo{
		Destination: key,
		PID:         os.Getpid(),
		Token:       hex.EncodeToString(rnd[:]),
		AcquiredAt:  time.Now().UTC(),
	}
	body, err := json.Marshal(info)
	if err != nil {
		return nil, fmt.Errorf("state: encode lock info: %w", err)
	}
	path := lockPath(locksDir, key)
	// O_EXCL makes acquisition atomic and nonblocking.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, fileMode)
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			return nil, lockedError(path, key)
		}
		return nil, fmt.Errorf("state: create lock file: %w", err)
	}
	// On any failure below the file is one this call just created, so
	// removing it cannot delete another holder's lock.
	if _, err := f.Write(body); err != nil {
		f.Close()
		os.Remove(path)
		return nil, fmt.Errorf("state: write lock file: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(path)
		return nil, fmt.Errorf("state: sync lock file: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(path)
		return nil, fmt.Errorf("state: close lock file: %w", err)
	}
	_ = syncDir(locksDir) // best effort, see Save
	return &Lock{path: path, dest: key, token: info.Token}, nil
}

// lockedError builds an ErrLocked error, naming the recorded holder when the
// existing lock file is readable.
func lockedError(path, key string) error {
	data, err := os.ReadFile(path)
	if err == nil {
		var info lockInfo
		if err := json.Unmarshal(data, &info); err == nil {
			return fmt.Errorf("%w: %s (held by pid %d since %s; remove %s once that process is gone)",
				ErrLocked, key, info.PID, info.AcquiredAt.Format(time.RFC3339), path)
		}
	}
	return fmt.Errorf("%w: %s", ErrLocked, key)
}

// Unlock releases the lock. It is idempotent and removes the lock file only
// after verifying the recorded token matches, so releasing can never delete
// another holder's lock or any user data. A lock file that is already gone
// is treated as released; one whose ownership cannot be verified is left in
// place and reported.
func (l *Lock) Unlock() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return nil
	}
	data, err := os.ReadFile(l.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			l.released = true
			return nil
		}
		return fmt.Errorf("state: read lock file: %w", err)
	}
	var info lockInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return fmt.Errorf("state: cannot verify ownership of %s; not removing it", l.path)
	}
	if info.Token != l.token {
		return fmt.Errorf("state: lock for %s is held by another owner; not removing it", l.dest)
	}
	if err := os.Remove(l.path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("state: remove lock file: %w", err)
	}
	l.released = true
	return nil
}
