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

// Lock is an exclusive, destination-scoped advisory lock. Ownership is held
// by an OS advisory lock (flock) on a stable lock-file inode, not by the mere
// existence of the file: a lock file left behind by a crashed process never
// blocks a new run, because the kernel releases the advisory lock when the
// holder's file descriptor closes.
//
// The file is deliberately never unlinked while a process may still open it:
// removing it would allow a second inode to be created and locked
// concurrently. Released files remain in the locks directory, unlocked and
// empty; they are harmless and are reused by the next run.
type Lock struct {
	file  *os.File
	path  string
	dest  string
	token string

	mu       sync.Mutex
	released bool
}

// lockInfo is the JSON body of a held lock file, recorded so a human can
// identify the holder. It is diagnostic only: no recovery decision may be
// based on the PID or timestamp in it.
type lockInfo struct {
	Destination string    `json:"destination"`
	PID         int       `json:"pid"`
	Token       string    `json:"token"`
	AcquiredAt  time.Time `json:"acquired_at"`
}

// Destination returns the canonical destination string this lock covers.
func (l *Lock) Destination() string { return l.dest }

// processLocks prevents two lock attempts in one process from both reaching
// the OS lock. It is cleanup state only; the advisory lock remains the
// cross-process authority.
var processLocks = struct {
	sync.Mutex
	held map[string]bool
}{held: make(map[string]bool)}

// lockPath derives the lock file name for a destination key. The SHA-256
// digest keeps the name short and free of characters that are unsafe in
// file names, whatever the destination looks like; the human-readable
// destination lives inside the file.
func lockPath(locksDir, key string) string {
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(locksDir, hex.EncodeToString(sum[:])+".lock")
}

// LockDestination acquires the exclusive nonblocking advisory lock for dest.
// Two runs writing the same remote tree can corrupt each other's data, so a
// second caller fails immediately with an error wrapping ErrLocked. The lock
// is released automatically by the kernel if the process dies, including
// SIGKILL; no manual stale-file cleanup is needed.
//
// The lock is only as reliable as the filesystem hosting the state
// directory. Local filesystems (APFS, ext4, xfs) provide correct flock
// semantics. Network filesystems or other environments that do not implement
// advisory locks reliably cannot provide cross-host exclusion; LockDestination
// fails closed (returns an error) when the OS reports advisory locking as
// unsupported, rather than pretending to be a global lock.
func (s *Store) LockDestination(dest Destination) (*Lock, error) {
	if dest.Path == "" {
		return nil, errors.New("state: cannot lock an incomplete destination")
	}
	key := dest.String()
	if dest.Host == "" {
		if !filepath.IsAbs(dest.Path) || dest.User != "" {
			return nil, errors.New("state: cannot lock an invalid local destination")
		}
		key = "local:" + filepath.Clean(dest.Path)
	}
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
	path := lockPath(locksDir, key)
	releaseProcessSlot := func() {
		processLocks.Lock()
		delete(processLocks.held, path)
		processLocks.Unlock()
	}
	processLocks.Lock()
	if processLocks.held[path] {
		processLocks.Unlock()
		return nil, lockedError(path, key)
	}
	processLocks.held[path] = true
	processLocks.Unlock()
	fail := func(err error) (*Lock, error) {
		releaseProcessSlot()
		return nil, err
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, fileMode)
	if err != nil {
		return fail(fmt.Errorf("state: open lock file: %w", err))
	}
	locked, lerr := tryLockFile(f)
	if lerr != nil {
		_ = f.Close()
		return fail(fmt.Errorf("state: acquisition for %s failed: %w", key, lerr))
	}
	if !locked {
		_ = f.Close()
		return fail(lockedError(path, key))
	}
	if err := writeLockInfo(f, info); err != nil {
		_ = releaseLockFile(f)
		_ = f.Close()
		return fail(fmt.Errorf("state: write lock file: %w", err))
	}
	return &Lock{file: f, path: path, dest: key, token: info.Token}, nil
}

// writeLockInfo replaces the diagnostic contents of the already-locked file.
func writeLockInfo(f *os.File, info lockInfo) error {
	body, err := json.Marshal(info)
	if err != nil {
		return err
	}
	if err := f.Truncate(0); err != nil {
		return err
	}
	if _, err := f.Seek(0, 0); err != nil {
		return err
	}
	if _, err := f.Write(body); err != nil {
		return err
	}
	return f.Sync()
}

// lockedError builds an ErrLocked error naming the holder when the lock file
// can be read. It never advises removing the file: the file is not the lock.
func lockedError(path, key string) error {
	data, err := os.ReadFile(path)
	if err == nil {
		var info lockInfo
		if err := json.Unmarshal(data, &info); err == nil && info.Destination != "" {
			return fmt.Errorf("%w: %s (held by pid %d since %s; advisory lock still held by a running catsync process)",
				ErrLocked, key, info.PID, info.AcquiredAt.Format(time.RFC3339))
		}
	}
	return fmt.Errorf("%w: %s (advisory lock held by another catsync process)", ErrLocked, key)
}

// Unlock releases the lock. It is idempotent. The lock file is kept (empty,
// unlocked) to keep its inode stable; it is never unlinked by a holder.
func (l *Lock) Unlock() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return nil
	}
	var firstErr error
	if l.file != nil {
		// Clear diagnostic contents while the advisory lock is still held.
		if err := l.file.Truncate(0); err != nil && !errors.Is(err, fs.ErrClosed) {
			firstErr = fmt.Errorf("state: clear lock file: %w", err)
		}
		_ = l.file.Sync()
		if err := releaseLockFile(l.file); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("state: release lock: %w", err)
		}
		if err := l.file.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("state: close lock file: %w", err)
		}
		l.file = nil
	}
	processLocks.Lock()
	delete(processLocks.held, l.path)
	processLocks.Unlock()
	l.released = true
	return firstErr
}

// releaseLockFile releases the advisory lock explicitly before close. Closing
// the descriptor alone would also release it; this makes the ordering
// explicit and surfaces errors.
func releaseLockFile(f *os.File) error {
	return unlockFile(f)
}
