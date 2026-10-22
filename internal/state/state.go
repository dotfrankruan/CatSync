// Package state persists CatSync run state on local disk.
//
// Each run is one versioned JSON document stored as <dir>/<run-id>.json with
// mode 0600 inside a state directory (and lock directory) created with mode
// 0700. Saves are atomic: the document is written to a temporary file in the
// same directory, fsynced, renamed into place, and the directory is synced
// where the platform supports it.
//
// A destination-scoped, nonblocking lock (see LockDestination) stops two
// runs from writing the same remote tree at once. Locking and unlocking
// never delete run state or user data; a stale lock left by a crashed
// process is reported, not reclaimed automatically.
//
// Runs interrupted while running are recovered explicitly: running tasks
// return to pending and a running run is marked failed but retryable, so
// resume can revalidate and continue it (see Run.RecoverInterrupted and
// Store.LoadAndRecover).
package state

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// SchemaVersion is the on-disk schema version written by Save and the only
// version Load accepts, so an incompatible future format is never
// interpreted silently.
const SchemaVersion = 1

// Sentinel errors wrapped by this package; test them with errors.Is.
var (
	// ErrInvalidID rejects run IDs that are not safe single file names.
	ErrInvalidID = errors.New("state: invalid run id")
	// ErrNotFound reports a run ID with no stored state.
	ErrNotFound = errors.New("state: run not found")
	// ErrNoRuns reports an empty state directory.
	ErrNoRuns = errors.New("state: no runs recorded")
	// ErrCorrupt reports a state file that does not parse or violates the
	// schema.
	ErrCorrupt = errors.New("state: corrupt run state")
	// ErrUnsupportedVersion reports a state file whose schema version this
	// build does not understand.
	ErrUnsupportedVersion = errors.New("state: unsupported schema version")
	// ErrLocked reports a destination already locked by another run.
	ErrLocked = errors.New("state: destination locked by another run")
)

// Backend names a transfer backend.
type Backend string

// Known backends. BackendAuto is a configuration choice; individual tasks
// are planned onto one of the concrete backends.
const (
	BackendAuto   Backend = "auto"
	BackendFPSync Backend = "fpsync"
	BackendMSCP   Backend = "mscp"
	BackendRsync  Backend = "rsync"
)

// Valid reports whether b is a known backend.
func (b Backend) Valid() bool {
	switch b {
	case BackendAuto, BackendFPSync, BackendMSCP, BackendRsync:
		return true
	}
	return false
}

// Status is the lifecycle state of a run or a task.
type Status string

// Known statuses. Runs use pending, running, completed, and failed; skipped
// and unsupported are meaningful for tasks only.
const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
	// StatusSkipped marks a task that needs no transfer because the
	// destination already matched the source metadata.
	StatusSkipped Status = "skipped"
	// StatusUnsupported marks a source entry the MVP cannot transfer: a
	// symlink, a special file (device, FIFO, socket), or an entry whose
	// scan failed. It is distinct from skipped (nothing to do) and failed
	// (the transfer was attempted and failed): the entry is explicitly
	// reported, never written to the destination, and never counted as
	// completed.
	StatusUnsupported Status = "unsupported"
)

// Valid reports whether s is a known status.
func (s Status) Valid() bool {
	switch s {
	case StatusPending, StatusRunning, StatusCompleted, StatusFailed, StatusSkipped, StatusUnsupported:
		return true
	}
	return false
}

// Verification modes recorded in Config.Verify.
const (
	VerifyNone   = "none"
	VerifySize   = "size"
	VerifySHA256 = "sha256"
)

// Config is the transfer configuration recorded with a run. It is immutable
// for the life of the run so resume reproduces the original behavior.
type Config struct {
	// Backend is the requested backend selection (auto or concrete).
	Backend Backend `json:"backend"`
	// DryRun records a plan-only run that transfers nothing.
	DryRun bool `json:"dry_run"`
	// IPv6 restricts backend tools to IPv6.
	IPv6 bool `json:"ipv6"`
	// Verify is the completion verification mode: none, size, or sha256.
	Verify string `json:"verify"`
	// Jobs caps parallel transfer jobs.
	Jobs int `json:"jobs"`
	// Streams caps parallel streams per job.
	Streams int `json:"streams"`
	// LargeThreshold is the size in bytes at which a file counts as large.
	LargeThreshold uint64 `json:"large_threshold"`
}

// validate checks the configuration invariants.
func (c *Config) validate() error {
	if !c.Backend.Valid() {
		return fmt.Errorf("invalid backend %q", c.Backend)
	}
	switch c.Verify {
	case VerifyNone, VerifySize, VerifySHA256:
	default:
		return fmt.Errorf("invalid verify mode %q", c.Verify)
	}
	if c.Jobs < 1 {
		return fmt.Errorf("jobs must be at least 1, got %d", c.Jobs)
	}
	if c.Streams < 1 {
		return fmt.Errorf("streams must be at least 1, got %d", c.Streams)
	}
	return nil
}

// Source describes the local tree a run transfers.
type Source struct {
	// Path is the absolute local source directory.
	Path string `json:"path"`
}

// Destination describes the remote tree a run writes to.
type Destination struct {
	// User is the optional remote user name; empty means the SSH default.
	User string `json:"user,omitempty"`
	// Host is the remote host name or address.
	Host string `json:"host"`
	// Path is the absolute path on the remote host.
	Path string `json:"path"`
}

// String renders the canonical [user@]host:path form. IPv6 literals (hosts
// containing a colon) are bracketed, whether or not the stored host already
// carries brackets, so equal destinations always produce equal strings.
func (d Destination) String() string {
	host := d.Host
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = host[1 : len(host)-1]
	}
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	if d.User != "" {
		return d.User + "@" + host + ":" + d.Path
	}
	return host + ":" + d.Path
}

// Task is the per-file unit of work recorded in a run.
type Task struct {
	// Path is the slash-separated path relative to the source root.
	Path string `json:"path"`
	// Size is the source size in bytes observed at plan time.
	Size int64 `json:"size"`
	// ModTime is the source modification time observed at plan time.
	ModTime time.Time `json:"mod_time"`
	// DestSize is the destination size in bytes, or -1 when the file is
	// absent or its metadata was never fetched.
	DestSize int64 `json:"dest_size"`
	// DestModTime is the destination modification time; zero when unknown.
	DestModTime time.Time `json:"dest_mod_time"`
	// Backend owns this file for the run; no other backend writes it.
	Backend Backend `json:"backend"`
	// Status is the task lifecycle state.
	Status Status `json:"status"`
	// Attempts counts started transfer attempts.
	Attempts int `json:"attempts"`
	// Checkpoint is the path of the backend checkpoint file (for example an
	// mscp -W checkpoint) used to resume this task, empty when none exists.
	Checkpoint string `json:"checkpoint,omitempty"`
	// Error records the most recent failure or interruption.
	Error string `json:"error,omitempty"`
}

// validate checks the task schema invariants, including that Path is a
// relative path that cannot escape the source or destination root.
func (t *Task) validate() error {
	if t.Path == "" {
		return errors.New("path is empty")
	}
	if strings.ContainsRune(t.Path, 0) {
		return fmt.Errorf("path %q contains a NUL byte", t.Path)
	}
	if strings.HasPrefix(t.Path, "/") {
		return fmt.Errorf("path %q is absolute; want relative", t.Path)
	}
	for _, elem := range strings.Split(t.Path, "/") {
		if elem == ".." {
			return fmt.Errorf("path %q contains a parent-directory element", t.Path)
		}
	}
	if t.Size < 0 {
		return fmt.Errorf("size %d is negative", t.Size)
	}
	if !t.Backend.Valid() {
		return fmt.Errorf("invalid backend %q", t.Backend)
	}
	if !t.Status.Valid() {
		return fmt.Errorf("invalid status %q", t.Status)
	}
	if t.Attempts < 0 {
		return fmt.Errorf("attempts %d is negative", t.Attempts)
	}
	return nil
}

// Run is one versioned CatSync run document.
type Run struct {
	// Version is the schema version; Save always stamps SchemaVersion.
	Version int `json:"version"`
	// ID is the unique run identifier from NewID; it doubles as the file
	// name and must satisfy ValidateID.
	ID string `json:"id"`
	// Source is the local tree being transferred.
	Source Source `json:"source"`
	// Destination is the remote tree being written.
	Destination Destination `json:"destination"`
	// WorkDir is the original working directory backends must run in;
	// checkpoint resume depends on it.
	WorkDir string `json:"work_dir"`
	// Config is the immutable transfer configuration.
	Config Config `json:"config"`
	// FPSyncRunID, FPSyncStateDir, and FPSyncOutput are LEGACY fields
	// written by the removed external-fpsync implementation. They remain
	// so old run documents still load; new runs never write them, native
	// resume does not read them, and a successful completion clears them.
	FPSyncRunID string `json:"fpsync_run_id,omitempty"`
	// FPSyncStateDir is the legacy fpsync -t directory; see FPSyncRunID.
	FPSyncStateDir string `json:"fpsync_state_dir,omitempty"`
	// FPSyncOutput is the legacy preserved fpsync stdout; see FPSyncRunID.
	FPSyncOutput string `json:"fpsync_output,omitempty"`
	// Status is the run lifecycle state.
	Status Status `json:"status"`
	// CreatedAt records when the run was created (UTC).
	CreatedAt time.Time `json:"created_at"`
	// UpdatedAt records the last save (UTC); Save always stamps it.
	UpdatedAt time.Time `json:"updated_at"`
	// FinishedAt records completion; nil while the run is unfinished.
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	// Error records the most recent run-level failure or interruption.
	Error string `json:"error,omitempty"`
	// Tasks holds the per-file units of work.
	Tasks []Task `json:"tasks"`
}

// validate checks the run schema invariants relied on by Load and Save.
func (r *Run) validate() error {
	if r.Version != SchemaVersion {
		return fmt.Errorf("%w: %d (this build writes %d)", ErrUnsupportedVersion, r.Version, SchemaVersion)
	}
	if err := ValidateID(r.ID); err != nil {
		return err
	}
	if !r.Status.Valid() {
		return fmt.Errorf("state: run %s: invalid status %q", r.ID, r.Status)
	}
	if r.Source.Path == "" {
		return fmt.Errorf("state: run %s: source path is empty", r.ID)
	}
	if r.Destination.Host == "" || r.Destination.Path == "" {
		return fmt.Errorf("state: run %s: destination host and path are required", r.ID)
	}
	if r.WorkDir == "" {
		return fmt.Errorf("state: run %s: working directory is empty", r.ID)
	}
	if r.CreatedAt.IsZero() {
		return fmt.Errorf("state: run %s: created_at is zero", r.ID)
	}
	if err := r.Config.validate(); err != nil {
		return fmt.Errorf("state: run %s: %w", r.ID, err)
	}
	for i := range r.Tasks {
		if err := r.Tasks[i].validate(); err != nil {
			return fmt.Errorf("state: run %s: task %d (%s): %w", r.ID, i, r.Tasks[i].Path, err)
		}
	}
	return nil
}

// interruptedMsg is recorded as the last error of tasks and runs recovered
// from an interrupted process.
const interruptedMsg = "interrupted: catsync stopped before finishing; safe to resume"

// RecoverInterrupted repairs leftover running state from a process that
// stopped mid-run: every running task returns to pending with the
// interruption recorded as its last error, and a running run is marked
// failed with the interruption recorded, which leaves it retryable through
// resume. It reports whether anything changed, so callers can persist the
// recovery only when needed.
func (r *Run) RecoverInterrupted() bool {
	changed := false
	for i := range r.Tasks {
		t := &r.Tasks[i]
		if t.Status == StatusRunning {
			t.Status = StatusPending
			t.Error = interruptedMsg
			changed = true
		}
	}
	if r.Status == StatusRunning {
		r.Status = StatusFailed
		r.Error = interruptedMsg
		r.FinishedAt = nil
		changed = true
	}
	return changed
}

// Resumable reports whether the run still has work resume can pick up: the
// run is not completed and at least one task is pending, running, or failed.
func (r *Run) Resumable() bool {
	if r.Status == StatusCompleted {
		return false
	}
	for _, t := range r.Tasks {
		switch t.Status {
		case StatusPending, StatusRunning, StatusFailed:
			return true
		}
	}
	return false
}

// maxIDLen bounds run IDs so state file names stay portable.
const maxIDLen = 128

// idPattern allows one portable file-name component: ASCII letters, digits,
// dot, underscore, and dash, never starting with a dot (so IDs are visible
// and cannot collide with hidden temporary files).
var idPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// NewID returns a unique run identifier: a compact UTC timestamp for
// rough sortability plus 64 bits of cryptographic randomness for uniqueness.
func NewID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("state: generate run id: %w", err)
	}
	return time.Now().UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(b[:]), nil
}

// ValidateID rejects run IDs that are not safe single file names: path
// separators, NUL bytes, parent-directory elements, leading dots, and
// overlong IDs are all refused, so an ID can never escape the state
// directory.
func ValidateID(id string) error {
	if id == "" {
		return fmt.Errorf("%w: empty id", ErrInvalidID)
	}
	if len(id) > maxIDLen {
		return fmt.Errorf("%w: %q is longer than %d bytes", ErrInvalidID, id, maxIDLen)
	}
	if !idPattern.MatchString(id) {
		return fmt.Errorf("%w: %q: use ASCII letters, digits, '.', '_', '-' and no leading dot", ErrInvalidID, id)
	}
	if strings.Contains(id, "..") {
		return fmt.Errorf("%w: %q: parent-directory elements are not allowed", ErrInvalidID, id)
	}
	return nil
}
