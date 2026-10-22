package multistream

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/frankruan/catsync/internal/discovery"
)

// CheckpointVersion is the only checkpoint schema version this build reads
// and writes.
const CheckpointVersion = 1

// Sentinel errors from checkpoint load/save; test them with errors.Is.
var (
	// ErrCheckpointNotFound reports a checkpoint file that does not exist.
	ErrCheckpointNotFound = errors.New("multistream: checkpoint not found")
	// ErrCheckpointCorrupt reports unreadable or invalid checkpoint data:
	// malformed JSON, unknown fields, trailing content, or values that fail
	// validation.
	ErrCheckpointCorrupt = errors.New("multistream: corrupt checkpoint")
	// ErrCheckpointUnsupportedVersion reports a schema version this build
	// does not understand.
	ErrCheckpointUnsupportedVersion = errors.New("multistream: unsupported checkpoint version")
)

// Checkpoint is the durable resume state of one native large-file transfer.
// It binds the exact source identity (relative path, size, nanosecond
// mtime), the canonical destination identity, the absolute remote temporary
// object path, the chunk size, and the set of completed chunk indexes.
//
// It contains no credentials, keys, file contents, or other secrets.
type Checkpoint struct {
	// Version is the schema version; always CheckpointVersion when written.
	Version int `json:"version"`
	// Source is the slash-separated path of the source file relative to the
	// transfer root.
	Source string `json:"source"`
	// Size is the source file size in bytes.
	Size uint64 `json:"size"`
	// ModTime is the source modification time, serialized with nanosecond
	// precision.
	ModTime time.Time `json:"mod_time"`
	// Destination is the canonical destination identity (for example
	// "user@host:/path"), used to refuse resuming a checkpoint against a
	// different destination.
	Destination string `json:"destination"`
	// TempPath is the absolute path of the remote temporary object.
	TempPath string `json:"temp_path"`
	// ChunkSize is the plan's chunk size in bytes.
	ChunkSize uint64 `json:"chunk_size"`
	// Completed is the ascending, duplicate-free set of completed chunk
	// indexes. Serialized deterministically ascending.
	Completed []int `json:"completed"`
}

// NewCheckpoint creates a checkpoint for a transfer with no completed
// chunks, validating every identity field.
func NewCheckpoint(source string, size uint64, modTime time.Time, destination, tempPath string, chunkSize uint64) (*Checkpoint, error) {
	c := &Checkpoint{
		Version:     CheckpointVersion,
		Source:      source,
		Size:        size,
		ModTime:     modTime.UTC(),
		Destination: destination,
		TempPath:    tempPath,
		ChunkSize:   chunkSize,
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	return c, nil
}

// validate checks every field's invariants.
func (c *Checkpoint) validate() error {
	if c.Version != CheckpointVersion {
		return fmt.Errorf("%w: %d", ErrCheckpointUnsupportedVersion, c.Version)
	}
	if err := discovery.ValidateRelPath(c.Source); err != nil {
		return fmt.Errorf("%w: source path: %v", ErrCheckpointCorrupt, err)
	}
	if c.Destination == "" {
		return fmt.Errorf("%w: destination is empty", ErrCheckpointCorrupt)
	}
	if strings.ContainsRune(c.Destination, 0) {
		return fmt.Errorf("%w: destination contains a NUL byte", ErrCheckpointCorrupt)
	}
	if c.TempPath == "" {
		return fmt.Errorf("%w: temporary path is empty", ErrCheckpointCorrupt)
	}
	if strings.ContainsRune(c.TempPath, 0) {
		return fmt.Errorf("%w: temporary path contains a NUL byte", ErrCheckpointCorrupt)
	}
	if !strings.HasPrefix(c.TempPath, "/") {
		return fmt.Errorf("%w: temporary path %q is not absolute", ErrCheckpointCorrupt, c.TempPath)
	}
	if c.ChunkSize == 0 {
		return fmt.Errorf("%w: chunk size must be positive", ErrCheckpointCorrupt)
	}
	return c.validateCompleted()
}

// validateCompleted checks the completed set: non-negative, unique, and
// within the plan for (Size, ChunkSize).
func (c *Checkpoint) validateCompleted() error {
	max := chunkCount(c.Size, c.ChunkSize)
	seen := make(map[int]struct{}, len(c.Completed))
	for _, idx := range c.Completed {
		if idx < 0 {
			return fmt.Errorf("%w: completed chunk index %d is negative", ErrCheckpointCorrupt, idx)
		}
		if uint64(idx) >= max {
			return fmt.Errorf("%w: completed chunk index %d is outside the plan of %d chunks", ErrCheckpointCorrupt, idx, max)
		}
		if _, dup := seen[idx]; dup {
			return fmt.Errorf("%w: completed chunk index %d appears twice", ErrCheckpointCorrupt, idx)
		}
		seen[idx] = struct{}{}
	}
	return nil
}

// CompletedSet returns the completed chunk indexes as a set suitable for
// CopyOptions.Completed. The result is a copy; mutating it does not affect
// the checkpoint.
func (c *Checkpoint) CompletedSet() map[int]struct{} {
	out := make(map[int]struct{}, len(c.Completed))
	for _, idx := range c.Completed {
		out[idx] = struct{}{}
	}
	return out
}

// MarkComplete records a chunk index as complete. Marking the same index
// twice is a no-op; marking an out-of-plan index is an error. The list
// stays sorted ascending.
func (c *Checkpoint) MarkComplete(index int) error {
	max := chunkCount(c.Size, c.ChunkSize)
	if index < 0 || uint64(index) >= max {
		return fmt.Errorf("multistream: chunk index %d is outside the plan of %d chunks", index, max)
	}
	i := sort.SearchInts(c.Completed, index)
	if i < len(c.Completed) && c.Completed[i] == index {
		return nil // already recorded
	}
	c.Completed = append(c.Completed, index)
	sort.Ints(c.Completed)
	return nil
}

// Save writes the checkpoint atomically and durably at path: a temporary
// file in the same directory (mode 0600), fsynced, renamed into place, and
// the directory fsynced where supported. Parent directories missing from
// the path are created with mode 0700; the permissions of directories that
// already exist are never modified. A failed save leaves any previously
// valid checkpoint at path intact.
func (c *Checkpoint) Save(path string) error {
	sort.Ints(c.Completed)
	if err := c.validate(); err != nil {
		return err
	}
	data, err := json.Marshal(c)
	if err != nil {
		return fmt.Errorf("multistream: encode checkpoint: %w", err)
	}
	data = append(data, '\n')

	dir := filepath.Dir(path)
	if err := ensurePrivateDir(dir, os.Mkdir); err != nil {
		return fmt.Errorf("multistream: create checkpoint directory: %w", err)
	}
	// CreateTemp guarantees mode 0600 and a unique hidden name in the same
	// directory, so the rename stays on one filesystem.
	tmp, err := os.CreateTemp(dir, ".catsync-ckpt-*")
	if err != nil {
		return fmt.Errorf("multistream: create temporary checkpoint file: %w", err)
	}
	tmpName := tmp.Name()
	committed := false
	defer func() {
		if !committed {
			tmp.Close()
			os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		return fmt.Errorf("multistream: write temporary checkpoint: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("multistream: sync temporary checkpoint: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("multistream: close temporary checkpoint: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("multistream: rename checkpoint into place: %w", err)
	}
	committed = true
	// Best effort: fsync the directory so the rename itself survives a
	// crash; not all platforms support it.
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// ensurePrivateDir creates the missing parents of dir with mode 0700 and
// leaves every directory that already exists untouched — in particular it
// never changes the mode of an existing directory, including the working
// directory when the checkpoint path is a bare file name. A directory is
// chmod'ed only when this call created it; when Mkdir reports ErrExist
// (another process won the creation race), the path is re-stat'ed, must be
// a directory, and is left alone.
func ensurePrivateDir(dir string, mkdir func(string, os.FileMode) error) error {
	var missing []string
	d := dir
	for {
		fi, err := os.Stat(d)
		if err == nil {
			if !fi.IsDir() {
				return fmt.Errorf("multistream: %q is not a directory", d)
			}
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("multistream: stat checkpoint directory: %w", err)
		}
		missing = append(missing, d)
		parent := filepath.Dir(d)
		if parent == d {
			break
		}
		d = parent
	}
	for i := len(missing) - 1; i >= 0; i-- {
		err := mkdir(missing[i], 0o700)
		switch {
		case err == nil:
			// We created it; enforce 0700 exactly (umask may strip bits).
			if err := os.Chmod(missing[i], 0o700); err != nil {
				return fmt.Errorf("multistream: checkpoint directory permissions: %w", err)
			}
		case errors.Is(err, os.ErrExist):
			// Another process won the race: verify it is a directory and
			// leave its permissions untouched.
			fi, statErr := os.Stat(missing[i])
			if statErr != nil {
				return fmt.Errorf("multistream: stat raced checkpoint directory %q: %w", missing[i], statErr)
			}
			if !fi.IsDir() {
				return fmt.Errorf("multistream: %q exists and is not a directory", missing[i])
			}
		default:
			return fmt.Errorf("multistream: create checkpoint directory %q: %w", missing[i], err)
		}
	}
	return nil
}

// LoadCheckpoint reads and strictly validates a checkpoint. Unknown fields,
// trailing content, malformed JSON, and invalid values are
// ErrCheckpointCorrupt; a missing file is ErrCheckpointNotFound; a version
// this build does not understand is ErrCheckpointUnsupportedVersion.
func LoadCheckpoint(path string) (*Checkpoint, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", ErrCheckpointNotFound, path)
		}
		return nil, fmt.Errorf("multistream: read checkpoint: %w", err)
	}
	var c Checkpoint
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrCheckpointCorrupt, path, err)
	}
	// Reject anything after the JSON document: a second decode must hit
	// io.EOF. Trailing whitespace is fine (the decoder skips it); any
	// trailing value or token — {} or null included — is corruption.
	var trailing json.RawMessage
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%w: %s: trailing content after JSON document", ErrCheckpointCorrupt, path)
	}
	// Report version mismatch distinctly from corruption.
	if c.Version != CheckpointVersion {
		return nil, fmt.Errorf("%w: %s: version %d", ErrCheckpointUnsupportedVersion, path, c.Version)
	}
	if err := c.validate(); err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrCheckpointCorrupt, path, err)
	}
	return &c, nil
}
