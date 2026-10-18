package state

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	// fileMode is the only permission state files are written with: owner
	// read/write only, because run state records host names and paths.
	fileMode = 0o600
	// dirMode keeps the state and lock directories traversable by their
	// owner only.
	dirMode = 0o700

	// runExt is the file extension of run documents.
	runExt = ".json"
)

// Store persists runs beneath one state directory. Its methods are safe for
// concurrent use; concurrent Saves of the same run are each atomic, with the
// last rename winning.
type Store struct {
	dir string
}

// NewStore returns a Store rooted at dir. The directory is created lazily,
// with mode 0700, on the first write.
func NewStore(dir string) (*Store, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, errors.New("state: state directory path is empty")
	}
	return &Store{dir: dir}, nil
}

// Dir returns the state directory root.
func (s *Store) Dir() string { return s.dir }

// runPath maps a run ID to its file. Callers validate the ID first, so the
// result is always inside the state directory.
func (s *Store) runPath(id string) string {
	return filepath.Join(s.dir, id+runExt)
}

// Save writes r durably and atomically. It first stamps r.Version with
// SchemaVersion and r.UpdatedAt with the current UTC time, then validates
// the whole document. The JSON is written to a temporary file in the state
// directory (mode 0600), fsynced, renamed over any previous version of the
// run, and the directory is synced where the platform supports it. A crash
// at any point leaves either the old or the new document, never a partial
// one, and a failed Save removes its temporary file.
func (s *Store) Save(r *Run) error {
	if r == nil {
		return errors.New("state: cannot save a nil run")
	}
	r.Version = SchemaVersion
	r.UpdatedAt = time.Now().UTC()
	if err := r.validate(); err != nil {
		return err
	}
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return fmt.Errorf("state: encode run %s: %w", r.ID, err)
	}
	data = append(data, '\n')
	if err := ensurePrivateDir(s.dir); err != nil {
		return fmt.Errorf("state: create state directory: %w", err)
	}
	// CreateTemp guarantees mode 0600 and a unique hidden name in the same
	// directory, so the later rename stays on one filesystem.
	tmp, err := os.CreateTemp(s.dir, ".save-*")
	if err != nil {
		return fmt.Errorf("state: create temporary file: %w", err)
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
		return fmt.Errorf("state: write temporary file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("state: sync temporary file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("state: close temporary file: %w", err)
	}
	if err := os.Rename(tmpName, s.runPath(r.ID)); err != nil {
		return fmt.Errorf("state: rename run %s into place: %w", r.ID, err)
	}
	committed = true
	// Best effort: fsync the directory so the rename itself survives a
	// crash. Some platforms and filesystems reject directory fsync; the
	// rename has already succeeded there, so the error is ignored.
	_ = syncDir(s.dir)
	return nil
}

// syncDir flushes directory metadata after a create or rename. It is
// best-effort: not every platform or filesystem supports directory fsync,
// so callers deliberately ignore the error.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// Load reads and validates the run with the given ID. The ID must satisfy
// ValidateID; a missing run wraps ErrNotFound, unparseable or schema-
// violating content wraps ErrCorrupt, and a schema version other than
// SchemaVersion wraps ErrUnsupportedVersion. Leftover running state from an
// interrupted process is returned as stored; see LoadAndRecover.
func (s *Store) Load(id string) (*Run, error) {
	if err := ValidateID(id); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(s.runPath(id))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, id)
		}
		return nil, fmt.Errorf("state: read run %s: %w", id, err)
	}
	return decodeRun(id, data)
}

// decodeRun parses and validates one run document. The schema version is
// checked before strict decoding so a future format reports a clear
// unsupported-version error instead of an unknown-field failure.
func decodeRun(id string, data []byte) (*Run, error) {
	var head struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(data, &head); err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrCorrupt, id, err)
	}
	if head.Version != SchemaVersion {
		return nil, fmt.Errorf("%w: %s: version %d (this build understands %d)",
			ErrUnsupportedVersion, id, head.Version, SchemaVersion)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var r Run
	if err := dec.Decode(&r); err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrCorrupt, id, err)
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("%w: %s: trailing data after the run document", ErrCorrupt, id)
		}
		return nil, fmt.Errorf("%w: %s: trailing data: %v", ErrCorrupt, id, err)
	}
	if r.ID != id {
		return nil, fmt.Errorf("%w: %s: file holds run %s", ErrCorrupt, id, r.ID)
	}
	if err := r.validate(); err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrCorrupt, id, err)
	}
	return &r, nil
}

// ensurePrivateDir creates path and also tightens a pre-existing directory.
// MkdirAll honors its mode only when creating a directory, so the Chmod is
// required when a user points CatSync at an existing, overly broad path.
func ensurePrivateDir(path string) error {
	if err := os.MkdirAll(path, dirMode); err != nil {
		return err
	}
	return os.Chmod(path, dirMode)
}

// LoadAndRecover loads a run and repairs leftover running state from an
// interrupted process (see Run.RecoverInterrupted), persisting the result
// atomically. The second result reports whether anything changed.
func (s *Store) LoadAndRecover(id string) (*Run, bool, error) {
	r, err := s.Load(id)
	if err != nil {
		return nil, false, err
	}
	if !r.RecoverInterrupted() {
		return r, false, nil
	}
	if err := s.Save(r); err != nil {
		return nil, false, err
	}
	return r, true, nil
}

// List returns every stored run sorted by creation time (oldest first), then
// by ID. Files that are not run documents — hidden temporary files left by a
// crashed Save, the locks directory, unrelated files — are ignored. A
// corrupt or unsupported run file fails the whole listing so damage is
// surfaced rather than hidden.
func (s *Store) List() ([]*Run, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("state: list runs: %w", err)
	}
	var runs []*Run
	for _, e := range entries {
		if e.IsDir() ||
			!strings.HasSuffix(e.Name(), runExt) ||
			strings.HasPrefix(e.Name(), ".") {
			continue
		}
		r, err := s.Load(strings.TrimSuffix(e.Name(), runExt))
		if err != nil {
			return nil, err
		}
		runs = append(runs, r)
	}
	sort.Slice(runs, func(i, j int) bool {
		a, b := runs[i], runs[j]
		if !a.CreatedAt.Equal(b.CreatedAt) {
			return a.CreatedAt.Before(b.CreatedAt)
		}
		return a.ID < b.ID
	})
	return runs, nil
}

// Latest returns the most recently updated run, breaking ties by creation
// time and then ID. It wraps ErrNoRuns when the state directory holds no
// runs.
func (s *Store) Latest() (*Run, error) {
	runs, err := s.List()
	if err != nil {
		return nil, err
	}
	if len(runs) == 0 {
		return nil, ErrNoRuns
	}
	latest := runs[0]
	for _, r := range runs[1:] {
		switch {
		case r.UpdatedAt.After(latest.UpdatedAt):
			latest = r
		case r.UpdatedAt.Equal(latest.UpdatedAt) && r.CreatedAt.After(latest.CreatedAt):
			latest = r
		case r.UpdatedAt.Equal(latest.UpdatedAt) && r.CreatedAt.Equal(latest.CreatedAt) && r.ID > latest.ID:
			latest = r
		}
	}
	return latest, nil
}
