package state_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dotfrankruan/CatSync/internal/state"
)

// fixedTime has no monotonic clock reading, so JSON round trips compare
// equal to the original values.
var fixedTime = time.Date(2025, 10, 18, 12, 0, 0, 0, time.UTC)

// testRun returns a complete, valid run ready to save.
func testRun(id string) *state.Run {
	return &state.Run{
		ID:     id,
		Status: state.StatusPending,
		Source: state.Source{Path: "/srv/data"},
		Destination: state.Destination{
			User: "bob",
			Host: "example.com",
			Path: "/backups/data",
		},
		WorkDir: "/srv",
		Config: state.Config{
			Backend:        state.BackendAuto,
			Verify:         state.VerifySize,
			Jobs:           4,
			Streams:        4,
			LargeThreshold: 512 << 20,
		},
		CreatedAt: fixedTime,
		Tasks: []state.Task{
			{
				Path:     "dir/a.txt",
				Size:     10,
				ModTime:  fixedTime,
				DestSize: -1,
				Backend:  state.BackendRsync,
				Status:   state.StatusPending,
			},
		},
	}
}

func TestNewIDUniqueAndValid(t *testing.T) {
	seen := make(map[string]bool, 1024)
	for i := 0; i < 1024; i++ {
		id, err := state.NewID()
		if err != nil {
			t.Fatalf("NewID: %v", err)
		}
		if err := state.ValidateID(id); err != nil {
			t.Fatalf("NewID produced invalid id %q: %v", id, err)
		}
		if seen[id] {
			t.Fatalf("NewID produced duplicate id %q", id)
		}
		seen[id] = true
	}
}

func TestValidateID(t *testing.T) {
	valid := []string{
		"20251018T120000Z-0123456789abcdef",
		"run-1",
		"a.b_c",
		strings.Repeat("x", 128),
	}
	for _, id := range valid {
		if err := state.ValidateID(id); err != nil {
			t.Errorf("ValidateID(%q) = %v, want nil", id, err)
		}
	}
	invalid := []string{
		"",                       // empty
		".",                      // current directory
		"..",                     // parent directory
		"../evil",                // traversal up
		"a/../b",                 // traversal in the middle
		"a/b",                    // subdirectory escape
		`a\b`,                    // Windows separator
		"/etc/passwd",            // absolute path
		".hidden",                // collides with temporary files
		"a..b",                   // embedded parent element
		"a b",                    // whitespace
		"a\x00b",                 // NUL
		"run:1",                  // colon
		strings.Repeat("x", 129), // too long
	}
	for _, id := range invalid {
		if err := state.ValidateID(id); err == nil {
			t.Errorf("ValidateID(%q) = nil, want error", id)
		}
	}
}

func TestDestinationStringCanonical(t *testing.T) {
	plain := state.Destination{User: "bob", Host: "example.com", Path: "/data"}
	if got := plain.String(); got != "bob@example.com:/data" {
		t.Errorf("String() = %q", got)
	}
	v6 := state.Destination{Host: "2001:db8::1", Path: "/data"}
	v6bracketed := state.Destination{Host: "[2001:db8::1]", Path: "/data"}
	if v6.String() != v6bracketed.String() {
		t.Errorf("bracket spellings differ: %q vs %q", v6.String(), v6bracketed.String())
	}
	if got := v6.String(); got != "[2001:db8::1]:/data" {
		t.Errorf("String() = %q", got)
	}
}

func TestRecoverInterrupted(t *testing.T) {
	r := testRun("20251018T120000Z-deadbeefdeadbeef")
	r.Status = state.StatusRunning
	r.Tasks = []state.Task{
		{Path: "running-1", Size: 1, ModTime: fixedTime, DestSize: -1, Backend: state.BackendRsync, Status: state.StatusRunning},
		{Path: "running-2", Size: 2, ModTime: fixedTime, DestSize: -1, Backend: state.BackendMSCP, Status: state.StatusRunning, Attempts: 1, Checkpoint: "/tmp/chk"},
		{Path: "done", Size: 3, ModTime: fixedTime, DestSize: 3, Backend: state.BackendFPSync, Status: state.StatusCompleted},
		{Path: "failed", Size: 4, ModTime: fixedTime, DestSize: -1, Backend: state.BackendRsync, Status: state.StatusFailed, Error: "boom"},
		{Path: "skipped", Size: 5, ModTime: fixedTime, DestSize: 5, Backend: state.BackendRsync, Status: state.StatusSkipped},
		{Path: "pending", Size: 6, ModTime: fixedTime, DestSize: -1, Backend: state.BackendRsync, Status: state.StatusPending},
	}
	if !r.RecoverInterrupted() {
		t.Fatal("RecoverInterrupted reported no change")
	}
	if r.Status != state.StatusFailed {
		t.Errorf("run status = %q, want failed", r.Status)
	}
	if !strings.Contains(r.Error, "interrupted") {
		t.Errorf("run error = %q, want an interruption note", r.Error)
	}
	for i, want := range []state.Status{
		state.StatusPending, state.StatusPending, state.StatusCompleted,
		state.StatusFailed, state.StatusSkipped, state.StatusPending,
	} {
		if got := r.Tasks[i].Status; got != want {
			t.Errorf("task %d status = %q, want %q", i, got, want)
		}
	}
	for _, i := range []int{0, 1} {
		if !strings.Contains(r.Tasks[i].Error, "interrupted") {
			t.Errorf("task %d error = %q, want an interruption note", i, r.Tasks[i].Error)
		}
	}
	// Untouched tasks keep their recorded state.
	if r.Tasks[3].Error != "boom" {
		t.Errorf("failed task error changed: %q", r.Tasks[3].Error)
	}
	// Attempt counts and checkpoint references survive recovery so resume
	// can pick up where the process stopped.
	if r.Tasks[1].Attempts != 1 || r.Tasks[1].Checkpoint != "/tmp/chk" {
		t.Errorf("recovered task lost attempts/checkpoint: %+v", r.Tasks[1])
	}
	if !r.Resumable() {
		t.Error("recovered failed run with pending work is not resumable")
	}
	// Recovery is idempotent.
	if r.RecoverInterrupted() {
		t.Error("second RecoverInterrupted reported a change")
	}
}

func TestRecoverInterruptedNoop(t *testing.T) {
	r := testRun("20251018T120000Z-0000000000000001")
	if r.RecoverInterrupted() {
		t.Error("RecoverInterrupted changed a pending run")
	}
	if r.Error != "" || r.Tasks[0].Error != "" {
		t.Error("RecoverInterrupted recorded errors on a clean run")
	}
}

func TestResumable(t *testing.T) {
	r := testRun("20251018T120000Z-0000000000000002")
	if !r.Resumable() {
		t.Error("pending run is not resumable")
	}
	r.Tasks[0].Status = state.StatusCompleted
	if r.Resumable() {
		t.Error("run with only completed work is resumable")
	}
	r.Status = state.StatusCompleted
	r.Tasks[0].Status = state.StatusPending
	if r.Resumable() {
		t.Error("completed run is resumable")
	}
}

func TestSaveRejectsInvalidRuns(t *testing.T) {
	dir := t.TempDir()
	s, err := state.NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	cases := map[string]func(r *state.Run){
		"bad status":         func(r *state.Run) { r.Status = "exploded" },
		"empty source":       func(r *state.Run) { r.Source.Path = "" },
		"empty dest host":    func(r *state.Run) { r.Destination.Host = "" },
		"empty dest path":    func(r *state.Run) { r.Destination.Path = "" },
		"empty workdir":      func(r *state.Run) { r.WorkDir = "" },
		"zero created_at":    func(r *state.Run) { r.CreatedAt = time.Time{} },
		"bad backend":        func(r *state.Run) { r.Config.Backend = "teleport" },
		"bad verify":         func(r *state.Run) { r.Config.Verify = "md5" },
		"zero jobs":          func(r *state.Run) { r.Config.Jobs = 0 },
		"task bad backend":   func(r *state.Run) { r.Tasks[0].Backend = "teleport" },
		"task bad status":    func(r *state.Run) { r.Tasks[0].Status = "exploded" },
		"task abs path":      func(r *state.Run) { r.Tasks[0].Path = "/etc/passwd" },
		"task dotdot path":   func(r *state.Run) { r.Tasks[0].Path = "../escape" },
		"task nested dotdot": func(r *state.Run) { r.Tasks[0].Path = "a/../../escape" },
		"task empty path":    func(r *state.Run) { r.Tasks[0].Path = "" },
		"task negative size": func(r *state.Run) { r.Tasks[0].Size = -5 },
	}
	for name, mutate := range cases {
		r := testRun("20251018T120000Z-0000000000000003")
		mutate(r)
		if err := s.Save(r); err == nil {
			t.Errorf("%s: Save succeeded, want error", name)
		}
	}
}

// TestUnsupportedStatusIsDistinct pins the unsupported task status: it is a
// valid, persisted status distinct from skipped, and never counts as
// resumable work or a completed transfer.
func TestUnsupportedStatusIsDistinct(t *testing.T) {
	if !state.StatusUnsupported.Valid() {
		t.Fatal("unsupported status is not valid")
	}
	if state.StatusUnsupported == state.StatusSkipped {
		t.Fatal("unsupported collides with skipped")
	}
	r := testRun("20251019T120000Z-0123456789abcdef")
	r.Status = state.StatusCompleted
	r.Tasks = []state.Task{
		{Path: "link", Size: 0, ModTime: fixedTime, DestSize: -1, Backend: state.BackendRsync, Status: state.StatusUnsupported, Error: "symlinks are unsupported"},
		{Path: "file", Size: 1, ModTime: fixedTime, DestSize: 1, Backend: state.BackendRsync, Status: state.StatusCompleted},
	}
	store, err := state.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(r); err != nil {
		t.Fatalf("run with unsupported task rejected: %v", err)
	}
	loaded, err := store.Load(r.ID)
	if err != nil {
		t.Fatalf("load run with unsupported task: %v", err)
	}
	if loaded.Tasks[0].Status != state.StatusUnsupported {
		t.Errorf("persisted task status = %q, want unsupported", loaded.Tasks[0].Status)
	}
	if r.Resumable() {
		t.Error("completed run with unsupported task must not be resumable")
	}
	if !r.RecoverInterrupted() == false {
		t.Error("RecoverInterrupted changed a run with no running tasks")
	}
	if got := r.Tasks[0].Status; got != state.StatusUnsupported {
		t.Errorf("recovery changed unsupported status to %q", got)
	}
}

// TestSaveFailsInUnwritableLocation exercises a real disk-level persistence
// failure: the state directory cannot be created, Save returns an error
// wrapping the filesystem cause, and no partial document appears.
func TestSaveFailsInUnwritableLocation(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: directory permissions are not enforced")
	}
	parent := t.TempDir()
	if err := os.Chmod(parent, 0o500); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(parent, 0o700) }()
	dir := filepath.Join(parent, "state")
	store, err := state.NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	r := testRun("20251019T120000Z-0123456789abcdef")
	err = store.Save(r)
	if err == nil {
		t.Fatal("Save into an unwritable location = nil, want error")
	}
	if !strings.Contains(err.Error(), "state:") {
		t.Errorf("error = %v, want a state persistence error", err)
	}
	// Nothing partial may remain, and loading reports the missing state
	// rather than a corrupt or half-written document.
	if entries, derr := os.ReadDir(parent); derr == nil && len(entries) != 0 {
		t.Errorf("failed save left entries in the unwritable parent: %v", entries)
	}
	if _, lerr := store.Load(r.ID); !errors.Is(lerr, state.ErrNotFound) {
		t.Errorf("Load after failed save = %v, want ErrNotFound", lerr)
	}
}
