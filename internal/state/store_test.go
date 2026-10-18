package state_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/frankruan/catsync/internal/state"
)

// sameJSON fails the test unless want and got encode to identical JSON,
// which sidesteps time.Location and monotonic-clock comparison pitfalls.
func sameJSON(t *testing.T, want, got any) {
	t.Helper()
	w, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal want: %v", err)
	}
	g, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal got: %v", err)
	}
	if !bytes.Equal(w, g) {
		t.Errorf("JSON mismatch:\nwant: %s\ngot:  %s", w, g)
	}
}

func newStore(t *testing.T) *state.Store {
	t.Helper()
	s, err := state.NewStore(filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return s
}

func TestSaveLoadRoundtrip(t *testing.T) {
	s := newStore(t)
	r := testRun("20251018T120000Z-1111111111111111")
	r.Status = state.StatusRunning
	r.Config.DryRun = true
	r.Config.IPv6 = true
	r.Config.Verify = state.VerifySHA256
	r.Error = "remote manifest unavailable; rsync fallback"
	finished := fixedTime.Add(time.Hour)
	r.FinishedAt = &finished
	r.Tasks = append(r.Tasks, state.Task{
		Path:        "big.bin",
		Size:        1 << 30,
		ModTime:     fixedTime.Add(-time.Hour),
		DestSize:    1 << 20,
		DestModTime: fixedTime.Add(-2 * time.Hour),
		Backend:     state.BackendMSCP,
		Status:      state.StatusFailed,
		Attempts:    2,
		Checkpoint:  "/state/checkpoints/big.bin.chk",
		Error:       "connection reset",
	})
	if err := s.Save(r); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if r.Version != state.SchemaVersion {
		t.Errorf("Save stamped version %d, want %d", r.Version, state.SchemaVersion)
	}
	if r.UpdatedAt.IsZero() {
		t.Error("Save did not stamp updated_at")
	}
	got, err := s.Load(r.ID)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	sameJSON(t, r, got)
}

func TestLoadNotFound(t *testing.T) {
	s := newStore(t)
	_, err := s.Load("20251018T120000Z-2222222222222222")
	if !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("Load missing run: %v, want ErrNotFound", err)
	}
}

func TestSaveAtomicOverwrite(t *testing.T) {
	s := newStore(t)
	r := testRun("20251018T120000Z-3333333333333333")
	if err := s.Save(r); err != nil {
		t.Fatalf("Save: %v", err)
	}
	r.Tasks[0].Status = state.StatusCompleted
	r.Tasks[0].Attempts = 1
	r.Status = state.StatusCompleted
	if err := s.Save(r); err != nil {
		t.Fatalf("Save overwrite: %v", err)
	}
	got, err := s.Load(r.ID)
	if err != nil {
		t.Fatalf("Load after overwrite: %v", err)
	}
	if got.Tasks[0].Status != state.StatusCompleted || got.Status != state.StatusCompleted {
		t.Errorf("overwrite not visible: task %q run %q", got.Tasks[0].Status, got.Status)
	}
	// No temporary or backup files may be left behind.
	entries, err := os.ReadDir(s.Dir())
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != r.ID+".json" {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("state directory holds %v, want exactly %s.json", names, r.ID)
	}
}

func TestSaveIntoMissingDirectory(t *testing.T) {
	// A save deep below a not-yet-existing path must create the directory.
	s, err := state.NewStore(filepath.Join(t.TempDir(), "deep", "state"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	r := testRun("20251018T120000Z-4444444444444444")
	if err := s.Save(r); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if _, err := s.Load(r.ID); err != nil {
		t.Fatalf("Load: %v", err)
	}
}

func TestLoadCorrupt(t *testing.T) {
	s := newStore(t)
	if err := os.MkdirAll(s.Dir(), 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	id := "20251018T120000Z-5555555555555555"
	cases := map[string]string{
		"not json":       "this is not json",
		"truncated":      `{"version":1,"id":"` + id,
		"trailing junk":  `{"version":1} garbage`,
		"unknown field":  `{"version":1,"id":"` + id + `","surprise":true}`,
		"mismatched id":  `{"version":1,"id":"20251018T120000Z-9999999999999999"}`,
		"invalid schema": `{"version":1,"id":"` + id + `","status":"exploded"}`,
	}
	for name, body := range cases {
		if err := os.WriteFile(filepath.Join(s.Dir(), id+".json"), []byte(body), 0o600); err != nil {
			t.Fatalf("%s: write fixture: %v", name, err)
		}
		if _, err := s.Load(id); !errors.Is(err, state.ErrCorrupt) {
			t.Errorf("%s: Load = %v, want ErrCorrupt", name, err)
		}
	}
}

func TestLoadUnsupportedVersion(t *testing.T) {
	s := newStore(t)
	if err := os.MkdirAll(s.Dir(), 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	id := "20251018T120000Z-6666666666666666"
	for _, version := range []int{0, 2, 99} {
		body := fmt.Sprintf(`{"version":%d,"id":%q}`, version, id)
		if err := os.WriteFile(filepath.Join(s.Dir(), id+".json"), []byte(body), 0o600); err != nil {
			t.Fatalf("write fixture: %v", err)
		}
		if _, err := s.Load(id); !errors.Is(err, state.ErrUnsupportedVersion) {
			t.Errorf("version %d: Load = %v, want ErrUnsupportedVersion", version, err)
		}
	}
}

func TestPathTraversalRejected(t *testing.T) {
	// A decoy file sits one level above the state directory; traversal must
	// be rejected before any path is built, so it is never read.
	root := t.TempDir()
	decoy := filepath.Join(root, "evil.json")
	if err := os.WriteFile(decoy, []byte(`{"version":1}`), 0o600); err != nil {
		t.Fatalf("write decoy: %v", err)
	}
	s, err := state.NewStore(filepath.Join(root, "state"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	bad := []string{"../evil", "..", "a/b", `a\b`, "/etc/passwd", "", ".hidden"}
	for _, id := range bad {
		if _, err := s.Load(id); !errors.Is(err, state.ErrInvalidID) {
			t.Errorf("Load(%q) = %v, want ErrInvalidID", id, err)
		}
		r := testRun("20251018T120000Z-7777777777777777")
		r.ID = id
		if err := s.Save(r); !errors.Is(err, state.ErrInvalidID) {
			t.Errorf("Save(%q) = %v, want ErrInvalidID", id, err)
		}
	}
	if _, err := s.Load("../evil"); err == nil {
		t.Error("Load of traversal id succeeded")
	}
}

func TestListAndLatest(t *testing.T) {
	s := newStore(t)
	if runs, err := s.List(); err != nil || len(runs) != 0 {
		t.Fatalf("List on empty store = %v, %v", runs, err)
	}
	if _, err := s.Latest(); !errors.Is(err, state.ErrNoRuns) {
		t.Fatalf("Latest on empty store = %v, want ErrNoRuns", err)
	}
	// Save in scrambled creation order.
	ids := []string{
		"20251018T120000Z-00000000000000c2",
		"20251018T120000Z-00000000000000a1",
		"20251018T120000Z-00000000000000b3",
	}
	created := []time.Time{
		fixedTime.Add(2 * time.Hour),
		fixedTime,
		fixedTime.Add(time.Hour),
	}
	for i, id := range ids {
		r := testRun(id)
		r.CreatedAt = created[i]
		if err := s.Save(r); err != nil {
			t.Fatalf("Save %s: %v", id, err)
		}
		// Keep updated_at ordering deterministic on coarse clocks.
		time.Sleep(2 * time.Millisecond)
	}
	runs, err := s.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(runs) != 3 {
		t.Fatalf("List returned %d runs, want 3", len(runs))
	}
	for i, want := range []string{ids[1], ids[2], ids[0]} {
		if runs[i].ID != want {
			t.Errorf("List()[%d] = %s, want %s (creation order)", i, runs[i].ID, want)
		}
	}
	latest, err := s.Latest()
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if latest.ID != ids[2] {
		t.Errorf("Latest = %s, want the most recently saved %s", latest.ID, ids[2])
	}
}

func TestListIgnoresNonRunFiles(t *testing.T) {
	s := newStore(t)
	r := testRun("20251018T120000Z-8888888888888888")
	if err := s.Save(r); err != nil {
		t.Fatalf("Save: %v", err)
	}
	strays := map[string]string{
		".save-123456": "partial temp file from a crashed save",
		"notes.txt":    "not a run document",
		"run.json.bak": "backup suffix",
		".json":        "hidden and empty id",
	}
	for name, body := range strays {
		if err := os.WriteFile(filepath.Join(s.Dir(), name), []byte(body), 0o600); err != nil {
			t.Fatalf("write stray %s: %v", name, err)
		}
	}
	runs, err := s.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(runs) != 1 || runs[0].ID != r.ID {
		t.Fatalf("List returned %d runs, want just %s", len(runs), r.ID)
	}
	// But a corrupt file that does look like a run must fail loudly.
	if err := os.WriteFile(filepath.Join(s.Dir(), "20251018T120000Z-9999999999999999.json"), []byte("junk"), 0o600); err != nil {
		t.Fatalf("write corrupt run: %v", err)
	}
	if _, err := s.List(); !errors.Is(err, state.ErrCorrupt) {
		t.Errorf("List with corrupt run = %v, want ErrCorrupt", err)
	}
}

func TestLoadAndRecoverPersistsRecovery(t *testing.T) {
	s := newStore(t)
	r := testRun("20251018T120000Z-aaaaaaaaaaaaaaaa")
	r.Status = state.StatusRunning
	r.Tasks[0].Status = state.StatusRunning
	if err := s.Save(r); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, recovered, err := s.LoadAndRecover(r.ID)
	if err != nil {
		t.Fatalf("LoadAndRecover: %v", err)
	}
	if !recovered {
		t.Fatal("LoadAndRecover reported no recovery")
	}
	if got.Status != state.StatusFailed || got.Tasks[0].Status != state.StatusPending {
		t.Errorf("recovered run = %q/%q, want failed/pending", got.Status, got.Tasks[0].Status)
	}
	if !got.Resumable() {
		t.Error("recovered run is not resumable")
	}
	// The recovery is durable: a plain load sees the recovered state.
	again, err := s.Load(r.ID)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	sameJSON(t, got, again)
	// And it is idempotent.
	_, recovered, err = s.LoadAndRecover(r.ID)
	if err != nil {
		t.Fatalf("second LoadAndRecover: %v", err)
	}
	if recovered {
		t.Error("second LoadAndRecover reported a change")
	}
}

func TestPermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permission bits do not apply on Windows")
	}
	s := newStore(t)
	r := testRun("20251018T120000Z-bbbbbbbbbbbbbbbb")
	if err := s.Save(r); err != nil {
		t.Fatalf("Save: %v", err)
	}
	assertPerm := func(path string, want os.FileMode) {
		t.Helper()
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("Stat %s: %v", path, err)
		}
		if got := info.Mode().Perm(); got != want {
			t.Errorf("%s mode = %o, want %o", path, got, want)
		}
	}
	assertPerm(s.Dir(), 0o700)
	assertPerm(filepath.Join(s.Dir(), r.ID+".json"), 0o600)

	lock, err := s.LockDestination(r.Destination)
	if err != nil {
		t.Fatalf("LockDestination: %v", err)
	}
	defer lock.Unlock()
	assertPerm(filepath.Join(s.Dir(), "locks"), 0o700)
	entries, err := os.ReadDir(filepath.Join(s.Dir(), "locks"))
	if err != nil {
		t.Fatalf("ReadDir locks: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("locks directory holds %d entries, want 1", len(entries))
	}
	assertPerm(filepath.Join(s.Dir(), "locks", entries[0].Name()), 0o600)
}

func TestSaveTightensExistingDirectoryPermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permission bits do not apply on Windows")
	}
	dir := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	s, err := state.NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	r := testRun("20251018T120000Z-cccccccccccccccc")
	if err := s.Save(r); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o700 {
		t.Fatalf("state directory mode = %o, want 700", got)
	}
}

func TestLoadRejectsSecondJSONDocument(t *testing.T) {
	s := newStore(t)
	r := testRun("20251018T120000Z-dddddddddddddddd")
	if err := s.Save(r); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(s.Dir(), r.ID+".json")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("{}\n"); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(r.ID); !errors.Is(err, state.ErrCorrupt) {
		t.Fatalf("Load with a second JSON document = %v, want ErrCorrupt", err)
	}
}

func TestConcurrentSaves(t *testing.T) {
	s := newStore(t)
	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("20251018T120000Z-%016x", i)
			r := testRun(id)
			for j := 0; j < 5; j++ {
				r.Tasks[0].Attempts = j
				if err := s.Save(r); err != nil {
					errs[i] = err
					return
				}
				if _, err := s.Load(id); err != nil {
					errs[i] = err
					return
				}
			}
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: %v", i, err)
		}
	}
	runs, err := s.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(runs) != n {
		t.Errorf("List returned %d runs, want %d", len(runs), n)
	}
}

func TestLoadAndRecoverIgnoresStrayFiles(t *testing.T) {
	// Latest/List must not trip over the locks directory created by a lock.
	s := newStore(t)
	r := testRun("20251018T120000Z-cccccccccccccccc")
	if err := s.Save(r); err != nil {
		t.Fatalf("Save: %v", err)
	}
	lock, err := s.LockDestination(r.Destination)
	if err != nil {
		t.Fatalf("LockDestination: %v", err)
	}
	defer lock.Unlock()
	if _, err := s.Latest(); err != nil {
		t.Fatalf("Latest with locks directory present: %v", err)
	}
	if strings.Contains(s.Dir(), "..") {
		t.Error("store dir contains parent elements")
	}
}
