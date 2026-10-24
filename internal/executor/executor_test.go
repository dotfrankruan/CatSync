package executor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dotfrankruan/CatSync/internal/planner"
	"github.com/dotfrankruan/CatSync/internal/state"
	"github.com/dotfrankruan/CatSync/internal/target"
)

func validTestConfig(t *testing.T) Config {
	t.Helper()
	return Config{
		SourceDir:      t.TempDir(),
		Dest:           target.Destination{User: "u", Host: "h", Path: "/data"},
		Backend:        state.BackendAuto,
		Verify:         state.VerifySize,
		Jobs:           2,
		Streams:        2,
		LargeThreshold: 1024,
		StateDir:       t.TempDir(),
	}
}

func TestValidateFull(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		cfg := validTestConfig(t)
		if err := cfg.validateFull(); err != nil {
			t.Errorf("valid config rejected: %v", err)
		}
	})
	cases := []struct {
		name   string
		mutate func(*Config)
		want   error
	}{
		{"bad backend", func(c *Config) { c.Backend = "bogus" }, ErrInvalidConfig},
		{"bad verify", func(c *Config) { c.Verify = "md5" }, ErrInvalidConfig},
		{"sha256 accepted", func(c *Config) { c.Verify = state.VerifySHA256 }, nil},
		{"auto accepted", func(c *Config) { c.Verify = state.VerifyAuto }, nil},
		{"zero jobs", func(c *Config) { c.Jobs = 0 }, ErrInvalidConfig},
		{"zero streams", func(c *Config) { c.Streams = 0 }, ErrInvalidConfig},
		{"connection budget", func(c *Config) { c.Jobs = 16; c.Streams = 16 }, ErrInvalidConfig},
		{"budget boundary", func(c *Config) { c.Jobs = 64; c.Streams = 1 }, nil},
		{"empty state dir", func(c *Config) { c.StateDir = "" }, ErrInvalidConfig},
		{"relative source", func(c *Config) { c.SourceDir = "rel" }, ErrInvalidConfig},
		{"bad destination", func(c *Config) { c.Dest.Path = "rel" }, ErrInvalidConfig},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validTestConfig(t)
			tc.mutate(&cfg)
			err := cfg.validateFull()
			if tc.want == nil {
				if err != nil {
					t.Fatalf("config rejected: %v", err)
				}
				return
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want it to wrap %v", err, tc.want)
			}
		})
	}
}

// TestBuildRunMapping checks the planner-decision → task mapping, including
// the distinct unsupported status with the planner's reason.
func TestBuildRunMapping(t *testing.T) {
	cfg := validTestConfig(t)
	e, err := newExecutor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	out := &PlanOutcome{Result: &planner.Result{Decisions: []planner.Decision{
		{Path: "new-small.txt", Size: 10, ModTime: time.Unix(100, 0), Backend: state.BackendFPSync, Status: planner.StatusPending, Reason: "new"},
		{Path: "same.txt", Size: 5, ModTime: time.Unix(90, 0), Backend: state.BackendRsync, Status: planner.StatusSkipped, Reason: "unchanged"},
		{Path: "link", Backend: state.BackendRsync, Status: planner.StatusUnsupported, Reason: "symlinks are unsupported"},
	}}}
	run, err := e.buildRun(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(run.Tasks) != 3 {
		t.Fatalf("got %d tasks, want 3", len(run.Tasks))
	}
	if run.Tasks[0].Status != state.StatusPending || run.Tasks[0].Backend != state.BackendFPSync {
		t.Errorf("task 0 = %+v", run.Tasks[0])
	}
	if run.Tasks[1].Status != state.StatusSkipped || run.Tasks[1].Error != "" {
		t.Errorf("unchanged task = %+v, want clean skip", run.Tasks[1])
	}
	if run.Tasks[2].Status != state.StatusUnsupported || !strings.Contains(run.Tasks[2].Error, "symlinks") {
		t.Errorf("unsupported task = %+v, want unsupported with the planner reason", run.Tasks[2])
	}
	if run.Tasks[2].Status == state.StatusSkipped {
		t.Error("unsupported entry collapsed into skipped")
	}
	if run.Tasks[0].DestSize != -1 {
		t.Errorf("DestSize = %d, want -1 (unknown)", run.Tasks[0].DestSize)
	}
	if run.WorkDir != run.Source.Path {
		t.Errorf("WorkDir = %q, want the source path %q (mscp checkpoint cwd)", run.WorkDir, run.Source.Path)
	}
	if run.ID == "" || run.CreatedAt.IsZero() {
		t.Error("run id or created_at missing")
	}
}

// TestSourceMutationDetection exercises the pre/post transfer source
// revalidation gates without any subprocess.
func TestSourceMutationDetection(t *testing.T) {
	cfg := validTestConfig(t)
	e, err := newExecutor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e.run = &state.Run{Source: state.Source{Path: cfg.SourceDir}}
	// These tests exercise mutation detection, not persistence: stub the
	// save so task transitions do not depend on a complete run document.
	e.saveFn = func(*state.Run) error { return nil }

	write := func(content string) {
		t.Helper()
		if err := os.MkdirAll(cfg.SourceDir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(cfg.SourceDir, "f.txt"), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("hello")
	fi, err := os.Stat(filepath.Join(cfg.SourceDir, "f.txt"))
	if err != nil {
		t.Fatal(err)
	}
	task := &state.Task{Path: "f.txt", Size: fi.Size(), ModTime: fi.ModTime(), Backend: state.BackendRsync, Status: state.StatusPending}
	if ok, err := e.recheckBefore(task); err != nil || !ok {
		t.Fatalf("unchanged source failed pre-check: ok=%v err=%v", ok, err)
	}
	// Mutate the source: the task must fail, never transfer.
	write("hello, longer now")
	if ok, err := e.recheckBefore(task); err != nil || ok {
		t.Fatalf("mutated source passed pre-check: ok=%v err=%v", ok, err)
	}
	if task.Status != state.StatusFailed {
		t.Errorf("task status = %s, want failed", task.Status)
	}

	// A vanished source must also fail.
	task2 := &state.Task{Path: "gone.txt", Size: 1, ModTime: fi.ModTime(), Backend: state.BackendRsync, Status: state.StatusPending}
	if ok, err := e.recheckBefore(task2); err != nil || ok {
		t.Fatalf("missing source passed pre-check: ok=%v err=%v", ok, err)
	}
	if task2.Status != state.StatusFailed {
		t.Errorf("task2 status = %s, want failed", task2.Status)
	}
}

// TestRecheckAfterCatchesMidTransferChange simulates a source that changes
// between the pre-check and the post-check.
func TestRecheckAfterCatchesMidTransferChange(t *testing.T) {
	cfg := validTestConfig(t)
	e, err := newExecutor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e.run = &state.Run{Source: state.Source{Path: cfg.SourceDir}}
	e.saveFn = func(*state.Run) error { return nil }
	p := filepath.Join(cfg.SourceDir, "f.txt")
	if err := os.WriteFile(p, []byte("v1"), 0o644); err != nil {
		t.Fatal(err)
	}
	fi, _ := os.Stat(p)
	task := &state.Task{Path: "f.txt", Size: fi.Size(), ModTime: fi.ModTime(), Backend: state.BackendRsync, Status: state.StatusRunning}
	if ok, err := e.recheckAfter(task); err != nil || !ok {
		t.Fatalf("unchanged source failed post-check: ok=%v err=%v", ok, err)
	}
	if err := os.WriteFile(p, []byte("v2 changed"), 0o644); err != nil {
		t.Fatal(err)
	}
	if ok, err := e.recheckAfter(task); err != nil || ok {
		t.Fatalf("changed source passed post-check: ok=%v err=%v", ok, err)
	}
	if task.Status != state.StatusFailed {
		t.Errorf("task status = %s, want failed", task.Status)
	}
}

// TestLatestResumable exercises run selection without any subprocess.
func TestLatestResumable(t *testing.T) {
	cfg := validTestConfig(t)
	e, err := newExecutor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.latestResumable(); !errors.Is(err, state.ErrNoRuns) {
		t.Fatalf("empty store = %v, want ErrNoRuns", err)
	}
	mk := func(status state.Status, taskStatus state.Status) *state.Run {
		t.Helper()
		id, err := state.NewID()
		if err != nil {
			t.Fatal(err)
		}
		r := &state.Run{
			ID:          id,
			Source:      state.Source{Path: cfg.SourceDir},
			Destination: state.Destination{Host: "h", Path: "/d"},
			WorkDir:     cfg.SourceDir,
			Config: state.Config{
				Backend: state.BackendAuto, Verify: state.VerifySize,
				Jobs: 1, Streams: 1, LargeThreshold: 1,
			},
			Status:    status,
			CreatedAt: time.Now().UTC(),
			Tasks:     []state.Task{{Path: "f", Backend: state.BackendRsync, Status: taskStatus}},
		}
		if err := e.store.Save(r); err != nil {
			t.Fatal(err)
		}
		return r
	}
	mk(state.StatusCompleted, state.StatusCompleted)
	pending := mk(state.StatusFailed, state.StatusFailed)
	got, err := e.latestResumable()
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != pending.ID {
		t.Errorf("latestResumable = %s, want %s", got.ID, pending.ID)
	}
}

// TestResumeRejectsCompletedRun checks the no-work-left error path.
func TestResumeRejectsCompletedRun(t *testing.T) {
	cfg := validTestConfig(t)
	e, err := newExecutor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := state.NewID()
	r := &state.Run{
		ID:          id,
		Source:      state.Source{Path: cfg.SourceDir},
		Destination: state.Destination{Host: "h", Path: "/d"},
		WorkDir:     cfg.SourceDir,
		Config: state.Config{
			Backend: state.BackendAuto, Verify: state.VerifySize,
			Jobs: 1, Streams: 1, LargeThreshold: 1,
		},
		Status:    state.StatusCompleted,
		CreatedAt: time.Now().UTC(),
		Tasks:     []state.Task{{Path: "f", Backend: state.BackendRsync, Status: state.StatusCompleted}},
	}
	if err := e.store.Save(r); err != nil {
		t.Fatal(err)
	}
	_, err = Resume(context.Background(), Config{StateDir: cfg.StateDir}, id)
	if !errors.Is(err, ErrNotResumable) {
		t.Fatalf("Resume = %v, want ErrNotResumable", err)
	}
	if _, err := Resume(context.Background(), Config{StateDir: cfg.StateDir}, "no-such-run"); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("Resume missing run = %v, want ErrNotFound", err)
	}
}

// TestValidateRejectsUnexpressibleSSHOverride checks the configuration gate:
// an ssh override rsync's -e cannot represent fails as a configuration
// error instead of silently running a different transport command.
func TestValidateRejectsUnexpressibleSSHOverride(t *testing.T) {
	cfg := validTestConfig(t)
	cfg.Programs.SSH = "/opt/a b/ssh"
	err := cfg.validateFull()
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("validateFull = %v, want ErrInvalidConfig", err)
	}
	cfg.Programs.SSH = "/opt/mock/ssh"
	if err := cfg.validateFull(); err != nil {
		t.Fatalf("valid ssh override rejected: %v", err)
	}
	// A NUL-bearing override is rejected as a configuration error.
	cfg.Programs.SSH = "/tmp/a" + string(rune(0)) + "b"
	if err := cfg.validateFull(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("NUL program override = %v, want ErrInvalidConfig", err)
	}
}

// TestRevertToPendingMutatesOnlyRunningTasks: a successful revert flips
// running tasks to pending with the cancellation note in exactly one save,
// keeps their attempt counts, and leaves tasks in any other status
// untouched.
func TestRevertToPendingMutatesOnlyRunningTasks(t *testing.T) {
	cfg := validTestConfig(t)
	e, err := newExecutor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e.run = &state.Run{ID: "run-revert", Source: state.Source{Path: cfg.SourceDir}}
	tasks := []*state.Task{
		{Path: "a.txt", Status: state.StatusRunning, Attempts: 1},
		{Path: "b.txt", Status: state.StatusCompleted, Attempts: 1},
		{Path: "c.txt", Status: state.StatusFailed, Attempts: 2, Error: "boom"},
	}
	saves := 0
	e.saveFn = func(*state.Run) error { saves++; return nil }

	if err := e.revertToPending(tasks, "cancelled: safe to resume"); err != nil {
		t.Fatalf("revertToPending: %v", err)
	}
	if saves != 1 {
		t.Errorf("save calls = %d, want exactly 1 (single batch save)", saves)
	}
	if got := tasks[0]; got.Status != state.StatusPending || got.Error != "cancelled: safe to resume" || got.Attempts != 1 {
		t.Errorf("running task after revert = %+v, want pending with the note and 1 attempt", got)
	}
	if got := tasks[1]; got.Status != state.StatusCompleted || got.Error != "" || got.Attempts != 1 {
		t.Errorf("completed task after revert = %+v, want untouched", got)
	}
	if got := tasks[2]; got.Status != state.StatusFailed || got.Error != "boom" || got.Attempts != 2 {
		t.Errorf("failed task after revert = %+v, want untouched", got)
	}
}

// TestRevertToPendingSaveFailureRollsBack is the atomic-cancellation
// regression: the revert is one batch transition, so when its single
// persistence call fails, revertToPending returns the error and every
// task's in-memory Status/Attempts/Error is restored to its pre-revert
// running values. Memory must never claim pending while the durable record
// may still say running.
func TestRevertToPendingSaveFailureRollsBack(t *testing.T) {
	cfg := validTestConfig(t)
	e, err := newExecutor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e.run = &state.Run{ID: "run-revert", Source: state.Source{Path: cfg.SourceDir}}
	tasks := []*state.Task{
		{Path: "a.txt", Status: state.StatusRunning, Attempts: 1},
		{Path: "b.txt", Status: state.StatusRunning, Attempts: 3, Error: "previous attempt note"},
		{Path: "c.txt", Status: state.StatusPending}, // not running: untouched
	}
	injected := errors.New("injected persistence failure")
	saves := 0
	var observed []state.Status
	e.saveFn = func(*state.Run) error {
		saves++
		// The single batch save must observe the fully reverted batch:
		// mutation and persistence happen in one critical section.
		observed = []state.Status{tasks[0].Status, tasks[1].Status, tasks[2].Status}
		return injected
	}

	err = e.revertToPending(tasks, "cancelled: safe to resume")
	if !errors.Is(err, injected) {
		t.Fatalf("revertToPending = %v, want the persistence error", err)
	}
	if saves != 1 {
		t.Errorf("save calls = %d, want exactly 1 (single batch save)", saves)
	}
	wantObserved := []state.Status{state.StatusPending, state.StatusPending, state.StatusPending}
	if !reflect.DeepEqual(observed, wantObserved) {
		t.Errorf("save observed statuses %v, want %v", observed, wantObserved)
	}

	// The failed save rolls the whole batch back in memory.
	if got := tasks[0].Status; got != state.StatusRunning {
		t.Errorf("status after failed save = %s, want rollback to running", got)
	}
	if got := tasks[0]; got.Attempts != 1 || got.Error != "" {
		t.Errorf("task a.txt after failed save = attempts %d error %q, want rollback to 1/\"\"", got.Attempts, got.Error)
	}
	if got := tasks[1]; got.Status != state.StatusRunning || got.Attempts != 3 || got.Error != "previous attempt note" {
		t.Errorf("task b.txt after failed save = %+v, want rollback to running/3/\"previous attempt note\"", got)
	}
	if got := tasks[2]; got.Status != state.StatusPending || got.Attempts != 0 || got.Error != "" {
		t.Errorf("untouched task c.txt after failed save = %+v, want pending/0/\"\"", got)
	}
}
