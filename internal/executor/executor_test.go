package executor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/frankruan/catsync/internal/planner"
	"github.com/frankruan/catsync/internal/state"
	"github.com/frankruan/catsync/internal/target"
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
		{"sha256 not implemented", func(c *Config) { c.Verify = state.VerifySHA256 }, ErrVerifyNotImplemented},
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
	cfg.Programs.MSCP = "/tmp/a\x00b"
	if err := cfg.validateFull(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("NUL program override = %v, want ErrInvalidConfig", err)
	}
}
