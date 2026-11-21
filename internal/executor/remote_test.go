package executor

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dotfrankruan/CatSync/internal/daemon/protocol"
	"github.com/dotfrankruan/CatSync/internal/planner"
	"github.com/dotfrankruan/CatSync/internal/state"
)

func TestValidateDaemonPathsRejectsNonUTF8(t *testing.T) {
	e := &Executor{cfg: Config{Transport: "daemon"}}
	bad := &PlanOutcome{Result: &planner.Result{Decisions: []planner.Decision{
		{Path: "ok.txt"},
		{Path: string([]byte{'b', 0xff})},
	}}}
	err := e.validateDaemonPaths(bad)
	if err == nil || !strings.Contains(err.Error(), protocol.CodeUnsupportedPathEncoding) {
		t.Fatalf("error = %v, want %s", err, protocol.CodeUnsupportedPathEncoding)
	}
	good := &PlanOutcome{Result: &planner.Result{Decisions: []planner.Decision{{Path: "ok.txt"}, {Path: "子/文件.txt"}}}}
	if err := e.validateDaemonPaths(good); err != nil {
		t.Fatalf("valid paths rejected: %v", err)
	}
}

func TestEnsureDestinationRootLocalDirectorySource(t *testing.T) {
	base := t.TempDir()
	run := &state.Run{
		Status:      state.StatusPending,
		Config:      state.Config{Direction: state.DirectionRemoteToLocal, Jobs: 1, Streams: 1},
		Source:      state.Source{Path: base, File: ""},
		Destination: state.Destination{Path: filepath.Join(base, "missing", "root")},
	}
	e := &Executor{run: run}
	if err := e.ensureDestinationRoot(context.Background()); err != nil {
		t.Fatalf("ensure missing root: %v", err)
	}
	if fi, err := os.Stat(run.Destination.Path); err != nil || !fi.IsDir() {
		t.Fatalf("root not created as directory: %v", err)
	}
	// Existing directory is accepted.
	if err := e.ensureDestinationRoot(context.Background()); err != nil {
		t.Fatalf("ensure existing dir: %v", err)
	}
	// Existing regular file is rejected and left intact.
	fileRoot := filepath.Join(base, "plainfile")
	if err := os.WriteFile(fileRoot, []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	run.Destination.Path = fileRoot
	err := e.ensureDestinationRoot(context.Background())
	if err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("existing file = %v, want not-a-directory error", err)
	}
	if data, _ := os.ReadFile(fileRoot); string(data) != "keep" {
		t.Errorf("existing file modified: %q", data)
	}
	// A symlink is not an acceptable root.
	link := filepath.Join(base, "link")
	if err := os.Symlink(filepath.Dir(fileRoot), link); err != nil {
		t.Fatal(err)
	}
	run.Destination.Path = link
	if err := e.ensureDestinationRoot(context.Background()); err == nil {
		t.Error("symlink destination root accepted")
	}
	// Single-file runs keep their point-to-point semantics: no directory is
	// created and no type check runs.
	singleDest := filepath.Join(base, "single", "file.bin")
	run.Destination.Path = singleDest
	run.Source.File = "file.bin"
	run.Config.Direction = state.DirectionLocalToRemote
	e.remote = nil
	if err := e.ensureDestinationRoot(context.Background()); err != nil {
		t.Fatalf("single-file ensure: %v", err)
	}
	if _, err := os.Stat(singleDest); !os.IsNotExist(err) {
		t.Errorf("single-file run created a directory: %v", err)
	}
}
