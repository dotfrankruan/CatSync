package executor

import (
	"strings"
	"testing"

	"github.com/dotfrankruan/CatSync/internal/daemon/protocol"
	"github.com/dotfrankruan/CatSync/internal/planner"
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
