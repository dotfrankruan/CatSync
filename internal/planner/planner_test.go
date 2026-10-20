package planner

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/frankruan/catsync/internal/discovery"
	"github.com/frankruan/catsync/internal/state"
)

var testMtime = time.Unix(1700000000, 123456789).UTC()

func file(path string, size int64) discovery.Entry {
	return discovery.Entry{Path: path, Size: size, ModTime: testMtime, Kind: discovery.KindFile}
}

func allTools() Tools {
	return Tools{Rsync: true, FPSync: true, FPart: true, MSCP: true}
}

func rsyncOnly() Tools {
	return Tools{Rsync: true}
}

// decide plans a single file and returns the decision.
func decide(t *testing.T, e discovery.Entry, dest map[string]discovery.RemoteEntry, cfg Config) Decision {
	t.Helper()
	res, err := Plan([]discovery.Entry{e}, dest, cfg)
	if err != nil {
		t.Fatalf("Plan error: %v", err)
	}
	if len(res.Decisions) != 1 {
		t.Fatalf("got %d decisions, want 1", len(res.Decisions))
	}
	return res.Decisions[0]
}

func TestAutoUnchangedSkips(t *testing.T) {
	dest := map[string]discovery.RemoteEntry{
		"same.txt": {Path: "same.txt", Size: 100, ModTime: testMtime},
	}
	d := decide(t, file("same.txt", 100), dest, Config{Backend: state.BackendAuto, Threshold: 1024, Tools: allTools()})
	if d.Status != StatusSkipped {
		t.Fatalf("status = %q, want skipped (%+v)", d.Status, d)
	}
	if d.Backend != state.BackendRsync {
		t.Errorf("skipped file nominal backend = %q, want rsync", d.Backend)
	}
	if d.Reason == "" {
		t.Error("decision has no reason")
	}
}

func TestAutoChangedMetadataGoesRsync(t *testing.T) {
	for _, tc := range []struct {
		name string
		size int64
		dest discovery.RemoteEntry
	}{
		{"size differs", 100, discovery.RemoteEntry{Path: "f", Size: 99, ModTime: testMtime}},
		{"mtime differs", 100, discovery.RemoteEntry{Path: "f", Size: 100, ModTime: testMtime.Add(time.Second)}},
		{"both differ", 1, discovery.RemoteEntry{Path: "f", Size: 100, ModTime: testMtime}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dest := map[string]discovery.RemoteEntry{"f": tc.dest}
			d := decide(t, file("f", tc.size), dest, Config{Backend: state.BackendAuto, Threshold: 1 << 20, Tools: allTools()})
			if d.Status != StatusPending || d.Backend != state.BackendRsync {
				t.Fatalf("decision = %+v, want pending rsync", d)
			}
		})
	}
}

func TestAutoNewSmallFile(t *testing.T) {
	tests := []struct {
		name  string
		tools Tools
		want  state.Backend
	}{
		{"fpsync and fpart available", allTools(), state.BackendFPSync},
		{"fpart missing", Tools{Rsync: true, FPSync: true, MSCP: true}, state.BackendRsync},
		{"fpsync missing", Tools{Rsync: true, FPart: true, MSCP: true}, state.BackendRsync},
		{"both missing", rsyncOnly(), state.BackendRsync},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dest := map[string]discovery.RemoteEntry{} // known-empty destination
			d := decide(t, file("new.txt", 512), dest, Config{Backend: state.BackendAuto, Threshold: 1024, Tools: tc.tools})
			if d.Status != StatusPending || d.Backend != tc.want {
				t.Fatalf("decision = %+v, want pending %q", d, tc.want)
			}
		})
	}
}

func TestAutoNewLargeFile(t *testing.T) {
	tests := []struct {
		name  string
		tools Tools
		want  state.Backend
	}{
		{"mscp available", allTools(), state.BackendMSCP},
		{"mscp missing", Tools{Rsync: true, FPSync: true, FPart: true}, state.BackendRsync},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dest := map[string]discovery.RemoteEntry{}
			d := decide(t, file("big.bin", 4096), dest, Config{Backend: state.BackendAuto, Threshold: 1024, Tools: tc.tools})
			if d.Status != StatusPending || d.Backend != tc.want {
				t.Fatalf("decision = %+v, want pending %q", d, tc.want)
			}
		})
	}
}

func TestAutoUnknownDestinationRoutesAllToRsync(t *testing.T) {
	// A nil dest map means the destination state is unknown (manifest
	// collection failed); every regular file falls back to rsync.
	for _, size := range []int64{0, 1, 1023, 1024, 1 << 30} {
		d := decide(t, file("f", size), nil, Config{Backend: state.BackendAuto, Threshold: 1024, Tools: allTools()})
		if d.Status != StatusPending || d.Backend != state.BackendRsync {
			t.Errorf("size %d: decision = %+v, want pending rsync", size, d)
		}
	}
}

func TestThresholdBoundaries(t *testing.T) {
	dest := map[string]discovery.RemoteEntry{}
	cfg := Config{Backend: state.BackendAuto, Threshold: 1024, Tools: allTools()}
	// Just below the threshold: small, so fpsync.
	if d := decide(t, file("at-1023", 1023), dest, cfg); d.Backend != state.BackendFPSync {
		t.Errorf("size 1023 (< threshold): backend = %q, want fpsync", d.Backend)
	}
	// Exactly at the threshold: large, so mscp.
	if d := decide(t, file("at-1024", 1024), dest, cfg); d.Backend != state.BackendMSCP {
		t.Errorf("size 1024 (== threshold): backend = %q, want mscp", d.Backend)
	}
	// A zero threshold makes even an empty file large.
	zero := Config{Backend: state.BackendAuto, Threshold: 0, Tools: allTools()}
	if d := decide(t, file("empty", 0), dest, zero); d.Backend != state.BackendMSCP {
		t.Errorf("size 0, threshold 0: backend = %q, want mscp", d.Backend)
	}
	// An empty file with a positive threshold is small.
	if d := decide(t, file("empty", 0), dest, cfg); d.Backend != state.BackendFPSync {
		t.Errorf("size 0, threshold 1024: backend = %q, want fpsync", d.Backend)
	}
}

func TestForcedRsync(t *testing.T) {
	dest := map[string]discovery.RemoteEntry{
		"same.txt": {Path: "same.txt", Size: 5, ModTime: testMtime},
		"diff.txt": {Path: "diff.txt", Size: 1, ModTime: testMtime},
	}
	entries := []discovery.Entry{file("same.txt", 5), file("diff.txt", 5), file("new.txt", 1<<30)}
	res, err := Plan(entries, dest, Config{Backend: state.BackendRsync, Threshold: 1024, Tools: rsyncOnly()})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]Decision{}
	for _, d := range res.Decisions {
		got[d.Path] = d
	}
	if got["same.txt"].Status != StatusSkipped {
		t.Errorf("same.txt = %+v, want skipped", got["same.txt"])
	}
	for _, p := range []string{"diff.txt", "new.txt"} {
		if got[p].Status != StatusPending || got[p].Backend != state.BackendRsync {
			t.Errorf("%s = %+v, want pending rsync", p, got[p])
		}
	}
}

func TestForcedFPSync(t *testing.T) {
	dest := map[string]discovery.RemoteEntry{
		"same.txt": {Path: "same.txt", Size: 5, ModTime: testMtime},
		"diff.txt": {Path: "diff.txt", Size: 1, ModTime: testMtime},
	}
	cfg := Config{Backend: state.BackendFPSync, Threshold: 1024, Tools: Tools{Rsync: true, FPSync: true, FPart: true}}
	entries := []discovery.Entry{file("same.txt", 5), file("diff.txt", 5), file("new.txt", 10)}
	res, err := Plan(entries, dest, cfg)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]Decision{}
	for _, d := range res.Decisions {
		got[d.Path] = d
	}
	if got["same.txt"].Status != StatusSkipped {
		t.Errorf("same.txt = %+v, want skipped", got["same.txt"])
	}
	for _, p := range []string{"diff.txt", "new.txt"} {
		if got[p].Status != StatusPending || got[p].Backend != state.BackendFPSync {
			t.Errorf("%s = %+v, want pending fpsync", p, got[p])
		}
	}
	// Unknown destination state is fine for forced fpsync: fpsync drives
	// rsync, which handles existing files safely.
	if d := decide(t, file("mystery.txt", 10), nil, cfg); d.Backend != state.BackendFPSync {
		t.Errorf("unknown dest, small file: backend = %q, want fpsync", d.Backend)
	}
}

func TestForcedFPSyncRejectsLarge(t *testing.T) {
	cfg := Config{Backend: state.BackendFPSync, Threshold: 1024, Tools: Tools{Rsync: true, FPSync: true, FPart: true}}
	entries := []discovery.Entry{file("small.txt", 1), file("big.bin", 1024), file("huge.bin", 4096)}
	res, err := Plan(entries, map[string]discovery.RemoteEntry{}, cfg)
	if err == nil {
		t.Fatalf("Plan succeeded, want rejection; result = %+v", res)
	}
	if res != nil {
		t.Fatalf("rejected plan returned a result: %+v", res)
	}
	if !strings.Contains(err.Error(), "big.bin") || !strings.Contains(err.Error(), "huge.bin") {
		t.Errorf("rejection does not name the large files: %v", err)
	}
	if strings.Contains(err.Error(), "small.txt") {
		t.Errorf("rejection wrongly names the small file: %v", err)
	}
	// An unchanged large file needs no transfer, so it is skipped, not
	// rejected.
	large := file("large-same.bin", 4096)
	dest := map[string]discovery.RemoteEntry{
		"large-same.bin": {Path: "large-same.bin", Size: 4096, ModTime: testMtime},
	}
	d := decide(t, large, dest, cfg)
	if d.Status != StatusSkipped {
		t.Errorf("unchanged large file = %+v, want skipped", d)
	}
}

func TestForcedMSCP(t *testing.T) {
	cfg := Config{Backend: state.BackendMSCP, Threshold: 1024, Tools: Tools{Rsync: true, MSCP: true}}
	// New files transfer with mscp at any size; unchanged files skip.
	dest := map[string]discovery.RemoteEntry{
		"same.txt": {Path: "same.txt", Size: 5, ModTime: testMtime},
	}
	res, err := Plan([]discovery.Entry{file("same.txt", 5), file("new-small.txt", 1), file("new-big.bin", 1<<30)}, dest, cfg)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]Decision{}
	for _, d := range res.Decisions {
		got[d.Path] = d
	}
	if got["same.txt"].Status != StatusSkipped {
		t.Errorf("same.txt = %+v, want skipped", got["same.txt"])
	}
	for _, p := range []string{"new-small.txt", "new-big.bin"} {
		if got[p].Status != StatusPending || got[p].Backend != state.BackendMSCP {
			t.Errorf("%s = %+v, want pending mscp", p, got[p])
		}
	}
}

func TestForcedMSCPRejectsExistingAndUnknown(t *testing.T) {
	cfg := Config{Backend: state.BackendMSCP, Threshold: 1024, Tools: Tools{Rsync: true, MSCP: true}}
	// Existing but changed.
	dest := map[string]discovery.RemoteEntry{
		"changed.txt": {Path: "changed.txt", Size: 1, ModTime: testMtime},
	}
	res, err := Plan([]discovery.Entry{file("changed.txt", 2)}, dest, cfg)
	if err == nil || res != nil {
		t.Fatalf("existing file: res=%+v err=%v, want rejection", res, err)
	}
	if !strings.Contains(err.Error(), "changed.txt") {
		t.Errorf("rejection does not name the file: %v", err)
	}
	// Unknown destination state: absence cannot be confirmed.
	res, err = Plan([]discovery.Entry{file("mystery.txt", 2)}, nil, cfg)
	if err == nil || res != nil {
		t.Fatalf("unknown destination: res=%+v err=%v, want rejection", res, err)
	}
	if !strings.Contains(err.Error(), "mystery.txt") {
		t.Errorf("rejection does not name the file: %v", err)
	}
}

func TestMissingTools(t *testing.T) {
	entries := []discovery.Entry{file("f", 1)}
	dest := map[string]discovery.RemoteEntry{}
	// rsync is required in every mode: all fallbacks land on it.
	for _, backend := range []state.Backend{state.BackendAuto, state.BackendRsync, state.BackendFPSync, state.BackendMSCP} {
		t.Run("no rsync/"+string(backend), func(t *testing.T) {
			tools := allTools()
			tools.Rsync = false
			if res, err := Plan(entries, dest, Config{Backend: backend, Threshold: 1024, Tools: tools}); err == nil || res != nil {
				t.Errorf("Plan succeeded without rsync: %+v", res)
			}
		})
	}
	if _, err := Plan(entries, dest, Config{Backend: state.BackendFPSync, Threshold: 1024, Tools: Tools{Rsync: true, FPart: true}}); err == nil {
		t.Error("forced fpsync without fpsync succeeded")
	}
	if _, err := Plan(entries, dest, Config{Backend: state.BackendFPSync, Threshold: 1024, Tools: Tools{Rsync: true, FPSync: true}}); err == nil {
		t.Error("forced fpsync without fpart succeeded")
	}
	if _, err := Plan(entries, dest, Config{Backend: state.BackendMSCP, Threshold: 1024, Tools: rsyncOnly()}); err == nil {
		t.Error("forced mscp without mscp succeeded")
	}
}

func TestInvalidBackend(t *testing.T) {
	if _, err := Plan(nil, nil, Config{Backend: "bogus", Tools: allTools()}); err == nil {
		t.Fatal("Plan with an invalid backend succeeded")
	}
}

func TestDuplicateSourcePaths(t *testing.T) {
	entries := []discovery.Entry{file("dup.txt", 1), file("other.txt", 2), file("dup.txt", 3)}
	res, err := Plan(entries, nil, Config{Backend: state.BackendAuto, Threshold: 1024, Tools: allTools()})
	if err == nil || res != nil {
		t.Fatalf("duplicate paths: res=%+v err=%v, want error", res, err)
	}
	if !strings.Contains(err.Error(), "dup.txt") || !strings.Contains(err.Error(), "duplicate") {
		t.Errorf("duplicate error = %q", err)
	}
}

func TestInvalidSourcePathsRejected(t *testing.T) {
	for _, p := range []string{"", "/abs", "../escape", "a/../b", "a//b", "a\x00b"} {
		entries := []discovery.Entry{file(p, 1)}
		if res, err := Plan(entries, nil, Config{Backend: state.BackendAuto, Threshold: 1024, Tools: allTools()}); err == nil || res != nil {
			t.Errorf("path %q: res=%+v err=%v, want error", p, res, err)
		}
	}
}

func TestUnsupportedEntries(t *testing.T) {
	scanErr := errors.New("permission denied")
	entries := []discovery.Entry{
		{Path: "link", Kind: discovery.KindSymlink},
		{Path: "pipe", Kind: discovery.KindSpecial},
		{Path: "broken", Kind: discovery.KindFile, Err: scanErr},
		file("ok.txt", 10),
	}
	res, err := Plan(entries, map[string]discovery.RemoteEntry{}, Config{Backend: state.BackendAuto, Threshold: 1024, Tools: allTools()})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]Decision{}
	for _, d := range res.Decisions {
		got[d.Path] = d
	}
	for _, p := range []string{"link", "pipe", "broken"} {
		d := got[p]
		if d.Status != StatusUnsupported {
			t.Errorf("%s = %+v, want unsupported", p, d)
		}
		if d.Reason == "" {
			t.Errorf("%s has no reason", p)
		}
		if !strings.Contains(got["broken"].Reason, "permission denied") {
			t.Errorf("scan error reason = %q, want the underlying error", got["broken"].Reason)
		}
	}
	if got["ok.txt"].Status != StatusPending {
		t.Errorf("ok.txt = %+v, want pending", got["ok.txt"])
	}
	// Unsupported entries are never listed as pending work for any backend.
	for _, b := range []state.Backend{state.BackendFPSync, state.BackendMSCP, state.BackendRsync} {
		for _, p := range res.Paths(b) {
			if p == "link" || p == "pipe" || p == "broken" {
				t.Errorf("backend %q pending list includes unsupported %q", b, p)
			}
		}
	}
}

func TestExactlyOneOwnerPerPath(t *testing.T) {
	// A mixed tree: every path appears exactly once across all pending
	// backend sets, the skipped set, and the unsupported set — no path has
	// two writers.
	dest := map[string]discovery.RemoteEntry{
		"same.txt":    {Path: "same.txt", Size: 5, ModTime: testMtime},
		"changed.txt": {Path: "changed.txt", Size: 1, ModTime: testMtime},
	}
	entries := []discovery.Entry{
		file("same.txt", 5),
		file("changed.txt", 9),
		file("small-new.txt", 10),
		file("big-new.bin", 4096),
		{Path: "link", Kind: discovery.KindSymlink},
	}
	res, err := Plan(entries, dest, Config{Backend: state.BackendAuto, Threshold: 1024, Tools: allTools()})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Decisions) != len(entries) {
		t.Fatalf("got %d decisions for %d entries", len(res.Decisions), len(entries))
	}
	owner := make(map[string]int)
	for _, d := range res.Decisions {
		owner[d.Path]++
		if !d.Backend.Valid() {
			t.Errorf("path %q has invalid backend %q", d.Path, d.Backend)
		}
		switch d.Status {
		case StatusPending, StatusSkipped, StatusUnsupported:
		default:
			t.Errorf("path %q has unknown status %q", d.Path, d.Status)
		}
	}
	for p, n := range owner {
		if n != 1 {
			t.Errorf("path %q has %d decisions, want exactly 1", p, n)
		}
	}
	want := map[string]struct {
		backend state.Backend
		status  Status
	}{
		"same.txt":      {state.BackendRsync, StatusSkipped},
		"changed.txt":   {state.BackendRsync, StatusPending},
		"small-new.txt": {state.BackendFPSync, StatusPending},
		"big-new.bin":   {state.BackendMSCP, StatusPending},
		"link":          {state.BackendRsync, StatusUnsupported},
	}
	for _, d := range res.Decisions {
		w := want[d.Path]
		if d.Backend != w.backend || d.Status != w.status {
			t.Errorf("%s = (%q, %q), want (%q, %q)", d.Path, d.Backend, d.Status, w.backend, w.status)
		}
	}
	counts := res.Counts()
	if counts[StatusPending] != 3 || counts[StatusSkipped] != 1 || counts[StatusUnsupported] != 1 {
		t.Errorf("counts = %v", counts)
	}
}

func TestDestinationEntriesWithoutSourceAreIgnored(t *testing.T) {
	// Remote files with no local counterpart are never planned and never
	// deleted.
	dest := map[string]discovery.RemoteEntry{
		"remote-only.txt": {Path: "remote-only.txt", Size: 1, ModTime: testMtime},
	}
	res, err := Plan([]discovery.Entry{file("local.txt", 1)}, dest, Config{Backend: state.BackendAuto, Threshold: 1024, Tools: allTools()})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Decisions) != 1 || res.Decisions[0].Path != "local.txt" {
		t.Fatalf("decisions = %+v", res.Decisions)
	}
}

func TestMtimeEqualityIsInstantEquality(t *testing.T) {
	// The same instant in a different time zone still counts as unchanged;
	// metadata equality is only a planning hint, and mismatch goes to rsync.
	dest := map[string]discovery.RemoteEntry{
		"f": {Path: "f", Size: 3, ModTime: testMtime.In(time.FixedZone("X", 3600))},
	}
	d := decide(t, file("f", 3), dest, Config{Backend: state.BackendAuto, Threshold: 1024, Tools: allTools()})
	if d.Status != StatusSkipped {
		t.Errorf("same instant different zone = %+v, want skipped", d)
	}
}

func TestNegativeSizeUnsupported(t *testing.T) {
	// Discovery never emits a negative size for a regular file; the
	// planner refuses defensively.
	e := file("neg", -1)
	d := decide(t, e, nil, Config{Backend: state.BackendAuto, Threshold: 1024, Tools: allTools()})
	if d.Status != StatusUnsupported {
		t.Fatalf("negative size = %+v, want unsupported", d)
	}
}

// TestThresholdEdgeCases pins the at-or-above semantics and the fpsync
// safety rule for thresholds that fpart cannot express: a threshold below 2
// bytes disables the small-file stage (rsync fallback), and forced fpsync
// with such a threshold is refused.
func TestThresholdEdgeCases(t *testing.T) {
	dest := map[string]discovery.RemoteEntry{}

	t.Run("threshold one routes zero-byte files to rsync", func(t *testing.T) {
		cfg := Config{Backend: state.BackendAuto, Threshold: 1, Tools: allTools()}
		d := decide(t, file("empty", 0), dest, cfg)
		if d.Backend != state.BackendRsync || d.Status != StatusPending {
			t.Errorf("size 0, threshold 1: %+v, want pending rsync fallback", d)
		}
		if !strings.Contains(d.Reason, "threshold below 2") {
			t.Errorf("reason = %q, want the threshold explanation", d.Reason)
		}
		// A 1-byte file is at the threshold: large, so mscp.
		if d := decide(t, file("one-byte", 1), dest, cfg); d.Backend != state.BackendMSCP {
			t.Errorf("size 1, threshold 1: backend = %q, want mscp", d.Backend)
		}
	})

	t.Run("threshold zero treats everything as large", func(t *testing.T) {
		cfg := Config{Backend: state.BackendAuto, Threshold: 0, Tools: allTools()}
		for _, size := range []int64{0, 1, 1 << 40} {
			if d := decide(t, file("f", size), dest, cfg); d.Backend != state.BackendMSCP {
				t.Errorf("size %d, threshold 0: backend = %q, want mscp", size, d.Backend)
			}
		}
	})

	t.Run("threshold two boundary", func(t *testing.T) {
		cfg := Config{Backend: state.BackendAuto, Threshold: 2, Tools: allTools()}
		if d := decide(t, file("one", 1), dest, cfg); d.Backend != state.BackendFPSync {
			t.Errorf("size 1, threshold 2: backend = %q, want fpsync", d.Backend)
		}
		if d := decide(t, file("two", 2), dest, cfg); d.Backend != state.BackendMSCP {
			t.Errorf("size 2, threshold 2: backend = %q, want mscp", d.Backend)
		}
	})

	t.Run("huge thresholds keep every file small", func(t *testing.T) {
		for _, threshold := range []uint64{1 << 63, ^uint64(0)} {
			cfg := Config{Backend: state.BackendAuto, Threshold: threshold, Tools: allTools()}
			if d := decide(t, file("f", 1<<40), dest, cfg); d.Backend != state.BackendFPSync {
				t.Errorf("size 1TiB, threshold %d: backend = %q, want fpsync", threshold, d.Backend)
			}
		}
	})

	t.Run("forced fpsync rejects a threshold below two", func(t *testing.T) {
		for _, threshold := range []uint64{0, 1} {
			cfg := Config{Backend: state.BackendFPSync, Threshold: threshold, Tools: allTools()}
			if _, err := Plan([]discovery.Entry{file("f", 10)}, dest, cfg); err == nil {
				t.Errorf("forced fpsync, threshold %d: plan succeeded, want refusal", threshold)
			}
		}
	})
}
