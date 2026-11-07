package progress

import (
	"strings"
	"testing"
	"time"
)

func TestFormatSnapshotIncludesRequestedFields(t *testing.T) {
	snap := Snapshot{
		Done: 512, Total: 1024, Speed: 256, Elapsed: 2 * time.Second,
		Phase: "transferring", HasETA: true, ETA: 2 * time.Second,
		Conns: 3, FilesDone: 4, FilesTotal: 9, Checked: 2, Skipped: 1,
		Files: []FileState{
			{Label: "data/big1.bin", Size: 1000, Done: 400, Speed: 128},
			{Label: "data/big2.bin", Size: 900, Done: 90, Speed: 64},
		},
	}
	lines := formatSnapshot(snap)
	if len(lines) < 4 {
		t.Fatalf("snapshot has %d lines, want header, counters, and two files: %q", len(lines), lines)
	}
	all := strings.Join(lines, "\n")
	for _, want := range []string{
		"50.00%", "Current Speed", "Elapsed 00:02", "ETA 00:02",
		"transferring",
		"ssh conns 3", "files 4/9 done, 2 checked, 1 skipped",
		"data/big1.bin", "400 B/1000 B", "ETA 00:04",
	} {
		if !strings.Contains(all, want) {
			t.Errorf("snapshot lacks %q:\n%s", want, all)
		}
	}
}

// TestReporterTracksFilesConnectionsAndCounters: the reporter exposes live
// per-file bytes, SSH connections, and the file counters.
func TestReporterTracksFilesConnectionsAndCounters(t *testing.T) {
	var out strings.Builder
	r := New(&out, 1000, false)
	r.FilesTotal(6)
	r.SkippedFiles(2)
	r.FileStart("a.bin", 400, 0)
	r.FileAdd("a.bin", 150)
	r.ConnDelta(2)
	r.FileStart("b.bin", 600, 0)
	r.FileAdd("b.bin", 100)
	r.CompletedFiles(1)
	r.CheckedFiles(1)
	snap := r.Snapshot()
	if snap.Done != 250 {
		t.Errorf("Done = %d, want 250", snap.Done)
	}
	if snap.Conns != 2 {
		t.Errorf("Conns = %d, want 2", snap.Conns)
	}
	if snap.FilesDone != 1 || snap.Checked != 1 || snap.Skipped != 2 || snap.FilesTotal != 6 {
		t.Errorf("counters = done %d checked %d skipped %d total %d", snap.FilesDone, snap.Checked, snap.Skipped, snap.FilesTotal)
	}
	if len(snap.Files) != 2 {
		t.Fatalf("active files = %d, want 2", len(snap.Files))
	}
	if snap.Files[0].Label != "a.bin" || snap.Files[0].Done != 150 || snap.Files[0].Size != 400 {
		t.Errorf("first file = %+v", snap.Files[0])
	}
	// Ending a file drops its line; a resumed start resets its counters.
	r.FileEnd("a.bin")
	snap = r.Snapshot()
	if len(snap.Files) != 1 || snap.Files[0].Label != "b.bin" {
		t.Fatalf("after FileEnd files = %+v", snap.Files)
	}
	r.FileStart("b.bin", 600, 600)
	snap = r.Snapshot()
	if snap.Files[0].Done != 600 {
		t.Errorf("resumed file done = %d, want 600", snap.Files[0].Done)
	}
	if !strings.Contains(out.String(), "ssh conns") {
		t.Errorf("rendered output lacks the counter line:\n%s", out.String())
	}
}

// TestSnapshotListsAtMostFiveFiles keeps the terminal block bounded.
func TestSnapshotListsAtMostFiveFiles(t *testing.T) {
	var out strings.Builder
	r := New(&out, 0, false)
	for _, name := range []string{"a", "b", "c", "d", "e", "f", "g"} {
		r.FileStart(name, 10, 0)
	}
	snap := r.Snapshot()
	if len(snap.Files) != 7 {
		t.Fatalf("active files = %d, want 7", len(snap.Files))
	}
	lines := formatSnapshot(snap)
	if len(lines) != 2+maxFileLines+1 {
		t.Fatalf("lines = %d, want header, counters, %d files, and the overflow note: %q", len(lines), maxFileLines, lines)
	}
	if !strings.Contains(strings.Join(lines, "\n"), "and 2 more file(s)") {
		t.Errorf("overflow note missing: %q", lines)
	}
}

func TestReporterClampsAndFinishes(t *testing.T) {
	var out strings.Builder
	r := New(&out, 10, false)
	r.Add(100)
	r.Finish(true)
	got := out.String()
	if !strings.Contains(got, "100.00%") || !strings.Contains(got, "completed") {
		t.Fatalf("final output: %s", got)
	}
}

// TestBaselineShapesTheWholeRun: a resumed reporter starts at the bytes that
// are already durable, so the bar and percentage describe the whole transfer.
func TestBaselineShapesTheWholeRun(t *testing.T) {
	var buf strings.Builder
	r := NewWithBaseline(&buf, 1000, 250, false)
	defer r.Finish(false)
	if got := r.Total(); got != 1000 {
		t.Errorf("Total = %d, want 1000", got)
	}
	if got := r.Done(); got != 250 {
		t.Errorf("Done = %d, want the 250-byte baseline", got)
	}
	out := buf.String()
	if !strings.Contains(out, "25.00%") {
		t.Errorf("first snapshot should report 25.0%%:\n%s", out)
	}
	if !strings.Contains(out, "250 B/1000 B") {
		t.Errorf("first snapshot should report the seeded bytes:\n%s", out)
	}
	// Newly transferred bytes add on top of the baseline.
	r.Add(750)
	if got := r.Done(); got != 1000 {
		t.Errorf("Done after Add = %d, want 1000", got)
	}
	// A baseline larger than the total is clamped.
	r2 := NewWithBaseline(&strings.Builder{}, 100, 500, false)
	defer r2.Finish(false)
	if got := r2.Done(); got != 100 {
		t.Errorf("clamped Done = %d, want 100", got)
	}
}
