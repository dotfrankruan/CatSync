package progress

import (
	"strings"
	"testing"
	"time"
)

func TestFormatIncludesRequestedTimingAndSpeedFields(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.Local)
	got := format(512, 1024, 256, 2*time.Second, now, "transferring")
	for _, want := range []string{"50.00%", "Current Speed", "Elapsed 00:02", "ETA 00:02", "ETD 12:00:02", "transferring"} {
		if !strings.Contains(got, want) {
			t.Errorf("format lacks %q: %s", want, got)
		}
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
