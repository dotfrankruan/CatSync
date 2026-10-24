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
