package fpsync

import (
	"reflect"
	"strings"
	"testing"

	"github.com/frankruan/catsync/internal/target"
)

func validConfig() Config {
	return Config{
		SourceDir:         "/srv/source",
		Dest:              target.Destination{User: "alice", Host: "example.com", Path: "/data"},
		Jobs:              4,
		FilesPerPartition: 2000,
		Threshold:         1 << 20,
		StateDir:          "/var/lib/catsync/fpsync",
	}
}

func TestCommandGolden(t *testing.T) {
	cfg := validConfig()
	argv, err := cfg.Command()
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	want := []string{
		"fpsync",
		"-v",
		"-n", "4",
		"-f", "2000",
		"-s", "1048576",
		"-O", "-S",
		"-t", "/var/lib/catsync/fpsync",
		"/srv/source/",
		"alice@example.com:/data/",
	}
	if !reflect.DeepEqual(argv, want) {
		t.Errorf("argv mismatch:\n got %#v\nwant %#v", argv, want)
	}
}

func TestCommandWithoutStateDir(t *testing.T) {
	cfg := validConfig()
	cfg.StateDir = ""
	argv, err := cfg.Command()
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	for i, a := range argv {
		if a == "-t" {
			t.Errorf("-t present at %d without state dir: %#v", i, argv)
		}
	}
}

// TestCommandFpartSkipBig verifies "-O -S" adjacency (fpart gets -S, fpsync
// does not) and the absence of fpsync's sudo -S, directory mode -E, and any
// tool-option override that could introduce deletion.
func TestCommandFpartSkipBig(t *testing.T) {
	cfg := validConfig()
	argv, err := cfg.Command()
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	oSeen := false
	for i, a := range argv {
		switch a {
		case "-O":
			oSeen = true
			if i+1 >= len(argv) || argv[i+1] != "-S" {
				t.Errorf("-O not followed by -S: %#v", argv)
			}
		case "-S":
			if i == 0 || argv[i-1] != "-O" {
				t.Errorf("bare fpsync -S (sudo mode) at %d: %#v", i, argv)
			}
		case "-E", "-o", "-w", "-d", "-p", "-R":
			t.Errorf("forbidden flag %q at %d: %#v", a, i, argv)
		default:
			if strings.Contains(a, "delete") {
				t.Errorf("deletion option %q at %d: %#v", a, i, argv)
			}
		}
	}
	if !oSeen {
		t.Errorf("-O -S missing: %#v", argv)
	}
}

func TestCommandOperandSlashes(t *testing.T) {
	cfg := validConfig()
	cfg.SourceDir = "/srv/source/" // already slashed
	cfg.Dest.Path = "/data/"
	argv, err := cfg.Command()
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	src, dst := argv[len(argv)-2], argv[len(argv)-1]
	if src != "/srv/source/" {
		t.Errorf("source = %q, want single trailing slash", src)
	}
	if dst != "alice@example.com:/data/" {
		t.Errorf("destination = %q, want single trailing slash", dst)
	}
}

func TestCommandValidation(t *testing.T) {
	bad := []struct {
		name   string
		mutate func(*Config)
	}{
		{"relative source", func(c *Config) { c.SourceDir = "rel/src" }},
		{"empty source", func(c *Config) { c.SourceDir = "" }},
		{"NUL source", func(c *Config) { c.SourceDir = "/srv/\x00src" }},
		{"relative state dir", func(c *Config) { c.StateDir = "state" }},
		{"NUL state dir", func(c *Config) { c.StateDir = "/var/\x00lib" }},
		{"zero jobs", func(c *Config) { c.Jobs = 0 }},
		{"negative jobs", func(c *Config) { c.Jobs = -1 }},
		{"zero files per partition", func(c *Config) { c.FilesPerPartition = 0 }},
		{"zero threshold", func(c *Config) { c.Threshold = 0 }},
		{"bad destination", func(c *Config) { c.Dest.Path = "relative" }},
		{"empty host", func(c *Config) { c.Dest.Host = "" }},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig()
			tc.mutate(&cfg)
			if _, err := cfg.Command(); err == nil {
				t.Error("invalid config accepted, want error")
			}
		})
	}
}

func TestResumeCommandGolden(t *testing.T) {
	cfg := validConfig()
	argv, err := cfg.ResumeCommand("1735689600-12345")
	if err != nil {
		t.Fatalf("ResumeCommand: %v", err)
	}
	want := []string{"fpsync", "-v", "-t", "/var/lib/catsync/fpsync", "-r", "1735689600-12345"}
	if !reflect.DeepEqual(argv, want) {
		t.Errorf("resume argv mismatch:\n got %#v\nwant %#v", argv, want)
	}
}

func TestResumeCommandWithoutStateDir(t *testing.T) {
	cfg := validConfig()
	cfg.StateDir = ""
	argv, err := cfg.ResumeCommand("1-2")
	if err != nil {
		t.Fatalf("ResumeCommand: %v", err)
	}
	want := []string{"fpsync", "-v", "-r", "1-2"}
	if !reflect.DeepEqual(argv, want) {
		t.Errorf("resume argv mismatch:\n got %#v\nwant %#v", argv, want)
	}
}

func TestResumeCommandRejectsBadRunID(t *testing.T) {
	for _, id := range []string{"", "abc", "1-2-3", "-1", "1-", "1 -2", "1-2\n", "1_2", "$(x)-1"} {
		cfg := validConfig()
		if _, err := cfg.ResumeCommand(id); err == nil {
			t.Errorf("run id %q accepted, want error", id)
		}
	}
}

func TestParseOutput(t *testing.T) {
	output := strings.Join([]string{
		"1792333896 Info: Fpsync started (syncing /src/ => /dst/, pid: 12345)",
		"1792333896 Info: Run ID: 1792333896-12345",
		"1792333896 ===> Analyzing filesystem...",
		"S\t4096\t./big.bin",
		"S\t2048\t./dir with space/ünïcødé.bin",
		"S\t100\t./-leading-dash.bin",
		"S\t50\t./with\ttab.bin",
		"S\tbogus\t./bad-size.bin",
		"S\t123",
		"S\t7\t../escape.bin",
		"S\t7\t/abs.bin",
		"S\t7\t",
		"not an s record",
		"1792333897 Info: Fpsync stopped (with success)",
	}, "\n")
	runID, skipped := ParseOutput(output)
	if runID != "1792333896-12345" {
		t.Errorf("run id = %q, want 1792333896-12345", runID)
	}
	want := []Skipped{
		{Path: "big.bin", Size: 4096},
		{Path: "dir with space/ünïcødé.bin", Size: 2048},
		{Path: "-leading-dash.bin", Size: 100},
		{Path: "with\ttab.bin", Size: 50},
	}
	if !reflect.DeepEqual(skipped, want) {
		t.Errorf("skipped mismatch:\n got %#v\nwant %#v", skipped, want)
	}
}

func TestParseOutputResumedRunID(t *testing.T) {
	runID, skipped := ParseOutput("1792333911 Info: Run ID: 1792333910-76140 (resumed)\n")
	if runID != "1792333910-76140" {
		t.Errorf("run id = %q, want resumed id", runID)
	}
	if len(skipped) != 0 {
		t.Errorf("skipped = %#v, want none", skipped)
	}
}

func TestParseOutputEmpty(t *testing.T) {
	runID, skipped := ParseOutput("")
	if runID != "" || skipped != nil {
		t.Errorf("ParseOutput(\"\") = (%q, %#v), want empty", runID, skipped)
	}
}

func TestValidateRunID(t *testing.T) {
	if err := ValidateRunID("1735689600-12345"); err != nil {
		t.Errorf("valid run id rejected: %v", err)
	}
	if err := ValidateRunID("nope"); err == nil {
		t.Error("invalid run id accepted")
	}
}
