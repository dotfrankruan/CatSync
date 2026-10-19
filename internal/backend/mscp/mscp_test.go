package mscp

import (
	"reflect"
	"testing"

	"github.com/frankruan/catsync/internal/backend"
	"github.com/frankruan/catsync/internal/target"
)

func validConfig() Config {
	return Config{
		WorkDir:     "/srv/source",
		Path:        "videos/big.bin",
		Dest:        target.Destination{User: "alice", Host: "example.com", Path: "/data"},
		Connections: 8,
		Inflight:    32,
		MinChunk:    16 << 20,
		Checkpoint:  "/var/lib/catsync/ckpt/big.bin.mscpckpt",
	}
}

func TestCommandGolden(t *testing.T) {
	cfg := validConfig()
	argv, err := cfg.Command()
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	want := []string{
		"mscp",
		"-n", "8",
		"-a", "32",
		"-s", "16777216",
		"-W", "/var/lib/catsync/ckpt/big.bin.mscpckpt",
		"--",
		"videos/big.bin",
		"alice@example.com:/data/videos/big.bin",
	}
	if !reflect.DeepEqual(argv, want) {
		t.Errorf("argv mismatch:\n got %#v\nwant %#v", argv, want)
	}
}

func TestCommandBitrateAndIPv6(t *testing.T) {
	cfg := validConfig()
	cfg.Bitrate = 100_000_000
	cfg.SSH = backend.SSH{IPv6: true}
	argv, err := cfg.Command()
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	want := []string{
		"mscp",
		"-n", "8",
		"-a", "32",
		"-s", "16777216",
		"-L", "100000000",
		"-6",
		"-W", "/var/lib/catsync/ckpt/big.bin.mscpckpt",
		"--",
		"videos/big.bin",
		"alice@example.com:/data/videos/big.bin",
	}
	if !reflect.DeepEqual(argv, want) {
		t.Errorf("argv mismatch:\n got %#v\nwant %#v", argv, want)
	}
}

func TestCommandLeadingDashPath(t *testing.T) {
	cfg := validConfig()
	cfg.Path = "-big.bin"
	argv, err := cfg.Command()
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	if got := argv[len(argv)-2]; got != "./-big.bin" {
		t.Errorf("source operand = %q, want ./-big.bin", got)
	}
	if got := argv[len(argv)-1]; got != "alice@example.com:/data/-big.bin" {
		t.Errorf("target operand = %q", got)
	}
}

func TestCommandLeadingDashCheckpoint(t *testing.T) {
	cfg := validConfig()
	cfg.Checkpoint = "-ckpt"
	argv, err := cfg.Command()
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	for i, a := range argv {
		if a == "-W" && argv[i+1] != "./-ckpt" {
			t.Errorf("-W value = %q, want ./-ckpt", argv[i+1])
		}
	}
}

func TestCommandSpecialPath(t *testing.T) {
	cfg := validConfig()
	cfg.Path = "dir with space/ünïcødé-文件.bin"
	argv, err := cfg.Command()
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	if got := argv[len(argv)-2]; got != cfg.Path {
		t.Errorf("source operand = %q, want %q", got, cfg.Path)
	}
	if got := argv[len(argv)-1]; got != "alice@example.com:/data/dir with space/ünïcødé-文件.bin" {
		t.Errorf("target operand = %q", got)
	}
}

func TestCommandDestTrailingSlash(t *testing.T) {
	cfg := validConfig()
	cfg.Dest.Path = "/data/"
	argv, err := cfg.Command()
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	if got := argv[len(argv)-1]; got != "alice@example.com:/data/videos/big.bin" {
		t.Errorf("target operand = %q, want no double slash", got)
	}
}

func TestCommandValidation(t *testing.T) {
	bad := []struct {
		name   string
		mutate func(*Config)
	}{
		{"empty workdir", func(c *Config) { c.WorkDir = "" }},
		{"relative workdir", func(c *Config) { c.WorkDir = "rel" }},
		{"NUL workdir", func(c *Config) { c.WorkDir = "/srv/\x00src" }},
		{"empty path", func(c *Config) { c.Path = "" }},
		{"absolute path", func(c *Config) { c.Path = "/etc/passwd" }},
		{"parent escape", func(c *Config) { c.Path = "../x" }},
		{"NUL path", func(c *Config) { c.Path = "a\x00b" }},
		{"zero connections", func(c *Config) { c.Connections = 0 }},
		{"zero inflight", func(c *Config) { c.Inflight = 0 }},
		{"zero min chunk", func(c *Config) { c.MinChunk = 0 }},
		{"empty checkpoint", func(c *Config) { c.Checkpoint = "" }},
		{"NUL checkpoint", func(c *Config) { c.Checkpoint = "/ck/\x00pt" }},
		{"relative destination", func(c *Config) { c.Dest.Path = "data" }},
		{"empty host", func(c *Config) { c.Dest.Host = "" }},
		{"dash destination", func(c *Config) { c.Dest.User = "-x" }},
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
	argv, err := cfg.ResumeCommand()
	if err != nil {
		t.Fatalf("ResumeCommand: %v", err)
	}
	want := []string{"mscp", "-R", "/var/lib/catsync/ckpt/big.bin.mscpckpt"}
	if !reflect.DeepEqual(argv, want) {
		t.Errorf("resume argv mismatch:\n got %#v\nwant %#v", argv, want)
	}
}

func TestResumeCommandIPv6(t *testing.T) {
	cfg := validConfig()
	cfg.SSH = backend.SSH{IPv6: true}
	argv, err := cfg.ResumeCommand()
	if err != nil {
		t.Fatalf("ResumeCommand: %v", err)
	}
	want := []string{"mscp", "-6", "-R", "/var/lib/catsync/ckpt/big.bin.mscpckpt"}
	if !reflect.DeepEqual(argv, want) {
		t.Errorf("resume argv mismatch:\n got %#v\nwant %#v", argv, want)
	}
}

// TestResumeRepeatsSSHOptions checks the mscp contract: resume repeats the
// failed run's SSH options but no transfer operands.
func TestResumeRepeatsSSHOptions(t *testing.T) {
	cfg := validConfig()
	cfg.SSH = backend.SSH{IPv6: true}
	resume, err := cfg.ResumeCommand()
	if err != nil {
		t.Fatalf("ResumeCommand: %v", err)
	}
	initial, err := cfg.Command()
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	found := false
	for _, a := range resume {
		if a == "-6" {
			found = true
		}
		if a == cfg.Path || a == "--" || a == "-W" {
			t.Errorf("resume argv carries initial-run token %q: %#v", a, resume)
		}
	}
	if !found {
		t.Errorf("resume argv missing -6: %#v (initial %#v)", resume, initial)
	}
}

func TestResumeValidation(t *testing.T) {
	t.Run("empty checkpoint", func(t *testing.T) {
		cfg := validConfig()
		cfg.Checkpoint = ""
		if _, err := cfg.ResumeCommand(); err == nil {
			t.Error("empty checkpoint accepted, want error")
		}
	})
	t.Run("relative workdir", func(t *testing.T) {
		cfg := validConfig()
		cfg.WorkDir = "rel"
		if _, err := cfg.ResumeCommand(); err == nil {
			t.Error("relative workdir accepted, want error")
		}
	})
	// Resume does not need the transfer fields.
	t.Run("minimal config", func(t *testing.T) {
		cfg := Config{WorkDir: "/srv/source", Checkpoint: "/ck/pt"}
		if _, err := cfg.ResumeCommand(); err != nil {
			t.Errorf("minimal resume config rejected: %v", err)
		}
	})
}
