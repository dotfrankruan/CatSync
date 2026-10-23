package rsync

import (
	"reflect"
	"strings"
	"testing"

	"github.com/dotfrankruan/CatSync/internal/backend"
	"github.com/dotfrankruan/CatSync/internal/target"
)

func testDest() target.Destination {
	return target.Destination{User: "alice", Host: "example.com", Path: "/data"}
}

func validConfig() Config {
	return Config{
		SourceDir: "/srv/source",
		Dest:      testDest(),
		Paths:     []string{"a/b.txt", "c.txt"},
	}
}

func TestCommandGolden(t *testing.T) {
	cfg := validConfig()
	argv, stdin, err := cfg.Command()
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	want := []string{
		"rsync",
		"--archive",
		"--partial",
		"--partial-dir=.catsync-rsync-partial",
		"--relative",
		"--files-from=-",
		"--from0",
		"-e", "ssh",
		"--",
		"/srv/source/",
		"alice@example.com:/data",
	}
	if !reflect.DeepEqual(argv, want) {
		t.Errorf("argv mismatch:\n got %#v\nwant %#v", argv, want)
	}
	if got, want := string(stdin), "a/b.txt\x00c.txt\x00"; got != want {
		t.Errorf("stdin = %q, want %q", got, want)
	}
}

func TestCommandDownloadGolden(t *testing.T) {
	cfg := validConfig()
	cfg.Direction = target.RemoteToLocal
	cfg.SourceDir = "/srv/download"
	cfg.Dest = target.Destination{User: "alice", Host: "example.com", Path: "/remote source"}
	argv, stdin, err := cfg.Command()
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	want := []string{
		"rsync",
		"--archive",
		"--partial",
		"--partial-dir=.catsync-rsync-partial",
		"--relative",
		"--files-from=-",
		"--from0",
		"-e", "ssh",
		"--",
		"alice@example.com:/remote source/",
		"/srv/download/",
	}
	if !reflect.DeepEqual(argv, want) {
		t.Errorf("argv mismatch:\n got %#v\nwant %#v", argv, want)
	}
	if got, want := string(stdin), "a/b.txt\x00c.txt\x00"; got != want {
		t.Errorf("stdin = %q, want %q", got, want)
	}
}

func TestCommandDryRunAndIPv6(t *testing.T) {
	cfg := validConfig()
	cfg.DryRun = true
	cfg.SSH = backend.SSH{IPv6: true}
	cfg.Dest = target.Destination{Host: "2001:db8::1", Path: "/data"}
	argv, _, err := cfg.Command()
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	joined := strings.Join(argv, "\x00")
	if !strings.Contains(joined, "--dry-run") {
		t.Error("dry-run missing --dry-run")
	}
	if !reflect.DeepEqual(argv[len(argv)-4:], []string{"ssh -6", "--", "/srv/source/", "[2001:db8::1]:/data"}) {
		t.Errorf("tail = %#v, want IPv6 transport and bracketed destination", argv[len(argv)-4:])
	}
	for i, a := range argv {
		if a == "-e" && argv[i+1] != "ssh -6" {
			t.Errorf("-e value = %q, want ssh -6", argv[i+1])
		}
	}
}

func TestCommandNeverForbiddenFlags(t *testing.T) {
	cfg := validConfig()
	cfg.DryRun = true
	cfg.SSH = backend.SSH{IPv6: true}
	argv, stdin, err := cfg.Command()
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	for _, a := range argv {
		for _, bad := range forbiddenFlags {
			if a == bad {
				t.Errorf("argv contains forbidden flag %q: %#v", bad, argv)
			}
		}
	}
	// Deletion strings must not appear even inside the payload.
	if strings.Contains(string(stdin), "--delete") {
		t.Errorf("stdin contains deletion flag: %q", stdin)
	}
}

func TestCommandPartialDir(t *testing.T) {
	cfg := validConfig()
	cfg.PartialDir = ".catsync/partial"
	argv, _, err := cfg.Command()
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	found := false
	for _, a := range argv {
		if a == "--partial-dir=.catsync/partial" {
			found = true
		}
	}
	if !found {
		t.Errorf("custom partial dir missing: %#v", argv)
	}

	for _, bad := range []string{"/abs/partial", "../escape", "a/../b"} {
		cfg := validConfig()
		cfg.PartialDir = bad
		if _, _, err := cfg.Command(); err == nil {
			t.Errorf("partial dir %q accepted, want error", bad)
		}
	}
}

func TestCommandRejectsUnsafePaths(t *testing.T) {
	bad := []struct {
		name string
		path string
	}{
		{"absolute", "/etc/passwd"},
		{"parent escape", "../x"},
		{"nested parent", "a/../../x"},
		{"current dir element", "./x"},
		{"empty element", "a//b"},
		{"trailing slash", "a/"},
		{"NUL", "a\x00b"},
		{"empty", ""},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig()
			cfg.Paths = []string{tc.path}
			if _, _, err := cfg.Command(); err == nil {
				t.Errorf("path %q accepted, want error", tc.path)
			}
		})
	}
	t.Run("duplicate", func(t *testing.T) {
		cfg := validConfig()
		cfg.Paths = []string{"a", "b", "a"}
		if _, _, err := cfg.Command(); err == nil {
			t.Error("duplicate path accepted, want error")
		}
	})
}

func TestCommandSpecialPathsTravelAsStdin(t *testing.T) {
	cfg := validConfig()
	cfg.Paths = []string{
		"plain.txt",
		"dir with space/file.txt",
		"new\nline.txt",
		"ünïcødé-文件.txt",
		"-leading-dash.txt",
		"quote'and\"double.txt",
	}
	argv, stdin, err := cfg.Command()
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	// None of the file names may leak into argv: they are stdin data.
	for _, a := range argv {
		for _, p := range cfg.Paths {
			if strings.Contains(a, p) {
				t.Errorf("path %q leaked into argv element %q", p, a)
			}
		}
	}
	want := "plain.txt\x00dir with space/file.txt\x00new\nline.txt\x00ünïcødé-文件.txt\x00-leading-dash.txt\x00quote'and\"double.txt\x00"
	if string(stdin) != want {
		t.Errorf("stdin = %q, want %q", stdin, want)
	}
}

func TestCommandValidation(t *testing.T) {
	t.Run("invalid direction", func(t *testing.T) {
		cfg := validConfig()
		cfg.Direction = target.Direction("sideways")
		if _, _, err := cfg.Command(); err == nil {
			t.Error("invalid direction accepted")
		}
	})
	t.Run("relative source", func(t *testing.T) {
		cfg := validConfig()
		cfg.SourceDir = "relative/src"
		if _, _, err := cfg.Command(); err == nil {
			t.Error("relative source accepted, want error")
		}
	})
	t.Run("empty source", func(t *testing.T) {
		cfg := validConfig()
		cfg.SourceDir = ""
		if _, _, err := cfg.Command(); err == nil {
			t.Error("empty source accepted, want error")
		}
	})
	t.Run("NUL source", func(t *testing.T) {
		cfg := validConfig()
		cfg.SourceDir = "/srv/\x00src"
		if _, _, err := cfg.Command(); err == nil {
			t.Error("NUL source accepted, want error")
		}
	})
	t.Run("bad destination", func(t *testing.T) {
		cfg := validConfig()
		cfg.Dest = target.Destination{Host: "example.com", Path: "relative"}
		if _, _, err := cfg.Command(); err == nil {
			t.Error("relative destination path accepted, want error")
		}
	})
	t.Run("dash operand", func(t *testing.T) {
		cfg := validConfig()
		cfg.Dest = target.Destination{User: "-x", Host: "example.com", Path: "/data"}
		if _, _, err := cfg.Command(); err == nil {
			t.Error("dash-leading destination operand accepted, want error")
		}
	})
}

func TestCommandSourceTrailingSlash(t *testing.T) {
	cfg := validConfig()
	cfg.SourceDir = "/srv/source/"
	argv, _, err := cfg.Command()
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	if argv[len(argv)-2] != "/srv/source/" {
		t.Errorf("source operand = %q, want single trailing slash", argv[len(argv)-2])
	}
}

func TestCommandProgramOverrideAndEmptyList(t *testing.T) {
	cfg := validConfig()
	cfg.Program = "/opt/custom/rsync"
	cfg.Paths = nil
	argv, stdin, err := cfg.Command()
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	if argv[0] != "/opt/custom/rsync" {
		t.Errorf("program = %q, want override", argv[0])
	}
	if len(stdin) != 0 {
		t.Errorf("empty path list payload = %q, want empty", stdin)
	}
}

// TestCommandSSHProgramOverride checks the transport override is emitted
// verbatim as a single -e token, matching the documented CATSYNC_SSH scope.
func TestCommandSSHProgramOverride(t *testing.T) {
	cfg := validConfig()
	cfg.SSH = backend.SSH{Program: "/opt/mock/ssh"}
	argv, _, err := cfg.Command()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for i, a := range argv {
		if a == "-e" {
			if i+1 >= len(argv) || argv[i+1] != "/opt/mock/ssh" {
				t.Errorf("-e value = %q, want /opt/mock/ssh", argv[i+1])
			}
			found = true
		}
	}
	if !found {
		t.Errorf("argv missing -e: %v", argv)
	}
}

// TestCommandRejectsUnexpressibleSSHProgram covers the backend's
// defense-in-depth transport validation: rsync splits -e on whitespace, so
// overrides it cannot represent must fail here too, not only in the
// executor.
func TestCommandRejectsUnexpressibleSSHProgram(t *testing.T) {
	for _, bad := range []string{
		"/opt/with space/ssh",
		"ssh\t-x",
		"ssh\nx",
		"-ssh",
		"a\x00b",
	} {
		cfg := validConfig()
		cfg.SSH = backend.SSH{Program: bad}
		if _, _, err := cfg.Command(); err == nil {
			t.Errorf("SSH.Program %q accepted, want transport validation error", bad)
		}
	}
}

// TestCommandCustomSSHProgramRendering checks valid custom paths and the
// IPv6 rendering pass through to -e unchanged.
func TestCommandCustomSSHProgramRendering(t *testing.T) {
	cfg := validConfig()
	cfg.SSH = backend.SSH{Program: "/opt/custom/ssh", IPv6: true}
	argv, _, err := cfg.Command()
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	want := "/opt/custom/ssh -6"
	found := false
	for i, a := range argv {
		if a == "-e" {
			found = true
			if i+1 >= len(argv) || argv[i+1] != want {
				t.Errorf("-e value = %q, want %q", argv[i+1], want)
			}
		}
	}
	if !found {
		t.Errorf("argv missing -e: %v", argv)
	}
}

// TestCommandNoImpliedDirs covers the option used by the native
// parallel-rsync stage: it appears exactly once and never drags in the
// forbidden options.
func TestCommandNoImpliedDirs(t *testing.T) {
	cfg := validConfig()
	cfg.NoImpliedDirs = true
	argv, stdin, err := cfg.Command()
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	count := 0
	for _, a := range argv {
		if a == "--no-implied-dirs" {
			count++
		}
		for _, bad := range forbiddenFlags {
			if a == bad {
				t.Errorf("argv contains forbidden flag %q: %#v", bad, argv)
			}
		}
	}
	if count != 1 {
		t.Fatalf("--no-implied-dirs appears %d times, want 1: %v", count, argv)
	}
	if got := string(stdin); got != "a/b.txt\x00c.txt\x00" {
		t.Errorf("stdin = %q, want the NUL-delimited file list", got)
	}

	// The conservative stage keeps the default behavior.
	plain := validConfig()
	argv, _, err = plain.Command()
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range argv {
		if a == "--no-implied-dirs" {
			t.Fatal("default command unexpectedly contains --no-implied-dirs")
		}
	}
}
