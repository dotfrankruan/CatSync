package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// run invokes Run and captures the exit code and both output streams.
func run(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errOut bytes.Buffer
	code = Run(args, &out, &errOut, "test-version")
	return code, out.String(), errOut.String()
}

// tempSource returns a validated-usable source directory.
func tempSource(t *testing.T) string {
	t.Helper()
	return t.TempDir()
}

func TestVersionCommand(t *testing.T) {
	for _, args := range [][]string{{"version"}, {"--version"}, {"-version"}} {
		code, stdout, stderr := run(t, args...)
		if code != ExitOK {
			t.Errorf("Run(%v) exit = %d, want %d (stderr %q)", args, code, ExitOK, stderr)
		}
		if got, want := stdout, "catsync test-version\n"; got != want {
			t.Errorf("Run(%v) stdout = %q, want %q", args, got, want)
		}
	}
}

func TestNoArgsPrintsUsage(t *testing.T) {
	code, _, stderr := run(t)
	if code != ExitUsage {
		t.Errorf("exit = %d, want %d", code, ExitUsage)
	}
	if !strings.Contains(stderr, "Usage: catsync") {
		t.Errorf("stderr missing usage text:\n%s", stderr)
	}
}

func TestUnknownCommand(t *testing.T) {
	code, _, stderr := run(t, "frobnicate")
	if code != ExitUsage {
		t.Errorf("exit = %d, want %d", code, ExitUsage)
	}
	if !strings.Contains(stderr, `unknown command "frobnicate"`) {
		t.Errorf("stderr missing unknown-command message:\n%s", stderr)
	}
}

func TestHelpCommand(t *testing.T) {
	code, stdout, _ := run(t, "help")
	if code != ExitOK {
		t.Errorf("exit = %d, want %d", code, ExitOK)
	}
	for _, cmd := range []string{"doctor", "plan", "sync", "resume", "status", "version"} {
		if !strings.Contains(stdout, cmd) {
			t.Errorf("usage text missing command %q:\n%s", cmd, stdout)
		}
	}
}

func TestDeleteIsRejectedExplicitly(t *testing.T) {
	src := tempSource(t)
	variants := [][]string{
		{"sync", "--delete", src, "user@host:/data"},
		{"sync", "-delete", src, "user@host:/data"},
		{"sync", "--delete=always", src, "user@host:/data"},
		{"sync", src, "user@host:/data", "--delete"},
		{"plan", "--delete", src, "host:/data"},
		{"resume", "--delete"},
	}
	for _, args := range variants {
		code, _, stderr := run(t, args...)
		if code != ExitUsage {
			t.Errorf("Run(%v) exit = %d, want %d", args, code, ExitUsage)
		}
		if !strings.Contains(stderr, "--delete is not supported") ||
			!strings.Contains(stderr, "never deletes") {
			t.Errorf("Run(%v) stderr missing explicit --delete rejection:\n%s", args, stderr)
		}
	}
}

func TestDeleteAfterDoubleDashIsPositional(t *testing.T) {
	src := tempSource(t)
	// "--" ends flag parsing; a trailing --delete becomes an operand and is
	// rejected by the operand-count check, not the deletion guard.
	code, _, _ := run(t, "sync", src, "host:/data", "--", "--delete")
	if code != ExitUsage {
		t.Errorf("exit = %d, want %d", code, ExitUsage)
	}
}

func TestPlanAndSyncNotImplemented(t *testing.T) {
	src := tempSource(t)
	for _, name := range []string{"plan", "sync"} {
		code, _, stderr := run(t, name, src, "user@host:/data")
		if code != ExitNotImplemented {
			t.Errorf("%s exit = %d, want %d", name, code, ExitNotImplemented)
		}
		if !strings.Contains(stderr, "not implemented yet") {
			t.Errorf("%s stderr missing not-implemented message:\n%s", name, stderr)
		}
		if !strings.Contains(stderr, "user@host:/data") {
			t.Errorf("%s stderr should echo the validated destination:\n%s", name, stderr)
		}
	}
}

func TestResumeAndStatusNotImplemented(t *testing.T) {
	for _, name := range []string{"resume", "status"} {
		for _, extra := range [][]string{nil, {"run-123"}} {
			args := append([]string{name}, extra...)
			code, _, stderr := run(t, args...)
			if code != ExitNotImplemented {
				t.Errorf("Run(%v) exit = %d, want %d", args, code, ExitNotImplemented)
			}
			if !strings.Contains(stderr, "not implemented yet") {
				t.Errorf("Run(%v) stderr missing not-implemented message:\n%s", args, stderr)
			}
		}
	}
}

func TestStateCommandsRejectExtraOperands(t *testing.T) {
	for _, name := range []string{"resume", "status"} {
		code, _, _ := run(t, name, "one", "two")
		if code != ExitUsage {
			t.Errorf("%s exit = %d, want %d", name, code, ExitUsage)
		}
	}
}

func TestTransferCommandsValidateSource(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	code, _, stderr := run(t, "sync", missing, "user@host:/data")
	if code != ExitUsage {
		t.Errorf("exit = %d, want %d", code, ExitUsage)
	}
	if !strings.Contains(stderr, "source") {
		t.Errorf("stderr should describe the source problem:\n%s", stderr)
	}

	// A regular file is not a usable source directory.
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	code, _, stderr = run(t, "sync", file, "user@host:/data")
	if code != ExitUsage {
		t.Errorf("exit = %d, want %d", code, ExitUsage)
	}
	if !strings.Contains(stderr, "not a directory") {
		t.Errorf("stderr should report the non-directory source:\n%s", stderr)
	}
}

func TestTransferCommandsValidateDestination(t *testing.T) {
	src := tempSource(t)
	cases := map[string]string{
		"local path, no host":  "/data/backup",
		"relative remote path": "user@host:data",
		"empty remote path":    "user@host:",
		"empty host":           "user@:/data",
	}
	for name, dest := range cases {
		t.Run(name, func(t *testing.T) {
			code, _, stderr := run(t, "sync", src, dest)
			if code != ExitUsage {
				t.Errorf("exit = %d, want %d", code, ExitUsage)
			}
			if !strings.Contains(stderr, "destination") {
				t.Errorf("stderr should describe the destination problem:\n%s", stderr)
			}
		})
	}
}

func TestTransferCommandsAcceptIPv6Destination(t *testing.T) {
	src := tempSource(t)
	code, _, stderr := run(t, "sync", "--ipv6", src, "user@[2001:db8::1]:/data")
	if code != ExitNotImplemented {
		t.Errorf("exit = %d, want %d (stderr %q)", code, ExitNotImplemented, stderr)
	}
}

func TestTransferCommandsRequireTwoOperands(t *testing.T) {
	src := tempSource(t)
	for _, args := range [][]string{
		{"sync"},
		{"sync", src},
		{"sync", src, "host:/data", "extra"},
	} {
		code, _, _ := run(t, args...)
		if code != ExitUsage {
			t.Errorf("Run(%v) exit = %d, want %d", args, code, ExitUsage)
		}
	}
}

func TestSharedOptionValidation(t *testing.T) {
	src := tempSource(t)
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"bad backend", []string{"--backend", "bogus"}, "--backend"},
		{"bad verify", []string{"--verify", "md5"}, "--verify"},
		{"quiet and verbose", []string{"--quiet", "--verbose"}, "cannot be used together"},
		{"zero jobs", []string{"--jobs", "0"}, "--jobs"},
		{"negative streams", []string{"--streams=-1"}, "--streams"},
		{"bad threshold", []string{"--large-threshold", "ten"}, "invalid value"},
		{"empty state dir", []string{"--state-dir", ""}, "--state-dir"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := append(append([]string{"sync"}, tc.args...), src, "host:/data")
			code, _, stderr := run(t, args...)
			if code != ExitUsage {
				t.Errorf("exit = %d, want %d (stderr %q)", code, ExitUsage, stderr)
			}
			if !strings.Contains(stderr, tc.want) {
				t.Errorf("stderr should mention %q:\n%s", tc.want, stderr)
			}
		})
	}
}

func TestSharedOptionsAccepted(t *testing.T) {
	src := tempSource(t)
	args := []string{
		"sync",
		"--backend", "mscp",
		"--dry-run",
		"--verbose",
		"--ipv6",
		"--state-dir", t.TempDir(),
		"--verify", "sha256", // accepted now; may fail as unimplemented later
		"--jobs", "8",
		"--streams", "2",
		"--large-threshold", "512MiB",
		src, "host:/data",
	}
	code, _, stderr := run(t, args...)
	if code != ExitNotImplemented {
		t.Errorf("exit = %d, want %d (stderr %q)", code, ExitNotImplemented, stderr)
	}
}

func TestUnknownFlagIsUsageError(t *testing.T) {
	src := tempSource(t)
	code, _, stderr := run(t, "sync", "--frobnicate", src, "host:/data")
	if code != ExitUsage {
		t.Errorf("exit = %d, want %d", code, ExitUsage)
	}
	if !strings.Contains(stderr, "flag provided but not defined") {
		t.Errorf("stderr should report the unknown flag:\n%s", stderr)
	}
}

func TestCommandHelpFlag(t *testing.T) {
	for _, name := range []string{"doctor", "plan", "sync", "resume", "status"} {
		code, _, stderr := run(t, name, "-h")
		if code != ExitOK {
			t.Errorf("%s -h exit = %d, want %d", name, code, ExitOK)
		}
		if !strings.Contains(stderr, "Usage: catsync "+name) {
			t.Errorf("%s -h should print command usage:\n%s", name, stderr)
		}
	}
}

func TestDefaultStateDirPrefersXDG(t *testing.T) {
	xdg := t.TempDir()
	t.Setenv("XDG_STATE_HOME", xdg)
	if got, want := defaultStateDir(), filepath.Join(xdg, "catsync"); got != want {
		t.Errorf("defaultStateDir() = %q, want %q", got, want)
	}
}
