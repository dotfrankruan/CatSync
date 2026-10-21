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

func TestResumeAndStatusWithEmptyStateDir(t *testing.T) {
	dir := t.TempDir()
	code, _, stderr := run(t, "resume", "--state-dir", dir)
	if code != ExitError {
		t.Errorf("resume exit = %d, want %d", code, ExitError)
	}
	if !strings.Contains(stderr, "no runs") {
		t.Errorf("resume stderr should report the empty state dir:\n%s", stderr)
	}
	code, _, stderr = run(t, "status", "--state-dir", dir)
	if code != ExitError {
		t.Errorf("status exit = %d, want %d", code, ExitError)
	}
	if !strings.Contains(stderr, "no runs") {
		t.Errorf("status stderr should report the empty state dir:\n%s", stderr)
	}
}

func TestStatusMissingRun(t *testing.T) {
	code, _, stderr := run(t, "status", "--state-dir", t.TempDir(), "no-such-run")
	if code != ExitError {
		t.Errorf("exit = %d, want %d", code, ExitError)
	}
	if !strings.Contains(stderr, "not found") {
		t.Errorf("stderr should report the missing run:\n%s", stderr)
	}
}

// TestSHA256VerificationFailsFast checks that --verify sha256 is refused
// with a clear usage error before any transfer work happens.
func TestSHA256VerificationFailsFast(t *testing.T) {
	src := tempSource(t)
	for _, name := range []string{"plan", "sync"} {
		code, _, stderr := run(t, name, "--verify", "sha256", "--state-dir", t.TempDir(), src, "user@host:/data")
		if code != ExitUsage {
			t.Errorf("%s --verify sha256 exit = %d, want %d", name, code, ExitUsage)
		}
		if !strings.Contains(stderr, "sha256") || !strings.Contains(stderr, "not implemented") {
			t.Errorf("%s stderr should explain sha256 is not implemented:\n%s", name, stderr)
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
	// No network: the executor fails at the ssh tool check, proving the
	// bracketed IPv6 destination parsed and validated (no usage error).
	t.Setenv("CATSYNC_SSH", filepath.Join(t.TempDir(), "no-ssh"))
	code, _, _ := run(t, "sync", "--ipv6", src, "user@[2001:db8::1]:/data")
	if code != ExitError {
		t.Errorf("exit = %d, want %d", code, ExitError)
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
		{"budget exceeded", []string{"--jobs", "64", "--streams", "64"}, "budget"},
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
	// Point ssh at a nonexistent program so the command fails fast at the
	// tool check without any network access; the point is that the option
	// set parses and validates (no usage error).
	t.Setenv("CATSYNC_SSH", filepath.Join(t.TempDir(), "no-ssh"))
	args := []string{
		"plan",
		"--backend", "rsync",
		"--dry-run",
		"--verbose",
		"--ipv6",
		"--state-dir", t.TempDir(),
		"--verify", "size",
		"--jobs", "8",
		"--streams", "2",
		"--large-threshold", "512MiB",
		src, "host:/data",
	}
	code, _, stderr := run(t, args...)
	if code != ExitError {
		t.Errorf("exit = %d, want %d (runtime tool failure)", code, ExitError)
	}
	if !strings.Contains(stderr, "ssh") {
		t.Errorf("stderr should blame ssh:\n%s", stderr)
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

// TestInterspersedFlags verifies options work before, between, and after
// operands: a configuration error in a trailing flag proves the flag was
// parsed rather than swallowed as an operand.
func TestInterspersedFlags(t *testing.T) {
	src := tempSource(t)
	dest := "user@host:/data"
	cases := []struct {
		name string
		args []string
	}{
		{"flag after operands", []string{"plan", src, dest, "--backend", "bogus"}},
		{"flag before operands", []string{"plan", "--backend", "bogus", src, dest}},
		{"flag between operands", []string{"plan", src, "--backend", "bogus", dest}},
		{"equals form after operands", []string{"plan", src, dest, "--backend=bogus"}},
		{"single dash after operands", []string{"sync", src, dest, "-backend", "bogus"}},
		{"trailing invalid value", []string{"sync", src, dest, "--jobs", "0"}},
		{"trailing equals value", []string{"sync", src, dest, "--verify=md5"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, _, stderr := run(t, tc.args...)
			if code != ExitUsage {
				t.Fatalf("exit = %d, want %d as the trailing flag must be parsed (stderr %q)", code, ExitUsage, stderr)
			}
			if !strings.Contains(stderr, "catsync: plan:") && !strings.Contains(stderr, "catsync: sync:") {
				t.Errorf("stderr missing the command's configuration error:\n%s", stderr)
			}
		})
	}

	// The operand count is still enforced when flags trail the operands.
	code, _, stderr := run(t, "sync", src, "--jobs", "2")
	if code != ExitUsage || !strings.Contains(stderr, "got 1 operands") {
		t.Errorf("sync with one operand and trailing flag = %d (%q), want a usage error naming 1 operand", code, stderr)
	}

	// "--" ends option parsing: flag-like tokens after it are operands.
	code, _, stderr = run(t, "sync", src, dest, "--", "--jobs")
	if code != ExitUsage || !strings.Contains(stderr, "got 3 operands") {
		t.Errorf("sync with -- and trailing token = %d (%q), want a usage error naming 3 operands", code, stderr)
	}
}

// TestInterspersedStateFlags covers resume/status with the run identifier
// before or after the options.
func TestInterspersedStateFlags(t *testing.T) {
	dir := t.TempDir()
	cases := [][]string{
		{"status", "no-such-run", "--state-dir", dir},
		{"status", "--state-dir", dir, "no-such-run"},
		{"resume", "no-such-run", "--state-dir", dir},
	}
	for _, args := range cases {
		code, _, stderr := run(t, args...)
		if code != ExitError {
			t.Errorf("Run(%v) exit = %d, want %d (stderr %q)", args, code, ExitError, stderr)
		}
		if !strings.Contains(stderr, "not found") {
			t.Errorf("Run(%v) stderr should report the missing run:\n%s", args, stderr)
		}
	}
	// More than one positional operand is still a usage error even with
	// trailing options.
	code, _, stderr := run(t, "status", "a", "b", "--state-dir", dir)
	if code != ExitUsage || !strings.Contains(stderr, "got 2 operands") {
		t.Errorf("status with two operands = %d (%q), want a usage error", code, stderr)
	}
}

// TestInvalidSSHOverrideIsUsageError checks an ssh override rsync's -e
// cannot represent is refused as configuration, before any remote work.
func TestInvalidSSHOverrideIsUsageError(t *testing.T) {
	src := tempSource(t)
	t.Setenv("CATSYNC_SSH", "/opt/with space/ssh")
	code, _, stderr := run(t, "sync", "--state-dir", t.TempDir(), src, "user@host:/data")
	if code != ExitUsage {
		t.Fatalf("exit = %d, want %d (stderr %q)", code, ExitUsage, stderr)
	}
	if !strings.Contains(stderr, "whitespace") {
		t.Errorf("stderr should explain the whitespace restriction:\n%s", stderr)
	}
}
