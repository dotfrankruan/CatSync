package cli

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// fakeTool installs an executable shell script named name into dir.
func fakeTool(t *testing.T, dir, name, script string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("fake PATH tools rely on /bin/sh")
	}
	path := filepath.Join(dir, name)
	content := "#!/bin/sh\n" + script
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		t.Fatal(err)
	}
}

// fakePath replaces PATH with a directory containing exactly the named fake
// tools and returns it.
func fakePath(t *testing.T, tools map[string]string) string {
	t.Helper()
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("test requires /bin/sh")
	}
	dir := t.TempDir()
	for name, script := range tools {
		fakeTool(t, dir, name, script)
	}
	t.Setenv("PATH", dir)
	return dir
}

func runDoctorCmd(t *testing.T, timeout time.Duration, opts Options) (code int, stdout, stderr string) {
	t.Helper()
	var out, errOut bytes.Buffer
	code = runDoctor(&out, &errOut, opts, timeout)
	return code, out.String(), errOut.String()
}

func TestDoctorAllToolsPresent(t *testing.T) {
	// fpsync, fpart, and mscp may be installed on the host; doctor must not
	// probe them at all: only rsync and ssh matter.
	fakePath(t, map[string]string{
		"rsync":  "echo 'rsync  version 3.2.7  protocol version 31'",
		"ssh":    "echo 'OpenSSH_9.7p1, LibreSSL 3.3.6' >&2", // real ssh -V prints to stderr
		"fpsync": "echo SHOULD-NOT-BE-PROBED",
		"fpart":  "echo SHOULD-NOT-BE-PROBED",
		"mscp":   "echo SHOULD-NOT-BE-PROBED",
	})
	code, stdout, stderr := runDoctorCmd(t, 2*time.Second, Options{})
	if code != ExitOK {
		t.Errorf("exit = %d, want %d (stderr %q)", code, ExitOK, stderr)
	}
	for _, want := range []string{
		"rsync", "ssh",
		"rsync  version 3.2.7", "OpenSSH_9.7p1",
		"all required tools are available",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("stdout missing %q:\n%s", want, stdout)
		}
	}
	if strings.Contains(stdout, "missing") {
		t.Errorf("no tool should be reported missing:\n%s", stdout)
	}
	for _, bad := range []string{"fpsync", "fpart", "mscp", "SHOULD-NOT-BE-PROBED"} {
		if strings.Contains(stdout, bad) {
			t.Errorf("doctor probed or listed %q:\n%s", bad, stdout)
		}
	}
}

// TestDoctorOnlyRequiredToolsProbed: with rsync and ssh present, the
// report is complete; no optional tool (fpsync, fpart, mscp) is probed.
func TestDoctorOnlyRequiredToolsProbed(t *testing.T) {
	fakePath(t, map[string]string{
		"rsync": "echo 'rsync  version 3.2.7'",
		"ssh":   "echo 'OpenSSH_9.7p1'",
	})
	code, stdout, stderr := runDoctorCmd(t, 2*time.Second, Options{})
	if code != ExitOK {
		t.Errorf("exit = %d, want %d (stderr %q)", code, ExitOK, stderr)
	}
	if strings.Contains(stdout, "mscp") || strings.Contains(stdout, "fpsync") || strings.Contains(stdout, "fpart") {
		t.Errorf("an optional tool was probed:\n%s", stdout)
	}
}

func TestDoctorMissingRequiredToolFails(t *testing.T) {
	cases := []struct {
		name    string
		tools   map[string]string
		missing string
	}{
		{"no rsync", map[string]string{"ssh": "echo v"}, "rsync"},
		{"no ssh", map[string]string{"rsync": "echo v"}, "ssh"},
		{"neither", nil, "rsync, ssh"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fakePath(t, tc.tools)
			code, stdout, stderr := runDoctorCmd(t, 2*time.Second, Options{})
			if code != ExitError {
				t.Errorf("exit = %d, want %d", code, ExitError)
			}
			if !strings.Contains(stderr, tc.missing) {
				t.Errorf("stderr should name missing required tools %q:\n%s", tc.missing, stderr)
			}
			if !strings.Contains(stdout, "REQUIRED") {
				t.Errorf("stdout should mark the tool as REQUIRED:\n%s", stdout)
			}
		})
	}
}

func TestDoctorProbeTimeoutIsBounded(t *testing.T) {
	fakePath(t, map[string]string{
		"rsync": "exec /bin/sleep 30",
		"ssh":   "echo 'OpenSSH_9.7p1'",
	})
	start := time.Now()
	code, stdout, stderr := runDoctorCmd(t, 150*time.Millisecond, Options{})
	elapsed := time.Since(start)
	if code != ExitOK {
		t.Errorf("exit = %d, want %d: a hung required tool is degraded, not missing (stderr %q)", code, ExitOK, stderr)
	}
	if !strings.Contains(stdout, "timed out") {
		t.Errorf("stdout should report the probe timeout:\n%s", stdout)
	}
	if elapsed > 5*time.Second {
		t.Errorf("doctor took %s; probe timeout was not enforced", elapsed)
	}
}

func TestDoctorFailingVersionProbeStillCountsAsFound(t *testing.T) {
	fakePath(t, map[string]string{
		"rsync": "echo 'no version here' >&2; exit 1",
		"ssh":   "echo 'OpenSSH_9.7p1'",
	})
	code, stdout, stderr := runDoctorCmd(t, 2*time.Second, Options{})
	if code != ExitOK {
		t.Errorf("exit = %d, want %d (stderr %q)", code, ExitOK, stderr)
	}
	if !strings.Contains(stdout, "version probe failed") {
		t.Errorf("stdout should explain the degraded probe:\n%s", stdout)
	}
}

func TestDoctorQuietHidesHealthyTools(t *testing.T) {
	fakePath(t, map[string]string{
		"rsync": "echo v",
		"ssh":   "echo v",
	})
	code, stdout, _ := runDoctorCmd(t, 2*time.Second, Options{Quiet: true})
	if code != ExitOK {
		t.Errorf("exit = %d, want %d", code, ExitOK)
	}
	if strings.Contains(stdout, "rsync") || strings.Contains(stdout, "ssh") {
		t.Errorf("quiet output should hide found tools:\n%s", stdout)
	}
	// With no optional tools left, quiet output lists nothing missing.
	if strings.Contains(stdout, "missing") {
		t.Errorf("quiet output should list nothing missing:\n%s", stdout)
	}
}

func TestDoctorVerbosePrintsFullVersionOutput(t *testing.T) {
	fakePath(t, map[string]string{
		"rsync": "echo line-one; echo line-two",
		"ssh":   "echo v",
	})
	code, stdout, _ := runDoctorCmd(t, 2*time.Second, Options{Verbose: true})
	if code != ExitOK {
		t.Errorf("exit = %d, want %d", code, ExitOK)
	}
	if !strings.Contains(stdout, "line-one") || !strings.Contains(stdout, "line-two") {
		t.Errorf("verbose output should include all version lines:\n%s", stdout)
	}
}

func TestDoctorViaRunDispatch(t *testing.T) {
	fakePath(t, map[string]string{
		"rsync": "echo v",
		"ssh":   "echo v",
	})
	code, stdout, _ := run(t, "doctor")
	if code != ExitOK {
		t.Errorf("exit = %d, want %d", code, ExitOK)
	}
	if !strings.Contains(stdout, "all required tools are available") {
		t.Errorf("stdout missing summary:\n%s", stdout)
	}
}

func TestDoctorRejectsOperandsAndConflictingFlags(t *testing.T) {
	code, _, _ := run(t, "doctor", "extra")
	if code != ExitUsage {
		t.Errorf("doctor extra exit = %d, want %d", code, ExitUsage)
	}
	code, _, _ = run(t, "doctor", "--quiet", "--verbose")
	if code != ExitUsage {
		t.Errorf("doctor --quiet --verbose exit = %d, want %d", code, ExitUsage)
	}
}
