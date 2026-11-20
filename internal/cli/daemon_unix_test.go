//go:build unix

package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeSSH writes an ssh stand-in and points CATSYNC_SSH at it.
func fakeSSH(t *testing.T, script string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "ssh")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+script+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CATSYNC_SSH", path)
}

func TestDaemonProbePrintsRemoteReport(t *testing.T) {
	fakeSSH(t, `echo '{"version":"1.2.3","protocol":1,"platform":"linux/amd64","default_port":43411,"features":{"sftp":true,"exec":false,"manifest":true,"metadata":true,"hash":true,"stat":true,"samefile":true}}'`)
	var stdout, stderr strings.Builder
	code := Run([]string{"daemon", "probe", "u@host"}, &stdout, &stderr, "test")
	if code != ExitOK {
		t.Fatalf("exit = %d, stderr %q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"protocol":1`) || !strings.Contains(stdout.String(), `"exec":false`) {
		t.Errorf("probe output = %q", stdout.String())
	}
}

func TestDaemonProbeMissingRemoteBinary(t *testing.T) {
	fakeSSH(t, `echo 'bash: catsyncd: command not found' >&2; exit 127`)
	var stdout, stderr strings.Builder
	code := Run([]string{"daemon", "probe", "host"}, &stdout, &stderr, "test")
	if code != ExitError {
		t.Fatalf("exit = %d, want %d", code, ExitError)
	}
	msg := stderr.String()
	for _, want := range []string{"not installed", "Install it yourself", "--remote-catsyncd"} {
		if !strings.Contains(msg, want) {
			t.Errorf("message %q lacks %q", msg, want)
		}
	}
}

func TestDaemonProbeRejectsInvalidOutput(t *testing.T) {
	fakeSSH(t, `echo 'not json'`)
	var stdout, stderr strings.Builder
	if code := Run([]string{"daemon", "probe", "host"}, &stdout, &stderr, "test"); code != ExitError {
		t.Fatalf("exit = %d, want %d", code, ExitError)
	}
	if !strings.Contains(stderr.String(), "not a JSON probe report") {
		t.Errorf("stderr = %q", stderr.String())
	}
}

func TestDaemonProbeValidatesHost(t *testing.T) {
	var stdout, stderr strings.Builder
	if code := Run([]string{"daemon", "probe", "user@host:/path"}, &stdout, &stderr, "test"); code != ExitUsage {
		t.Fatalf("exit = %d, want usage", code)
	}
	if code := Run([]string{"daemon", "probe"}, &stdout, &stderr, "test"); code != ExitUsage {
		t.Fatalf("missing operand: exit = %d", code)
	}
	if code := Run([]string{"daemon", "bogus"}, &stdout, &stderr, "test"); code != ExitUsage {
		t.Fatalf("unknown subcommand: exit = %d", code)
	}
}
