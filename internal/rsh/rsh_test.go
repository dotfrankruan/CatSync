//go:build unix

package rsh

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

func TestRunHelperRequiresAgentEnvironment(t *testing.T) {
	for _, env := range []string{EnvSocket, EnvToken, EnvHost} {
		t.Setenv(env, "")
	}
	t.Setenv(EnvUser, "u")
	var stderr bytes.Buffer
	code := runHelperWithStderr([]string{"-l", "u", "host", "rsync", "--server", ".", "/x"}, &stderr)
	if code != 255 {
		t.Fatalf("exit = %d, want 255", code)
	}
	if !strings.Contains(stderr.String(), "internal") {
		t.Errorf("stderr = %q", stderr.String())
	}
}

func TestRunHelperRejectsWrongHostAndProgram(t *testing.T) {
	t.Setenv(EnvSocket, "/nonexistent/sock")
	t.Setenv(EnvToken, "tok")
	t.Setenv(EnvHost, "expected-host")
	t.Setenv(EnvUser, "u")
	var stderr bytes.Buffer
	if code := runHelperWithStderr([]string{"-l", "u", "other-host", "rsync", "--server", ".", "/x"}, &stderr); code != 255 {
		t.Errorf("wrong host exit = %d", code)
	}
	stderr.Reset()
	if code := runHelperWithStderr([]string{"-l", "u", "expected-host", "sh", "-c", "id"}, &stderr); code != 255 {
		t.Errorf("wrong program exit = %d", code)
	}
}

// runHelperWithStderr isolates RunHelper's stderr for tests; it swaps
// os.Stderr (the helper is a process-level command).
func runHelperWithStderr(args []string, stderr *bytes.Buffer) int {
	old := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		return 255
	}
	os.Stderr = w
	code := RunHelper(args)
	_ = w.Close()
	os.Stderr = old
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)
	stderr.Write(buf.Bytes())
	return code
}
