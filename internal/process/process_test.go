package process

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"
)

func TestRunRequiresProgram(t *testing.T) {
	err := Run(context.Background(), Spec{})
	if err == nil || !strings.Contains(err.Error(), "empty program name") {
		t.Fatalf("Run with empty name = %v, want empty-name error", err)
	}
}

func TestRunNilContext(t *testing.T) {
	// Deliberately exercise the nil-context guard.
	if err := Run(nil, Spec{Name: "true"}); err == nil {
		t.Fatal("Run with nil context = nil, want error")
	}
}

func TestRunRejectsNUL(t *testing.T) {
	if err := Run(context.Background(), Spec{Name: "a\x00b"}); err == nil {
		t.Fatal("Run with NUL in name = nil, want error")
	}
	if err := Run(context.Background(), Spec{Name: "true", Args: []string{"a\x00b"}}); err == nil {
		t.Fatal("Run with NUL in args = nil, want error")
	}
}

func TestRunStartFailure(t *testing.T) {
	err := Run(context.Background(), Spec{Name: "/nonexistent/catsync-no-such-program"})
	if err == nil {
		t.Fatal("Run with missing program = nil, want error")
	}
	var exitErr *ExitError
	if errors.As(err, &exitErr) {
		t.Fatalf("start failure classified as ExitError: %v", err)
	}
	if !strings.Contains(err.Error(), "start") {
		t.Fatalf("start failure error = %q, want it to mention the start", err)
	}
}

func TestExitErrorFormatting(t *testing.T) {
	ee := &ExitError{Name: "tool", Code: 23, Err: &exec.ExitError{}}
	if got := ee.Error(); !strings.Contains(got, "tool") || !strings.Contains(got, "23") {
		t.Errorf("ExitError.Error() = %q, want program and code", got)
	}
	if !errors.Is(ee, ee.Err) {
		t.Error("ExitError does not unwrap to the underlying *exec.ExitError")
	}
	sig := &ExitError{Name: "tool", Code: -1, Err: &exec.ExitError{}}
	if got := sig.Error(); !strings.Contains(got, "tool") {
		t.Errorf("signal ExitError.Error() = %q, want program name", got)
	}
}

func TestQuoteArg(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"", "''"},
		{"simple", "simple"},
		{"a/b-c_d.txt", "a/b-c_d.txt"},
		{"with space", "'with space'"},
		{"new\nline", "'new\nline'"},
		{"quote'inside", `'quote'\''inside'`},
		{"$HOME", "'$HOME'"},
		{"*glob*", "'*glob*'"},
		{"`id`", "'`id`'"},
		{"$(id)", "'$(id)'"},
		{"-leading", "-leading"},
		{"ünïcødé", "'ünïcødé'"},
	}
	for _, tc := range cases {
		if got := QuoteArg(tc.in); got != tc.want {
			t.Errorf("QuoteArg(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestSpecStringIsDiagnosticOnly(t *testing.T) {
	s := Spec{
		Name: "rsync",
		Args: []string{"--archive", "with space", "quote'x", ""},
		Env:  []string{"SECRET=hunter2"},
	}
	got := s.String()
	want := "rsync --archive 'with space' 'quote'\\''x' ''"
	if got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
	if strings.Contains(got, "hunter2") || strings.Contains(got, "SECRET") {
		t.Errorf("String() leaks environment: %q", got)
	}
}
