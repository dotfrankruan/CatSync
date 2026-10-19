// Package process runs external commands with context cancellation and
// without any shell.
//
// Every execution is a direct exec.CommandContext call with an argument
// array: no shell is involved locally, so arguments carry any byte except
// NUL as literal data. When the context is cancelled, the direct child is
// killed and waited for, so it is never orphaned or left as a zombie.
// Subprocess failures are returned as *ExitError, which wraps the
// underlying *exec.ExitError.
package process

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

// Spec describes one command execution.
type Spec struct {
	// Name is the program to execute: an absolute or relative path, or a
	// bare name resolved through PATH. It must not be empty.
	Name string
	// Args is the argument vector after the program name. Each element is
	// passed to the child verbatim; no shell ever splits, joins, or
	// expands them.
	Args []string
	// Dir is the working directory; empty inherits the current directory.
	Dir string
	// Env lists environment additions (KEY=value) applied on top of the
	// current process environment; nil runs the child with the inherited
	// environment unchanged. On duplicate keys the addition wins.
	//
	// Env values are never included in errors or diagnostic renderings,
	// so secrets passed this way cannot leak through logs.
	Env []string
	// Stdin supplies standard input; nil connects the null device.
	Stdin io.Reader
	// Stdout and Stderr receive the output; nil discards it.
	//
	// When a writer is not an *os.File, os/exec copies through a pipe and
	// Wait blocks until every process holding the pipe — including
	// grandchildren that survived a cancelled direct child — closes it.
	// Callers that need prompt cancellation of tools with child processes
	// should pass *os.File writers (for example a temporary file).
	Stdout io.Writer
	Stderr io.Writer
}

// String renders the command for diagnostics only: the program name and
// arguments, each shell-quoted so the result is safe to paste into a POSIX
// shell for manual reproduction. The rendering is never parsed or executed;
// execution always uses the argument array. Environment additions are
// deliberately omitted so secrets never appear in logs.
func (s Spec) String() string {
	quoted := make([]string, 0, len(s.Args)+1)
	quoted = append(quoted, QuoteArg(s.Name))
	for _, a := range s.Args {
		quoted = append(quoted, QuoteArg(a))
	}
	return strings.Join(quoted, " ")
}

// QuoteArg renders one argument as a POSIX-shell word: wrapped in single
// quotes, with each embedded single quote replaced by the end-quote /
// escaped-quote / reopen-quote sequence. The result carries any byte except
// NUL — spaces, newlines, quotes, and expansions included — as literal
// text. It exists for diagnostics only.
func QuoteArg(s string) string {
	if s == "" {
		return "''"
	}
	// Fast path: a word of safe characters needs no quoting.
	safe := true
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case strings.ContainsRune("._-/:+@%^,", r):
		default:
			safe = false
		}
		if !safe {
			break
		}
	}
	if safe {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// ExitError reports a subprocess that started but did not succeed: a
// non-zero exit status, or termination by a signal (Code is then -1 and
// Signal reports the signal where the platform exposes it).
type ExitError struct {
	// Name is the program that was run.
	Name string
	// Code is the exit status, or -1 when the process was killed by a
	// signal.
	Code int
	// Err is the underlying *exec.ExitError.
	Err *exec.ExitError
}

// Error describes the failure. It never includes environment or output
// content.
func (e *ExitError) Error() string {
	if e.Code >= 0 {
		return fmt.Sprintf("process: %s exited with status %d", e.Name, e.Code)
	}
	if sig, ok := exitSignal(e.Err); ok {
		return fmt.Sprintf("process: %s terminated by signal %s", e.Name, sig)
	}
	return fmt.Sprintf("process: %s terminated: %v", e.Name, e.Err)
}

// Unwrap returns the underlying *exec.ExitError.
func (e *ExitError) Unwrap() error { return e.Err }

// Run executes spec and waits for completion.
//
// The result is classified so callers can distinguish the outcomes:
//
//   - nil: the subprocess exited with status 0.
//   - *ExitError: the subprocess ran but failed (non-zero exit or signal
//     not caused by the context). errors.As also reaches *exec.ExitError.
//   - an error wrapping ctx.Err(): the context was cancelled or its
//     deadline expired. The direct child has been killed and reaped before
//     Run returns; it is never left running or as a zombie.
//   - any other error: the process could not be started at all.
func Run(ctx context.Context, spec Spec) error {
	if ctx == nil {
		return errors.New("process: nil context")
	}
	if spec.Name == "" {
		return errors.New("process: empty program name")
	}
	if strings.ContainsRune(spec.Name, 0) {
		return errors.New("process: program name contains a NUL byte")
	}
	for _, a := range spec.Args {
		if strings.ContainsRune(a, 0) {
			return fmt.Errorf("process: argument %q contains a NUL byte", a)
		}
	}
	// CommandContext kills the direct child when ctx is done; Wait below
	// reaps it, so the child is terminated and waited for, never orphaned.
	cmd := exec.CommandContext(ctx, spec.Name, spec.Args...)
	cmd.Dir = spec.Dir
	if spec.Env != nil {
		// Additions on top of the inherited environment; on duplicate
		// keys os/exec keeps the last value, which is the addition.
		cmd.Env = append(os.Environ(), spec.Env...)
	}
	cmd.Stdin = spec.Stdin
	cmd.Stdout = spec.Stdout
	cmd.Stderr = spec.Stderr

	err := cmd.Run()
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return fmt.Errorf("process: %s terminated: %w", spec.Name, ctx.Err())
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return &ExitError{Name: spec.Name, Code: exitErr.ExitCode(), Err: exitErr}
	}
	return fmt.Errorf("process: start %s: %w", spec.Name, err)
}
