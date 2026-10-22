// Package process runs external commands with context cancellation and
// without any shell.
//
// Every execution is a direct exec.Command call with an argument array: no
// shell is involved locally, so arguments carry any byte except NUL as
// literal data.
//
// On Unix the child is started as the leader of a new process group. When
// the context is cancelled — typically because catsync received SIGINT or
// SIGTERM — the same termination signal is forwarded to the whole group,
// descendants included; after a bounded grace period any survivor is
// SIGKILLed. Run returns only after the direct child has been waited for
// and the process group has been verified empty, so rsync and ssh
// descendants cannot outlive Run. stdout/stderr pipes cannot block
// cancellation forever: exec.Cmd.WaitDelay bounds them by the same grace
// period.
//
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
	"sync"
	"time"
)

// DefaultGracePeriod is the time granted between the graceful signal sent
// to a process group and the SIGKILL that force-cleans any survivor. It is
// also the bound on waiting for output pipes to close after the child
// exits.
const DefaultGracePeriod = 5 * time.Second

// ErrDescendantsAlive reports that a process group still had live
// processes after the grace period and the forcing SIGKILL, so Run could
// not guarantee the tree is gone.
var ErrDescendantsAlive = errors.New("process: descendant processes survived cleanup")

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
	// Cancellation is bounded even when a grandchild inherits a pipe:
	// WaitDelay closes the pipes after the grace period, so Run cannot
	// block forever on them.
	Stdout io.Writer
	Stderr io.Writer
	// GracePeriod overrides DefaultGracePeriod: how long a cancelled
	// process group gets to exit after the forwarded signal before it is
	// SIGKILLed. Non-positive values select the default.
	GracePeriod time.Duration
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

// CancelSignal records which signal cancelled a context, so Run can
// forward the same signal to the child's process group instead of always
// sending SIGTERM. It is safe for concurrent use; the signal is set by a
// signal handler goroutine that may run after Run reads it, in which case
// the SIGTERM default applies.
type CancelSignal struct {
	mu  sync.Mutex
	sig os.Signal
}

// NewCancelSignal returns an empty signal record.
func NewCancelSignal() *CancelSignal { return &CancelSignal{} }

// Set records the signal that triggered cancellation.
func (c *CancelSignal) Set(sig os.Signal) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sig = sig
}

// Get returns the recorded signal, if any.
func (c *CancelSignal) Get() (os.Signal, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sig, c.sig != nil
}

type cancelSignalKey struct{}

// WithCancelSignal attaches a CancelSignal record to ctx.
func WithCancelSignal(ctx context.Context, c *CancelSignal) context.Context {
	return context.WithValue(ctx, cancelSignalKey{}, c)
}

// CancelSignalFromContext returns the CancelSignal attached to ctx.
func CancelSignalFromContext(ctx context.Context) (*CancelSignal, bool) {
	c, ok := ctx.Value(cancelSignalKey{}).(*CancelSignal)
	return c, ok
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
//   - nil: the subprocess exited with status 0 and no descendants survived.
//   - *ExitError: the subprocess ran but failed (non-zero exit or signal
//     not caused by the context). errors.As also reaches *exec.ExitError.
//   - an error wrapping ctx.Err(): the context was cancelled or its
//     deadline expired. The direct child has been killed and waited for,
//     and the process group has been cleaned up, before Run returns.
//   - an error wrapping ErrDescendantsAlive: cleanup could not guarantee
//     the process tree is gone.
//   - any other error: the process could not be started at all.
//
// validateSpec rejects unusable specs before any process starts.
func validateSpec(spec Spec) error {
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
	return nil
}

// buildCmd constructs the exec.Cmd for spec: argument array, working
// directory, environment additions, and the given stdio.
func buildCmd(spec Spec) *exec.Cmd {
	cmd := exec.Command(spec.Name, spec.Args...)
	cmd.Dir = spec.Dir
	if spec.Env != nil {
		// Additions on top of the inherited environment; on duplicate
		// keys os/exec keeps the last value, which is the addition.
		cmd.Env = append(os.Environ(), spec.Env...)
	}
	cmd.Stdin = spec.Stdin
	cmd.Stdout = spec.Stdout
	cmd.Stderr = spec.Stderr
	return cmd
}

func Run(ctx context.Context, spec Spec) error {
	if ctx == nil {
		return errors.New("process: nil context")
	}
	if err := validateSpec(spec); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		// A pre-cancelled context must not start a subprocess at all:
		// running then killing it would be a race, not a cancellation.
		return fmt.Errorf("process: %s: %w", spec.Name, err)
	}
	h, err := startInteractive(ctx, buildCmd(spec), spec)
	if err != nil {
		return err
	}
	return h.Wait()
}

// StartInteractive starts spec exactly like Run, but returns a Handle for
// interactive sessions: the caller drives the subprocess's stdin/stdout
// while it runs (for example an SFTP session over ssh). The caller must
// call Wait once the session is over; Wait reaps the process and cleans up
// the process group exactly like Run. A pre-cancelled context fails before
// the process starts.
func StartInteractive(ctx context.Context, spec Spec) (*Handle, error) {
	if ctx == nil {
		return nil, errors.New("process: nil context")
	}
	if err := validateSpec(spec); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("process: %s: %w", spec.Name, err)
	}
	return startInteractive(ctx, buildCmd(spec), spec)
}

// classifyRunResult converts the outcomes of one execution into the
// package's error vocabulary.
//
// ctxErr is non-nil only when the cancellation watcher actually observed
// the context's cancellation and tore the process group down; the caller
// must not pass a late ctx.Err() observed after the direct child already
// finished, or a normally completed command could be reclassified as
// interrupted. A watcher-observed cancellation is always reported as an
// error wrapping ctxErr, even when the child handled the forwarded signal
// and exited 0: the command did not finish on its own, and callers use the
// distinction to keep the run resumable instead of marking it complete.
// cleanupErr reports that the process group could not be proven empty;
// when both apply, errors.Join keeps errors.Is working for each.
func classifyRunResult(name string, waitErr, ctxErr, cleanupErr error) error {
	var errs []error
	if ctxErr != nil {
		errs = append(errs, fmt.Errorf("process: %s terminated: %w", name, ctxErr))
	}
	if cleanupErr != nil {
		errs = append(errs, cleanupErr)
	}
	if waitErr == nil {
		if len(errs) == 0 {
			return nil
		}
		return errors.Join(errs...)
	}
	if errors.Is(waitErr, exec.ErrWaitDelay) {
		// Keep the diagnostic WaitDelay cause even when the same run was
		// also cancelled (a survivor held a pipe past the grace period).
		errs = append(errs, fmt.Errorf("process: %s: output pipes stayed open past the grace period: %w", name, waitErr))
		return errors.Join(errs...)
	}
	if ctxErr != nil {
		// Cancellation caused the termination: the context error carries
		// the meaning, so the signal-induced wait error is not repeated.
		return errors.Join(errs...)
	}
	if len(errs) > 0 {
		// A cleanup failure with a non-cancelled non-zero exit: keep both
		// causes so errors.As still reaches *ExitError when applicable.
		var exitErr *exec.ExitError
		if errors.As(waitErr, &exitErr) {
			errs = append(errs, &ExitError{Name: name, Code: exitErr.ExitCode(), Err: exitErr})
			return errors.Join(errs...)
		}
		errs = append(errs, fmt.Errorf("process: %s: %w", name, waitErr))
		return errors.Join(errs...)
	}
	var exitErr *exec.ExitError
	if errors.As(waitErr, &exitErr) {
		return &ExitError{Name: name, Code: exitErr.ExitCode(), Err: exitErr}
	}
	return fmt.Errorf("process: %s: %w", name, waitErr)
}
