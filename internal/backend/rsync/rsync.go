// Package rsync builds and runs CatSync's bidirectional conservative
// transfer stage: a disjoint set of relative file paths streamed to rsync over a
// NUL-delimited --files-from list, with archive semantics, partial-transfer
// retention in a relative --partial-dir, and never any deletion.
//
// The command shape is fixed by policy:
//
//	rsync --archive --partial --partial-dir=<relative> --relative \
//	      --files-from=- --from0 [--dry-run] -e <ssh transport> \
//	      -- <source-root>/ <destination-root>/
//
// The per-file Config never emits --whole-file, --inplace, or --delete.
// MirrorConfig is a separate, explicit whole-tree reconciliation used only by
// the `sync` command after all ordinary transfer stages succeed; it uses
// --delete-delay so removals happen after successful transfer work. File names travel
// as NUL-delimited standard-input data, not as arguments, so spaces,
// newlines, quotes, Unicode, and leading dashes are all safe; every path is
// validated as a clean relative path first. The SSH transport is a fixed
// token sequence ("ssh" or "ssh -6"), so rsync's splitting of the -e value
// cannot be influenced by operands.
package rsync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/dotfrankruan/CatSync/internal/backend"
	"github.com/dotfrankruan/CatSync/internal/discovery"
	"github.com/dotfrankruan/CatSync/internal/process"
	"github.com/dotfrankruan/CatSync/internal/target"
)

// DefaultPartialDir is the relative --partial-dir used when
// Config.PartialDir is empty. Being relative, it stays inside the
// destination directory it belongs to.
const DefaultPartialDir = ".catsync-rsync-partial"

// forbiddenFlags lists rsync options CatSync's safety policy bans; tests
// assert they never appear in a built command.
var forbiddenFlags = []string{"--delete", "--whole-file", "--inplace"}

// Config describes one rsync transfer of a disjoint relative-path set.
type Config struct {
	// Program is the rsync executable; empty means "rsync" (PATH lookup).
	Program string
	// Direction selects upload or download. The zero value preserves the
	// historical local-to-remote behavior.
	Direction target.Direction
	// SourceDir is the absolute local root: the source for uploads and the
	// destination for downloads.
	SourceDir string
	// Dest is the validated remote endpoint: the destination for uploads
	// and the source for downloads.
	Dest target.Destination
	// SSH carries SSH transport options.
	SSH backend.SSH
	// DryRun adds --dry-run: rsync reports what it would transfer.
	DryRun bool
	// PartialDir is the relative --partial-dir; empty means
	// DefaultPartialDir.
	PartialDir string
	// Paths is the disjoint set of slash-separated paths relative to
	// SourceDir. An empty set is valid: Run then does nothing.
	Paths []string
	// NoImpliedDirs adds --no-implied-dirs: the implied parent
	// directories are neither created nor updated with source attributes,
	// so concurrent rsync batches that share destination parents cannot
	// race on directory metadata. Callers that set this must ensure the
	// parent directories already exist (CatSync creates them with one
	// batched remote mkdir before launching batches).
	NoImpliedDirs bool
	// Env lists environment additions applied on top of the inherited
	// environment (see process.Spec.Env); nil inherits it unchanged.
	Env []string
	// Stdout and Stderr receive rsync's output; nil discards it.
	Stdout io.Writer
	Stderr io.Writer
	// Progress receives successful-byte deltas parsed from rsync progress2.
	// When set, Command enables machine-stable progress output.
	Progress func(uint64)
}

// Command validates the configuration and returns the argument vector
// (including the program name at index 0) and the NUL-delimited
// --files-from payload for standard input. Every relative path is checked
// with discovery.ValidateRelPath, so the command can never reference
// anything outside the source or destination root.
func (c *Config) Command() (argv []string, stdin []byte, err error) {
	program := c.Program
	if program == "" {
		program = "rsync"
	}
	if strings.ContainsRune(program, 0) {
		return nil, nil, fmt.Errorf("rsync: program name contains a NUL byte")
	}
	if c.SourceDir == "" {
		return nil, nil, errors.New("rsync: source directory is empty")
	}
	if strings.ContainsRune(c.SourceDir, 0) {
		return nil, nil, fmt.Errorf("rsync: source directory %q contains a NUL byte", c.SourceDir)
	}
	if !filepath.IsAbs(c.SourceDir) {
		return nil, nil, fmt.Errorf("rsync: source directory %q is not absolute", c.SourceDir)
	}
	direction := c.Direction
	if direction == "" {
		direction = target.LocalToRemote
	}
	if direction != target.LocalToRemote && direction != target.RemoteToLocal {
		return nil, nil, fmt.Errorf("rsync: invalid transfer direction %q", c.Direction)
	}
	if err := backend.ValidateDestination(c.Dest); err != nil {
		return nil, nil, err
	}
	// Defense in depth: the transport override must be representable in
	// rsync's whitespace-split -e value even when this backend is used
	// without the executor's configuration validation.
	if err := c.SSH.ValidateTransport(); err != nil {
		return nil, nil, err
	}
	partialDir := c.PartialDir
	if partialDir == "" {
		partialDir = DefaultPartialDir
	}
	if err := discovery.ValidateRelPath(partialDir); err != nil {
		return nil, nil, fmt.Errorf("rsync: partial dir: %w", err)
	}
	seen := make(map[string]struct{}, len(c.Paths))
	var payload bytes.Buffer
	for _, p := range c.Paths {
		if err := discovery.ValidateRelPath(p); err != nil {
			return nil, nil, fmt.Errorf("rsync: file list: %w", err)
		}
		if _, dup := seen[p]; dup {
			return nil, nil, fmt.Errorf("rsync: file list: duplicate path %q", p)
		}
		seen[p] = struct{}{}
		payload.WriteString(p)
		payload.WriteByte(0)
	}

	args := []string{
		program,
		"--archive",
		"--partial",
		"--partial-dir=" + partialDir,
		"--relative",
		"--files-from=-",
		"--from0",
	}
	if c.NoImpliedDirs {
		args = append(args, "--no-implied-dirs")
	}
	if c.Progress != nil {
		args = append(args, "--info=progress2", "--outbuf=L")
	}
	if c.DryRun {
		args = append(args, "--dry-run")
	}
	args = append(args, "-e", c.SSH.RsyncTransport())
	// Both local operands are absolute and the remote endpoint was validated;
	// "--" ends option parsing for defense in depth.
	if direction == target.RemoteToLocal {
		args = append(args, "--", remoteWithTrailingSlash(c.Dest), withTrailingSlash(c.SourceDir))
	} else {
		args = append(args, "--", withTrailingSlash(c.SourceDir), c.Dest.String())
	}
	return args, payload.Bytes(), nil
}

func remoteWithTrailingSlash(remote target.Destination) string {
	if !strings.HasSuffix(remote.Path, "/") {
		remote.Path += "/"
	}
	return remote.String()
}

// Run executes the configured transfer and waits for completion. An empty
// path set succeeds without executing anything. A non-zero rsync exit is
// returned as *process.ExitError; context cancellation terminates and reaps
// rsync and returns an error wrapping the context's cause.
func (c *Config) Run(ctx context.Context) error {
	argv, stdin, err := c.Command()
	if err != nil {
		return err
	}
	if len(c.Paths) == 0 {
		return nil
	}
	stdout := c.Stdout
	var pw *progressWriter
	if c.Progress != nil {
		pw = &progressWriter{forward: c.Stdout, report: c.Progress}
		stdout = pw
		defer pw.Flush()
	}
	if err := process.Run(ctx, process.Spec{
		Name:   argv[0],
		Args:   argv[1:],
		Env:    c.Env,
		Stdin:  bytes.NewReader(stdin),
		Stdout: stdout,
		Stderr: c.Stderr,
	}); err != nil {
		return fmt.Errorf("rsync: %w", err)
	}
	return nil
}

// progressWriter consumes rsync --info=progress2 carriage-return records,
// reports monotonic byte deltas, and forwards non-progress output.
type progressWriter struct {
	mu      sync.Mutex
	buf     strings.Builder
	last    uint64
	forward io.Writer
	report  func(uint64)
}

func (w *progressWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, b := range p {
		if b == '\r' || b == '\n' {
			w.consume(w.buf.String(), b)
			w.buf.Reset()
			continue
		}
		w.buf.WriteByte(b)
	}
	return len(p), nil
}

func (w *progressWriter) Flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.buf.Len() > 0 {
		w.consume(w.buf.String(), '\n')
		w.buf.Reset()
	}
}

func (w *progressWriter) consume(line string, delimiter byte) {
	fields := strings.Fields(line)
	if len(fields) >= 2 && strings.HasSuffix(fields[1], "%") {
		n, err := strconv.ParseUint(strings.ReplaceAll(fields[0], ",", ""), 10, 64)
		if err == nil {
			if n > w.last && w.report != nil {
				w.report(n - w.last)
			}
			if n > w.last {
				w.last = n
			}
			return
		}
	}
	if w.forward != nil {
		fmt.Fprint(w.forward, line)
		if delimiter != 0 {
			fmt.Fprintf(w.forward, "%c", delimiter)
		}
	}
}

// withTrailingSlash appends the slash marking the source as the directory
// whose contents --files-from paths are relative to.
func withTrailingSlash(dir string) string {
	if strings.HasSuffix(dir, "/") {
		return dir
	}
	return dir + "/"
}

// MirrorConfig describes the explicit whole-tree archive reconciliation used
// by `catsync sync`. Unlike Config, it intentionally permits destination
// deletion. Callers must run it only after all preceding transfer stages have
// succeeded; --delete-delay defers removals until rsync finishes its transfer
// work.
type MirrorConfig struct {
	Program   string
	Direction target.Direction
	SourceDir string
	Dest      target.Destination
	SSH       backend.SSH
	DryRun    bool
	Env       []string
	Stdout    io.Writer
	Stderr    io.Writer
	Progress  func(uint64)
}

// Command validates and builds a full-tree mirror command.
func (c *MirrorConfig) Command() ([]string, error) {
	base := Config{
		Program: c.Program, Direction: c.Direction, SourceDir: c.SourceDir,
		Dest: c.Dest, SSH: c.SSH, PartialDir: DefaultPartialDir,
	}
	// Reuse endpoint and transport validation without using the resulting
	// file-list command.
	if _, _, err := base.Command(); err != nil {
		return nil, err
	}
	program := c.Program
	if program == "" {
		program = "rsync"
	}
	args := []string{
		program,
		"--archive",
		"--checksum",
		"--hard-links",
		"--acls",
		"--xattrs",
		"--delete-delay",
		"--partial",
		"--partial-dir=" + DefaultPartialDir,
	}
	if c.DryRun {
		args = append(args, "--dry-run", "--itemize-changes")
	}
	if c.Progress != nil {
		args = append(args, "--info=progress2", "--outbuf=L")
	}
	args = append(args, "-e", c.SSH.RsyncTransport(), "--")
	direction := c.Direction
	if direction == "" {
		direction = target.LocalToRemote
	}
	if direction == target.RemoteToLocal {
		args = append(args, remoteWithTrailingSlash(c.Dest), withTrailingSlash(c.SourceDir))
	} else {
		args = append(args, withTrailingSlash(c.SourceDir), c.Dest.String())
	}
	return args, nil
}

// Run executes the full-tree mirror reconciliation.
func (c *MirrorConfig) Run(ctx context.Context) error {
	argv, err := c.Command()
	if err != nil {
		return err
	}
	stdout := c.Stdout
	if c.Progress != nil {
		pw := &progressWriter{forward: c.Stdout, report: c.Progress}
		defer pw.Flush()
		stdout = pw
	}
	if err := process.Run(ctx, process.Spec{
		Name: argv[0], Args: argv[1:], Env: c.Env,
		Stdout: stdout, Stderr: c.Stderr,
	}); err != nil {
		return fmt.Errorf("rsync mirror: %w", err)
	}
	return nil
}
