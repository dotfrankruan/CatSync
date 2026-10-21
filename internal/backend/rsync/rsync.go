// Package rsync builds and runs CatSync's conservative transfer stage: a
// disjoint set of relative file paths streamed to rsync over a
// NUL-delimited --files-from list, with archive semantics, partial-transfer
// retention in a relative --partial-dir, and never any deletion.
//
// The command shape is fixed by policy:
//
//	rsync --archive --partial --partial-dir=<relative> --relative \
//	      --files-from=- --from0 [--dry-run] -e <ssh transport> \
//	      -- <absolute-source>/ <[user@]host:path>
//
// --whole-file, --inplace, and --delete are never emitted. File names travel
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
	"strings"

	"github.com/frankruan/catsync/internal/backend"
	"github.com/frankruan/catsync/internal/discovery"
	"github.com/frankruan/catsync/internal/process"
	"github.com/frankruan/catsync/internal/target"
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
	// SourceDir is the absolute local source root the relative paths are
	// based on.
	SourceDir string
	// Dest is the validated remote destination.
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
	// Env lists environment additions applied on top of the inherited
	// environment (see process.Spec.Env); nil inherits it unchanged.
	Env []string
	// Stdout and Stderr receive rsync's output; nil discards it.
	Stdout io.Writer
	Stderr io.Writer
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
	if c.DryRun {
		args = append(args, "--dry-run")
	}
	args = append(args, "-e", c.SSH.RsyncTransport())
	// Both operands are proven never to begin with '-' (the source is
	// absolute, the destination was validated); "--" ends option parsing
	// for defense in depth.
	args = append(args, "--", withTrailingSlash(c.SourceDir), c.Dest.String())
	return args, payload.Bytes(), nil
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
	if err := process.Run(ctx, process.Spec{
		Name:   argv[0],
		Args:   argv[1:],
		Env:    c.Env,
		Stdin:  bytes.NewReader(stdin),
		Stdout: c.Stdout,
		Stderr: c.Stderr,
	}); err != nil {
		return fmt.Errorf("rsync: %w", err)
	}
	return nil
}

// withTrailingSlash appends the slash marking the source as the directory
// whose contents --files-from paths are relative to.
func withTrailingSlash(dir string) string {
	if strings.HasSuffix(dir, "/") {
		return dir
	}
	return dir + "/"
}
