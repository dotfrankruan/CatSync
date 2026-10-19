// Package mscp builds and runs CatSync's parallel large-file stage: exactly
// one confirmed-new large file per invocation, executed from the original
// source working directory so checkpoint resume reproduces the run.
//
// The initial command shape is fixed by policy:
//
//	mscp -n <connections> -a <inflight> -s <min-chunk> [-L <bitrate>] \
//	     [-6] -W <checkpoint> -- <relative-source> <[user@]host:remote-path>
//
// Resume repeats the SSH options and runs from the same working directory,
// loading source, target, and remaining chunks from the checkpoint:
//
//	mscp [-6] -R <checkpoint>
//
// mscp never deletes the checkpoint after a successful resume; this package
// never removes it either — retention until CatSync's own verification gate
// passes is the policy. The checkpoint records the remote (including the
// user), so resume needs no source or target operands.
//
// mscp follows scp semantics and does not create missing remote parent
// directories; ensuring the destination directory tree exists is the
// orchestrator's responsibility.
package mscp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/frankruan/catsync/internal/backend"
	"github.com/frankruan/catsync/internal/discovery"
	"github.com/frankruan/catsync/internal/process"
	"github.com/frankruan/catsync/internal/target"
)

// Config describes one mscp transfer of a single confirmed-new large file.
type Config struct {
	// Program is the mscp executable; empty means "mscp" (PATH lookup).
	Program string
	// WorkDir is the absolute original source working directory. Both the
	// initial run and resume execute here: checkpoint paths are relative
	// to it.
	WorkDir string
	// Path is the slash-separated path of the one file to transfer,
	// relative to WorkDir. A leading dash is made safe by a "./" prefix.
	Path string
	// Dest is the validated remote destination; the file lands at
	// Dest.Path joined with Path.
	Dest target.Destination
	// Connections is the number of parallel SSH connections (-n); at
	// least 1.
	Connections int
	// Inflight is the number of inflight SFTP requests (-a); at least 1.
	Inflight int
	// MinChunk is the minimum chunk size in bytes (-s); at least 1.
	MinChunk uint64
	// Bitrate optionally caps the transfer bitrate in bits per second
	// (-L); zero omits the limit.
	Bitrate uint64
	// SSH carries SSH transport options; they are repeated on resume.
	SSH backend.SSH
	// Checkpoint is the checkpoint file path: -W on the initial run, -R
	// on resume. It must not be empty; resume depends on it. A leading
	// dash is made safe by a "./" prefix.
	Checkpoint string
	// Env lists environment additions applied on top of the inherited
	// environment (see process.Spec.Env); nil inherits it unchanged.
	Env []string
	// Stdout and Stderr receive mscp's output; nil discards it.
	Stdout io.Writer
	Stderr io.Writer
}

// Command validates the configuration and returns the initial-run argument
// vector (including the program name at index 0).
func (c *Config) Command() ([]string, error) {
	program, err := c.program()
	if err != nil {
		return nil, err
	}
	if err := c.workDir(); err != nil {
		return nil, err
	}
	src, err := c.sourceOperand()
	if err != nil {
		return nil, err
	}
	if err := backend.ValidateDestination(c.Dest); err != nil {
		return nil, err
	}
	if c.Connections < 1 {
		return nil, fmt.Errorf("mscp: connections must be at least 1, got %d", c.Connections)
	}
	if c.Inflight < 1 {
		return nil, fmt.Errorf("mscp: inflight requests must be at least 1, got %d", c.Inflight)
	}
	if c.MinChunk < 1 {
		return nil, errors.New("mscp: minimum chunk size must be at least 1 byte")
	}
	checkpoint, err := c.checkpointOperand()
	if err != nil {
		return nil, err
	}

	args := []string{
		program,
		"-n", strconv.Itoa(c.Connections),
		"-a", strconv.Itoa(c.Inflight),
		"-s", strconv.FormatUint(c.MinChunk, 10),
	}
	if c.Bitrate > 0 {
		args = append(args, "-L", strconv.FormatUint(c.Bitrate, 10))
	}
	args = append(args, c.SSH.MSCPArgs()...)
	args = append(args, "-W", checkpoint)
	// "--" ends option parsing for defense in depth; both operands are
	// already proven safe (the source is dash-prefixed, the destination
	// was validated).
	dst := target.Destination{User: c.Dest.User, Host: c.Dest.Host, Path: backend.RemotePath(c.Dest.Path, c.Path)}
	return append(args, "--", src, dst.String()), nil
}

// ResumeCommand validates the resume-relevant configuration and returns
// "mscp [<ssh options>] -R <checkpoint>" — no source or target operands, as
// the checkpoint supplies them. The working directory and SSH options match
// the failed run.
func (c *Config) ResumeCommand() ([]string, error) {
	program, err := c.program()
	if err != nil {
		return nil, err
	}
	if err := c.workDir(); err != nil {
		return nil, err
	}
	checkpoint, err := c.checkpointOperand()
	if err != nil {
		return nil, err
	}
	args := []string{program}
	args = append(args, c.SSH.MSCPArgs()...)
	return append(args, "-R", checkpoint), nil
}

// Run executes the initial transfer from WorkDir and waits for completion.
// A non-zero mscp exit is returned as *process.ExitError; context
// cancellation terminates and reaps mscp and returns an error wrapping the
// context's cause. The checkpoint file is left untouched either way.
func (c *Config) Run(ctx context.Context) error {
	argv, err := c.Command()
	if err != nil {
		return err
	}
	return c.run(ctx, argv)
}

// Resume continues a failed run from its checkpoint, from the same working
// directory and with the same SSH options as the failed run. The checkpoint
// file is preserved: mscp does not remove it after success, and neither
// does this package — the verification gate owns its retirement.
func (c *Config) Resume(ctx context.Context) error {
	argv, err := c.ResumeCommand()
	if err != nil {
		return err
	}
	return c.run(ctx, argv)
}

func (c *Config) run(ctx context.Context, argv []string) error {
	if err := process.Run(ctx, process.Spec{
		Name:   argv[0],
		Args:   argv[1:],
		Dir:    c.WorkDir,
		Env:    c.Env,
		Stdout: c.Stdout,
		Stderr: c.Stderr,
	}); err != nil {
		return fmt.Errorf("mscp: %w", err)
	}
	return nil
}

// program resolves the executable name.
func (c *Config) program() (string, error) {
	program := c.Program
	if program == "" {
		program = "mscp"
	}
	if strings.ContainsRune(program, 0) {
		return "", fmt.Errorf("mscp: program name contains a NUL byte")
	}
	return program, nil
}

// workDir validates the working directory both run modes share.
func (c *Config) workDir() error {
	if c.WorkDir == "" {
		return errors.New("mscp: working directory is empty")
	}
	if strings.ContainsRune(c.WorkDir, 0) {
		return fmt.Errorf("mscp: working directory %q contains a NUL byte", c.WorkDir)
	}
	if !filepath.IsAbs(c.WorkDir) {
		return fmt.Errorf("mscp: working directory %q is not absolute", c.WorkDir)
	}
	return nil
}

// sourceOperand validates Path and renders the source operand. A path
// beginning with '-' is prefixed with "./" so the operand names the same
// file from the same working directory while being unparseable as options.
func (c *Config) sourceOperand() (string, error) {
	if err := discovery.ValidateRelPath(c.Path); err != nil {
		return "", fmt.Errorf("mscp: file path: %w", err)
	}
	return dashSafeOperand(c.Path), nil
}

// checkpointOperand validates Checkpoint and renders the -W/-R operand.
func (c *Config) checkpointOperand() (string, error) {
	if c.Checkpoint == "" {
		return "", errors.New("mscp: checkpoint path is empty")
	}
	if strings.ContainsRune(c.Checkpoint, 0) {
		return "", fmt.Errorf("mscp: checkpoint path %q contains a NUL byte", c.Checkpoint)
	}
	return dashSafeOperand(c.Checkpoint), nil
}

// dashSafeOperand renders a file operand that a getopt-style parser cannot
// mistake for flags: paths beginning with '-' gain a "./" prefix.
func dashSafeOperand(path string) string {
	if strings.HasPrefix(path, "-") {
		return "./" + path
	}
	return path
}
