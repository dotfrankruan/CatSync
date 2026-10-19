// Package fpsync builds and runs CatSync's parallel small-file stage by
// driving fpsync (which partitions the tree with fpart and syncs the
// partitions with rsync).
//
// The command shape is fixed by policy:
//
//	fpsync -v -n <jobs> -f <files> -s <bytes> -O -S [-t <state-dir>] \
//	       <absolute-source>/ <[user@]host:path/>
//
// "-O -S" replaces fpart's default options with fpart's own -S, which makes
// fpart skip files larger than the -s partition limit and report them on
// standard output as pseudo-partition S records ("S\t<size>\t./<path>").
// This is never fpsync's own -S, which would enable sudo and is never
// emitted. Directory mode (-E) is never emitted either, because it enables
// rsync's --delete. Synchronization runs with local jobs only (no -w SSH
// workers, so no -d shared directory is needed; the shared directory
// defaults to the temp directory).
//
// fpsync prints its run ID ("Info: Run ID: <epoch>-<pid>") at verbose level
// 1, so -v is always passed and the run's standard output is preserved in a
// capture file: the run ID and the recorded output are what resume needs.
// Resume re-runs with the same state directory and "-r <runid>"; the
// synchronization options are ignored by fpsync when resuming.
//
// The capture file (not a pipe) receives standard output, so cancelling a
// run can never leave Run blocked behind orphaned grandchildren that
// inherited the descriptor.
package fpsync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/frankruan/catsync/internal/backend"
	"github.com/frankruan/catsync/internal/discovery"
	"github.com/frankruan/catsync/internal/process"
	"github.com/frankruan/catsync/internal/target"
)

// Config describes one fpsync small-file stage.
type Config struct {
	// Program is the fpsync executable; empty means "fpsync" (PATH
	// lookup).
	Program string
	// SourceDir is the absolute local source directory; a trailing slash
	// is added when missing, as fpsync's src_dir/ syntax requires.
	SourceDir string
	// Dest is the validated remote destination.
	Dest target.Destination
	// Jobs is the number of concurrent sync jobs (-n); at least 1.
	Jobs int
	// FilesPerPartition caps files per partition (-f); at least 1.
	FilesPerPartition int
	// Threshold is the partition size limit in bytes (-s); at least 1.
	// fpart's -S skip-big only works with a size limit, so zero (meaning
	// unlimited) is rejected.
	Threshold uint64
	// StateDir is fpsync's temp/state directory (-t); empty uses fpsync's
	// default (/tmp/fpsync). When set it must be absolute. Resume requires
	// the same StateDir as the original run.
	StateDir string
	// Env lists environment additions applied on top of the inherited
	// environment (see process.Spec.Env); nil inherits it unchanged.
	Env []string
	// Stderr receives fpsync's standard error; nil discards it. Standard
	// output is always captured to a file (see Result.OutputPath).
	Stderr io.Writer
}

// Command validates the configuration and returns the initial-run argument
// vector (including the program name at index 0).
func (c *Config) Command() ([]string, error) {
	program, err := c.program()
	if err != nil {
		return nil, err
	}
	src, dst, err := c.operands()
	if err != nil {
		return nil, err
	}
	if c.Jobs < 1 {
		return nil, fmt.Errorf("fpsync: jobs must be at least 1, got %d", c.Jobs)
	}
	if c.FilesPerPartition < 1 {
		return nil, fmt.Errorf("fpsync: files per partition must be at least 1, got %d", c.FilesPerPartition)
	}
	if c.Threshold < 1 {
		return nil, errors.New("fpsync: size threshold must be at least 1 byte: fpart skip-big (-O -S) requires a size limit")
	}
	if err := c.stateDir(); err != nil {
		return nil, err
	}

	args := []string{
		program,
		"-v", // run ID and progress on stdout, needed for resume
		"-n", strconv.Itoa(c.Jobs),
		"-f", strconv.Itoa(c.FilesPerPartition),
		"-s", strconv.FormatUint(c.Threshold, 10),
		// "-O -S": fpart (not fpsync) receives -S and skips files larger
		// than the -s limit. Never fpsync's own -S (sudo) and never -E
		// (rsync --delete).
		"-O", "-S",
	}
	if c.StateDir != "" {
		args = append(args, "-t", c.StateDir)
	}
	return append(args, src, dst), nil
}

// ResumeCommand validates the configuration and returns the argument vector
// that resumes runID: "fpsync -v [-t <state-dir>] -r <runid>". fpsync
// ignores the synchronization options and operands when resuming, so they
// are not emitted; the state directory still selects where the run lives.
func (c *Config) ResumeCommand(runID string) ([]string, error) {
	program, err := c.program()
	if err != nil {
		return nil, err
	}
	if err := c.stateDir(); err != nil {
		return nil, err
	}
	if err := ValidateRunID(runID); err != nil {
		return nil, err
	}
	args := []string{program, "-v"}
	if c.StateDir != "" {
		args = append(args, "-t", c.StateDir)
	}
	return append(args, "-r", runID), nil
}

// runIDPattern is fpsync's own run-id grammar (is_runid): <epoch>-<pid>.
var runIDPattern = regexp.MustCompile(`^[0-9]+-[0-9]+$`)

// ValidateRunID rejects run IDs fpsync would refuse.
func ValidateRunID(id string) error {
	if !runIDPattern.MatchString(id) {
		return fmt.Errorf("fpsync: invalid run id %q: want <epoch>-<pid>", id)
	}
	return nil
}

// Skipped is one fpart pseudo-partition S record: a file larger than the
// partition size limit that fpsync did not transfer.
type Skipped struct {
	// Path is the slash-separated path relative to the source root, with
	// fpart's leading "./" stripped.
	Path string
	// Size is the file size in bytes.
	Size int64
}

// Result reports a finished fpsync invocation.
type Result struct {
	// RunID is the fpsync run identifier needed for resume. On Resume it
	// echoes the requested run ID even when the output cannot be parsed.
	RunID string
	// Skipped lists the files fpart skipped as too large (S records), in
	// output order. A path containing a newline cannot round-trip through
	// fpsync's line-based output and is not represented.
	Skipped []Skipped
	// OutputPath is the file holding the run's preserved standard output.
	// It lives in StateDir when configured, otherwise in the system temp
	// directory, and is never removed by this package.
	OutputPath string
}

// Run executes the initial transfer and parses the preserved output. On
// failure the partially parsed Result is still returned alongside the
// error, so the run ID remains available for resume.
func (c *Config) Run(ctx context.Context) (*Result, error) {
	argv, err := c.Command()
	if err != nil {
		return nil, err
	}
	return c.run(ctx, argv, "")
}

// Resume re-executes a previous run by ID. The caller must supply the same
// StateDir as the original run; fpsync re-reads the run's partitions from
// it. The source tree is not rescanned by fpsync when resuming.
func (c *Config) Resume(ctx context.Context, runID string) (*Result, error) {
	argv, err := c.ResumeCommand(runID)
	if err != nil {
		return nil, err
	}
	return c.run(ctx, argv, runID)
}

// run executes argv, capturing standard output to a file, and parses the
// result. knownID is the already-known run ID on resume.
func (c *Config) run(ctx context.Context, argv []string, knownID string) (*Result, error) {
	if c.StateDir != "" {
		// The capture file lives beside fpsync's own state; fpsync
		// mkdirs the directory itself only later.
		if err := os.MkdirAll(c.StateDir, 0700); err != nil {
			return nil, fmt.Errorf("fpsync: create state dir: %w", err)
		}
	}
	out, err := os.CreateTemp(c.StateDir, "catsync-fpsync-*.log")
	if err != nil {
		return nil, fmt.Errorf("fpsync: create output capture file: %w", err)
	}
	res := &Result{RunID: knownID, OutputPath: out.Name()}

	runErr := process.Run(ctx, process.Spec{
		Name:   argv[0],
		Args:   argv[1:],
		Env:    c.Env,
		Stdout: out,
		Stderr: c.Stderr,
	})
	if cerr := out.Close(); cerr != nil && runErr == nil {
		runErr = fmt.Errorf("fpsync: close output capture: %w", cerr)
	}

	data, err := os.ReadFile(res.OutputPath)
	if err != nil {
		return res, fmt.Errorf("fpsync: read captured output: %w", err)
	}
	parsedID, skipped := ParseOutput(string(data))
	if parsedID != "" {
		res.RunID = parsedID
	}
	res.Skipped = skipped

	if runErr != nil {
		return res, fmt.Errorf("fpsync: %w", runErr)
	}
	if res.RunID == "" {
		return res, errors.New("fpsync: run id not found in output; cannot resume this run later")
	}
	return res, nil
}

// runIDLine matches fpsync's verbose run-id announcement, for example
// "1735689600 Info: Run ID: 1735689600-12345" or "... (resumed)".
var runIDLine = regexp.MustCompile(`Info: Run ID: ([0-9]+-[0-9]+)`)

// ParseOutput extracts the run ID and the fpart skipped-file S records from
// preserved fpsync standard output. Lines that are not run-id announcements
// or well-formed S records (verbose log lines, fpart warnings, S records
// with unsafe paths) are ignored.
func ParseOutput(output string) (runID string, skipped []Skipped) {
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSuffix(line, "\r")
		if runID == "" {
			if m := runIDLine.FindStringSubmatch(line); m != nil {
				runID = m[1]
			}
		}
		if s, ok := parseSkippedLine(line); ok {
			skipped = append(skipped, s)
		}
	}
	return runID, skipped
}

// parseSkippedLine parses one S record of the form "S\t<size>\t<path>".
// Everything after the second tab is the path verbatim, so paths may
// contain spaces, tabs, and Unicode. fpart reports paths relative to the
// crawl root with a "./" prefix, which is stripped; the result must still
// be a safe relative path.
func parseSkippedLine(line string) (Skipped, bool) {
	if !strings.HasPrefix(line, "S\t") {
		return Skipped{}, false
	}
	rest := line[len("S\t"):]
	tab := strings.IndexByte(rest, '\t')
	if tab < 0 {
		return Skipped{}, false
	}
	size, err := strconv.ParseInt(rest[:tab], 10, 64)
	if err != nil || size < 0 {
		return Skipped{}, false
	}
	path := strings.TrimPrefix(rest[tab+1:], "./")
	if err := discovery.ValidateRelPath(path); err != nil {
		return Skipped{}, false
	}
	return Skipped{Path: path, Size: size}, true
}

// program resolves the executable name.
func (c *Config) program() (string, error) {
	program := c.Program
	if program == "" {
		program = "fpsync"
	}
	if strings.ContainsRune(program, 0) {
		return "", fmt.Errorf("fpsync: program name contains a NUL byte")
	}
	return program, nil
}

// stateDir validates the optional state directory: fpsync requires -t to be
// absolute.
func (c *Config) stateDir() error {
	if c.StateDir == "" {
		return nil
	}
	if strings.ContainsRune(c.StateDir, 0) {
		return fmt.Errorf("fpsync: state dir contains a NUL byte")
	}
	if !filepath.IsAbs(c.StateDir) {
		return fmt.Errorf("fpsync: state dir %q is not absolute", c.StateDir)
	}
	return nil
}

// operands validates and renders the source and destination operands, both
// with the trailing slash fpsync's src_dir/ dst_url/ syntax uses. The
// source must be absolute; both operands then necessarily begin with a
// non-dash character, so option parsing cannot mistake them for flags.
func (c *Config) operands() (string, string, error) {
	if c.SourceDir == "" {
		return "", "", errors.New("fpsync: source directory is empty")
	}
	if strings.ContainsRune(c.SourceDir, 0) {
		return "", "", fmt.Errorf("fpsync: source directory %q contains a NUL byte", c.SourceDir)
	}
	if !filepath.IsAbs(c.SourceDir) {
		return "", "", fmt.Errorf("fpsync: source directory %q is not absolute", c.SourceDir)
	}
	if err := backend.ValidateDestination(c.Dest); err != nil {
		return "", "", err
	}
	src := c.SourceDir
	if !strings.HasSuffix(src, "/") {
		src += "/"
	}
	dst := c.Dest.String()
	if !strings.HasSuffix(dst, "/") {
		dst += "/"
	}
	return src, dst, nil
}
