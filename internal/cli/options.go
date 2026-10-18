package cli

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/frankruan/catsync/internal/bytesize"
)

// Options carries the configuration shared by the transfer commands. Later
// milestones consume it to build backend invocations; this slice parses and
// validates it.
type Options struct {
	// Backend selects the transfer backend: auto, fpsync, mscp, or rsync.
	Backend string
	// DryRun plans and reports without transferring data.
	DryRun bool
	// Verbose increases output detail.
	Verbose bool
	// Quiet suppresses non-error output.
	Quiet bool
	// IPv6 restricts backend tools to IPv6.
	IPv6 bool
	// StateDir holds run state used by resume and status.
	StateDir string
	// Verify selects the completion verification mode: none, size, or
	// sha256. sha256 is accepted now and may fail as unimplemented later.
	Verify string
	// Jobs caps parallel transfer jobs.
	Jobs int
	// Streams caps parallel streams per job.
	Streams int
	// LargeThreshold is the size at which a file counts as large.
	LargeThreshold bytesize.Size
}

// defaultOptions returns the baseline configuration exposed in flag help.
func defaultOptions() Options {
	return Options{
		Backend:        "auto",
		StateDir:       defaultStateDir(),
		Verify:         "size",
		Jobs:           4,
		Streams:        4,
		LargeThreshold: bytesize.Size(512 * bytesize.MiB),
	}
}

// defaultStateDir follows the XDG base directory convention, falling back to
// the user's home directory and finally to a directory in the working tree.
func defaultStateDir() string {
	if xdg := os.Getenv("XDG_STATE_HOME"); xdg != "" {
		return filepath.Join(xdg, "catsync")
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		return filepath.Join(home, ".local", "state", "catsync")
	}
	return ".catsync-state"
}

// bindSharedFlags registers the full shared option set on fs. The defaults
// are the receiver's current values, so callers must start from
// defaultOptions for help text to render correct defaults.
func (o *Options) bindSharedFlags(fs *flag.FlagSet) {
	fs.StringVar(&o.Backend, "backend", o.Backend, "transfer backend: auto, fpsync, mscp, or rsync")
	fs.BoolVar(&o.DryRun, "dry-run", o.DryRun, "plan and report without transferring data")
	fs.BoolVar(&o.Verbose, "verbose", o.Verbose, "increase output detail")
	fs.BoolVar(&o.Quiet, "quiet", o.Quiet, "suppress non-error output")
	fs.BoolVar(&o.IPv6, "ipv6", o.IPv6, "restrict backend tools to IPv6")
	fs.StringVar(&o.StateDir, "state-dir", o.StateDir, "directory holding catsync run state")
	fs.StringVar(&o.Verify, "verify", o.Verify, "completion verification: none, size, or sha256")
	fs.IntVar(&o.Jobs, "jobs", o.Jobs, "maximum parallel transfer jobs")
	fs.IntVar(&o.Streams, "streams", o.Streams, "maximum parallel streams per job")
	fs.Var(&o.LargeThreshold, "large-threshold", "size at which a file counts as large (e.g. 512MiB)")
}

// validateOutput checks the options shared by every command.
func (o *Options) validateOutput() error {
	if o.Quiet && o.Verbose {
		return errors.New("--quiet and --verbose cannot be used together")
	}
	if strings.TrimSpace(o.StateDir) == "" {
		return errors.New("--state-dir must not be empty")
	}
	return nil
}

// Validate checks the full shared option set.
func (o *Options) Validate() error {
	if err := o.validateOutput(); err != nil {
		return err
	}
	switch o.Backend {
	case "auto", "fpsync", "mscp", "rsync":
	default:
		return fmt.Errorf("--backend %q is not valid: want auto, fpsync, mscp, or rsync", o.Backend)
	}
	switch o.Verify {
	case "none", "size", "sha256":
	default:
		return fmt.Errorf("--verify %q is not valid: want none, size, or sha256", o.Verify)
	}
	if o.Jobs < 1 {
		return fmt.Errorf("--jobs must be at least 1, got %d", o.Jobs)
	}
	if o.Streams < 1 {
		return fmt.Errorf("--streams must be at least 1, got %d", o.Streams)
	}
	return nil
}
