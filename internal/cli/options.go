package cli

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/dotfrankruan/CatSync/internal/bytesize"
	"github.com/dotfrankruan/CatSync/internal/executor"
)

// Options carries the configuration shared by the transfer commands. Later
// milestones consume it to build backend invocations; this slice parses and
// validates it.
type Options struct {
	// Backend selects the transfer backend: auto, fpsync, mscp, or rsync.
	// "fpsync" and "mscp" are compatibility values for the native
	// parallel-rsync and native multistream backends.
	Backend string
	// DryRun plans and reports without transferring data.
	DryRun bool
	// Fresh forces a new run instead of auto-resuming an interrupted run for
	// the same source and destination.
	Fresh bool
	// Verbose increases output detail.
	Verbose bool
	// Quiet suppresses non-error output.
	Quiet bool
	// Progress controls progress rendering: auto, always, or never.
	Progress string
	// IPv6 restricts backend tools to IPv6.
	IPv6 bool
	// Port selects an explicit SSH server port. Zero uses ssh_config.
	Port int
	// StateDir holds run state used by resume and status.
	StateDir string
	// Verify selects the completion verification mode: auto, none, size,
	// or sha256.
	Verify string
	// Jobs caps parallel transfer jobs.
	Jobs int
	// Streams caps parallel streams per job.
	Streams int
	// OnAuthFailure selects the behavior when ssh authentication is
	// rejected (for example an expired certificate): "fail" stops the run
	// (it stays resumable), "retry" asks the operator to renew credentials
	// and retries the stage.
	OnAuthFailure string
	// LargeThreshold is the size at which a file counts as large.
	LargeThreshold bytesize.Size
	// Transport selects the remote data path: empty (historical SSH on a new
	// run, recorded value on resume), "ssh", or "daemon".
	Transport string
	// DaemonPort is the daemon TCP port (0 = ephemeral).
	DaemonPort int
	// DaemonTransport is "tcp" or the experimental "quic".
	DaemonTransport string
	// DaemonBootstrap is "direct" or "ssh-forward".
	DaemonBootstrap string
	// RemoteCatsyncd is the remote executable name or path.
	RemoteCatsyncd string
	// DaemonListen is the remote --listen value.
	DaemonListen string
	// DaemonAddr overrides the data connection address.
	DaemonAddr string
	// Explicit records which flags the user actually set.
	Explicit map[string]bool
}

// defaultOptions returns the baseline configuration exposed in flag help.
func defaultOptions() Options {
	return Options{
		Backend:         "auto",
		Progress:        "auto",
		StateDir:        defaultStateDir(),
		Verify:          "auto",
		Jobs:            4,
		Streams:         4,
		LargeThreshold:  bytesize.Size(512 * bytesize.MiB),
		OnAuthFailure:   "fail",
		DaemonPort:      43411,
		DaemonTransport: "tcp",
		DaemonBootstrap: "direct",
		RemoteCatsyncd:  "catsyncd",
		Explicit:        map[string]bool{},
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
	fs.StringVar(&o.Backend, "backend", o.Backend,
		"transfer backend: auto, fpsync (native parallel-rsync), mscp (native multistream over system ssh), sftp (native single-file SFTP), or rsync")
	fs.BoolVar(&o.DryRun, "dry-run", o.DryRun, "plan and report without transferring data")
	fs.BoolVar(&o.Fresh, "fresh", o.Fresh, "start a new run instead of resuming an interrupted run for the same source and destination")
	fs.BoolVar(&o.Verbose, "verbose", o.Verbose, "increase output detail")
	fs.BoolVar(&o.Quiet, "quiet", o.Quiet, "suppress non-error output")
	fs.StringVar(&o.Progress, "progress", o.Progress, "progress display: auto, always, or never")
	fs.BoolVar(&o.IPv6, "ipv6", o.IPv6, "restrict backend tools to IPv6")
	fs.IntVar(&o.Port, "port", o.Port, "SSH server port (1-65535; default uses ssh_config)")
	fs.StringVar(&o.StateDir, "state-dir", o.StateDir, "directory holding catsync run state")
	fs.StringVar(&o.Verify, "verify", o.Verify, "completion verification: auto, none, size, or sha256")
	fs.IntVar(&o.Jobs, "jobs", o.Jobs, "maximum parallel transfer jobs")
	fs.IntVar(&o.Streams, "streams", o.Streams, "maximum parallel streams per job (for a single file: parallel SFTP sessions)")
	fs.Var(&o.LargeThreshold, "large-threshold", "size at or above which a file counts as large (e.g. 512MiB)")
	fs.StringVar(&o.OnAuthFailure, "on-auth-failure", o.OnAuthFailure,
		"behavior when ssh authentication is rejected: fail (run stays resumable) or retry (ask to renew credentials, then retry the stage)")
	o.bindDaemonFlags(fs)
}

// bindDaemonFlags registers the daemon-transport options shared by new runs
// and resume.
func (o *Options) bindDaemonFlags(fs *flag.FlagSet) {
	fs.StringVar(&o.Transport, "transport", o.Transport,
		"remote data transport: ssh (default), or daemon (one-shot catsyncd over pinned mTLS)")
	fs.IntVar(&o.DaemonPort, "daemon-port", o.DaemonPort,
		"daemon TCP port (0 selects an ephemeral port; default 43411)")
	fs.StringVar(&o.DaemonTransport, "daemon-transport", o.DaemonTransport,
		"daemon data transport: tcp (default) or quic (experimental)")
	fs.StringVar(&o.DaemonBootstrap, "daemon-bootstrap", o.DaemonBootstrap,
		"how catsync reaches the daemon: direct (default) or ssh-forward")
	fs.StringVar(&o.RemoteCatsyncd, "remote-catsyncd", o.RemoteCatsyncd,
		"catsyncd executable on the remote host (default catsyncd); catsync never installs it")
	fs.StringVar(&o.DaemonListen, "daemon-listen", o.DaemonListen,
		"listen address passed to catsyncd (empty derives from the port and bootstrap mode)")
	fs.StringVar(&o.DaemonAddr, "daemon-addr", o.DaemonAddr,
		"override the daemon data address as host:port")
}

// recordExplicit records which flags the user set, so resume overrides only
// the dimensions that were actually specified.
func (o *Options) recordExplicit(fs *flag.FlagSet) {
	o.Explicit = map[string]bool{}
	fs.Visit(func(f *flag.Flag) { o.Explicit[f.Name] = true })
}

// validateDaemonFlags checks the daemon option values and rejects transports
// this build does not implement yet.
func (o *Options) validateDaemonFlags() error {
	switch o.Transport {
	case "", "ssh", "daemon":
	default:
		return fmt.Errorf("--transport %q is not valid: want ssh or daemon", o.Transport)
	}
	if o.DaemonPort < 0 || o.DaemonPort > 65535 {
		return fmt.Errorf("--daemon-port must be between 0 and 65535, got %d", o.DaemonPort)
	}
	switch o.DaemonTransport {
	case "tcp":
	case "quic":
		return fmt.Errorf("--daemon-transport quic is not implemented yet; use tcp")
	default:
		return fmt.Errorf("--daemon-transport %q is not valid: want tcp or quic", o.DaemonTransport)
	}
	switch o.DaemonBootstrap {
	case "direct":
	case "ssh-forward":
		if o.DaemonPort == 0 && o.Explicit["daemon-port"] {
			return fmt.Errorf("--daemon-bootstrap ssh-forward needs a fixed --daemon-port; port 0 cannot be forwarded")
		}
		if o.DaemonTransport == "quic" {
			return fmt.Errorf("--daemon-bootstrap ssh-forward is incompatible with --daemon-transport quic")
		}
	default:
		return fmt.Errorf("--daemon-bootstrap %q is not valid: want direct or ssh-forward", o.DaemonBootstrap)
	}
	if strings.TrimSpace(o.RemoteCatsyncd) == "" {
		return errors.New("--remote-catsyncd must not be empty")
	}
	return nil
}

// validateOutput checks the options shared by every command.
func (o *Options) validateOutput() error {
	if o.Quiet && o.Verbose {
		return errors.New("--quiet and --verbose cannot be used together")
	}
	switch o.Progress {
	case "auto", "always", "never":
	default:
		return fmt.Errorf("--progress %q is not valid: want auto, always, or never", o.Progress)
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
	case "auto", "fpsync", "mscp", "sftp", "rsync":
	default:
		return fmt.Errorf("--backend %q is not valid: want auto, fpsync, mscp, sftp, or rsync", o.Backend)
	}
	switch o.OnAuthFailure {
	case "fail", "retry":
	default:
		return fmt.Errorf("--on-auth-failure %q is not valid: want fail or retry", o.OnAuthFailure)
	}
	switch o.Verify {
	case "auto", "none", "size", "sha256":
	default:
		return fmt.Errorf("--verify %q is not valid: want auto, none, size, or sha256", o.Verify)
	}
	if o.Jobs < 1 {
		return fmt.Errorf("--jobs must be at least 1, got %d", o.Jobs)
	}
	if o.Streams < 1 {
		return fmt.Errorf("--streams must be at least 1, got %d", o.Streams)
	}
	// Parallelism is the operator's choice: the planned peak is only reported
	// as a warning. These per-dimension bounds just catch typos.
	if o.Jobs > executor.MaxSSHConnections {
		return fmt.Errorf("--jobs must be at most %d, got %d", executor.MaxSSHConnections, o.Jobs)
	}
	if o.Streams > executor.MaxSSHConnections {
		return fmt.Errorf("--streams must be at most %d, got %d", executor.MaxSSHConnections, o.Streams)
	}
	if o.Port < 0 || o.Port > 65535 {
		return fmt.Errorf("--port must be between 1 and 65535, got %d", o.Port)
	}
	if err := o.validateDaemonFlags(); err != nil {
		return err
	}
	return nil
}
