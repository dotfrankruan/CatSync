package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/dotfrankruan/CatSync/internal/discovery"
	"github.com/dotfrankruan/CatSync/internal/process"
)

func daemonUsage(header string) string {
	return header + `
Options:
  --remote-catsyncd PATH  catsyncd executable on the remote host (default "catsyncd")
  --port N                SSH server port (1-65535; default uses ssh_config)
  --ipv6                  restrict ssh to IPv6
`
}

// runDaemonCommand dispatches the daemon diagnostic commands.
func runDaemonCommand(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, daemonUsage("Usage: catsync daemon <probe> [options] [user@]host"))
		return ExitUsage
	}
	switch args[0] {
	case "probe":
		return runDaemonProbe(args[1:], stdout, stderr)
	case "help", "-h", "--help":
		fmt.Fprint(stdout, daemonUsage("Usage: catsync daemon <probe> [options] [user@]host"))
		return ExitOK
	default:
		fmt.Fprintf(stderr, "catsync: daemon: unknown command %q\n\n", args[0])
		fmt.Fprint(stderr, daemonUsage("Usage: catsync daemon <probe> [options] [user@]host"))
		return ExitUsage
	}
}

// runDaemonProbe runs `catsyncd probe --json` on the remote host over one
// read-only ssh session. It never binds a port, writes remote files, or
// installs anything.
func runDaemonProbe(args []string, stdout, stderr io.Writer) int {
	fs := newFlagSet("daemon probe", daemonUsage(`Usage: catsync daemon probe [options] [user@]host

Runs "catsyncd probe --json" on the remote host over one ssh session and
prints the capability report. It does not start a listener, modify remote
files, or install catsyncd.`), stderr)
	remote := fs.String("remote-catsyncd", "catsyncd", "catsyncd executable on the remote host")
	port := fs.Int("port", 0, "SSH server port (1-65535; default uses ssh_config)")
	ipv6 := fs.Bool("ipv6", false, "restrict ssh to IPv6")
	operands, code, ok := splitArgs(fs, args)
	if !ok {
		return code
	}
	if len(operands) != 1 {
		fmt.Fprintf(stderr, "catsync: daemon probe: want exactly one [user@]host operand, got %d\n", len(operands))
		return ExitUsage
	}
	who := operands[0]
	if err := validateProbeHost(who); err != nil {
		fmt.Fprintf(stderr, "catsync: daemon probe: %v\n", err)
		return ExitUsage
	}
	if *port < 0 || *port > 65535 {
		fmt.Fprintf(stderr, "catsync: daemon probe: --port %d is outside 1..65535\n", *port)
		return ExitUsage
	}
	if strings.TrimSpace(*remote) == "" {
		fmt.Fprintln(stderr, "catsync: daemon probe: --remote-catsyncd must not be empty")
		return ExitUsage
	}
	progs := programsFromEnv()
	sshProg := progs.SSH
	if sshProg == "" {
		sshProg = "ssh"
	}
	argv := []string{sshProg}
	if *ipv6 {
		argv = append(argv, "-6")
	}
	if *port != 0 {
		argv = append(argv, "-p", fmt.Sprintf("%d", *port))
	}
	remoteCmd := discovery.ShellQuote(*remote) + " probe --json"
	argv = append(argv, "-T", who, remoteCmd)

	ctx, stop := signalContext()
	defer stop()
	var out, errBuf strings.Builder
	err := process.Run(ctx, process.Spec{
		Name: argv[0], Args: argv[1:],
		Stdout: &out, Stderr: &errBuf,
	})
	if err != nil {
		detail := strings.TrimSpace(errBuf.String())
		switch {
		case isRemoteCommandMissing(err, detail):
			fmt.Fprintf(stderr, "catsync: daemon probe: catsyncd is not installed or not executable on %s.\n"+
				"Install it yourself from the CatSync release archive (it ships catsync and catsyncd), "+
				"or pass --remote-catsyncd PATH. CatSync never installs it for you.\n", who)
			return ExitError
		}
		if detail != "" {
			fmt.Fprintf(stderr, "catsync: daemon probe: %v (%s)\n", err, firstLine(detail))
		} else {
			fmt.Fprintf(stderr, "catsync: daemon probe: %v\n", err)
		}
		return ExitError
	}
	report := strings.TrimSpace(out.String())
	var parsed map[string]any
	if err := json.Unmarshal([]byte(report), &parsed); err != nil {
		fmt.Fprintf(stderr, "catsync: daemon probe: remote output is not a JSON probe report: %v\n", err)
		return ExitError
	}
	fmt.Fprintln(stdout, report)
	return ExitOK
}

// validateProbeHost accepts [user@]host and rejects path-like operands.
func validateProbeHost(s string) error {
	if s == "" {
		return errors.New("host is empty")
	}
	if strings.ContainsAny(s, "/\x00") {
		return fmt.Errorf("host %q must not contain a path", s)
	}
	if strings.Count(s, "@") > 1 {
		return fmt.Errorf("host %q has more than one '@'", s)
	}
	if i := strings.LastIndexByte(s, '@'); i >= 0 {
		if i == 0 || i == len(s)-1 {
			return fmt.Errorf("host %q is missing a user or host component", s)
		}
	}
	return nil
}

// isRemoteCommandMissing reports whether ssh failed because the remote
// program does not exist.
func isRemoteCommandMissing(err error, stderr string) bool {
	var exitErr *process.ExitError
	if errors.As(err, &exitErr) && exitErr.Code == 127 {
		return true
	}
	for _, marker := range []string{"command not found", "No such file or directory", "not found"} {
		if strings.Contains(stderr, marker) {
			return true
		}
	}
	return false
}
