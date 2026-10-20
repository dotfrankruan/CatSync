// Package cli implements the catsync command-line interface: command
// dispatch, shared option parsing and validation, the built-in doctor and
// version commands, and the plan, sync, resume, and status transfer
// commands driven by the executor package.
package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/frankruan/catsync/internal/executor"
	"github.com/frankruan/catsync/internal/target"
)

// Process exit codes returned by Run.
const (
	// ExitOK reports success.
	ExitOK = 0
	// ExitError reports a runtime failure: a failed transfer, an
	// unreachable destination, a missing run, and so on.
	ExitError = 1
	// ExitUsage reports invalid command-line input or configuration.
	ExitUsage = 2
)

const usageText = `Usage: catsync <command> [options]

Commands:
  doctor    check external tools (rsync, ssh, fpsync, fpart, mscp)
  plan      build a transfer plan for SOURCE [user@]host:/absolute/path
  sync      transfer SOURCE to [user@]host:/absolute/path
  resume    resume an interrupted run
  status    show the status of a run
  version   print the catsync version

Run 'catsync <command> -h' for command-specific options.
`

// Run executes the catsync command line described by args (excluding the
// program name) and returns the process exit code. Human-readable output is
// written to stdout and errors to stderr.
func Run(args []string, stdout, stderr io.Writer, version string) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usageText)
		return ExitUsage
	}
	name, rest := args[0], args[1:]
	switch name {
	case "doctor":
		return runDoctorCommand(rest, stdout, stderr)
	case "plan", "sync":
		return runTransferCommand(name, rest, stdout, stderr)
	case "resume", "status":
		return runStateCommand(name, rest, stdout, stderr)
	case "version", "-version", "--version":
		fmt.Fprintf(stdout, "catsync %s\n", version)
		return ExitOK
	case "help", "-h", "--help":
		fmt.Fprint(stdout, usageText)
		return ExitOK
	default:
		fmt.Fprintf(stderr, "catsync: unknown command %q\n\n", name)
		fmt.Fprint(stderr, usageText)
		return ExitUsage
	}
}

// rejectDelete scans raw arguments for any spelling of rsync-style deletion
// and rejects it explicitly. CatSync never deletes destination files, so a
// clear error beats the generic "flag not defined" message. Scanning stops
// at "--", after which tokens are positional operands.
func rejectDelete(args []string) error {
	for _, a := range args {
		if a == "--" {
			return nil
		}
		if a == "-delete" || a == "--delete" ||
			strings.HasPrefix(a, "-delete=") || strings.HasPrefix(a, "--delete=") {
			return errors.New("--delete is not supported: catsync never deletes destination files")
		}
	}
	return nil
}

// newFlagSet builds a FlagSet that prints errors and a header-plus-defaults
// usage block to stderr.
func newFlagSet(name, header string, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprint(stderr, header)
		fs.PrintDefaults()
	}
	return fs
}

// parseFlags parses args, translating flag package outcomes into exit codes.
// The second result reports whether parsing succeeded.
func parseFlags(fs *flag.FlagSet, args []string) (int, bool) {
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return ExitOK, false
		}
		return ExitUsage, false
	}
	return ExitOK, true
}

// splitInterspersed separates flag tokens from positional operands, so
// options may appear before, between, or after operands:
//
//	catsync sync --jobs 8 SRC DEST
//	catsync sync SRC DEST --jobs 8
//	catsync sync SRC --jobs 8 DEST
//
// A token after "--" is always an operand. A flag that takes a value
// consumes the following token verbatim, even when it starts with a dash
// (so --jobs -1 reaches the flag parser's range check). Unknown flags are
// passed through to the flag parser so it produces the standard usage
// error. The boolean/lookup logic uses only the flag package's public API.
func splitInterspersed(fs *flag.FlagSet, args []string) (flagTokens, operands []string) {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			operands = append(operands, args[i+1:]...)
			return flagTokens, operands
		}
		if len(a) < 2 || a[0] != '-' || a == "-" {
			operands = append(operands, a)
			continue
		}
		flagTokens = append(flagTokens, a)
		name := strings.TrimLeft(a, "-")
		if name == "" || strings.ContainsRune(name, '=') {
			// "-" or "--name=value": the value is inline or the parser
			// will report the malformed flag.
			continue
		}
		f := fs.Lookup(name)
		if f == nil {
			// Unknown: let the parser report it.
			continue
		}
		if bf, ok := f.Value.(interface{ IsBoolFlag() bool }); ok && bf.IsBoolFlag() {
			continue
		}
		if i+1 < len(args) {
			i++
			flagTokens = append(flagTokens, args[i])
		}
	}
	return flagTokens, operands
}

// splitArgs is shorthand for splitInterspersed: it parses flag tokens into
// fs and returns the operands in order. The second result reports whether
// parsing succeeded (false, with an exit code, for help or errors).
func splitArgs(fs *flag.FlagSet, args []string) (operands []string, code int, ok bool) {
	flagTokens, operands := splitInterspersed(fs, args)
	if code, ok := parseFlags(fs, flagTokens); !ok {
		return nil, code, false
	}
	return operands, ExitOK, true
}

// runTransferCommand implements plan and sync. Both commands share the full
// option set and operand validation; plan stops after the transfer plan
// while sync executes it.
func runTransferCommand(name string, args []string, stdout, stderr io.Writer) int {
	if err := rejectDelete(args); err != nil {
		fmt.Fprintf(stderr, "catsync: %s: %v\n", name, err)
		return ExitUsage
	}
	fs := newFlagSet(name, transferUsage(name), stderr)
	opts := defaultOptions()
	opts.bindSharedFlags(fs)
	operands, code, ok := splitArgs(fs, args)
	if !ok {
		return code
	}
	if err := opts.Validate(); err != nil {
		fmt.Fprintf(stderr, "catsync: %s: %v\n", name, err)
		return ExitUsage
	}
	if len(operands) != 2 {
		fmt.Fprintf(stderr, "catsync: %s: want SOURCE and [user@]host:/absolute/path, got %d operands\n", name, len(operands))
		fs.Usage()
		return ExitUsage
	}
	src, err := target.ValidateSource(operands[0])
	if err != nil {
		fmt.Fprintf(stderr, "catsync: %s: %v\n", name, err)
		return ExitUsage
	}
	dst, err := target.ParseDestination(operands[1])
	if err != nil {
		fmt.Fprintf(stderr, "catsync: %s: %v\n", name, err)
		return ExitUsage
	}
	cfg := opts.executorConfig(src, dst, stdout, stderr)
	ctx, stop := signalContext()
	defer stop()
	if name == "plan" {
		return runPlan(ctx, cfg, &opts, stdout, stderr)
	}
	return runSync(ctx, cfg, &opts, stdout, stderr)
}

// runStateCommand implements resume and status. Both operate on recorded
// run state rather than new transfer operands, so they accept a reduced
// option set and at most one optional run identifier.
func runStateCommand(name string, args []string, stdout, stderr io.Writer) int {
	if err := rejectDelete(args); err != nil {
		fmt.Fprintf(stderr, "catsync: %s: %v\n", name, err)
		return ExitUsage
	}
	fs := newFlagSet(name, stateUsage(name), stderr)
	opts := defaultOptions()
	fs.StringVar(&opts.StateDir, "state-dir", opts.StateDir, "directory holding catsync run state")
	fs.BoolVar(&opts.Verbose, "verbose", false, "increase output detail")
	fs.BoolVar(&opts.Quiet, "quiet", false, "suppress non-error output")
	operands, code, ok := splitArgs(fs, args)
	if !ok {
		return code
	}
	if err := opts.validateOutput(); err != nil {
		fmt.Fprintf(stderr, "catsync: %s: %v\n", name, err)
		return ExitUsage
	}
	if len(operands) > 1 {
		fmt.Fprintf(stderr, "catsync: %s: want at most one run identifier, got %d operands\n", name, len(operands))
		fs.Usage()
		return ExitUsage
	}
	id := ""
	if len(operands) == 1 {
		id = operands[0]
	}
	if name == "resume" {
		ctx, stop := signalContext()
		defer stop()
		return runResume(ctx, &opts, id, stdout, stderr)
	}
	return runStatus(&opts, id, stdout, stderr)
}

// exitForError maps an executor error to a process exit code: invalid
// configuration is a usage error; everything else is a runtime failure.
func exitForError(err error) int {
	if errors.Is(err, executor.ErrInvalidConfig) || errors.Is(err, executor.ErrVerifyNotImplemented) {
		return ExitUsage
	}
	return ExitError
}

func transferUsage(name string) string {
	return fmt.Sprintf(`Usage: catsync %s [options] SOURCE [user@]host:/absolute/path

SOURCE must be an existing, readable local directory. The destination must
name an absolute path on a remote host; bracketed IPv6 literals such as
user@[2001:db8::1]:/data are accepted. CatSync never deletes destination
files; --delete is rejected.

Options may appear before, between, or after the operands: for example
"catsync %s --jobs 8 SRC DEST" and "catsync %s SRC DEST --jobs 8" are
equivalent. Use "--" to end option parsing.

Options:
`, name, name, name)
}

func stateUsage(name string) string {
	return fmt.Sprintf(`Usage: catsync %s [options] [run-id]

Without a run identifier the latest resumable run in the state directory is
used. Options may appear before, between, or after the run identifier, for
example "catsync %s --state-dir DIR RUN_ID" and
"catsync %s RUN_ID --state-dir DIR" are equivalent.

Options:
`, name, name, name)
}
