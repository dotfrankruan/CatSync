package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"
)

// defaultProbeTimeout bounds each external version probe so doctor stays
// responsive even when an installed tool hangs.
const defaultProbeTimeout = 5 * time.Second

// toolSpec describes one external tool doctor looks for.
type toolSpec struct {
	name        string
	required    bool
	versionArgs []string
	purpose     string
}

// doctorTools lists the tools CatSync shells out to, in display order.
// rsync and ssh are required; mscp enables the optional large-file
// backend, and its absence only narrows auto mode. The small-file stage is
// native (parallel rsync) and needs no external tools beyond rsync and ssh.
var doctorTools = []toolSpec{
	{name: "rsync", required: true, versionArgs: []string{"--version"}, purpose: "required transfer backend (conservative and native small-file batches)"},
	{name: "ssh", required: true, versionArgs: []string{"-V"}, purpose: "required remote transport"},
	// mscp has no --version flag; its -h help banner carries the version on
	// the first line and exits zero.
	{name: "mscp", required: false, versionArgs: []string{"-h"}, purpose: "optional parallel large-file backend"},
}

// toolReport records the outcome of probing one tool.
type toolReport struct {
	spec    toolSpec
	found   bool
	path    string
	version string
	output  string
	detail  string
}

// runDoctorCommand implements the doctor command.
func runDoctorCommand(args []string, stdout, stderr io.Writer) int {
	fs := newFlagSet("doctor", `Usage: catsync doctor [options]

Check that the external tools catsync relies on are installed and report
their versions. Missing optional tools (mscp) only narrow what auto mode
can use; missing required tools (rsync, ssh) fail the check.

Options:
`, stderr)
	opts := defaultOptions()
	fs.BoolVar(&opts.Verbose, "verbose", false, "print full version output instead of the first line")
	fs.BoolVar(&opts.Quiet, "quiet", false, "only report problems")
	if code, ok := parseFlags(fs, args); !ok {
		return code
	}
	if opts.Quiet && opts.Verbose {
		fmt.Fprintln(stderr, "catsync: doctor: --quiet and --verbose cannot be used together")
		return ExitUsage
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "catsync: doctor: unexpected operand %q\n", fs.Arg(0))
		fs.Usage()
		return ExitUsage
	}
	return runDoctor(stdout, stderr, opts, defaultProbeTimeout)
}

// probeTool locates one tool and runs its bounded version command. A tool
// that exists but fails or times out its version probe is still reported as
// found, with detail explaining the degraded probe.
func probeTool(spec toolSpec, timeout time.Duration) toolReport {
	rep := toolReport{spec: spec}
	path, err := exec.LookPath(spec.name)
	if err != nil {
		rep.detail = "not found in PATH"
		return rep
	}
	rep.found = true
	rep.path = path

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, path, spec.versionArgs...).CombinedOutput()
	rep.output = string(out)
	rep.version = firstLine(rep.output)
	switch {
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		rep.detail = fmt.Sprintf("version probe timed out after %s", timeout)
	case err != nil:
		rep.detail = fmt.Sprintf("version probe failed: %v", err)
	}
	return rep
}

// runDoctor probes every known tool and prints a per-tool report. It returns
// a nonzero exit code only when a required tool (rsync or ssh) is missing.
func runDoctor(stdout, stderr io.Writer, opts Options, timeout time.Duration) int {
	var missing []string
	for _, spec := range doctorTools {
		rep := probeTool(spec, timeout)
		printToolReport(stdout, rep, opts)
		if rep.spec.required && !rep.found {
			missing = append(missing, rep.spec.name)
		}
	}
	if len(missing) > 0 {
		fmt.Fprintf(stderr, "catsync: doctor: missing required tools: %s\n", strings.Join(missing, ", "))
		return ExitError
	}
	if !opts.Quiet {
		fmt.Fprintln(stdout, "doctor: all required tools are available")
	}
	return ExitOK
}

// printToolReport renders one probe result. Missing tools are always shown
// (they are the point of the report); found tools are hidden by --quiet.
func printToolReport(w io.Writer, rep toolReport, opts Options) {
	if !rep.found {
		kind := "optional"
		if rep.spec.required {
			kind = "REQUIRED"
		}
		fmt.Fprintf(w, "%-7s missing  %s: %s\n", rep.spec.name, kind, rep.spec.purpose)
		return
	}
	if opts.Quiet {
		return
	}
	info := rep.version
	if rep.detail != "" {
		if info != "" {
			info = info + " (" + rep.detail + ")"
		} else {
			info = rep.detail
		}
	}
	if info == "" {
		info = "version unknown"
	}
	fmt.Fprintf(w, "%-7s ok       %s: %s\n", rep.spec.name, rep.path, info)
	if opts.Verbose && rep.output != "" {
		for _, line := range strings.Split(strings.TrimRight(rep.output, "\n"), "\n") {
			fmt.Fprintf(w, "         | %s\n", line)
		}
	}
}

// firstLine returns the first non-empty-looking line of s, trimmed.
func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	return strings.TrimSpace(s)
}
