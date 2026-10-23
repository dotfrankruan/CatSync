package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/dotfrankruan/CatSync/internal/executor"
	"github.com/dotfrankruan/CatSync/internal/planner"
	"github.com/dotfrankruan/CatSync/internal/state"
	"github.com/dotfrankruan/CatSync/internal/target"
)

// executorConfig maps the shared options and operands to an executor
// configuration. Program overrides come from the CATSYNC_* environment
// (diagnostics and tests); they are never printed. With --quiet the
// executor's progress output is discarded; errors always reach stderr.
func (o *Options) executorConfig(transfer target.Transfer, stdout, stderr io.Writer) executor.Config {
	execOut := stdout
	if o.Quiet {
		execOut = nil
	}
	return executor.Config{
		Direction:      transfer.Direction,
		SourceDir:      transfer.LocalPath,
		Dest:           transfer.Remote,
		Backend:        state.Backend(o.Backend),
		DryRun:         o.DryRun,
		IPv6:           o.IPv6,
		Verify:         o.Verify,
		Jobs:           o.Jobs,
		Streams:        o.Streams,
		LargeThreshold: uint64(o.LargeThreshold),
		StateDir:       o.StateDir,
		Programs:       programsFromEnv(),
		Stdout:         execOut,
		Stderr:         stderr,
	}
}

// programsFromEnv reads the CATSYNC_<TOOL> program overrides for the
// programs CatSync itself starts (ssh and rsync). They exist for
// diagnostics and tests; values are paths or program names and are never
// printed. There is no mscp override: the large-file backend is the native
// multistream implementation over system ssh.
func programsFromEnv() executor.Programs {
	return executor.Programs{
		SSH:   os.Getenv("CATSYNC_SSH"),
		Rsync: os.Getenv("CATSYNC_RSYNC"),
	}
}

// runPlan executes the plan command: discover, collect the remote manifest
// (with the conservative fallback), plan, and print a summary. Nothing is
// transferred and no run state is written.
func runPlan(ctx context.Context, cfg executor.Config, opts *Options, stdout, stderr io.Writer) int {
	out, err := executor.Plan(ctx, cfg)
	if err != nil {
		fmt.Fprintf(stderr, "catsync: plan: %v\n", err)
		return exitForError(err)
	}
	printPlan(stdout, out, opts)
	return ExitOK
}

// printPlan renders the plan summary.
func printPlan(w io.Writer, out *executor.PlanOutcome, opts *Options) {
	counts := out.Result.Counts()
	byBackend := map[state.Backend]int{}
	var unsupported []string
	for _, d := range out.Result.Decisions {
		if d.Status == planner.StatusPending {
			byBackend[d.Backend]++
		}
		if d.Status == planner.StatusUnsupported {
			unsupported = append(unsupported, d.Path)
		}
	}
	fmt.Fprintf(w, "plan: %d source entry(ies)\n", len(out.Result.Decisions))
	if out.ManifestKnown {
		if out.Direction == target.RemoteToLocal {
			fmt.Fprintln(w, "destination metadata: authoritative (local destination scanned)")
		} else {
			fmt.Fprintln(w, "destination metadata: authoritative (remote manifest collected)")
		}
	} else {
		fmt.Fprintln(w, "destination metadata: UNKNOWN — conservative fallback, every transferable file routes to rsync")
	}
	fmt.Fprintf(w, "to transfer: %d file(s), %d logical source bytes (not a network-traffic estimate)\n",
		counts[planner.StatusPending], out.LogicalBytes)
	fmt.Fprintf(w, "  native small-file (parallel rsync): %d\n", byBackend[state.BackendFPSync])
	fmt.Fprintf(w, "  mscp (large new, native multistream): %d\n", byBackend[state.BackendMSCP])
	fmt.Fprintf(w, "  rsync (conservative): %d\n", byBackend[state.BackendRsync])
	fmt.Fprintf(w, "skipped (unchanged): %d\n", counts[planner.StatusSkipped])
	fmt.Fprintf(w, "unsupported (symlink/special/scan error): %d\n", counts[planner.StatusUnsupported])
	if opts.Verbose {
		for _, d := range out.Result.Decisions {
			fmt.Fprintf(w, "  %-11s %-6s %8d  %s — %s\n", d.Status, d.Backend, d.Size, d.Path, d.Reason)
		}
	} else if len(unsupported) > 0 {
		for _, p := range unsupported {
			fmt.Fprintf(w, "  unsupported: %s\n", p)
		}
	}
	fmt.Fprintln(w, "no data was transferred (plan only)")
}

// runSync executes the sync command and prints the run summary.
func runSync(ctx context.Context, cfg executor.Config, opts *Options, stdout, stderr io.Writer) int {
	run, err := executor.Sync(ctx, cfg)
	if err != nil {
		fmt.Fprintf(stderr, "catsync: sync: %v\n", err)
		if run != nil {
			fmt.Fprintf(stderr, "catsync: sync: run %s is recorded; inspect with 'catsync status %s'\n", run.ID, run.ID)
		}
		return exitForError(err)
	}
	printRunSummary(stdout, run, opts)
	warnUnsupported(stderr, "sync", run)
	return ExitOK
}

// runResume executes the resume command.
func runResume(ctx context.Context, opts *Options, id string, stdout, stderr io.Writer) int {
	execOut := stdout
	if opts.Quiet {
		execOut = nil
	}
	cfg := executor.Config{
		StateDir: opts.StateDir,
		Programs: programsFromEnv(),
		Stdout:   execOut,
		Stderr:   stderr,
	}
	run, err := executor.Resume(ctx, cfg, id)
	if err != nil {
		fmt.Fprintf(stderr, "catsync: resume: %v\n", err)
		return exitForError(err)
	}
	printRunSummary(stdout, run, opts)
	warnUnsupported(stderr, "resume", run)
	return ExitOK
}

// printRunSummary renders the final counts of a finished sync or resume.
// Unsupported entries are always called out, even without --verbose: they
// need a transfer the MVP cannot perform and must not look like skipped
// work.
func printRunSummary(w io.Writer, run *state.Run, opts *Options) {
	var counts map[state.Status]int = map[state.Status]int{}
	byBackend := map[state.Backend]int{}
	for _, t := range run.Tasks {
		counts[t.Status]++
		byBackend[t.Backend]++
	}
	if run.Config.DryRun {
		fmt.Fprintf(w, "run %s: dry run — no data was transferred\n", run.ID)
	} else {
		fmt.Fprintf(w, "run %s: %s\n", run.ID, run.Status)
	}
	fmt.Fprintf(w, "  completed %d, pending %d, failed %d, skipped %d, unsupported %d (of %d tasks)\n",
		counts[state.StatusCompleted], counts[state.StatusPending],
		counts[state.StatusFailed], counts[state.StatusSkipped],
		counts[state.StatusUnsupported], len(run.Tasks))
	if n := counts[state.StatusUnsupported]; n > 0 {
		fmt.Fprintf(w, "  unsupported %d — NOT transferred (symlinks, special files, or scan errors)\n", n)
		if !opts.Verbose {
			for _, p := range unsupportedPaths(run, 5) {
				fmt.Fprintf(w, "    %s\n", p)
			}
		}
	}
	if opts.Verbose {
		for _, t := range run.Tasks {
			line := fmt.Sprintf("  %-9s %-6s %8d  %s", t.Status, t.Backend, t.Size, t.Path)
			if t.Error != "" {
				line += " — " + t.Error
			}
			fmt.Fprintln(w, line)
		}
	}
}

// unsupportedPaths returns up to max unsupported task paths, for concise
// reporting.
func unsupportedPaths(run *state.Run, max int) []string {
	var out []string
	for _, t := range run.Tasks {
		if t.Status != state.StatusUnsupported {
			continue
		}
		out = append(out, t.Path)
		if len(out) >= max {
			break
		}
	}
	return out
}

// warnUnsupported prints the non-verbose warning for runs that completed but
// left unsupported source entries untransferred. It never changes the exit
// status: unsupported entries are a documented MVP limitation, reported
// loudly rather than folded into skipped or completed work.
func warnUnsupported(w io.Writer, command string, run *state.Run) {
	n := 0
	for _, t := range run.Tasks {
		if t.Status == state.StatusUnsupported {
			n++
		}
	}
	if n == 0 {
		return
	}
	paths := unsupportedPaths(run, 5)
	msg := strings.Join(paths, ", ")
	if n > len(paths) {
		msg += fmt.Sprintf(", and %d more", n-len(paths))
	}
	fmt.Fprintf(w, "catsync: %s: warning: %d unsupported source entrie(s) were NOT transferred: %s\n", command, n, msg)
	fmt.Fprintf(w, "catsync: %s: see 'catsync plan' for details; symlinks and special files are not supported by this MVP\n", command)
}

// statusView renders a run for the status command without mutating it.
func printRunStatus(w io.Writer, run *state.Run, verbose bool) {
	fmt.Fprintf(w, "run %s: %s\n", run.ID, run.Status)
	fmt.Fprintf(w, "  source:      %s\n", run.Source.String())
	fmt.Fprintf(w, "  destination: %s\n", run.Destination.String())
	fmt.Fprintf(w, "  created:     %s\n", run.CreatedAt.Format("2006-01-02 15:04:05 UTC"))
	fmt.Fprintf(w, "  updated:     %s\n", run.UpdatedAt.Format("2006-01-02 15:04:05 UTC"))
	if run.FinishedAt != nil {
		fmt.Fprintf(w, "  finished:    %s\n", run.FinishedAt.Format("2006-01-02 15:04:05 UTC"))
	}
	fmt.Fprintf(w, "  config:      direction=%s backend=%s verify=%s jobs=%d streams=%d threshold=%d ipv6=%v dry_run=%v\n",
		run.Config.Direction.Effective(), run.Config.Backend, run.Config.Verify, run.Config.Jobs, run.Config.Streams,
		run.Config.LargeThreshold, run.Config.IPv6, run.Config.DryRun)
	counts := map[state.Status]int{}
	byBackend := map[state.Backend]int{}
	checkpoints := 0
	for _, t := range run.Tasks {
		counts[t.Status]++
		byBackend[t.Backend]++
		if t.Checkpoint != "" {
			checkpoints++
		}
	}
	fmt.Fprintf(w, "  tasks:       %d total — completed %d, pending %d, failed %d, skipped %d, unsupported %d\n",
		len(run.Tasks), counts[state.StatusCompleted], counts[state.StatusPending],
		counts[state.StatusFailed], counts[state.StatusSkipped], counts[state.StatusUnsupported])
	if n := counts[state.StatusUnsupported]; n > 0 {
		fmt.Fprintf(w, "  unsupported: %d (NOT transferred: symlinks, special files, or scan errors)\n", n)
	}
	fmt.Fprintf(w, "  backends:    native-small %d, native-large (mscp) %d, rsync %d\n",
		byBackend[state.BackendFPSync], byBackend[state.BackendMSCP], byBackend[state.BackendRsync])
	if run.FPSyncRunID != "" {
		// Legacy runs recorded an external-fpsync run id; the native
		// backend ignores it, but keep it visible for diagnostics.
		fmt.Fprintf(w, "  legacy fpsync run id: %s (state dir %s)\n", run.FPSyncRunID, run.FPSyncStateDir)
	}
	if checkpoints > 0 {
		fmt.Fprintf(w, "  checkpoints: %d retained (resume artifacts)\n", checkpoints)
	}
	if run.Error != "" {
		fmt.Fprintf(w, "  error:       %s\n", run.Error)
	}
	if verbose {
		for _, t := range run.Tasks {
			line := fmt.Sprintf("    %-9s %-6s %8d  %s", t.Status, t.Backend, t.Size, t.Path)
			var extras []string
			if t.Error != "" {
				extras = append(extras, t.Error)
			}
			if t.Checkpoint != "" {
				extras = append(extras, "checkpoint "+t.Checkpoint)
			}
			if len(extras) > 0 {
				line += " — " + strings.Join(extras, "; ")
			}
			fmt.Fprintln(w, line)
		}
	}
}

// runStatus implements the status command: it reads a run without mutating
// it and prints the summary.
func runStatus(opts *Options, id string, stdout, stderr io.Writer) int {
	store, err := state.NewStore(opts.StateDir)
	if err != nil {
		fmt.Fprintf(stderr, "catsync: status: %v\n", err)
		return ExitUsage
	}
	var run *state.Run
	if id == "" {
		run, err = store.Latest()
	} else {
		run, err = store.Load(id)
	}
	if err != nil {
		fmt.Fprintf(stderr, "catsync: status: %v\n", err)
		return ExitError
	}
	printRunStatus(stdout, run, opts.Verbose)
	return ExitOK
}
