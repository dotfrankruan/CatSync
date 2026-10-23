// Package planner assigns every discovered source entry exactly one
// transfer backend and status, producing a plan in which no two backends
// ever own the same destination path.
//
// Assignment policy, given source entries, optional destination metadata,
// the large-file threshold, the requested backend, and tool availability:
//
//   - unchanged size and mtime:        skipped (no transfer)
//   - new file below the threshold:    BackendFPSync, the native parallel
//     rsync small-file stage (the backend name is kept for compatibility
//     with existing runs; no external fpsync or fpart process is used)
//   - new file at or above threshold:  mscp when available, otherwise rsync
//   - existing/modified/unknown file:  rsync
//   - symlink, special, or scan error: unsupported (explicit)
//
// Destination metadata is optional: a nil map means the destination state
// is unknown (for example when manifest collection failed), and every
// transferable file falls back to rsync. A non-nil map — even an empty
// one — is authoritative, so absent paths count as new.
//
// Size and mtime equality is only a planning hint, never proof of content
// equality; a mismatch routes the file to rsync, the correctness fallback.
//
// Forced backends must preserve the safety policy or fail: a forced mscp
// rejects files that exist at the destination or whose destination state is
// unknown (mscp writes only files confirmed absent), and a forced fpsync
// (the native parallel-rsync small-file backend) rejects files at or above
// the large threshold. Rejections abort planning with an error naming the
// offending paths. Duplicate source paths are likewise fatal.
package planner

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/frankruan/catsync/internal/discovery"
	"github.com/frankruan/catsync/internal/state"
)

// Status is the planning outcome for one path. It mirrors the state's
// pending/skipped vocabulary and adds an explicit unsupported marker for
// entries the MVP cannot transfer.
type Status string

// Planning statuses.
const (
	// StatusPending assigns the file to its backend for transfer.
	StatusPending Status = "pending"
	// StatusSkipped marks a file unchanged at the destination; it is
	// never handed to a backend.
	StatusSkipped Status = "skipped"
	// StatusUnsupported marks an entry the MVP cannot transfer: a
	// symlink, a special file, or a failed scan.
	StatusUnsupported Status = "unsupported"
)

// Tools reports which external transfer tools are available.
type Tools struct {
	// Rsync is the required correctness fallback; planning fails without
	// it, as every fallback path lands on rsync and the native parallel
	// small-file stage drives rsync directly.
	Rsync bool
	// MSCP is the optional parallel large-file backend.
	MSCP bool
}

// Config is one planning request.
type Config struct {
	// Backend is the requested backend: auto or a concrete backend.
	Backend state.Backend
	// Threshold is the size in bytes at which a file counts as large; a
	// file of exactly Threshold bytes is large.
	Threshold uint64
	// Tools reports external tool availability.
	Tools Tools
}

// validate checks the configuration, including that the tools every policy
// path can fall back on are present.
func (c *Config) validate() error {
	if !c.Backend.Valid() {
		return fmt.Errorf("planner: invalid backend %q", c.Backend)
	}
	if !c.Tools.Rsync {
		return errors.New("planner: rsync is required but not available")
	}
	// The native small-file and large-file stages need only rsync and ssh;
	// "mscp" is a compatibility name for the native multistream backend and
	// requires no external tool.
	return nil
}

// Decision is the planned fate of one source path. Backend is the single
// owner of the path for the run; skipped and unsupported paths carry a
// nominal rsync owner so every decision has exactly one valid backend, but
// only StatusPending decisions are ever handed to a backend.
type Decision struct {
	// Path is the slash-separated path relative to the source root.
	Path string
	// Size is the source size in bytes observed at scan time.
	Size int64
	// ModTime is the source modification time observed at scan time.
	ModTime time.Time
	// Backend owns this path for the run.
	Backend state.Backend
	// Status is the planning outcome.
	Status Status
	// Reason records why the decision was made, for reporting and
	// auditing.
	Reason string
}

// Result is the complete assignment for one run.
type Result struct {
	// Decisions holds exactly one decision per source entry, in input
	// order.
	Decisions []Decision
}

// Paths returns the paths of all pending decisions owned by backend b, in
// decision order. Skipped and unsupported decisions are never included.
func (r *Result) Paths(b state.Backend) []string {
	var out []string
	for _, d := range r.Decisions {
		if d.Status == StatusPending && d.Backend == b {
			out = append(out, d.Path)
		}
	}
	return out
}

// Counts returns the number of decisions per status.
func (r *Result) Counts() map[Status]int {
	counts := make(map[Status]int)
	for _, d := range r.Decisions {
		counts[d.Status]++
	}
	return counts
}

// Plan assigns each source entry exactly one backend and status. dest is
// the destination metadata keyed by relative path, or nil when the
// destination state is unknown. The result contains one decision per entry,
// in input order. Planning fails — with no result returned — on an invalid
// configuration, duplicate or invalid source paths, or forced-mode
// rejections.
func Plan(entries []discovery.Entry, dest map[string]discovery.RemoteEntry, cfg Config) (*Result, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	result := &Result{Decisions: make([]Decision, 0, len(entries))}
	seen := make(map[string]struct{}, len(entries))
	var rejected []string
	for _, e := range entries {
		if err := discovery.ValidateRelPath(e.Path); err != nil {
			return nil, fmt.Errorf("planner: source entry: %w", err)
		}
		if _, dup := seen[e.Path]; dup {
			return nil, fmt.Errorf("planner: duplicate source entry %q", e.Path)
		}
		seen[e.Path] = struct{}{}

		d := Decision{Path: e.Path, Size: e.Size, ModTime: e.ModTime}
		switch {
		case e.Err != nil:
			d.Status = StatusUnsupported
			d.Backend = state.BackendRsync
			d.Reason = fmt.Sprintf("scan error: %v", e.Err)
		case e.Kind == discovery.KindSymlink:
			d.Status = StatusUnsupported
			d.Backend = state.BackendRsync
			d.Reason = "symlinks are unsupported"
		case e.Kind == discovery.KindSpecial:
			d.Status = StatusUnsupported
			d.Backend = state.BackendRsync
			d.Reason = "special files are unsupported"
		default:
			planFile(&d, e, dest, &cfg, &rejected)
		}
		result.Decisions = append(result.Decisions, d)
	}
	if len(rejected) > 0 {
		return nil, fmt.Errorf("planner: backend %q refuses %d file(s): %s", cfg.Backend, len(rejected), joinQuoted(rejected))
	}
	return result, nil
}

// planFile decides one regular file. Unchanged files are skipped before any
// backend rule applies, so a forced backend never rejects a file that needs
// no transfer.
func planFile(d *Decision, e discovery.Entry, dest map[string]discovery.RemoteEntry, cfg *Config, rejected *[]string) {
	if e.Size < 0 {
		d.Status = StatusUnsupported
		d.Backend = state.BackendRsync
		d.Reason = "invalid negative size"
		return
	}
	de, known := dest[e.Path]
	isNew := dest != nil && !known
	if known && de.Size == e.Size && de.ModTime.Equal(e.ModTime) {
		d.Status = StatusSkipped
		d.Backend = state.BackendRsync
		d.Reason = "unchanged: size and mtime match destination metadata (hint only)"
		return
	}
	large := uint64(e.Size) >= cfg.Threshold

	switch cfg.Backend {
	case state.BackendRsync:
		d.Status = StatusPending
		d.Backend = state.BackendRsync
		d.Reason = "forced rsync"
	case state.BackendFPSync:
		if large {
			*rejected = append(*rejected, e.Path)
			d.Status = StatusUnsupported
			d.Backend = state.BackendRsync
			d.Reason = "forced fpsync (native parallel rsync) refuses files at or above the large threshold"
			return
		}
		d.Status = StatusPending
		d.Backend = state.BackendFPSync
		d.Reason = "forced fpsync (native parallel rsync)"
	case state.BackendMSCP:
		if !isNew {
			*rejected = append(*rejected, e.Path)
			d.Status = StatusUnsupported
			d.Backend = state.BackendRsync
			if dest == nil {
				d.Reason = "forced mscp refuses files whose destination state is unknown"
			} else {
				d.Reason = "forced mscp refuses files that already exist at the destination"
			}
			return
		}
		d.Status = StatusPending
		d.Backend = state.BackendMSCP
		d.Reason = "forced mscp: file confirmed absent at the destination"
	default: // auto
		planAuto(d, e, dest, isNew, large, cfg)
	}
}

// planAuto applies the auto-mode assignment policy to one regular file.
func planAuto(d *Decision, e discovery.Entry, dest map[string]discovery.RemoteEntry, isNew, large bool, cfg *Config) {
	d.Status = StatusPending
	switch {
	case isNew && !large:
		// The native small-file stage is always usable: it drives rsync,
		// which is a required tool.
		d.Backend = state.BackendFPSync
		d.Reason = "new file below threshold: native parallel rsync"
	case isNew && large:
		d.Backend = state.BackendMSCP
		d.Reason = `new file at or above threshold: native multistream ("mscp")`
	default:
		d.Backend = state.BackendRsync
		if dest == nil {
			d.Reason = "destination metadata unknown: rsync"
		} else {
			d.Reason = "existing file with changed size or mtime: rsync"
		}
	}
}

// joinQuoted quotes and joins paths for an error message, capping the list
// so a mass rejection stays readable.
func joinQuoted(paths []string) string {
	const maxShown = 5
	shown := paths
	if len(shown) > maxShown {
		shown = shown[:maxShown]
	}
	quoted := make([]string, len(shown))
	for i, p := range shown {
		quoted[i] = strconv.Quote(p)
	}
	msg := strings.Join(quoted, ", ")
	if rest := len(paths) - len(shown); rest > 0 {
		msg += fmt.Sprintf(", and %d more", rest)
	}
	return msg
}
