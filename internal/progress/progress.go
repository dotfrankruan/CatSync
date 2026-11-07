// Package progress renders a bounded, concurrency-safe transfer progress
// display. It owns no transfer goroutines and receives byte deltas from the
// backends, so reporting can never control correctness or cancellation.
package progress

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

const (
	barWidth = 24
	// maxFileLines bounds how many active transfers the display lists, so a
	// wide run cannot scroll the terminal.
	maxFileLines = 5
	// pathWidth bounds the file column.
	pathWidth = 44
)

// FileState is one active transfer as shown in a snapshot.
type FileState struct {
	// Label identifies the transfer: a relative path, or a batch label.
	Label string
	// Size is the planned byte count of the transfer.
	Size uint64
	// Done is how many bytes this transfer has moved.
	Done uint64
	// Speed is the smoothed bytes/second of this transfer.
	Speed float64
}

// Snapshot is the display state at one instant. It is exported so tests and
// alternative front ends can assert on it without scraping text.
type Snapshot struct {
	Done       uint64
	Total      uint64
	Speed      float64
	Elapsed    time.Duration
	Phase      string
	Terminal   bool
	Conns      int // currently open SSH connections
	FilesDone  int // transfers completed
	Checked    int // files verified
	Skipped    int // files skipped because the destination already matched
	FilesTotal int // files in the run
	Files      []FileState
	ETA        time.Duration
	HasETA     bool
}

// fileProgress tracks one active transfer.
type fileProgress struct {
	label      string
	size       uint64
	done       uint64
	lastBytes  uint64
	lastSample time.Time
	speed      float64
}

// Reporter aggregates byte progress from concurrent backends. Terminal mode
// redraws a multi-line block in place; line mode emits periodic snapshots
// suitable for logs.
type Reporter struct {
	mu         sync.Mutex
	w          io.Writer
	total      uint64
	done       uint64
	start      time.Time
	lastSample time.Time
	lastBytes  uint64
	speed      float64
	phase      string
	terminal   bool
	lastDraw   time.Time
	conns      int
	filesDone  int
	totalFiles int
	checked    int
	skipped    int
	files      map[string]*fileProgress
	order      []string
	drawnLines int
	stop       chan struct{}
	stopped    chan struct{}
	stopOnce   sync.Once
}

// New starts a reporter. A nil writer disables reporting and returns nil.
func New(w io.Writer, total uint64, terminal bool) *Reporter {
	return NewWithBaseline(w, total, 0, terminal)
}

// NewWithBaseline starts a reporter whose already-completed byte count is
// known. A resumed transfer uses it so the bar, percentage, and ETA describe
// the whole run instead of only the bytes this process moves.
func NewWithBaseline(w io.Writer, total, baseline uint64, terminal bool) *Reporter {
	if w == nil {
		return nil
	}
	if baseline > total {
		baseline = total
	}
	now := time.Now()
	r := &Reporter{
		w: w, total: total, done: baseline, start: now, lastSample: now, lastBytes: baseline,
		phase: "starting", terminal: terminal,
		files: make(map[string]*fileProgress),
		stop:  make(chan struct{}), stopped: make(chan struct{}),
	}
	r.draw(now, true)
	go r.loop()
	return r
}

// Total returns the planned byte count.
func (r *Reporter) Total() uint64 {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.total
}

// Done returns the bytes transferred or already known complete.
func (r *Reporter) Done() uint64 {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.done
}

// Snapshot returns the current display state.
func (r *Reporter) Snapshot() Snapshot {
	if r == nil {
		return Snapshot{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.snapshotLocked(time.Now())
}

// Add records newly transferred bytes that belong to no single file (for
// example a manifest walk). Concurrent calls are safe.
func (r *Reporter) Add(n uint64) {
	if r == nil || n == 0 {
		return
	}
	r.mu.Lock()
	r.addLocked(n)
	shouldDraw := r.terminal && time.Since(r.lastDraw) >= 100*time.Millisecond
	r.mu.Unlock()
	if shouldDraw {
		r.draw(time.Now(), false)
	}
}

// Connections reports the current number of open SSH connections.
func (r *Reporter) Connections(n int) {
	if r == nil {
		return
	}
	r.mu.Lock()
	if n < 0 {
		n = 0
	}
	r.conns = n
	r.mu.Unlock()
}

// ConnDelta adjusts the SSH connection count by delta.
func (r *Reporter) ConnDelta(delta int) {
	if r == nil || delta == 0 {
		return
	}
	r.mu.Lock()
	r.conns += delta
	if r.conns < 0 {
		r.conns = 0
	}
	r.mu.Unlock()
}

// FilesTotal sets how many files exist in the run, for the "done/total" column.
func (r *Reporter) FilesTotal(n int) {
	if r == nil || n < 0 {
		return
	}
	r.mu.Lock()
	r.totalFiles = n
	r.mu.Unlock()
}

// FileStart begins tracking one active transfer. Repeating a label replaces
// its state (a resumed file starts from its durable byte count).
func (r *Reporter) FileStart(label string, size, done uint64) {
	if r == nil || label == "" {
		return
	}
	now := time.Now()
	r.mu.Lock()
	f, ok := r.files[label]
	if !ok {
		f = &fileProgress{label: label}
		r.files[label] = f
		r.order = append(r.order, label)
	}
	if size > 0 {
		f.size = size
	}
	if done > f.done {
		f.done = done
	}
	f.lastBytes = f.done
	f.lastSample = now
	r.mu.Unlock()
}

// FileAdd records bytes for one active transfer and in the aggregate.
func (r *Reporter) FileAdd(label string, n uint64) {
	if r == nil || n == 0 {
		return
	}
	now := time.Now()
	r.mu.Lock()
	r.addLocked(n)
	if f, ok := r.files[label]; ok {
		f.done += n
		if f.size > 0 && f.done > f.size {
			f.done = f.size
		}
	}
	shouldDraw := r.terminal && now.Sub(r.lastDraw) >= 100*time.Millisecond
	r.mu.Unlock()
	if shouldDraw {
		r.draw(now, false)
	}
}

// FileEnd stops tracking an active transfer.
func (r *Reporter) FileEnd(label string) {
	if r == nil || label == "" {
		return
	}
	r.mu.Lock()
	if _, ok := r.files[label]; ok {
		delete(r.files, label)
		for i, l := range r.order {
			if l == label {
				r.order = append(r.order[:i], r.order[i+1:]...)
				break
			}
		}
	}
	r.mu.Unlock()
}

// CompletedFiles records n finished transfers.
func (r *Reporter) CompletedFiles(n int) {
	if r == nil || n <= 0 {
		return
	}
	r.mu.Lock()
	r.filesDone += n
	r.mu.Unlock()
}

// CheckedFiles records n verified files.
func (r *Reporter) CheckedFiles(n int) {
	if r == nil || n <= 0 {
		return
	}
	r.mu.Lock()
	r.checked += n
	r.mu.Unlock()
}

// SkippedFiles records n files that needed no transfer.
func (r *Reporter) SkippedFiles(n int) {
	if r == nil || n <= 0 {
		return
	}
	r.mu.Lock()
	r.skipped += n
	r.mu.Unlock()
}

// addLocked adds aggregate bytes, clamped to the total.
func (r *Reporter) addLocked(n uint64) {
	if n > r.total-r.done {
		r.done = r.total
	} else {
		r.done += n
	}
}

// Phase changes the activity label and redraws immediately.
func (r *Reporter) Phase(phase string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.phase = phase
	r.mu.Unlock()
	r.draw(time.Now(), true)
}

// Clear removes the active terminal block before another diagnostic is
// printed. Line-mode reporters need no clearing.
func (r *Reporter) Clear() {
	if r == nil || !r.terminal {
		return
	}
	r.mu.Lock()
	r.clearLocked()
	r.mu.Unlock()
}

// Redraw restores a terminal block after a diagnostic.
func (r *Reporter) Redraw() {
	if r != nil && r.terminal {
		r.draw(time.Now(), true)
	}
}

// Finish stops the ticker and prints a final snapshot. Success forces 100%;
// failure preserves the last observed byte count.
func (r *Reporter) Finish(success bool) {
	if r == nil {
		return
	}
	r.stopOnce.Do(func() { close(r.stop) })
	<-r.stopped
	r.mu.Lock()
	if success {
		r.done = r.total
		r.phase = "completed"
	} else {
		r.phase = "stopped"
	}
	r.files = make(map[string]*fileProgress)
	r.order = nil
	r.mu.Unlock()
	r.draw(time.Now(), true)
	if r.terminal {
		r.mu.Lock()
		fmt.Fprintln(r.w)
		r.drawnLines = 0
		r.mu.Unlock()
	}
}

func (r *Reporter) loop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	defer close(r.stopped)
	for {
		select {
		case now := <-ticker.C:
			r.draw(now, true)
		case <-r.stop:
			return
		}
	}
}

func (r *Reporter) draw(now time.Time, force bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !force && !r.terminal {
		return
	}
	dt := now.Sub(r.lastSample).Seconds()
	if dt > 0 {
		instant := float64(r.done-r.lastBytes) / dt
		if r.speed == 0 {
			r.speed = instant
		} else {
			r.speed = 0.7*r.speed + 0.3*instant
		}
		r.lastSample, r.lastBytes = now, r.done
		for _, f := range r.files {
			fdt := now.Sub(f.lastSample).Seconds()
			if fdt > 0 {
				fs := float64(f.done-f.lastBytes) / fdt
				if f.speed == 0 {
					f.speed = fs
				} else {
					f.speed = 0.7*f.speed + 0.3*fs
				}
				f.lastSample, f.lastBytes = now, f.done
			}
		}
	}
	r.lastDraw = now
	snap := r.snapshotLocked(now)
	r.render(snap)
}

// snapshotLocked builds the display state; the caller holds the lock.
func (r *Reporter) snapshotLocked(now time.Time) Snapshot {
	snap := Snapshot{
		Done: r.done, Total: r.total, Speed: r.speed,
		Elapsed: now.Sub(r.start), Phase: r.phase, Terminal: r.terminal,
		Conns: r.conns, FilesDone: r.filesDone,
		Checked: r.checked, Skipped: r.skipped, FilesTotal: r.totalFiles,
	}
	if r.speed > 0 && r.done < r.total {
		snap.ETA = time.Duration(float64(r.total-r.done)/r.speed) * time.Second
		snap.HasETA = true
	}
	if r.done >= r.total && r.total > 0 {
		snap.ETA = 0
		snap.HasETA = true
	}
	for _, label := range r.order {
		f, ok := r.files[label]
		if !ok {
			continue
		}
		snap.Files = append(snap.Files, FileState{Label: f.label, Size: f.size, Done: f.done, Speed: f.speed})
	}
	return snap
}

// render writes the current block.
func (r *Reporter) render(snap Snapshot) {
	lines := formatSnapshot(snap)
	if !r.terminal {
		for _, line := range lines {
			fmt.Fprintln(r.w, line)
		}
		return
	}
	// Move back to the first line of the previous block, then rewrite.
	if r.drawnLines > 1 {
		fmt.Fprintf(r.w, "\x1b[%dA", r.drawnLines-1)
	}
	for i, line := range lines {
		fmt.Fprint(r.w, "\r\x1b[K")
		fmt.Fprint(r.w, line)
		if i < len(lines)-1 {
			fmt.Fprint(r.w, "\n")
		}
	}
	// Erase lines left over from a taller previous block.
	if extra := r.drawnLines - len(lines); extra > 0 {
		for i := 0; i < extra; i++ {
			fmt.Fprint(r.w, "\n\r\x1b[K")
		}
		fmt.Fprintf(r.w, "\x1b[%dA", extra)
	}
	r.drawnLines = len(lines)
}

// clearLocked erases the block and leaves the cursor at its first line.
func (r *Reporter) clearLocked() {
	if r.drawnLines > 1 {
		fmt.Fprintf(r.w, "\x1b[%dA", r.drawnLines-1)
	}
	for i := 0; i < r.drawnLines; i++ {
		fmt.Fprint(r.w, "\r\x1b[K")
		if i < r.drawnLines-1 {
			fmt.Fprint(r.w, "\n")
		}
	}
	if r.drawnLines > 1 {
		fmt.Fprintf(r.w, "\x1b[%dA", r.drawnLines-1)
	}
	fmt.Fprint(r.w, "\r\x1b[K")
	r.drawnLines = 0
}

// formatSnapshot renders the display lines: one aggregate header, one counter
// line, then up to maxFileLines active transfers.
func formatSnapshot(s Snapshot) []string {
	fraction := 1.0
	if s.Total > 0 {
		fraction = float64(s.Done) / float64(s.Total)
	}
	if fraction < 0 {
		fraction = 0
	}
	if fraction > 1 {
		fraction = 1
	}
	filled := int(fraction * barWidth)
	bar := strings.Repeat("=", filled) + strings.Repeat("-", barWidth-filled)
	etaText, etdText := "--", "--"
	if s.HasETA {
		etaText = duration(s.ETA)
		etdText = time.Now().Add(s.ETA).Format("15:04:05")
	}
	header := fmt.Sprintf("[%s] %6.2f%%  %s/%s  Current Speed %s/s  Elapsed %s  ETA %s  ETD %s  %s",
		bar, fraction*100, bytes(s.Done), bytes(s.Total), bytes(uint64(s.Speed)),
		duration(s.Elapsed), etaText, etdText, s.Phase)
	counters := fmt.Sprintf("ssh conns %d  |  files %d/%d done, %d checked, %d skipped",
		s.Conns, s.FilesDone, s.FilesTotal, s.Checked, s.Skipped)
	lines := []string{header, counters}
	shown := s.Files
	if len(shown) > maxFileLines {
		shown = shown[:maxFileLines]
	}
	for _, f := range shown {
		lines = append(lines, formatFile(f))
	}
	if extra := len(s.Files) - len(shown); extra > 0 {
		lines = append(lines, fmt.Sprintf("  … and %d more file(s)", extra))
	}
	return lines
}

// formatFile renders one active transfer line.
func formatFile(f FileState) string {
	size := f.Size
	if size < f.Done {
		size = f.Done
	}
	eta := "--"
	if f.Speed > 0 && size > f.Done {
		eta = duration(time.Duration(float64(size-f.Done)/f.Speed) * time.Second)
	} else if size > 0 && size == f.Done {
		eta = "00:00"
	}
	return fmt.Sprintf("  %-*s  %s/%s  %s/s  ETA %s",
		pathWidth, truncatePath(f.Label, pathWidth), bytes(f.Done), bytes(size),
		bytes(uint64(f.Speed)), eta)
}

// truncatePath shortens a path from the left so the tail stays readable.
func truncatePath(p string, width int) string {
	if len(p) <= width {
		return p
	}
	if width <= 1 {
		return p[len(p)-width:]
	}
	return "…" + p[len(p)-(width-1):]
}

func duration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	d = d.Round(time.Second)
	h := int(d / time.Hour)
	m := int(d/time.Minute) % 60
	s := int(d/time.Second) % 60
	if h > 0 {
		return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
	}
	return fmt.Sprintf("%02d:%02d", m, s)
}

func bytes(n uint64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := uint64(unit), 0
	for q := n / unit; q >= unit && exp < 5; q /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
