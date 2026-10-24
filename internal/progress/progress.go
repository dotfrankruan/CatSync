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

const barWidth = 24

// Reporter aggregates byte progress from concurrent backends. Terminal mode
// redraws one line; line mode emits periodic snapshots suitable for logs.
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
	stop       chan struct{}
	stopped    chan struct{}
	stopOnce   sync.Once
}

// New starts a reporter. A nil writer disables reporting and returns nil.
func New(w io.Writer, total uint64, terminal bool) *Reporter {
	if w == nil {
		return nil
	}
	now := time.Now()
	r := &Reporter{
		w: w, total: total, start: now, lastSample: now,
		phase: "starting", terminal: terminal,
		stop: make(chan struct{}), stopped: make(chan struct{}),
	}
	r.draw(now, true)
	go r.loop()
	return r
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

// Add records newly transferred bytes. Concurrent calls are safe.
func (r *Reporter) Add(n uint64) {
	if r == nil || n == 0 {
		return
	}
	r.mu.Lock()
	if n > r.total-r.done {
		r.done = r.total
	} else {
		r.done += n
	}
	now := time.Now()
	shouldDraw := r.terminal && now.Sub(r.lastDraw) >= 100*time.Millisecond
	r.mu.Unlock()
	if shouldDraw {
		r.draw(now, false)
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

// Clear removes the active terminal line before another diagnostic is
// printed. Line-mode reporters need no clearing.
func (r *Reporter) Clear() {
	if r == nil || !r.terminal {
		return
	}
	r.mu.Lock()
	fmt.Fprint(r.w, "\r\x1b[K")
	r.mu.Unlock()
}

// Redraw restores a terminal line after a diagnostic.
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
	r.mu.Unlock()
	r.draw(time.Now(), true)
	if r.terminal {
		r.mu.Lock()
		fmt.Fprintln(r.w)
		r.mu.Unlock()
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
	}
	r.lastDraw = now
	line := format(r.done, r.total, r.speed, now.Sub(r.start), now, r.phase)
	if r.terminal {
		fmt.Fprintf(r.w, "\r\x1b[K%s", line)
	} else {
		fmt.Fprintln(r.w, line)
	}
}

func format(done, total uint64, speed float64, elapsed time.Duration, now time.Time, phase string) string {
	fraction := 1.0
	if total > 0 {
		fraction = float64(done) / float64(total)
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
	if speed > 0 && done < total {
		eta := time.Duration(float64(total-done)/speed) * time.Second
		etaText = duration(eta)
		etdText = now.Add(eta).Format("15:04:05")
	} else if done >= total {
		etaText = "00:00"
		etdText = now.Format("15:04:05")
	}
	return fmt.Sprintf("[%s] %6.2f%%  %s/%s  Current Speed %s/s  Elapsed %s  ETA %s  ETD %s  %s",
		bar, fraction*100, bytes(done), bytes(total), bytes(uint64(speed)), duration(elapsed), etaText, etdText, phase)
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
