package daemon

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

// timingRecorder is an opt-in, benchmark-only diagnostic. Setting
// CATSYNC_BENCH_TIMING to a path appends one JSON event per bootstrap phase.
// It carries no identities, paths, or payload data and is disabled by
// default.
type timingRecorder struct {
	mu   sync.Mutex
	file *os.File
	enc  *json.Encoder
}

type timingEvent struct {
	Event     string `json:"event"`
	UnixNano  int64  `json:"unix_nano"`
	ElapsedMS int64  `json:"elapsed_ms,omitempty"`
}

func newTimingRecorder() *timingRecorder {
	path := os.Getenv("CATSYNC_BENCH_TIMING")
	if path == "" {
		return nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil
	}
	return &timingRecorder{file: f, enc: json.NewEncoder(f)}
}

var timingStart = time.Now()

func (r *timingRecorder) event(name string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	_ = r.enc.Encode(timingEvent{Event: name, UnixNano: time.Now().UnixNano(), ElapsedMS: time.Since(timingStart).Milliseconds()})
}
