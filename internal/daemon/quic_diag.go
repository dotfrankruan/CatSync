package daemon

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlogwriter"

	"github.com/dotfrankruan/CatSync/internal/daemon/protocol"
)

// qlogFileWriter buffers qlog output and makes sure the file is closed when
// the trace ends.
type qlogFileWriter struct {
	f *os.File
	w *bufio.Writer
}

func (q *qlogFileWriter) Write(p []byte) (int, error) { return q.w.Write(p) }
func (q *qlogFileWriter) Close() error {
	ferr := q.w.Flush()
	cerr := q.f.Close()
	if ferr != nil {
		return ferr
	}
	return cerr
}

// openQlogTracer opens path with owner-only permissions and returns a tracer
// for exactly one QUIC connection. The qlog writer is closed by quic-go when
// the connection trace has no producers left.
func openQlogTracer(path string) (func(context.Context, bool, quic.ConnectionID) qlogwriter.Trace, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}
	if strings.ContainsRune(path, 0) {
		return nil, fmt.Errorf("daemon: qlog path contains NUL")
	}
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("daemon: create qlog directory: %w", err)
		}
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("daemon: open qlog file: %w", err)
	}
	// Existing files keep their old mode unless chmodded explicitly.
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("daemon: secure qlog file: %w", err)
	}
	var used atomic.Bool
	return func(_ context.Context, isClient bool, connID quic.ConnectionID) qlogwriter.Trace {
		// One run has exactly one QUIC connection. Refuse to interleave a
		// second trace into the same file if that invariant is ever broken.
		if !used.CompareAndSwap(false, true) {
			return nil
		}
		seq := qlogwriter.NewConnectionFileSeq(&qlogFileWriter{f: f, w: bufio.NewWriterSize(f, 256<<10)}, isClient, connID)
		go seq.Run()
		return seq
	}, nil
}

// quicActivity tracks the last successful application-level progress per
// stream type. It is enabled only for diagnostic runs (when a qlog path is
// set) so the normal data path pays no synchronization cost beyond a nil
// check. Values are queried when a QUIC connection ends, to distinguish
// "transport stopped receiving ACKs" from "application stopped reading or
// writing".
type quicActivity struct {
	start time.Time
	last  [4]atomic.Int64
	bps   [4]atomic.Uint64
}

func newQUICActivity(enabled bool) *quicActivity {
	if !enabled {
		return nil
	}
	a := &quicActivity{start: time.Now()}
	for i := range a.last {
		a.last[i].Store(a.start.UnixNano())
	}
	return a
}

func activityIndex(typ byte) int {
	switch typ {
	case protocol.StreamControl:
		return 1
	case protocol.StreamSFTP:
		return 2
	case protocol.StreamExec:
		return 3
	default:
		return 0
	}
}

// touch records n bytes of application progress for one stream type.
func (a *quicActivity) touch(typ byte, n int) {
	if a == nil || n <= 0 {
		return
	}
	i := activityIndex(typ)
	a.last[i].Store(time.Now().UnixNano())
	a.bps[i].Add(uint64(n))
}

func (a *quicActivity) String() string {
	if a == nil {
		return "disabled"
	}
	now := time.Now()
	var b strings.Builder
	for i, label := range [...]string{"other", "control", "sftp", "exec"} {
		ago := now.Sub(time.Unix(0, a.last[i].Load())).Round(time.Millisecond)
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "%s{last=%s bytes=%d}", label, ago, a.bps[i].Load())
	}
	return b.String()
}

// logQUICDiagnostic writes a one-line end-of-connection summary when qlog
// diagnostics are enabled. It goes to stderr so it is visible in the normal
// run output and is never written into user data.
func logQUICDiagnostic(w io.Writer, label string, a *quicActivity, reason error) {
	if a == nil || w == nil {
		return
	}
	fmt.Fprintf(w, "%s: QUIC diagnostic: reason=%v activity: %s\n", label, reason, a.String())
}
