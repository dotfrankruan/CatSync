package daemon

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/quic-go/quic-go"

	"github.com/dotfrankruan/CatSync/internal/daemon/protocol"
)

func TestQlogTracerCreatesOwnerOnlyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logs", "conn.sqlog")
	// A pre-existing world-readable file must be tightened.
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	tracer, err := openQlogTracer(path)
	if err != nil {
		t.Fatalf("openQlogTracer: %v", err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := fi.Mode().Perm(); got != 0o600 {
		t.Fatalf("qlog mode = %o, want 600", got)
	}
	trace := tracer(context.Background(), true, quic.ConnectionIDFromBytes([]byte{1, 2, 3, 4}))
	if trace == nil {
		t.Fatal("tracer returned nil")
	}
	rec := trace.AddProducer()
	if rec == nil {
		t.Fatal("trace has no producer")
	}
	if err := rec.Close(); err != nil {
		t.Fatal(err)
	}
	// A second connection must not be able to interleave into the same file.
	if again := tracer(context.Background(), true, quic.ConnectionIDFromBytes([]byte{5, 6, 7, 8})); again != nil {
		t.Fatal("second trace was accepted for the same qlog file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 || !strings.Contains(string(data), "qlog") {
		t.Fatalf("qlog file has no header: %q", string(data[:min(80, len(data))]))
	}
}

func TestQUICActivityTracksPerType(t *testing.T) {
	a := newQUICActivity(true)
	a.touch(protocol.StreamControl, 12)
	a.touch(protocol.StreamSFTP, 1<<20)
	a.touch(protocol.StreamExec, 99)
	got := a.String()
	for _, want := range []string{"control{last=", "bytes=12", "sftp{last=", "bytes=1048576", "exec{last=", "bytes=99"} {
		if !strings.Contains(got, want) {
			t.Errorf("activity %q lacks %q", got, want)
		}
	}
	if newQUICActivity(false) != nil {
		t.Fatal("disabled activity allocated state")
	}
	// touch on nil is safe on the normal data path.
	var nilAct *quicActivity
	nilAct.touch(protocol.StreamSFTP, 10)
	if !strings.Contains(nilAct.String(), "disabled") {
		t.Fatal("nil activity should render disabled")
	}
}

func TestQlogTracerEmptyPath(t *testing.T) {
	tracer, err := openQlogTracer("")
	if err != nil || tracer != nil {
		t.Fatalf("empty path: err=%v tracerNil=%v; want nil,nil", err, tracer == nil)
	}
}
