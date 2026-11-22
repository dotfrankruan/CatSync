package daemon

import (
	"encoding/json"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dotfrankruan/CatSync/internal/daemon/protocol"
)

type lifecycleWriter struct {
	value   any
	aborted error
}

func (w *lifecycleWriter) BeginJSON() {}
func (w *lifecycleWriter) JSON(v any) error {
	w.value = v
	return nil
}
func (w *lifecycleWriter) BeginStream() *json.Encoder { return json.NewEncoder(io.Discard) }
func (w *lifecycleWriter) Flush()                     {}
func (w *lifecycleWriter) Abort(err error)            { w.aborted = err }

type closeBuffer struct{ closed atomic.Bool }

func (b *closeBuffer) Read([]byte) (int, error)    { return 0, io.EOF }
func (b *closeBuffer) Write(p []byte) (int, error) { return len(p), nil }
func (b *closeBuffer) Close() error {
	b.closed.Store(true)
	return nil
}

func lifecycleServer(detach bool) (*server, *closeBuffer, *closeBuffer, *closeBuffer) {
	stdin, stdout, stderr := &closeBuffer{}, &closeBuffer{}, &closeBuffer{}
	s := &server{
		cfg:         ServerConfig{DetachOnHandoff: detach, Stdin: stdin, Stdout: stdout, Stderr: stderr},
		sessionID:   "session-1",
		shutdownReq: make(chan bool, 1),
	}
	return s, stdin, stdout, stderr
}

func TestHandoffRejectsUnknownSessionWithoutDetaching(t *testing.T) {
	s, stdin, stdout, _ := lifecycleServer(true)
	w := &lifecycleWriter{}
	s.handoffRPC(&protocol.HandoffRequest{SessionID: "wrong"}, w)
	if w.aborted == nil || !strings.Contains(w.aborted.Error(), "not bound") {
		t.Fatalf("abort = %v", w.aborted)
	}
	if s.detached.Load() {
		t.Fatal("wrong session detached the server")
	}
	if stdin.closed.Load() || stdout.closed.Load() {
		t.Fatal("wrong session closed bootstrap descriptors")
	}
}

func TestHandoffRequiresServerCapability(t *testing.T) {
	s, _, _, _ := lifecycleServer(false)
	w := &lifecycleWriter{}
	s.handoffRPC(&protocol.HandoffRequest{SessionID: "session-1"}, w)
	if w.aborted == nil || !strings.Contains(w.aborted.Error(), "does not support") {
		t.Fatalf("abort = %v", w.aborted)
	}
	if s.detached.Load() {
		t.Fatal("unsupported handoff detached the server")
	}
}

func TestHandoffIsIdempotentAndClosesBootstrap(t *testing.T) {
	s, stdin, stdout, stderr := lifecycleServer(true)
	first := &lifecycleWriter{}
	s.handoffRPC(&protocol.HandoffRequest{SessionID: "session-1"}, first)
	if got, ok := first.value.(protocol.LifecycleResponse); !ok || got.State != "detached" {
		t.Fatalf("first response = %#v", first.value)
	}
	if !s.detached.Load() {
		t.Fatal("server did not detach")
	}
	// beginDetach is asynchronous; wait for all three descriptors.
	for i := 0; i < 100; i++ {
		if stdin.closed.Load() && stdout.closed.Load() && stderr.closed.Load() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !stdin.closed.Load() || !stdout.closed.Load() || !stderr.closed.Load() {
		t.Fatal("bootstrap descriptors were not closed after handoff")
	}
	second := &lifecycleWriter{}
	s.handoffRPC(&protocol.HandoffRequest{SessionID: "session-1"}, second)
	if got, ok := second.value.(protocol.LifecycleResponse); !ok || got.State != "detached" {
		t.Fatalf("second response = %#v", second.value)
	}
	if second.aborted != nil {
		t.Fatalf("duplicate handoff aborted: %v", second.aborted)
	}
}

func TestHeartbeatAndByeRequireDetachedSession(t *testing.T) {
	s, _, _, _ := lifecycleServer(true)
	w := &lifecycleWriter{}
	s.heartbeatRPC(&protocol.HeartbeatRequest{SessionID: "session-1"}, w)
	if w.aborted == nil {
		t.Fatal("heartbeat accepted before handoff")
	}
	w = &lifecycleWriter{}
	s.byeRPC(&protocol.ByeRequest{SessionID: "session-1"}, w)
	if w.aborted == nil {
		t.Fatal("data bye accepted before handoff")
	}

	s.detached.Store(true)
	w = &lifecycleWriter{}
	s.heartbeatRPC(&protocol.HeartbeatRequest{SessionID: "session-1"}, w)
	if w.aborted != nil {
		t.Fatalf("heartbeat after handoff: %v", w.aborted)
	}
	w = &lifecycleWriter{}
	s.byeRPC(&protocol.ByeRequest{SessionID: "session-1"}, w)
	if w.aborted != nil {
		t.Fatalf("bye after handoff: %v", w.aborted)
	}
	if got, ok := w.value.(protocol.LifecycleResponse); !ok || got.State != "draining" {
		t.Fatalf("bye response = %#v", w.value)
	}
	select {
	case graceful := <-s.shutdownReq:
		if !graceful {
			t.Fatal("bye requested non-graceful shutdown")
		}
	default:
		t.Fatal("bye did not queue shutdown")
	}
}

func TestHandoffUnsupportedClassification(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{protocol.NewError(protocol.CodeFeatureUnavailable, "server does not support detached SSH handoff"), true},
		{protocol.NewError(protocol.CodeInvalidArgument, `unknown control path "/v1/handoff"`), true},
		{protocol.NewError(protocol.CodeInternal, "rpc /v1/handoff: HTTP 404"), true},
		{protocol.NewError(protocol.CodeInvalidArgument, "lifecycle control is not bound to this session"), false},
		{protocol.NewError(protocol.CodeControlChannelLost, "rpc failed"), false},
	}
	for _, tc := range cases {
		if got := isHandoffUnsupported(tc.err); got != tc.want {
			t.Errorf("isHandoffUnsupported(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}
