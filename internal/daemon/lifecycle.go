package daemon

import (
	"io"
	"time"

	"github.com/dotfrankruan/CatSync/internal/daemon/protocol"
)

// noteDirectControl records authenticated data-control activity. After
// handoff this is the only liveness authority for the one-shot worker.
func (s *server) noteDirectControl() {
	s.lastDirect.Store(time.Now().UnixNano())
}

func (s *server) checkLifecycleSession(id string) error {
	if id == "" || id != s.sessionID {
		return protocol.NewError(protocol.CodeInvalidArgument, "lifecycle control is not bound to this session")
	}
	return nil
}

// handoffRPC transfers session ownership from the SSH bootstrap to the
// authenticated data control channel. It is idempotent: a retried handoff
// after a lost response returns detached without starting a second worker.
func (s *server) handoffRPC(req *protocol.HandoffRequest, w rpcWriter) {
	if !s.cfg.DetachOnHandoff {
		w.Abort(protocol.NewError(protocol.CodeFeatureUnavailable, "server does not support detached SSH handoff"))
		return
	}
	if err := s.checkLifecycleSession(req.SessionID); err != nil {
		w.Abort(err)
		return
	}
	s.noteDirectControl()
	started := s.detached.CompareAndSwap(false, true)
	if err := w.JSON(protocol.LifecycleResponse{State: "detached"}); err != nil {
		return
	}
	if started {
		// The response is on the authenticated data channel. Closing the SSH
		// stdio only after it is written means a lost response can retry.
		go s.beginDetach()
	}
}

func (s *server) heartbeatRPC(req *protocol.HeartbeatRequest, w rpcWriter) {
	if !s.detached.Load() {
		w.Abort(protocol.NewError(protocol.CodeInvalidArgument, "heartbeat requires a detached session"))
		return
	}
	if err := s.checkLifecycleSession(req.SessionID); err != nil {
		w.Abort(err)
		return
	}
	s.noteDirectControl()
	_ = w.JSON(protocol.LifecycleResponse{State: "active"})
}

func (s *server) byeRPC(req *protocol.ByeRequest, w rpcWriter) {
	if !s.detached.Load() {
		w.Abort(protocol.NewError(protocol.CodeInvalidArgument, "data bye requires a detached session"))
		return
	}
	if err := s.checkLifecycleSession(req.SessionID); err != nil {
		w.Abort(err)
		return
	}
	s.noteDirectControl()
	if err := w.JSON(protocol.LifecycleResponse{State: "draining"}); err != nil {
		return
	}
	select {
	case s.shutdownReq <- true:
	default:
		// A concurrent shutdown is already queued. The session is still
		// observed as draining by the response above.
	}
}

// beginDetach releases the SSH bootstrap descriptors. The process itself is
// already in a detached session (the remote launch wrapper); closing these
// descriptors lets OpenSSH finish and guarantees the data plane cannot depend
// on the bootstrap stdin/stderr lifetime.
func (s *server) beginDetach() {
	s.logf("handoff complete; detached data control owns the session")
	closeIfCloser(s.cfg.Stdin)
	closeIfCloser(s.cfg.Stdout)
	closeIfCloser(s.cfg.Stderr)
}

func closeIfCloser(v any) {
	if c, ok := v.(io.Closer); ok {
		_ = c.Close()
	}
}
