package daemon

import (
	"encoding/binary"
	"io"
	"os/exec"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/dotfrankruan/CatSync/internal/daemon/protocol"
	"github.com/dotfrankruan/CatSync/internal/daemon/strictjson"
)

// quicServerConfig builds the server's QUIC transport parameters. Stream
// credit release is explicit so the per-type budgets are deterministic, and
// 0-RTT is never enabled.
func quicServerConfig(cfg *ServerConfig) *quic.Config {
	incoming := int64(cfg.Limits.MaxQUICStreams)
	return &quic.Config{
		MaxIncomingStreams:             incoming,
		MaxIncomingUniStreams:          -1,
		RequireStreamCreditRelease:     true,
		Allow0RTT:                      false,
		HandshakeIdleTimeout:           cfg.HandshakeTimeout,
		MaxIdleTimeout:                 2 * cfg.ControlTimeout,
		InitialStreamReceiveWindow:     cfg.QUICInitialStreamWindow,
		MaxStreamReceiveWindow:         cfg.QUICMaxStreamWindow,
		InitialConnectionReceiveWindow: cfg.QUICInitialStreamWindow,
		MaxConnectionReceiveWindow:     cfg.QUICMaxConnWindow,
	}
}

// quicClientTLS is the server-side view used only to document parity; the
// actual configuration is built by serverTLSConfig with the QUIC ALPN.

func (s *server) quicAcceptLoop(logf func(string, ...any)) {
	for {
		conn, err := s.quicLn.Accept(s.ctx)
		if err != nil {
			return
		}
		s.quicConnMu.Lock()
		if s.quicConn != nil {
			s.quicConnMu.Unlock()
			_ = conn.CloseWithError(1, "only one QUIC connection is allowed per run")
			continue
		}
		s.quicConn = conn
		s.quicConnMu.Unlock()
		go s.runQUICConn(conn, logf)
	}
}

// runQUICConn serves all streams of the single run connection. Losing the
// connection is an abnormal end of the data plane unless shutdown already
// started: the server then drains and exits non-zero.
func (s *server) runQUICConn(conn *quic.Conn, logf func(string, ...any)) {
	for {
		st, err := conn.AcceptStream(s.ctx)
		if err != nil {
			if s.ctx.Err() == nil {
				logf("QUIC connection closed: %v", err)
				go s.shutdown(nil, logf, false)
			}
			return
		}
		go s.handleQUICStream(st)
	}
}

// tryAcquire takes one per-type stream slot without blocking; a full budget
// is a clean reset, never a silently serialized transfer.
func tryAcquire(ch chan struct{}) bool {
	select {
	case ch <- struct{}{}:
		return true
	default:
		return false
	}
}

func release(ch chan struct{}) { <-ch }

func (s *server) handleQUICStream(st *quic.Stream) {
	if s.cfg.Limits.MaxQUICStreams <= 0 {
		st.CancelRead(0)
		return
	}
	// Stream completion releases the peer's stream credit explicitly.
	defer st.ReleaseStreamCredit()
	_ = st.SetReadDeadline(time.Now().Add(s.cfg.HandshakeTimeout))
	var typ [1]byte
	if _, err := io.ReadFull(st, typ[:]); err != nil {
		s.rejectQUICStream(st)
		return
	}
	if !protocol.ValidStreamType(typ[0]) {
		s.rejectQUICStream(st)
		return
	}
	_ = st.SetReadDeadline(time.Time{})
	switch typ[0] {
	case protocol.StreamControl:
		if !tryAcquire(s.quicCtrlSem) {
			s.rejectQUICStream(st)
			return
		}
		defer release(s.quicCtrlSem)
		s.serveQUICControl(st)
	case protocol.StreamSFTP:
		if !tryAcquire(s.quicSFTPSem) {
			s.rejectQUICStream(st)
			return
		}
		defer release(s.quicSFTPSem)
		s.counters.sftpConns.Add(1)
		defer s.counters.sftpConns.Add(-1)
		s.serveQUICSFTP(st)
	case protocol.StreamExec:
		if !tryAcquire(s.quicExecSem) {
			s.rejectQUICStream(st)
			return
		}
		defer release(s.quicExecSem)
		s.counters.execActive.Add(1)
		defer s.counters.execActive.Add(-1)
		s.serveExec(st, func() {
			_ = st.Close()
			st.CancelRead(0)
		})
	}
}

func (s *server) rejectQUICStream(st *quic.Stream) {
	// A reset is deliberate: an invalid type or exhausted budget must be an
	// explicit stream error, never an empty successful response.
	st.CancelWrite(1)
	st.CancelRead(0)
}

// serveQUICControl runs one frozen RPC over a CONTROL stream. Request and
// response framing is length-delimited JSON; stream responses use NDJSON.
func (s *server) serveQUICControl(st *quic.Stream) {
	ctx := st.Context()
	_ = st.SetReadDeadline(time.Now().Add(s.cfg.ControlTimeout))
	var hdr [4]byte
	if _, err := io.ReadFull(st, hdr[:]); err != nil {
		s.rejectQUICStream(st)
		return
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 || n > protocol.MaxQUICControlFrameBytes {
		s.rejectQUICStream(st)
		return
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(st, payload); err != nil {
		s.rejectQUICStream(st)
		return
	}
	var req protocol.QUICControlRequest
	if err := strictjson.Decode(payload, &req, strictjson.Limits{MaxBytes: protocol.MaxQUICControlFrameBytes, MaxDepth: 8}); err != nil {
		w := &quicRPCWriter{w: st}
		w.Abort(protocol.NewError(protocol.CodeJSONTypeMismatch, "control frame: %v", err))
		_ = st.Close()
		return
	}
	if !protocol.IsControlPath(req.Path) {
		w := &quicRPCWriter{w: st}
		w.Abort(protocol.NewError(protocol.CodeInvalidArgument, "unknown control path %q", req.Path))
		_ = st.Close()
		return
	}
	_ = st.SetReadDeadline(time.Time{})
	s.counters.rpcActive.Add(1)
	defer s.counters.rpcActive.Add(-1)
	w := &quicRPCWriter{w: st}
	s.dispatchControlRPC(ctx, req.Path, req.Body, w)
	if !w.started && !w.failed {
		w.Abort(protocol.NewError(protocol.CodeInternal, "control handler produced no response"))
	}
	_ = st.Close()
	st.CancelRead(0)
}

// serveQUICSFTP runs one real OpenSSH sftp-server bound to one QUIC stream.
func (s *server) serveQUICSFTP(st *quic.Stream) {
	defer func() {
		_ = st.Close()
		st.CancelRead(0)
	}()
	if s.cfg.SFTPPath == "" {
		return
	}
	cmd := exec.Command(s.cfg.SFTPPath)
	cmd.Stdin = st
	cmd.Stdout = st
	if s.cfg.Stderr != nil {
		cmd.Stderr = s.cfg.Stderr
	}
	configureChild(cmd)
	if err := cmd.Start(); err != nil {
		s.logf("start sftp-server: %v", err)
		return
	}
	child := &childProcess{cmd: cmd, done: make(chan struct{})}
	s.mu.Lock()
	s.children[child] = struct{}{}
	s.mu.Unlock()
	s.counters.children.Add(1)
	go func() {
		child.err = cmd.Wait()
		close(child.done)
		s.counters.children.Add(-1)
		s.mu.Lock()
		delete(s.children, child)
		s.mu.Unlock()
	}()
	select {
	case <-s.ctx.Done():
		_ = st.Close()
		st.CancelRead(0)
	case <-child.done:
	}
}
