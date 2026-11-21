package daemon

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/dotfrankruan/CatSync/internal/daemon/protocol"
	"github.com/dotfrankruan/CatSync/internal/daemon/strictjson"
)

// encodeExecStart renders one strict JSON start line (newline included).
func encodeExecStart(start protocol.ExecStart) ([]byte, error) {
	return strictjson.Encode(start)
}

// ExecSession is one framed rsync server process on the remote side. Stdin
// is the rsync client->server protocol stream; Stdout is the server->client
// protocol stream; Stderr carries diagnostics; Wait reports the remote exit.
type ExecSession struct {
	rw     io.ReadWriteCloser
	wmu    sync.Mutex
	closed bool

	stdoutR *io.PipeReader
	stdoutW *io.PipeWriter
	stderrR *io.PipeReader
	stderrW *io.PipeWriter

	exitOnce sync.Once
	exitCh   chan execResult
	closeOne sync.Once
}

type execResult struct {
	code int
	err  error
}

// Exec starts one remote rsync server through the pinned exec ALPN and
// returns a duplex session. Only rsync is accepted by the server.
func (c *Client) Exec(ctx context.Context, program string, args []string) (*ExecSession, error) {
	if c.transport == "quic" {
		return c.execQUIC(ctx, program, args)
	}
	if c.execTLS == nil {
		return nil, protocol.NewError(protocol.CodeFeatureUnavailable, "exec is not available")
	}
	conn, err := dialTLS(ctx, c.dialAddr, c.execTLS, protocol.ALPNExec)
	if err != nil {
		return nil, err
	}
	line, err := encodeExecStart(protocol.ExecStart{Type: "start", Program: program, Args: args})
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if dl, ok := conn.(interface{ SetWriteDeadline(time.Time) error }); ok {
		_ = dl.SetWriteDeadline(time.Now().Add(30 * time.Second))
	}
	if _, err := conn.Write(line); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("daemon: send exec start: %w", err)
	}
	if dl, ok := conn.(interface{ SetWriteDeadline(time.Time) error }); ok {
		_ = dl.SetWriteDeadline(time.Time{})
	}
	return newExecSession(conn), nil
}

// newExecSession starts the frame reader for one exec stream.
func newExecSession(rw io.ReadWriteCloser) *ExecSession {
	stdoutR, stdoutW := io.Pipe()
	stderrR, stderrW := io.Pipe()
	s := &ExecSession{
		rw: rw, stdoutR: stdoutR, stdoutW: stdoutW, stderrR: stderrR, stderrW: stderrW,
		exitCh: make(chan execResult, 1),
	}
	go s.readLoop()
	return s
}

func (s *ExecSession) readLoop() {
	br := bufio.NewReaderSize(s.rw, 16*1024)
	for {
		typ, payload, err := readExecFrame(br)
		if err != nil {
			s.finish(execResult{err: fmt.Errorf("daemon: exec stream: %w", err)})
			return
		}
		switch typ {
		case protocol.ExecFrameStdout:
			_, _ = s.stdoutW.Write(payload)
		case protocol.ExecFrameStderr:
			_, _ = s.stderrW.Write(payload)
		case protocol.ExecFrameExit:
			code, perr := protocol.ParseExecExitPayload(payload)
			s.finish(execResult{code: code, err: perr})
			return
		case protocol.ExecFrameError:
			code, msg := parseExecError(payload)
			s.finish(execResult{err: protocol.NewError(code, "%s", msg)})
			return
		default:
			s.finish(execResult{err: protocol.NewError(protocol.CodeExecRejected, "unexpected exec frame type %d", typ)})
			return
		}
	}
}

func (s *ExecSession) finish(res execResult) {
	s.exitOnce.Do(func() {
		_ = s.stdoutW.Close()
		_ = s.stderrW.Close()
		s.exitCh <- res
		close(s.exitCh)
	})
}

func parseExecError(payload []byte) (string, string) {
	code, msg := "", string(payload)
	if i := strings.IndexByte(msg, '\x00'); i >= 0 {
		code, msg = msg[:i], msg[i+1:]
	}
	if code == "" {
		code = protocol.CodeRemoteProcessFailed
	}
	return code, msg
}

// Stdin returns the remote stdin. Writes are framed and bounded.
func (s *ExecSession) Stdin() io.Writer { return execStdin{s: s} }

// CloseStdin sends the stdin EOF frame (remote rsync sees end of input).
func (s *ExecSession) CloseStdin() error {
	return s.writeFrame(protocol.ExecFrameStdinEOF, nil)
}

// Stdout returns the remote stdout stream (rsync protocol data).
func (s *ExecSession) Stdout() io.Reader { return s.stdoutR }

// Stderr returns the remote stderr stream (diagnostics).
func (s *ExecSession) Stderr() io.Reader { return s.stderrR }

func (s *ExecSession) writeFrame(typ byte, payload []byte) error {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if s.closed {
		return net.ErrClosed
	}
	if len(payload) > protocol.MaxExecFrameBytes {
		return protocol.NewError(protocol.CodeTooLarge, "exec frame too large")
	}
	var hdr [5]byte
	hdr[0] = typ
	binary.BigEndian.PutUint32(hdr[1:], uint32(len(payload)))
	if _, err := s.rw.Write(hdr[:]); err != nil {
		s.closed = true
		return err
	}
	if len(payload) > 0 {
		if _, err := s.rw.Write(payload); err != nil {
			s.closed = true
			return err
		}
	}
	return nil
}

// Wait waits for the remote process exit. Cancellation sends a cancel frame
// and closes the connection; the error is never reported as success.
func (s *ExecSession) Wait(ctx context.Context) (int, error) {
	select {
	case res, ok := <-s.exitCh:
		if !ok {
			return 0, protocol.NewError(protocol.CodeControlChannelLost, "exec stream closed without an exit record")
		}
		return res.code, res.err
	case <-ctx.Done():
		_ = s.writeFrame(protocol.ExecFrameCancel, nil)
		s.Close()
		return 0, ctx.Err()
	}
}

// Close tears down the exec stream. It is idempotent.
func (s *ExecSession) Close() {
	s.closeOne.Do(func() {
		_ = s.writeFrame(protocol.ExecFrameCancel, nil)
		_ = s.rw.Close()
	})
}

type execStdin struct{ s *ExecSession }

func (w execStdin) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		n := len(p)
		if n > 32*1024 {
			n = 32 * 1024
		}
		if err := w.s.writeFrame(protocol.ExecFrameStdin, p[:n]); err != nil {
			return written, err
		}
		written += n
		p = p[n:]
	}
	return written, nil
}

var _ = errors.Is
