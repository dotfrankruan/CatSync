package daemon

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"github.com/quic-go/quic-go"

	"github.com/dotfrankruan/CatSync/internal/daemon/protocol"
)

// quicClientConfig builds the client QUIC transport parameters. The receive
// windows govern server->client downloads; they are deliberately generous
// because a large file may be streamed through one QUIC stream.
func quicClientConfig(cfg BootstrapConfig) *quic.Config {
	initial := cfg.QUICInitialStreamWindow
	if initial == 0 {
		initial = 4 << 20
	}
	maxStream := cfg.QUICMaxStreamWindow
	if maxStream == 0 {
		maxStream = 16 << 20
	}
	maxConn := cfg.QUICMaxConnWindow
	if maxConn == 0 {
		maxConn = 64 << 20
	}
	return &quic.Config{
		// The server never opens streams toward the client.
		MaxIncomingStreams:             -1,
		MaxIncomingUniStreams:          -1,
		Allow0RTT:                      false,
		HandshakeIdleTimeout:           cfg.HandshakeTimeout,
		MaxIdleTimeout:                 2 * cfg.ControlTimeout,
		InitialStreamReceiveWindow:     initial,
		MaxStreamReceiveWindow:         maxStream,
		InitialConnectionReceiveWindow: initial,
		MaxConnectionReceiveWindow:     maxConn,
	}
}

// quicByteStream adapts a quic.Stream to io.ReadWriteCloser for
// sftp.NewClientPipe and the exec framing. Close completes both halves.
type quicByteStream struct {
	st  *quic.Stream
	typ byte
	act *quicActivity
}

func (q *quicByteStream) Read(p []byte) (int, error) {
	n, err := q.st.Read(p)
	q.act.touch(q.typ, n)
	return n, err
}

func (q *quicByteStream) Write(p []byte) (int, error) {
	n, err := q.st.Write(p)
	q.act.touch(q.typ, n)
	return n, err
}
func (q *quicByteStream) Close() error {
	q.st.CancelRead(0)
	return q.st.Close()
}

// openQUICStream opens one bidirectional stream and writes its type header.
func (c *Client) openQUICStream(ctx context.Context, typ byte) (*quicByteStream, error) {
	if c.quicConn == nil {
		return nil, protocol.NewError(protocol.CodeControlChannelLost, "QUIC connection is closed")
	}
	st, err := c.quicConn.OpenStreamSync(ctx)
	if err != nil {
		return nil, protocol.NewError(protocol.CodeControlChannelLost, "open QUIC stream: %v", err)
	}
	if _, err := st.Write([]byte{typ}); err != nil {
		st.CancelRead(0)
		_ = st.Close()
		return nil, protocol.NewError(protocol.CodeControlChannelLost, "write QUIC stream header: %v", err)
	}
	return &quicByteStream{st: st, typ: typ, act: c.quicAct}, nil
}

// quicPost performs one non-streaming CONTROL RPC.
func (c *Client) quicPost(ctx context.Context, path string, req, out any) error {
	st, err := c.quicStreamRequest(ctx, path, req)
	if err != nil {
		return err
	}
	defer st.Close()
	// Non-stream response: 4-byte length + JSON body after the status byte
	// (already consumed by quicStreamRequest).
	var hdr [4]byte
	if _, err := io.ReadFull(st, hdr[:]); err != nil {
		return protocol.NewError(protocol.CodeControlChannelLost, "read %s response: %v", path, err)
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 || n > protocol.MaxQUICControlFrameBytes {
		return protocol.NewError(protocol.CodeTooLarge, "rpc %s response length %d is invalid", path, n)
	}
	data := make([]byte, n)
	if _, err := io.ReadFull(st, data); err != nil {
		return protocol.NewError(protocol.CodeControlChannelLost, "read %s response body: %v", path, err)
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return protocol.NewError(protocol.CodeInternal, "decode %s response: %v", path, err)
	}
	return nil
}

// quicStreamRequest sends a CONTROL request and returns the stream position
// after a successful status byte.
func (c *Client) quicStreamRequest(ctx context.Context, path string, req any) (io.ReadWriteCloser, error) {
	if c.quicConn == nil {
		return nil, protocol.NewError(protocol.CodeControlChannelLost, "QUIC connection is closed")
	}
	st, err := c.quicConn.OpenStreamSync(ctx)
	if err != nil {
		return nil, protocol.NewError(protocol.CodeControlChannelLost, "open control stream: %v", err)
	}
	if _, err := st.Write([]byte{protocol.StreamControl}); err != nil {
		st.CancelRead(0)
		_ = st.Close()
		return nil, protocol.NewError(protocol.CodeControlChannelLost, "write control stream header: %v", err)
	}
	body, err := json.Marshal(req)
	if err != nil {
		st.CancelRead(0)
		_ = st.Close()
		return nil, protocol.NewError(protocol.CodeInternal, "encode request: %v", err)
	}
	payload, err := json.Marshal(protocol.QUICControlRequest{Path: path, Body: body})
	if err != nil {
		st.CancelRead(0)
		_ = st.Close()
		return nil, protocol.NewError(protocol.CodeInternal, "encode control frame: %v", err)
	}
	if len(payload) > protocol.MaxQUICControlFrameBytes {
		st.CancelRead(0)
		_ = st.Close()
		return nil, protocol.NewError(protocol.CodeTooLarge, "control frame exceeds %d bytes", protocol.MaxQUICControlFrameBytes)
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)))
	if _, err := st.Write(hdr[:]); err != nil {
		st.CancelRead(0)
		_ = st.Close()
		return nil, protocol.NewError(protocol.CodeControlChannelLost, "write control frame: %v", err)
	}
	if _, err := st.Write(payload); err != nil {
		st.CancelRead(0)
		_ = st.Close()
		return nil, protocol.NewError(protocol.CodeControlChannelLost, "write control frame: %v", err)
	}
	// The request is complete; only the response direction remains.
	if err := st.Close(); err != nil {
		st.CancelRead(0)
		return nil, protocol.NewError(protocol.CodeControlChannelLost, "close control write side: %v", err)
	}
	var status [1]byte
	if _, err := io.ReadFull(st, status[:]); err != nil {
		st.CancelRead(0)
		return nil, protocol.NewError(protocol.CodeControlChannelLost, "read %s status: %v", path, err)
	}
	if status[0] == protocol.QUICControlStatusError {
		var hdr [4]byte
		if _, err := io.ReadFull(st, hdr[:]); err != nil {
			st.CancelRead(0)
			return nil, protocol.NewError(protocol.CodeControlChannelLost, "read %s error: %v", path, err)
		}
		n := binary.BigEndian.Uint32(hdr[:])
		if n == 0 || n > protocol.MaxQUICControlFrameBytes {
			st.CancelRead(0)
			return nil, protocol.NewError(protocol.CodeInternal, "rpc %s error frame is invalid", path)
		}
		data := make([]byte, n)
		if _, err := io.ReadFull(st, data); err != nil {
			st.CancelRead(0)
			return nil, protocol.NewError(protocol.CodeControlChannelLost, "read %s error body: %v", path, err)
		}
		st.CancelRead(0)
		var er protocol.ErrorResponse
		if err := json.Unmarshal(data, &er); err == nil && er.Error != nil && er.Error.Code != "" {
			return nil, er.Error
		}
		return nil, protocol.NewError(protocol.CodeInternal, "rpc %s failed", path)
	}
	if status[0] != protocol.QUICControlStatusOK {
		st.CancelRead(0)
		return nil, protocol.NewError(protocol.CodeInternal, "rpc %s returned unknown status %d", path, status[0])
	}
	return st, nil
}

// quicStreamBody adapts a control stream to io.ReadCloser for NDJSON.
type quicStreamBody struct{ st *quic.Stream }

func (q *quicStreamBody) Read(p []byte) (int, error) { return q.st.Read(p) }
func (q *quicStreamBody) Close() error {
	q.st.CancelRead(0)
	return nil
}

func newQuicStreamBody(rw io.ReadWriteCloser) io.ReadCloser {
	return &genericReadCloser{r: rw}
}

type genericReadCloser struct{ r io.ReadWriteCloser }

func (g *genericReadCloser) Read(p []byte) (int, error) { return g.r.Read(p) }
func (g *genericReadCloser) Close() error               { return g.r.Close() }

// ExecQUIC starts one remote rsync server on a fresh EXEC stream. The
// existing M3 framing and validation are reused unchanged.
func (c *Client) execQUIC(ctx context.Context, program string, args []string) (*ExecSession, error) {
	if c.quicConn == nil {
		return nil, protocol.NewError(protocol.CodeControlChannelLost, "QUIC connection is closed")
	}
	st, err := c.quicConn.OpenStreamSync(ctx)
	if err != nil {
		return nil, protocol.NewError(protocol.CodeControlChannelLost, "open exec stream: %v", err)
	}
	if _, err := st.Write([]byte{protocol.StreamExec}); err != nil {
		st.CancelRead(0)
		_ = st.Close()
		return nil, protocol.NewError(protocol.CodeControlChannelLost, "write exec stream header: %v", err)
	}
	line, err := strictjsonEncodeExec(program, args)
	if err != nil {
		st.CancelRead(0)
		_ = st.Close()
		return nil, err
	}
	if _, err := st.Write(line); err != nil {
		st.CancelRead(0)
		_ = st.Close()
		return nil, fmt.Errorf("daemon: send exec start: %w", err)
	}
	return newExecSession(&quicByteStream{st: st}), nil
}

func strictjsonEncodeExec(program string, args []string) ([]byte, error) {
	// strictjson.Encode appends the newline that terminates the start line.
	return encodeExecStart(protocol.ExecStart{Type: "start", Program: program, Args: args})
}
