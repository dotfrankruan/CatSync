package daemon

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/dotfrankruan/CatSync/internal/daemon/protocol"
	"github.com/dotfrankruan/CatSync/internal/daemon/strictjson"
)

// Control message types exchanged on the SSH bootstrap channel.
const (
	msgServerHello = "server_hello"
	msgClientHello = "client_hello"
	msgReady       = "ready"
	msgReadyAck    = "ready_ack"
	msgActiveAck   = "active_ack"
	msgPing        = "ping"
	msgPong        = "pong"
	msgBye         = "bye"
	msgByeAck      = "bye_ack"
	msgError       = "error"
)

// Control channel defaults.
const (
	defaultHandshakeTimeout = 10 * time.Second
	defaultControlTimeout   = 30 * time.Second
	defaultHeartbeat        = 5 * time.Second
	defaultDrainTimeout     = 30 * time.Second
	defaultIdleTimeout      = 15 * time.Minute
	defaultCloseTimeout     = 10 * time.Second
)

type serverHelloMsg struct {
	Type          string            `json:"type"`
	Protocol      int               `json:"protocol"`
	Version       string            `json:"version"`
	ServerCertPEM string            `json:"server_cert_pem"`
	ServerNonce   string            `json:"server_nonce"`
	RequestedPort int               `json:"requested_port"`
	Features      protocol.Features `json:"features"`
	Limits        protocol.Limits   `json:"limits"`
}

type clientHelloMsg struct {
	Type          string `json:"type"`
	Protocol      int    `json:"protocol"`
	ClientCertPEM string `json:"client_cert_pem"`
	ServerNonce   string `json:"server_nonce"`
	ClientNonce   string `json:"client_nonce"`
}

type readyMsg struct {
	Type        string            `json:"type"`
	Protocol    int               `json:"protocol"`
	Port        int               `json:"port"`
	ServerNonce string            `json:"server_nonce"`
	ClientNonce string            `json:"client_nonce"`
	SessionID   string            `json:"session_id"`
	Features    protocol.Features `json:"features"`
	Limits      protocol.Limits   `json:"limits"`
}

type readyAckMsg struct {
	Type        string `json:"type"`
	Protocol    int    `json:"protocol"`
	SessionID   string `json:"session_id"`
	ServerNonce string `json:"server_nonce"`
	ClientNonce string `json:"client_nonce"`
}

type activeAckMsg struct {
	Type      string `json:"type"`
	Protocol  int    `json:"protocol"`
	SessionID string `json:"session_id"`
}

type pingMsg struct {
	Type string `json:"type"`
	Seq  int64  `json:"seq"`
}

type pongMsg struct {
	Type string `json:"type"`
	Seq  int64  `json:"seq"`
}

type byeMsg struct {
	Type string `json:"type"`
}

type byeAckMsg struct {
	Type       string `json:"type"`
	Drained    bool   `json:"drained"`
	RPCActive  int    `json:"rpc_active"`
	SFTPConns  int    `json:"sftp_conns"`
	ExecActive int    `json:"exec_active"`
	Children   int    `json:"children"`
}

type errorMsg struct {
	Type    string `json:"type"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// newNonce returns a fresh 32-byte nonce in base64.
func newNonce() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("daemon: generate nonce: %w", err)
	}
	return base64.StdEncoding.EncodeToString(b[:]), nil
}

// validNonce reports whether s is a base64-encoded 32-byte value.
func validNonce(s string) bool {
	b, err := base64.StdEncoding.DecodeString(s)
	return err == nil && len(b) == 32
}

// sessionID binds the protocol version, both certificates, both nonces, and
// the transport name. It is a log/correlation identifier, never a credential.
func sessionID(serverDER, clientDER []byte, serverNonce, clientNonce, transport string) string {
	h := sha256.New()
	fmt.Fprintf(h, "catsync-session\x00%d\x00", protocol.Version)
	h.Write(serverDER)
	h.Write([]byte{0})
	h.Write(clientDER)
	h.Write([]byte{0})
	h.Write([]byte(serverNonce))
	h.Write([]byte{0})
	h.Write([]byte(clientNonce))
	h.Write([]byte{0})
	h.Write([]byte(transport))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// controlWriter serializes control messages onto one stream.
type controlWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func newControlWriter(w io.Writer) *controlWriter { return &controlWriter{w: w} }

func (cw *controlWriter) send(v any) error {
	b, err := strictjson.Encode(v)
	if err != nil {
		return err
	}
	if len(b) > protocol.MaxControlLineBytes {
		return protocol.NewError(protocol.CodeJSONTooLarge, "control message is %d bytes", len(b))
	}
	cw.mu.Lock()
	defer cw.mu.Unlock()
	_, err = cw.w.Write(b)
	return err
}

// controlReader parses JSONL control messages from the SSH channel.
type controlReader struct {
	msgs chan any
	errs chan error
}

func newControlReader(r io.Reader) *controlReader {
	cr := &controlReader{msgs: make(chan any, 16), errs: make(chan error, 1)}
	go cr.loop(r)
	return cr
}

func (cr *controlReader) loop(r io.Reader) {
	br := bufio.NewReaderSize(r, 4096)
	for {
		line, err := readControlLine(br)
		if err != nil {
			cr.errs <- err
			return
		}
		msg, derr := decodeControlLine(line)
		if derr != nil {
			cr.errs <- derr
			return
		}
		cr.msgs <- msg
	}
}

// readControlLine reads one newline-terminated line with a hard size bound.
func readControlLine(br *bufio.Reader) ([]byte, error) {
	var out []byte
	for {
		frag, err := br.ReadSlice('\n')
		out = append(out, frag...)
		if len(out) > protocol.MaxControlLineBytes {
			return nil, protocol.NewError(protocol.CodeJSONTooLarge, "control line exceeds %d bytes", protocol.MaxControlLineBytes)
		}
		switch {
		case err == nil:
			return trimNewline(out), nil
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case errors.Is(err, io.EOF):
			return nil, io.EOF
		default:
			return nil, err
		}
	}
}

func trimNewline(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == '\r') {
		b = b[:len(b)-1]
	}
	return b
}

// decodeControlLine dispatches one line to its typed message.
func decodeControlLine(line []byte) (any, error) {
	var envelope struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(line, &envelope); err != nil {
		return nil, protocol.NewError(protocol.CodeBootstrapProtocolError, "control message is not JSON: %v", err)
	}
	lim := strictjson.DefaultLimits
	switch envelope.Type {
	case msgServerHello:
		var m serverHelloMsg
		return &m, strictjson.Decode(line, &m, lim)
	case msgClientHello:
		var m clientHelloMsg
		return &m, strictjson.Decode(line, &m, lim)
	case msgReady:
		var m readyMsg
		return &m, strictjson.Decode(line, &m, lim)
	case msgReadyAck:
		var m readyAckMsg
		return &m, strictjson.Decode(line, &m, lim)
	case msgActiveAck:
		var m activeAckMsg
		return &m, strictjson.Decode(line, &m, lim)
	case msgPing:
		var m pingMsg
		return &m, strictjson.Decode(line, &m, lim)
	case msgPong:
		var m pongMsg
		return &m, strictjson.Decode(line, &m, lim)
	case msgBye:
		var m byeMsg
		return &m, strictjson.Decode(line, &m, lim)
	case msgByeAck:
		var m byeAckMsg
		return &m, strictjson.Decode(line, &m, lim)
	case msgError:
		var m errorMsg
		return &m, strictjson.Decode(line, &m, lim)
	case "":
		return nil, protocol.NewError(protocol.CodeBootstrapProtocolError, "control message has no type")
	default:
		return nil, protocol.NewError(protocol.CodeBootstrapProtocolError, "unknown control message type %q", envelope.Type)
	}
}

// waitMsg waits for the next control message until timeout.
func waitMsg(ctx context.Context, cr *controlReader, timeout time.Duration) (any, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case msg := <-cr.msgs:
		return msg, nil
	case err := <-cr.errs:
		if errors.Is(err, io.EOF) {
			return nil, protocol.NewError(protocol.CodeControlChannelLost, "bootstrap control channel closed")
		}
		return nil, err
	case <-timer.C:
		return nil, protocol.NewError(protocol.CodeBootstrapProtocolError, "timed out waiting for the next control message")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// controlError renders a coded control error message.
func controlError(code, format string, args ...any) *errorMsg {
	return &errorMsg{Type: msgError, Code: code, Message: fmt.Sprintf(format, args...)}
}

// protocolErr converts any error into the closest control error code.
func protocolErr(err error) *errorMsg {
	if err == nil {
		return nil
	}
	var pe *protocol.Error
	if errors.As(err, &pe) {
		return controlError(pe.Code, "%s", pe.Message)
	}
	if strings.Contains(err.Error(), "timed out") {
		return controlError(protocol.CodeBootstrapProtocolError, "%v", err)
	}
	return controlError(protocol.CodeBootstrapProtocolError, "%v", err)
}
