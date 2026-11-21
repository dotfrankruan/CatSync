package daemon

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/dotfrankruan/CatSync/internal/daemon/protocol"
	"github.com/dotfrankruan/CatSync/internal/daemon/strictjson"
	"github.com/dotfrankruan/CatSync/internal/discovery"
	"github.com/dotfrankruan/CatSync/internal/process"
)

// ControlSession is the duplex bootstrap channel plus its process lifecycle.
type ControlSession interface {
	io.ReadWriteCloser
	// Wait waits for the transport process to exit; ctx bounds the wait.
	Wait(ctx context.Context) error
}

// ControlTransport starts the bootstrap channel. The SSH implementation is
// the production path; tests provide an in-memory duplex transport.
type ControlTransport interface {
	Start(ctx context.Context) (ControlSession, error)
}

// SSHTransport starts catsyncd over one ssh process. It is the only way the
// client reaches the remote, and it is never used for data.
type SSHTransport struct {
	Program        string // ssh executable; empty means "ssh"
	User           string
	Host           string
	Port           int
	IPv6           bool
	RemoteCatsyncd string // remote executable; default "catsyncd"
	Listen         string // remote --listen value; default ":43411"
	Env            []string
	Stderr         io.Writer
	// ForwardLocalPort enables ssh TCP forwarding for the bootstrap: the
	// same ssh process also exposes 127.0.0.1:<ForwardLocalPort> and
	// forwards it to the daemon's loopback listener.
	ForwardLocalPort int
	// DaemonTransport is "tcp" (default) or "quic"; quic makes the remote
	// catsyncd bind UDP instead of TCP.
	DaemonTransport string
	// RemoteQUICQLog is an absolute remote path where catsyncd writes its
	// QUIC qlog trace. Empty disables remote qlog diagnostics.
	RemoteQUICQLog string
}

// command builds the ssh argv for this transport.
func (t SSHTransport) command() ([]string, error) {
	if t.Host == "" {
		return nil, protocol.NewError(protocol.CodeInvalidArgument, "bootstrap host is empty")
	}
	remote := t.RemoteCatsyncd
	if remote == "" {
		remote = "catsyncd"
	}
	listen := t.Listen
	if listen == "" {
		listen = ":43411"
	}
	args := []string{"-T"}
	if t.IPv6 {
		args = append(args, "-6")
	}
	if t.Port != 0 {
		args = append(args, "-p", strconv.Itoa(t.Port))
	}
	if t.ForwardLocalPort != 0 {
		if t.ForwardLocalPort < 1 || t.ForwardLocalPort > 65535 {
			return nil, protocol.NewError(protocol.CodeInvalidArgument, "forward local port %d is outside 1..65535", t.ForwardLocalPort)
		}
		host, portStr, err := net.SplitHostPort(listen)
		if err != nil || portStr == "0" || host != "127.0.0.1" {
			return nil, protocol.NewError(protocol.CodeInvalidArgument, "ssh-forward requires a loopback fixed daemon listen address, got %q", listen)
		}
		if _, err := strconv.Atoi(portStr); err != nil {
			return nil, protocol.NewError(protocol.CodeInvalidArgument, "ssh-forward daemon listen port is not numeric: %q", listen)
		}
		args = append(args, "-o", "ExitOnForwardFailure=yes",
			"-L", fmt.Sprintf("127.0.0.1:%d:127.0.0.1:%s", t.ForwardLocalPort, portStr))
	}
	remoteCmd := discovery.ShellQuote(remote) + " serve --once --listen " + discovery.ShellQuote(listen)
	if t.DaemonTransport == "quic" {
		remoteCmd += " --transport quic"
	}
	if t.RemoteQUICQLog != "" {
		if t.DaemonTransport != "quic" {
			return nil, protocol.NewError(protocol.CodeInvalidArgument, "remote QUIC qlog requires the quic daemon transport")
		}
		remoteCmd += " --quic-qlog " + discovery.ShellQuote(t.RemoteQUICQLog)
	}
	who := t.Host
	if t.User != "" {
		who = t.User + "@" + t.Host
	}
	args = append(args, who, remoteCmd)
	return args, nil
}

// Start implements ControlTransport.
func (t SSHTransport) Start(ctx context.Context) (ControlSession, error) {
	args, err := t.command()
	if err != nil {
		return nil, err
	}
	prog := t.Program
	if prog == "" {
		prog = "ssh"
	}

	clientTxR, clientTxW, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("daemon: bootstrap pipe: %w", err)
	}
	sshRxR, sshRxW, err := os.Pipe()
	if err != nil {
		clientTxR.Close()
		clientTxW.Close()
		return nil, fmt.Errorf("daemon: bootstrap pipe: %w", err)
	}
	tail := &tailWriter{w: t.Stderr, max: 8192}
	sessCtx, cancel := context.WithCancel(ctx)
	h, err := process.StartInteractive(sessCtx, process.Spec{
		Name: prog, Args: args, Env: t.Env,
		Stdin: clientTxR, Stdout: sshRxW, Stderr: tail,
	})
	if err != nil {
		cancel()
		clientTxR.Close()
		clientTxW.Close()
		sshRxR.Close()
		sshRxW.Close()
		return nil, err
	}
	clientTxR.Close()
	sshRxW.Close()
	return &sshControlSession{w: clientTxW, r: sshRxR, h: h, tail: tail, cancel: cancel}, nil
}

// sshControlSession is the ssh process plus its stdio pipes.
type sshControlSession struct {
	w      *os.File
	r      *os.File
	h      *process.Handle
	tail   *tailWriter
	cancel context.CancelFunc
	once   sync.Once
}

func (s *sshControlSession) Read(p []byte) (int, error)  { return s.r.Read(p) }
func (s *sshControlSession) Write(p []byte) (int, error) { return s.w.Write(p) }

// Close closes the channel ends; the remote command sees stdin EOF.
func (s *sshControlSession) Close() error {
	s.once.Do(func() {
		_ = s.w.Close()
		_ = s.r.Close()
	})
	return nil
}

// Wait waits for the ssh process within ctx; on timeout it cancels the
// process group and still reaps, so a stuck remote command cannot leak the
// child or block cleanup forever.
func (s *sshControlSession) Wait(ctx context.Context) error {
	select {
	case <-s.h.Exited():
	case <-ctx.Done():
		s.cancel()
	}
	return s.h.Wait()
}

// StderrTail returns captured ssh diagnostics for error classification.
func (s *sshControlSession) StderrTail() string { return s.tail.String() }

// tailWriter keeps the last max bytes and optionally forwards to a writer.
type tailWriter struct {
	mu     sync.Mutex
	w      io.Writer
	max    int
	buf    []byte
	buffer []byte
}

func (t *tailWriter) Write(p []byte) (int, error) {
	t.mu.Lock()
	t.buf = append(t.buf, p...)
	if len(t.buf) > t.max {
		t.buf = t.buf[len(t.buf)-t.max:]
	}
	t.mu.Unlock()
	if t.w != nil {
		_, _ = t.w.Write(p)
	}
	return len(p), nil
}

func (t *tailWriter) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return string(t.buf)
}

// BootstrapConfig configures one client bootstrap.
type BootstrapConfig struct {
	Transport ControlTransport
	Version   string
	Stderr    io.Writer
	// Host is the dial host for the data connection (the SSH target host).
	Host string
	// DataTransport is "tcp" (default) or "quic". QUIC uses one connection
	// and multiplexed streams; TCP keeps the per-connection TLS model.
	DataTransport string
	// QUIC receive windows override the tested defaults when non-zero.
	QUICInitialStreamWindow uint64
	QUICMaxStreamWindow     uint64
	QUICMaxConnWindow       uint64
	// DaemonAddr optionally overrides the data address as host:port.
	DaemonAddr string
	// QUICQLogPath, when set, writes a qlog trace for this QUIC connection
	// to the given local file (created with mode 0600) and enables
	// low-overhead application progress diagnostics.
	QUICQLogPath string

	HandshakeTimeout  time.Duration
	ControlTimeout    time.Duration
	HeartbeatInterval time.Duration
	CloseTimeout      time.Duration
}

// Client is one bootstrapped daemon session.
type Client struct {
	hello  serverHelloMsg
	ready  readyMsg
	sess   ControlSession
	reader *controlReader
	writer *controlWriter

	dialAddr  string
	transport string
	httpTLS   *tls.Config
	sftpTLS   *tls.Config
	execTLS   *tls.Config
	quicTLS   *tls.Config
	quicConn  *quic.Conn
	quicAct   *quicActivity
	http      *http.Client

	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	closeOne    sync.Once
	goodbyeOnce sync.Once
	closeErr    error

	mu      sync.Mutex
	byeAck  *byeAckMsg
	control error

	stderr io.Writer
}

// Bootstrap performs the full handshake and returns a ready client.
func Bootstrap(ctx context.Context, cfg BootstrapConfig) (*Client, error) {
	if cfg.Transport == nil {
		return nil, protocol.NewError(protocol.CodeInvalidArgument, "no bootstrap transport")
	}
	if cfg.HandshakeTimeout <= 0 {
		cfg.HandshakeTimeout = defaultHandshakeTimeout
	}
	if cfg.ControlTimeout <= 0 {
		cfg.ControlTimeout = defaultControlTimeout
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = defaultHeartbeat
	}
	if cfg.CloseTimeout <= 0 {
		cfg.CloseTimeout = defaultCloseTimeout
	}
	sess, err := cfg.Transport.Start(ctx)
	if err != nil {
		return nil, wrapBootstrapError(err, nil)
	}
	c := &Client{sess: sess, reader: newControlReader(sess), writer: newControlWriter(sess), stderr: cfg.Stderr}
	fail := func(err error) (*Client, error) {
		c.cleanup()
		return nil, wrapBootstrapError(err, sess)
	}

	raw, err := waitMsg(ctx, c.reader, cfg.HandshakeTimeout)
	if err != nil {
		return fail(err)
	}
	hello, ok := raw.(*serverHelloMsg)
	if !ok {
		return fail(protocol.NewError(protocol.CodeBootstrapProtocolError, "expected server_hello, got %T", raw))
	}
	if hello.Protocol != protocol.Version {
		return fail(protocol.NewError(protocol.CodeProtocolVersionMismatch, "server protocol %d, client protocol %d", hello.Protocol, protocol.Version))
	}
	serverCert, serverDER, err := parseSingleCertPEM([]byte(hello.ServerCertPEM))
	if err != nil {
		return fail(err)
	}
	if err := verifyPeerCert(serverCert, roleServer, time.Now()); err != nil {
		return fail(err)
	}
	if !validNonce(hello.ServerNonce) {
		return fail(protocol.NewError(protocol.CodeBootstrapProtocolError, "server nonce is not 32 random bytes"))
	}
	clientPair, err := newCertPair(roleClient, DefaultCertTTL)
	if err != nil {
		return fail(err)
	}
	clientNonce, err := newNonce()
	if err != nil {
		return fail(err)
	}
	if err := c.writer.send(&clientHelloMsg{
		Type: msgClientHello, Protocol: protocol.Version,
		ClientCertPEM: string(clientPair.pem), ServerNonce: hello.ServerNonce, ClientNonce: clientNonce,
	}); err != nil {
		return fail(err)
	}
	raw, err = waitMsg(ctx, c.reader, cfg.HandshakeTimeout)
	if err != nil {
		return fail(err)
	}
	ready, ok := raw.(*readyMsg)
	if !ok {
		return fail(protocol.NewError(protocol.CodeBootstrapProtocolError, "expected ready, got %T", raw))
	}
	if ready.Protocol != protocol.Version || ready.Port <= 0 || ready.Port > 65535 {
		return fail(protocol.NewError(protocol.CodeBootstrapProtocolError, "invalid ready message"))
	}
	if ready.ServerNonce != hello.ServerNonce || ready.ClientNonce != clientNonce {
		return fail(protocol.NewError(protocol.CodeBootstrapProtocolError, "ready did not bind both nonces"))
	}
	transport := cfg.DataTransport
	if transport == "" {
		transport = "tcp"
	}
	wantSession := sessionID(serverDER, clientPair.der, hello.ServerNonce, clientNonce, transport)
	if ready.SessionID != wantSession {
		return fail(protocol.NewError(protocol.CodeBootstrapProtocolError, "ready session id does not match the exchanged material"))
	}

	dialAddr := cfg.DaemonAddr
	if dialAddr == "" {
		if cfg.Host == "" {
			return fail(protocol.NewError(protocol.CodeInvalidArgument, "no daemon host to dial"))
		}
		dialAddr = net.JoinHostPort(strings.Trim(cfg.Host, "[]"), strconv.Itoa(ready.Port))
	}
	httpTLS := clientTLSConfig(clientPair, serverCert, []string{protocol.ALPNHTTP})
	sftpTLS := clientTLSConfig(clientPair, serverCert, []string{protocol.ALPNSFTP})
	execTLS := clientTLSConfig(clientPair, serverCert, []string{protocol.ALPNExec})
	if err := c.writer.send(&readyAckMsg{
		Type: msgReadyAck, Protocol: protocol.Version, SessionID: ready.SessionID,
		ServerNonce: hello.ServerNonce, ClientNonce: clientNonce,
	}); err != nil {
		return fail(err)
	}
	raw, err = waitMsg(ctx, c.reader, cfg.HandshakeTimeout)
	if err != nil {
		return fail(err)
	}
	active, ok := raw.(*activeAckMsg)
	if !ok {
		return fail(protocol.NewError(protocol.CodeBootstrapProtocolError, "expected active_ack, got %T", raw))
	}
	if active.Protocol != protocol.Version || active.SessionID != ready.SessionID {
		return fail(protocol.NewError(protocol.CodeBootstrapProtocolError, "active_ack does not match this session"))
	}

	c.hello, c.ready = *hello, *ready
	c.dialAddr, c.httpTLS, c.sftpTLS, c.execTLS = dialAddr, httpTLS, sftpTLS, execTLS
	c.transport = cfg.DataTransport
	if c.transport == "" {
		c.transport = "tcp"
	}
	if c.transport != "tcp" && c.transport != "quic" {
		return fail(protocol.NewError(protocol.CodeInvalidArgument, "unknown data transport %q", c.transport))
	}
	if c.transport == "quic" {
		qTLS := clientTLSConfig(clientPair, serverCert, []string{protocol.ALPNQUIC})
		c.quicTLS = qTLS
		c.quicAct = newQUICActivity(cfg.QUICQLogPath != "")
		qcfg := quicClientConfig(cfg)
		if cfg.QUICQLogPath != "" {
			tracer, terr := openQlogTracer(cfg.QUICQLogPath)
			if terr != nil {
				return fail(terr)
			}
			qcfg.Tracer = tracer
		}
		dialCtx, cancel := context.WithTimeout(ctx, cfg.HandshakeTimeout)
		qc, derr := quic.DialAddr(dialCtx, dialAddr, qTLS, qcfg)
		cancel()
		if derr != nil {
			return fail(derr)
		}
		c.quicConn = qc
		c.ctx, c.cancel = context.WithCancel(ctx)
		// Prove the authenticated control stream works before planning.
		if _, err := c.Stat(ctx, "/"); err != nil {
			return fail(fmt.Errorf("daemon: QUIC control precheck: %w", err))
		}
		c.startBackground(cfg)
		return c, nil
	}
	c.http = &http.Client{Transport: &http.Transport{
		DisableKeepAlives: true,
		DialContext: func(dctx context.Context, network, addr string) (net.Conn, error) {
			return dialTLS(dctx, dialAddr, httpTLS, protocol.ALPNHTTP)
		},
	}}
	// After ACTIVE (never earlier) prove one pinned data connection works
	// before any RPC or write is attempted. With ssh forwarding the local
	// forward may need a moment after active_ack, so retry within the
	// handshake budget instead of failing on the first dial.
	deadline := time.Now().Add(cfg.HandshakeTimeout)
	var conn net.Conn
	var derr error
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		conn, derr = dialTLS(attemptCtx, dialAddr, httpTLS, protocol.ALPNHTTP)
		cancel()
		if derr == nil || time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			derr = ctx.Err()
		case <-time.After(50 * time.Millisecond):
			continue
		}
		break
	}
	if derr != nil {
		return fail(derr)
	}
	_ = conn.Close()
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.startBackground(cfg)
	return c, nil
}

// startBackground runs the heartbeat and the control reader.
func (c *Client) startBackground(cfg BootstrapConfig) {
	c.wg.Add(2)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(cfg.HeartbeatInterval)
		defer ticker.Stop()
		var seq int64
		for {
			select {
			case <-c.ctx.Done():
				return
			case <-ticker.C:
				seq++
				if err := c.writer.send(&pingMsg{Type: msgPing, Seq: seq}); err != nil {
					c.cancel()
					return
				}
			}
		}
	}()
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-c.ctx.Done():
				return
			case msg := <-c.reader.msgs:
				switch m := msg.(type) {
				case *pongMsg:
				case *byeAckMsg:
					c.mu.Lock()
					ack := *m
					c.byeAck = &ack
					c.mu.Unlock()
				case *errorMsg:
					c.mu.Lock()
					c.control = protocol.NewError(m.Code, "%s", m.Message)
					c.mu.Unlock()
				}
			case err := <-c.reader.errs:
				c.mu.Lock()
				if c.control == nil && !errors.Is(err, io.EOF) {
					c.control = err
				}
				c.mu.Unlock()
				return
			}
		}
	}()
}

// dialTLS opens one pinned mTLS connection and checks ALPN.
func dialTLS(ctx context.Context, addr string, cfg *tls.Config, want string) (net.Conn, error) {
	d := &net.Dialer{}
	raw, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, protocol.NewError(protocol.CodeControlChannelLost, "dial %s: %v", addr, err)
	}
	conn := tls.Client(raw, cfg)
	if err := conn.HandshakeContext(ctx); err != nil {
		raw.Close()
		return nil, protocol.NewError(protocol.CodeTLSPeerMismatch, "data connection handshake failed: %v", err)
	}
	if got := conn.ConnectionState().NegotiatedProtocol; got != want {
		conn.Close()
		return nil, protocol.NewError(protocol.CodeTLSPeerMismatch, "data connection negotiated ALPN %q, want %q", got, want)
	}
	return conn, nil
}

// Features reports the server's advertised capabilities.
func (c *Client) Features() protocol.Features { return c.ready.Features }

// Limits reports the server's advertised budgets.
func (c *Client) Limits() protocol.Limits { return c.ready.Limits }

// Port reports the bound data port.
func (c *Client) Port() int { return c.ready.Port }

// SessionID reports the run's correlation identifier.
func (c *Client) SessionID() string { return c.ready.SessionID }

// close ensures one bounded cleanup, idempotent across failure paths.
func (c *Client) cleanup() {
	c.closeOne.Do(func() {
		if c.cancel != nil {
			c.cancel()
		}
		if c.sess != nil {
			_ = c.sess.Close()
		}
		if c.http != nil {
			if t, ok := c.http.Transport.(*http.Transport); ok {
				t.CloseIdleConnections()
			}
			c.http.CloseIdleConnections()
		}
		if c.quicConn != nil {
			logQUICDiagnostic(c.stderr, "catsync", c.quicAct, c.control)
			_ = c.quicConn.CloseWithError(0, "client closing")
		}
		if c.sess != nil {
			waitCtx, cancel := context.WithTimeout(context.Background(), defaultCloseTimeout)
			defer cancel()
			c.closeErr = c.sess.Wait(waitCtx)
		}
	})
}

// Close shuts the session down. It may be called repeatedly; all calls
// return the same result. On the normal path it first sends bye and waits
// (bounded) for the server's drained acknowledgement.
func (c *Client) Close() error {
	c.goodbyeOnce.Do(c.sendGoodbye)
	c.cleanup()
	done := make(chan struct{})
	go func() { c.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(defaultCloseTimeout):
	}
	return c.closeErr
}

// sendGoodbye asks the server to drain and waits briefly for the ack. A
// control-channel EOF is also accepted: the server may have already exited.
func (c *Client) sendGoodbye() {
	if c.ctx == nil || c.ctx.Err() != nil {
		return
	}
	if err := c.writer.send(&byeMsg{Type: msgBye}); err != nil {
		return
	}
	deadline := time.Now().Add(defaultCloseTimeout)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		ack := c.byeAck
		ctrlErr := c.control
		c.mu.Unlock()
		if ack != nil {
			return
		}
		if ctrlErr != nil {
			return
		}
		select {
		case <-c.ctx.Done():
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// wrapBootstrapError classifies a missing remote executable.
func wrapBootstrapError(err error, sess ControlSession) error {
	var pe *protocol.Error
	if errors.As(err, &pe) && pe.Code != protocol.CodeControlChannelLost && pe.Code != protocol.CodeBootstrapProtocolError {
		return err
	}
	if tailer, ok := sess.(interface{ StderrTail() string }); ok {
		tail := tailer.StderrTail()
		switch {
		case strings.Contains(tail, "command not found"),
			strings.Contains(tail, "No such file or directory"),
			strings.Contains(tail, "not found"):
			return protocol.NewError(protocol.CodeDaemonNotInstalled,
				"catsyncd is not installed or not executable on the remote host; install it yourself or pass --remote-catsyncd PATH (CatSync never installs it): %v", err)
		case strings.Contains(tail, "Permission denied"),
			strings.Contains(tail, "Too many authentication failures"),
			strings.Contains(tail, "certificate"):
			return protocol.NewError(protocol.CodeSSHAuthFailed, "ssh authentication failed: %v", err)
		}
	}
	return err
}

// post performs one JSON RPC call.
func (c *Client) post(ctx context.Context, path string, req, out any) error {
	if c.transport == "quic" {
		return c.quicPost(ctx, path, req, out)
	}
	body, err := json.Marshal(req)
	if err != nil {
		return protocol.NewError(protocol.CodeInternal, "encode request: %v", err)
	}
	url := "http://" + c.dialAddr + path
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(body)))
	if err != nil {
		return protocol.NewError(protocol.CodeInternal, "build request: %v", err)
	}
	hreq.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(hreq)
	if err != nil {
		return protocol.NewError(protocol.CodeControlChannelLost, "rpc %s: %v", path, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, protocol.MaxRPCBodyBytes))
	if err != nil {
		return protocol.NewError(protocol.CodeControlChannelLost, "read %s response: %v", path, err)
	}
	if resp.StatusCode != http.StatusOK {
		var er protocol.ErrorResponse
		if err := json.Unmarshal(data, &er); err == nil && er.Error != nil && er.Error.Code != "" {
			return er.Error
		}
		return protocol.NewError(protocol.CodeInternal, "rpc %s: HTTP %d", path, resp.StatusCode)
	}
	if out == nil {
		return nil
	}
	dec := json.NewDecoder(strings.NewReader(string(data)))
	if err := dec.Decode(out); err != nil {
		return protocol.NewError(protocol.CodeInternal, "decode %s response: %v", path, err)
	}
	return nil
}

// streamNDJSON posts a request and consumes typed NDJSON records. It returns
// only after a valid end record; a missing end is an integrity failure.
func (c *Client) streamNDJSON(ctx context.Context, path string, req any, onRecord func(kind string, line []byte) error) (int, error) {
	if c.transport == "quic" {
		st, err := c.quicStreamRequest(ctx, path, req)
		if err != nil {
			return 0, err
		}
		defer st.Close()
		return consumeNDJSON(c, newQuicStreamBody(st), path, onRecord)
	}
	body, err := json.Marshal(req)
	if err != nil {
		return 0, protocol.NewError(protocol.CodeInternal, "encode request: %v", err)
	}
	url := "http://" + c.dialAddr + path
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(body)))
	if err != nil {
		return 0, protocol.NewError(protocol.CodeInternal, "build request: %v", err)
	}
	hreq.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(hreq)
	if err != nil {
		return 0, protocol.NewError(protocol.CodeControlChannelLost, "rpc %s: %v", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, protocol.MaxRPCBodyBytes))
		var er protocol.ErrorResponse
		if err := json.Unmarshal(data, &er); err == nil && er.Error != nil && er.Error.Code != "" {
			return 0, er.Error
		}
		return 0, protocol.NewError(protocol.CodeInternal, "rpc %s: HTTP %d", path, resp.StatusCode)
	}
	return consumeNDJSON(c, resp.Body, path, onRecord)
}

// consumeNDJSON validates one NDJSON stream. It requires a final end record
// and rejects truncation, records after end, and count mismatches.
func consumeNDJSON(c *Client, body io.ReadCloser, path string, onRecord func(kind string, line []byte) error) (int, error) {
	br := bufio.NewReaderSize(body, 4096)
	count := -1
	for {
		line, readErr := readStreamLine(br)
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return 0, protocol.NewError(protocol.CodeManifestIncomplete, "%s stream ended without an end record", path)
			}
			return 0, protocol.NewError(protocol.CodeControlChannelLost, "read %s stream: %v", path, readErr)
		}
		var envelope struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(line, &envelope); err != nil {
			return 0, protocol.NewError(protocol.CodeJSONTypeMismatch, "malformed %s record: %v", path, err)
		}
		switch envelope.Type {
		case protocol.RecordTypeEnd:
			var end protocol.EndRecord
			if err := strictjson.Decode(line, &end, strictjson.Limits{MaxBytes: protocol.MaxControlLineBytes, MaxDepth: 32}); err != nil {
				return 0, err
			}
			if count < 0 {
				count = end.Count
			} else if count != end.Count {
				return 0, protocol.NewError(protocol.CodeManifestIncomplete, "%s end count mismatch", path)
			}
			// Nothing may follow the end record: a trailing entry would
			// mean the stream is not the one that was validated.
			tail, _ := io.ReadAll(io.LimitReader(br, 1024))
			if len(bytes.TrimSpace(tail)) != 0 {
				return 0, protocol.NewError(protocol.CodeManifestIncomplete, "%s produced data after the end record", path)
			}
			return end.Count, nil
		case protocol.RecordTypeError:
			var rec protocol.ErrorRecord
			if err := strictjson.Decode(line, &rec, strictjson.Limits{MaxBytes: protocol.MaxControlLineBytes, MaxDepth: 32}); err != nil {
				return 0, err
			}
			return 0, protocol.NewError(rec.Code, "%s", rec.Message)
		default:
			if count >= 0 {
				return 0, protocol.NewError(protocol.CodeManifestIncomplete, "%s produced records after end", path)
			}
			if err := onRecord(envelope.Type, line); err != nil {
				return 0, err
			}
		}
	}
}

// readStreamLine reads one NDJSON stream line with a larger bound than the
// control channel: a path may be up to MaxPathBytes and framing adds to it.
func readStreamLine(br *bufio.Reader) ([]byte, error) {
	var out []byte
	limit := protocol.MaxPathBytes + 4096
	for {
		frag, err := br.ReadSlice('\n')
		out = append(out, frag...)
		if len(out) > limit {
			return nil, protocol.NewError(protocol.CodeTooLarge, "stream line exceeds %d bytes", limit)
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

// ManifestResult is one fully validated manifest.
type ManifestResult struct {
	Mode    protocol.ManifestMode
	Entries []protocol.ManifestEntryRecord
}

// Manifest fetches and validates one manifest.
func (c *Client) Manifest(ctx context.Context, mode protocol.ManifestMode, root string) (*ManifestResult, error) {
	var res ManifestResult
	prev := ""
	sawMode := false
	count, err := c.streamNDJSON(ctx, "/v1/manifest", protocol.ManifestRequest{Root: root, Mode: mode}, func(kind string, line []byte) error {
		if !sawMode {
			var modeRec protocol.ManifestModeRecord
			if err := strictjson.Decode(line, &modeRec, strictjson.Limits{MaxBytes: protocol.MaxControlLineBytes, MaxDepth: 32}); err != nil {
				return err
			}
			if modeRec.Mode != mode {
				return protocol.NewError(protocol.CodeManifestIncomplete, "server echoed mode %q, want %q", modeRec.Mode, mode)
			}
			res.Mode = modeRec.Mode
			sawMode = true
			return nil
		}
		var entry protocol.ManifestEntryRecord
		if err := strictjson.Decode(line, &entry, strictjson.Limits{MaxBytes: protocol.MaxControlLineBytes, MaxDepth: 32}); err != nil {
			return err
		}
		if entry.Kind != protocol.KindFile && entry.Kind != protocol.KindSymlink && entry.Kind != protocol.KindSpecial {
			return protocol.NewError(protocol.CodeManifestIncomplete, "invalid entry kind %q", entry.Kind)
		}
		if entry.Path <= prev {
			return protocol.NewError(protocol.CodeManifestIncomplete, "manifest paths are not strictly increasing at %q", entry.Path)
		}
		prev = entry.Path
		res.Entries = append(res.Entries, entry)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !sawMode {
		return nil, protocol.NewError(protocol.CodeManifestIncomplete, "manifest has no mode record")
	}
	if count != len(res.Entries) {
		return nil, protocol.NewError(protocol.CodeManifestIncomplete, "manifest end count %d does not match %d entries", count, len(res.Entries))
	}
	return &res, nil
}

// Mkdir ensures every directory exists.
func (c *Client) Mkdir(ctx context.Context, dirs []string) (int, error) {
	var out protocol.MkdirResponse
	if err := c.post(ctx, "/v1/mkdir", protocol.MkdirRequest{Dirs: dirs}, &out); err != nil {
		return 0, err
	}
	return out.Created, nil
}

// Metadata applies mode bits and an exact nanosecond mtime.
func (c *Client) Metadata(ctx context.Context, path string, mode uint32, mtimeNS int64) error {
	return c.MetadataMtime(ctx, path, &mode, mtimeNS)
}

// MetadataMtime applies an exact nanosecond mtime; nil mode leaves the
// permissions untouched.
func (c *Client) MetadataMtime(ctx context.Context, path string, mode *uint32, mtimeNS int64) error {
	var out protocol.MetadataResponse
	if err := c.post(ctx, "/v1/metadata", protocol.MetadataRequest{Path: path, Mode: mode, MtimeNS: mtimeNS}, &out); err != nil {
		return err
	}
	if !out.OK {
		return protocol.NewError(protocol.CodeInternal, "metadata was not applied")
	}
	return nil
}

// Hash fetches SHA-256 for the given paths, requiring a complete stream.
func (c *Client) Hash(ctx context.Context, paths []string) (map[string]string, error) {
	out := make(map[string]string, len(paths))
	prev := ""
	count, err := c.streamNDJSON(ctx, "/v1/hash", protocol.HashRequest{Paths: paths}, func(kind string, line []byte) error {
		var rec protocol.HashRecord
		if err := strictjson.Decode(line, &rec, strictjson.Limits{MaxBytes: protocol.MaxControlLineBytes, MaxDepth: 32}); err != nil {
			return err
		}
		if len(rec.SHA256) != 64 {
			return protocol.NewError(protocol.CodeHashIncomplete, "hash for %q is not 64 hex characters", rec.Path)
		}
		if rec.Path <= prev {
			return protocol.NewError(protocol.CodeHashIncomplete, "hash paths are not strictly increasing at %q", rec.Path)
		}
		prev = rec.Path
		if _, dup := out[rec.Path]; dup {
			return protocol.NewError(protocol.CodeHashIncomplete, "duplicate hash for %q", rec.Path)
		}
		out[rec.Path] = rec.SHA256
		return nil
	})
	if err != nil {
		// A stream that ends without its end record is a hash integrity
		// failure, never a partial success.
		if strings.Contains(err.Error(), protocol.CodeManifestIncomplete) {
			return nil, protocol.NewError(protocol.CodeHashIncomplete, "hash stream is incomplete: %v", err)
		}
		return nil, err
	}
	if count != len(paths) || len(out) != len(paths) {
		return nil, protocol.NewError(protocol.CodeHashIncomplete, "hash end count %d does not match %d paths", count, len(paths))
	}
	return out, nil
}

// Stat returns Lstat metadata for one path.
func (c *Client) Stat(ctx context.Context, path string) (protocol.StatResponse, error) {
	var out protocol.StatResponse
	err := c.post(ctx, "/v1/stat", protocol.StatRequest{Path: path}, &out)
	return out, err
}

// SameFile reports whether both paths resolve to one inode.
func (c *Client) SameFile(ctx context.Context, a, b string) (bool, error) {
	var out protocol.SameFileResponse
	if err := c.post(ctx, "/v1/samefile", protocol.SameFileRequest{A: a, B: b}, &out); err != nil {
		return false, err
	}
	return out.Same, nil
}

// DialSFTP opens one SFTP data connection with the negotiated sftp ALPN.
// DialSFTP opens one independent SFTP data stream. TCP uses a fresh pinned
// TLS connection; QUIC opens a stream on the single run connection.
func (c *Client) DialSFTP(ctx context.Context) (io.ReadWriteCloser, error) {
	if c.transport == "quic" {
		return c.openQUICStream(ctx, protocol.StreamSFTP)
	}
	return dialTLS(ctx, c.dialAddr, c.sftpTLS, protocol.ALPNSFTP)
}
