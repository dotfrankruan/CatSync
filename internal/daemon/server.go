package daemon

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dotfrankruan/CatSync/internal/daemon/protocol"
)

// ServerConfig is one one-shot server invocation.
type ServerConfig struct {
	// Listen is the TCP listen address, for example ":43411" or
	// "127.0.0.1:0". The actual port is published in ready.
	Listen string
	// RequestedPort is the configured port for the server_hello record. It
	// is not authoritative: only ready.port comes from the bound socket.
	RequestedPort int
	// SFTPPath is the resolved sftp-server executable; empty disables the
	// SFTP capability.
	SFTPPath string
	// Limits bounds concurrent work. Zero fields default.
	Limits protocol.Limits
	// Manifest bounds traversal memory and stream size; zero fields default.
	Manifest manifestConfig
	// CertTTL is the ephemeral certificate lifetime.
	CertTTL time.Duration
	// Timeouts. Zero fields default.
	IdleTimeout      time.Duration
	DrainTimeout     time.Duration
	ControlTimeout   time.Duration
	HandshakeTimeout time.Duration
	// Version is the catsyncd version reported in server_hello/probe.
	Version string
	// OnShutdown, when set, receives the final counters after DRAINING.
	OnShutdown func(ShutdownStats)
	// Stdin/Stdout carry the SSH bootstrap control protocol; Stderr is
	// diagnostics only.
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

func (c *ServerConfig) applyDefaults() {
	if c.Listen == "" {
		c.Listen = ":43411"
	}
	if c.RequestedPort < 0 || c.RequestedPort > 65535 {
		c.RequestedPort = 0
	}
	if c.CertTTL <= 0 {
		c.CertTTL = DefaultCertTTL
	}
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = defaultIdleTimeout
	}
	if c.DrainTimeout <= 0 {
		c.DrainTimeout = defaultDrainTimeout
	}
	if c.ControlTimeout <= 0 {
		c.ControlTimeout = defaultControlTimeout
	}
	if c.HandshakeTimeout <= 0 {
		c.HandshakeTimeout = defaultHandshakeTimeout
	}
	if c.Limits.MaxSFTPConns <= 0 {
		c.Limits.MaxSFTPConns = 64
	}
	if c.Limits.MaxExecConns <= 0 {
		c.Limits.MaxExecConns = 64
	}
	if c.Limits.MaxControlConns <= 0 {
		c.Limits.MaxControlConns = 16
	}
	if c.Limits.MaxPendingHandshake <= 0 {
		c.Limits.MaxPendingHandshake = 64
	}
	if c.Limits.MaxConns <= 0 {
		c.Limits.MaxConns = 256
	}
}

// features reports only capabilities this build actually provides.
func (c *ServerConfig) features() protocol.Features {
	return protocol.Features{
		SFTP:     c.SFTPPath != "",
		Exec:     false, // exec is not implemented in M1
		Manifest: true,
		Metadata: true,
		Hash:     true,
		Stat:     true,
		SameFile: true,
	}
}

// counters are the independent resource counters used by DRAINING and
// bye_ack. Transparent SFTP forwarding is counted as a connection, never as
// a file-level operation.
type counters struct {
	rpcActive    atomic.Int64
	sftpConns    atomic.Int64
	execActive   atomic.Int64
	children     atomic.Int64
	alpnRejected atomic.Int64
}

// server is one serving listener after bootstrap.
type server struct {
	cfg       ServerConfig
	ctx       context.Context
	cancel    context.CancelFunc
	tlsConfig *tls.Config
	listener  net.Listener
	http      *http.Server
	httpLn    *chanListener
	counters  counters

	pendingSem   chan struct{}
	mu           sync.Mutex
	conns        map[*trackedConn]struct{}
	pending      map[net.Conn]struct{}
	children     map[*childProcess]struct{}
	closedOnce   sync.Once
	shutdownOnce sync.Once
	done         chan struct{}
	final        countsSnapshot
	drained      bool
}

// Serve runs the one-shot bootstrap and, on success, serves until bye, a
// lost control channel, or cancellation. It returns the process exit code.
func Serve(ctx context.Context, cfg ServerConfig) int {
	cfg.applyDefaults()
	writer := newControlWriter(cfg.Stdout)
	logf := func(format string, args ...any) {
		if cfg.Stderr != nil {
			fmt.Fprintf(cfg.Stderr, "catsyncd: "+format+"\n", args...)
		}
	}
	reader := newControlReader(cfg.Stdin)

	serverPair, err := newCertPair(roleServer, cfg.CertTTL)
	if err != nil {
		logf("generate identity: %v", err)
		return 1
	}
	serverNonce, err := newNonce()
	if err != nil {
		logf("generate nonce: %v", err)
		return 1
	}
	hello := &serverHelloMsg{
		Type: msgServerHello, Protocol: protocol.Version, Version: cfg.Version,
		ServerCertPEM: string(serverPair.pem), ServerNonce: serverNonce,
		RequestedPort: cfg.RequestedPort,
		Features:      cfg.features(), Limits: cfg.Limits,
	}
	if err := writer.send(hello); err != nil {
		logf("send server_hello: %v", err)
		return 1
	}

	raw, err := waitMsg(ctx, reader, defaultHandshakeTimeout)
	if err != nil {
		return bootstrapFailure(writer, logf, err)
	}
	clientHello, ok := raw.(*clientHelloMsg)
	if !ok {
		return bootstrapFailure(writer, logf, protocol.NewError(protocol.CodeBootstrapProtocolError, "expected client_hello, got %T", raw))
	}
	if clientHello.Protocol != protocol.Version {
		return bootstrapFailure(writer, logf, protocol.NewError(protocol.CodeProtocolVersionMismatch, "client protocol %d, server protocol %d", clientHello.Protocol, protocol.Version))
	}
	if clientHello.ServerNonce != serverNonce {
		return bootstrapFailure(writer, logf, protocol.NewError(protocol.CodeBootstrapProtocolError, "client hello did not echo the server nonce"))
	}
	if !validNonce(clientHello.ClientNonce) {
		return bootstrapFailure(writer, logf, protocol.NewError(protocol.CodeBootstrapProtocolError, "client nonce is not 32 random bytes"))
	}
	clientCert, clientDER, err := parseSingleCertPEM([]byte(clientHello.ClientCertPEM))
	if err != nil {
		return bootstrapFailure(writer, logf, err)
	}
	if err := verifyPeerCert(clientCert, roleClient, time.Now()); err != nil {
		return bootstrapFailure(writer, logf, err)
	}

	// Only after the client identity is verified and pinned does the
	// listener exist.
	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return bootstrapFailure(writer, logf, protocol.NewError(protocol.CodePortInUse, "listen %s: %v", cfg.Listen, err))
	}
	defer listener.Close()
	actualPort := 0
	if addr, ok := listener.Addr().(*net.TCPAddr); ok {
		actualPort = addr.Port
	}
	session := sessionID(serverPair.der, clientDER, serverNonce, clientHello.ClientNonce, "tcp")
	protos := enabledALPN(cfg.features())
	ready := &readyMsg{
		Type: msgReady, Protocol: protocol.Version, Port: actualPort,
		ServerNonce: serverNonce, ClientNonce: clientHello.ClientNonce,
		SessionID: session, Features: cfg.features(), Limits: cfg.Limits,
	}
	if err := writer.send(ready); err != nil {
		logf("send ready: %v", err)
		return 1
	}
	raw, err = waitMsg(ctx, reader, defaultHandshakeTimeout)
	if err != nil {
		return bootstrapFailure(writer, logf, err)
	}
	ack, ok := raw.(*readyAckMsg)
	if !ok {
		return bootstrapFailure(writer, logf, protocol.NewError(protocol.CodeBootstrapProtocolError, "expected ready_ack, got %T", raw))
	}
	if ack.Protocol != protocol.Version || ack.SessionID != session || ack.ServerNonce != serverNonce || ack.ClientNonce != clientHello.ClientNonce {
		return bootstrapFailure(writer, logf, protocol.NewError(protocol.CodeBootstrapProtocolError, "ready_ack does not match this session"))
	}
	if err := writer.send(&activeAckMsg{Type: msgActiveAck, Protocol: protocol.Version, SessionID: session}); err != nil {
		logf("send active_ack: %v", err)
		return 1
	}

	serverCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	srv := &server{
		cfg: cfg, ctx: serverCtx, cancel: cancel,
		tlsConfig:  serverTLSConfig(serverPair, clientCert, protos),
		listener:   listener,
		pendingSem: make(chan struct{}, cfg.Limits.MaxPendingHandshake),
		conns:      make(map[*trackedConn]struct{}),
		pending:    make(map[net.Conn]struct{}),
		children:   make(map[*childProcess]struct{}),
		done:       make(chan struct{}),
	}
	srv.httpLn = newChanListener(listener.Addr())
	srv.http = &http.Server{
		Handler:           srv.httpHandler(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		if err := srv.http.Serve(srv.httpLn); err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			logf("http server: %v", err)
		}
	}()
	go srv.acceptLoop(logf)
	go srv.idleMonitor()

	lastControl := &atomic.Int64{}
	lastControl.Store(time.Now().UnixNano())
	go func() {
		for {
			select {
			case <-serverCtx.Done():
				return
			case m := <-reader.msgs:
				lastControl.Store(time.Now().UnixNano())
				switch msg := m.(type) {
				case *pingMsg:
					_ = writer.send(&pongMsg{Type: msgPong, Seq: msg.Seq})
				case *byeMsg:
					srv.shutdown(writer, logf, true)
					return
				case *errorMsg:
					logf("client reported %s: %s", msg.Code, msg.Message)
				default:
					_ = writer.send(controlError(protocol.CodeBootstrapProtocolError, "unexpected control message after ACTIVE"))
					srv.shutdown(writer, logf, false)
					return
				}
			case err := <-reader.errs:
				if !errors.Is(err, io.EOF) {
					logf("control channel error: %v", err)
				}
				srv.shutdown(writer, logf, false)
				return
			}
		}
	}()
	heartbeat := cfg.ControlTimeout / 3
	if heartbeat <= 0 {
		heartbeat = defaultHeartbeat
	}
	ticker := time.NewTicker(heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-srv.done:
			return srv.exitCode()
		case <-serverCtx.Done():
			srv.shutdown(writer, logf, false)
			<-srv.done
			return srv.exitCode()
		case <-ticker.C:
			if time.Since(time.Unix(0, lastControl.Load())) > cfg.ControlTimeout {
				logf("control channel silent for %s; terminating", cfg.ControlTimeout)
				srv.shutdown(writer, logf, false)
				<-srv.done
				return srv.exitCode()
			}
		}
	}
}

// bootstrapFailure reports a coded error and returns an exit code.
func bootstrapFailure(writer *controlWriter, logf func(string, ...any), err error) int {
	em := protocolErr(err)
	logf("bootstrap failed: %s: %s", em.Code, em.Message)
	_ = writer.send(em)
	return 2
}

func enabledALPN(f protocol.Features) []string {
	protos := []string{protocol.ALPNHTTP}
	if f.SFTP {
		protos = append(protos, protocol.ALPNSFTP)
	}
	return protos
}

func (s *server) logf(format string, args ...any) {
	if s.cfg.Stderr != nil {
		fmt.Fprintf(s.cfg.Stderr, "catsyncd: "+format+"\n", args...)
	}
}

func (s *server) allowedALPN(p string) bool {
	for _, want := range enabledALPN(s.cfg.features()) {
		if p == want {
			return true
		}
	}
	return false
}

// acceptLoop accepts TLS connections until the listener closes.
func (s *server) acceptLoop(logf func(string, ...any)) {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		go s.handleConn(conn)
	}
}

// handleConn performs the TLS handshake, enforces ALPN, then dispatches.
func (s *server) handleConn(conn net.Conn) {
	if s.atCapacity() {
		_ = conn.Close()
		return
	}
	select {
	case s.pendingSem <- struct{}{}:
	default:
		_ = conn.Close()
		return
	}
	s.mu.Lock()
	s.pending[conn] = struct{}{}
	s.mu.Unlock()
	releasePending := func() {
		s.mu.Lock()
		delete(s.pending, conn)
		s.mu.Unlock()
		<-s.pendingSem
	}
	_ = conn.SetDeadline(time.Now().Add(s.cfg.HandshakeTimeout))
	tlsConn := tls.Server(conn, s.tlsConfig)
	if err := tlsConn.HandshakeContext(s.ctx); err != nil {
		releasePending()
		_ = conn.Close()
		return
	}
	_ = conn.SetDeadline(time.Time{})
	releasePending()
	alpn := tlsConn.ConnectionState().NegotiatedProtocol
	if !s.allowedALPN(alpn) {
		s.counters.alpnRejected.Add(1)
		_ = tlsConn.Close()
		return
	}
	if alpn == protocol.ALPNHTTP {
		tc := newTrackedConn(tlsConn, s)
		if !s.httpLn.push(tc) {
			_ = tc.Close()
			return
		}
		return
	}
	// SFTP: one sftp-server child per connection.
	s.counters.sftpConns.Add(1)
	defer s.counters.sftpConns.Add(-1)
	tc := newTrackedConn(tlsConn, s)
	defer tc.Close()
	s.runSFTPChild(tc)
}

// runSFTPChild starts one sftp-server bound to the TLS stream and waits.
func (s *server) runSFTPChild(conn io.ReadWriter) {
	// Not CommandContext: DRAINING must close data connections before it
	// terminates children, so the child is not killed while the connection
	// is still open.
	cmd := exec.Command(s.cfg.SFTPPath)
	cmd.Stdin = conn
	cmd.Stdout = conn
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
	// Closing the connection on cancellation makes the child see EOF.
	select {
	case <-s.ctx.Done():
		_ = conn.(interface{ Close() error }).Close()
	case <-child.done:
	}
}

// atCapacity enforces the global connection budget.
func (s *server) atCapacity() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)+len(s.pending) >= s.cfg.Limits.MaxConns
}

// shutdown performs the DRAINING sequence in the documented order and closes
// the done channel exactly once.
func (s *server) shutdown(writer *controlWriter, logf func(string, ...any), graceful bool) {
	s.shutdownOnce.Do(func() {
		s.closedOnce.Do(func() {
			_ = s.listener.Close()
			s.cancel()
		})
		logf("draining: rpc=%d sftp=%d exec=%d children=%d", s.counters.rpcActive.Load(), s.counters.sftpConns.Load(), s.counters.execActive.Load(), s.counters.children.Load())
		deadline := time.Now().Add(s.cfg.DrainTimeout)
		// 1. Stop accepting and close pending handshakes.
		s.mu.Lock()
		pending := make([]net.Conn, 0, len(s.pending))
		for c := range s.pending {
			pending = append(pending, c)
		}
		s.mu.Unlock()
		for _, c := range pending {
			_ = c.Close()
		}
		// 2. Bounded wait for in-flight RPC handlers to observe cancellation.
		rpcDeadline := time.Now().Add(10 * time.Second)
		if rpcDeadline.After(deadline) {
			rpcDeadline = deadline
		}
		for s.counters.rpcActive.Load() > 0 && time.Now().Before(rpcDeadline) {
			time.Sleep(20 * time.Millisecond)
		}
		// 3. Close the HTTP server and every remaining data connection; this
		// unblocks sftp-server children before they are reaped.
		_ = s.http.Close()
		s.mu.Lock()
		conns := make([]*trackedConn, 0, len(s.conns))
		for c := range s.conns {
			conns = append(conns, c)
		}
		s.mu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
		// 4. Terminate children with a bounded grace, then wait for reaping.
		s.mu.Lock()
		children := make([]*childProcess, 0, len(s.children))
		for c := range s.children {
			children = append(children, c)
		}
		s.mu.Unlock()
		grace := 5 * time.Second
		for _, c := range children {
			terminateChild(c.cmd, grace)
		}
		for _, c := range children {
			select {
			case <-c.done:
			case <-time.After(2 * time.Second):
				logf("child did not exit after termination")
			}
		}
		// 5. Wait for all counters to reach zero, bounded by the drain deadline.
		for time.Now().Before(deadline) {
			if s.snapshotCounts() == (countsSnapshot{}) {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		snap := s.snapshotCounts()
		logf("drain complete: rpc=%d sftp=%d exec=%d children=%d", snap.rpc, snap.sftp, snap.exec, snap.children)
		s.mu.Lock()
		s.final = snap
		s.drained = graceful && snap == (countsSnapshot{})
		s.mu.Unlock()
		if s.cfg.OnShutdown != nil {
			s.cfg.OnShutdown(ShutdownStats{
				RPCActive: snap.rpc, SFTPConns: snap.sftp, ExecActive: snap.exec,
				Children: snap.children, Drained: s.drained,
			})
		}
		if graceful {
			if snap != (countsSnapshot{}) {
				_ = writer.send(&errorMsg{Type: msgError, Code: protocol.CodeShutdownTimeout,
					Message: fmt.Sprintf("drain timed out: rpc=%d sftp=%d exec=%d children=%d", snap.rpc, snap.sftp, snap.exec, snap.children)})
			} else {
				_ = writer.send(&byeAckMsg{Type: msgByeAck, Drained: true})
			}
		}
		close(s.done)
	})
}

// exitCode maps the shutdown outcome to a process exit code.
func (s *server) exitCode() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.final != (countsSnapshot{}) {
		return 4
	}
	if !s.drained {
		return 3
	}
	return 0
}

// ShutdownStats is the final resource state reported after DRAINING.
type ShutdownStats struct {
	RPCActive  int
	SFTPConns  int
	ExecActive int
	Children   int
	Drained    bool
}

type countsSnapshot struct {
	rpc, sftp, exec, children int
}

func (s *server) snapshotCounts() countsSnapshot {
	return countsSnapshot{
		rpc:      int(s.counters.rpcActive.Load()),
		sftp:     int(s.counters.sftpConns.Load()),
		exec:     int(s.counters.execActive.Load()),
		children: int(s.counters.children.Load()),
	}
}

// idleMonitor closes data connections that have been silent for too long.
func (s *server) idleMonitor() {
	interval := s.cfg.IdleTimeout / 4
	if interval <= 0 || interval > 30*time.Second {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-s.cfg.IdleTimeout).UnixNano()
			s.mu.Lock()
			var idle []*trackedConn
			for c := range s.conns {
				if c.lastActive.Load() < cutoff {
					idle = append(idle, c)
				}
			}
			s.mu.Unlock()
			for _, c := range idle {
				_ = c.Close()
			}
		}
	}
}

// chanListener feeds accepted TLS connections into net/http.
type chanListener struct {
	ch   chan net.Conn
	done chan struct{}
	addr net.Addr
	once sync.Once
}

func newChanListener(addr net.Addr) *chanListener {
	return &chanListener{ch: make(chan net.Conn), done: make(chan struct{}), addr: addr}
}

func (l *chanListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.ch:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *chanListener) Close() error {
	l.once.Do(func() { close(l.done) })
	return nil
}

func (l *chanListener) Addr() net.Addr { return l.addr }

func (l *chanListener) push(c net.Conn) bool {
	select {
	case l.ch <- c:
		return true
	case <-l.done:
		return false
	}
}

// trackedConn updates activity and removes itself from the registry on close.
type trackedConn struct {
	net.Conn
	srv        *server
	lastActive atomic.Int64
}

func newTrackedConn(c net.Conn, s *server) *trackedConn {
	tc := &trackedConn{Conn: c, srv: s}
	tc.lastActive.Store(time.Now().UnixNano())
	s.mu.Lock()
	s.conns[tc] = struct{}{}
	s.mu.Unlock()
	return tc
}

func (t *trackedConn) Read(p []byte) (int, error) {
	n, err := t.Conn.Read(p)
	t.lastActive.Store(time.Now().UnixNano())
	return n, err
}

func (t *trackedConn) Write(p []byte) (int, error) {
	n, err := t.Conn.Write(p)
	t.lastActive.Store(time.Now().UnixNano())
	return n, err
}

func (t *trackedConn) Close() error {
	t.srv.mu.Lock()
	delete(t.srv.conns, t)
	t.srv.mu.Unlock()
	return t.Conn.Close()
}

// childProcess tracks one sftp-server (and, later, rsync) child.
type childProcess struct {
	cmd  *exec.Cmd
	done chan struct{}
	err  error
}
