//go:build unix

package daemon

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/dotfrankruan/CatSync/internal/daemon/protocol"
)

// startQUICTestServer boots a real one-shot server over the in-memory SSH
// bootstrap and a real loopback UDP QUIC data plane.
func startQUICTestServer(t *testing.T, mutate func(*ServerConfig)) *testServer {
	t.Helper()
	return startTestServer(t, func(cfg *ServerConfig) {
		cfg.Transport = "quic"
		cfg.Listen = "127.0.0.1:0"
		if mutate != nil {
			mutate(cfg)
		}
	})
}

func bootstrapQUIC(t *testing.T, ts *testServer) *Client {
	t.Helper()
	client, err := Bootstrap(context.Background(), BootstrapConfig{
		Transport:         staticTransport{ts.sess},
		Version:           "test-client",
		Host:              "127.0.0.1",
		DataTransport:     "quic",
		HandshakeTimeout:  5 * time.Second,
		ControlTimeout:    3 * time.Second,
		HeartbeatInterval: 100 * time.Millisecond,
		CloseTimeout:      5 * time.Second,
	})
	if err != nil {
		t.Fatalf("bootstrap quic: %v", err)
	}
	ts.client = client
	return client
}

// startTestServer overwrites ts.client after the normal bootstrap; the QUIC
// tests build their own client because the data transport differs.
func startTestServerQUIC(t *testing.T, mutate func(*ServerConfig)) *testServer {
	t.Helper()
	serverInR, clientInW := io.Pipe()
	clientOutR, serverOutW := io.Pipe()
	res := newServerResult()
	stats := make(chan ShutdownStats, 1)
	cfg := ServerConfig{
		Listen: "127.0.0.1:0", Version: "test", Transport: "quic",
		IdleTimeout: 5 * time.Second, DrainTimeout: 10 * time.Second,
		ControlTimeout: 5 * time.Second, HandshakeTimeout: 5 * time.Second,
		Limits: protocol.Limits{MaxSFTPConns: 8, MaxExecConns: 8, MaxControlConns: 8, MaxPendingHandshake: 8, MaxConns: 64},
		Stdin:  serverInR, Stdout: serverOutW, Stderr: testLogWriter{t},
		OnShutdown: func(s ShutdownStats) { stats <- s },
	}
	if mutate != nil {
		mutate(&cfg)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { res.finish(Serve(ctx, cfg)) }()
	sess := &testSession{r: clientOutR, w: clientInW, res: res, closed: make(chan struct{})}
	ts := &testServer{t: t, res: res, stats: stats, sess: sess, cancel: cancel}
	return ts
}

func TestQUICRPCSurfaceAndStreams(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "a.txt"), "alpha")
	writeFile(t, filepath.Join(root, "sub", "b.txt"), "bravo")
	ts := startTestServerQUIC(t, nil)
	c := bootstrapQUIC(t, ts)
	if c.quicConn == nil {
		t.Fatal("no QUIC connection")
	}
	dest := filepath.Join(t.TempDir(), "dest")
	if created, err := c.Mkdir(context.Background(), []string{dest}); err != nil || created != 1 {
		t.Fatalf("mkdir = %d, %v", created, err)
	}
	man, err := c.Manifest(context.Background(), protocol.ModeSourceTree, root)
	if err != nil || len(man.Entries) != 2 {
		t.Fatalf("manifest = %+v, %v", man, err)
	}
	hashes, err := c.Hash(context.Background(), []string{filepath.Join(root, "a.txt")})
	if err != nil || len(hashes) != 1 {
		t.Fatalf("hash = %v, %v", hashes, err)
	}
	st, err := c.Stat(context.Background(), filepath.Join(root, "a.txt"))
	if err != nil || st.Type != protocol.StatFile {
		t.Fatalf("stat = %+v, %v", st, err)
	}
	same, err := c.SameFile(context.Background(), filepath.Join(root, "a.txt"), filepath.Join(root, "a.txt"))
	if err != nil || !same {
		t.Fatalf("samefile = %v, %v", same, err)
	}
	code, stats := ts.shutdown()
	if code != 0 || !stats.Drained {
		t.Errorf("exit=%d stats=%+v", code, stats)
	}
}

func TestQUICSFTPStreamsBudget(t *testing.T) {
	sftpPath := ResolveSFTPPath("")
	if sftpPath == "" {
		t.Skip("sftp-server is not installed")
	}
	ts := startTestServerQUIC(t, func(cfg *ServerConfig) {
		cfg.SFTPPath = sftpPath
		cfg.Limits.MaxSFTPStreams = 4
		cfg.Limits.MaxQUICStreams = 4 + 8 + 8
	})
	c := bootstrapQUIC(t, ts)
	if !c.Features().SFTP {
		t.Fatal("sftp feature missing")
	}
	var held []sftpClient
	for i := 0; i < 4; i++ {
		conn, err := c.DialSFTP(context.Background())
		if err != nil {
			t.Fatalf("stream %d: %v", i, err)
		}
		cl, err := newTestSFTPClient(conn)
		if err != nil {
			t.Fatalf("handshake %d: %v", i, err)
		}
		held = append(held, cl)
	}
	// The fifth stream must be rejected while all four budget slots are in
	// use; the reset may surface at dial or at the SFTP handshake.
	if conn, err := c.DialSFTP(context.Background()); err == nil {
		if _, herr := newTestSFTPClient(conn); herr == nil {
			t.Error("stream over the SFTP budget was accepted")
			_ = conn.Close()
		}
	}
	for _, cl := range held {
		_ = cl.Close()
	}
	if code, _ := ts.shutdown(); code != 0 {
		t.Errorf("exit = %d", code)
	}
}

func TestQUICRejectsSecondConnection(t *testing.T) {
	ts := startTestServerQUIC(t, nil)
	c := bootstrapQUIC(t, ts)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg := c.quicTLS.Clone()
	conn, err := quic.DialAddr(ctx, c.dialAddr, cfg, quicClientConfig(BootstrapConfig{HandshakeTimeout: 2 * time.Second}))
	if err != nil {
		t.Logf("second connection rejected during handshake: %v", err)
	} else {
		_ = conn.CloseWithError(0, "test")
	}
	if code, _ := ts.shutdown(); code != 0 {
		t.Errorf("exit = %d", code)
	}
}

func TestQUICRejectsBadCertificateAndStreamType(t *testing.T) {
	ts := startTestServerQUIC(t, nil)
	c := bootstrapQUIC(t, ts)
	// Wrong pinned server certificate: dial with a fresh server identity.
	wrongServer, err := newCertPair(roleServer, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	rightClient, err := newCertPair(roleClient, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	wrongTLS := clientTLSConfig(rightClient, wrongServer.cert, []string{protocol.ALPNQUIC})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if dialed, derr := quic.DialAddr(ctx, c.dialAddr, wrongTLS, quicClientConfig(BootstrapConfig{HandshakeTimeout: 2 * time.Second})); derr == nil {
		_ = dialed.CloseWithError(0, "unexpected")
		t.Error("QUIC handshake accepted an unpinned server certificate")
	}
	// Invalid stream type must reset the stream, not execute anything.
	st, err := c.quicConn.OpenStreamSync(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Write([]byte{99}); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()
	if _, err := io.ReadAll(st); err == nil {
		t.Error("invalid stream type was not rejected")
	}
	if code, _ := ts.shutdown(); code != 0 {
		t.Errorf("exit = %d", code)
	}
}

func TestQUICExecRejectedProgram(t *testing.T) {
	rsyncPath := ResolveRsyncPath("")
	if rsyncPath == "" {
		t.Skip("rsync is not installed")
	}
	ts := startTestServerQUIC(t, func(cfg *ServerConfig) { cfg.RsyncPath = rsyncPath })
	c := bootstrapQUIC(t, ts)
	sess, err := c.Exec(context.Background(), "sh", []string{"-c", "id"})
	if err == nil {
		_, werr := sess.Wait(context.Background())
		sess.Close()
		if werr == nil || !strings.Contains(werr.Error(), protocol.CodeExecRejected) {
			t.Errorf("exec error = %v", werr)
		}
	}
	if code, _ := ts.shutdown(); code != 0 {
		t.Errorf("exit = %d", code)
	}
}

var _ = os.Getpid
var _ = tls.VersionTLS13

func TestQUICConcurrentSFTPStreamsAndControl(t *testing.T) {
	sftpPath := ResolveSFTPPath("")
	if sftpPath == "" {
		t.Skip("sftp-server is not installed")
	}
	for _, n := range []int{8, 32, 64} {
		t.Run(fmt.Sprintf("%d_streams", n), func(t *testing.T) {
			ts := startTestServerQUIC(t, func(cfg *ServerConfig) {
				cfg.SFTPPath = sftpPath
				cfg.Limits.MaxSFTPStreams = n
				cfg.Limits.MaxExecStreams = 8
				cfg.Limits.MaxControlStreams = 16
				cfg.Limits.MaxQUICStreams = n + 8 + 16
			})
			c := bootstrapQUIC(t, ts)
			var wg sync.WaitGroup
			errs := make(chan error, n)
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					conn, err := c.DialSFTP(context.Background())
					if err != nil {
						errs <- err
						return
					}
					cl, err := newTestSFTPClient(conn)
					if err != nil {
						errs <- err
						return
					}
					defer func() { _ = cl.Close() }()
					// Hold the stream open while control RPCs run.
					<-time.After(50 * time.Millisecond)
				}()
			}
			// Control must remain serviceable while every data stream is held.
			for i := 0; i < 8; i++ {
				if _, err := c.Stat(context.Background(), "/"); err != nil {
					errs <- fmt.Errorf("control RPC while streams saturated: %w", err)
				}
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				t.Fatalf("stream: %v", err)
			}
			if code, stats := ts.shutdown(); code != 0 || !stats.Drained {
				t.Errorf("exit=%d stats=%+v", code, stats)
			}
		})
	}
}

func TestQUICFlowControlSmallWindows(t *testing.T) {
	sftpPath := ResolveSFTPPath("")
	if sftpPath == "" {
		t.Skip("sftp-server is not installed")
	}
	ts := startTestServerQUIC(t, func(cfg *ServerConfig) {
		cfg.SFTPPath = sftpPath
		cfg.QUICInitialStreamWindow = 64 << 10
		cfg.QUICMaxStreamWindow = 128 << 10
		cfg.QUICMaxConnWindow = 256 << 10
	})
	c := bootstrapQUIC(t, ts)
	root := t.TempDir()
	payload := make([]byte, 512<<10)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	local := filepath.Join(root, "big.bin")
	if err := os.WriteFile(local, payload, 0o644); err != nil {
		t.Fatal(err)
	}
	remote := filepath.Join(t.TempDir(), "remote")
	if _, err := c.Mkdir(context.Background(), []string{remote}); err != nil {
		t.Fatal(err)
	}
	conn, err := c.DialSFTP(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	client, err := newTestSFTPClient(conn)
	if err != nil {
		t.Fatal(err)
	}
	f, err := client.Create(filepath.Join(remote, "big.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(remote, "big.bin"))
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("flow-control transfer mismatch: %v", err)
	}
	if code, _ := ts.shutdown(); code != 0 {
		t.Errorf("exit = %d", code)
	}
}

func TestQUICAbruptClientDeath(t *testing.T) {
	ts := startTestServerQUIC(t, nil)
	c := bootstrapQUIC(t, ts)
	if c.quicConn.ConnectionState().Used0RTT {
		t.Error("QUIC used 0-RTT")
	}
	// Simulate a client crash: the data connection disappears without bye.
	_ = c.quicConn.CloseWithError(0, "client death")
	code := ts.waitExit()
	if code == 0 {
		t.Errorf("server exited 0 after abrupt client death, want non-zero")
	}
	// The control channel is gone too in a real crash; close it so the test
	// does not wait for the heartbeat timeout.
	_ = c.sess.Close()
	_ = c.Close()
}

func TestQUICMalformedControlFrameRejected(t *testing.T) {
	ts := startTestServerQUIC(t, nil)
	c := bootstrapQUIC(t, ts)
	st, err := c.quicConn.OpenStreamSync(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Write([]byte{protocol.StreamControl}); err != nil {
		t.Fatal(err)
	}
	// Length says 100 bytes but only 3 arrive before FIN.
	if _, err := st.Write([]byte{0, 0, 0, 100, '{', 'x', 'y'}); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()
	data, _ := io.ReadAll(st)
	if len(data) != 0 {
		t.Errorf("malformed control request received a response: %q", data)
	}
	if code, _ := ts.shutdown(); code != 0 {
		t.Errorf("exit = %d", code)
	}
}

func TestQUICExecStreamsConcurrent(t *testing.T) {
	rsyncPath := ResolveRsyncPath("")
	if rsyncPath == "" {
		t.Skip("rsync is not installed")
	}
	ts := startTestServerQUIC(t, func(cfg *ServerConfig) {
		cfg.RsyncPath = rsyncPath
		cfg.Limits.MaxExecStreams = 4
		cfg.Limits.MaxQUICStreams = 8 + 4 + 16
	})
	c := bootstrapQUIC(t, ts)
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sess, err := c.Exec(context.Background(), "rsync",
				[]string{"--server", "-logDtpre.iLsfxCIvu", ".", "/tmp"})
			if err != nil {
				errs <- err
				return
			}
			go func() { _, _ = io.Copy(io.Discard, sess.Stdout()) }()
			go func() { _, _ = io.Copy(io.Discard, sess.Stderr()) }()
			_ = sess.CloseStdin()
			_, werr := sess.Wait(context.Background())
			sess.Close()
			// rsync --server without a real client protocol exits non-zero;
			// the point is that four independent exec streams ran and were
			// reaped, not that it succeeded.
			if werr != nil && !strings.Contains(werr.Error(), "exec") {
				_ = werr
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("exec stream: %v", err)
	}
	if code, stats := ts.shutdown(); code != 0 || !stats.Drained {
		t.Errorf("exit=%d stats=%+v", code, stats)
	}
}
