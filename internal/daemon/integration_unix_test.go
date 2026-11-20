//go:build unix

package daemon

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dotfrankruan/CatSync/internal/daemon/protocol"
)

// serverResult shares the Serve return value between the session wait and
// the test's exit assertion without consuming a channel twice.
type serverResult struct {
	done chan struct{}
	code int
	mu   sync.Mutex
}

func newServerResult() *serverResult { return &serverResult{done: make(chan struct{})} }

func (r *serverResult) finish(code int) {
	r.mu.Lock()
	r.code = code
	r.mu.Unlock()
	close(r.done)
}

func (r *serverResult) wait(ctx context.Context) (int, error) {
	select {
	case <-r.done:
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.code, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// testSession is the client end of an in-memory bootstrap control channel.
type testSession struct {
	r      io.ReadCloser
	w      io.WriteCloser
	res    *serverResult
	once   sync.Once
	closed chan struct{}
}

func (s *testSession) Read(p []byte) (int, error)  { return s.r.Read(p) }
func (s *testSession) Write(p []byte) (int, error) { return s.w.Write(p) }
func (s *testSession) Close() error {
	s.once.Do(func() {
		_ = s.w.Close()
		_ = s.r.Close()
		close(s.closed)
	})
	return nil
}

func (s *testSession) Wait(ctx context.Context) error {
	_, err := s.res.wait(ctx)
	return err
}

type staticTransport struct{ sess ControlSession }

func (t staticTransport) Start(ctx context.Context) (ControlSession, error) { return t.sess, nil }

type testServer struct {
	t      *testing.T
	res    *serverResult
	stats  chan ShutdownStats
	client *Client
	sess   *testSession
	cancel context.CancelFunc
}

// startTestServer runs Serve over in-memory pipes and bootstraps a client.
func startTestServer(t *testing.T, mutate func(*ServerConfig)) *testServer {
	t.Helper()
	serverInR, clientInW := io.Pipe()
	clientOutR, serverOutW := io.Pipe()
	res := newServerResult()
	stats := make(chan ShutdownStats, 1)
	cfg := ServerConfig{
		Listen:           "127.0.0.1:0",
		Version:          "test",
		IdleTimeout:      2 * time.Second,
		DrainTimeout:     5 * time.Second,
		ControlTimeout:   3 * time.Second,
		HandshakeTimeout: 3 * time.Second,
		Limits:           protocol.Limits{MaxSFTPConns: 8, MaxExecConns: 8, MaxControlConns: 8, MaxPendingHandshake: 8, MaxConns: 32},
		Stdin:            serverInR,
		Stdout:           serverOutW,
		Stderr:           testLogWriter{t},
		OnShutdown:       func(s ShutdownStats) { stats <- s },
	}
	if mutate != nil {
		mutate(&cfg)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { res.finish(Serve(ctx, cfg)) }()
	sess := &testSession{r: clientOutR, w: clientInW, res: res, closed: make(chan struct{})}
	ts := &testServer{t: t, res: res, stats: stats, sess: sess, cancel: cancel}
	client, err := Bootstrap(context.Background(), BootstrapConfig{
		Transport:         staticTransport{sess},
		Version:           "test-client",
		Host:              "127.0.0.1",
		HandshakeTimeout:  5 * time.Second,
		ControlTimeout:    3 * time.Second,
		HeartbeatInterval: 100 * time.Millisecond,
		CloseTimeout:      5 * time.Second,
	})
	if err != nil {
		cancel()
		t.Fatalf("bootstrap: %v", err)
	}
	ts.client = client
	return ts
}

// waitExit waits for the server process result.
func (ts *testServer) waitExit() int {
	ts.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	code, err := ts.res.wait(ctx)
	if err != nil {
		ts.t.Fatal("server did not exit")
	}
	return code
}

// shutdown closes the client gracefully and returns the server exit code and
// final counters.
func (ts *testServer) shutdown() (int, ShutdownStats) {
	ts.t.Helper()
	if err := ts.client.Close(); err != nil {
		ts.t.Fatalf("client close: %v", err)
	}
	code := ts.waitExit()
	select {
	case s := <-ts.stats:
		return code, s
	case <-time.After(5 * time.Second):
		ts.t.Fatal("no shutdown stats")
		return -1, ShutdownStats{}
	}
}

type testLogWriter struct{ t *testing.T }

func (w testLogWriter) Write(p []byte) (int, error) {
	w.t.Logf("catsyncd: %s", strings.TrimSpace(string(p)))
	return len(p), nil
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestBootstrapRPCSurfaceAndGracefulShutdown(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "a.txt"), "alpha")
	writeFile(t, filepath.Join(root, "b.txt"), "bravo")
	writeFile(t, filepath.Join(root, "sub", "c.txt"), "charlie")
	if err := os.Symlink("a.txt", filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(t.TempDir(), "dest")

	ts := startTestServer(t, nil)
	c := ts.client
	if c.Port() <= 0 || c.SessionID() == "" {
		t.Fatalf("port/session: %d %q", c.Port(), c.SessionID())
	}
	f := c.Features()
	if f.Exec {
		t.Error("M1 advertises exec")
	}
	if !f.Manifest || !f.Metadata || !f.Hash || !f.Stat || !f.SameFile {
		t.Errorf("features missing: %+v", f)
	}

	created, err := c.Mkdir(context.Background(), []string{dest, filepath.Join(dest, "nested")})
	if err != nil || created != 2 {
		t.Fatalf("Mkdir = %d, %v", created, err)
	}
	if fi, err := os.Stat(dest); err != nil || !fi.IsDir() {
		t.Fatalf("mkdir did not create %s: %v", dest, err)
	}

	source, err := c.Manifest(context.Background(), protocol.ModeSourceTree, root)
	if err != nil {
		t.Fatalf("source manifest: %v", err)
	}
	var got []string
	kinds := map[string]string{}
	for _, e := range source.Entries {
		got = append(got, e.Path)
		kinds[e.Path] = e.Kind
	}
	want := []string{"a.txt", "b.txt", "link", "sub/c.txt"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("source manifest = %v, want %v", got, want)
	}
	if kinds["link"] != protocol.KindSymlink {
		t.Errorf("link kind = %q", kinds["link"])
	}
	if kinds["a.txt"] != protocol.KindFile {
		t.Errorf("a.txt kind = %q", kinds["a.txt"])
	}

	d, err := c.Manifest(context.Background(), protocol.ModeDestTree, dest)
	if err != nil || len(d.Entries) != 0 {
		t.Fatalf("empty dest manifest = %+v, %v", d, err)
	}
	writeFile(t, filepath.Join(dest, "existing.txt"), "x")
	if err := os.Symlink("existing.txt", filepath.Join(dest, "sym")); err != nil {
		t.Fatal(err)
	}
	d, err = c.Manifest(context.Background(), protocol.ModeDestTree, dest)
	if err != nil {
		t.Fatalf("dest manifest: %v", err)
	}
	if len(d.Entries) != 1 || d.Entries[0].Path != "existing.txt" {
		t.Errorf("dest manifest = %+v", d.Entries)
	}

	one, err := c.Manifest(context.Background(), protocol.ModeSourceFile, filepath.Join(root, "a.txt"))
	if err != nil || len(one.Entries) != 1 || one.Entries[0].Path != "a.txt" {
		t.Fatalf("single-file manifest = %+v, %v", one, err)
	}
	if _, err := c.Manifest(context.Background(), protocol.ModeSourceFile, root); err == nil {
		t.Error("directory accepted as single-file source")
	}

	st, err := c.Stat(context.Background(), filepath.Join(root, "a.txt"))
	if err != nil || st.Type != protocol.StatFile || st.Size != 5 {
		t.Fatalf("stat = %+v, %v", st, err)
	}
	if st, err := c.Stat(context.Background(), filepath.Join(root, "link")); err != nil || st.Type != protocol.StatSymlink {
		t.Fatalf("symlink stat = %+v, %v", st, err)
	}

	same, err := c.SameFile(context.Background(), filepath.Join(root, "a.txt"), filepath.Join(root, "link"))
	if err != nil || !same {
		t.Fatalf("SameFile = %v, %v (os.Stat follows symlinks)", same, err)
	}
	if same, err := c.SameFile(context.Background(), filepath.Join(root, "a.txt"), filepath.Join(root, "b.txt")); err != nil || same {
		t.Fatalf("SameFile distinct = %v, %v", same, err)
	}

	target := filepath.Join(dest, "meta.txt")
	writeFile(t, target, "meta")
	const mtime = int64(1_700_000_000_123_456_789)
	if err := c.Metadata(context.Background(), target, 0o640, mtime); err != nil {
		t.Fatalf("metadata: %v", err)
	}
	fi, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o640 {
		t.Errorf("mode = %o", fi.Mode().Perm())
	}
	if got := fi.ModTime().UnixNano(); got != mtime {
		t.Errorf("mtime = %d, want %d (filesystem may truncate)", got, mtime)
	}

	hashes, err := c.Hash(context.Background(), []string{filepath.Join(root, "a.txt"), filepath.Join(root, "b.txt")})
	if err != nil || len(hashes) != 2 {
		t.Fatalf("hash = %v, %v", hashes, err)
	}
	for p, h := range hashes {
		if len(h) != 64 {
			t.Errorf("hash %q = %q", p, h)
		}
	}
	if _, err := c.Hash(context.Background(), []string{root}); err == nil {
		t.Error("hash accepted a directory")
	}

	if _, err := c.Manifest(context.Background(), protocol.ModeDestTree, filepath.Join(root, "missing")); err == nil {
		t.Error("missing destination accepted")
	}

	code, stats := ts.shutdown()
	if code != 0 {
		t.Errorf("server exit = %d, want 0", code)
	}
	if !stats.Drained || stats.RPCActive != 0 || stats.SFTPConns != 0 || stats.ExecActive != 0 || stats.Children != 0 {
		t.Errorf("shutdown stats = %+v", stats)
	}
}

func TestALPNEnforcement(t *testing.T) {
	ts := startTestServer(t, func(cfg *ServerConfig) { cfg.SFTPPath = "" })
	c := ts.client
	c.httpTLS.NextProtos = []string{protocol.ALPNExec}
	if _, err := dialTLS(context.Background(), c.dialAddr, c.httpTLS, protocol.ALPNExec); err == nil {
		t.Error("exec ALPN accepted in M1")
	}
	c.httpTLS.NextProtos = []string{protocol.ALPNHTTP}
	if _, err := dialTLS(context.Background(), c.dialAddr, c.sftpTLS, protocol.ALPNSFTP); err == nil {
		t.Error("sftp ALPN accepted while sftp capability is disabled")
	}
	// A connection with no ALPN completes the TLS handshake but must be
	// closed before any application bytes are served.
	cfg := c.httpTLS.Clone()
	cfg.NextProtos = nil
	raw, err := tls.Dial("tcp", c.dialAddr, cfg)
	if err != nil {
		t.Logf("no-ALPN connection rejected at handshake: %v", err)
	} else {
		_ = raw.SetReadDeadline(time.Now().Add(2 * time.Second))
		var buf [1]byte
		if _, err := raw.Read(buf[:]); err == nil {
			t.Error("no-ALPN connection received application data")
		}
		_ = raw.Close()
	}
	// The legitimate session still works.
	if _, err := c.Stat(context.Background(), filepath.Clean(os.TempDir())); err != nil {
		t.Errorf("session broken after ALPN probing: %v", err)
	}
	if code, _ := ts.shutdown(); code != 0 {
		t.Errorf("exit = %d", code)
	}
}

func TestSFTPForwardingAndDrain(t *testing.T) {
	sftpPath := ResolveSFTPPath("")
	if sftpPath == "" {
		t.Skip("sftp-server is not installed")
	}
	root := t.TempDir()
	ts := startTestServer(t, func(cfg *ServerConfig) { cfg.SFTPPath = sftpPath })
	if !ts.client.Features().SFTP {
		t.Fatal("sftp feature not advertised despite a resolved sftp-server")
	}
	client, err := sftpNewClient(ts.client)
	if err != nil {
		t.Fatalf("sftp client: %v", err)
	}
	target := filepath.Join(root, "hello.txt")
	f, err := client.Create(target)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := f.Write([]byte("hello over tls")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if data, err := os.ReadFile(target); err != nil || string(data) != "hello over tls" {
		t.Fatalf("read back = %q, %v", data, err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("sftp client close: %v", err)
	}
	code, stats := ts.shutdown()
	if code != 0 {
		t.Errorf("exit = %d", code)
	}
	if stats.SFTPConns != 0 || stats.Children != 0 {
		t.Errorf("stats after close = %+v", stats)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	ts := startTestServer(t, nil)
	c := ts.client
	if err := c.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if code := ts.waitExit(); code != 0 {
		t.Errorf("exit = %d", code)
	}
}

func TestManifestOrderingBudgetsAndEncoding(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "a.b"), "x")
	writeFile(t, filepath.Join(root, "a", "x"), "y")
	writeFile(t, filepath.Join(root, "z"), "z")

	ts := startTestServer(t, nil)
	res, err := ts.client.Manifest(context.Background(), protocol.ModeSourceTree, root)
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	var got []string
	for _, e := range res.Entries {
		got = append(got, e.Path)
	}
	want := []string{"a.b", "a/x", "z"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("order = %v, want %v", got, want)
	}
	if code, _ := ts.shutdown(); code != 0 {
		t.Errorf("exit = %d", code)
	}

	// Entry budget failure: the stream must end with an error record.
	ts2 := startTestServer(t, func(cfg *ServerConfig) { cfg.Manifest.MaxEntries = 2 })
	writeFile(t, filepath.Join(root, "b1"), "1")
	writeFile(t, filepath.Join(root, "b2"), "2")
	writeFile(t, filepath.Join(root, "b3"), "3")
	if _, err := ts2.client.Manifest(context.Background(), protocol.ModeSourceTree, root); err == nil ||
		!strings.Contains(err.Error(), protocol.CodeManifestTooLarge) {
		t.Errorf("budget error = %v, want %s", err, protocol.CodeManifestTooLarge)
	}
	if code, _ := ts2.shutdown(); code != 0 {
		t.Errorf("exit = %d", code)
	}

	// Non-UTF-8 names are rejected before any sync could act on them.
	bad := filepath.Join(root, string([]byte{'b', 0xff}))
	if err := os.WriteFile(bad, []byte("x"), 0o644); err == nil {
		ts3 := startTestServer(t, nil)
		if _, err := ts3.client.Manifest(context.Background(), protocol.ModeSourceTree, root); err == nil ||
			!strings.Contains(err.Error(), protocol.CodeUnsupportedPathEncoding) {
			t.Errorf("non-UTF-8 error = %v, want %s", err, protocol.CodeUnsupportedPathEncoding)
		}
		if code, _ := ts3.shutdown(); code != 0 {
			t.Errorf("exit = %d", code)
		}
	}
}

func TestStrictRPCBoundaries(t *testing.T) {
	ts := startTestServer(t, nil)
	c := ts.client
	raw := func(path, body string) int {
		req, err := http.NewRequest(http.MethodPost, "http://"+c.dialAddr+path, bytes.NewBufferString(body))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := c.http.Do(req)
		if err != nil {
			t.Fatalf("raw request: %v", err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}
	for _, body := range []string{
		`{"path":"/tmp","path":"/var"}`,
		`{"path":"/tmp"} {}`,
		`{"path":"/tmp","bogus":1}`,
		`{"path":5}`,
	} {
		if code := raw("/v1/stat", body); code != http.StatusBadRequest {
			t.Errorf("stat body %q: HTTP %d, want 400", body, code)
		}
	}
	oversized := `{"path":"/` + strings.Repeat("a", protocol.MaxRPCBodyBytes) + `"}`
	if code := raw("/v1/stat", oversized); code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized body: HTTP %d, want 413", code)
	}
	if code, _ := ts.shutdown(); code != 0 {
		t.Errorf("exit = %d", code)
	}
}

func TestCloseLeavesNoGoroutines(t *testing.T) {
	before := runtime.NumGoroutine()
	ts := startTestServer(t, nil)
	if err := ts.client.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if code := ts.waitExit(); code != 0 {
		t.Errorf("exit = %d", code)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before+2 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Errorf("goroutines: before=%d after=%d", before, runtime.NumGoroutine())
}

func TestBootstrapProtocolErrors(t *testing.T) {
	// A transport that emits an invalid first line must fail cleanly.
	a, b := io.Pipe()
	res := newServerResult()
	go func() {
		_, _ = b.Write([]byte("not a json line\n"))
		res.finish(1)
		_ = b.Close()
	}()
	sess := &testSession{r: a, w: nopWriteCloser{io.Discard}, res: res, closed: make(chan struct{})}
	_, err := Bootstrap(context.Background(), BootstrapConfig{
		Transport: staticTransport{sess}, Host: "127.0.0.1",
		HandshakeTimeout: 2 * time.Second, CloseTimeout: 2 * time.Second,
	})
	if err == nil {
		t.Fatal("bootstrap succeeded on garbage")
	}
	if !strings.Contains(err.Error(), protocol.CodeBootstrapProtocolError) && !strings.Contains(err.Error(), protocol.CodeJSONTypeMismatch) {
		t.Errorf("error = %v, want a protocol error", err)
	}
}

type nopWriteCloser struct{ io.Writer }

func (n nopWriteCloser) Close() error { return nil }

// sftpNewClient wraps one SFTP data connection from the client.
func sftpNewClient(c *Client) (sftpClient, error) {
	conn, err := c.DialSFTP(context.Background())
	if err != nil {
		return nil, err
	}
	return newTestSFTPClient(conn)
}

func TestDrainWithIdleSFTPConnection(t *testing.T) {
	sftpPath := ResolveSFTPPath("")
	if sftpPath == "" {
		t.Skip("sftp-server is not installed")
	}
	ts := startTestServer(t, func(cfg *ServerConfig) { cfg.SFTPPath = sftpPath })
	conn, err := ts.client.DialSFTP(context.Background())
	if err != nil {
		t.Fatalf("dial sftp: %v", err)
	}
	// Leave the data connection idle (no SFTP protocol traffic) and close
	// the session: DRAINING must close it and reap the child without
	// waiting for the idle timeout or reporting SHUTDOWN_TIMEOUT.
	code, stats := ts.shutdown()
	_ = conn
	if code != 0 {
		t.Errorf("exit = %d, want 0", code)
	}
	if stats.SFTPConns != 0 || stats.Children != 0 || !stats.Drained {
		t.Errorf("stats = %+v", stats)
	}
}

func TestALPNUnknownRejected(t *testing.T) {
	ts := startTestServer(t, nil)
	c := ts.client
	cfg := c.httpTLS.Clone()
	cfg.NextProtos = []string{"catsync-bogus/1"}
	if _, err := dialTLS(context.Background(), c.dialAddr, cfg, "catsync-bogus/1"); err == nil {
		t.Error("unknown ALPN accepted")
	}
	if code, _ := ts.shutdown(); code != 0 {
		t.Errorf("exit = %d", code)
	}
}

func TestUncleanControlEOFExitsNonZero(t *testing.T) {
	ts := startTestServer(t, nil)
	// Drop the bootstrap channel without bye.
	_ = ts.sess.Close()
	code := ts.waitExit()
	if code != 3 {
		t.Errorf("exit = %d, want 3 for a lost control channel", code)
	}
}

func TestRPCBoundaryErrors(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "f"), "x")
	link := filepath.Join(root, "l")
	if err := os.Symlink("f", link); err != nil {
		t.Fatal(err)
	}
	ts := startTestServer(t, nil)
	c := ts.client
	ctx := context.Background()

	if _, err := c.Mkdir(ctx, []string{"relative/path"}); err == nil {
		t.Error("relative mkdir accepted")
	} else if !strings.Contains(err.Error(), protocol.CodeInvalidArgument) {
		t.Errorf("mkdir error = %v", err)
	}
	if err := c.Metadata(ctx, root, 0o755, 0); err == nil {
		t.Error("metadata accepted a directory")
	}
	if _, err := c.Hash(ctx, []string{link}); err == nil {
		t.Error("hash accepted a symlink")
	}
	if _, err := c.Hash(ctx, []string{filepath.Join(root, "f"), filepath.Join(root, "f")}); err == nil {
		t.Error("hash accepted duplicate paths")
	}
	if _, err := c.Stat(ctx, filepath.Join(root, "missing")); err == nil {
		t.Error("stat accepted a missing path")
	}
	if _, err := c.SameFile(ctx, root, filepath.Join(root, "missing")); err == nil {
		t.Error("samefile accepted a missing path")
	}
	var out protocol.MkdirResponse
	if err := c.post(ctx, "/v1/mkdir", map[string]any{"dirs": []string{"/tmp"}, "bogus": 1}, &out); err == nil {
		t.Error("unknown field accepted")
	}
	if code, _ := ts.shutdown(); code != 0 {
		t.Errorf("exit = %d", code)
	}
}

func TestServerRejectsGarbageBootstrap(t *testing.T) {
	serverInR, clientInW := io.Pipe()
	clientOutR, serverOutW := io.Pipe()
	res := newServerResult()
	cfg := ServerConfig{
		Listen: "127.0.0.1:0", Version: "test",
		Stdin: serverInR, Stdout: serverOutW, Stderr: testLogWriter{t},
	}
	go func() { res.finish(Serve(context.Background(), cfg)) }()
	// Consume the server's stdout so its hello/error writes never block.
	go func() { _, _ = io.Copy(io.Discard, clientOutR) }()
	_, _ = clientInW.Write([]byte("garbage\n"))
	_ = clientInW.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	code, err := res.wait(ctx)
	if err != nil || code != 2 {
		t.Errorf("garbage bootstrap = code %d, err %v, want 2", code, err)
	}
}

func TestMissingRemoteBinaryClassification(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "ssh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\necho 'bash: catsyncd: command not found' >&2\nexit 127\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	tr := SSHTransport{Program: script, Host: "example", RemoteCatsyncd: "catsyncd", Listen: ":0"}
	_, err := Bootstrap(context.Background(), BootstrapConfig{
		Transport: tr, Host: "127.0.0.1", HandshakeTimeout: 2 * time.Second, CloseTimeout: 2 * time.Second,
	})
	if err == nil || !strings.Contains(err.Error(), protocol.CodeDaemonNotInstalled) {
		t.Fatalf("error = %v, want %s", err, protocol.CodeDaemonNotInstalled)
	}
}

func TestServerFeaturesReflectCapabilities(t *testing.T) {
	ts := startTestServer(t, func(cfg *ServerConfig) { cfg.SFTPPath = "" })
	f := ts.client.Features()
	if f.SFTP || f.Exec {
		t.Errorf("features = %+v, want sftp=false exec=false", f)
	}
	if !f.Manifest || !f.Metadata || !f.Hash || !f.Stat || !f.SameFile {
		t.Errorf("features = %+v", f)
	}
	if code, _ := ts.shutdown(); code != 0 {
		t.Errorf("exit = %d", code)
	}
}

func TestAtCapacity(t *testing.T) {
	s := &server{
		cfg:     ServerConfig{Limits: protocol.Limits{MaxConns: 2}},
		conns:   map[*trackedConn]struct{}{},
		pending: map[net.Conn]struct{}{},
	}
	if s.atCapacity() {
		t.Fatal("empty server at capacity")
	}
	s.pending[oneConn{}] = struct{}{}
	s.conns[&trackedConn{}] = struct{}{}
	if !s.atCapacity() {
		t.Fatal("server did not report capacity")
	}
}

type oneConn struct{}

func (oneConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (oneConn) Write([]byte) (int, error)        { return 0, io.EOF }
func (oneConn) Close() error                     { return nil }
func (oneConn) LocalAddr() net.Addr              { return nil }
func (oneConn) RemoteAddr() net.Addr             { return nil }
func (oneConn) SetDeadline(time.Time) error      { return nil }
func (oneConn) SetReadDeadline(time.Time) error  { return nil }
func (oneConn) SetWriteDeadline(time.Time) error { return nil }

func TestExecRejectsUnauthorizedCommands(t *testing.T) {
	rsyncPath := ResolveRsyncPath("")
	if rsyncPath == "" {
		t.Skip("rsync is not installed")
	}
	ts := startTestServer(t, func(cfg *ServerConfig) { cfg.RsyncPath = rsyncPath })
	c := ts.client
	if !c.Features().Exec {
		t.Fatal("exec feature not advertised despite a resolved rsync")
	}
	cases := []struct {
		program string
		args    []string
		want    string
	}{
		{"sh", []string{"-c", "true"}, protocol.CodeExecRejected},
		{"rsync", []string{"--server", "--rsh=sh", ".", "/tmp"}, protocol.CodeExecRejected},
		{"rsync", []string{"--server", "-log", ".", "relative"}, protocol.CodeExecRejected},
		{"rsync", []string{"--server", "-log", ".", "/tmp/a", "/tmp/b"}, protocol.CodeExecRejected},
	}
	for _, tc := range cases {
		sess, err := c.Exec(context.Background(), tc.program, tc.args)
		if err != nil {
			continue // a handshake-level rejection is fine too
		}
		_, werr := sess.Wait(context.Background())
		sess.Close()
		if werr == nil || !strings.Contains(werr.Error(), tc.want) {
			t.Errorf("%q %v: error = %v, want %s", tc.program, tc.args, werr, tc.want)
		}
	}
	if code, _ := ts.shutdown(); code != 0 {
		t.Errorf("exit = %d", code)
	}
}

func TestExecDisabledWithoutRsync(t *testing.T) {
	ts := startTestServer(t, func(cfg *ServerConfig) { cfg.RsyncPath = "" })
	if ts.client.Features().Exec {
		t.Fatal("exec advertised without a resolved rsync")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := ts.client.Exec(ctx, "rsync", []string{"--server", "-log", ".", "/tmp"}); err == nil {
		t.Error("exec handshake succeeded while the capability is disabled")
	}
	if code, _ := ts.shutdown(); code != 0 {
		t.Errorf("exit = %d", code)
	}
}
