//go:build unix

package executor

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dotfrankruan/CatSync/internal/daemon"
	"github.com/dotfrankruan/CatSync/internal/daemon/protocol"
	"github.com/dotfrankruan/CatSync/internal/state"
	"github.com/dotfrankruan/CatSync/internal/target"
)

// inProcDaemonTransport starts one real catsyncd Serve loop over pipes per
// bootstrap, so executor daemon tests exercise the real bootstrap, mTLS,
// RPC, and SFTP forwarding paths without an external process.
type inProcDaemonTransport struct {
	t        *testing.T
	sftpPath string
	limits   protocol.Limits
	mu       sync.Mutex
	sessions []*inProcDaemonSession
}

func (tr *inProcDaemonTransport) Start(ctx context.Context) (daemon.ControlSession, error) {
	serverInR, clientInW := io.Pipe()
	clientOutR, serverOutW := io.Pipe()
	done := make(chan struct{})
	var code int
	cfg := daemon.ServerConfig{
		Listen:           "127.0.0.1:0",
		Version:          "test",
		SFTPPath:         tr.sftpPath,
		Limits:           tr.limits,
		IdleTimeout:      5 * time.Second,
		DrainTimeout:     10 * time.Second,
		ControlTimeout:   5 * time.Second,
		HandshakeTimeout: 5 * time.Second,
		Stdin:            serverInR,
		Stdout:           serverOutW,
		Stderr:           io.Discard,
	}
	go func() {
		code = daemon.Serve(context.Background(), cfg)
		close(done)
	}()
	sess := &inProcDaemonSession{r: clientOutR, w: clientInW, done: done, code: &code}
	tr.mu.Lock()
	tr.sessions = append(tr.sessions, sess)
	tr.mu.Unlock()
	return sess, nil
}

func (tr *inProcDaemonTransport) waitAll(t *testing.T) {
	tr.mu.Lock()
	sessions := append([]*inProcDaemonSession(nil), tr.sessions...)
	tr.mu.Unlock()
	for _, s := range sessions {
		// Every bootstrap session must exit; an interrupted session exits
		// non-zero by design (lost control channel), a normal bye exits 0.
		_ = s.waitExit(t)
	}
}

type testDaemonLog struct{ t *testing.T }

func (l testDaemonLog) Write(p []byte) (int, error) {
	l.t.Logf("catsyncd: %s", strings.TrimSpace(string(p)))
	return len(p), nil
}

type inProcDaemonSession struct {
	r    io.ReadCloser
	w    io.WriteCloser
	done chan struct{}
	code *int
	once sync.Once
}

func (s *inProcDaemonSession) Read(p []byte) (int, error)  { return s.r.Read(p) }
func (s *inProcDaemonSession) Write(p []byte) (int, error) { return s.w.Write(p) }
func (s *inProcDaemonSession) Close() error {
	s.once.Do(func() { _ = s.w.Close(); _ = s.r.Close() })
	return nil
}
func (s *inProcDaemonSession) Wait(ctx context.Context) error {
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (s *inProcDaemonSession) waitExit(t *testing.T) int {
	t.Helper()
	select {
	case <-s.done:
		return *s.code
	case <-time.After(20 * time.Second):
		t.Fatal("catsyncd did not exit")
		return -1
	}
}

// installDaemonTransport replaces the SSH bootstrap factory for one test.
func installDaemonTransport(t *testing.T, tr *inProcDaemonTransport) {
	t.Helper()
	old := newDaemonControlTransport
	newDaemonControlTransport = func(o daemonTransportOptions) daemon.ControlTransport { return tr }
	t.Cleanup(func() {
		newDaemonControlTransport = old
		tr.waitAll(t)
	})
}

func daemonTestConfig(t *testing.T, direction target.Direction, src, remote string, backend state.Backend, jobs, streams int) Config {
	t.Helper()
	cfg := Config{
		Direction:       direction,
		SourceDir:       src,
		Dest:            target.Destination{User: "user", Host: "127.0.0.1", Path: remote},
		Backend:         backend,
		Verify:          state.VerifySize,
		Jobs:            jobs,
		Streams:         streams,
		LargeThreshold:  1 << 10,
		StateDir:        t.TempDir(),
		Transport:       "daemon",
		DaemonPort:      0,
		DaemonTransport: "tcp",
		DaemonBootstrap: "direct",
		RemoteCatsyncd:  "catsyncd",
	}
	return cfg
}

func writeDaemonFile(t *testing.T, path string, size int, mtime time.Time) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatal(err)
	}
}

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

func requireFileEqual(t *testing.T, a, b string) {
	t.Helper()
	da, err := os.ReadFile(a)
	if err != nil {
		t.Fatalf("read %s: %v", a, err)
	}
	db, err := os.ReadFile(b)
	if err != nil {
		t.Fatalf("read %s: %v", b, err)
	}
	if len(da) != len(db) || !strings.EqualFold(string(da), string(db)) {
		t.Fatalf("%s and %s differ (%d vs %d bytes)", a, b, len(da), len(db))
	}
	if fi, err := os.Stat(a); err == nil {
		if gi, err := os.Stat(b); err == nil {
			if fi.Mode().Perm() != gi.Mode().Perm() {
				t.Errorf("mode %v vs %v", fi.Mode().Perm(), gi.Mode().Perm())
			}
			if fi.ModTime().UnixNano() != gi.ModTime().UnixNano() {
				t.Errorf("mtime %d vs %d", fi.ModTime().UnixNano(), gi.ModTime().UnixNano())
			}
		}
	}
}

func TestDaemonNativeUploadDownload(t *testing.T) {
	sftpPath := daemon.ResolveSFTPPath("")
	if sftpPath == "" {
		t.Skip("sftp-server is not installed")
	}
	tr := &inProcDaemonTransport{t: t, sftpPath: sftpPath, limits: protocol.Limits{MaxSFTPConns: 8, MaxExecConns: 8, MaxControlConns: 8, MaxPendingHandshake: 8, MaxConns: 64}}
	installDaemonTransport(t, tr)

	local := t.TempDir()
	remote := filepath.Join(t.TempDir(), "remote")
	mtime := time.Unix(1_700_100_000, 123_456_789)
	writeDaemonFile(t, filepath.Join(local, "big.bin"), 256<<10, mtime)
	writeDaemonFile(t, filepath.Join(local, "sub", "nested.bin"), 128<<10, mtime)

	run, err := Sync(context.Background(), daemonTestConfig(t, target.LocalToRemote, local, remote, state.BackendMSCP, 2, 2))
	if err != nil {
		t.Fatalf("Sync upload: %v", err)
	}
	if run.Status != state.StatusCompleted {
		t.Fatalf("upload run = %s (%q)", run.Status, run.Error)
	}
	if run.Config.Transport != "daemon" {
		t.Errorf("recorded transport = %q", run.Config.Transport)
	}
	requireFileEqual(t, filepath.Join(local, "big.bin"), filepath.Join(remote, "big.bin"))
	requireFileEqual(t, filepath.Join(local, "sub", "nested.bin"), filepath.Join(remote, "sub", "nested.bin"))

	// Download the same tree through a fresh daemon session.
	localDest := t.TempDir()
	dl, err := Sync(context.Background(), daemonTestConfig(t, target.RemoteToLocal, localDest, remote, state.BackendMSCP, 2, 2))
	if err != nil {
		t.Fatalf("Sync download: %v", err)
	}
	if dl.Status != state.StatusCompleted {
		t.Fatalf("download run = %s (%q)", dl.Status, dl.Error)
	}
	requireFileEqual(t, filepath.Join(remote, "big.bin"), filepath.Join(localDest, "big.bin"))
	requireFileEqual(t, filepath.Join(remote, "sub", "nested.bin"), filepath.Join(localDest, "sub", "nested.bin"))
}

func TestDaemonVerifySHA256(t *testing.T) {
	sftpPath := daemon.ResolveSFTPPath("")
	if sftpPath == "" {
		t.Skip("sftp-server is not installed")
	}
	tr := &inProcDaemonTransport{t: t, sftpPath: sftpPath, limits: protocol.Limits{MaxSFTPConns: 8, MaxExecConns: 8, MaxControlConns: 8, MaxPendingHandshake: 8, MaxConns: 64}}
	installDaemonTransport(t, tr)
	local := t.TempDir()
	remote := t.TempDir()
	writeDaemonFile(t, filepath.Join(local, "one.bin"), 300<<10, time.Unix(1_700_000_000, 0))
	cfg := daemonTestConfig(t, target.LocalToRemote, local, remote, state.BackendMSCP, 2, 2)
	cfg.Verify = state.VerifySHA256
	run, err := Sync(context.Background(), cfg)
	if err != nil {
		if run != nil {
			for _, task := range run.Tasks {
				t.Logf("task %s: status=%s error=%s", task.Path, task.Status, task.Error)
			}
		}
		t.Fatalf("Sync: %v", err)
	}
	if run.Status != state.StatusCompleted {
		t.Fatalf("run = %s (%q)", run.Status, run.Error)
	}
	requireFileEqual(t, filepath.Join(local, "one.bin"), filepath.Join(remote, "one.bin"))
}

func TestDaemonCapacityPrecheckFailsBeforeAnyWrite(t *testing.T) {
	sftpPath := daemon.ResolveSFTPPath("")
	if sftpPath == "" {
		t.Skip("sftp-server is not installed")
	}
	tr := &inProcDaemonTransport{t: t, sftpPath: sftpPath, limits: protocol.Limits{MaxSFTPConns: 2, MaxExecConns: 8, MaxControlConns: 8, MaxPendingHandshake: 8, MaxConns: 32}}
	installDaemonTransport(t, tr)
	local := t.TempDir()
	for _, name := range []string{"a.bin", "b.bin", "c.bin", "d.bin"} {
		writeDaemonFile(t, filepath.Join(local, name), 64<<10, time.Now())
	}
	remote := filepath.Join(t.TempDir(), "remote")
	cfg := daemonTestConfig(t, target.LocalToRemote, local, remote, state.BackendMSCP, 4, 4)
	_, err := Sync(context.Background(), cfg)
	if err == nil || !strings.Contains(err.Error(), protocol.CodeConnectionLimit) {
		t.Fatalf("Sync = %v, want %s", err, protocol.CodeConnectionLimit)
	}
	if _, statErr := os.Stat(remote); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("precheck failure wrote remote data: %v", statErr)
	}
	store, err := state.NewStore(cfg.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	runs, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 0 {
		t.Errorf("precheck failure persisted %d run(s)", len(runs))
	}
}

func TestDaemonNonUTF8PathRejected(t *testing.T) {
	sftpPath := daemon.ResolveSFTPPath("")
	if sftpPath == "" {
		t.Skip("sftp-server is not installed")
	}
	tr := &inProcDaemonTransport{t: t, sftpPath: sftpPath, limits: protocol.Limits{MaxSFTPConns: 8, MaxExecConns: 8, MaxControlConns: 8, MaxPendingHandshake: 8, MaxConns: 64}}
	installDaemonTransport(t, tr)
	local := t.TempDir()
	if err := os.WriteFile(filepath.Join(local, string([]byte{'b', 0xff})), []byte("x"), 0o644); err != nil {
		t.Skipf("filesystem rejects non-UTF-8 names: %v", err)
	}
	remote := filepath.Join(t.TempDir(), "remote")
	_, err := Sync(context.Background(), daemonTestConfig(t, target.LocalToRemote, local, remote, state.BackendMSCP, 2, 2))
	if err == nil || !strings.Contains(err.Error(), protocol.CodeUnsupportedPathEncoding) {
		t.Fatalf("Sync = %v, want %s", err, protocol.CodeUnsupportedPathEncoding)
	}
}

func TestDaemonMissingSFTPRejected(t *testing.T) {
	tr := &inProcDaemonTransport{t: t, sftpPath: "", limits: protocol.Limits{MaxSFTPConns: 8, MaxExecConns: 8, MaxControlConns: 8, MaxPendingHandshake: 8, MaxConns: 64}}
	installDaemonTransport(t, tr)
	local := t.TempDir()
	writeDaemonFile(t, filepath.Join(local, "a.bin"), 4096, time.Now())
	remote := filepath.Join(t.TempDir(), "remote")
	_, err := Sync(context.Background(), daemonTestConfig(t, target.LocalToRemote, local, remote, state.BackendMSCP, 2, 2))
	if err == nil || !strings.Contains(err.Error(), protocol.CodeFeatureUnavailable) {
		t.Fatalf("Sync = %v, want %s", err, protocol.CodeFeatureUnavailable)
	}
}

func TestDaemonPlanClosesSession(t *testing.T) {
	sftpPath := daemon.ResolveSFTPPath("")
	if sftpPath == "" {
		t.Skip("sftp-server is not installed")
	}
	tr := &inProcDaemonTransport{t: t, sftpPath: sftpPath, limits: protocol.Limits{MaxSFTPConns: 8, MaxExecConns: 8, MaxControlConns: 8, MaxPendingHandshake: 8, MaxConns: 64}}
	installDaemonTransport(t, tr)
	local := t.TempDir()
	remote := filepath.Join(t.TempDir(), "remote")
	writeDaemonFile(t, filepath.Join(local, "a.bin"), 8192, time.Now())
	cfg := daemonTestConfig(t, target.LocalToRemote, local, remote, state.BackendMSCP, 2, 2)
	cfg.DryRun = true
	out, err := Plan(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if out == nil || len(out.Result.Decisions) != 1 {
		t.Fatalf("plan = %+v", out)
	}
	tr.mu.Lock()
	sessions := append([]*inProcDaemonSession(nil), tr.sessions...)
	tr.mu.Unlock()
	for _, s := range sessions {
		if code := s.waitExit(t); code != 0 {
			t.Errorf("plan left catsyncd exit = %d", code)
		}
	}
}

func TestDaemonResumeAfterInterrupt(t *testing.T) {
	sftpPath := daemon.ResolveSFTPPath("")
	if sftpPath == "" {
		t.Skip("sftp-server is not installed")
	}
	tr := &inProcDaemonTransport{t: t, sftpPath: sftpPath, limits: protocol.Limits{MaxSFTPConns: 8, MaxExecConns: 8, MaxControlConns: 8, MaxPendingHandshake: 8, MaxConns: 64}}
	installDaemonTransport(t, tr)
	local := t.TempDir()
	remote := t.TempDir()
	writeDaemonFile(t, filepath.Join(local, "big.bin"), 64<<20, time.Unix(1_700_000_000, 0))
	cfg := daemonTestConfig(t, target.LocalToRemote, local, remote, state.BackendMSCP, 1, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	run, err := Sync(ctx, cfg)
	if err == nil {
		t.Skip("transfer finished before the interrupt; nothing to resume")
	}
	if run == nil {
		t.Fatalf("Sync = %v without a run", err)
	}
	for _, task := range run.Tasks {
		t.Logf("interrupted task %s: status=%s error=%s", task.Path, task.Status, task.Error)
	}
	// A fresh daemon session and fresh ephemeral identity must finish it.
	resumed, err := Resume(context.Background(), Config{StateDir: cfg.StateDir, Transport: "daemon", DaemonPort: 0, RemoteCatsyncd: "catsyncd"}, run.ID)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if resumed.Status != state.StatusCompleted {
		t.Fatalf("resume = %s (%q)", resumed.Status, resumed.Error)
	}
	requireFileEqual(t, filepath.Join(local, "big.bin"), filepath.Join(remote, "big.bin"))
}
