//go:build unix

package executor

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dotfrankruan/CatSync/internal/backend/multistream"
	"github.com/dotfrankruan/CatSync/internal/daemon"
	"github.com/dotfrankruan/CatSync/internal/daemon/protocol"
	"github.com/dotfrankruan/CatSync/internal/state"
	"github.com/dotfrankruan/CatSync/internal/target"
)

// inProcDaemonTransport starts one real catsyncd Serve loop over pipes per
// bootstrap, so executor daemon tests exercise the real bootstrap, mTLS,
// RPC, and SFTP forwarding paths without an external process.
type inProcDaemonTransport struct {
	t         *testing.T
	sftpPath  string
	rsyncPath string
	transport string
	limits    protocol.Limits
	mu        sync.Mutex
	sessions  []*inProcDaemonSession
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
		RsyncPath:        tr.rsyncPath,
		Transport:        tr.transport,
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

var (
	buildCatsyncOnce sync.Once
	builtCatsyncPath string
	buildCatsyncErr  error
)

// realCatsyncBinary builds cmd/catsync once so rsync's -e helper can run the
// actual hidden rsh command instead of the test binary.
func realCatsyncBinary(t *testing.T) string {
	t.Helper()
	buildCatsyncOnce.Do(func() {
		dir, err := os.MkdirTemp("", "catsync-bin-")
		if err != nil {
			buildCatsyncErr = err
			return
		}
		out := filepath.Join(dir, "catsync")
		cmd := exec.Command("go", "build", "-o", out, "github.com/dotfrankruan/CatSync/cmd/catsync")
		cmd.Dir = repoRoot(t)
		if data, err := cmd.CombinedOutput(); err != nil {
			buildCatsyncErr = fmt.Errorf("build catsync: %v: %s", err, data)
			return
		}
		builtCatsyncPath = out
	})
	if buildCatsyncErr != nil {
		t.Fatalf("%v", buildCatsyncErr)
	}
	return builtCatsyncPath
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found")
		}
		dir = parent
	}
}

func TestDaemonMixedTreeUsesSingleBootstrap(t *testing.T) {
	sftpPath := daemon.ResolveSFTPPath("")
	rsyncPath := daemon.ResolveRsyncPath("")
	if sftpPath == "" || rsyncPath == "" {
		t.Skip("sftp-server or rsync is not installed")
	}
	exe := realCatsyncBinary(t)
	oldExe := catsyncExecutable
	catsyncExecutable = func() (string, error) { return exe, nil }
	t.Cleanup(func() { catsyncExecutable = oldExe })

	tr := &inProcDaemonTransport{t: t, sftpPath: sftpPath, rsyncPath: rsyncPath, limits: protocol.Limits{MaxSFTPConns: 8, MaxExecConns: 8, MaxControlConns: 8, MaxPendingHandshake: 8, MaxConns: 64}}
	installDaemonTransport(t, tr)

	// Any real ssh invocation would fail loudly; daemon mode must not spawn
	// one for rsync or metadata.
	badSSH := filepath.Join(t.TempDir(), "ssh")
	if err := os.WriteFile(badSSH, []byte("#!/bin/sh\necho 'ssh must not run in daemon mode' >&2\nexit 99\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CATSYNC_SSH", badSSH)

	local := t.TempDir()
	mtime := time.Unix(1_700_200_000, 987_654_321)
	writeDaemonFile(t, filepath.Join(local, "big.bin"), 8<<10, mtime)
	writeDaemonFile(t, filepath.Join(local, "small1.txt"), 100, mtime)
	writeDaemonFile(t, filepath.Join(local, "small2.txt"), 200, mtime)
	writeDaemonFile(t, filepath.Join(local, "sub", "nested.txt"), 300, mtime)
	writeDaemonFile(t, filepath.Join(local, "unicode-文件.txt"), 50, mtime)
	writeDaemonFile(t, filepath.Join(local, "with space.txt"), 60, mtime)
	if err := os.WriteFile(filepath.Join(local, "empty.bin"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	remote := t.TempDir()
	cfg := daemonTestConfig(t, target.LocalToRemote, local, remote, state.BackendAuto, 2, 2)
	cfg.LargeThreshold = 1 << 10
	cfg.Verify = state.VerifySHA256
	run, err := Sync(context.Background(), cfg)
	if err != nil {
		if run != nil {
			for _, task := range run.Tasks {
				t.Logf("task %s (%s): %s %s", task.Path, task.Backend, task.Status, task.Error)
			}
		}
		t.Fatalf("Sync mixed tree: %v", err)
	}
	if run.Status != state.StatusCompleted {
		t.Fatalf("run = %s (%q)", run.Status, run.Error)
	}
	for _, name := range []string{"big.bin", "small1.txt", "small2.txt", "sub/nested.txt", "unicode-文件.txt", "with space.txt", "empty.bin"} {
		requireFileEqual(t, filepath.Join(local, filepath.FromSlash(name)), filepath.Join(remote, filepath.FromSlash(name)))
	}
	tr.mu.Lock()
	sessions := len(tr.sessions)
	tr.mu.Unlock()
	if sessions != 1 {
		t.Fatalf("bootstrap sessions = %d, want exactly 1 for the whole run", sessions)
	}
}

// daemonMixedHarness builds a real mixed tree (native large + rsync small +
// special names) and returns the local root.
func daemonMixedHarness(t *testing.T) (string, string, *inProcDaemonTransport) {
	t.Helper()
	sftpPath := daemon.ResolveSFTPPath("")
	rsyncPath := daemon.ResolveRsyncPath("")
	if sftpPath == "" || rsyncPath == "" {
		t.Skip("sftp-server or rsync is not installed")
	}
	exe := realCatsyncBinary(t)
	oldExe := catsyncExecutable
	catsyncExecutable = func() (string, error) { return exe, nil }
	t.Cleanup(func() { catsyncExecutable = oldExe })
	tr := &inProcDaemonTransport{t: t, sftpPath: sftpPath, rsyncPath: rsyncPath, limits: protocol.Limits{MaxSFTPConns: 8, MaxExecConns: 8, MaxControlConns: 8, MaxPendingHandshake: 8, MaxConns: 64}}
	installDaemonTransport(t, tr)
	local := t.TempDir()
	mtime := time.Unix(1_700_300_000, 111_222_333)
	writeDaemonFile(t, filepath.Join(local, "big.bin"), 8<<10, mtime)
	writeDaemonFile(t, filepath.Join(local, "small1.txt"), 100, mtime)
	writeDaemonFile(t, filepath.Join(local, "sub", "nested.txt"), 300, mtime)
	writeDaemonFile(t, filepath.Join(local, "unicode-文件.txt"), 50, mtime)
	writeDaemonFile(t, filepath.Join(local, "with\nnewline.txt"), 70, mtime)
	writeDaemonFile(t, filepath.Join(local, "with space.txt"), 60, mtime)
	return local, t.TempDir(), tr
}

func TestDaemonMixedTreeDownload(t *testing.T) {
	remote, localDest, _ := daemonMixedHarness(t)
	cfg := daemonTestConfig(t, target.RemoteToLocal, localDest, remote, state.BackendAuto, 2, 2)
	cfg.LargeThreshold = 1 << 10
	run, err := Sync(context.Background(), cfg)
	if err != nil {
		if run != nil {
			for _, task := range run.Tasks {
				t.Logf("task %s (%s): %s %s", task.Path, task.Backend, task.Status, task.Error)
			}
		}
		t.Fatalf("Sync download: %v", err)
	}
	if run.Status != state.StatusCompleted {
		t.Fatalf("run = %s (%q)", run.Status, run.Error)
	}
	for _, name := range []string{"big.bin", "small1.txt", "sub/nested.txt", "unicode-文件.txt", "with\nnewline.txt", "with space.txt"} {
		requireFileEqual(t, filepath.Join(remote, filepath.FromSlash(name)), filepath.Join(localDest, filepath.FromSlash(name)))
	}
}

func TestDaemonMirrorDeletesExtraneous(t *testing.T) {
	local, remote, _ := daemonMixedHarness(t)
	cfg := daemonTestConfig(t, target.LocalToRemote, local, remote, state.BackendAuto, 2, 2)
	cfg.LargeThreshold = 1 << 10
	cfg.Mirror = true
	run, err := Sync(context.Background(), cfg)
	if err != nil {
		t.Fatalf("initial sync: %v", err)
	}
	if run.Status != state.StatusCompleted {
		t.Fatalf("run = %s (%q)", run.Status, run.Error)
	}
	extra := filepath.Join(remote, "extraneous.txt")
	if err := os.WriteFile(extra, []byte("delete me"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg2 := daemonTestConfig(t, target.LocalToRemote, local, remote, state.BackendAuto, 2, 2)
	cfg2.LargeThreshold = 1 << 10
	cfg2.Mirror = true
	if _, err := Sync(context.Background(), cfg2); err != nil {
		t.Fatalf("mirror sync: %v", err)
	}
	if _, err := os.Stat(extra); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("mirror did not delete the extraneous remote file: %v", err)
	}
}

func TestDaemonForwardTargets(t *testing.T) {
	cfg := Config{Transport: "daemon", DaemonBootstrap: "ssh-forward", DaemonPort: 43411}
	e := &Executor{cfg: cfg}
	listen, addr, port, err := e.daemonBootstrapTargets()
	if err != nil {
		t.Fatalf("forward targets: %v", err)
	}
	if listen != "127.0.0.1:43411" {
		t.Errorf("listen = %q", listen)
	}
	if port < 1 || port > 65535 || addr != fmt.Sprintf("127.0.0.1:%d", port) {
		t.Errorf("forward port=%d addr=%q", port, addr)
	}
	bad := []Config{
		{Transport: "daemon", DaemonBootstrap: "ssh-forward", DaemonPort: 0},
		{Transport: "daemon", DaemonBootstrap: "ssh-forward", DaemonPort: 43411, DaemonListen: "0.0.0.0:43411"},
	}
	for _, c := range bad {
		ec := &Executor{cfg: c}
		if _, _, _, err := ec.daemonBootstrapTargets(); err == nil {
			t.Errorf("accepted invalid forward config %+v", c)
		}
	}
	direct := &Executor{cfg: Config{Transport: "daemon", DaemonBootstrap: "direct", DaemonPort: 0}}
	l, a, p, err := direct.daemonBootstrapTargets()
	if err != nil || p != 0 || l != ":0" || a != "" {
		t.Errorf("direct targets = %q %q %d %v", l, a, p, err)
	}
}

// daemonTransportTestConfig is daemonTestConfig with an explicit data
// transport.
func daemonTransportTestConfig(t *testing.T, transport string, direction target.Direction, src, remote string, backend state.Backend) Config {
	t.Helper()
	cfg := daemonTestConfig(t, direction, src, remote, backend, 2, 2)
	cfg.LargeThreshold = 1 << 10
	cfg.DaemonTransport = transport
	cfg.DaemonPort = 0
	return cfg
}

func TestDaemonQUICMixedTreeMatchesTCP(t *testing.T) {
	sftpPath := daemon.ResolveSFTPPath("")
	rsyncPath := daemon.ResolveRsyncPath("")
	if sftpPath == "" || rsyncPath == "" {
		t.Skip("sftp-server or rsync is not installed")
	}
	exe := realCatsyncBinary(t)
	oldExe := catsyncExecutable
	catsyncExecutable = func() (string, error) { return exe, nil }
	t.Cleanup(func() { catsyncExecutable = oldExe })

	// The same local tree is uploaded once over TCP and once over QUIC; the
	// destination trees must be byte-identical and keep metadata.
	local := t.TempDir()
	mtime := time.Unix(1_700_400_000, 222_333_444)
	writeDaemonFile(t, filepath.Join(local, "big.bin"), 8<<10, mtime)
	writeDaemonFile(t, filepath.Join(local, "small1.txt"), 100, mtime)
	writeDaemonFile(t, filepath.Join(local, "sub", "nested.txt"), 300, mtime)
	writeDaemonFile(t, filepath.Join(local, "unicode-文件.txt"), 50, mtime)
	writeDaemonFile(t, filepath.Join(local, "with space.txt"), 60, mtime)

	upload := func(transport string) string {
		tr := &inProcDaemonTransport{t: t, sftpPath: sftpPath, rsyncPath: rsyncPath, transport: transport, limits: protocol.Limits{MaxSFTPConns: 8, MaxExecConns: 8, MaxControlConns: 8, MaxPendingHandshake: 8, MaxConns: 64}}
		installDaemonTransport(t, tr)
		dest := t.TempDir()
		cfg := daemonTransportTestConfig(t, transport, target.LocalToRemote, local, dest, state.BackendAuto)
		if transport == "quic" {
			cfg.Verify = state.VerifySHA256
		}
		run, err := Sync(context.Background(), cfg)
		if err != nil {
			if run != nil {
				for _, task := range run.Tasks {
					t.Logf("[%s] task %s (%s): %s %s", transport, task.Path, task.Backend, task.Status, task.Error)
				}
			}
			t.Fatalf("[%s] sync: %v", transport, err)
		}
		if run.Status != state.StatusCompleted {
			t.Fatalf("[%s] run = %s (%q)", transport, run.Status, run.Error)
		}
		return dest
	}
	tcpDest := upload("tcp")
	quicDest := upload("quic")
	for _, name := range []string{"big.bin", "small1.txt", "sub/nested.txt", "unicode-文件.txt", "with space.txt"} {
		rel := filepath.FromSlash(name)
		requireFileEqual(t, filepath.Join(tcpDest, rel), filepath.Join(quicDest, rel))
		requireFileEqual(t, filepath.Join(local, rel), filepath.Join(quicDest, rel))
	}
	// Download the QUIC destination tree back over QUIC.
	tr := &inProcDaemonTransport{t: t, sftpPath: sftpPath, rsyncPath: rsyncPath, transport: "quic", limits: protocol.Limits{MaxSFTPConns: 8, MaxExecConns: 8, MaxControlConns: 8, MaxPendingHandshake: 8, MaxConns: 64}}
	installDaemonTransport(t, tr)
	down := t.TempDir()
	if _, err := Sync(context.Background(), daemonTransportTestConfig(t, "quic", target.RemoteToLocal, down, quicDest, state.BackendAuto)); err != nil {
		t.Fatalf("quic download: %v", err)
	}
	for _, name := range []string{"big.bin", "small1.txt", "sub/nested.txt", "unicode-文件.txt", "with space.txt"} {
		rel := filepath.FromSlash(name)
		requireFileEqual(t, filepath.Join(quicDest, rel), filepath.Join(down, rel))
	}
}

func TestDaemonQUICMirrorAndResume(t *testing.T) {
	sftpPath := daemon.ResolveSFTPPath("")
	rsyncPath := daemon.ResolveRsyncPath("")
	if sftpPath == "" || rsyncPath == "" {
		t.Skip("sftp-server or rsync is not installed")
	}
	exe := realCatsyncBinary(t)
	oldExe := catsyncExecutable
	catsyncExecutable = func() (string, error) { return exe, nil }
	t.Cleanup(func() { catsyncExecutable = oldExe })
	tr := &inProcDaemonTransport{t: t, sftpPath: sftpPath, rsyncPath: rsyncPath, transport: "quic", limits: protocol.Limits{MaxSFTPConns: 8, MaxExecConns: 8, MaxControlConns: 8, MaxPendingHandshake: 8, MaxConns: 64}}
	installDaemonTransport(t, tr)
	local := t.TempDir()
	remote := t.TempDir()
	writeDaemonFile(t, filepath.Join(local, "big.bin"), 8<<10, time.Unix(1_700_500_000, 0))
	writeDaemonFile(t, filepath.Join(local, "small.txt"), 100, time.Unix(1_700_500_000, 0))
	cfg := daemonTransportTestConfig(t, "quic", target.LocalToRemote, local, remote, state.BackendAuto)
	cfg.Mirror = true
	if _, err := Sync(context.Background(), cfg); err != nil {
		t.Fatalf("quic mirror: %v", err)
	}
	extra := filepath.Join(remote, "extra.txt")
	if err := os.WriteFile(extra, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg2 := daemonTransportTestConfig(t, "quic", target.LocalToRemote, local, remote, state.BackendAuto)
	cfg2.Mirror = true
	if _, err := Sync(context.Background(), cfg2); err != nil {
		t.Fatalf("quic mirror second run: %v", err)
	}
	if _, err := os.Stat(extra); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("quic mirror did not delete extraneous file: %v", err)
	}
}

// backendSFTPConfig builds a multistream config whose data plane is the
// daemon transport, for direct backend checkpoint-reuse tests.
func backendSFTPConfig(t *testing.T, tr *inProcDaemonTransport, transport string) (*daemon.Client, func()) {
	t.Helper()
	sess, err := tr.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	client, err := daemon.Bootstrap(context.Background(), daemon.BootstrapConfig{
		Transport: staticControlTransport{sess: sess}, Host: "127.0.0.1",
		DataTransport: transport, HandshakeTimeout: 10 * time.Second,
		ControlTimeout: 10 * time.Second, CloseTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	return client, func() { _ = client.Close() }
}

type staticControlTransport struct{ sess daemon.ControlSession }

func (t staticControlTransport) Start(context.Context) (daemon.ControlSession, error) {
	return t.sess, nil
}

func TestDaemonCheckpointChunkReuse(t *testing.T) {
	sftpPath := daemon.ResolveSFTPPath("")
	if sftpPath == "" {
		t.Skip("sftp-server is not installed")
	}
	for _, transport := range []string{"tcp", "quic"} {
		t.Run(transport, func(t *testing.T) {
			tr := &inProcDaemonTransport{t: t, sftpPath: sftpPath, transport: transport,
				limits: protocol.Limits{MaxSFTPConns: 8, MaxExecConns: 8, MaxControlConns: 8, MaxPendingHandshake: 8, MaxConns: 64}}
			installDaemonTransport(t, tr)
			client, closeClient := backendSFTPConfig(t, tr, transport)
			defer closeClient()
			local := t.TempDir()
			remoteDir := t.TempDir()
			if _, err := client.Mkdir(context.Background(), []string{remoteDir}); err != nil {
				t.Fatal(err)
			}
			const size = 1 << 20
			const chunk = 64 << 10
			src := filepath.Join(local, "chunks.bin")
			data := randomBytes(t, size)
			if err := os.WriteFile(src, data, 0o644); err != nil {
				t.Fatal(err)
			}
			cfg := multistream.SFTPConfig{
				Dest: target.Destination{User: "u", Host: "127.0.0.1", Path: remoteDir},
				SetMtime: func(ctx context.Context, p string, tm time.Time) error {
					return client.MetadataMtime(ctx, p, nil, tm.UnixNano())
				},
				Dial: func(ctx context.Context) (io.ReadWriteCloser, func(), error) {
					c, err := client.DialSFTP(ctx)
					if err != nil {
						return nil, nil, err
					}
					return c, func() { _ = c.Close() }, nil
				},
			}
			ckpt := filepath.Join(local, "file.ckpt")
			remoteFinal := filepath.Join(remoteDir, "chunks.bin")
			ctx, cancel := context.WithCancel(context.Background())
			opts := multistream.TransferOptions{
				SFTP: cfg, Streams: 2, ChunkSize: chunk, CheckpointPath: ckpt,
				OnChunkComplete: func(done, total int) {
					if done == 4 {
						cancel()
					}
				},
			}
			_, err := multistream.Transfer(ctx, src, "chunks.bin", remoteFinal, opts)
			cancel()
			if err == nil {
				t.Fatal("interrupted transfer unexpectedly succeeded")
			}
			ck, err := multistream.LoadCheckpoint(ckpt)
			if err != nil {
				t.Fatalf("load checkpoint: %v", err)
			}
			if len(ck.Completed) < 4 {
				t.Fatalf("completed chunks = %v, want at least 4", ck.Completed)
			}
			completedBytes := uint64(0)
			for _, idx := range ck.Completed {
				start := uint64(idx) * chunk
				end := start + chunk
				if end > size {
					end = size
				}
				completedBytes += end - start
			}
			if completedBytes >= size {
				t.Fatalf("checkpoint already covers the whole file (%d bytes)", completedBytes)
			}
			var retransferred atomic.Uint64
			resumeOpts := multistream.TransferOptions{
				SFTP: cfg, Streams: 2, ChunkSize: chunk, CheckpointPath: ckpt,
				OnProgress: func(n uint64) { retransferred.Add(n) },
			}
			res, err := multistream.Resume(context.Background(), src, "chunks.bin", ckpt, remoteFinal, resumeOpts)
			if err != nil {
				t.Fatalf("resume: %v", err)
			}
			if !res.Published {
				t.Fatal("resume did not publish")
			}
			want := uint64(size) - completedBytes
			if got := retransferred.Load(); got != want {
				t.Fatalf("resume retransferred %d bytes, want exactly %d (checkpoint must cover %d)", got, want, completedBytes)
			}
			final, err := os.ReadFile(remoteFinal)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(final, data) {
				t.Fatal("resumed file content differs")
			}
		})
	}
}

func TestDaemonQUICResumeAfterInterrupt(t *testing.T) {
	sftpPath := daemon.ResolveSFTPPath("")
	if sftpPath == "" {
		t.Skip("sftp-server is not installed")
	}
	tr := &inProcDaemonTransport{t: t, sftpPath: sftpPath, transport: "quic",
		limits: protocol.Limits{MaxSFTPConns: 8, MaxExecConns: 8, MaxControlConns: 8, MaxPendingHandshake: 8, MaxConns: 64}}
	installDaemonTransport(t, tr)
	local := t.TempDir()
	remote := t.TempDir()
	writeDaemonFile(t, filepath.Join(local, "big.bin"), 64<<20, time.Unix(1_700_700_000, 0))
	cfg := daemonTransportTestConfig(t, "quic", target.LocalToRemote, local, remote, state.BackendMSCP)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	run, err := Sync(ctx, cfg)
	if err == nil {
		t.Skip("transfer finished before the interrupt; nothing to resume")
	}
	if run == nil {
		t.Skipf("interrupt landed before the run was created: %v", err)
	}
	resumed, err := Resume(context.Background(), Config{StateDir: cfg.StateDir}, run.ID)
	if err != nil {
		t.Fatalf("QUIC resume: %v", err)
	}
	if resumed.Status != state.StatusCompleted {
		t.Fatalf("QUIC resume = %s (%q)", resumed.Status, resumed.Error)
	}
	requireFileEqual(t, filepath.Join(local, "big.bin"), filepath.Join(remote, "big.bin"))
}

func TestDaemonPublishNoClobberCrashWindows(t *testing.T) {
	sftpPath := daemon.ResolveSFTPPath("")
	if sftpPath == "" {
		t.Skip("sftp-server is not installed")
	}
	for _, transport := range []string{"tcp", "quic"} {
		t.Run(transport, func(t *testing.T) {
			tr := &inProcDaemonTransport{t: t, sftpPath: sftpPath, transport: transport,
				limits: protocol.Limits{MaxSFTPConns: 8, MaxExecConns: 8, MaxControlConns: 8, MaxPendingHandshake: 8, MaxConns: 64}}
			installDaemonTransport(t, tr)
			client, closeClient := backendSFTPConfig(t, tr, transport)
			defer closeClient()
			local := t.TempDir()
			remoteDir := t.TempDir()
			if _, err := client.Mkdir(context.Background(), []string{remoteDir}); err != nil {
				t.Fatal(err)
			}
			cfg := multistream.SFTPConfig{
				Dest: target.Destination{User: "u", Host: "127.0.0.1", Path: remoteDir},
				SetMtime: func(ctx context.Context, p string, tm time.Time) error {
					return client.MetadataMtime(ctx, p, nil, tm.UnixNano())
				},
				SameFile: func(ctx context.Context, a, b string) (bool, error) {
					return client.SameFile(ctx, a, b)
				},
				Dial: func(ctx context.Context) (io.ReadWriteCloser, func(), error) {
					c, err := client.DialSFTP(ctx)
					if err != nil {
						return nil, nil, err
					}
					return c, func() { _ = c.Close() }, nil
				},
			}
			const size = 256 << 10
			const chunk = 32 << 10
			data := randomBytes(t, size)
			opts := func(ckpt string) multistream.TransferOptions {
				return multistream.TransferOptions{SFTP: cfg, Streams: 2, ChunkSize: chunk, CheckpointPath: ckpt}
			}

			// A pre-existing final must never be overwritten.
			src := filepath.Join(local, "a.bin")
			if err := os.WriteFile(src, data, 0o644); err != nil {
				t.Fatal(err)
			}
			finalA := filepath.Join(remoteDir, "a.bin")
			if err := os.WriteFile(finalA, []byte("foreign"), 0o644); err != nil {
				t.Fatal(err)
			}
			_, err := multistream.Transfer(context.Background(), src, "a.bin", finalA, opts(filepath.Join(local, "a.ckpt")))
			if err == nil || !strings.Contains(err.Error(), "already exists") {
				t.Fatalf("transfer onto existing final = %v, want an already-exists refusal", err)
			}
			if got, _ := os.ReadFile(finalA); string(got) != "foreign" {
				t.Fatalf("existing final was clobbered: %q", got)
			}

			// Normal publish, then simulate a crash after the hardlink publish
			// but before the run state was saved: Resume must recover without
			// copying and must verify the existing final.
			src = filepath.Join(local, "b.bin")
			if err := os.WriteFile(src, data, 0o644); err != nil {
				t.Fatal(err)
			}
			finalB := filepath.Join(remoteDir, "b.bin")
			ckpt := filepath.Join(local, "b.ckpt")
			first, err := multistream.Transfer(context.Background(), src, "b.bin", finalB, opts(ckpt))
			if err != nil {
				t.Fatalf("Transfer: %v", err)
			}
			copies := 0
			recovery := opts(ckpt)
			recovery.OnChunkComplete = func(done, total int) { copies++ }
			res, err := multistream.Resume(context.Background(), src, "b.bin", ckpt, finalB, recovery)
			if err != nil {
				t.Fatalf("Resume recovery: %v", err)
			}
			if !res.Recovered || res.TempPath != first.TempPath {
				t.Fatalf("recovery = %+v (recovered=%v), want published recovery of %q", res, res.Recovered, first.TempPath)
			}
			if copies != 0 {
				t.Fatalf("recovered publish re-copied %d chunks", copies)
			}
			if got, _ := os.ReadFile(finalB); !bytes.Equal(got, data) {
				t.Fatal("recovered final content differs")
			}
			fi, err := os.Lstat(finalB)
			if err != nil || fi.Mode().Perm() != 0o644 {
				t.Fatalf("recovered final mode = %v, %v", fi.Mode(), err)
			}

			// Hardlink exists but a different object now occupies the final
			// path: resume must refuse instead of overwriting it.
			if err := os.Remove(finalB); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(finalB, []byte("foreign-final"), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := multistream.Resume(context.Background(), src, "b.bin", ckpt, finalB, opts(ckpt)); err == nil || !errors.Is(err, multistream.ErrPublishConflict) {
				t.Fatalf("resume onto a foreign final = %v, want ErrPublishConflict", err)
			}
			if got, _ := os.ReadFile(finalB); string(got) != "foreign-final" {
				t.Fatalf("foreign final was overwritten: %q", got)
			}
		})
	}
}
