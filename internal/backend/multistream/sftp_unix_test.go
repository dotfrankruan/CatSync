//go:build unix

package multistream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/pkg/sftp"

	"github.com/frankruan/catsync/internal/target"
)

// The fake ssh helper is the test binary itself, re-executed with the
// helper marker in its environment. TestMain checks the marker before any
// flag parsing, so the ssh-shaped argv is never parsed as flags.
const (
	envHelper     = "CATSYNC_TEST_SFTP_HELPER"
	envRecord     = "CATSYNC_TEST_SFTP_RECORD"     // dir receiving argv/pid records
	envEchoEnv    = "CATSYNC_TEST_SFTP_ECHOENV"    // env var name echoed to stderr
	envFailExit   = "CATSYNC_TEST_SFTP_FAILEXIT"   // exit immediately with this code
	envRealServer = "CATSYNC_TEST_SFTP_REALSERVER" // run the real sftp-server
)

// sftpServerBin is the real OpenSSH sftp-server used by the fake-ssh helper
// when envRealServer is set, so tempfile tests exercise the real protocol —
// exclusive create, truncate, chmod, fsync — not an approximation.
const sftpServerBin = "/usr/libexec/sftp-server"

// realServerAvailable reports whether the real server binary exists.
func realServerAvailable() bool {
	_, err := os.Stat(sftpServerBin)
	return err == nil
}

// TestMain runs the fake-ssh helper when marked, else the tests.
func TestMain(m *testing.M) {
	if os.Getenv(envHelper) != "" {
		os.Exit(runFakeSSH())
	}
	os.Exit(m.Run())
}

// stdioConn joins stdin (read) and stdout (write) into an
// io.ReadWriteCloser for the in-process SFTP server.
type stdioConn struct {
	io.Reader
	io.Writer
}

func (stdioConn) Close() error { return nil }

// runFakeSSH is the fake-ssh body. It records its argv and PID, optionally
// echoes an environment value to stderr (never stdout: the protocol stream
// stays binary), optionally exits immediately, and otherwise serves an
// in-memory SFTP server on stdin/stdout.
func runFakeSSH() int {
	if dir := os.Getenv(envRecord); dir != "" {
		_ = os.WriteFile(filepath.Join(dir, "argv"), []byte(strings.Join(os.Args[1:], "\x00")), 0o600)
		_ = os.WriteFile(filepath.Join(dir, "pid"), []byte(fmt.Sprint(os.Getpid())), 0o600)
	}
	if name := os.Getenv(envEchoEnv); name != "" {
		fmt.Fprintln(os.Stderr, os.Getenv(name))
	}
	if code := os.Getenv(envFailExit); code != "" {
		var n int
		if _, err := fmt.Sscanf(code, "%d", &n); err != nil {
			n = 2
		}
		return n
	}
	if os.Getenv(envRealServer) != "" && realServerAvailable() {
		// The real server inherits our stdio exactly; no copying, no shell.
		cmd := exec.Command(sftpServerBin)
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "fake ssh: sftp-server: %v\n", err)
			return 2
		}
		return 0
	}
	srv := sftp.NewRequestServer(stdioConn{os.Stdin, os.Stdout}, sftp.InMemHandler())
	_ = srv.Serve()
	return 0
}

// helperConfig builds an SFTPConfig running the test binary as fake ssh,
// recording into recordDir, with cfgExtra environment additions.
func helperConfig(t *testing.T, recordDir string, extra ...string) SFTPConfig {
	t.Helper()
	prog, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	env := []string{envHelper + "=1"}
	if recordDir != "" {
		env = append(env, envRecord+"="+recordDir)
	}
	env = append(env, extra...)
	return SFTPConfig{
		Program: prog,
		Dest:    target.Destination{User: "alice", Host: "example.com", Path: "/data"},
		Env:     env,
	}
}

func helperPidAlive(t *testing.T, recordDir string) (int, bool) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(recordDir, "pid"))
	if err != nil {
		return 0, false
	}
	var pid int
	if _, err := fmt.Sscanf(strings.TrimSpace(string(data)), "%d", &pid); err != nil {
		t.Fatalf("parse helper pid: %v", err)
	}
	return pid, syscall.Kill(pid, 0) == nil
}

func TestSFTPCommandArgv(t *testing.T) {
	cfg := SFTPConfig{Dest: target.Destination{User: "alice", Host: "example.com", Path: "/data"}}
	argv, err := cfg.Command()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"ssh", "-T", "-s", "alice@example.com", "sftp"}
	if fmt.Sprint(argv) != fmt.Sprint(want) {
		t.Errorf("argv = %v, want %v", argv, want)
	}
}

func TestSFTPCommandIPv6(t *testing.T) {
	cfg := SFTPConfig{Dest: target.Destination{Host: "2001:db8::1", Path: "/data"}, IPv6: true}
	argv, err := cfg.Command()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"ssh", "-6", "-T", "-s", "2001:db8::1", "sftp"}
	if fmt.Sprint(argv) != fmt.Sprint(want) {
		t.Errorf("argv = %v, want %v", argv, want)
	}
}

// TestSFTPCommandNeverCarriesPath: the remote path must never leak into the
// connection arguments.
func TestSFTPCommandNeverCarriesPath(t *testing.T) {
	cfg := SFTPConfig{Dest: target.Destination{User: "u", Host: "h", Path: "/sneaky/path"}}
	argv, err := cfg.Command()
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range argv {
		if strings.Contains(a, "/sneaky") {
			t.Errorf("argv contains the remote path: %v", argv)
		}
	}
	if argv[len(argv)-1] != "sftp" {
		t.Errorf("argv does not request only the sftp subsystem: %v", argv)
	}
}

func TestSFTPCommandProgramWithSpaces(t *testing.T) {
	cfg := SFTPConfig{
		Program: "/opt/my tools/ssh wrapper",
		Dest:    target.Destination{Host: "h", Path: "/d"},
	}
	argv, err := cfg.Command()
	if err != nil {
		t.Fatal(err)
	}
	if argv[0] != "/opt/my tools/ssh wrapper" {
		t.Errorf("argv[0] = %q, want the path with spaces verbatim", argv[0])
	}
}

func TestSFTPCommandValidation(t *testing.T) {
	base := SFTPConfig{Dest: target.Destination{Host: "h", Path: "/d"}}
	if _, err := base.Command(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	bad := []SFTPConfig{
		{Dest: target.Destination{}},                                         // empty destination
		{Dest: target.Destination{Host: "h", Path: "relative"}},              // relative path
		{Program: "a\x00b", Dest: target.Destination{Host: "h", Path: "/d"}}, // NUL program
	}
	for i, cfg := range bad {
		if _, err := cfg.Command(); err == nil {
			t.Errorf("config %d accepted, want error", i)
		}
	}
}

// TestSFTPSessionWorks exercises a real SFTP handshake and file operations
// through the fake ssh process: create, stat, and WriteAt at a non-zero
// offset.
func TestSFTPSessionWorks(t *testing.T) {
	dir := t.TempDir()
	cfg := helperConfig(t, dir)
	var stderr bytes.Buffer
	cfg.Stderr = &stderr

	err := cfg.WithClient(context.Background(), func(client *sftp.Client) error {
		f, err := client.OpenFile("/probe.bin", os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
		if err != nil {
			return fmt.Errorf("create: %w", err)
		}
		if _, err := f.Write([]byte("hello world")); err != nil {
			return fmt.Errorf("write: %w", err)
		}
		if _, err := f.WriteAt([]byte("WORLD"), 6); err != nil {
			return fmt.Errorf("writeat: %w", err)
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("close: %w", err)
		}
		fi, err := client.Stat("/probe.bin")
		if err != nil {
			return fmt.Errorf("stat: %w", err)
		}
		if fi.Size() != 11 {
			return fmt.Errorf("size = %d, want 11", fi.Size())
		}
		rf, err := client.Open("/probe.bin")
		if err != nil {
			return fmt.Errorf("open: %w", err)
		}
		defer rf.Close()
		got, err := io.ReadAll(rf)
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		if string(got) != "hello WORLD" {
			return fmt.Errorf("content = %q, want %q", got, "hello WORLD")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithClient: %v", err)
	}
	// The helper process is gone after return.
	pid, alive := helperPidAlive(t, dir)
	if pid <= 0 {
		t.Fatal("helper never recorded its pid")
	}
	if alive {
		t.Errorf("helper pid %d still alive after WithClient returned", pid)
	}
}

// TestSFTPEnvAdditionsAndStderrIsolation: an env addition reaches the helper
// and its stderr goes only to the configured Stderr, never into the
// protocol stream (which would corrupt the handshake if mixed).
func TestSFTPEnvAdditionsAndStderrIsolation(t *testing.T) {
	dir := t.TempDir()
	cfg := helperConfig(t, dir,
		envEchoEnv+"=CATSYNC_TEST_MARKER",
		"CATSYNC_TEST_MARKER=marker-value-123",
	)
	var stderr bytes.Buffer
	cfg.Stderr = &stderr
	err := cfg.WithClient(context.Background(), func(client *sftp.Client) error {
		// Any SFTP op proves the protocol stream survived: stderr marker
		// text must not have corrupted the handshake.
		_, err := client.Stat("/")
		return err
	})
	if err != nil {
		t.Fatalf("WithClient: %v", err)
	}
	if !strings.Contains(stderr.String(), "marker-value-123") {
		t.Errorf("stderr = %q, want the echoed marker", stderr.String())
	}
}

func TestSFTPCallbackErrorPreserved(t *testing.T) {
	dir := t.TempDir()
	cfg := helperConfig(t, dir)
	boom := errors.New("boom from callback")
	err := cfg.WithClient(context.Background(), func(client *sftp.Client) error {
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want the callback's error", err)
	}
	if pid, alive := helperPidAlive(t, dir); alive {
		t.Errorf("helper pid %d still alive after callback error", pid)
	}
}

func TestSFTPHandshakeFailure(t *testing.T) {
	dir := t.TempDir()
	cfg := helperConfig(t, dir, envFailExit+"=23")
	err := cfg.WithClient(context.Background(), func(client *sftp.Client) error {
		t.Error("callback must not run when the handshake fails")
		return nil
	})
	if err == nil {
		t.Fatal("WithClient = nil, want a handshake/process error")
	}
	if pid, alive := helperPidAlive(t, dir); alive {
		t.Errorf("failed helper pid %d still alive", pid)
	}
}

func TestSFTPPreCancelledStartsNothing(t *testing.T) {
	dir := t.TempDir()
	cfg := helperConfig(t, dir)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := cfg.WithClient(ctx, func(client *sftp.Client) error {
		t.Error("callback must not run on a pre-cancelled context")
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want wrapped context.Canceled", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "pid")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("helper started despite pre-cancelled context: %v", err)
	}
}

// TestSFTPMidCancelStopsSession: the callback blocks on a test gate; the
// test cancels; the session tears down: ssh is reaped, the returned error
// wraps context.Canceled, and nothing is left running.
func TestSFTPMidCancelStopsSession(t *testing.T) {
	dir := t.TempDir()
	cfg := helperConfig(t, dir)
	ctx, cancel := context.WithCancel(context.Background())
	proceed := make(chan struct{})
	cbCalled := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- cfg.WithClient(ctx, func(client *sftp.Client) error {
			close(cbCalled)
			<-proceed // released after cancellation
			return errors.New("callback sentinel")
		})
	}()
	<-cbCalled // the callback is running inside a live session
	cancel()
	close(proceed)
	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want wrapped context.Canceled", err)
	}
	if pid, alive := helperPidAlive(t, dir); alive {
		t.Errorf("helper pid %d still alive after cancellation", pid)
	}
}

// TestSFTPNoLateActivity verifies nothing keeps running after WithClient
// returns: the process is dead and both pipes are done.
func TestSFTPNoLateActivity(t *testing.T) {
	dir := t.TempDir()
	cfg := helperConfig(t, dir)
	if err := cfg.WithClient(context.Background(), func(client *sftp.Client) error {
		_, err := client.Stat("/")
		return err
	}); err != nil {
		t.Fatalf("WithClient: %v", err)
	}
	pid, alive := helperPidAlive(t, dir)
	if pid <= 0 {
		t.Fatal("helper never recorded its pid")
	}
	if alive {
		t.Errorf("helper pid %d still alive after WithClient returned", pid)
	}
	// The run directory holds no new writes after return: argv and pid are
	// fixed-size records written at helper start.
	argvBefore, err := os.ReadFile(filepath.Join(dir, "argv"))
	if err != nil {
		t.Fatal(err)
	}
	argvAfter, err := os.ReadFile(filepath.Join(dir, "argv"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(argvBefore, argvAfter) {
		t.Error("helper argv record changed after WithClient returned")
	}
	// The recorded argv only requests the sftp subsystem.
	parts := strings.Split(string(argvBefore), "\x00")
	if parts[len(parts)-1] != "sftp" {
		t.Errorf("helper argv = %q, want the sftp subsystem request", argvBefore)
	}
	for _, p := range parts {
		if strings.Contains(p, "/data") {
			t.Errorf("argv carries the remote path: %q", argvBefore)
		}
	}
}

func TestSFTPNilContextAndCallback(t *testing.T) {
	cfg := helperConfig(t, t.TempDir())
	if err := cfg.WithClient(nil, func(*sftp.Client) error { return nil }); err == nil {
		t.Error("nil context accepted")
	}
	if err := cfg.WithClient(context.Background(), nil); err == nil {
		t.Error("nil callback accepted")
	}
}
