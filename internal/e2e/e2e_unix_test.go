//go:build unix

// Package e2e exercises the real catsync and catsyncd binaries through a
// controlled local fake-ssh harness: no sshd, no remote host, but a genuine
// process boundary, real bootstrap pipes, and loopback TCP/UDP data paths.
package e2e

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	buildOnce     sync.Once
	binDir        string
	catsyncBin    string
	catsyncdBin   string
	sshHarnessBin string
	buildErr      error
)

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

func buildTools(t *testing.T) {
	t.Helper()
	buildOnce.Do(func() {
		binDir, buildErr = os.MkdirTemp("", "catsync-e2e-")
		if buildErr != nil {
			return
		}
		catsyncBin = filepath.Join(binDir, "catsync")
		catsyncdBin = filepath.Join(binDir, "catsyncd")
		sshHarnessBin = filepath.Join(binDir, "sshharness")
		root := ""
		// repoRoot needs a testing.T; use the module path from the working
		// directory instead.
		wd, err := os.Getwd()
		if err != nil {
			buildErr = err
			return
		}
		root = wd
		for {
			if _, err := os.Stat(filepath.Join(root, "go.mod")); err == nil {
				break
			}
			parent := filepath.Dir(root)
			if parent == root {
				buildErr = fmt.Errorf("go.mod not found from %s", wd)
				return
			}
			root = parent
		}
		for _, spec := range []struct {
			out string
			pkg string
		}{
			{catsyncBin, "./cmd/catsync"},
			{catsyncdBin, "./cmd/catsyncd"},
			{sshHarnessBin, "./internal/testutil/sshharness"},
		} {
			cmd := exec.Command("go", "build", "-o", spec.out, spec.pkg)
			cmd.Dir = root
			if out, err := cmd.CombinedOutput(); err != nil {
				buildErr = fmt.Errorf("build %s: %v: %s", spec.pkg, err, out)
				return
			}
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
}

func freeTCPPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return port
}

func freeUDPPort(t *testing.T) int {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := pc.LocalAddr().(*net.UDPAddr).Port
	_ = pc.Close()
	return port
}

type runEnv struct {
	home  string
	state string
	log   string
	extra []string
	env   []string
}

func newRunEnv(t *testing.T, extraEnv ...string) *runEnv {
	t.Helper()
	buildTools(t)
	root := t.TempDir()
	state := filepath.Join(root, "state")
	if err := os.MkdirAll(state, 0o700); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(root, "ssh.log")
	env := append(os.Environ(),
		"CATSYNC_SSH="+sshHarnessBin,
		"CATSYNC_SSHH_LOG="+logPath,
		"CATSYNC_RSYNC="+findRsync(t),
		"PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
	)
	env = append(env, extraEnv...)
	return &runEnv{home: root, state: state, log: logPath, env: env}
}

func findRsync(t *testing.T) string {
	t.Helper()
	if p, err := exec.LookPath("rsync"); err == nil {
		return p
	}
	t.Skip("rsync is not installed")
	return ""
}

func (e *runEnv) run(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	cmd := exec.Command(catsyncBin, args...)
	cmd.Dir = e.home
	cmd.Env = e.env
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	code := 0
	if ee, ok := err.(*exec.ExitError); ok {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatalf("run catsync: %v", err)
	}
	return code, stdout.String(), stderr.String()
}

func (e *runEnv) sshInvocations(t *testing.T) int {
	t.Helper()
	data, err := os.ReadFile(e.log)
	if err != nil {
		return 0
	}
	return strings.Count(string(data), "invoke ")
}

func (e *runEnv) assertNoCatsyncd(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		out, _ := exec.Command("ps", "-axo", "args").Output()
		if !strings.Contains(string(out), catsyncdBin+" serve --once") {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Errorf("a catsyncd process outlived the run")
}

func writeTree(t *testing.T, root string) {
	t.Helper()
	files := map[string][]byte{
		"big.bin":           make([]byte, 2<<20),
		"small.txt":         []byte("small file"),
		"sub/nested.txt":    []byte("nested"),
		"unicode-文件.txt":    []byte("unicode"),
		"with space.txt":    []byte("space"),
		"with\nnewline.txt": []byte("newline"),
	}
	for name, data := range files {
		p := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if name == "big.bin" {
			if _, err := rand.Read(data); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(p, data, 0o640); err != nil {
			t.Fatal(err)
		}
		mod := time.Unix(1_700_600_000, int64(len(name))*1000)
		if err := os.Chtimes(p, mod, mod); err != nil {
			t.Fatal(err)
		}
	}
}

func requireTreeEqual(t *testing.T, want, got string) {
	t.Helper()
	err := filepath.Walk(want, func(p string, fi os.FileInfo, err error) error {
		if err != nil || fi.IsDir() {
			return err
		}
		rel, err := filepath.Rel(want, p)
		if err != nil {
			return err
		}
		a, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		b, err := os.ReadFile(filepath.Join(got, rel))
		if err != nil {
			return fmt.Errorf("%s: %w", rel, err)
		}
		if !bytes.Equal(a, b) {
			return fmt.Errorf("%s content differs", rel)
		}
		gi, err := os.Stat(filepath.Join(got, rel))
		if err != nil {
			return err
		}
		if gi.Mode().Perm() != fi.Mode().Perm() {
			return fmt.Errorf("%s mode %o != %o", rel, gi.Mode().Perm(), fi.Mode().Perm())
		}
		if gi.ModTime().UnixNano() != fi.ModTime().UnixNano() {
			return fmt.Errorf("%s mtime %d != %d", rel, gi.ModTime().UnixNano(), fi.ModTime().UnixNano())
		}
		return nil
	})
	if err != nil {
		t.Fatalf("tree comparison: %v", err)
	}
}

func TestProcessDirectDaemonCopy(t *testing.T) {
	env := newRunEnv(t)
	src := t.TempDir()
	dest := filepath.Join(t.TempDir(), "dest")
	writeTree(t, src)
	port := freeTCPPort(t)
	code, stdout, stderr := env.run(t, "copy",
		"--transport", "daemon",
		"--daemon-port", fmt.Sprint(port),
		"--daemon-addr", fmt.Sprintf("127.0.0.1:%d", port),
		"--remote-catsyncd", catsyncdBin,
		"--state-dir", env.state,
		"--progress", "never",
		"--verify", "sha256",
		"--large-threshold", "1MiB",
		src, "user@fake-host:"+dest)
	if code != 0 {
		t.Fatalf("copy exit=%d\nstdout=%s\nstderr=%s", code, stdout, stderr)
	}
	requireTreeEqual(t, src, dest)
	if n := env.sshInvocations(t); n != 1 {
		t.Fatalf("ssh invocations = %d, want exactly 1 bootstrap", n)
	}
	env.assertNoCatsyncd(t)
}

func TestProcessSSHForwardCopy(t *testing.T) {
	env := newRunEnv(t)
	src := t.TempDir()
	dest := filepath.Join(t.TempDir(), "dest")
	writeTree(t, src)
	port := freeTCPPort(t)
	code, stdout, stderr := env.run(t, "copy",
		"--transport", "daemon",
		"--daemon-bootstrap", "ssh-forward",
		"--daemon-port", fmt.Sprint(port),
		"--remote-catsyncd", catsyncdBin,
		"--state-dir", env.state,
		"--progress", "never",
		"--verify", "sha256",
		"--large-threshold", "1MiB",
		src, "user@fake-host:"+dest)
	if code != 0 {
		t.Fatalf("copy exit=%d\nstdout=%s\nstderr=%s", code, stdout, stderr)
	}
	requireTreeEqual(t, src, dest)
	if n := env.sshInvocations(t); n != 1 {
		t.Fatalf("ssh invocations = %d, want exactly 1 bootstrap", n)
	}
	env.assertNoCatsyncd(t)
}

func TestProcessForwardFailureIsReported(t *testing.T) {
	env := newRunEnv(t, "CATSYNC_SSHH_FAIL_FORWARD=1")
	src := t.TempDir()
	dest := filepath.Join(t.TempDir(), "dest")
	writeTree(t, src)
	port := freeTCPPort(t)
	code, _, stderr := env.run(t, "copy",
		"--transport", "daemon",
		"--daemon-bootstrap", "ssh-forward",
		"--daemon-port", fmt.Sprint(port),
		"--remote-catsyncd", catsyncdBin,
		"--state-dir", env.state,
		"--progress", "never",
		src, "user@fake-host:"+dest)
	if code == 0 {
		t.Fatalf("forwarding failure reported success")
	}
	if !strings.Contains(stderr, "bootstrap") && !strings.Contains(stderr, "forward") && !strings.Contains(stderr, "ssh") {
		t.Errorf("stderr does not explain the forwarding failure:\n%s", stderr)
	}
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Errorf("forwarding failure wrote remote data: %v", err)
	}
	env.assertNoCatsyncd(t)
}

func TestProcessQUICCopy(t *testing.T) {
	env := newRunEnv(t)
	src := t.TempDir()
	dest := filepath.Join(t.TempDir(), "dest")
	writeTree(t, src)
	port := freeUDPPort(t)
	code, stdout, stderr := env.run(t, "copy",
		"--transport", "daemon",
		"--daemon-transport", "quic",
		"--daemon-port", fmt.Sprint(port),
		"--daemon-addr", fmt.Sprintf("127.0.0.1:%d", port),
		"--remote-catsyncd", catsyncdBin,
		"--state-dir", env.state,
		"--progress", "never",
		"--verify", "sha256",
		"--large-threshold", "1MiB",
		src, "user@fake-host:"+dest)
	if code != 0 {
		t.Fatalf("copy exit=%d\nstdout=%s\nstderr=%s", code, stdout, stderr)
	}
	requireTreeEqual(t, src, dest)
	if n := env.sshInvocations(t); n != 1 {
		t.Fatalf("ssh invocations = %d, want exactly 1 bootstrap", n)
	}
	env.assertNoCatsyncd(t)
}
