//go:build unix

package executor

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dotfrankruan/CatSync/internal/daemon"
	"github.com/dotfrankruan/CatSync/internal/daemon/protocol"
	"github.com/dotfrankruan/CatSync/internal/state"
	"github.com/dotfrankruan/CatSync/internal/target"
)

// fakeSSHForRootTests writes a stand-in ssh client that executes local
// commands (rsync --server, manifest/mkdir shell scripts) and the SFTP
// subsystem, so SSH-mode destination-root semantics can be tested against a
// real local filesystem without sshd.
func fakeSSHForRootTests(t *testing.T) string {
	t.Helper()
	sftp := daemon.ResolveSFTPPath("")
	if sftp == "" {
		t.Skip("sftp-server is not installed")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "ssh")
	script := `#!/bin/sh
subsystem=0
while [ $# -gt 0 ]; do
  case "$1" in
    -l|-p|-o|-i|-F|-J|-c) shift 2 ;;
    -s) subsystem=1; shift ;;
    -*) shift ;;
    *) break ;;
  esac
done
if [ $# -eq 0 ]; then exit 255; fi
host=$1; shift
if [ $subsystem -eq 1 ]; then exec ` + sftp + `; fi
if [ $# -eq 1 ]; then exec /bin/sh -c "$1"; fi
exec "$@"
`
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func rootSSHConfig(t *testing.T, src, dest string) Config {
	t.Helper()
	ssh := fakeSSHForRootTests(t)
	cfg := Config{
		SourceDir:      src,
		Dest:           target.Destination{Host: "fake-host", Path: dest},
		Backend:        state.BackendAuto,
		Verify:         state.VerifyNone,
		Jobs:           2,
		Streams:        2,
		LargeThreshold: 1 << 20,
		StateDir:       t.TempDir(),
		Programs:       Programs{SSH: ssh, Rsync: findRealRsync(t)},
	}
	return cfg
}

func findRealRsync(t *testing.T) string {
	t.Helper()
	p, err := exec.LookPath("rsync")
	if err != nil {
		t.Skip("rsync is not installed")
	}
	return p
}

func writeRootFile(t *testing.T, dir, name, content string) {
	t.Helper()
	p := filepath.Join(dir, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestSSHDirectorySourceDestinationRoot(t *testing.T) {
	cases := []struct {
		name    string
		files   map[string]string
		destSet func(t *testing.T, dest string)
		wantErr string
	}{
		{
			name:  "one root-level file to missing destination",
			files: map[string]string{"only.txt": "one"},
		},
		{
			name:  "multiple root-level files to missing destination",
			files: map[string]string{"a.txt": "a", "b.txt": "b"},
		},
		{
			name:  "root file plus subdirectory",
			files: map[string]string{"a.txt": "a", "sub/n.txt": "nested"},
		},
		{
			name:  "existing destination directory",
			files: map[string]string{"a.txt": "a"},
			destSet: func(t *testing.T, dest string) {
				if err := os.MkdirAll(dest, 0o755); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:  "destination is an existing regular file",
			files: map[string]string{"a.txt": "a"},
			destSet: func(t *testing.T, dest string) {
				if err := os.WriteFile(dest, []byte("keep"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
			wantErr: "not a directory",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := t.TempDir()
			for name, content := range tc.files {
				writeRootFile(t, src, name, content)
			}
			dest := filepath.Join(t.TempDir(), "dest")
			if tc.destSet != nil {
				tc.destSet(t, dest)
			}
			cfg := rootSSHConfig(t, src, dest)
			run, err := Sync(context.Background(), cfg)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("Sync = %v, want error containing %q", err, tc.wantErr)
				}
				if run == nil || run.Status != state.StatusFailed {
					t.Errorf("run = %+v, want failed", run)
				}
				data, _ := os.ReadFile(dest)
				if string(data) != "keep" {
					t.Errorf("existing destination file was modified: %q", data)
				}
				return
			}
			if err != nil {
				t.Fatalf("Sync: %v", err)
			}
			if run.Status != state.StatusCompleted {
				t.Fatalf("run = %s (%q)", run.Status, run.Error)
			}
			fi, err := os.Stat(dest)
			if err != nil || !fi.IsDir() {
				t.Fatalf("destination root is not a directory: %v", err)
			}
			for name, content := range tc.files {
				data, err := os.ReadFile(filepath.Join(dest, filepath.FromSlash(name)))
				if err != nil || string(data) != content {
					t.Errorf("%s = %q, %v; want %q", name, data, err, content)
				}
			}
		})
	}
}

func TestSSHSingleFileDestinationUnchanged(t *testing.T) {
	srcDir := t.TempDir()
	writeRootFile(t, srcDir, "one.bin", "single")
	dest := filepath.Join(t.TempDir(), "point-to-point")
	cfg := rootSSHConfig(t, srcDir, dest)
	cfg.SourceFile = "one.bin"
	run, err := Sync(context.Background(), cfg)
	if err != nil {
		t.Fatalf("single-file Sync: %v", err)
	}
	if run.Status != state.StatusCompleted {
		t.Fatalf("run = %s (%q)", run.Status, run.Error)
	}
	fi, err := os.Stat(dest)
	if err != nil {
		t.Fatalf("single-file destination missing: %v", err)
	}
	if fi.IsDir() {
		t.Fatalf("single-file destination became a directory")
	}
	data, _ := os.ReadFile(dest)
	if string(data) != "single" {
		t.Errorf("content = %q", data)
	}
}

// --- daemon variants -------------------------------------------------------

func daemonRootConfig(t *testing.T, transport, src, dest string) Config {
	t.Helper()
	cfg := daemonTestConfig(t, target.LocalToRemote, src, dest, state.BackendAuto, 2, 2)
	cfg.LargeThreshold = 1 << 20
	cfg.DaemonTransport = transport
	cfg.DaemonPort = 0
	cfg.Verify = state.VerifyNone
	return cfg
}

func TestDaemonDirectorySourceDestinationRoot(t *testing.T) {
	sftpPath := daemon.ResolveSFTPPath("")
	rsyncPath := daemon.ResolveRsyncPath("")
	if sftpPath == "" || rsyncPath == "" {
		t.Skip("sftp-server or rsync is not installed")
	}
	exe := realCatsyncBinary(t)
	oldExe := catsyncExecutable
	catsyncExecutable = func() (string, error) { return exe, nil }
	t.Cleanup(func() { catsyncExecutable = oldExe })

	for _, transport := range []string{"tcp", "quic"} {
		t.Run(transport, func(t *testing.T) {
			cases := []struct {
				name    string
				files   map[string]string
				destSet func(t *testing.T, dest string)
				wantErr string
			}{
				{"one_root_file", map[string]string{"only.txt": "one"}, nil, ""},
				{"many_root_files", map[string]string{"a.txt": "a", "b.txt": "b"}, nil, ""},
				{"mixed", map[string]string{"a.txt": "a", "sub/n.txt": "nested"}, nil, ""},
				{"existing_dir", map[string]string{"a.txt": "a"}, func(t *testing.T, d string) {
					if err := os.MkdirAll(d, 0o755); err != nil {
						t.Fatal(err)
					}
				}, ""},
				{"existing_file", map[string]string{"a.txt": "a"}, func(t *testing.T, d string) {
					if err := os.WriteFile(d, []byte("keep"), 0o644); err != nil {
						t.Fatal(err)
					}
				}, "not a directory"},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					tr := &inProcDaemonTransport{t: t, sftpPath: sftpPath, rsyncPath: rsyncPath, transport: transport,
						limits: protocol.Limits{MaxSFTPConns: 8, MaxExecConns: 8, MaxControlConns: 8, MaxPendingHandshake: 8, MaxConns: 64}}
					installDaemonTransport(t, tr)
					src := t.TempDir()
					for name, content := range tc.files {
						writeRootFile(t, src, name, content)
					}
					dest := filepath.Join(t.TempDir(), "dest")
					if tc.destSet != nil {
						tc.destSet(t, dest)
					}
					run, err := Sync(context.Background(), daemonRootConfig(t, transport, src, dest))
					if tc.wantErr != "" {
						if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
							t.Fatalf("Sync = %v, want error containing %q", err, tc.wantErr)
						}
						if run == nil || run.Status != state.StatusFailed {
							t.Errorf("run = %+v, want failed", run)
						}
						data, _ := os.ReadFile(dest)
						if string(data) != "keep" {
							t.Errorf("existing destination file modified: %q", data)
						}
						return
					}
					if err != nil {
						t.Fatalf("Sync: %v", err)
					}
					if run.Status != state.StatusCompleted {
						t.Fatalf("run = %s (%q)", run.Status, run.Error)
					}
					fi, err := os.Stat(dest)
					if err != nil || !fi.IsDir() {
						t.Fatalf("destination root is not a directory: %v", err)
					}
					for name, content := range tc.files {
						data, err := os.ReadFile(filepath.Join(dest, filepath.FromSlash(name)))
						if err != nil || string(data) != content {
							t.Errorf("%s = %q, %v; want %q", name, data, err, content)
						}
					}
				})
			}
		})
	}
}

func TestDaemonMirrorDestinationRootAndScope(t *testing.T) {
	exe := realCatsyncBinary(t)
	oldExe := catsyncExecutable
	catsyncExecutable = func() (string, error) { return exe, nil }
	t.Cleanup(func() { catsyncExecutable = oldExe })
	sftpPath := daemon.ResolveSFTPPath("")
	rsyncPath := daemon.ResolveRsyncPath("")
	if sftpPath == "" || rsyncPath == "" {
		t.Skip("sftp-server or rsync is not installed")
	}
	tr := &inProcDaemonTransport{t: t, sftpPath: sftpPath, rsyncPath: rsyncPath, transport: "quic",
		limits: protocol.Limits{MaxSFTPConns: 8, MaxExecConns: 8, MaxControlConns: 8, MaxPendingHandshake: 8, MaxConns: 64}}
	installDaemonTransport(t, tr)
	src := t.TempDir()
	writeRootFile(t, src, "a.txt", "a")
	writeRootFile(t, src, "sub/n.txt", "n")
	parent := t.TempDir()
	dest := filepath.Join(parent, "mirror-root")
	outside := filepath.Join(parent, "outside.txt")
	if err := os.WriteFile(outside, []byte("outside"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := daemonRootConfig(t, "quic", src, dest)
	cfg.Mirror = true
	if _, err := Sync(context.Background(), cfg); err != nil {
		t.Fatalf("mirror initial: %v", err)
	}
	if fi, err := os.Stat(dest); err != nil || !fi.IsDir() {
		t.Fatalf("mirror destination root missing: %v", err)
	}
	extra := filepath.Join(dest, "extra.txt")
	if err := os.WriteFile(extra, []byte("delete me"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg2 := daemonRootConfig(t, "quic", src, dest)
	cfg2.Mirror = true
	if _, err := Sync(context.Background(), cfg2); err != nil {
		t.Fatalf("mirror second: %v", err)
	}
	if _, err := os.Stat(extra); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("mirror did not delete inside-root extra: %v", err)
	}
	if data, err := os.ReadFile(outside); err != nil || string(data) != "outside" {
		t.Errorf("mirror touched a path outside the destination root: %q, %v", data, err)
	}
}

var _ = exec.LookPath
var _ = time.Now
