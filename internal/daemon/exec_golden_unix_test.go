//go:build unix

package daemon

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dotfrankruan/CatSync/internal/daemon/protocol"
)

// TestRsyncGoldenArgvOnThisHost records the remote argv produced by the
// locally installed rsync for the four CatSync invocation shapes and checks
// that the exec allowlist accepts them. Versions other than the local one are
// not claimed as tested.
func TestRsyncGoldenArgvOnThisHost(t *testing.T) {
	rsyncPath, err := exec.LookPath("rsync")
	if err != nil {
		t.Skip("rsync is not installed")
	}
	dir := t.TempDir()
	capture := filepath.Join(dir, "capture")
	script := filepath.Join(dir, "rsh")
	scriptBody := "#!/bin/sh\nfor a in \"$@\"; do printf '%s\\000' \"$a\" >> " + shellSingleQuote(capture) + "; done\nprintf '\\001' >> " + shellSingleQuote(capture) + "\nexit 23\n"
	if err := os.WriteFile(script, []byte(scriptBody), 0o755); err != nil {
		t.Fatal(err)
	}
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "a.txt"), []byte("hi"), 0o644); err != nil {
		t.Fatal(err)
	}
	fileList := filepath.Join(dir, "files")
	if err := os.WriteFile(fileList, []byte("a.txt\x00"), 0o644); err != nil {
		t.Fatal(err)
	}
	shapes := [][]string{
		{"--archive", "--partial", "--partial-dir=.catsync-rsync-partial", "--relative", "--files-from=-", "--from0", "-e", script, "--", src + "/", "user@host:/data/"},
		{"--archive", "--partial", "--partial-dir=.catsync-rsync-partial", "--relative", "--files-from=-", "--from0", "-e", script, "--", "user@host:/data/", dst + "/"},
		{"--archive", "--checksum", "--hard-links", "--acls", "--xattrs", "--delete-delay", "--partial", "-e", script, "--", src + "/", "user@host:/data/"},
		{"--archive", "--checksum", "--hard-links", "--acls", "--xattrs", "--delete-delay", "--partial", "--dry-run", "--itemize-changes", "-e", script, "--", "user@host:/data/", dst + "/"},
	}
	names := []string{"upload-batch", "download-batch", "mirror", "mirror-dry-run"}
	for i, args := range shapes {
		if err := os.Remove(capture); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		cmd := exec.Command(rsyncPath, args...)
		cmd.Stdin, _ = os.Open(fileList)
		out, err := cmd.CombinedOutput()
		if cmd.Stdin != nil {
			_ = cmd.Stdin.(*os.File).Close()
		}
		if err == nil {
			t.Fatalf("%s: rsync unexpectedly succeeded", names[i])
		}
		data, rerr := os.ReadFile(capture)
		if rerr != nil {
			t.Skipf("local rsync rejected CatSync options (%s): %s", names[i], strings.TrimSpace(string(out)))
		}
		fields := strings.Split(string(data), "\x00")
		var remote []string
		seenRsync := false
		for _, f := range fields {
			if f == "" || strings.ContainsRune(f, '\x01') {
				continue
			}
			if f == "rsync" {
				seenRsync = true
				continue
			}
			if seenRsync {
				remote = append(remote, f)
			}
		}
		if len(remote) == 0 {
			t.Fatalf("%s: no remote argv captured (%q)", names[i], fields)
		}
		if err := protocol.ValidateExecStart("rsync", remote); err != nil {
			t.Errorf("%s: local rsync argv rejected: %v: %q", names[i], err, remote)
		}
	}
}

func shellSingleQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}
