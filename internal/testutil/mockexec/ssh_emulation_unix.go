//go:build unix

package mockexec

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/sftp"

	"github.com/dotfrankruan/CatSync/internal/epochtime"
)

// sftpServerBin is the real OpenSSH sftp-server used to emulate the remote
// end of an ssh SFTP subsystem invocation.
const sftpServerBin = "/usr/libexec/sftp-server"

// SFTPServerAvailable reports whether the real sftp-server binary exists.
func SFTPServerAvailable() bool {
	_, err := os.Stat(sftpServerBin)
	return err == nil
}

// envSFTPFailNth makes the Nth SFTP subsystem invocation in this record
// directory exit 42 without serving: a deterministic session failure for
// resume tests. Sequential starts make the counter file race-free.
const envSFTPFailNth = "MOCKEXEC_SFTP_FAILNTH"

// EnvSFTPNoExt makes the emulated SFTP server advertise no extensions at
// all: the hardlink capability probe then reports unsupported.
const EnvSFTPNoExt = "MOCKEXEC_SFTP_NOEXT"

// EnvSSHNative gates the ssh emulation: only with it set does the mock ssh
// answer SFTP subsystem and controlled metadata commands against the local
// filesystem. Without it the tool stays purely script-driven, so legacy
// script-mode tests keep full control.
const EnvSSHNative = "MOCKEXEC_SSH_NATIVE"

// EnvSSHHashTool selects the SHA-256 utility reported by the capability
// probe in tests. Empty means no supported tool.
const EnvSSHHashTool = "MOCKEXEC_SSH_HASH_TOOL"

// sshEmulation handles an invocation of the mock "ssh" tool that asks for
// the SFTP subsystem or runs one of the controlled metadata commands the
// native large-file backend uses (touch -m -d @epoch.nanos, test -ef,
// mkdir -p). Everything else returns handled=false so the script behavior
// applies. The remote filesystem is the local filesystem.
func sshEmulation(args []string) (int, bool) {
	// Default script-mode tests emulate a minimal remote with no hash
	// utility. This exact capability probe is safe to answer without the
	// broader native-filesystem emulation and exercises auto's size-only
	// downgrade path.
	if len(args) > 0 && strings.Contains(args[len(args)-1], "command -v sha256sum") && strings.Contains(args[len(args)-1], "command -v openssl") {
		tool := os.Getenv(EnvSSHHashTool)
		if tool == "" {
			tool = "none"
		}
		fmt.Fprint(os.Stdout, tool)
		return 0, true
	}
	if os.Getenv(EnvSSHNative) == "" {
		return 0, false
	}
	// Skip leading flag tokens (-6, -T, -s), then the [user@]host token;
	// the rest is the remote command, each element shell-quoted by the
	// caller.
	i := 0
	for i < len(args) && strings.HasPrefix(args[i], "-") {
		i++
	}
	if i >= len(args) {
		return 0, false
	}
	i++ // host
	if i > len(args) {
		return 0, false
	}
	cmd := args[i:]
	for j := range cmd {
		cmd[j] = unquoteShellWord(cmd[j])
	}
	if len(cmd) == 0 {
		return 0, false
	}
	switch {
	case len(cmd) == 1 && cmd[0] == "sftp":
		if failNthSFTPSession() {
			return 42, true
		}
		if os.Getenv(EnvSFTPNoExt) != "" {
			return serveInMemNoExt(), true
		}
		return serveSFTP(), true
	case cmd[0] == "touch":
		return emulateTouch(cmd), true
	case cmd[0] == "test":
		return emulateTest(cmd), true
	case cmd[0] == "mkdir":
		return emulateMkdir(cmd), true
	case len(cmd) == 1 && strings.HasPrefix(cmd[0], "mkdir -p"):
		// The production code sends mkdir as one joined, shell-quoted
		// string ("mkdir -p 'a' 'b'"): extract the quoted directory list.
		dirs := extractQuotedTokens(cmd[0][len("mkdir -p"):])
		if len(dirs) == 0 {
			return 0, false
		}
		for _, d := range dirs {
			if err := os.MkdirAll(d, 0o755); err != nil {
				fmt.Fprintf(os.Stderr, "mockexec: mkdir: %v\n", err)
				return 1, true
			}
		}
		return 0, true
	case len(cmd) == 1 && strings.HasPrefix(cmd[0], "if [ -f "):
		return emulateSourceIdentity(cmd[0]), true
	case len(cmd) == 1 && strings.HasPrefix(cmd[0], "set -e; out=$("):
		c := exec.Command("/bin/sh", "-c", cmd[0])
		c.Stdout, c.Stderr = os.Stdout, os.Stderr
		if err := c.Run(); err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				return exitErr.ExitCode(), true
			}
			return 2, true
		}
		return 0, true
	}
	return 0, false
}

func emulateSourceIdentity(script string) int {
	const prefix = "if [ -f "
	const suffix = " ] && [ ! -L "
	end := strings.Index(script, suffix)
	if end < 0 {
		fmt.Fprintln(os.Stderr, "mockexec: malformed source identity probe")
		return 2
	}
	p := unquoteShellWord(script[len(prefix):end])
	fi, err := os.Lstat(p)
	if err != nil || !fi.Mode().IsRegular() || fi.Mode()&os.ModeSymlink != 0 {
		return 44
	}
	fmt.Fprintf(os.Stdout, "file%c%d%c%s%c", 0, fi.Size(), 0, epochtime.Format(fi.ModTime()), 0)
	return 0
}

// extractQuotedTokens returns the single-quoted words of s, unquoted. The
// production shell quote renders every path as one single-quoted word.
func extractQuotedTokens(s string) []string {
	var out []string
	for {
		i := strings.IndexByte(s, '\'')
		if i < 0 {
			return out
		}
		s = s[i+1:]
		var b strings.Builder
		for {
			j := strings.IndexByte(s, '\'')
			if j < 0 {
				return out
			}
			b.WriteString(s[:j])
			s = s[j+1:]
			if strings.HasPrefix(s, "\\'") {
				// The '\'' escape: literal quote continues the word.
				b.WriteByte('\'')
				s = s[3:]
				continue
			}
			break
		}
		out = append(out, b.String())
	}
}

// failNthSFTPSession reports whether this SFTP invocation should fail: the
// envSFTPFailNth-th subsystem call in this record directory.
func failNthSFTPSession() bool {
	nth := os.Getenv(envSFTPFailNth)
	if nth == "" {
		return false
	}
	var n, count int
	if _, err := fmt.Sscanf(nth, "%d", &n); err != nil || n <= 0 {
		return false
	}
	dir := os.Getenv(EnvRecordDir)
	if dir == "" {
		return false
	}
	p := filepath.Join(dir, "sftp-count")
	if data, err := os.ReadFile(p); err == nil {
		fmt.Sscanf(strings.TrimSpace(string(data)), "%d", &count)
	}
	count++
	_ = os.WriteFile(p, []byte(strconv.Itoa(count)), 0o600)
	return count == n
}

// serveSFTP runs the real OpenSSH sftp-server with our own stdin/stdout, so
// the wire protocol, exclusive create, truncate, chmod, fsync, hardlink,
// and stat all behave exactly as a real remote end would.
func serveSFTP() int {
	if !SFTPServerAvailable() {
		fmt.Fprintf(os.Stderr, "mockexec: %s not available\n", sftpServerBin)
		return 2
	}
	c := exec.Command(sftpServerBin)
	c.Stdin = os.Stdin
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	if err := c.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "mockexec: sftp-server: %v\n", err)
		return 2
	}
	return 0
}

// serveInMemNoExt serves an in-memory SFTP server that advertises no
// extensions, for the hardlink-capability fallback tests.
func serveInMemNoExt() int {
	_ = sftp.SetSFTPExtensions()
	srv := sftp.NewRequestServer(stdioConn{os.Stdin, os.Stdout}, sftp.InMemHandler())
	_ = srv.Serve()
	return 0
}

// stdioConn joins stdin (read) and stdout (write) for the in-process SFTP
// server.
type stdioConn struct {
	io.Reader
	io.Writer
}

func (stdioConn) Close() error { return nil }

// unquoteShellWord reverses the single-quote shell quoting used by the
// production code (discovery.ShellQuote).
func unquoteShellWord(s string) string {
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return strings.ReplaceAll(s[1:len(s)-1], `'\''`, "'")
	}
	return s
}

// emulateTouch implements GNU `touch -m -d @<sec>.<nsec> <path>`: set the
// mtime with nanosecond precision, preserving atime.
func emulateTouch(cmd []string) int {
	if len(cmd) != 5 || cmd[1] != "-m" || cmd[2] != "-d" {
		fmt.Fprintln(os.Stderr, "mockexec: bad touch argv")
		return 2
	}
	if !strings.HasPrefix(cmd[3], "@") {
		fmt.Fprintln(os.Stderr, "mockexec: touch needs @epoch")
		return 2
	}
	mt, err := parseEpochNanos(cmd[3][1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "mockexec: %v\n", err)
		return 2
	}
	p := cmd[4]
	fi, err := os.Stat(p)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mockexec: touch: %v\n", err)
		return 1
	}
	if err := os.Chtimes(p, fi.ModTime(), mt); err != nil {
		fmt.Fprintf(os.Stderr, "mockexec: touch: %v\n", err)
		return 1
	}
	return 0
}

// emulateTest implements `test <a> -ef <b>`.
func emulateTest(cmd []string) int {
	if len(cmd) != 4 || cmd[2] != "-ef" {
		fmt.Fprintln(os.Stderr, "mockexec: bad test argv")
		return 2
	}
	fa, err := os.Lstat(cmd[1])
	if err != nil {
		return 1
	}
	fb, err := os.Lstat(cmd[3])
	if err != nil {
		return 1
	}
	if os.SameFile(fa, fb) {
		return 0
	}
	return 1
}

// emulateMkdir implements `mkdir -p <dir>...`.
func emulateMkdir(cmd []string) int {
	if len(cmd) < 2 || cmd[1] != "-p" {
		fmt.Fprintln(os.Stderr, "mockexec: bad mkdir argv")
		return 2
	}
	for _, d := range cmd[2:] {
		if err := os.MkdirAll(d, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "mockexec: mkdir: %v\n", err)
			return 1
		}
	}
	return 0
}

// parseEpochNanos parses GNU touch's "@SECONDS" operand as a true signed
// decimal: "@-0.5" is half a second before the epoch. It returns the instant
// so the caller can set exactly that mtime.
func parseEpochNanos(s string) (time.Time, error) {
	t, err := epochtime.Parse(s)
	if err != nil {
		return time.Time{}, err
	}
	return t, nil
}
