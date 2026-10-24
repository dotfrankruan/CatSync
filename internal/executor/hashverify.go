package executor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/dotfrankruan/CatSync/internal/backend"
	"github.com/dotfrankruan/CatSync/internal/discovery"
	"github.com/dotfrankruan/CatSync/internal/process"
	"github.com/dotfrankruan/CatSync/internal/state"
	"github.com/dotfrankruan/CatSync/internal/target"
)

type remoteHashTool string

const (
	hashSHA256Sum remoteHashTool = "sha256sum"
	hashSHASum    remoteHashTool = "shasum"
	hashOpenSSL   remoteHashTool = "openssl"
	hashBusyBox   remoteHashTool = "busybox"
)

const noRemoteHashToolWarning = "SHA-256 CONTENT VERIFICATION WAS NOT PERFORMED: the remote endpoint has no supported SHA-256 utility (sha256sum, shasum, openssl, or busybox sha256sum). Only file-size verification was performed. Install one of those tools on the remote endpoint to enable content verification. CatSync did not download and will never download remote files as a hash-verification fallback."

func remoteCommandArgv(program string, remote target.Destination, ipv6 bool, script string) ([]string, error) {
	if program == "" {
		program = "ssh"
	}
	if strings.ContainsRune(program, 0) {
		return nil, errors.New("executor: ssh program contains a NUL byte")
	}
	who := remote.Host
	if remote.User != "" {
		who = remote.User + "@" + remote.Host
	}
	argv := []string{program}
	if ipv6 {
		argv = append(argv, "-6")
	}
	return append(argv, "-T", who, script), nil
}

func (e *Executor) detectRemoteHashTool(ctx context.Context) (remoteHashTool, error) {
	const script = "if command -v sha256sum >/dev/null 2>&1; then printf sha256sum; " +
		"elif command -v shasum >/dev/null 2>&1; then printf shasum; " +
		"elif command -v openssl >/dev/null 2>&1; then printf openssl; " +
		"elif command -v busybox >/dev/null 2>&1; then printf busybox; " +
		"else printf none; fi"
	argv, err := remoteCommandArgv(e.progs.sshProgram(), e.destTarget(), e.run.Config.IPv6, script)
	if err != nil {
		return "", err
	}
	var stdout, stderr strings.Builder
	if err := process.Run(ctx, process.Spec{Name: argv[0], Args: argv[1:], Env: e.cfg.Env, Stdout: &stdout, Stderr: &stderr}); err != nil {
		return "", fmt.Errorf("detect remote SHA-256 utility: %w%s", err, stderrSuffix(stderr.String()))
	}
	tool := remoteHashTool(strings.TrimSpace(stdout.String()))
	switch tool {
	case hashSHA256Sum, hashSHASum, hashOpenSSL, hashBusyBox:
		return tool, nil
	case "none":
		return "", nil
	default:
		return "", fmt.Errorf("detect remote SHA-256 utility: unexpected response %q", tool)
	}
}

func stderrSuffix(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	return " (" + firstLine(s) + ")"
}

// remoteHashes asks the remote host to hash the named paths in bounded
// command batches. It returns only digest records; it never reads file data
// through SSH/SFTP and therefore can never turn verification into a hidden
// download.
func (e *Executor) remoteHashes(ctx context.Context, tool remoteHashTool, tasks []*state.Task) (map[string]string, error) {
	const maxScriptBytes = 48 << 10
	result := make(map[string]string, len(tasks))
	for start := 0; start < len(tasks); {
		var script strings.Builder
		script.WriteString("set -e; ")
		end := start
		for end < len(tasks) {
			t := tasks[end]
			remotePath := backend.RemotePath(e.destTarget().Path, t.Path)
			cmd := hashCommand(tool, t.Path, remotePath)
			if end > start && script.Len()+len(cmd) > maxScriptBytes {
				break
			}
			script.WriteString(cmd)
			end++
		}
		argv, err := remoteCommandArgv(e.progs.sshProgram(), e.destTarget(), e.run.Config.IPv6, script.String())
		if err != nil {
			return nil, err
		}
		var stdout, stderr strings.Builder
		if err := process.Run(ctx, process.Spec{Name: argv[0], Args: argv[1:], Env: e.cfg.Env, Stdout: &stdout, Stderr: &stderr}); err != nil {
			return nil, fmt.Errorf("remote SHA-256 with %s: %w%s", tool, err, stderrSuffix(stderr.String()))
		}
		if err := parseHashRecords(stdout.String(), result); err != nil {
			return nil, err
		}
		start = end
	}
	if len(result) != len(tasks) {
		return nil, fmt.Errorf("remote SHA-256 returned %d records for %d files", len(result), len(tasks))
	}
	return result, nil
}

func hashCommand(tool remoteHashTool, rel, absolute string) string {
	qpath := discovery.ShellQuote(absolute)
	var invoke string
	switch tool {
	case hashSHA256Sum:
		invoke = "sha256sum < " + qpath
	case hashSHASum:
		invoke = "shasum -a 256 -b < " + qpath
	case hashOpenSSL:
		invoke = "openssl dgst -sha256 -r < " + qpath
	case hashBusyBox:
		invoke = "busybox sha256sum < " + qpath
	}
	return "out=$(" + invoke + ") || exit 45; h=${out%% *}; printf '%s\\000%s\\000' " + discovery.ShellQuote(rel) + " \"$h\"; "
}

func parseHashRecords(data string, out map[string]string) error {
	fields := strings.Split(data, "\x00")
	if len(fields) == 0 || fields[len(fields)-1] != "" || (len(fields)-1)%2 != 0 {
		return errors.New("remote SHA-256 returned a malformed NUL-delimited response")
	}
	for i := 0; i < len(fields)-1; i += 2 {
		rel, digest := fields[i], strings.ToLower(fields[i+1])
		if err := discovery.ValidateRelPath(rel); err != nil {
			return fmt.Errorf("remote SHA-256 path: %w", err)
		}
		decoded, err := hex.DecodeString(digest)
		if err != nil || len(decoded) != sha256.Size {
			return fmt.Errorf("remote SHA-256 returned invalid digest for %q", rel)
		}
		if _, exists := out[rel]; exists {
			return fmt.Errorf("remote SHA-256 returned duplicate path %q", rel)
		}
		out[rel] = digest
	}
	return nil
}

func (e *Executor) localHash(t *state.Task) (string, error) {
	p := filepath.Join(e.localRoot(), filepath.FromSlash(t.Path))
	f, err := os.Open(p)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
