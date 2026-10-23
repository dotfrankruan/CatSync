// Package target validates local and remote CatSync transfer operands.
package target

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Direction identifies which endpoint owns the source data.
type Direction string

const (
	LocalToRemote Direction = "local-to-remote"
	RemoteToLocal Direction = "remote-to-local"
)

// Transfer is a validated pair containing exactly one local endpoint and
// one remote endpoint. LocalPath is the local source root for LocalToRemote
// and the local destination root for RemoteToLocal.
type Transfer struct {
	Direction Direction
	LocalPath string
	Remote    Destination
}

// ParseTransfer validates a SOURCE/DESTINATION operand pair. Exactly one
// operand must use the remote [user@]host:/absolute/path syntax.
func ParseTransfer(source, destination string) (Transfer, error) {
	sourceRemote := looksRemote(source)
	destinationRemote := looksRemote(destination)
	if sourceRemote == destinationRemote {
		if sourceRemote {
			return Transfer{}, fmt.Errorf("remote-to-remote transfers are not supported; exactly one endpoint must be local")
		}
		return Transfer{}, fmt.Errorf("local-to-local transfers are not supported; exactly one endpoint must be remote")
	}
	if sourceRemote {
		remote, err := ParseDestination(source)
		if err != nil {
			return Transfer{}, fmt.Errorf("remote source: %w", err)
		}
		local, err := ValidateLocalDestination(destination)
		if err != nil {
			return Transfer{}, err
		}
		return Transfer{Direction: RemoteToLocal, LocalPath: local, Remote: remote}, nil
	}
	local, err := ValidateSource(source)
	if err != nil {
		return Transfer{}, err
	}
	remote, err := ParseDestination(destination)
	if err != nil {
		return Transfer{}, err
	}
	return Transfer{Direction: LocalToRemote, LocalPath: local, Remote: remote}, nil
}

// looksRemote recognizes an intended rsync/SSH operand without treating an
// ordinary absolute local path as remote. Validation remains ParseDestination's
// responsibility so malformed remote operands produce a useful error.
func looksRemote(s string) bool {
	if s == "" || filepath.IsAbs(s) {
		return false
	}
	return separatorColon(s) >= 0
}

// ValidateLocalDestination returns an absolute local destination directory.
// An absent path is allowed because sync creates it later; an existing path
// must be a readable directory. Symlinked directory roots are accepted just
// like local source roots.
func ValidateLocalDestination(p string) (string, error) {
	if p == "" {
		return "", fmt.Errorf("local destination path is empty")
	}
	if strings.ContainsRune(p, 0) {
		return "", fmt.Errorf("local destination path %q: NUL bytes are not allowed", p)
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		return "", fmt.Errorf("local destination %q: resolve absolute path: %w", p, err)
	}
	info, err := os.Stat(abs)
	if err != nil {
		if os.IsNotExist(err) {
			return abs, nil
		}
		return "", fmt.Errorf("local destination %q: %w", p, err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("local destination %q is not a directory", p)
	}
	f, err := os.Open(abs)
	if err != nil {
		return "", fmt.Errorf("local destination %q is not readable: %w", p, err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("local destination %q: close: %w", p, err)
	}
	return abs, nil
}

// Destination is a validated remote destination.
type Destination struct {
	// User is the optional remote user name; empty means the SSH default.
	User string
	// Host is the remote host name or address. Brackets around IPv6
	// literals are stripped.
	Host string
	// Path is the absolute path on the remote host.
	Path string
}

// String renders the canonical [user@]host:path form. IPv6 literals
// (hosts containing a colon) are re-bracketed.
func (d Destination) String() string {
	host := d.Host
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
		host = "[" + host + "]"
	}
	if d.User != "" {
		return d.User + "@" + host + ":" + d.Path
	}
	return host + ":" + d.Path
}

// ParseDestination parses and validates a remote destination of the form
// [user@]host:path. An error is returned for local paths (no host part),
// empty hosts, empty or non-absolute paths, and disallowed characters.
func ParseDestination(s string) (Destination, error) {
	var d Destination
	if s == "" {
		return d, fmt.Errorf("destination is empty; want [user@]host:path")
	}
	if strings.ContainsRune(s, 0) {
		return d, fmt.Errorf("destination %q: NUL bytes are not allowed", s)
	}

	rest := s
	// A user part exists only when '@' appears before the host/path
	// separator colon. Bracketed IPv6 literals complicate a naive search,
	// so find the separator colon first.
	colon := separatorColon(rest)
	if colon < 0 {
		return d, fmt.Errorf("destination %q: missing host:path separator; remote destinations look like [user@]host:path", s)
	}
	if at := strings.Index(rest, "@"); at >= 0 && at < colon {
		d.User = rest[:at]
		if d.User == "" {
			return d, fmt.Errorf("destination %q: user before '@' is empty", s)
		}
		rest = rest[at+1:]
		colon = separatorColon(rest)
	}

	switch {
	case strings.HasPrefix(rest, "["):
		end := strings.Index(rest, "]")
		if end < 0 {
			return d, fmt.Errorf("destination %q: unterminated '[' in host", s)
		}
		d.Host = rest[1:end]
		if len(rest) <= end+1 || rest[end+1] != ':' {
			return d, fmt.Errorf("destination %q: expected ':' after bracketed host", s)
		}
		d.Path = rest[end+2:]
	default:
		d.Host = rest[:colon]
		d.Path = rest[colon+1:]
	}

	if err := validateUser(d.User); err != nil {
		return d, fmt.Errorf("destination %q: %w", s, err)
	}
	if err := validateHost(d.Host); err != nil {
		return d, fmt.Errorf("destination %q: %w", s, err)
	}
	if d.Path == "" {
		return d, fmt.Errorf("destination %q: remote path is empty", s)
	}
	if !strings.HasPrefix(d.Path, "/") {
		return d, fmt.Errorf("destination %q: remote path %q is not absolute; use an absolute path to avoid ambiguity", s, d.Path)
	}
	return d, nil
}

// separatorColon returns the index of the colon separating host from path,
// skipping colons inside a bracketed IPv6 literal. It returns -1 when there
// is no usable separator.
func separatorColon(s string) int {
	if strings.HasPrefix(s, "[") {
		end := strings.Index(s, "]")
		if end < 0 {
			return -1
		}
		if len(s) > end+1 && s[end+1] == ':' {
			return end + 1
		}
		return -1
	}
	return strings.Index(s, ":")
}

func validateUser(u string) error {
	if u == "" {
		return nil
	}
	for _, r := range u {
		if !isUserChar(r) {
			return fmt.Errorf("user %q contains disallowed character %q", u, r)
		}
	}
	return nil
}

func validateHost(h string) error {
	if h == "" {
		return fmt.Errorf("host is empty")
	}
	for _, r := range h {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case strings.ContainsRune("._:-[]%", r):
		default:
			return fmt.Errorf("host %q contains disallowed character %q", h, r)
		}
	}
	return nil
}

func isUserChar(r rune) bool {
	switch {
	case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		return true
	case strings.ContainsRune("._-", r):
		return true
	}
	return false
}

// ValidateSource checks that path is an existing, readable directory and
// returns its absolute form. Symlinks to directories are accepted; the
// returned path preserves the caller's spelling, made absolute.
func ValidateSource(path string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("source path is empty")
	}
	if strings.ContainsRune(path, 0) {
		return "", fmt.Errorf("source path %q: NUL bytes are not allowed", path)
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("source %q: %w", path, err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("source %q is not a directory", path)
	}
	// Opening proves the directory is readable by this user, which the
	// discovery stage depends on.
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("source %q is not readable: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("source %q: close: %w", path, err)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("source %q: resolve absolute path: %w", path, err)
	}
	return abs, nil
}
