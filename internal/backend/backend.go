// Package backend holds the configuration and helpers shared by CatSync's
// external transfer backends. The backends themselves live in the
// subpackages:
//
//   - internal/backend/rsync: the conservative correctness fallback; it
//     transfers a disjoint set of relative paths with archive semantics and
//     never deletes.
//   - internal/backend/fpsync: the parallel small-file stage driving
//     fpart+fpsync, with large-file skipping and resumable runs.
//   - internal/backend/mscp: the parallel single-large-file stage with
//     checkpoint/resume.
//
// Every backend executes through internal/process with argument arrays —
// no shell is involved locally — and honors context cancellation.
package backend

import (
	"errors"
	"fmt"
	"strings"
	"unicode"

	"github.com/frankruan/catsync/internal/target"
)

// SSH carries the SSH transport options honored by the backends.
//
// fpsync has no per-run SSH option channel (it shells out to a stock rsync
// with default options), so these settings apply to the rsync and mscp
// backends only.
type SSH struct {
	// IPv6 restricts connections to IPv6 (rsync transport "ssh -6",
	// mscp -6).
	IPv6 bool
	// Program overrides the ssh executable used as rsync's remote shell
	// (-e). Empty means the default "ssh". The value becomes the first
	// token of rsync's -e value and is therefore restricted by
	// ValidateTransport to a single whitespace-free token.
	Program string
}

// RsyncTransport renders the rsync -e (remote shell) value: the transport
// program ("ssh" by default) plus " -6" when IPv6 is requested. The value
// is a fixed token sequence chosen by CatSync, never file or destination
// data; Program is vetted by ValidateTransport so rsync's whitespace
// splitting of the transport command cannot be influenced by operands.
func (s SSH) RsyncTransport() string {
	prog := s.Program
	if prog == "" {
		prog = "ssh"
	}
	if s.IPv6 {
		return prog + " -6"
	}
	return prog
}

// ValidateTransport checks that the ssh program override can be represented
// in rsync's -e value, which rsync splits on whitespace without quote
// interpretation. A value containing whitespace or control characters
// would be silently mis-split into a different command, and a leading dash
// could be mistaken for an option, so both are refused as configuration
// errors instead.
func (s SSH) ValidateTransport() error {
	prog := s.Program
	if prog == "" {
		return nil
	}
	if strings.ContainsRune(prog, 0) {
		return errors.New("backend: ssh program contains a NUL byte")
	}
	if strings.HasPrefix(prog, "-") {
		return fmt.Errorf("backend: ssh program %q must not begin with '-'", prog)
	}
	if strings.ContainsFunc(prog, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsControl(r)
	}) {
		return fmt.Errorf("backend: ssh program %q contains whitespace or control characters, which rsync's -e value cannot represent", prog)
	}
	return nil
}

// MSCPArgs returns the mscp argument tokens carrying the SSH options.
// Resume invocations must repeat them, matching the failed run's options.
func (s SSH) MSCPArgs() []string {
	if s.IPv6 {
		return []string{"-6"}
	}
	return nil
}

// ValidateDestination checks a destination for backend command use: host
// and path are present, the path is absolute, no field carries a NUL byte,
// and the rendered operand never begins with '-'. A leading dash would let
// an option parser mistake the operand for flags; backends reject such
// destinations explicitly instead of hoping the tool tolerates them.
func ValidateDestination(dest target.Destination) error {
	if dest.Host == "" {
		return errors.New("backend: destination host is empty")
	}
	if dest.Path == "" {
		return errors.New("backend: destination path is empty")
	}
	if !strings.HasPrefix(dest.Path, "/") {
		return fmt.Errorf("backend: destination path %q is not absolute", dest.Path)
	}
	for _, field := range []struct {
		name, value string
	}{
		{"user", dest.User}, {"host", dest.Host}, {"path", dest.Path},
	} {
		if strings.ContainsRune(field.value, 0) {
			return fmt.Errorf("backend: destination %s contains a NUL byte", field.name)
		}
	}
	if operand := dest.String(); strings.HasPrefix(operand, "-") {
		return fmt.Errorf("backend: destination operand %q begins with '-'; refusing to build a command a tool could parse as options", operand)
	}
	return nil
}

// RemotePath joins an absolute remote base path with a slash-separated
// relative path, e.g. ("/dest", "a/b.bin") -> "/dest/a/b.bin".
func RemotePath(base, rel string) string {
	return strings.TrimSuffix(base, "/") + "/" + rel
}
