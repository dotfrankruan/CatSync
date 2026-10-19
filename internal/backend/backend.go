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
}

// RsyncTransport renders the rsync -e (remote shell) value: "ssh" or, when
// IPv6 is requested, "ssh -6". The value is a fixed token sequence chosen by
// CatSync — never user data — so rsync's whitespace splitting of the
// transport command cannot be influenced by operands.
func (s SSH) RsyncTransport() string {
	if s.IPv6 {
		return "ssh -6"
	}
	return "ssh"
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
