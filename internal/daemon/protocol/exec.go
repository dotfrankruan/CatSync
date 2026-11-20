package protocol

import (
	"fmt"
	"path"
	"strings"
	"unicode/utf8"
)

// Exec ALPN and framing constants (frozen exec protocol).
const (
	// MaxExecStartBytes bounds the JSON start line.
	MaxExecStartBytes = 1 << 20
	// MaxExecFrameBytes bounds one binary frame payload.
	MaxExecFrameBytes = 1 << 20
	// MaxExecArgs bounds the remote argument vector.
	MaxExecArgs = 64
	// ExecProgramRsync is the only executable the M3 server will run.
	ExecProgramRsync = "rsync"
)

// Exec frame types: a one-byte type and a big-endian uint32 payload length.
const (
	ExecFrameStdin    byte = 1
	ExecFrameStdout   byte = 2
	ExecFrameStderr   byte = 3
	ExecFrameExit     byte = 4
	ExecFrameError    byte = 5
	ExecFrameCancel   byte = 6
	ExecFrameStdinEOF byte = 7
)

// ExecStart is the JSON start record for an exec stream.
type ExecStart struct {
	Type    string   `json:"type"`
	Program string   `json:"program"`
	Args    []string `json:"args"`
}

// allowedServerLongOptions is the exact long-option vocabulary rsync 3.x
// sends to a --server child for CatSync's fixed option set.
var allowedServerLongOptions = map[string]bool{
	"--server":          true,
	"--sender":          true,
	"--partial":         true,
	"--delete-delay":    true,
	"--files-from=-":    true,
	"--from0":           true,
	"--numeric-ids":     true,
	"--protect-args":    true,
	"--relative":        true,
	"--no-implied-dirs": true,
	// --partial-dir takes its value as the next argument.
	"--partial-dir": true,
}

// shortFlagChars is the allowed alphabet of rsync's compact server flag
// string. It covers every flag CatSync's option set can cause rsync to emit
// (archive/checksum/hard-links/acls/xattrs/delete-delay/partial/relative/
// files-from/from0/protect-args/dry-run/itemize/no-implied-dirs), learned
// from golden argv captures against rsync 3.5.
const shortFlagChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789."

// ValidateExecStart checks an rsync server invocation and returns the
// argument vector to execute (without argv[0]). It accepts exactly the
// shapes CatSync's own rsync invocations produce and rejects everything
// else: no shell, no arbitrary programs, no arbitrary remote options.
func ValidateExecStart(program string, args []string) error {
	if program != ExecProgramRsync && path.Base(program) != ExecProgramRsync {
		return NewError(CodeExecRejected, "program %q is not allowed (only rsync)", program)
	}
	if len(args) == 0 || len(args) > MaxExecArgs {
		return NewError(CodeExecRejected, "rsync argv has %d elements, want 1..%d", len(args), MaxExecArgs)
	}
	if args[0] != "--server" {
		return NewError(CodeExecRejected, "rsync argv must begin with --server, got %q", args[0])
	}
	i := 1
	if i < len(args) && args[i] == "--sender" {
		i++
	}
	for ; i < len(args) && args[i] != "."; i++ {
		arg := args[i]
		switch {
		case strings.HasPrefix(arg, "--"):
			if !allowedServerLongOptions[arg] {
				return NewError(CodeExecRejected, "rsync option %q is not allowed", arg)
			}
			if arg == "--partial-dir" {
				i++
				if i >= len(args) || !safeRelativeValue(args[i]) {
					return NewError(CodeExecRejected, "rsync --partial-dir value is missing or unsafe")
				}
			}
		case strings.HasPrefix(arg, "-") && len(arg) > 1:
			for _, r := range arg[1:] {
				if !strings.ContainsRune(shortFlagChars, r) {
					return NewError(CodeExecRejected, "rsync flag %q contains an unsupported character", arg)
				}
			}
		default:
			return NewError(CodeExecRejected, "unexpected rsync argument %q", arg)
		}
	}
	if i >= len(args) || args[i] != "." {
		return NewError(CodeExecRejected, "rsync argv is missing the '.' separator")
	}
	paths := args[i+1:]
	if len(paths) != 1 {
		return NewError(CodeExecRejected, "rsync argv must carry exactly one path, got %d", len(paths))
	}
	if err := validateExecPath(paths[0]); err != nil {
		return err
	}
	return nil
}

// safeRelativeValue accepts a relative --partial-dir style value.
func safeRelativeValue(v string) bool {
	if v == "" || len(v) > MaxPathBytes || !utf8.ValidString(v) {
		return false
	}
	if strings.HasPrefix(v, "/") || strings.ContainsAny(v, "\x00\n\r") {
		return false
	}
	if err := ValidateRelPath(v); err != nil {
		return false
	}
	return true
}

func validateExecPath(p string) error {
	if p == "" || len(p) > MaxPathBytes || !utf8.ValidString(p) {
		return NewError(CodeExecRejected, "rsync path is empty, too long, or not UTF-8")
	}
	if strings.ContainsAny(p, "\x00\n\r") {
		return NewError(CodeExecRejected, "rsync path contains control characters")
	}
	clean := path.Clean(p)
	if !strings.HasPrefix(clean, "/") {
		return NewError(CodeExecRejected, "rsync path %q is not absolute", p)
	}
	return nil
}

// ExecExitPayload encodes/decodes the exit frame payload.
func ExecExitPayload(code int) []byte {
	if code < 0 {
		code = 255
	}
	return []byte{byte(code >> 24), byte(code >> 16), byte(code >> 8), byte(code)}
}

// ParseExecExitPayload decodes an exit frame payload.
func ParseExecExitPayload(b []byte) (int, error) {
	if len(b) != 4 {
		return 0, fmt.Errorf("exec exit payload has %d bytes, want 4", len(b))
	}
	return int(int32(uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3]))), nil
}
