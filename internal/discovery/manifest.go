package discovery

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/frankruan/catsync/internal/target"
)

// Remote manifest format.
//
// The destination tree manifest is a stream of NUL-delimited records, one
// per remote regular file:
//
//	path  NUL  size  NUL  mtime  NUL
//
// path is slash-separated and relative to the destination root (find's %P),
// validated with ValidateRelPath so a manifest can never name a file
// outside the destination tree. size is the decimal byte count. mtime is
// decimal seconds since the Unix epoch with an optional fractional part of
// up to 9 digits (nanoseconds), as printed by find's %T@. NUL is the only
// delimiter, so paths may contain spaces, newlines, quotes, and any other
// byte except NUL.
//
// Parsing is streaming: at most one record is held in memory, each field is
// length-bounded, and malformed input is rejected with a record-numbered
// error wrapping ErrMalformedManifest.
const (
	// MaxManifestPathBytes bounds one path field in a manifest record.
	MaxManifestPathBytes = 1 << 16
	// maxManifestNumBytes bounds the size and mtime fields.
	maxManifestNumBytes = 64
)

// ErrMalformedManifest wraps every manifest parsing error; test it with
// errors.Is.
var ErrMalformedManifest = errors.New("discovery: malformed manifest")

// RemoteEntry is one remote regular file described by the manifest.
type RemoteEntry struct {
	// Path is the slash-separated path relative to the destination root.
	Path string
	// Size is the remote size in bytes.
	Size int64
	// ModTime is the remote modification time (UTC).
	ModTime time.Time
}

// ParseManifest streams manifest records from r, delivering each to emit.
// It stops and returns the first emit error. Malformed input produces a
// record-numbered error wrapping ErrMalformedManifest. An empty stream is a
// valid empty manifest.
func ParseManifest(r io.Reader, emit func(RemoteEntry) error) error {
	if emit == nil {
		return errors.New("discovery: emit callback is nil")
	}
	br := bufio.NewReader(r)
	for rec := 1; ; rec++ {
		path, eof, err := readManifestField(br, MaxManifestPathBytes)
		if err != nil {
			return malformedf(rec, "path: %v", err)
		}
		if eof {
			return nil
		}
		sizeField, eof, err := readManifestField(br, maxManifestNumBytes)
		if err != nil {
			return malformedf(rec, "size: %v", err)
		}
		if eof {
			return malformedf(rec, "truncated: stream ended after the path field")
		}
		mtimeField, eof, err := readManifestField(br, maxManifestNumBytes)
		if err != nil {
			return malformedf(rec, "mtime: %v", err)
		}
		if eof {
			return malformedf(rec, "truncated: stream ended after the size field")
		}

		if err := ValidateRelPath(path); err != nil {
			return malformedf(rec, "path: %v", err)
		}
		size, err := parseManifestSize(sizeField)
		if err != nil {
			return malformedf(rec, "size: %v", err)
		}
		mtime, err := parseManifestTime(mtimeField)
		if err != nil {
			return malformedf(rec, "mtime: %v", err)
		}
		if err := emit(RemoteEntry{Path: path, Size: size, ModTime: mtime}); err != nil {
			return err
		}
	}
}

// CollectManifest parses the manifest from r into a path-keyed map. A path
// appearing twice is rejected as malformed, so downstream consumers never
// see ambiguous metadata.
func CollectManifest(r io.Reader) (map[string]RemoteEntry, error) {
	entries := make(map[string]RemoteEntry)
	rec := 0
	err := ParseManifest(r, func(e RemoteEntry) error {
		rec++
		if _, dup := entries[e.Path]; dup {
			return malformedf(rec, "duplicate record for path %q", e.Path)
		}
		entries[e.Path] = e
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func malformedf(rec int, format string, args ...any) error {
	return fmt.Errorf("%w: record %d: %s", ErrMalformedManifest, rec, fmt.Sprintf(format, args...))
}

// readManifestField reads one NUL-terminated field, bounding its length to
// max bytes. It reports eof only when the stream ends cleanly at a field
// boundary; ending mid-field is a truncation error. The delimiter is
// consumed but not included in the result.
func readManifestField(br *bufio.Reader, max int) (field string, eof bool, err error) {
	var buf []byte
	for {
		frag, rerr := br.ReadSlice(0)
		if rerr == nil {
			// A complete field ends with the NUL delimiter; strip it.
			// ErrBufferFull and EOF fragments carry no delimiter.
			frag = frag[:len(frag)-1]
		}
		buf = append(buf, frag...)
		if len(buf) > max {
			return "", false, fmt.Errorf("field exceeds %d bytes", max)
		}
		switch {
		case rerr == bufio.ErrBufferFull:
			continue
		case rerr == io.EOF:
			if len(buf) == 0 {
				return "", true, nil
			}
			return "", false, errors.New("truncated field: missing NUL terminator")
		case rerr != nil:
			return "", false, rerr
		default:
			return string(buf), false, nil
		}
	}
}

// parseManifestSize parses a decimal byte count into an int64, rejecting
// empty, signed, non-numeric, and overflowing values.
func parseManifestSize(s string) (int64, error) {
	if s == "" {
		return 0, errors.New("empty size")
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q", s)
	}
	if n > math.MaxInt64 {
		return 0, fmt.Errorf("size %q overflows int64", s)
	}
	return int64(n), nil
}

// parseManifestTime parses "SECONDS[.FRACTION]" (epoch seconds with up to 9
// fractional digits) into a UTC time. Signs and other non-digit characters
// are rejected.
func parseManifestTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, errors.New("empty mtime")
	}
	secPart, fracPart, hasFrac := strings.Cut(s, ".")
	if !isDigits(secPart) {
		return time.Time{}, fmt.Errorf("invalid mtime %q", s)
	}
	sec, err := strconv.ParseInt(secPart, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid mtime %q", s)
	}
	var nsec int64
	if hasFrac {
		if len(fracPart) > 9 || !isDigits(fracPart) {
			return time.Time{}, fmt.Errorf("invalid mtime fraction %q", s)
		}
		// Right-pad to nanosecond precision: ".5" means 500ms.
		fracPart += strings.Repeat("0", 9-len(fracPart))
		nsec, err = strconv.ParseInt(fracPart, 10, 64)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid mtime %q", s)
		}
	}
	return time.Unix(sec, nsec).UTC(), nil
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// ManifestCommand builds the single ssh invocation that streams the
// destination tree manifest on its standard output. The result is an
// argument array for direct execution (exec.Command), never a shell command
// line: no local shell is involved and no arguments are concatenated into a
// local shell string.
//
// The remote command runs find on the destination path, printing one
// NUL-delimited record per regular file (see the format documentation
// above); find does not follow symlinks by default. The destination path is
// single-quote escaped for the remote shell, which ssh always involves on
// the remote side. The remote host needs GNU findutils for -printf; when
// collection fails, callers must plan with unknown (nil) destination
// metadata, which routes every file to rsync.
func ManifestCommand(dest target.Destination) ([]string, error) {
	if dest.Host == "" {
		return nil, errors.New("discovery: destination host is empty")
	}
	if dest.Path == "" {
		return nil, errors.New("discovery: destination path is empty")
	}
	if !strings.HasPrefix(dest.Path, "/") {
		return nil, fmt.Errorf("discovery: destination path %q is not absolute", dest.Path)
	}
	if strings.ContainsRune(dest.Path, 0) || strings.ContainsRune(dest.Host, 0) || strings.ContainsRune(dest.User, 0) {
		return nil, errors.New("discovery: destination contains a NUL byte")
	}
	who := dest.Host
	if dest.User != "" {
		who = dest.User + "@" + dest.Host
	}
	script := "find " + shellQuote(dest.Path) + " -type f -printf '%P\\0%s\\0%T@\\0'"
	return []string{"ssh", who, script}, nil
}

// shellQuote renders s as a single POSIX-shell word: wrapped in single
// quotes, with each embedded single quote replaced by the end-quote /
// escaped-quote / reopen-quote sequence. The result carries any byte
// except NUL — spaces, newlines, quotes, and expansions included — as
// literal text.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
