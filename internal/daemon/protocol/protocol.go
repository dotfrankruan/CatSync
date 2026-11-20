// Package protocol holds the wire types shared by catsync and catsyncd.
//
// Both ends decode these structures with the strict decoder, so the field
// names, required fields, error codes, and HTTP status mapping live in one
// place: neither side guesses the other's interface.
package protocol

import (
	"fmt"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/dotfrankruan/CatSync/internal/discovery"
)

// Version is the bootstrap/RPC protocol version. It changes only with a
// frozen-protocol revision.
const Version = 1

// ALPN protocol names.
const (
	ALPNHTTP = "catsync-http/1.1"
	ALPNSFTP = "catsync-sftp/1"
	ALPNExec = "catsync-exec/1"
)

// Control line and RPC body limits.
const (
	MaxControlLineBytes = 64 << 10
	MaxRPCBodyBytes     = 1 << 20
	MaxPathBytes        = 64 << 10
	MaxDirsPerRequest   = 4096
	MaxPathsPerRequest  = 100000
)

// Manifest modes. The mode chosen by the caller is echoed in the first
// response record; each mode has its own traversal and missing-path rules.
type ManifestMode string

const (
	// ModeDestTree lists the regular files of a destination directory.
	ModeDestTree ManifestMode = "dest_tree"
	// ModeSourceTree lists every non-directory entry of a source directory.
	ModeSourceTree ManifestMode = "source_tree"
	// ModeSourceFile describes one single-file source by base name.
	ModeSourceFile ManifestMode = "source_file"
)

// Valid reports whether m is a known mode.
func (m ManifestMode) Valid() bool {
	switch m {
	case ModeDestTree, ModeSourceTree, ModeSourceFile:
		return true
	}
	return false
}

// Entry kinds.
const (
	KindFile    = "file"
	KindSymlink = "symlink"
	KindSpecial = "special"
)

// NDJSON record types.
const (
	RecordTypeMode  = "mode"
	RecordTypeEntry = "entry"
	RecordTypeHash  = "hash"
	RecordTypeEnd   = "end"
	RecordTypeError = "error"
)

// Error codes. Client-side integrity failures have their own codes.
const (
	CodeDaemonNotInstalled      = "DAEMON_NOT_INSTALLED"
	CodeBootstrapProtocolError  = "BOOTSTRAP_PROTOCOL_ERROR"
	CodeProtocolVersionMismatch = "PROTOCOL_VERSION_MISMATCH"
	CodeTLSPeerMismatch         = "TLS_PEER_MISMATCH"
	CodeTLSCertInvalid          = "TLS_CERT_INVALID"
	CodeFeatureUnavailable      = "FEATURE_UNAVAILABLE"
	CodePortInUse               = "PORT_IN_USE"
	CodeConnectionLimit         = "CONNECTION_LIMIT"
	CodeControlChannelLost      = "CONTROL_CHANNEL_LOST"
	CodeShutdownTimeout         = "SHUTDOWN_TIMEOUT"
	CodeSSHAuthFailed           = "SSH_AUTH_FAILED"
	CodeNotFound                = "ENOENT"
	CodeNotDir                  = "ENOTDIR"
	CodePermission              = "EACCES"
	CodeInvalidArgument         = "INVALID_ARGUMENT"
	CodeUnsupportedPathEncoding = "UNSUPPORTED_PATH_ENCODING"
	CodeTooLarge                = "TOO_LARGE"
	CodeInternal                = "INTERNAL"
	CodeManifestTooLarge        = "MANIFEST_TOO_LARGE"
	CodeManifestIncomplete      = "MANIFEST_INCOMPLETE"
	CodeHashIncomplete          = "HASH_INCOMPLETE"
	CodeJSONDuplicateKey        = "JSON_DUPLICATE_KEY"
	CodeJSONTrailingData        = "JSON_TRAILING_DATA"
	CodeJSONTooDeep             = "JSON_TOO_DEEP"
	CodeJSONTooLarge            = "JSON_TOO_LARGE"
	CodeJSONUnknownField        = "JSON_UNKNOWN_FIELD"
	CodeJSONTypeMismatch        = "JSON_TYPE_MISMATCH"
	CodeJSONMissingField        = "JSON_MISSING_FIELD"
	CodeExecRejected            = "EXEC_REJECTED"
	CodeRemoteProcessFailed     = "REMOTE_PROCESS_FAILED"
)

// Error is a coded protocol error that the peers map to HTTP statuses and
// diagnostics.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Error implements error.
func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Message == "" {
		return e.Code
	}
	return e.Code + ": " + e.Message
}

// NewError builds a coded error.
func NewError(code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// HTTPStatus maps a protocol error code to the HTTP status used by the RPC
// layer. Stream records reuse the same codes.
func HTTPStatus(code string) int {
	switch code {
	case CodeNotFound:
		return 404
	case CodePermission:
		return 403
	case CodeTooLarge, CodeManifestTooLarge:
		return 413
	case CodeInvalidArgument, CodeNotDir, CodeUnsupportedPathEncoding,
		CodeJSONDuplicateKey, CodeJSONTrailingData, CodeJSONTooDeep,
		CodeJSONTooLarge, CodeJSONUnknownField, CodeJSONTypeMismatch,
		CodeJSONMissingField:
		return 400
	case CodeFeatureUnavailable:
		return 501
	case CodeConnectionLimit:
		return 429
	default:
		return 500
	}
}

// Features advertises capabilities that are actually available.
type Features struct {
	SFTP     bool `json:"sftp"`
	Exec     bool `json:"exec"`
	Manifest bool `json:"manifest"`
	Metadata bool `json:"metadata"`
	Hash     bool `json:"hash"`
	Stat     bool `json:"stat"`
	SameFile bool `json:"samefile"`
}

// Limits are the server's concurrency budgets.
type Limits struct {
	MaxSFTPConns        int `json:"max_sftp_conns"`
	MaxExecConns        int `json:"max_exec_conns"`
	MaxControlConns     int `json:"max_control_conns"`
	MaxPendingHandshake int `json:"max_pending_handshakes"`
	MaxConns            int `json:"max_conns"`
}

// ValidateAbsPath checks an absolute remote path: valid UTF-8, no NUL,
// absolute, already clean, and bounded.
func ValidateAbsPath(p string) error {
	if p == "" {
		return NewError(CodeInvalidArgument, "path is empty")
	}
	if !utf8.ValidString(p) {
		return NewError(CodeUnsupportedPathEncoding, "path is not valid UTF-8")
	}
	if strings.ContainsRune(p, 0) {
		return NewError(CodeInvalidArgument, "path contains a NUL byte")
	}
	if len(p) > MaxPathBytes {
		return NewError(CodeTooLarge, "path exceeds %d bytes", MaxPathBytes)
	}
	if !filepath.IsAbs(p) {
		return NewError(CodeInvalidArgument, "path %q is not absolute", p)
	}
	if filepath.Clean(p) != p {
		return NewError(CodeInvalidArgument, "path %q is not clean", p)
	}
	return nil
}

// ValidateRelPath checks a manifest-relative path: valid UTF-8 plus the
// existing discovery rules.
func ValidateRelPath(p string) error {
	if !utf8.ValidString(p) {
		return NewError(CodeUnsupportedPathEncoding, "path is not valid UTF-8")
	}
	if err := discovery.ValidateRelPath(p); err != nil {
		return NewError(CodeInvalidArgument, "%v", err)
	}
	return nil
}

// ManifestRequest is POST /v1/manifest.
type ManifestRequest struct {
	Root string       `json:"root"`
	Mode ManifestMode `json:"mode"`
}

// ManifestModeRecord starts a manifest stream and echoes the mode.
type ManifestModeRecord struct {
	Type string       `json:"type"`
	Mode ManifestMode `json:"mode"`
}

// ManifestEntryRecord is one manifest entry.
type ManifestEntryRecord struct {
	Type    string `json:"type"`
	Path    string `json:"path"`
	Kind    string `json:"kind"`
	Size    int64  `json:"size"`
	MtimeNS int64  `json:"mtime_ns"`
	Perm    uint32 `json:"mode"`
}

// HashRecord is one SHA-256 result.
type HashRecord struct {
	Type   string `json:"type"`
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

// EndRecord terminates a streaming response. It is the only success marker.
type EndRecord struct {
	Type  string `json:"type"`
	Count int    `json:"count"`
}

// ErrorRecord reports a mid-stream failure; no EndRecord follows.
type ErrorRecord struct {
	Type    string `json:"type"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// MkdirRequest is POST /v1/mkdir.
type MkdirRequest struct {
	Dirs []string `json:"dirs"`
}

// MkdirResponse reports how many directories the call ensured.
type MkdirResponse struct {
	Created int `json:"created"`
}

// MetadataRequest is POST /v1/metadata: mode bits and exact nanosecond
// mtime for a regular file.
type MetadataRequest struct {
	Path    string `json:"path"`
	Mode    uint32 `json:"mode"`
	MtimeNS int64  `json:"mtime_ns"`
}

// MetadataResponse reports success.
type MetadataResponse struct {
	OK bool `json:"ok"`
}

// HashRequest is POST /v1/hash.
type HashRequest struct {
	Paths []string `json:"paths"`
}

// StatRequest is POST /v1/stat.
type StatRequest struct {
	Path string `json:"path"`
}

// StatResponse reports Lstat metadata; symlinks are never followed.
type StatResponse struct {
	Type    string `json:"type"`
	Size    int64  `json:"size"`
	MtimeNS int64  `json:"mtime_ns"`
	Perm    uint32 `json:"mode"`
}

// Stat type names.
const (
	StatFile    = "file"
	StatSymlink = "symlink"
	StatOther   = "other"
)

// SameFileRequest is POST /v1/samefile.
type SameFileRequest struct {
	A string `json:"a"`
	B string `json:"b"`
}

// SameFileResponse reports whether both paths resolve to one inode.
type SameFileResponse struct {
	Same bool `json:"same"`
}

// ErrorResponse is the JSON body of a non-streaming RPC failure.
type ErrorResponse struct {
	Error *Error `json:"error"`
}
