package protocol

// QUIC connection-level ALPN and per-stream type headers (M5).
const (
	ALPNQUIC = "catsync-quic/1"

	// Stream types are the first byte on every client-opened QUIC stream.
	StreamControl byte = 1
	StreamSFTP    byte = 2
	StreamExec    byte = 3

	// MaxQUICControlFrameBytes bounds one control request/response frame.
	MaxQUICControlFrameBytes = 1 << 20
	// QUICControlStatusOK / Error are the response status bytes on a
	// control stream.
	QUICControlStatusOK    byte = 0
	QUICControlStatusError byte = 1
)

// QUICControlRequest is the request frame on a CONTROL stream: the RPC path
// plus the strict JSON body of the existing request type.
type QUICControlRequest struct {
	Path string `json:"path"`
	Body []byte `json:"body"`
}

// IsControlPath reports whether path is one of the six frozen RPC paths.
func IsControlPath(path string) bool {
	switch path {
	case "/v1/manifest", "/v1/mkdir", "/v1/metadata", "/v1/hash", "/v1/stat", "/v1/samefile",
		"/v1/handoff", "/v1/heartbeat", "/v1/bye":
		return true
	}
	return false
}

// ValidStreamType reports whether b is a known client stream type.
func ValidStreamType(b byte) bool {
	return b == StreamControl || b == StreamSFTP || b == StreamExec
}
