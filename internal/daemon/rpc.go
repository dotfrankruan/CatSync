package daemon

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"sort"
	"time"

	"github.com/dotfrankruan/CatSync/internal/daemon/protocol"
	"github.com/dotfrankruan/CatSync/internal/daemon/strictjson"
)

// rpcWriter abstracts an RPC response so the same business functions serve
// HTTP/TCP connections and QUIC control streams.
type rpcWriter interface {
	// BeginJSON starts a one-object JSON response.
	BeginJSON()
	// JSON writes the response object after BeginJSON.
	JSON(v any) error
	// BeginStream starts an NDJSON stream response.
	BeginStream() *json.Encoder
	// Flush flushes buffered stream output when supported.
	Flush()
	// Abort reports a failure before any stream record was written.
	Abort(err error)
}

// httpRPCWriter adapts net/http to rpcWriter.
type httpRPCWriter struct {
	w      http.ResponseWriter
	enc    *json.Encoder
	stream bool
	broken bool
}

func (h *httpRPCWriter) BeginJSON() {
	h.w.Header().Set("Content-Type", "application/json")
	h.w.WriteHeader(http.StatusOK)
	h.enc = json.NewEncoder(h.w)
}

func (h *httpRPCWriter) JSON(v any) error {
	if h.enc == nil {
		h.BeginJSON()
	}
	return h.enc.Encode(v)
}

func (h *httpRPCWriter) BeginStream() *json.Encoder {
	h.w.Header().Set("Content-Type", "application/x-ndjson")
	h.w.WriteHeader(http.StatusOK)
	h.stream = true
	h.enc = json.NewEncoder(h.w)
	return h.enc
}

func (h *httpRPCWriter) Flush() {
	if f, ok := h.w.(http.Flusher); ok {
		f.Flush()
	}
}

func (h *httpRPCWriter) Abort(err error) { writeError(h.w, err) }

// httpHandler builds the mTLS RPC surface. The counter middleware tracks
// in-flight RPC handlers; it never wraps data connections.
func (s *server) httpHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/manifest", s.handleManifestHTTP)
	mux.HandleFunc("/v1/mkdir", s.handleMkdirHTTP)
	mux.HandleFunc("/v1/metadata", s.handleMetadataHTTP)
	mux.HandleFunc("/v1/hash", s.handleHashHTTP)
	mux.HandleFunc("/v1/stat", s.handleStatHTTP)
	mux.HandleFunc("/v1/samefile", s.handleSameFileHTTP)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.counters.rpcActive.Add(1)
		defer s.counters.rpcActive.Add(-1)
		mux.ServeHTTP(w, r)
	})
}

// manifestCfg returns the traversal budgets.
func (s *server) manifestCfg() manifestConfig {
	cfg := s.cfg.Manifest
	def := defaultManifestConfig()
	if cfg.MaxEntries <= 0 {
		cfg.MaxEntries = def.MaxEntries
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = def.MaxBytes
	}
	if cfg.MaxFrontierEntries <= 0 {
		cfg.MaxFrontierEntries = def.MaxFrontierEntries
	}
	if cfg.MaxFrontierBytes <= 0 {
		cfg.MaxFrontierBytes = def.MaxFrontierBytes
	}
	if cfg.MaxDirEntries <= 0 {
		cfg.MaxDirEntries = def.MaxDirEntries
	}
	return cfg
}

// decodeRequest reads at most MaxRPCBodyBytes and strictly decodes one JSON
// object. No side effects occur before this returns nil.
func decodeRequest(w http.ResponseWriter, r *http.Request, dst any) error {
	if r.Method != http.MethodPost {
		return protocol.NewError(protocol.CodeInvalidArgument, "method %s is not allowed", r.Method)
	}
	body := http.MaxBytesReader(w, r.Body, protocol.MaxRPCBodyBytes)
	data, err := io.ReadAll(body)
	if err != nil {
		return protocol.NewError(protocol.CodeTooLarge, "read request body: %v", err)
	}
	return strictjson.Decode(data, dst, strictjson.Limits{MaxBytes: protocol.MaxRPCBodyBytes, MaxDepth: 32})
}

// decodeBody strictly decodes one RPC body from bytes (QUIC control path).
func decodeBody(data []byte, dst any) error {
	return strictjson.Decode(data, dst, strictjson.Limits{MaxBytes: protocol.MaxRPCBodyBytes, MaxDepth: 32})
}

// writeJSON writes a JSON response.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError renders a protocol error as a JSON body.
func writeError(w http.ResponseWriter, err error) {
	pe := asProtocolError(err)
	writeJSON(w, protocol.HTTPStatus(pe.Code), protocol.ErrorResponse{Error: pe})
}

// asProtocolError normalises an error to a coded protocol error.
func asProtocolError(err error) *protocol.Error {
	var pe *protocol.Error
	if errors.As(err, &pe) {
		return pe
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return protocol.NewError(protocol.CodeControlChannelLost, "%v", err)
	}
	return protocol.NewError(protocol.CodeInternal, "%v", err)
}

// HTTP adapters ------------------------------------------------------------

func (s *server) handleManifestHTTP(w http.ResponseWriter, r *http.Request) {
	var req protocol.ManifestRequest
	if err := decodeRequest(w, r, &req); err != nil {
		writeError(w, err)
		return
	}
	s.manifestRPC(r.Context(), &req, &httpRPCWriter{w: w})
}

func (s *server) handleMkdirHTTP(w http.ResponseWriter, r *http.Request) {
	var req protocol.MkdirRequest
	if err := decodeRequest(w, r, &req); err != nil {
		writeError(w, err)
		return
	}
	s.mkdirRPC(&req, &httpRPCWriter{w: w})
}

func (s *server) handleMetadataHTTP(w http.ResponseWriter, r *http.Request) {
	var req protocol.MetadataRequest
	if err := decodeRequest(w, r, &req); err != nil {
		writeError(w, err)
		return
	}
	s.metadataRPC(&req, &httpRPCWriter{w: w})
}

func (s *server) handleHashHTTP(w http.ResponseWriter, r *http.Request) {
	var req protocol.HashRequest
	if err := decodeRequest(w, r, &req); err != nil {
		writeError(w, err)
		return
	}
	s.hashRPC(r.Context(), &req, &httpRPCWriter{w: w})
}

func (s *server) handleStatHTTP(w http.ResponseWriter, r *http.Request) {
	var req protocol.StatRequest
	if err := decodeRequest(w, r, &req); err != nil {
		writeError(w, err)
		return
	}
	s.statRPC(&req, &httpRPCWriter{w: w})
}

func (s *server) handleSameFileHTTP(w http.ResponseWriter, r *http.Request) {
	var req protocol.SameFileRequest
	if err := decodeRequest(w, r, &req); err != nil {
		writeError(w, err)
		return
	}
	s.sameFileRPC(&req, &httpRPCWriter{w: w})
}

// Business operations ------------------------------------------------------

// manifestRPC implements the three distinct manifest modes.
func (s *server) manifestRPC(ctx context.Context, req *protocol.ManifestRequest, w rpcWriter) {
	if !req.Mode.Valid() {
		w.Abort(protocol.NewError(protocol.CodeInvalidArgument, "unknown manifest mode %q", req.Mode))
		return
	}
	if err := protocol.ValidateAbsPath(req.Root); err != nil {
		w.Abort(err)
		return
	}
	if err := precheckManifest(req.Mode, req.Root); err != nil {
		w.Abort(err)
		return
	}
	enc := w.BeginStream()
	if err := enc.Encode(protocol.ManifestModeRecord{Type: protocol.RecordTypeMode, Mode: req.Mode}); err != nil {
		return
	}
	w.Flush()
	count, err := walkTree(ctx, req.Mode, req.Root, s.manifestCfg(), func(rec protocol.ManifestEntryRecord) error {
		return enc.Encode(rec)
	})
	if err != nil {
		pe := asProtocolError(err)
		_ = enc.Encode(protocol.ErrorRecord{Type: protocol.RecordTypeError, Code: pe.Code, Message: pe.Message})
		w.Flush()
		return
	}
	_ = enc.Encode(protocol.EndRecord{Type: protocol.RecordTypeEnd, Count: count})
	w.Flush()
}

// mkdirRPC ensures every requested directory exists.
func (s *server) mkdirRPC(req *protocol.MkdirRequest, w rpcWriter) {
	if len(req.Dirs) > protocol.MaxDirsPerRequest {
		w.Abort(protocol.NewError(protocol.CodeTooLarge, "too many directories"))
		return
	}
	dirs := append([]string(nil), req.Dirs...)
	sort.Strings(dirs)
	for _, d := range dirs {
		if err := protocol.ValidateAbsPath(d); err != nil {
			w.Abort(err)
			return
		}
		if err := os.MkdirAll(d, 0o755); err != nil {
			w.Abort(osError("create directory", err))
			return
		}
	}
	w.BeginJSON()
	_ = w.JSON(protocol.MkdirResponse{Created: len(dirs)})
}

// metadataRPC applies optional permission bits and exact nanosecond mtime to
// one regular file.
func (s *server) metadataRPC(req *protocol.MetadataRequest, w rpcWriter) {
	if err := protocol.ValidateAbsPath(req.Path); err != nil {
		w.Abort(err)
		return
	}
	info, err := os.Lstat(req.Path)
	if err != nil {
		w.Abort(osError("lstat metadata target", err))
		return
	}
	if !info.Mode().IsRegular() {
		w.Abort(protocol.NewError(protocol.CodeInvalidArgument, "metadata target %q is not a regular file", req.Path))
		return
	}
	if req.Mode != nil {
		if err := os.Chmod(req.Path, os.FileMode(*req.Mode&0o7777)); err != nil {
			w.Abort(osError("chmod metadata target", err))
			return
		}
	}
	t := unixNanoTime(req.MtimeNS)
	if err := os.Chtimes(req.Path, t, t); err != nil {
		w.Abort(osError("set mtime", err))
		return
	}
	w.BeginJSON()
	_ = w.JSON(protocol.MetadataResponse{OK: true})
}

// unixNanoTime converts a signed nanosecond epoch to time.Time.
func unixNanoTime(ns int64) time.Time {
	sec := ns / int64(time.Second)
	nsec := ns % int64(time.Second)
	if nsec < 0 {
		sec--
		nsec += int64(time.Second)
	}
	return time.Unix(sec, nsec)
}

// hashRPC streams SHA-256 results and ends with a count record.
func (s *server) hashRPC(ctx context.Context, req *protocol.HashRequest, w rpcWriter) {
	if len(req.Paths) > protocol.MaxPathsPerRequest {
		w.Abort(protocol.NewError(protocol.CodeTooLarge, "too many hash paths"))
		return
	}
	for _, p := range req.Paths {
		if err := protocol.ValidateAbsPath(p); err != nil {
			w.Abort(err)
			return
		}
	}
	enc := w.BeginStream()
	count, err := hashPaths(ctx, req.Paths, func(rec protocol.HashRecord) error {
		return enc.Encode(rec)
	})
	if err != nil {
		pe := asProtocolError(err)
		_ = enc.Encode(protocol.ErrorRecord{Type: protocol.RecordTypeError, Code: pe.Code, Message: pe.Message})
		w.Flush()
		return
	}
	_ = enc.Encode(protocol.EndRecord{Type: protocol.RecordTypeEnd, Count: count})
	w.Flush()
}

// statRPC returns Lstat metadata for one path.
func (s *server) statRPC(req *protocol.StatRequest, w rpcWriter) {
	if err := protocol.ValidateAbsPath(req.Path); err != nil {
		w.Abort(err)
		return
	}
	info, err := os.Lstat(req.Path)
	if err != nil {
		w.Abort(osError("lstat", err))
		return
	}
	kind := protocol.StatOther
	switch {
	case info.Mode().IsRegular():
		kind = protocol.StatFile
	case info.Mode()&os.ModeSymlink != 0:
		kind = protocol.StatSymlink
	}
	w.BeginJSON()
	_ = w.JSON(protocol.StatResponse{
		Type: kind, Size: info.Size(), MtimeNS: info.ModTime().UnixNano(), Perm: uint32(info.Mode().Perm()),
	})
}

// sameFileRPC reports whether both paths resolve to one inode.
func (s *server) sameFileRPC(req *protocol.SameFileRequest, w rpcWriter) {
	if err := protocol.ValidateAbsPath(req.A); err != nil {
		w.Abort(err)
		return
	}
	if err := protocol.ValidateAbsPath(req.B); err != nil {
		w.Abort(err)
		return
	}
	a, err := os.Stat(req.A)
	if err != nil {
		w.Abort(osError("stat first path", err))
		return
	}
	b, err := os.Stat(req.B)
	if err != nil {
		w.Abort(osError("stat second path", err))
		return
	}
	w.BeginJSON()
	_ = w.JSON(protocol.SameFileResponse{Same: os.SameFile(a, b)})
}

// quicRPCWriter writes one status-prefixed response on a CONTROL QUIC
// stream. Non-stream responses use a length-prefixed JSON object; stream
// responses write NDJSON and close the stream to delimit the response.
type quicRPCWriter struct {
	w       io.Writer
	stream  bool
	started bool
	failed  bool
	enc     *json.Encoder
}

func (q *quicRPCWriter) BeginJSON() {
	if q.started {
		return
	}
	q.started = true
	_, _ = q.w.Write([]byte{protocol.QUICControlStatusOK})
}

func (q *quicRPCWriter) JSON(v any) error {
	if !q.started {
		q.BeginJSON()
	}
	data, err := json.Marshal(v)
	if err != nil {
		q.failed = true
		return err
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(data)))
	if _, err := q.w.Write(hdr[:]); err != nil {
		q.failed = true
		return err
	}
	_, err = q.w.Write(data)
	return err
}

func (q *quicRPCWriter) BeginStream() *json.Encoder {
	if !q.started {
		q.started = true
		_, _ = q.w.Write([]byte{protocol.QUICControlStatusOK})
	}
	q.stream = true
	q.enc = json.NewEncoder(q.w)
	return q.enc
}

func (q *quicRPCWriter) Flush() {}

func (q *quicRPCWriter) Abort(err error) {
	if q.started || q.failed {
		return
	}
	q.failed = true
	pe := asProtocolError(err)
	data, _ := json.Marshal(protocol.ErrorResponse{Error: pe})
	_, _ = q.w.Write([]byte{protocol.QUICControlStatusError})
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(data)))
	_, _ = q.w.Write(hdr[:])
	_, _ = q.w.Write(data)
}

// dispatchControlRPC decodes and runs one RPC for a QUIC CONTROL stream.
func (s *server) dispatchControlRPC(ctx context.Context, path string, body []byte, w rpcWriter) {
	switch path {
	case "/v1/manifest":
		var req protocol.ManifestRequest
		if err := decodeBody(body, &req); err != nil {
			w.Abort(err)
			return
		}
		s.manifestRPC(ctx, &req, w)
	case "/v1/mkdir":
		var req protocol.MkdirRequest
		if err := decodeBody(body, &req); err != nil {
			w.Abort(err)
			return
		}
		s.mkdirRPC(&req, w)
	case "/v1/metadata":
		var req protocol.MetadataRequest
		if err := decodeBody(body, &req); err != nil {
			w.Abort(err)
			return
		}
		s.metadataRPC(&req, w)
	case "/v1/hash":
		var req protocol.HashRequest
		if err := decodeBody(body, &req); err != nil {
			w.Abort(err)
			return
		}
		s.hashRPC(ctx, &req, w)
	case "/v1/stat":
		var req protocol.StatRequest
		if err := decodeBody(body, &req); err != nil {
			w.Abort(err)
			return
		}
		s.statRPC(&req, w)
	case "/v1/samefile":
		var req protocol.SameFileRequest
		if err := decodeBody(body, &req); err != nil {
			w.Abort(err)
			return
		}
		s.sameFileRPC(&req, w)
	default:
		w.Abort(protocol.NewError(protocol.CodeInvalidArgument, "unknown control path %q", path))
	}
}
