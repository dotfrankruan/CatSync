package daemon

import (
	"context"
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

// httpHandler builds the mTLS RPC surface. The counter middleware tracks
// in-flight RPC handlers; it never wraps data connections.
func (s *server) httpHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/manifest", s.handleManifest)
	mux.HandleFunc("/v1/mkdir", s.handleMkdir)
	mux.HandleFunc("/v1/metadata", s.handleMetadata)
	mux.HandleFunc("/v1/hash", s.handleHash)
	mux.HandleFunc("/v1/stat", s.handleStat)
	mux.HandleFunc("/v1/samefile", s.handleSameFile)
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

// startNDJSON begins a streaming response.
func startNDJSON(w http.ResponseWriter) *json.Encoder {
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)
	return json.NewEncoder(w)
}

// flush flushes a streaming response when supported.
func flush(w http.ResponseWriter) {
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

// handleManifest implements the three distinct manifest modes.
func (s *server) handleManifest(w http.ResponseWriter, r *http.Request) {
	var req protocol.ManifestRequest
	if err := decodeRequest(w, r, &req); err != nil {
		writeError(w, err)
		return
	}
	if !req.Mode.Valid() {
		writeError(w, protocol.NewError(protocol.CodeInvalidArgument, "unknown manifest mode %q", req.Mode))
		return
	}
	if err := protocol.ValidateAbsPath(req.Root); err != nil {
		writeError(w, err)
		return
	}
	if err := precheckManifest(req.Mode, req.Root); err != nil {
		writeError(w, err)
		return
	}
	enc := startNDJSON(w)
	if err := enc.Encode(protocol.ManifestModeRecord{Type: protocol.RecordTypeMode, Mode: req.Mode}); err != nil {
		return
	}
	flush(w)
	count, err := walkTree(r.Context(), req.Mode, req.Root, s.manifestCfg(), func(rec protocol.ManifestEntryRecord) error {
		return enc.Encode(rec)
	})
	if err != nil {
		pe := asProtocolError(err)
		_ = enc.Encode(protocol.ErrorRecord{Type: protocol.RecordTypeError, Code: pe.Code, Message: pe.Message})
		flush(w)
		return
	}
	_ = enc.Encode(protocol.EndRecord{Type: protocol.RecordTypeEnd, Count: count})
	flush(w)
}

// handleMkdir ensures every requested directory exists.
func (s *server) handleMkdir(w http.ResponseWriter, r *http.Request) {
	var req protocol.MkdirRequest
	if err := decodeRequest(w, r, &req); err != nil {
		writeError(w, err)
		return
	}
	if len(req.Dirs) > protocol.MaxDirsPerRequest {
		writeError(w, protocol.NewError(protocol.CodeTooLarge, "too many directories"))
		return
	}
	dirs := append([]string(nil), req.Dirs...)
	sort.Strings(dirs)
	for _, d := range dirs {
		if err := protocol.ValidateAbsPath(d); err != nil {
			writeError(w, err)
			return
		}
		if err := os.MkdirAll(d, 0o755); err != nil {
			writeError(w, osError("create directory", err))
			return
		}
	}
	writeJSON(w, http.StatusOK, protocol.MkdirResponse{Created: len(dirs)})
}

// handleMetadata applies permission bits and exact nanosecond mtime to one
// regular file.
func (s *server) handleMetadata(w http.ResponseWriter, r *http.Request) {
	var req protocol.MetadataRequest
	if err := decodeRequest(w, r, &req); err != nil {
		writeError(w, err)
		return
	}
	if err := protocol.ValidateAbsPath(req.Path); err != nil {
		writeError(w, err)
		return
	}
	info, err := os.Lstat(req.Path)
	if err != nil {
		writeError(w, osError("lstat metadata target", err))
		return
	}
	if !info.Mode().IsRegular() {
		writeError(w, protocol.NewError(protocol.CodeInvalidArgument, "metadata target %q is not a regular file", req.Path))
		return
	}
	if req.Mode != nil {
		if err := os.Chmod(req.Path, os.FileMode(*req.Mode&0o7777)); err != nil {
			writeError(w, osError("chmod metadata target", err))
			return
		}
	}
	t := unixNanoTime(req.MtimeNS)
	if err := os.Chtimes(req.Path, t, t); err != nil {
		writeError(w, osError("set mtime", err))
		return
	}
	writeJSON(w, http.StatusOK, protocol.MetadataResponse{OK: true})
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

// handleHash streams SHA-256 results and ends with a count record.
func (s *server) handleHash(w http.ResponseWriter, r *http.Request) {
	var req protocol.HashRequest
	if err := decodeRequest(w, r, &req); err != nil {
		writeError(w, err)
		return
	}
	if len(req.Paths) > protocol.MaxPathsPerRequest {
		writeError(w, protocol.NewError(protocol.CodeTooLarge, "too many hash paths"))
		return
	}
	for _, p := range req.Paths {
		if err := protocol.ValidateAbsPath(p); err != nil {
			writeError(w, err)
			return
		}
	}
	enc := startNDJSON(w)
	count, err := hashPaths(r.Context(), req.Paths, func(rec protocol.HashRecord) error {
		return enc.Encode(rec)
	})
	if err != nil {
		pe := asProtocolError(err)
		_ = enc.Encode(protocol.ErrorRecord{Type: protocol.RecordTypeError, Code: pe.Code, Message: pe.Message})
		flush(w)
		return
	}
	_ = enc.Encode(protocol.EndRecord{Type: protocol.RecordTypeEnd, Count: count})
	flush(w)
}

// handleStat returns Lstat metadata for one path.
func (s *server) handleStat(w http.ResponseWriter, r *http.Request) {
	var req protocol.StatRequest
	if err := decodeRequest(w, r, &req); err != nil {
		writeError(w, err)
		return
	}
	if err := protocol.ValidateAbsPath(req.Path); err != nil {
		writeError(w, err)
		return
	}
	info, err := os.Lstat(req.Path)
	if err != nil {
		writeError(w, osError("lstat", err))
		return
	}
	kind := protocol.StatOther
	switch {
	case info.Mode().IsRegular():
		kind = protocol.StatFile
	case info.Mode()&os.ModeSymlink != 0:
		kind = protocol.StatSymlink
	}
	writeJSON(w, http.StatusOK, protocol.StatResponse{
		Type: kind, Size: info.Size(), MtimeNS: info.ModTime().UnixNano(), Perm: uint32(info.Mode().Perm()),
	})
}

// handleSameFile reports whether both paths resolve to one inode.
func (s *server) handleSameFile(w http.ResponseWriter, r *http.Request) {
	var req protocol.SameFileRequest
	if err := decodeRequest(w, r, &req); err != nil {
		writeError(w, err)
		return
	}
	if err := protocol.ValidateAbsPath(req.A); err != nil {
		writeError(w, err)
		return
	}
	if err := protocol.ValidateAbsPath(req.B); err != nil {
		writeError(w, err)
		return
	}
	a, err := os.Stat(req.A)
	if err != nil {
		writeError(w, osError("stat first path", err))
		return
	}
	b, err := os.Stat(req.B)
	if err != nil {
		writeError(w, osError("stat second path", err))
		return
	}
	writeJSON(w, http.StatusOK, protocol.SameFileResponse{Same: os.SameFile(a, b)})
}
