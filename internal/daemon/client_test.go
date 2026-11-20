package daemon

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dotfrankruan/CatSync/internal/daemon/protocol"
)

// newStreamClient returns a Client pointed at an httptest RPC stub.
func newStreamClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	host := strings.TrimPrefix(srv.URL, "http://")
	return &Client{dialAddr: host, http: srv.Client()}
}

func TestHashRejectsTruncatedStream(t *testing.T) {
	c := newStreamClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte(`{"type":"hash","path":"/a","sha256":"` + strings.Repeat("0", 64) + `"}` + "\n"))
		// No end record: a truncated response must fail, not look like one
		// successfully hashed path.
	})
	_, err := c.Hash(context.Background(), []string{"/a", "/b"})
	if err == nil || !strings.Contains(err.Error(), protocol.CodeHashIncomplete) {
		t.Fatalf("Hash = %v, want %s", err, protocol.CodeHashIncomplete)
	}
}

func TestHashRejectsCountMismatch(t *testing.T) {
	c := newStreamClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"type":"hash","path":"/a","sha256":"` + strings.Repeat("0", 64) + `"}` + "\n" +
			`{"type":"end","count":1}` + "\n"))
	})
	_, err := c.Hash(context.Background(), []string{"/a", "/b"})
	if err == nil || !strings.Contains(err.Error(), protocol.CodeHashIncomplete) {
		t.Fatalf("Hash = %v, want %s", err, protocol.CodeHashIncomplete)
	}
}

func TestHashRejectsDuplicateAndUnsorted(t *testing.T) {
	c := newStreamClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"type":"hash","path":"/b","sha256":"` + strings.Repeat("0", 64) + `"}` + "\n" +
			`{"type":"hash","path":"/a","sha256":"` + strings.Repeat("0", 64) + `"}` + "\n" +
			`{"type":"end","count":2}` + "\n"))
	})
	_, err := c.Hash(context.Background(), []string{"/a", "/b"})
	if err == nil || !strings.Contains(err.Error(), protocol.CodeHashIncomplete) {
		t.Fatalf("Hash = %v, want %s", err, protocol.CodeHashIncomplete)
	}
}

func TestManifestRejectsMissingEndAndRecordsAfterEnd(t *testing.T) {
	for name, body := range map[string]string{
		"missing end":  `{"type":"mode","mode":"source_tree"}` + "\n" + `{"type":"entry","path":"a","kind":"file","size":1,"mtime_ns":1,"mode":420}` + "\n",
		"after end":    `{"type":"mode","mode":"source_tree"}` + "\n" + `{"type":"end","count":0}` + "\n" + `{"type":"entry","path":"a","kind":"file","size":1,"mtime_ns":1,"mode":420}` + "\n",
		"count differ": `{"type":"mode","mode":"source_tree"}` + "\n" + `{"type":"entry","path":"a","kind":"file","size":1,"mtime_ns":1,"mode":420}` + "\n" + `{"type":"end","count":2}` + "\n",
	} {
		t.Run(name, func(t *testing.T) {
			c := newStreamClient(t, func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(body))
			})
			if _, err := c.Manifest(context.Background(), protocol.ModeSourceTree, "/nonexistent"); err == nil ||
				!strings.Contains(err.Error(), protocol.CodeManifestIncomplete) {
				t.Fatalf("Manifest = %v, want %s", err, protocol.CodeManifestIncomplete)
			}
		})
	}
}
