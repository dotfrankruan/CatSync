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

func TestSSHTransportForwardCommand(t *testing.T) {
	tr := SSHTransport{Host: "h", User: "u", Listen: "127.0.0.1:43411", ForwardLocalPort: 49001}
	argv, err := tr.command()
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(argv, " ")
	if !strings.Contains(joined, "-o ExitOnForwardFailure=yes") {
		t.Errorf("argv lacks ExitOnForwardFailure: %v", argv)
	}
	if !strings.Contains(joined, "-L 127.0.0.1:49001:127.0.0.1:43411") {
		t.Errorf("argv lacks forwarding spec: %v", argv)
	}
	last := argv[len(argv)-1]
	if !strings.Contains(last, "127.0.0.1:43411") || !strings.Contains(last, "serve --once --listen") {
		t.Errorf("remote command is not loopback-bound: %v", argv)
	}
	direct := SSHTransport{Host: "h", Listen: "127.0.0.1:43411"}
	dargv, err := direct.command()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.Join(dargv, " "), "-L ") {
		t.Errorf("direct transport forwarded unexpectedly: %v", dargv)
	}
	bad := []SSHTransport{
		{Host: "h", Listen: "0.0.0.0:43411", ForwardLocalPort: 49001},
		{Host: "h", Listen: "127.0.0.1:0", ForwardLocalPort: 49001},
		{Host: "h", Listen: "127.0.0.1:43411", ForwardLocalPort: 70000},
	}
	for _, b := range bad {
		if _, err := b.command(); err == nil {
			t.Errorf("accepted invalid forward transport %+v", b)
		}
	}
}
