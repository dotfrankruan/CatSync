//go:build unix

package multistream

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/pkg/sftp"
)

func TestRemoteReaderPoolRealProtocol(t *testing.T) {
	if !realServerAvailable() {
		t.Skip("OpenSSH sftp-server unavailable")
	}
	root := t.TempDir()
	remote := filepath.Join(root, "source.bin")
	want := patternBytes(96 << 10)
	if err := os.WriteFile(remote, want, 0o640); err != nil {
		t.Fatal(err)
	}
	cfg := helperConfig(t, t.TempDir(), envRealServer+"=1")
	err := cfg.WithClients(context.Background(), 3, func(clients []*sftp.Client) error {
		pool, err := OpenRemoteReaderPool(RemoteSourceSpec{Path: remote, Size: uint64(len(want))}, clients)
		if err != nil {
			return err
		}
		for i := 0; i < 3; i++ {
			buf := make([]byte, 4096)
			off := int64(i * 4096)
			n, err := pool.ReadAt(buf, off)
			if err != nil || n != len(buf) || !bytes.Equal(buf, want[off:off+int64(len(buf))]) {
				t.Fatalf("ReadAt(%d) = %d, %v, data match %v", off, n, err, bytes.Equal(buf[:n], want[off:off+int64(n)]))
			}
		}
		if got := pool.UsedSessions(); !reflect.DeepEqual(got, []int{0, 1, 2}) {
			t.Fatalf("used sessions = %v, want all three", got)
		}
		if err := pool.CloseAll(); err != nil {
			return err
		}
		if _, err := pool.ReadAt(make([]byte, 1), 0); !errors.Is(err, ErrReaderPoolClosed) {
			t.Fatalf("read after close = %v", err)
		}
		return pool.CloseAll()
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRemoteReaderPoolRejectsMismatchAndSymlink(t *testing.T) {
	if !realServerAvailable() {
		t.Skip("OpenSSH sftp-server unavailable")
	}
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.WriteFile(source, []byte("payload"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(source, link); err != nil {
		t.Fatal(err)
	}
	cfg := helperConfig(t, t.TempDir(), envRealServer+"=1")
	err := cfg.WithClient(context.Background(), func(client *sftp.Client) error {
		if _, err := OpenRemoteReaderPool(RemoteSourceSpec{Path: source, Size: 99}, []*sftp.Client{client}); err == nil {
			t.Fatal("size mismatch accepted")
		}
		if _, err := OpenRemoteReaderPool(RemoteSourceSpec{Path: link, Size: 7}, []*sftp.Client{client}); err == nil {
			t.Fatal("symlink accepted")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRemoteReaderPoolValidationBeforeProtocol(t *testing.T) {
	for _, spec := range []RemoteSourceSpec{{}, {Path: "relative", Size: 1}, {Path: "/", Size: 1}, {Path: "/bad\x00path", Size: 1}} {
		if _, err := OpenRemoteReaderPool(spec, nil); !errors.Is(err, ErrInvalidRemoteSource) {
			t.Fatalf("spec %+v error = %v", spec, err)
		}
	}
}
