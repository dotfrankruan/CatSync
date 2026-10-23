//go:build unix

package multistream

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/pkg/sftp"
)

// poolPids returns all helper pids recorded for this pool.
func poolPids(t *testing.T, dir string) []int {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "pids"))
	if err != nil {
		return nil
	}
	var pids []int
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		var pid int
		if _, err := fmt.Sscanf(line, "%d", &pid); err != nil {
			t.Fatalf("parse pid %q: %v", line, err)
		}
		pids = append(pids, pid)
	}
	return pids
}

func assertAllDead(t *testing.T, dir string, min int) {
	t.Helper()
	pids := poolPids(t, dir)
	if len(pids) < min {
		t.Fatalf("recorded %d helper pids, want at least %d", len(pids), min)
	}
	for _, pid := range pids {
		if err := syscall.Kill(pid, 0); err == nil {
			t.Errorf("helper pid %d still alive after return", pid)
		}
	}
}

func TestSFTPPoolSingleSession(t *testing.T) {
	dir := t.TempDir()
	cfg := helperConfig(t, dir)
	err := cfg.WithClients(context.Background(), 1, func(clients []*sftp.Client) error {
		if len(clients) != 1 {
			return fmt.Errorf("clients = %d, want 1", len(clients))
		}
		_, err := clients[0].Stat("/")
		return err
	})
	if err != nil {
		t.Fatalf("WithClients: %v", err)
	}
	assertAllDead(t, dir, 1)
}

func TestSFTPPoolMultipleSessions(t *testing.T) {
	dir := t.TempDir()
	cfg := helperConfig(t, dir)
	err := cfg.WithClients(context.Background(), 4, func(clients []*sftp.Client) error {
		if len(clients) != 4 {
			return fmt.Errorf("clients = %d, want 4", len(clients))
		}
		// Each session answers independently.
		for i, c := range clients {
			if _, err := c.Stat("/"); err != nil {
				return fmt.Errorf("session %d: %w", i, err)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithClients: %v", err)
	}
	pids := poolPids(t, dir)
	if len(pids) != 4 {
		t.Fatalf("recorded %d helper pids, want 4", len(pids))
	}
	seen := map[int]bool{}
	for _, pid := range pids {
		if seen[pid] {
			t.Errorf("pid %d reused: sessions must be distinct processes", pid)
		}
		seen[pid] = true
	}
	assertAllDead(t, dir, 4)
}

func TestSFTPPoolRejectsBadStreamCount(t *testing.T) {
	dir := t.TempDir()
	cfg := helperConfig(t, dir)
	for _, n := range []int{0, -1, 65} {
		err := cfg.WithClients(context.Background(), n, func([]*sftp.Client) error { return nil })
		if err == nil {
			t.Errorf("streams=%d accepted, want error", n)
		}
	}
	// No process may have started.
	assertAllDead(t, dir, 0)
	if _, err := os.Stat(filepath.Join(dir, "pids")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a process started despite invalid stream count: %v", err)
	}
}

func TestSFTPPoolNthSessionFailsAndRecycles(t *testing.T) {
	dir := t.TempDir()
	cfg := helperConfig(t, dir, envFailNth+"=3")
	boom := errors.New("callback must not run")
	err := cfg.WithClients(context.Background(), 4, func(clients []*sftp.Client) error {
		return boom
	})
	if err == nil {
		t.Fatal("WithClients with a failing third session = nil, want error")
	}
	if errors.Is(err, boom) {
		t.Error("callback ran despite the failed session")
	}
	if !strings.Contains(err.Error(), "session 2") {
		t.Errorf("error = %v, want it to name the failing session", err)
	}
	// Sessions 0 and 1 were established and must be torn down.
	assertAllDead(t, dir, 2)
	pids := poolPids(t, dir)
	if len(pids) != 3 {
		t.Errorf("recorded %d helper pids, want exactly 3 (two good, one failing)", len(pids))
	}
}

func TestSFTPPoolPreCancelled(t *testing.T) {
	dir := t.TempDir()
	cfg := helperConfig(t, dir)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := cfg.WithClients(ctx, 3, func(clients []*sftp.Client) error {
		t.Error("callback must not run on a pre-cancelled context")
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want wrapped context.Canceled", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "pids")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a helper process started despite the pre-cancelled context: %v", err)
	}
}

func TestSFTPPoolMidCancelReapsAll(t *testing.T) {
	dir := t.TempDir()
	cfg := helperConfig(t, dir)
	ctx, cancel := context.WithCancel(context.Background())
	proceed := make(chan struct{})
	cbCalled := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- cfg.WithClients(ctx, 4, func(clients []*sftp.Client) error {
			close(cbCalled)
			<-proceed
			return errors.New("callback sentinel")
		})
	}()
	<-cbCalled // all four sessions are live
	cancel()
	close(proceed)
	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want wrapped context.Canceled", err)
	}
	assertAllDead(t, dir, 4)
}
