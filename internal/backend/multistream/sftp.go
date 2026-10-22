package multistream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/pkg/sftp"

	"github.com/frankruan/catsync/internal/backend"
	"github.com/frankruan/catsync/internal/process"
	"github.com/frankruan/catsync/internal/target"
)

// SFTPConfig describes one system-SSH SFTP session: ssh is executed with
// an argument array (never a shell), using the user's own ssh
// configuration, credentials, agent, known_hosts, ProxyJump, and hardware
// keys. CatSync never parses or overrides any of those.
type SFTPConfig struct {
	// Program is the ssh executable; empty means "ssh" (PATH lookup). It
	// may be an absolute path containing spaces — it is passed to exec
	// verbatim, never through a shell. Only empty and NUL-containing values
	// are rejected.
	Program string
	// Dest is the validated remote destination; only [user@]host goes onto
	// the ssh command line — the remote path never does.
	Dest target.Destination
	// IPv6 adds -6.
	IPv6 bool
	// Env lists environment additions for the ssh process (see
	// process.Spec.Env); values are never printed or logged.
	Env []string
	// Stderr receives ssh's stderr (kept strictly separate from the SFTP
	// protocol stream on stdout); nil discards it.
	Stderr io.Writer
}

// Command builds the ssh argv: ssh [-6] -T -s [user@]host sftp. The remote
// path is never part of it: the SFTP subsystem is what the server speaks,
// not a shell command.
func (c SFTPConfig) Command() ([]string, error) {
	prog := c.Program
	if prog == "" {
		prog = "ssh"
	}
	if strings.ContainsRune(prog, 0) {
		return nil, errors.New("multistream: ssh program contains a NUL byte")
	}
	if err := backend.ValidateDestination(c.Dest); err != nil {
		return nil, err
	}
	who := c.Dest.Host
	if c.Dest.User != "" {
		who = c.Dest.User + "@" + c.Dest.Host
	}
	argv := []string{prog}
	if c.IPv6 {
		argv = append(argv, "-6")
	}
	return append(argv, "-T", "-s", who, "sftp"), nil
}

// idempotentWriteCloser makes Close idempotent. pkg/sftp's client closes its
// write end twice on teardown: once from the recv goroutine's defer and once
// from client.Close().
type idempotentWriteCloser struct {
	w    io.WriteCloser
	once sync.Once
	err  error
}

func (c *idempotentWriteCloser) Write(p []byte) (int, error) { return c.w.Write(p) }

func (c *idempotentWriteCloser) Close() error {
	c.once.Do(func() { c.err = c.w.Close() })
	return c.err
}

// WithClient starts one ssh process running the remote SFTP subsystem and
// invokes fn with a live SFTP client over its stdin/stdout. The client is
// callback-scoped: fn must not retain or use it after returning.
//
// Lifecycle guarantees:
//
//   - Invalid configuration, nil ctx/callback, and pre-cancelled contexts
//     fail before ssh is started.
//   - On callback error the callback's error is preserved and ssh is
//     cancelled and reaped.
//   - On context cancellation the SFTP pipes are closed (unblocking any
//     in-flight SFTP call), the process group is signalled and reaped, and
//     the returned error wraps ctx.Err().
//   - If ssh fails to start, dies before the handshake, or exits non-zero,
//     the handshake never blocks forever and the process error is
//     preserved.
//   - Every helper goroutine and the ssh process are gone before return.
func (c SFTPConfig) WithClient(ctx context.Context, fn func(*sftp.Client) error) error {
	if ctx == nil {
		return errors.New("multistream: nil context")
	}
	if fn == nil {
		return errors.New("multistream: nil callback")
	}
	argv, err := c.Command()
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("multistream: sftp session cancelled before starting: %w", err)
	}

	// Two OS pipe pairs join the SFTP client to ssh's stdin/stdout:
	// clientTx → ssh stdin, ssh stdout → clientRx. OS pipes (not io.Pipe)
	// are used so exec hands the FDs to the child directly: no copy
	// goroutines, and closing our ends is exact — a closed transport can
	// never surface as a fake process failure.
	//
	// Descriptor ownership:
	//   - clientTx_r and sshRx_w are the child's ends; the parent's copies
	//     are closed right after Start so EOF propagation is exact.
	//   - clientTx_w is owned by the sftp client; client.Close() closes it.
	//     Before a client exists (handshake failure or cancellation) we
	//     close it ourselves.
	//   - sshRx_r is owned by us and closed exactly once (guarded).
	clientTx_r, clientTx_w, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("multistream: sftp transport pipe: %w", err)
	}
	sshRx_r, sshRx_w, err := os.Pipe()
	if err != nil {
		clientTx_r.Close()
		clientTx_w.Close()
		return fmt.Errorf("multistream: sftp transport pipe: %w", err)
	}
	var rxOnce sync.Once
	closeClientRx := func() { rxOnce.Do(func() { _ = sshRx_r.Close() }) }

	h, err := process.StartInteractive(ctx, process.Spec{
		Name:   argv[0],
		Args:   argv[1:],
		Env:    c.Env,
		Stdin:  clientTx_r,
		Stdout: sshRx_w,
		Stderr: c.Stderr,
	})
	if err != nil {
		clientTx_r.Close()
		clientTx_w.Close()
		sshRx_r.Close()
		sshRx_w.Close()
		return err
	}
	// The child owns its copies of these ends now; closing ours means the
	// child sees EOF exactly when the client side closes.
	clientTx_r.Close()
	sshRx_w.Close()

	// The handshake runs in a goroutine so context cancellation and early
	// process death can interrupt it (NewClientPipe reads the server
	// version, which never arrives if ssh died or was cancelled).
	type hsResult struct {
		client *sftp.Client
		err    error
	}
	hsCh := make(chan hsResult, 1)
	go func() {
		client, err := sftp.NewClientPipe(sshRx_r, &idempotentWriteCloser{w: clientTx_w})
		hsCh <- hsResult{client, err}
	}()

	var client *sftp.Client
	var hsErr error
	select {
	case res := <-hsCh:
		client, hsErr = res.client, res.err
	case <-ctx.Done():
		// Close the client-side pipe ends so the handshake unblocks.
		clientTx_w.Close()
		closeClientRx()
		<-hsCh // returns a pipe error; the ctx error is what matters
	case <-h.Exited():
		// ssh died before the handshake (auth failure, bad host, ...).
		clientTx_w.Close()
		closeClientRx()
		res := <-hsCh // unblocked by the closed pipes
		if res.err != nil {
			hsErr = res.err
		}
	}

	if client == nil {
		closeClientRx()
		waitErr := h.Wait()
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("multistream: sftp session interrupted: %w", err)
		}
		if hsErr != nil && waitErr != nil {
			return fmt.Errorf("multistream: sftp handshake failed: %w; ssh: %v", hsErr, waitErr)
		}
		if hsErr != nil {
			return fmt.Errorf("multistream: sftp handshake failed: %w", hsErr)
		}
		if waitErr != nil {
			return fmt.Errorf("multistream: sftp transport: %w", waitErr)
		}
		return errors.New("multistream: sftp handshake produced no client")
	}

	// Session watcher: on cancellation, close the client's read end so
	// in-flight SFTP calls unblock. (A client blocked writing unblocks when
	// the process-group teardown kills ssh, breaking the pipe.)
	sessDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			closeClientRx()
		case <-sessDone:
		}
	}()

	cbErr := fn(client)
	close(sessDone)

	// Teardown order matters: the client's recv goroutine reads from the
	// protocol pipe, so closing the client's read end first unblocks it;
	// only then can client.Close() join its goroutines and close the write
	// end. The remote sftp server then sees stdin EOF and exits, and Wait
	// reaps ssh. A lingering ssh is still bounded by the caller's ctx via
	// the process-group teardown.
	closeClientRx()
	clientCloseErr := client.Close()
	waitErr := h.Wait()

	switch {
	case ctx.Err() != nil:
		// Cancellation dominates: a callback error produced by the
		// teardown is an effect, not the cause.
		return fmt.Errorf("multistream: sftp session interrupted: %w", ctx.Err())
	case cbErr != nil:
		// The callback's error is causal; transport teardown cannot mask it.
		return cbErr
	case clientCloseErr != nil:
		return fmt.Errorf("multistream: sftp client close: %w", clientCloseErr)
	default:
		return waitErr
	}
}
