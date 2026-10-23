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

	"github.com/dotfrankruan/CatSync/internal/backend"
	"github.com/dotfrankruan/CatSync/internal/process"
	"github.com/dotfrankruan/CatSync/internal/target"
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

// sftpSession is one live system-SSH SFTP session: a client plus the ssh
// process underneath it.
type sftpSession struct {
	client  *sftp.Client
	handle  *process.Handle
	closeRx func() // closes the client's read end, exactly once
}

// close tears the session down: the client's read end closes first so its
// recv goroutine unblocks, then client.Close() joins its goroutines and
// closes the write end, the remote sftp server sees stdin EOF and exits,
// and Wait reaps ssh. A lingering ssh is bounded by the caller's context
// through the process-group teardown.
func (s *sftpSession) close() error {
	s.closeRx()
	clientErr := s.client.Close()
	waitErr := s.handle.Wait()
	if clientErr != nil {
		return clientErr
	}
	return waitErr
}

// startSession starts one ssh process and completes the SFTP handshake. On
// any failure the process is torn down and no pipe leaks. The handshake
// runs in a goroutine so cancellation and early process death can
// interrupt it.
func (c SFTPConfig) startSession(ctx context.Context, argv []string) (*sftpSession, error) {
	clientTx_r, clientTx_w, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("multistream: sftp transport pipe: %w", err)
	}
	sshRx_r, sshRx_w, err := os.Pipe()
	if err != nil {
		clientTx_r.Close()
		clientTx_w.Close()
		return nil, fmt.Errorf("multistream: sftp transport pipe: %w", err)
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
		return nil, err
	}
	// The child owns its copies of these ends now; closing ours means the
	// child sees EOF exactly when the client side closes.
	clientTx_r.Close()
	sshRx_w.Close()

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
		clientTx_w.Close()
		closeClientRx()
		<-hsCh
	case <-h.Exited():
		// ssh died before the handshake (auth failure, bad host, ...).
		clientTx_w.Close()
		closeClientRx()
		res := <-hsCh
		if res.err != nil {
			hsErr = res.err
		}
	}

	if client == nil {
		closeClientRx()
		waitErr := h.Wait()
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("multistream: sftp session interrupted: %w", err)
		}
		if hsErr != nil && waitErr != nil {
			return nil, fmt.Errorf("multistream: sftp handshake failed: %w; ssh: %v", hsErr, waitErr)
		}
		if hsErr != nil {
			return nil, fmt.Errorf("multistream: sftp handshake failed: %w", hsErr)
		}
		if waitErr != nil {
			return nil, fmt.Errorf("multistream: sftp transport: %w", waitErr)
		}
		return nil, errors.New("multistream: sftp handshake produced no client")
	}
	return &sftpSession{client: client, handle: h, closeRx: closeClientRx}, nil
}

// MaxSFTPStreams caps one pool: the number of concurrent SFTP sessions over
// one destination. It keeps a configuration mistake from opening hundreds
// of ssh processes.
const MaxSFTPStreams = 64

// WithClients establishes streams SFTP sessions, one after another — never
// concurrently, so interactive password/PIN/host-key prompts never pile up
// — and invokes fn with all of them once every handshake has succeeded. If
// session N fails, the previous N-1 sessions are closed and reaped first.
// Clients are callback-scoped: fn must not retain or use them after
// returning, and every ssh process is reaped before return.
//
// Error precedence: context cancellation, then the callback's error, then
// the session/teardown failure that is the real cause.
func (c SFTPConfig) WithClients(ctx context.Context, streams int, fn func([]*sftp.Client) error) error {
	if ctx == nil {
		return errors.New("multistream: nil context")
	}
	if fn == nil {
		return errors.New("multistream: nil callback")
	}
	if streams < 1 || streams > MaxSFTPStreams {
		return fmt.Errorf("multistream: streams must be between 1 and %d, got %d", MaxSFTPStreams, streams)
	}
	argv, err := c.Command()
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("multistream: sftp sessions cancelled before starting: %w", err)
	}

	var sessions []*sftpSession
	for i := 0; i < streams; i++ {
		s, err := c.startSession(ctx, argv)
		if err != nil {
			var teardownErr error
			for _, prev := range sessions {
				if cerr := prev.close(); cerr != nil && teardownErr == nil {
					teardownErr = cerr
				}
			}
			if ctx.Err() != nil {
				return fmt.Errorf("multistream: sftp session %d interrupted: %w", i, ctx.Err())
			}
			if teardownErr != nil {
				return errors.Join(fmt.Errorf("multistream: sftp session %d: %w", i, err),
					fmt.Errorf("multistream: teardown of earlier sessions: %w", teardownErr))
			}
			return fmt.Errorf("multistream: sftp session %d: %w", i, err)
		}
		sessions = append(sessions, s)
	}

	clients := make([]*sftp.Client, len(sessions))
	for i, s := range sessions {
		clients[i] = s.client
	}

	// Pool watcher: on cancellation, close every client's read end so
	// in-flight SFTP calls unblock.
	sessDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			for _, s := range sessions {
				s.closeRx()
			}
		case <-sessDone:
		}
	}()

	cbErr := fn(clients)
	close(sessDone)

	var teardownErr error
	for _, s := range sessions {
		if err := s.close(); err != nil && teardownErr == nil {
			teardownErr = err
		}
	}
	switch {
	case ctx.Err() != nil:
		return fmt.Errorf("multistream: sftp sessions interrupted: %w", ctx.Err())
	case cbErr != nil:
		return cbErr
	default:
		return teardownErr
	}
}

// WithClient starts one ssh process running the remote SFTP subsystem and
// invokes fn with a live SFTP client over its stdin/stdout. The client is
// callback-scoped: fn must not retain or use it after returning. It is the
// single-session form of WithClients. A nil callback is rejected before
// any process starts, because the wrapper would otherwise defeat the check.
func (c SFTPConfig) WithClient(ctx context.Context, fn func(*sftp.Client) error) error {
	if fn == nil {
		return errors.New("multistream: nil callback")
	}
	return c.WithClients(ctx, 1, func(clients []*sftp.Client) error {
		return fn(clients[0])
	})
}
