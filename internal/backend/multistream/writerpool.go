package multistream

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/pkg/sftp"
)

// Sentinel errors of the writer pool; test with errors.Is.
var (
	// ErrWriterPoolClosed rejects writes after CloseAll or teardown.
	ErrWriterPoolClosed = errors.New("multistream: writer pool is closed")
	// ErrZeroProgressWrite reports a handle that accepted no bytes without
	// an error, which would otherwise spin forever.
	ErrZeroProgressWrite = errors.New("multistream: zero-progress write")
)

// poolWriter is one O_WRONLY handle to the shared remote temporary object,
// opened through one SFTP session.
type poolWriter struct {
	file    remoteFile
	session int
	lent    bool // set when this writer was borrowed at least once
}

// RemoteWriterPool is a concurrency-safe io.WriterAt fanning range writes
// over one SFTP handle per session, all to the same remote temporary
// object. Writes borrow a handle from a bounded channel — never
// concurrently re-entered, never busy-waiting. Memory stays
// O(streams × fixed buffer): no per-file allocations.
type RemoteWriterPool struct {
	writers     chan poolWriter
	all         []poolWriter
	done        chan struct{}
	outstanding sync.WaitGroup
	mu          sync.Mutex
	closed      bool
}

// OpenRemoteWriterPool opens one independent O_WRONLY handle per client to
// the same TempPath. No handle is created with O_CREATE, O_TRUNC, or
// O_APPEND. Each handle is re-Stat'ed and must be a regular file of exactly
// spec.Size. If opening handle N fails, handles 0..N-1 are closed first.
func OpenRemoteWriterPool(spec RemoteTempSpec, clients []*sftp.Client) (*RemoteWriterPool, error) {
	ops := make([]remoteOps, len(clients))
	for i, c := range clients {
		if c == nil {
			return nil, fmt.Errorf("%w: nil sftp client at session %d", ErrInvalidRemoteTemp, i)
		}
		ops[i] = clientOps{c}
	}
	return openRemoteWriterPool(spec, ops)
}

func openRemoteWriterPool(spec RemoteTempSpec, ops []remoteOps) (*RemoteWriterPool, error) {
	if err := spec.validate(); err != nil {
		return nil, err
	}
	if len(ops) == 0 {
		return nil, fmt.Errorf("%w: no sessions", ErrInvalidRemoteTemp)
	}
	// Resume validation against the first session: the final path must not
	// exist yet, and the temp must already be a regular file of the planned
	// size. Each handle is re-checked after opening below.
	if err := checkResumeTarget(ops[0], spec); err != nil {
		return nil, err
	}
	p := &RemoteWriterPool{
		writers: make(chan poolWriter, len(ops)),
		done:    make(chan struct{}),
	}
	for i, o := range ops {
		f, err := o.OpenFile(spec.TempPath, os.O_WRONLY)
		if err != nil {
			p.closeAllQuietly()
			return nil, fmt.Errorf("multistream: session %d: open temp: %w", i, err)
		}
		fi, err := f.Stat()
		if err != nil || !fi.Mode().IsRegular() || fi.Mode()&os.ModeSymlink != 0 || fi.Size() != int64(spec.Size) {
			_ = f.Close()
			p.closeAllQuietly()
			if err == nil {
				err = fmt.Errorf("%w: session %d: temp is not the expected regular file of %d bytes", ErrRemoteTempMismatch, i, spec.Size)
			}
			return nil, fmt.Errorf("multistream: session %d: stat temp: %w", i, err)
		}
		w := poolWriter{file: f, session: i}
		p.all = append(p.all, w)
		p.writers <- w
	}
	return p, nil
}

// closeAllQuietly closes every opened handle, used when opening fails
// partway.
func (p *RemoteWriterPool) closeAllQuietly() {
	for _, w := range p.all {
		_ = w.file.Close()
	}
}

// WriteAt writes b fully at off, borrowing one handle for the duration.
// Short writes are retried; zero progress without error and real write
// errors are returned. After CloseAll (or teardown) it fails with
// ErrWriterPoolClosed.
func (p *RemoteWriterPool) WriteAt(b []byte, off int64) (int, error) {
	// Add under the mutex so CloseAll's Wait can never observe a zero
	// counter while a write is still being admitted.
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return 0, ErrWriterPoolClosed
	}
	p.outstanding.Add(1)
	p.mu.Unlock()
	defer p.outstanding.Done()
	select {
	case w := <-p.writers:
		p.mu.Lock()
		w.lent = true
		p.all[w.session].lent = true
		p.mu.Unlock()
		n, err := writeFully(w.file, b, off)
		p.writers <- w // always room: the channel capacity equals the handle count
		return n, err
	case <-p.done:
		return 0, ErrWriterPoolClosed
	}
}

// writeFully writes b completely via w, tolerating short writes.
func writeFully(w io.WriterAt, b []byte, off int64) (int, error) {
	written := 0
	for written < len(b) {
		n, err := w.WriteAt(b[written:], off+int64(written))
		if n < 0 {
			return written, fmt.Errorf("multistream: negative write count %d at %d", n, off+int64(written))
		}
		written += n
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, fmt.Errorf("%w: at %d", ErrZeroProgressWrite, off+int64(written))
		}
	}
	return written, nil
}

// SyncAll syncs every handle after waiting for in-flight writes to return.
// The first data error wins; teardown never masks it.
func (p *RemoteWriterPool) SyncAll() error {
	p.outstanding.Wait()
	var first error
	for _, w := range p.all {
		if err := w.file.Sync(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// CloseAll stops lending, waits for every in-flight write to finish, and
// closes every handle. The first error wins.
func (p *RemoteWriterPool) CloseAll() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	close(p.done)
	p.mu.Unlock()
	p.outstanding.Wait()
	var first error
	for _, w := range p.all {
		if err := w.file.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// UsedSessions returns the sorted indexes of sessions that were lent a
// writer at least once — proof that range writes spread over multiple SFTP
// sessions.
func (p *RemoteWriterPool) UsedSessions() []int {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []int
	for _, w := range p.all {
		if w.lent {
			out = append(out, w.session)
		}
	}
	return out
}
