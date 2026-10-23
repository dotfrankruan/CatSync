package multistream

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/pkg/sftp"
)

var (
	ErrReaderPoolClosed    = errors.New("multistream: reader pool is closed")
	ErrInvalidRemoteSource = errors.New("multistream: invalid remote source")
)

// RemoteSourceSpec binds a reader pool to one absolute regular file and its
// planned size.
type RemoteSourceSpec struct {
	Path string
	Size uint64
}

func (s RemoteSourceSpec) validate() error {
	if s.Path == "" || strings.ContainsRune(s.Path, 0) || !path.IsAbs(s.Path) || path.Clean(s.Path) != s.Path || s.Path == "/" {
		return fmt.Errorf("%w: path %q must be a clean absolute non-root POSIX path", ErrInvalidRemoteSource, s.Path)
	}
	if s.Size > math.MaxInt64 {
		return fmt.Errorf("%w: size %d exceeds int64", ErrInvalidRemoteSource, s.Size)
	}
	return nil
}

type poolReader struct {
	file    *sftp.File
	session int
	lent    bool
}

// RemoteReaderPool is a concurrency-safe io.ReaderAt backed by one
// independently opened SFTP handle per system-SSH session.
type RemoteReaderPool struct {
	readers     chan poolReader
	all         []poolReader
	done        chan struct{}
	outstanding sync.WaitGroup
	mu          sync.Mutex
	closed      bool
}

// OpenRemoteReaderPool validates the source through every opened handle.
// A partial open failure closes all earlier handles before returning.
func OpenRemoteReaderPool(spec RemoteSourceSpec, clients []*sftp.Client) (*RemoteReaderPool, error) {
	if err := spec.validate(); err != nil {
		return nil, err
	}
	if len(clients) == 0 {
		return nil, fmt.Errorf("%w: no sessions", ErrInvalidRemoteSource)
	}
	p := &RemoteReaderPool{readers: make(chan poolReader, len(clients)), done: make(chan struct{})}
	for i, client := range clients {
		if client == nil {
			p.closeAllQuietly()
			return nil, fmt.Errorf("%w: nil sftp client at session %d", ErrInvalidRemoteSource, i)
		}
		fi, err := client.Lstat(spec.Path)
		if err != nil || !fi.Mode().IsRegular() || fi.Mode()&os.ModeSymlink != 0 || fi.Size() != int64(spec.Size) {
			p.closeAllQuietly()
			if err == nil {
				err = fmt.Errorf("source is not the expected regular file of %d bytes", spec.Size)
			}
			return nil, fmt.Errorf("multistream: session %d: lstat remote source: %w", i, err)
		}
		f, err := client.Open(spec.Path)
		if err != nil {
			p.closeAllQuietly()
			return nil, fmt.Errorf("multistream: session %d: open remote source: %w", i, err)
		}
		opened, err := f.Stat()
		if err != nil || !opened.Mode().IsRegular() || opened.Size() != int64(spec.Size) {
			_ = f.Close()
			p.closeAllQuietly()
			if err == nil {
				err = fmt.Errorf("opened source is not the expected regular file of %d bytes", spec.Size)
			}
			return nil, fmt.Errorf("multistream: session %d: stat opened remote source: %w", i, err)
		}
		r := poolReader{file: f, session: i}
		p.all = append(p.all, r)
		p.readers <- r
	}
	return p, nil
}

func (p *RemoteReaderPool) closeAllQuietly() {
	for _, r := range p.all {
		_ = r.file.Close()
	}
}

func (p *RemoteReaderPool) ReadAt(buf []byte, off int64) (int, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return 0, ErrReaderPoolClosed
	}
	p.outstanding.Add(1)
	p.mu.Unlock()
	defer p.outstanding.Done()
	select {
	case r := <-p.readers:
		p.mu.Lock()
		r.lent = true
		p.all[r.session].lent = true
		p.mu.Unlock()
		n, err := r.file.ReadAt(buf, off)
		p.readers <- r
		return n, err
	case <-p.done:
		return 0, ErrReaderPoolClosed
	}
}

func (p *RemoteReaderPool) CloseAll() error {
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
	for _, r := range p.all {
		if err := r.file.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (p *RemoteReaderPool) UsedSessions() []int {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []int
	for _, r := range p.all {
		if r.lent {
			out = append(out, r.session)
		}
	}
	return out
}
