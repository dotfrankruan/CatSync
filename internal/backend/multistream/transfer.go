package multistream

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/pkg/sftp"

	"github.com/frankruan/catsync/internal/discovery"
	"github.com/frankruan/catsync/internal/target"
)

// Sentinel errors of the transfer orchestration; test with errors.Is.
var (
	// ErrSourceChanged reports a source whose size or nanosecond mtime
	// changed during the transfer. The temp and checkpoint are preserved
	// for diagnosis; the transfer must not be resumed against the changed
	// source.
	ErrSourceChanged = errors.New("multistream: source file changed during transfer")
	// ErrCheckpointConflict rejects a fresh Transfer when a checkpoint
	// already exists for this file: the caller must Resume instead.
	ErrCheckpointConflict = errors.New("multistream: checkpoint already exists; use Resume")
	// ErrDestinationMismatch rejects a Resume against a destination that
	// does not match the checkpoint's bound identity.
	ErrDestinationMismatch = errors.New("multistream: destination does not match checkpoint")
	// ErrSourceMismatch rejects a Resume against a source that does not
	// match the checkpoint's bound identity.
	ErrSourceMismatch = errors.New("multistream: source does not match checkpoint")
)

// DefaultChunkSize is the plan chunk size when the caller does not
// override it.
const DefaultChunkSize = 64 << 20 // 64 MiB

// TransferOptions configures one native large-file transfer.
type TransferOptions struct {
	// SFTP is the session configuration.
	SFTP SFTPConfig
	// Streams is the number of SFTP sessions (1..MaxSFTPStreams).
	Streams int
	// Workers is the number of copy workers; 0 means Streams. Workers
	// beyond Streams simply wait for a free writer — bounded, no busy-wait.
	Workers int
	// ChunkSize is the plan chunk size; 0 means DefaultChunkSize.
	ChunkSize uint64
	// CheckpointPath is the local checkpoint file path. The caller owns
	// this path; the transfer never deletes it.
	CheckpointPath string
	// OnChunkComplete is an optional progress hook invoked after each
	// chunk's checkpoint save.
	OnChunkComplete func(done, total int)
}

func (o *TransferOptions) normalize() error {
	if o.Streams == 0 {
		o.Streams = 1
	}
	if o.Streams < 1 || o.Streams > MaxSFTPStreams {
		return fmt.Errorf("multistream: streams must be between 1 and %d, got %d", MaxSFTPStreams, o.Streams)
	}
	if o.Workers == 0 {
		o.Workers = o.Streams
	}
	if o.Workers < 1 {
		return fmt.Errorf("multistream: workers must be positive, got %d", o.Workers)
	}
	if o.ChunkSize == 0 {
		o.ChunkSize = DefaultChunkSize
	}
	if o.CheckpointPath == "" {
		return errors.New("multistream: checkpoint path is empty")
	}
	return nil
}

// statSource stats the local source; it must be a regular file.
func statSource(abs string) (size uint64, mtime time.Time, err error) {
	fi, err := os.Stat(abs)
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("multistream: stat source: %w", err)
	}
	if !fi.Mode().IsRegular() {
		return 0, time.Time{}, fmt.Errorf("multistream: source %q is not a regular file", abs)
	}
	return uint64(fi.Size()), fi.ModTime(), nil
}

// verifySourceUnchanged confirms the source still has the exact identity
// the transfer was planned against.
func verifySourceUnchanged(abs string, size uint64, mtime time.Time) error {
	s2, m2, err := statSource(abs)
	if err != nil {
		return err
	}
	if s2 != size || !m2.Equal(mtime) {
		return fmt.Errorf("%w: %s changed size or mtime during transfer", ErrSourceChanged, abs)
	}
	return nil
}

// destinationIdentity binds a checkpoint to a canonical destination:
// [user@]host:finalPath.
func destinationIdentity(d target.Destination, finalPath string) string {
	who := d.Host
	if d.User != "" {
		who = d.User + "@" + d.Host
	}
	return who + ":" + finalPath
}

// randomTempPath builds an unpredictable, run-specific temp path in the
// final path's directory.
func randomTempPath(finalPath string) (string, error) {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("multistream: generate temp name: %w", err)
	}
	dir := path.Dir(finalPath)
	base := path.Base(finalPath)
	return path.Join(dir, ".catsync-tmp-"+base+"-"+hex.EncodeToString(b[:])), nil
}

// Transfer performs a fresh native large-file transfer: deterministic plan,
// atomic local checkpoint, sequential SFTP session pool, exclusive
// same-directory remote temp creation, range writes through the writer
// pool, per-chunk atomic checkpoint persistence, a final source re-check.
// The final path is NOT published here — publishing is a separate step.
//
// A checkpoint must not already exist; otherwise the caller must Resume.
// Any failure preserves the temp and checkpoint for a safe resume.
func Transfer(ctx context.Context, sourceAbs, sourceRel, remoteFinal string, opts TransferOptions) error {
	if ctx == nil {
		return errors.New("multistream: nil context")
	}
	if err := opts.normalize(); err != nil {
		return err
	}
	if err := discovery.ValidateRelPath(sourceRel); err != nil {
		return fmt.Errorf("multistream: source relative path: %w", err)
	}
	if err := backend2ValidateFinal(remoteFinal); err != nil {
		return err
	}
	size, mtime, err := statSource(sourceAbs)
	if err != nil {
		return err
	}
	if _, err := os.Stat(opts.CheckpointPath); err == nil {
		return fmt.Errorf("%w: %s", ErrCheckpointConflict, opts.CheckpointPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("multistream: stat checkpoint: %w", err)
	}
	chunks, err := Plan(size, opts.ChunkSize)
	if err != nil {
		return err
	}
	tempPath, err := randomTempPath(remoteFinal)
	if err != nil {
		return err
	}
	ckpt, err := NewCheckpoint(sourceRel, size, mtime, destinationIdentity(opts.SFTP.Dest, remoteFinal), tempPath, opts.ChunkSize)
	if err != nil {
		return err
	}
	// The checkpoint exists before any remote write.
	if err := ckpt.Save(opts.CheckpointPath); err != nil {
		return fmt.Errorf("multistream: save initial checkpoint: %w", err)
	}
	return copyWithPool(ctx, sourceAbs, size, mtime, chunks, ckpt, remoteFinal, tempPath, true, &opts)
}

// Resume continues a transfer from a strict checkpoint. The destination
// identity, source relative path, size, nanosecond mtime, chunk size, and
// temp path must all match; the temp is validated (not re-created, not
// re-truncated) and only the pending chunks are transferred.
func Resume(ctx context.Context, sourceAbs, checkpointPath, remoteFinal string, opts TransferOptions) error {
	if ctx == nil {
		return errors.New("multistream: nil context")
	}
	if err := opts.normalize(); err != nil {
		return err
	}
	ckpt, err := LoadCheckpoint(checkpointPath)
	if err != nil {
		return err
	}
	if ckpt.Destination != destinationIdentity(opts.SFTP.Dest, remoteFinal) {
		return fmt.Errorf("%w: checkpoint is bound to %q, not %q", ErrDestinationMismatch, ckpt.Destination, destinationIdentity(opts.SFTP.Dest, remoteFinal))
	}
	size, mtime, err := statSource(sourceAbs)
	if err != nil {
		return err
	}
	if size != ckpt.Size || !mtime.Equal(ckpt.ModTime) {
		return fmt.Errorf("%w: checkpoint expects size %d mtime %s; source has size %d mtime %s",
			ErrSourceMismatch, ckpt.Size, ckpt.ModTime, size, mtime)
	}
	chunks, err := Plan(ckpt.Size, ckpt.ChunkSize)
	if err != nil {
		return err
	}
	opts.CheckpointPath = checkpointPath
	return copyWithPool(ctx, sourceAbs, ckpt.Size, ckpt.ModTime, chunks, ckpt, remoteFinal, ckpt.TempPath, false, &opts)
}

// copyWithPool is the shared fresh/resume copy engine: sequential session
// pool, resume-target validation, writer pool, chunked Copy with per-chunk
// atomic checkpoint persistence, final source re-check.
func copyWithPool(ctx context.Context, sourceAbs string, size uint64, mtime time.Time, chunks []Chunk, ckpt *Checkpoint, remoteFinal, tempPath string, fresh bool, opts *TransferOptions) error {
	return opts.SFTP.WithClients(ctx, opts.Streams, func(clients []*sftp.Client) error {
		spec := RemoteTempSpec{FinalPath: remoteFinal, TempPath: tempPath, Size: size}
		// Fresh: exclusive create. Resume: the pool validates the existing
		// temp (regular file, exact size, final not yet published).
		if fresh {
			if err := PrepareFreshTemp(clients[0], spec); err != nil {
				return err
			}
		}
		pool, err := OpenRemoteWriterPool(spec, clients)
		if err != nil {
			return err
		}
		src, err := os.Open(sourceAbs)
		if err != nil {
			pool.CloseAll()
			return fmt.Errorf("multistream: open source: %w", err)
		}
		copyErr := Copy(ctx, src, pool, size, chunks, CopyOptions{
			Workers:   opts.Workers,
			Completed: ckpt.CompletedSet(),
			OnComplete: func(chunk Chunk) error {
				if err := ckpt.MarkComplete(chunk.Index); err != nil {
					return err
				}
				if err := ckpt.Save(opts.CheckpointPath); err != nil {
					// The checkpoint is the resume contract: a save failure
					// stops the copy immediately rather than claiming work
					// that is not durably recorded.
					return err
				}
				if opts.OnChunkComplete != nil {
					opts.OnChunkComplete(len(ckpt.Completed), len(chunks))
				}
				return nil
			},
		})
		src.Close()
		if copyErr != nil {
			pool.CloseAll()
			return copyErr
		}
		if err := pool.SyncAll(); err != nil {
			pool.CloseAll()
			return fmt.Errorf("multistream: sync temp: %w", err)
		}
		if err := pool.CloseAll(); err != nil {
			return fmt.Errorf("multistream: close writer pool: %w", err)
		}
		// The source must not have changed while we copied it.
		if err := verifySourceUnchanged(sourceAbs, size, mtime); err != nil {
			return err
		}
		return nil
	})
}

// backend2ValidateFinal validates the remote final path locally.
func backend2ValidateFinal(finalPath string) error {
	if finalPath == "" {
		return fmt.Errorf("%w: final path is empty", ErrInvalidRemoteTemp)
	}
	for _, r := range finalPath {
		if r == 0 {
			return fmt.Errorf("%w: final path contains a NUL byte", ErrInvalidRemoteTemp)
		}
	}
	if !path.IsAbs(finalPath) {
		return fmt.Errorf("%w: final path %q is not absolute", ErrInvalidRemoteTemp, finalPath)
	}
	if path.Clean(finalPath) != finalPath {
		return fmt.Errorf("%w: final path %q is not clean", ErrInvalidRemoteTemp, finalPath)
	}
	if finalPath == "/" {
		return fmt.Errorf("%w: final path must not be the root", ErrInvalidRemoteTemp)
	}
	return nil
}
