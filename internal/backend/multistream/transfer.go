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

	"github.com/dotfrankruan/CatSync/internal/discovery"
	"github.com/dotfrankruan/CatSync/internal/target"
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
	// OnProgress receives successful transferred-byte deltas.
	OnProgress func(uint64)
	// ExpectedRemote, when non-nil, binds a fresh remote-to-local download
	// to the size and nanosecond mtime observed by the planner. Uploads and
	// resume (which is bound by its checkpoint) ignore this field.
	ExpectedRemote *RemoteIdentity
}

// RemoteIdentity is the immutable source identity used to prevent a fresh
// download from silently accepting a file that changed after planning.
type RemoteIdentity struct {
	Size    uint64
	ModTime time.Time
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
func statSource(abs string) (size uint64, mtime time.Time, mode os.FileMode, err error) {
	fi, err := os.Stat(abs)
	if err != nil {
		return 0, time.Time{}, 0, fmt.Errorf("multistream: stat source: %w", err)
	}
	if !fi.Mode().IsRegular() {
		return 0, time.Time{}, 0, fmt.Errorf("multistream: source %q is not a regular file", abs)
	}
	return uint64(fi.Size()), fi.ModTime(), fi.Mode().Perm(), nil
}

// verifySourceUnchanged confirms the source still has the exact identity
// the transfer was planned against.
func verifySourceUnchanged(abs string, size uint64, mtime time.Time) error {
	s2, m2, _, err := statSource(abs)
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

// TransferResult is the outcome of a completed Transfer/Resume.
type TransferResult struct {
	// Published: the final path is now live (hardlink published, or an
	// earlier crashed run's publish recovered via the same-inode probe).
	Published bool
	// Recovered: Published was reached by recovering an earlier run's
	// completed-but-unrecorded publish (the crash window).
	Recovered bool
	// TempPath still exists after publish; removing it is cleanup that
	// must happen only after the run state is durably recorded.
	TempPath string
	// CheckpointPath must be retired only after the run state is durably
	// recorded.
	CheckpointPath string
}

// Transfer performs a fresh native large-file transfer: deterministic plan,
// atomic local checkpoint, sequential SFTP session pool, exclusive
// same-directory remote temp creation, range writes through the writer
// pool, per-chunk atomic checkpoint persistence, a final source re-check,
// metadata, and a no-clobber hardlink publish.
//
// A checkpoint must not already exist; otherwise the caller must Resume.
// Any failure preserves the temp and checkpoint for a safe resume.
func Transfer(ctx context.Context, sourceAbs, sourceRel, remoteFinal string, opts TransferOptions) (*TransferResult, error) {
	if ctx == nil {
		return nil, errors.New("multistream: nil context")
	}
	if err := opts.normalize(); err != nil {
		return nil, err
	}
	if err := discovery.ValidateRelPath(sourceRel); err != nil {
		return nil, fmt.Errorf("multistream: source relative path: %w", err)
	}
	if err := validateFinalPath(remoteFinal); err != nil {
		return nil, err
	}
	size, mtime, mode, err := statSource(sourceAbs)
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(opts.CheckpointPath); err == nil {
		return nil, fmt.Errorf("%w: %s", ErrCheckpointConflict, opts.CheckpointPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("multistream: stat checkpoint: %w", err)
	}
	chunks, err := Plan(size, opts.ChunkSize)
	if err != nil {
		return nil, err
	}
	tempPath, err := randomTempPath(remoteFinal)
	if err != nil {
		return nil, err
	}
	ckpt, err := NewCheckpoint(sourceRel, size, mtime, destinationIdentity(opts.SFTP.Dest, remoteFinal), tempPath, opts.ChunkSize)
	if err != nil {
		return nil, err
	}
	// The checkpoint exists before any remote write.
	if err := ckpt.Save(opts.CheckpointPath); err != nil {
		return nil, fmt.Errorf("multistream: save initial checkpoint: %w", err)
	}
	err = opts.SFTP.WithClients(ctx, opts.Streams, func(clients []*sftp.Client) error {
		spec := RemoteTempSpec{FinalPath: remoteFinal, TempPath: tempPath, Size: size}
		// Capability probe before any remote write: without the hardlink
		// extension we could never publish without clobbering, so fail
		// here — the checkpoint stays for diagnosis and nothing remote is
		// created.
		if !supportsHardlink(clients[0]) {
			return ErrHardlinkUnsupported
		}
		if err := PrepareFreshTemp(clients[0], spec); err != nil {
			return err
		}
		return copyAndPublish(ctx, sourceAbs, size, mtime, mode, chunks, ckpt, spec, &opts, clients)
	})
	if err != nil {
		return nil, err
	}
	return &TransferResult{Published: true, TempPath: tempPath, CheckpointPath: opts.CheckpointPath}, nil
}

// Resume continues a transfer from a strict checkpoint. The destination
// identity, source relative path, size, nanosecond mtime, chunk size, and
// temp path must all match; the temp is validated (not re-created, not
// re-truncated) and only the pending chunks are transferred.
func Resume(ctx context.Context, sourceAbs, sourceRel, checkpointPath, remoteFinal string, opts TransferOptions) (*TransferResult, error) {
	if ctx == nil {
		return nil, errors.New("multistream: nil context")
	}
	if err := opts.normalize(); err != nil {
		return nil, err
	}
	// The library enforces the source binding itself rather than relying on
	// an executor caller to do so. Both validation and comparison happen
	// before any SSH/SFTP session can start.
	if err := discovery.ValidateRelPath(sourceRel); err != nil {
		return nil, fmt.Errorf("%w: invalid expected source path %q: %v", ErrSourceMismatch, sourceRel, err)
	}
	ckpt, err := LoadCheckpoint(checkpointPath)
	if err != nil {
		return nil, err
	}
	if ckpt.Source != sourceRel {
		return nil, fmt.Errorf("%w: checkpoint is bound to source %q, not %q", ErrSourceMismatch, ckpt.Source, sourceRel)
	}
	if ckpt.Destination != destinationIdentity(opts.SFTP.Dest, remoteFinal) {
		return nil, fmt.Errorf("%w: checkpoint is bound to %q, not %q", ErrDestinationMismatch, ckpt.Destination, destinationIdentity(opts.SFTP.Dest, remoteFinal))
	}
	size, mtime, mode, err := statSource(sourceAbs)
	if err != nil {
		return nil, err
	}
	if size != ckpt.Size || !mtime.Equal(ckpt.ModTime) {
		return nil, fmt.Errorf("%w: checkpoint expects size %d mtime %s; source has size %d mtime %s",
			ErrSourceMismatch, ckpt.Size, ckpt.ModTime, size, mtime)
	}
	chunks, err := Plan(ckpt.Size, ckpt.ChunkSize)
	if err != nil {
		return nil, err
	}
	opts.CheckpointPath = checkpointPath
	var result *TransferResult
	err = opts.SFTP.WithClients(ctx, opts.Streams, func(clients []*sftp.Client) error {
		spec := RemoteTempSpec{FinalPath: remoteFinal, TempPath: ckpt.TempPath, Size: ckpt.Size}
		// Crash-window recovery: a previous run may have published the
		// final via hardlink but crashed before the run state was recorded.
		// If final and temp are provably the same object, the publish is
		// already done.
		published, err := ProbePublishState(ctx, opts.SFTP, clients[0], spec)
		if err != nil {
			return err
		}
		if published {
			if len(ckpt.Completed) != len(chunks) {
				return fmt.Errorf("multistream: final %s is published but the checkpoint is incomplete (%d/%d chunks) — refusing to trust the state", remoteFinal, len(ckpt.Completed), len(chunks))
			}
			fi, err := clients[0].Lstat(remoteFinal)
			if err != nil {
				return fmt.Errorf("multistream: verify recovered final: %w", err)
			}
			if fi.Size() != int64(ckpt.Size) {
				return fmt.Errorf("%w: recovered final is %d bytes, want %d", ErrRemoteTempMismatch, fi.Size(), ckpt.Size)
			}
			result = &TransferResult{Published: true, Recovered: true, TempPath: ckpt.TempPath, CheckpointPath: checkpointPath}
			return nil
		}
		if !supportsHardlink(clients[0]) {
			return ErrHardlinkUnsupported
		}
		return copyAndPublish(ctx, sourceAbs, ckpt.Size, ckpt.ModTime, mode, chunks, ckpt, spec, &opts, clients)
	})
	if err != nil {
		return nil, err
	}
	if result != nil {
		return result, nil
	}
	return &TransferResult{Published: true, TempPath: ckpt.TempPath, CheckpointPath: checkpointPath}, nil
}

// copyAndPublish runs the shared fresh/resume engine inside an established
// session pool: resume-target validation, writer pool, chunked Copy with
// per-chunk remote durability + atomic checkpoint persistence, SyncAll,
// CloseAll, final source re-check, temp metadata, and the no-clobber
// publish with verification.
//
// Durability ordering is the core invariant: a chunk's data must be fsync'd
// on the server (SyncAny, one borrowed handle — server fsync covers the
// whole file) BEFORE its index is recorded in the checkpoint. A sync or
// save failure cancels the copy immediately and the unrecorded chunk is
// re-copied on resume, so the checkpoint never claims work the server has
// not durably persisted.
func copyAndPublish(ctx context.Context, sourceAbs string, size uint64, mtime time.Time, mode os.FileMode, chunks []Chunk, ckpt *Checkpoint, spec RemoteTempSpec, opts *TransferOptions, clients []*sftp.Client) error {
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
		Workers:    opts.Workers,
		Completed:  ckpt.CompletedSet(),
		OnProgress: opts.OnProgress,
		OnComplete: func(chunk Chunk) error {
			// Durability first: the chunk's bytes must be fsync'd on the
			// server before the checkpoint may claim them.
			if err := pool.SyncAny(); err != nil {
				return fmt.Errorf("multistream: sync chunk %d: %w", chunk.Index, err)
			}
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
	// Metadata on the still-unpublished temp: permission bits and the exact
	// nanosecond mtime. Metadata failure prevents the publish.
	if err := SetRemoteMeta(ctx, opts.SFTP, clients[0], spec.TempPath, mode, mtime); err != nil {
		return err
	}
	if err := PublishFinal(ctx, opts.SFTP, clients[0], spec, mtime); err != nil {
		return err
	}
	return nil
}

// validateFinalPath validates the remote final path locally.
func validateFinalPath(finalPath string) error {
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
