package multistream

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pkg/sftp"

	"github.com/dotfrankruan/CatSync/internal/discovery"
)

func downloadDestinationIdentity(localFinal string) string { return "local:" + localFinal }

// Download copies a new remote regular file into a same-directory local
// temp using multiple system-SSH SFTP sessions, then publishes without
// clobbering an existing local destination.
func Download(ctx context.Context, remoteSource, sourceRel, localFinal string, opts TransferOptions) (*TransferResult, error) {
	if ctx == nil {
		return nil, errors.New("multistream: nil context")
	}
	if err := opts.normalize(); err != nil {
		return nil, err
	}
	if err := discovery.ValidateRelPath(sourceRel); err != nil {
		return nil, fmt.Errorf("multistream: source relative path: %w", err)
	}
	if localFinal == "" || !filepath.IsAbs(localFinal) || filepath.Clean(localFinal) != localFinal {
		return nil, fmt.Errorf("%w: final path %q must be clean and absolute", ErrInvalidLocalTemp, localFinal)
	}
	if _, err := os.Stat(opts.CheckpointPath); err == nil {
		return nil, fmt.Errorf("%w: %s", ErrCheckpointConflict, opts.CheckpointPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("multistream: stat checkpoint: %w", err)
	}
	size, mtime, err := remoteFileIdentity(ctx, opts.SFTP, remoteSource)
	if err != nil {
		return nil, err
	}
	if want := opts.ExpectedRemote; want != nil && (size != want.Size || !mtime.Equal(want.ModTime)) {
		return nil, fmt.Errorf("%w: remote source changed since planning (size %d, mtime %s; planned size %d, mtime %s)",
			ErrSourceChanged, size, mtime, want.Size, want.ModTime)
	}
	chunks, err := Plan(size, opts.ChunkSize)
	if err != nil {
		return nil, err
	}
	tempPath, err := randomLocalTempPath(localFinal)
	if err != nil {
		return nil, err
	}
	ckpt, err := NewCheckpoint(sourceRel, size, mtime, downloadDestinationIdentity(localFinal), tempPath, opts.ChunkSize)
	if err != nil {
		return nil, err
	}
	if err := ckpt.Save(opts.CheckpointPath); err != nil {
		return nil, fmt.Errorf("multistream: save initial download checkpoint: %w", err)
	}
	spec := LocalTempSpec{FinalPath: localFinal, TempPath: tempPath, Size: size}
	err = opts.SFTP.WithClients(ctx, opts.Streams, func(clients []*sftp.Client) error {
		fi, err := clients[0].Lstat(remoteSource)
		if err != nil || !fi.Mode().IsRegular() || fi.Mode()&os.ModeSymlink != 0 || fi.Size() != int64(size) {
			if err == nil {
				err = fmt.Errorf("source is not the planned regular file")
			}
			return fmt.Errorf("multistream: lstat remote source: %w", err)
		}
		local, err := PrepareFreshLocalTemp(spec)
		if err != nil {
			return err
		}
		return copyAndPublishDownload(ctx, remoteSource, fi.Mode().Perm(), chunks, ckpt, spec, local, &opts, clients)
	})
	if err != nil {
		return nil, err
	}
	return &TransferResult{Published: true, TempPath: tempPath, CheckpointPath: opts.CheckpointPath}, nil
}

// ResumeDownload resumes only against the exact remote source and local
// final bound into the checkpoint. Completed chunks are never re-read.
func ResumeDownload(ctx context.Context, remoteSource, sourceRel, checkpointPath, localFinal string, opts TransferOptions) (*TransferResult, error) {
	if ctx == nil {
		return nil, errors.New("multistream: nil context")
	}
	// The explicit resume argument is authoritative. Callers should not
	// have to duplicate it in TransferOptions merely to pass normalization.
	opts.CheckpointPath = checkpointPath
	if err := opts.normalize(); err != nil {
		return nil, err
	}
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
	if ckpt.Destination != downloadDestinationIdentity(localFinal) {
		return nil, fmt.Errorf("%w: checkpoint is bound to %q", ErrDestinationMismatch, ckpt.Destination)
	}
	if err := verifyRemoteFileIdentity(ctx, opts.SFTP, remoteSource, ckpt.Size, ckpt.ModTime); err != nil {
		return nil, err
	}
	chunks, err := Plan(ckpt.Size, ckpt.ChunkSize)
	if err != nil {
		return nil, err
	}
	spec := LocalTempSpec{FinalPath: localFinal, TempPath: ckpt.TempPath, Size: ckpt.Size}
	published, err := ProbeLocalPublishState(spec)
	if err != nil {
		return nil, err
	}
	if published {
		if len(ckpt.Completed) != len(chunks) {
			return nil, fmt.Errorf("multistream: local final is published but checkpoint is incomplete")
		}
		return &TransferResult{Published: true, Recovered: true, TempPath: ckpt.TempPath, CheckpointPath: checkpointPath}, nil
	}
	err = opts.SFTP.WithClients(ctx, opts.Streams, func(clients []*sftp.Client) error {
		fi, err := clients[0].Lstat(remoteSource)
		if err != nil || !fi.Mode().IsRegular() || fi.Size() != int64(ckpt.Size) {
			if err == nil {
				err = fmt.Errorf("source is not the planned regular file")
			}
			return fmt.Errorf("multistream: lstat remote source: %w", err)
		}
		local, err := OpenResumeLocalTemp(spec)
		if err != nil {
			return err
		}
		return copyAndPublishDownload(ctx, remoteSource, fi.Mode().Perm(), chunks, ckpt, spec, local, &opts, clients)
	})
	if err != nil {
		return nil, err
	}
	return &TransferResult{Published: true, TempPath: ckpt.TempPath, CheckpointPath: checkpointPath}, nil
}

func copyAndPublishDownload(ctx context.Context, remoteSource string, mode os.FileMode, chunks []Chunk, ckpt *Checkpoint, spec LocalTempSpec, local *os.File, opts *TransferOptions, clients []*sftp.Client) error {
	readers, err := OpenRemoteReaderPool(RemoteSourceSpec{Path: remoteSource, Size: ckpt.Size}, clients)
	if err != nil {
		_ = local.Close()
		return err
	}
	copyErr := Copy(ctx, readers, local, ckpt.Size, chunks, CopyOptions{
		Workers: opts.Workers, Completed: ckpt.CompletedSet(),
		OnComplete: func(chunk Chunk) error {
			if err := local.Sync(); err != nil {
				return fmt.Errorf("multistream: sync local chunk %d: %w", chunk.Index, err)
			}
			if err := ckpt.MarkComplete(chunk.Index); err != nil {
				return err
			}
			if err := ckpt.Save(opts.CheckpointPath); err != nil {
				return err
			}
			if opts.OnChunkComplete != nil {
				opts.OnChunkComplete(len(ckpt.Completed), len(chunks))
			}
			return nil
		},
	})
	readerCloseErr := readers.CloseAll()
	if copyErr != nil {
		_ = local.Close()
		return copyErr
	}
	if readerCloseErr != nil {
		_ = local.Close()
		return fmt.Errorf("multistream: close remote reader pool: %w", readerCloseErr)
	}
	if err := local.Sync(); err != nil {
		_ = local.Close()
		return fmt.Errorf("multistream: sync local temp: %w", err)
	}
	if err := verifyRemoteFileIdentity(ctx, opts.SFTP, remoteSource, ckpt.Size, ckpt.ModTime); err != nil {
		_ = local.Close()
		return err
	}
	if err := SetLocalTempMeta(local, spec, mode, ckpt.ModTime); err != nil {
		_ = local.Close()
		return err
	}
	if err := local.Close(); err != nil {
		return fmt.Errorf("multistream: close local temp: %w", err)
	}
	return PublishLocalFinal(spec, ckpt.ModTime)
}
