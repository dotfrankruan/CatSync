package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"

	"github.com/dotfrankruan/CatSync/internal/daemon/protocol"
)

// hashPaths streams SHA-256 for each regular file, in request order. Symlinks
// are rejected: the planner verifies regular files only.
func hashPaths(ctx context.Context, paths []string, emit func(protocol.HashRecord) error) (int, error) {
	count := 0
	for _, p := range paths {
		if err := ctx.Err(); err != nil {
			return count, err
		}
		if err := protocol.ValidateAbsPath(p); err != nil {
			return count, err
		}
		info, err := os.Lstat(p)
		if err != nil {
			return count, osError("lstat hash target", err)
		}
		if !info.Mode().IsRegular() {
			return count, protocol.NewError(protocol.CodeInvalidArgument, "hash target %q is not a regular file", p)
		}
		f, err := os.Open(p)
		if err != nil {
			return count, osError("open hash target", err)
		}
		h := sha256.New()
		_, err = io.Copy(h, &ctxReader{ctx: ctx, r: f})
		closeErr := f.Close()
		if err != nil {
			return count, err
		}
		if closeErr != nil {
			return count, osError("close hash target", closeErr)
		}
		if err := emit(protocol.HashRecord{Type: protocol.RecordTypeHash, Path: p, SHA256: hex.EncodeToString(h.Sum(nil))}); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

// ctxReader cancels long hashes promptly.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c *ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}
