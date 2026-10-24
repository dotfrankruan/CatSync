package multistream

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dotfrankruan/CatSync/internal/discovery"
	"github.com/dotfrankruan/CatSync/internal/process"
)

// remoteFileIdentity obtains exact nanosecond source identity through GNU
// find. SFTP v3 exposes only whole-second mtime, which is insufficient for
// deciding whether a source stayed unchanged before no-clobber publish.
func remoteFileIdentity(ctx context.Context, cfg SFTPConfig, remotePath string) (uint64, time.Time, error) {
	if err := (RemoteSourceSpec{Path: remotePath}).validate(); err != nil {
		return 0, time.Time{}, err
	}
	q := discovery.ShellQuote(remotePath)
	script := "if [ -f " + q + " ] && [ ! -L " + q + " ]; then find " + q + " -maxdepth 0 -printf 'file\\0%s\\0%T@\\0'; else exit 44; fi"
	argv, err := sshCommandArgv(cfg, []string{script})
	if err != nil {
		return 0, time.Time{}, err
	}
	var stdout, stderr strings.Builder
	if err := process.Run(ctx, process.Spec{Name: argv[0], Args: argv[1:], Env: cfg.Env, Stdout: &stdout, Stderr: &stderr}); err != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail != "" {
			return 0, time.Time{}, fmt.Errorf("multistream: stat remote source: %w (%s)", err, detail)
		}
		return 0, time.Time{}, fmt.Errorf("multistream: stat remote source: %w", err)
	}
	manifest, err := discovery.CollectManifest(strings.NewReader(stdout.String()))
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("multistream: parse remote source identity: %w", err)
	}
	entry, ok := manifest["file"]
	if !ok || len(manifest) != 1 {
		return 0, time.Time{}, fmt.Errorf("%w: remote source identity returned %d records", ErrSourceChanged, len(manifest))
	}
	return uint64(entry.Size), entry.ModTime, nil
}

func verifyRemoteFileIdentity(ctx context.Context, cfg SFTPConfig, remotePath string, size uint64, mtime time.Time) error {
	gotSize, gotMtime, err := remoteFileIdentity(ctx, cfg, remotePath)
	if err != nil {
		return err
	}
	if gotSize != size || !gotMtime.Equal(mtime) {
		return fmt.Errorf("%w: remote source %s changed size or mtime during transfer", ErrSourceChanged, remotePath)
	}
	return nil
}
