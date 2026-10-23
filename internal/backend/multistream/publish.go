package multistream

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/pkg/sftp"

	"github.com/frankruan/catsync/internal/backend"
	"github.com/frankruan/catsync/internal/discovery"
	"github.com/frankruan/catsync/internal/process"
)

// Sentinel errors of the publish layer; test with errors.Is.
var (
	// ErrHardlinkUnsupported reports a server without the
	// hardlink@openssh.com extension. PosixRename is never used as a
	// fallback because it would overwrite an existing final path.
	ErrHardlinkUnsupported = errors.New("multistream: server lacks the hardlink@openssh.com extension")
	// ErrPublishConflict reports a final path that exists and cannot be
	// proven to be this transfer's object. It is never overwritten or
	// deleted.
	ErrPublishConflict = errors.New("multistream: final path exists and is not this transfer's object")
)

// sshCommandArgv builds the argv for a controlled remote command:
// ssh [-6] -T [user@]host cmd...  — exec'd without a local shell; remote
// paths in remoteCmd must already be shell-quoted, everything else must be
// constants we formatted.
func sshCommandArgv(cfg SFTPConfig, remoteCmd []string) ([]string, error) {
	prog := cfg.Program
	if prog == "" {
		prog = "ssh"
	}
	for _, r := range prog {
		if r == 0 {
			return nil, errors.New("multistream: ssh program contains a NUL byte")
		}
	}
	if err := backend.ValidateDestination(cfg.Dest); err != nil {
		return nil, err
	}
	who := cfg.Dest.Host
	if cfg.Dest.User != "" {
		who = cfg.Dest.User + "@" + cfg.Dest.Host
	}
	argv := []string{prog}
	if cfg.IPv6 {
		argv = append(argv, "-6")
	}
	argv = append(argv, "-T", who)
	return append(argv, remoteCmd...), nil
}

// setRemoteMtime sets the nanosecond mtime of a remote path via GNU touch
// over a controlled ssh command. SFTP v3 cannot express sub-second times.
// The decimal is built from floor-seconds plus a non-negative nanosecond
// fraction, which is correct for pre-epoch times too.
func setRemoteMtime(ctx context.Context, cfg SFTPConfig, p string, mtime time.Time) error {
	dec := fmt.Sprintf("%d.%09d", mtime.Unix(), mtime.Nanosecond())
	argv, err := sshCommandArgv(cfg, []string{"touch", "-m", "-d", "@" + dec, discovery.ShellQuote(p)})
	if err != nil {
		return err
	}
	return process.Run(ctx, process.Spec{Name: argv[0], Args: argv[1:], Env: cfg.Env, Stderr: cfg.Stderr})
}

// remoteSameFile runs `test a -ef b` over a controlled ssh command and
// reports whether a and b are the same filesystem object (same inode).
func remoteSameFile(ctx context.Context, cfg SFTPConfig, a, b string) (bool, error) {
	argv, err := sshCommandArgv(cfg, []string{"test", discovery.ShellQuote(a), "-ef", discovery.ShellQuote(b)})
	if err != nil {
		return false, err
	}
	err = process.Run(ctx, process.Spec{Name: argv[0], Args: argv[1:], Env: cfg.Env, Stderr: cfg.Stderr})
	if err == nil {
		return true, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
		return false, nil
	}
	return false, fmt.Errorf("multistream: remote same-inode probe: %w", err)
}

// supportsHardlink reports whether the server advertises the
// hardlink@openssh.com extension.
func supportsHardlink(client *sftp.Client) bool {
	if client == nil {
		return false
	}
	_, ok := client.HasExtension("hardlink@openssh.com")
	return ok
}

// SetRemoteMeta applies permission bits and the exact nanosecond mtime to
// the still-unpublished temporary object. Mode comes only from formatted
// permission bits; the mtime is exact to nanoseconds via GNU touch. chown,
// ACLs, and xattrs are never attempted.
func SetRemoteMeta(ctx context.Context, cfg SFTPConfig, client *sftp.Client, p string, mode os.FileMode, mtime time.Time) error {
	if client == nil {
		return fmt.Errorf("%w: nil sftp client", ErrInvalidRemoteTemp)
	}
	if err := client.Chmod(p, mode.Perm()); err != nil {
		return fmt.Errorf("multistream: set temp permissions: %w", err)
	}
	if err := setRemoteMtime(ctx, cfg, p, mtime); err != nil {
		return fmt.Errorf("multistream: set temp mtime: %w", err)
	}
	return nil
}

// PublishFinal publishes the synced temp object to its final path with
// no-clobber semantics: a hardlink in the same directory, never a
// PosixRename. An existing final path is never overwritten. After the link,
// the final's size (and mtime seconds) are verified.
//
// The temporary object must already be synced, closed, metadata-applied,
// and source-verified.
func PublishFinal(ctx context.Context, cfg SFTPConfig, client *sftp.Client, spec RemoteTempSpec, wantMtime time.Time) error {
	if client == nil {
		return fmt.Errorf("%w: nil sftp client", ErrInvalidRemoteTemp)
	}
	if err := spec.validate(); err != nil {
		return err
	}
	if _, ok := client.HasExtension("hardlink@openssh.com"); !ok {
		return ErrHardlinkUnsupported
	}
	if err := client.Link(spec.TempPath, spec.FinalPath); err != nil {
		// The hardlink is authoritative: a pre-existing final path makes
		// it fail. Lstat only reclassifies the error.
		if _, lerr := client.Lstat(spec.FinalPath); lerr == nil {
			return fmt.Errorf("%w: %s", ErrRemoteFinalExists, spec.FinalPath)
		}
		return fmt.Errorf("multistream: publish hardlink: %w", err)
	}
	fi, err := client.Lstat(spec.FinalPath)
	if err != nil {
		return fmt.Errorf("multistream: verify published final: %w", err)
	}
	if fi.Size() != int64(spec.Size) {
		return fmt.Errorf("%w: published final is %d bytes, want %d", ErrRemoteTempMismatch, fi.Size(), spec.Size)
	}
	// SFTP v3 carries mtime as uint32 seconds: pre-epoch times wrap to
	// post-2106 values on the wire. The exact ns mtime was set via the
	// command channel; here we verify the wire-representable seconds.
	wireSec := uint64(uint32(wantMtime.Unix()))
	if uint64(fi.ModTime().Unix()) != wireSec {
		return fmt.Errorf("%w: published final mtime %d, want %d (wire seconds)", ErrRemoteTempMismatch, fi.ModTime().Unix(), wantMtime.Unix())
	}
	return nil
}

// ProbePublishState resolves the publish state on resume. It reports:
//
//   - published: final exists and is provably the same object as the temp
//     (crash window: hardlink succeeded but the run state was not saved).
//   - conflict: final exists but is not provably our object — refuse.
//
// When final does not exist, it returns (false, nil, nil): the transfer
// continues normally.
func ProbePublishState(ctx context.Context, cfg SFTPConfig, client *sftp.Client, spec RemoteTempSpec) (published bool, err error) {
	_, ferr := client.Lstat(spec.FinalPath)
	if ferr != nil {
		if errors.Is(ferr, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("multistream: lstat final path: %w", ferr)
	}
	// final exists. Prove identity against the temp object.
	if _, terr := client.Lstat(spec.TempPath); terr != nil {
		// No temp to compare against: cannot prove identity. Refuse.
		return false, fmt.Errorf("%w: %s exists and no temp object is available for identity proof", ErrPublishConflict, spec.FinalPath)
	}
	same, err := remoteSameFile(ctx, cfg, spec.TempPath, spec.FinalPath)
	if err != nil {
		return false, err
	}
	if !same {
		return false, fmt.Errorf("%w: %s exists and is not the same object as the temp", ErrPublishConflict, spec.FinalPath)
	}
	return true, nil
}
