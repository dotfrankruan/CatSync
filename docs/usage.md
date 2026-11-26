# CatSync usage

CatSync copies files between a local machine and a remote host over SSH in
either direction. It plans first, transfers in parallel, never overwrites an
existing destination file with `copy`, and can resume an interrupted run.

- New here? Start with [First transfer](#first-transfer).
- Looking for a flag? See [Options](#options).
- Using the optional daemon? See [Daemon transport](#daemon-transport-one-shot-catsyncd).
- Something failed? See [Troubleshooting](#troubleshooting).

## Install

```bash
brew install dotfrankruan/catsync/catsync
```

Release archives for linux/darwin on amd64/arm64 are on the
[releases page](https://github.com/dotfrankruan/CatSync/releases). From source:

```bash
make install   # catsync and catsyncd -> ~/.local/bin
```

See the [README](../README.md#-install) for archive and `go install` options.

**Runtime requirements**

| | |
| --- | --- |
| Local | `ssh`; `rsync` for updated files, mirror sync, and parallel small-file batches. |
| Remote | POSIX shell, GNU findutils, and `sshd` with `sftp-server`; `rsync` when updates, mirror sync, or parallel small-file batches are needed. |
| Native uploads | OpenSSH ≥ 9.0 `sftp-server` and GNU coreutils. |
| Default daemon | Not required. Only install `catsyncd` for `--transport daemon`. |

Run `catsync doctor` to check the local tools.

## First transfer

```bash
catsync doctor
catsync plan ./data user@host:/srv/data
catsync copy ./data user@host:/srv/data
```

`plan` only inspects; it writes nothing. `copy` adds or updates files and
never deletes anything that exists only at the destination.

```bash
# exact mirror: deletes destination-only entries, after all transfer stages pass
catsync sync ./data user@host:/srv/data

# single file, remote to local
catsync copy user@host:/srv/archive.tar ./downloads
```

Remote paths must be absolute. Both operand orders are supported; exactly one
side must be remote.

## Commands

| Command | What it does |
| --- | --- |
| `catsync doctor` | Checks `ssh` and `rsync`; no secrets are printed. |
| `catsync plan SRC DEST` | Discovers, plans, and prints a summary. Transfers nothing. |
| `catsync copy SRC DEST` | Safe parallel transfer; never deletes destination data. |
| `catsync sync SRC DEST` | Optimized transfer, then a checksum-verified mirror pass. |
| `catsync resume [RUN_ID]` | Resumes the named or latest resumable run. |
| `catsync status [RUN_ID]` | Prints run state without changing it. |
| `catsync version` | Prints the version. |

### `plan`

Prints per-backend counts, skipped/unsupported entries, logical source bytes,
and whether an interrupted run would be resumed. Exit is nonzero for unsafe
forced-backend combinations.

### `copy`

The default safe choice:

```bash
catsync copy ./photos user@host:/srv/photos
catsync copy user@host:/srv/photos ./photos
```

### `sync`

`sync` is the only mode that can delete or replace destination entries. It runs
the optimized stages first and performs archive reconciliation with
`--delete-delay` only if every stage succeeded.

### `resume`

```bash
catsync resume                         # latest resumable run
catsync resume RUN_ID                  # explicit run
catsync resume --streams 16 RUN_ID     # parallelism can change
```

`resume` revalidates the source, reuses checkpoints and rsync partial files,
and rejects changed source files rather than silently copying them.

### `status`

```bash
catsync status
catsync status RUN_ID --verbose
```

Run state lives under `--state-dir` (default:
`$XDG_STATE_HOME/catsync` or `~/.local/state/catsync`).

## Paths and sources

- Upload: `[user@]host:/absolute/path LOCAL`
- Download: `LOCAL [user@]host:/absolute/path`
- Relative remote paths and `~` are rejected.
- Bracketed IPv6 literals are accepted.
- Remote-to-remote and local-to-local transfers are rejected.
- A single regular-file source is a point-to-point transfer and keeps its
  base name at the destination directory.

## Options

| Flag | Default | Meaning |
| --- | --- | --- |
| `--transport ssh\|daemon` | `ssh` | Remote data transport. `resume` inherits the recorded value. |
| `--backend` | `auto` | `auto`, `fpsync`, `mscp`, `sftp`, or `rsync`. |
| `--verify` | `auto` | `auto`, `none`, `size`, or `sha256`. |
| `--dry-run` | off | Transfer nothing; `sync` also previews deletions. |
| `--jobs N` | 4 | Concurrent files/batches; capped at 64. |
| `--streams N` | 4 | SFTP sessions per large file; capped at 64. |
| `--large-threshold SIZE` | `512MiB` | Size at or above which a file is “large”. |
| `--port N` | ssh config | Explicit SSH port; persisted for resume. |
| `--ipv6` | off | Restrict SSH/SFTP/rsync to IPv6. |
| `--on-auth-failure` | `fail` | `fail` or `retry` after an authentication rejection. |
| `--fresh` | off | Start a new run instead of resuming the matching one. |
| `--state-dir DIR` | XDG state | Run-state directory. |
| `--progress` | `auto` | `auto`, `always`, or `never`. |
| `--quiet` / `--verbose` | off | Output control. |
| `--transport daemon` flags | — | See [Daemon transport](#daemon-transport-one-shot-catsyncd). |

Options can appear before, between, or after operands. `--` ends option parsing.
Raw `--delete` is rejected; choose `copy` or `sync` explicitly.

**Parallelism.** `--jobs` bounds concurrent files/batches; `--streams` bounds
SFTP sessions for one large file. Both default to 4 and are capped at 64. An
estimated peak above 64 connections prints one warning but is not refused.

## Transport modes

| Mode | Selection | Data path | SSH after handoff |
| --- | --- | --- | --- |
| SSH | default | one SSH/SFTP connection per stream | N/A |
| Daemon direct TCP | `--transport daemon` | pinned mTLS TCP | Can exit |
| Daemon direct QUIC | `... --daemon-transport quic` | pinned QUIC TLS 1.3 | Can exit; experimental |
| Daemon SSH-forward | `... --daemon-bootstrap ssh-forward` | mTLS inside SSH tunnel | Required |

### Daemon transport (one-shot `catsyncd`)

The daemon transport replaces one SSH channel per data stream with a remote
one-shot agent. It is opt-in and requires `catsyncd` on the remote host:

```bash
catsync copy --transport daemon ./data user@host:/srv/data
catsync copy --transport daemon --daemon-transport quic ./data user@host:/srv/data
```

The flow:

1. One SSH bootstrap session starts `catsyncd serve --once`.
2. Both sides generate in-memory P-256 identities and pin the exact peer
   certificate.
3. Data uses pinned mutual TLS 1.3 (TCP) or pinned QUIC TLS 1.3 (no 0-RTT).
4. For direct TCP/QUIC, the authenticated data-control channel takes over:
   the bootstrap SSH exits, data heartbeats and the final authenticated bye own
   the session, and the one-shot worker exits when the run ends.
5. If the client dies, the worker exits after the bounded data-control
   heartbeat timeout. It never becomes a permanent service.

SSH-forward keeps the data path inside the SSH tunnel, so that mode remains
SSH-dependent. Legacy SSH is unchanged. If the direct data connection is lost,
the run fails safely and can be resumed; CatSync does not re-authenticate a
**new** direct session without SSH.

#### Daemon options

| Flag | Default | Meaning |
| --- | --- | --- |
| `--daemon-port N` | 43411 | TCP/UDP data port; `0` asks for an ephemeral port. |
| `--daemon-transport tcp\|quic` | `tcp` | QUIC is experimental, one UDP connection, no 0-RTT. |
| `--daemon-bootstrap direct\|ssh-forward` | `direct` | Direct detaches after handoff; ssh-forward stays tunnel-bound. |
| `--remote-catsyncd PATH` | `catsyncd` | Remote executable; CatSync never installs it. |
| `--daemon-listen ADDR` | `:43411` | Passed to `catsyncd serve --listen`. |
| `--daemon-addr HOST:PORT` | — | Override the data address (advanced proxy/NAT cases). |
| `--quic-qlog PATH` | — | Local QUIC qlog, mode 0600. |
| `--remote-quic-qlog PATH` | — | Remote QUIC qlog path, absolute. |

Remote prerequisites: `catsyncd`, `sftp-server`, and `rsync` when the plan needs
it. Direct handoff additionally needs `setsid`; without it the client warns and
keeps the SSH-bound lifecycle. Run `catsync daemon probe USER@HOST` for a
read-only capability report.

#### Security

- The listener binds only after the client certificate is verified and pinned.
- TLS requires 1.3, mutual auth, the pinned leaf, and a known ALPN.
- Handoff is accepted only over the authenticated data channel and only with
  the matching bootstrap session id.
- `exec` runs only the fixed `rsync` with a strictly validated `--server`
  argument vector.
- Identities, keys, and nonces are memory-only for one run.
- QUIC failure never silently switches transports. The error names the TCP and
  SSH alternatives explicitly.

Non-UTF-8 filenames are not representable in the daemon protocol; use
`--transport ssh` for trees containing them.

## Backend selection

`auto` assigns every regular file exactly one owner:

| Source / destination state | Backend |
| --- | --- |
| Same size and mtime | skipped (hint only; verification is the real gate) |
| New file below threshold | parallel `rsync` batches (`fpsync` compatibility name) |
| New file at/above threshold | native multistream (`mscp` compatibility name) |
| Existing or unknown destination state | `rsync` |
| Symlink, special file, scan error | `unsupported`, recorded distinctly |
| No rsync available | new files use native single-file SFTP (`sftp`) |

Forced backends must preserve the safety policy. For example, `--backend mscp`
refuses a file that already exists at the destination.

`copy` never follows symlinks or writes special files. A successful `sync`
pass delegates tree-level entries to its final rsync mirror; a failed mirror
never claims to be exact.

## Verification

| Mode | Gate |
| --- | --- |
| `none` | Backend exits zero and the source is unchanged. |
| `size` | Receiving-side size matches for every transferred file. |
| `auto` (default) | Size, then SHA-256 when a remote hashing tool is available. |
| `sha256` | Size and SHA-256, or fail if the remote cannot hash. |

`auto` falls back to size-only with a prominent warning **after** completion
when no SHA-256 utility exists. Size is not content proof. CatSync never
downloads a remote file just to hash it locally.

## Interruptions and resume

Ctrl-C, a dropped connection, or a failed stage leaves a resumable run.
Already durable bytes stay on disk; the next identical command resumes it.

- **Automatic:** rerun the same `copy`/`sync` command.
- **Explicit:** `catsync resume [RUN_ID]`.
- **Start over:** `--fresh`.

A destination lock is an OS advisory `flock`, not the existence of a lock file.
`kill -9` and power loss release it automatically. Lock files may remain; do
not delete them to “clear” a lock. State is not shareable across hosts, and
all clients sharing a state directory must be upgraded together.

## Authentication

CatSync delegates SSH to the system `ssh`: `~/.ssh/config`, agent,
`known_hosts`, `ProxyJump`, hardware keys, and `SSH_AUTH_SOCK` all work as
usual. It does not add `-i`, set `IdentitiesOnly`, parse keys, or require a
certificate file.

For a short-lived certificate in an agent:

```bash
step ssh login your-name
catsync copy ./data user@host:/srv/data
```

If credentials expire mid-run, the default fails with a resumable run. Use
`--on-auth-failure retry` to pause, renew credentials, and retry the stage on a
terminal:

```bash
catsync copy --on-auth-failure retry ./data user@host:/srv/data
```

CatSync never invokes `step`, never refreshes certificates itself, and never
bypasses SSH authentication.

## Troubleshooting

| Symptom | Fix |
| --- | --- |
| `Permission denied` after a while | Expired credentials. Re-authenticate and resume, or use `--on-auth-failure retry`. |
| `--jobs`/`--streams` seem ignored | `--streams` parallelizes one file; `--jobs` parallelizes files. Both cap at 64. |
| Large file planned for rsync | Native multistream handles new files only. Existing/unknown destinations use rsync. |
| `rsync: unrecognized option '--acls'` on macOS | System rsync is openrsync. Install GNU rsync and put it first in `PATH`. |
| `sync` refuses to delete | Reconciliation only starts after every transfer stage succeeds. |
| `corrupt run state ... unknown field "transport"` | An older `catsync` in `PATH` is reading newer state. Check `command -v catsync` / `catsync version`; upgrade all clients sharing the state dir. |
| `sshd` or `sftp-server` missing | Fix the remote OpenSSH setup; CatSync never installs system tools. |
| Daemon handoff did not detach | Remote lacks `setsid`, or the server is older. The warning explains the fallback. |

## Known limitations

- The source is not snapshotted. A file changed between checks fails its task,
  but a consistent copy of a mutating tree is not promised.
- `copy` does not transfer symlinks, special files, or scan errors. `sync` may
  delegate them to its final rsync pass.
- Native multistream is for new files only. Modified large files fall back to
  rsync. Remote-to-remote copies are not supported.
- Progress totals are logical write bytes, not exact network bytes; ETA/ETD are
  estimates.
- Default verification can fall back to size-only when the remote has no
  hashing utility. Use `--verify sha256` to make that fatal.
- Remote discovery requires a POSIX shell and GNU findutils. A failed upload
  destination manifest can fall back to rsync; failed remote source discovery
  for downloads fails closed.
- QUIC on the public path can still hit the known quic-go decryption/idle
  timeout issue. Keep TCP or SSH if this matters.

## Advanced: program overrides

For diagnostics and tests only:

| Variable | Scope |
| --- | --- |
| `CATSYNC_SSH` | Every SSH invocation, including rsync’s `-e` transport. One whitespace-free token. |
| `CATSYNC_RSYNC` | Every rsync process CatSync starts. |

Values are never printed. `CATSYNC_FPSYNC`, `CATSYNC_FPART`, and
`CATSYNC_MSCP` no longer exist.

## Advanced: security invariants

- All subprocesses use argument arrays; no local shell parses command lines.
- File lists use NUL-delimited stdin.
- Remote paths in shell snippets are single-quote escaped.
- SSH host-key checking is never disabled.
- No destination path has two writers: one owner per path, sequential stages,
  and disjoint small-file batches.
- `copy` never deletes; `sync` deletion is delayed until after successful
  transfer and checksum reconciliation.
- CatSync never reads or prints credentials or private keys.
