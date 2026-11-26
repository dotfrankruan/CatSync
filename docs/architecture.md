# CatSync architecture

CatSync is a local orchestrator. It discovers a source tree, plans one backend
per path, runs the transfer in sequential stages with bounded parallelism, and
verifies the result. The default transport uses only the system `ssh` and
`rsync`; the optional daemon transport replaces the remote data path with a
one-shot `catsyncd` and pinned mutual TLS.

- **One owner per path.** Backends are immutable for a run, so two programs
  never write the same destination file.
- **Stages are sequential.** Parallelism happens inside a stage, not between
  stages that could race for destination metadata.
- **State is crash-aware.** Run state is atomic; native chunks are checkpointed
  after durability; destination locks are kernel advisory locks.
- **Copy is safe by default.** Only `sync` can delete, and only after every
  optimized transfer stage succeeded.

## Components

| Component | Responsibility |
| --- | --- |
| `internal/planner` | Classifies every source path and chooses one backend. |
| `internal/partition` | Packs small files into deterministic disjoint batches. |
| `internal/backend/multistream` | Native SFTP range copier with checkpoints and no-clobber publish. |
| `internal/backend/rsync` | Small-file batches, conservative updates, and mirror reconciliation. |
| `internal/executor` | Runs stages, tracks progress, resume, and verification. |
| `internal/state` | Atomic run/task persistence and advisory destination locks. |
| `internal/daemon` | Optional bootstrap, pinned TLS/QUIC data plane, RPC/SFTP/EXEC. |

## Planning and ownership

| Source / destination state | Auto backend |
| --- | --- |
| Unchanged size + mtime | skip (planning hint only) |
| New file below `--large-threshold` | native parallel-rsync batches |
| New file at/above threshold | native multistream |
| Existing / unknown destination | `rsync` |
| Symlink, special file, scan error | `unsupported` |

Size and mtime are hints, not content proof. Verification is a separate gate.
`--backend` can force a backend only when it can preserve the safety rules;
otherwise planning fails.

### Single-file sources

A single-file source is normalized to a point-to-point transfer. The file keeps
its base name at the destination; the rest of the pipeline still uses the
directory-root model.

## Execution pipeline

1. **Discover** local files with a bounded walk, or a remote with one
   NUL-delimited GNU-find manifest.
2. **Collect metadata** for the receiving side.
3. **Persist the plan** and original working directory.
4. **Small-file stage:** deterministic disjoint batches, safe parent creation,
   then up to `--jobs` concurrent rsync processes.
5. **Large-file stage:** native multistream for new large files.
6. **Conservative stage:** rsync for existing/uncertain files.
7. **Mirror stage** (`sync` only): full-tree archive reconciliation with
   delayed deletion.
8. **Verify** by size and optionally SHA-256, then save terminal state.

Stages 4–6 do not delete. If remote metadata collection fails, auto mode routes
regular files to rsync. A failed upload destination manifest can fall back to
rsync; a failed remote source manifest for a download fails closed.

## Transfer backends

### Native small-file batching

`internal/partition` packs small files with a stable next-fit algorithm. Each
batch owns a disjoint file set; resume deterministically rebuilds batches from
pending tasks. rsync's per-file partial files live next to the destination, so
progress survives re-batching.

### Native multistream

The large-file backend speaks SFTP directly through `github.com/pkg/sftp`; no
remote helper is required.

1. Plan deterministic chunks and persist a local checkpoint before any remote
   write.
2. Create an exclusive same-directory temp object; preallocate and sync.
3. Copy ranges over sequential SFTP sessions; each chunk crosses a remote fsync
   barrier before it enters the checkpoint.
4. Recheck the source, publish with `hardlink@openssh.com` without overwriting
   an existing destination path.
5. Verify final size and mtime.

`hardlink@openssh.com` is required for native uploads; POSIX rename is never
used for publish. Resume validates the checkpoint and temp object, skips
durable ranges, and fails closed if the identity no longer matches.

### rsync

`rsync` owns existing files, delta updates, the mirror stage, and small-file
batches. CatSync uses argument arrays, NUL-delimited file lists, relative
partial directories for non-mirror stages, and never passes a raw user
`--delete`. The mirror stage has no file filter and uses
`--archive --checksum --hard-links --acls --xattrs --partial --delete-delay`.

macOS ships Apple openrsync at `/usr/bin/rsync`; its missing ACL/xattr options
make it unsuitable for `sync`. Install GNU rsync and put it first in `PATH`.

## State, resume, and locks

Each run is a JSON document with a schema version, endpoint identity, config,
timestamps, and tasks. Tasks record source identity, destination metadata,
backend, status, checkpoint reference, attempts, and errors.

- A task is completed only after subprocess success, an unchanged-source check,
  and requested verification.
- On startup, interrupted `running` tasks become `pending`. Before work, the
  executor revalidates the source and destination; a changed source fails the
  task instead of copying it.
- State writes use temp-file + rename. Batch running-intent is one atomic save;
  cancellation is checked before launch.
- A completion is persisted before its checkpoint is retired. Crash recovery
  therefore sees either a live checkpoint reference or a harmless orphan.
- Resume repairs references on already-completed tasks and removes the
  checkpoint directory after a fully successful run.

The destination lock is an OS advisory `flock` on a stable lock-file inode. It
is released by the kernel after a crash, including `kill -9`. Lock files may
remain on disk unlocked; their existence is not ownership. All clients that
share a state directory must be upgraded together, and the state directory
must be on a local filesystem with reliable `flock`.

Run-state compatibility is one-way: new binaries read older additive state,
but older binaries may reject fields written by a newer release. A failure
like `unknown field "transport"` usually means an older `catsync` earlier in
`PATH`, not a corrupt file.

## Daemon transport

The daemon transport is opt-in and changes only the remote data path. Planner,
checkpoints, publish rules, and verification stay the same.

```text
server_hello -> client_hello -> bind -> ready -> ready_ack -> active_ack
```

Both sides create a fresh ECDSA P-256 self-signed leaf per run. The client
verifies and pins the exact server DER; the server does the same for the
client. The listener is created only after the client identity is verified,
and the accept loop starts only after `active_ack`.

### Data planes

| ALPN | Purpose |
| --- | --- |
| `catsync-http/1.1` | Strict JSON control RPC over mutual TLS. |
| `catsync-sftp/1` | Byte tunnel to a real per-stream `sftp-server` child. |
| `catsync-exec/1` | Framed rsync server process with a validated argv. |

Control operations are manifest, mkdir, metadata, hash, stat, and samefile.
Streams are NDJSON; only a final `end{count}` record makes a result valid.
Every RPC/control JSON message enforces frame bounds, duplicate-key rejection,
unknown-field rejection, and version checks.

QUIC is one connection per run, TLS 1.3 mutual auth, pinned leaf, ALPN
`catsync-quic/1`, no 0-RTT. The first byte of every stream selects
CONTROL/SFTP/EXEC; each type has its own budget so control retains capacity.
The connection is single-per-run and explicit stream-credit release is
required. QUIC is still experimental on some public paths.

The local rsh agent bridges rsync's `-e "<catsync> rsh"` to the exec channel
with a private 0700 directory, a 0600 Unix socket, same-uid peer credentials,
and a random per-run environment token. That keeps direct mode at one SSH
bootstrap session.

### SSH bootstrap lifecycle handoff

Direct TCP and direct QUIC use the SSH bootstrap only to establish trust.

1. `catsyncd` is launched under `setsid -w` in a new OS session.
2. Certificates are exchanged and pinned; the authenticated data control
   channel passes its control precheck.
3. The client sends `/v1/handoff` with the bootstrap session id.
4. The server writes `detached`, closes SSH stdio, and stops treating SSH
   EOF/bye as shutdown authority.
5. The client kills only its local bootstrap `ssh` process.
6. Data heartbeats and `/v1/bye` own the lifecycle. A client crash is detected
   by a bounded heartbeat timeout; normal completion drains and exits.

The worker is one-shot. There is no permanent listener or service. SSH-forward
is intentionally excluded because its data path lives inside the SSH channel.
If handoff is unavailable, the client warns and stays on the SSH-bound
lifecycle; it never silently falls back. Losing the direct data connection
later is a safe failure with checkpoint/resume, not a no-SSH re-authentication
mechanism.

See [ssh-handoff.svg](assets/ssh-handoff.svg).

## Progress and verification

Progress aggregates successful write-byte deltas from all active backends.
Native multistream reports range writes; rsync reports per-batch progress.
The display includes percentage, logical bytes, speed, elapsed, ETA/ETD,
connection count, finished/checked/skipped files, and up to five active
transfers. Logical progress is not a network-byte counter.

Verification is a completion gate:

| Mode | Behavior |
| --- | --- |
| `none` | Backend success + source unchanged. |
| `size` | Receiving-side size must match. |
| `auto` | Size, then SHA-256 when a remote hashing utility exists. |
| `sha256` | Size + SHA-256, or fail if no remote hashing tool. |

The remote hashing utility reads only remote content and returns digest
records; CatSync never downloads files just to hash them.

## Compatibility and limitations

- `CATSYNC_SSH` and `CATSYNC_RSYNC` replace programs for diagnostics/tests.
  `CATSYNC_SSH` must be a single whitespace-free token.
- An explicit `--port` is recorded and applied to every SSH/SFTP/rsync path.
- The source is not a snapshot. A file changed between checks fails its task.
- `copy` does not transfer symlinks/special files; `sync` may delegate them to
  the final rsync pass.
- Native multistream handles new files only; modified large files use rsync.
- Remote-to-remote copies are unsupported.
- Default verification can complete with size-only and a post-completion
  warning if the remote has no SHA-256 tool.
- Remote manifests are buffered in memory during collection.

## References

- <https://github.com/pkg/sftp>
- <https://github.com/RsyncProject/rsync/blob/master/rsync.1.md>
