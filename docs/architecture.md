# CatSync MVP architecture

CatSync is a local orchestration CLI for local-to-remote and remote-to-local
transfers. It discovers the source tree, obtains endpoint metadata in batches,
assigns every path exactly one backend in an
immutable plan, and drives external `rsync` and `ssh` processes plus its own
native large-file backend. The small-file stage is native: CatSync partitions
small files in pure Go and runs concurrent rsync batches itself; no external
fpsync/fpart is used. The large-file stage is native multistream: sequential
system-ssh SFTP sessions copy disjoint byte ranges into a receiving-side
same-directory temp object with per-chunk atomic checkpoints and a no-clobber hardlink
publish; no external mscp is used. The remote host needs only SSH/SFTP,
rsync, and GNU findutils (plus GNU coreutils for native upload metadata); it
does not run a CatSync daemon.

## Safety model

Planning produces one owner for every relative path:

| Source/destination state | Auto backend |
| --- | --- |
| unchanged size and mtime | skip |
| new file below threshold | native parallel-rsync batches (backend name `fpsync` for compatibility) |
| new file at or above threshold | native multistream (backend name `mscp` for compatibility); uploads use rsync if the server lacks the hardlink extension |
| existing or uncertain file | rsync |
| symlink, special file, or scan error | unsupported (distinct task status) |

Size and modification time are planning hints, not proof of content equality.
Verification is a separate completion gate. Backend sets are immutable for a
run and stages execute sequentially, so two programs never write the same
destination path concurrently. Ownership is exact: each path has one backend,
and within the small-file stage each batch owns a disjoint file set, so the
concurrent rsync processes never touch the same destination file. Shared
destination parents are pre-created with one batched remote mkdir for uploads
or symlink-safe component creation locally for downloads, and the
batches run with `--no-implied-dirs`, so implied parent directories are
never created or attribute-updated by rsync — no directory-metadata race.
`copy` stops after these non-deleting stages. `sync` is explicitly destructive:
only after every optimized transfer stage succeeds, one full-tree rsync pass
uses archive, checksum, hard-link, ACL, xattr, and delayed-delete semantics.
That final pass represents empty directories and symlinks that the per-file
planner intentionally omits. A failed earlier stage never reaches deletion.

The source is rescanned before a task starts and after it exits (locally for
uploads, through batched remote manifests and exact per-file native probes
for downloads). A changed
source is a failed task, not a successful transfer. State is written by
temporary-file plus rename and a destination lock prevents overlapping runs.

## Execution stages

1. Stream regular-file discovery into a bounded planner (local Lstat walk or
   one NUL-delimited remote GNU-find manifest).
2. Collect receiving-side metadata (one remote manifest for uploads or one
   local walk for downloads).
3. Persist the plan and original source working directory.
4. Run the native small-file stage: pack pending small-file tasks into
   deterministic disjoint batches (at most 2000 files and at most the large
   threshold in logical bytes per batch), create receiving-side parent
   directories safely, then run up to `jobs` concurrent
   rsync processes with `--no-implied-dirs` and per-batch NUL-delimited
   `--files-from` lists. Then run native multistream new-large-file tasks
   (checkpoint per chunk, exclusive temp, hardlink publish), then the rsync
   conservative/delta stage.
5. For `sync` only, persist an unreconciled intent and run the full-tree
   checksum mirror with `--delete-delay`; persist reconciliation success.
   `copy` skips this stage and never deletes destination-only entries.
6. Verify successful tasks and atomically save their terminal state. The
   default `auto` policy always checks size, then compares SHA-256 digests when
   the remote endpoint provides `sha256sum`, `shasum`, `openssl`, or BusyBox.
   If none is installed, it completes with size verification and emits a
   prominent warning after the completion line. Strict `sha256` mode fails
   instead. The remote utility reads remote content and returns only digest
   records; CatSync never downloads remote files as a hashing fallback.

If remote manifest collection fails, auto mode routes all regular files to
rsync. Forced backends fail when they cannot preserve the safety policy.

## External-tool findings

### Native small-file batching (replacing external fpsync/fpart)

Earlier revisions drove external `fpsync`/`fpart`; the current build packs
small files itself (`internal/partition`, pure Go, stable next-fit, no
filesystem or subprocess access) and runs the resulting disjoint batches as
concurrent rsync invocations. The change removed a real boundary hazard:
fpart's skip-big option skips only files *strictly larger* than its limit,
which let a threshold-sized file be copied by both fpsync and its planned
owner; the planner now owns the boundary exactly (`size >= threshold` is
large) with no tool-side skip list. Batches are deterministic, so resume
rebuilds equivalent batches from pending tasks without any fpsync run ID or
state directory. rsync's per-file partial files live next to the
destination file, so partial progress survives re-batching.

### Native multistream large-file backend

The native backend (`internal/backend/multistream`) needs no transfer tool
beyond system ssh: it speaks the SFTP subsystem itself through
`github.com/pkg/sftp`. One transfer: deterministic chunk plan → atomic local
checkpoint (JSON; source size, nanosecond mtime, destination identity, chunk
size, completed set, unpredictable same-directory temp path) saved before any
remote write → sequential SFTP sessions → exclusive temp create, preallocate,
sync → per-session O_WRONLY handle pool → range copies where each chunk first
crosses a remote fsync durability barrier and only then enters the checkpoint
(so "completed" means remotely durable) → final sync, source re-check,
metadata (permissions via SFTP, exact nanosecond mtime via a controlled ssh
`touch` command; SFTP v3 carries only whole seconds) → no-clobber hardlink
publish → final size/mtime verification. Publish uses `hardlink@openssh.com`, never PosixRename; a final
path that exists is never overwritten.

Resume validates the checkpoint strictly (version, source, destination, size,
mtime, chunk size) and the existing temp (regular file, exact size, checked
again through the open handle), skips completed chunks, and copies only the
pending ranges. The crash window between a successful hardlink and the
recorded completion is recovered by proving final and temp are the same inode
via a controlled remote `test -ef`. Legacy external-mscp checkpoints (binary,
relative-path format) are detected and fail closed.

Native uploads require an OpenSSH ≥ 9.0 sftp-server (fsync, hardlink) and GNU
coreutils on the remote. Upload auto mode probes the capability before any remote
write; only a task proven never started (no checkpoint reference, no derived
`.nativeckpt`, no legacy `.mscpckpt`) may fall back to the conservative rsync
stage. Everything else fails closed and keeps its backend, checkpoint, temp,
and final untouched, because rsync would bypass the no-clobber publish. A
probe error is not an "unsupported" answer and never causes a fallback; a
forced run fails clearly.

Remote-to-local native transfer mirrors these invariants: a GNU-find probe
binds the exact remote size and nanosecond mtime to the immutable plan; each
system-ssh SFTP session opens an independent read handle; range workers write
an exclusive same-directory local temp; local fsync precedes each checkpoint
completion; the remote identity is rechecked before local mode/mtime and a
no-clobber `link(2)` publish. Resume validates the checkpoint and local temp
and skips completed ranges. Since publication is local, downloads do not
require the remote hardlink extension or GNU `touch`.

### rsync

Rsync is the correctness fallback and the owner of existing large files. The
default command uses archive semantics without deletion, a relative
`--partial-dir`, and NUL-delimited `--files-from=- --from0`. Modern rsync
protects remote arguments by default; CatSync detects remote compatibility
before relying on version-dependent flags. It does not use `--whole-file` or
`--inplace`.

The separate mirror configuration is reachable only from `sync`. It has no
`--files-from` filter and uses `--archive --checksum --hard-links --acls
--xattrs --partial --delete-delay`, without protecting a separate partial
directory that could remain as an extra destination entry. Deletion is therefore an explicit command-level
policy, not a pass-through flag. `sync --dry-run` runs the same command with
`--dry-run --itemize-changes` so deletions and non-regular entries are visible
without mutation. `MirrorReconciled` is saved false before launch and true
only after exit zero; a failed reconciliation remains resumable even when all
per-file tasks completed.

## Progress accounting

One concurrency-safe reporter aggregates successful write-byte deltas from
all active backends. Native multistream reports every successful range write;
each rsync process emits `--info=progress2`, whose monotonic counters are
converted to deltas before aggregation. Terminal mode redraws one line at a
bounded rate. Redirected output is silent by default; forced line mode emits
periodic snapshots. Every snapshot includes percentage, logical bytes,
smoothed current speed, elapsed time, ETA, ETD, and the active stage. These
are logical progress estimates rather than exact network-byte accounting.

## Persistent model

Each JSON run records a schema version, run ID, source, destination, original
working directory, configuration, timestamps, and tasks. A task records its
relative path, source size/mtime, destination metadata, backend, status,
attempt count, checkpoint reference, and last error. Valid statuses are
`pending`, `running`, `completed`, `failed`, `skipped`, and `unsupported`.
`unsupported` records entries the optimized per-file planner cannot transfer
(symlinks, special files, scan errors). They remain unsupported and are
reported for `copy`. A successful full-tree `sync` reconciliation owns them
through rsync and atomically changes their task status to completed; if rsync
cannot reproduce them, reconciliation fails and the run never claims an exact
mirror.
Metadata-skipped regular files are likewise promoted to completed after the
checksum reconciliation proves or repairs their contents.

A task is completed only after subprocess success, unchanged-source checks,
and requested verification. On startup, `running` becomes `pending` with an
interruption error so `resume` can revalidate it.

Persistence failures are correctness failures, not warnings. A task's
`running` state is saved before its subprocess starts, and a failed save
prevents the launch and propagates to the command. For the parallel
small-file batches, the whole batch's running intent is one atomic save
(mutate and save under the run lock, with full rollback on failure), so a
failed save leaves every task pending and no batch process starts.
Cancellation is checked at every batch boundary — before work, during
pre-checks, before the intent save, and between save and launch — so a
cancelled batch never lingers as running and never starts a process. Completion is saved
before a native checkpoint is retired; the checkpoint reference is cleared
and persisted before the file is removed, so a crash leaves either a
referenced checkpoint or a harmless orphan file, never a completed task
whose only copy state vanished. A resume-time recovery pass retires
references left on durably completed tasks, and a fully successful run
removes its per-run checkpoint directory.

## Compatibility and limitations

Program overrides (diagnostics and tests): `CATSYNC_SSH` and `CATSYNC_RSYNC`
replace executables CatSync itself spawns; `CATSYNC_SSH` also becomes rsync's
`-e` transport and must be a whitespace-free token. `CATSYNC_FPSYNC`,
`CATSYNC_FPART`, and `CATSYNC_MSCP` no longer exist: CatSync never runs or
probes those tools. The native large-file backend uses the system ssh for
every connection, so ssh_config, agents, known_hosts, ProxyJump, and hardware
keys apply unchanged. All child processes inherit `SSH_AUTH_SOCK`; CatSync
never adds `-i` or `IdentitiesOnly`, parses a key, invokes `step`, or requires
a certificate file. A Smallstep `step ssh login` identity that exists only in
the inherited agent is therefore usable directly; an expired certificate can
be refreshed and the durable run resumed.

CatSync checks installed help/version output at runtime. A missing rsync or
ssh is fatal. Optimized `copy` excludes symlinks and special files and never
deletes destination-only data; `sync` delegates those entries and delayed
deletion to its final full-tree rsync pass. Neither mode promises a consistent
snapshot of a mutating source tree.

Primary references:

- <https://github.com/pkg/sftp>
- <https://github.com/RsyncProject/rsync/blob/master/rsync.1.md>
