# CatSync MVP architecture

CatSync is a local orchestration CLI. It discovers a local source tree, obtains
remote metadata in batches, assigns every path exactly one backend in an
immutable plan, and drives external `rsync`, `ssh`, and `mscp` processes.
The small-file stage is native: CatSync partitions small files in pure Go
and runs concurrent rsync batches itself; no external fpsync/fpart is used.
The remote host needs only SSH/SFTP and rsync; it does not run a CatSync
daemon.

## Safety model

Planning produces one owner for every relative path:

| Source/destination state | Auto backend |
| --- | --- |
| unchanged size and mtime | skip |
| new file below threshold | native parallel-rsync batches (backend name `fpsync` for compatibility) |
| new file at or above threshold | mscp when compatible, otherwise rsync |
| existing or uncertain file | rsync |
| symlink, special file, or scan error | unsupported (distinct task status) |

Size and modification time are planning hints, not proof of content equality.
Verification is a separate completion gate. Backend sets are immutable for a
run and stages execute sequentially, so two programs never write the same
destination path concurrently. Ownership is exact: each path has one backend,
and within the small-file stage each batch owns a disjoint file set, so the
concurrent rsync processes never touch the same destination file. Shared
destination parents are pre-created with one batched remote mkdir and the
batches run with `--no-implied-dirs`, so implied parent directories are
never created or attribute-updated by rsync — no directory-metadata race.
The MVP never passes a deletion option.

The source is rescanned before a task starts and after it exits. A changed
source is a failed task, not a successful transfer. State is written by
temporary-file plus rename and a destination lock prevents overlapping runs.

## Execution stages

1. Stream regular-file discovery into a bounded planner.
2. Fetch one NUL-delimited remote manifest over SSH, rather than one SSH call
   per file.
3. Persist the plan and original source working directory.
4. Run the native small-file stage: pack pending small-file tasks into
   deterministic disjoint batches (at most 2000 files and at most the large
   threshold in logical bytes per batch), create all remote parent
   directories with one batched ssh mkdir, then run up to `jobs` concurrent
   rsync processes with `--no-implied-dirs` and per-batch NUL-delimited
   `--files-from` lists. Then run mscp new-large-file tasks, then the rsync
   conservative/delta stage.
5. Verify successful tasks and atomically save their terminal state.

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

### mscp

mscp 0.2.4 supports parallel SSH connections (`-n`), in-flight SFTP requests
(`-a`), minimum chunks (`-s`), bitrate limits (`-L`), IPv6-only resolution
(`-6`), checkpoint writing (`-W`), and resume (`-R`). It does not support
remote-to-remote copying or block-level delta updates. Auto mode therefore
uses it only for large regular files confirmed absent at the destination.

Checkpoints contain relative paths. Resume must execute with the same working
directory as the failed command and repeat any SSH options. A checkpoint is
not deleted by mscp after success; CatSync retains it until its own verification
gate passes.

### rsync

Rsync is the correctness fallback and the owner of existing large files. The
default command uses archive semantics without deletion, a relative
`--partial-dir`, and NUL-delimited `--files-from=- --from0`. Modern rsync
protects remote arguments by default; CatSync detects remote compatibility
before relying on version-dependent flags. It does not use `--whole-file` or
`--inplace`.

## Persistent model

Each JSON run records a schema version, run ID, source, destination, original
working directory, configuration, timestamps, and tasks. A task records its
relative path, source size/mtime, destination metadata, backend, status,
attempt count, checkpoint reference, and last error. Valid statuses are
`pending`, `running`, `completed`, `failed`, `skipped`, and `unsupported`.
`unsupported` records entries the MVP cannot transfer (symlinks, special
files, scan errors); it is never folded into `skipped` and never counted as
completed. Run summaries and `status` always print the unsupported count and
name the paths on stderr after `sync`/`resume`.

A task is completed only after subprocess success, unchanged-source checks,
and requested verification. On startup, `running` becomes `pending` with an
interruption error so `resume` can revalidate it.

Persistence failures are correctness failures, not warnings. A task's
`running` state is saved before its subprocess starts, and a failed save
prevents the launch and propagates to the command. Completion is saved
before an mscp checkpoint is retired; the checkpoint reference is cleared
and persisted before the file is removed, so a crash leaves either a
referenced checkpoint or a harmless orphan file, never a completed task
whose only copy state vanished. A resume-time recovery pass retires
references left on durably completed tasks, and a fully successful run
removes its per-run checkpoint directory.

## Compatibility and limitations

Program overrides (diagnostics and tests): `CATSYNC_SSH`, `CATSYNC_RSYNC`,
and `CATSYNC_MSCP` replace executables CatSync itself spawns; `CATSYNC_SSH`
also becomes rsync's `-e` transport and must be a whitespace-free token.
`CATSYNC_FPSYNC`/`CATSYNC_FPART` no longer exist: CatSync never runs or
probes those tools. Processes spawned by external tools (mscp's libssh)
inherit their own resolution and are outside CatSync's control.

CatSync checks installed help/version output at runtime. A missing mscp
narrows auto mode (large files fall back to rsync); a missing rsync is
fatal. The MVP excludes symlinks and special files, does not delete remote
files, and does not promise a consistent snapshot of a mutating source tree.

Primary references:

- <https://github.com/upa/mscp/blob/main/doc/mscp.rst>
- <https://github.com/RsyncProject/rsync/blob/master/rsync.1.md>
