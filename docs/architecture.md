# CatSync MVP architecture

CatSync is a local orchestration CLI. It discovers a local source tree, obtains
remote metadata in batches, assigns every path exactly one backend in an
immutable plan, and invokes `fpsync`, `mscp`, and `rsync` as external
programs. The remote host needs only SSH/SFTP and rsync; it does not run a
CatSync daemon.

## Safety model

Planning produces one owner for every relative path:

| Source/destination state | Auto backend |
| --- | --- |
| unchanged size and mtime | skip |
| new file below threshold | fpsync when compatible and threshold >= 2 bytes, otherwise rsync |
| new file at or above threshold | mscp when compatible, otherwise rsync |
| existing or uncertain file | rsync |
| symlink, special file, or scan error | unsupported (distinct task status) |

Size and modification time are planning hints, not proof of content equality.
Verification is a separate completion gate. Backend sets are immutable for a
run and stages execute sequentially, so two programs never write the same
destination path concurrently. Ownership is exact: each path has one backend.
fpsync is the exception in *mechanism*, not in concurrent access — it crawls
the whole tree and cannot take the per-file list, so its internal rsync may
redundantly copy a file owned by another backend when the destination
differs; that write is sequential, content-identical, and later verified, and
the file's owner still drives verification and failure accounting. The MVP
never passes a deletion option.

The source is rescanned before a task starts and after it exits. A changed
source is a failed task, not a successful transfer. State is written by
temporary-file plus rename and a destination lock prevents overlapping runs.

## Execution stages

1. Stream regular-file discovery into a bounded planner.
2. Fetch one NUL-delimited remote manifest over SSH, rather than one SSH call
   per file.
3. Persist the plan and original source working directory.
4. Run the fpsync small-file stage (bounded by fpart `-s threshold-1` so
   at-or-above-threshold files are skipped, not copied), mscp
   new-large-file tasks, then the rsync conservative/delta stage
   (NUL-delimited `--files-from` list).
5. Verify successful tasks and atomically save their terminal state.

If remote manifest collection fails, auto mode routes all regular files to
rsync. Forced backends fail when they cannot preserve the safety policy.

## External-tool findings

### fpsync and fpart

`fpsync -S` enables sudo. It is unrelated to `fpart -S`, which skips files
larger than the `fpart -s` partition limit in live mode. `fpsync -O` replaces
the default fpart options and requires option/value tokens separated by `|`.
Therefore the safe small-file command shape is:

```text
fpsync -n JOBS -f FILES -s (THRESHOLD-1) -O -S -t STATE_DIR SOURCE/ DEST/
```

The shifted limit is required because fpart skips only files *strictly
larger* than `-s`, while CatSync classifies a file at or above the threshold
as large. Passing `threshold-1` makes the tool boundary exactly
`size >= threshold`; a threshold below 2 bytes cannot be expressed (fpart
rejects `-s 0`) and disables the fpsync stage in auto mode. `-O -S` enables
fpart large-file skipping. CatSync must preserve fpsync's standard output
because skipped files are reported as pseudo-partition `S` and are not part of
a normal partition or fpsync's success accounting. CatSync does not use fpsync
directory mode `-E` because it enables rsync deletion.

The behavior was exercised locally with fpsync/fpart 1.7.1. A tree containing
6-byte and 7-byte files plus a 2048-byte file with a 1024-byte limit copies
only the small files; the large file appears as `S\t2048\t./large.bin` on
standard output. A dedicated boundary experiment with 1000/1024/2048-byte
files confirms the strictly-greater rule: with `-s 1024` the 1024-byte file
is copied and only 2048 is skipped, while with `-s 1023` (the CatSync
mapping for a 1024-byte threshold) both the 1024- and 2048-byte files are
reported as `S` records and only the below-threshold file is copied.

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
`CATSYNC_FPSYNC`, and `CATSYNC_MSCP` replace executables CatSync itself
spawns; `CATSYNC_SSH` also becomes rsync's `-e` transport and must be a
whitespace-free token. `CATSYNC_FPART` feeds only the availability probe,
because fpsync resolves fpart through `PATH`; descendants spawned by the
external tools inherit their own resolution and are outside CatSync's
control.

CatSync checks installed help/version output at runtime. Missing mscp or
fpsync causes auto mode to fall back to rsync; a missing rsync is fatal. The
MVP excludes symlinks and special files, does not delete remote files, and does
not promise a consistent snapshot of a mutating source tree.

Primary references:

- <https://github.com/martymac/fpart/blob/master/man/fpart.1>
- <https://github.com/martymac/fpart/blob/master/man/fpsync.1>
- <https://github.com/upa/mscp/blob/main/doc/mscp.rst>
- <https://github.com/RsyncProject/rsync/blob/master/rsync.1.md>
