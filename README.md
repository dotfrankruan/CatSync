# CatSync

*Because one TCP connection is never enough.*

CatSync is a local orchestration CLI for moving large directory trees to a
remote host over SSH. It discovers a local source tree, collects the remote
destination's metadata in one batched SSH call, splits the work into
**disjoint** sets — one backend per file, never two writers for the same
destination path — and drives battle-tested external tools to move the data
in parallel:

- **fpsync** (fpart + rsync) for the small-file stage,
- **mscp** for large files confirmed new at the destination (parallel SSH
  connections per file),
- **rsync** for everything else, as the conservative correctness fallback
  with delta transfer.

The remote host needs only SSH/SFTP and rsync; no CatSync daemon runs
remotely.

CatSync is licensed under the AGPL-3.0-only; see [LICENSE](LICENSE).

## Architecture

```
catsync (local orchestrator)
  │ 1. stream local discovery (Lstat; symlinks never followed)
  │ 2. one batched remote manifest over SSH (GNU find, NUL-delimited)
  │ 3. plan: assign every relative path exactly one backend
  │ 4. persist the run atomically (state dir) before transferring
  │ 5. execute stages sequentially, so writers never overlap:
  │      fpsync (small files) → mscp (new large files) → rsync (rest)
  │ 6. verify completed tasks (batched remote size check)
  │ 7. mark tasks completed; only then retire mscp checkpoints
  └ all subprocesses are exec'd with argument arrays — no local shell
```

A run is a versioned JSON document in the state directory (default
`$XDG_STATE_HOME/catsync` or `~/.local/state/catsync`), written
temp-file-plus-rename with fsync. A destination-scoped lock file prevents
two runs from writing the same remote tree at once. Interrupted runs are
recovered explicitly: `running` tasks return to `pending` and `resume`
revalidates and continues them.

## External dependencies

| Tool | Required | Purpose |
| --- | --- | --- |
| `rsync` | **yes** | conservative transfer and delta backend |
| `ssh` | **yes** | remote transport, manifest, mkdir |
| `fpsync` | optional | parallel small-file stage |
| `fpart` | optional | partitioning helper fpsync drives |
| `mscp` | optional | parallel large-file backend |

The remote host needs GNU findutils for the optimized manifest collection
(`find -printf`). If manifest collection fails — no GNU find, no directory
yet handled separately, SSH failure — auto mode plans **conservatively**:
every transferable file routes to rsync, and CatSync says so. A missing
destination directory is detected remotely (the script exits 42) and counts
as an authoritative *empty* destination, so first-time syncs still use
fpsync/mscp.

`catsync doctor` checks all five tools and reports versions.

## Build and install

```sh
make build     # writes bin/catsync with the git version baked in
make test      # go test ./...
make check     # gofmt, go vet, go test -race
make cross     # dist/ binaries: linux/amd64, linux/arm64, darwin/arm64
make clean     # remove bin/ and dist/
```

Install with `go install github.com/frankruan/catsync/cmd/catsync@latest`
or copy `bin/catsync` into your `PATH`.

## Commands

### `catsync doctor`

Probes rsync, ssh, fpsync, fpart, and mscp with bounded version/help
probes, distinguishing required from optional tools. Missing optional tools
only narrow auto mode; missing rsync or ssh fails the check. No secrets or
environment contents are printed.

### `catsync plan SOURCE [user@]host:/absolute/path`

Discovers the source, collects the remote manifest (or announces the
conservative fallback), runs the planner, and prints the summary: per-backend
counts, skipped/unsupported counts, and the logical source byte volume (a
source-side sum, **not** a network-traffic estimate). Nothing is
transferred and no run state is written. Unsafe forced-backend plans (for
example `--backend fpsync` with a large file) exit nonzero.

### `catsync sync SOURCE [user@]host:/absolute/path`

Executes the full durable lifecycle and prints the run ID and final
summary. Nonzero exit on any transfer or verification failure. A run
interrupted with Ctrl-C stays resumable.

### `catsync resume [RUN_ID]`

Resumes the named run, or the latest resumable run without an argument.
Reacquires the destination lock, revalidates source metadata (a changed
source file fails its task instead of transferring), reuses the fpsync run
ID and state directory, resumes mscp with `-R CHECKPOINT` from the
**original working directory** (checkpoint paths are relative to it —
resume from a different working directory and mscp cannot find its data),
and reuses rsync's partial directory. Retained checkpoints survive failed
resumes.

### `catsync status [RUN_ID]`

Prints a run without mutating it: status, source/destination, timestamps,
configuration, per-status and per-backend task counts, the fpsync run ID,
retained checkpoint count, and recorded errors. Missing or corrupt state
exits nonzero.

### `catsync version`

Prints the version.

### Options

| Flag | Default | Meaning |
| --- | --- | --- |
| `--backend` | `auto` | `auto`, `fpsync`, `mscp`, or `rsync` |
| `--dry-run` | off | plan and persist the run, transfer nothing |
| `--verify` | `size` | `none` or `size` (`sha256` is not implemented, see below) |
| `--jobs` | 4 | max concurrent transfer processes |
| `--streams` | 4 | mscp SSH connections per process |
| `--large-threshold` | `512MiB` | size at which a file counts as large |
| `--ipv6` | off | restrict ssh, rsync transport, and mscp to IPv6 |
| `--state-dir` | XDG state | run state directory |
| `--quiet` / `--verbose` | off | output control |

`--delete` is always rejected: CatSync never deletes destination files.

**SSH connection budget.** A run's estimated worst-case connection count is
`jobs × streams` (each of up to `jobs` mscp processes holds `streams`
connections; fpsync uses `jobs`; the manifest and verification phases use
one). Configurations with `jobs × streams > 64` are rejected, so a typo
cannot open hundreds of connections.

## Backend selection

Auto mode assigns every regular file exactly one owner:

| Source / destination state | Backend |
| --- | --- |
| unchanged size and mtime (hint only) | skipped — no transfer |
| new file below the threshold | fpsync (rsync if fpsync/fpart missing) |
| new file at or above the threshold | mscp (rsync if mscp missing) |
| existing, modified, or state unknown | rsync |
| symlink or special file | unsupported — reported, never followed |

Size and mtime are planning *hints*, not proof of content equality; the
verification stage is the completion gate. Forced backends must preserve
the safety policy: `--backend mscp` rejects files that exist at the
destination or whose destination state is unknown, and `--backend fpsync`
rejects files at or above the threshold.

### fpsync, fpart, and the two different `-S` flags

CatSync runs `fpsync -v -n JOBS -f 2000 -s THRESHOLD -O -S -t STATEDIR SRC/ DST/`.
The `-S` here is passed *through* `fpsync -O` to **fpart**, enabling
fpart's skip-files-larger-than-the-partition-limit mode — completely
unrelated to **fpsync's own** `-S`, which enables sudo and is never used.
fpsync's `-E` (directory mode) is never used either: it enables rsync's
`--delete`.

fpsync always crawls the whole source tree. Files at or above the threshold
are skipped by fpart and printed as pseudo-partition records
(`S<TAB>size<TAB>./path`). CatSync parses those records and **fails the
run** if a skipped file has no other planned backend owner — a large file
is never silently dropped. (Files owned by rsync may additionally be copied
by fpsync's internal rsync when they differ; stages run sequentially, so
writers never overlap, and the rsync stage then finds them up to date.)

### mscp: new large files only

mscp has no delta mode and no remote-to-remote mode, so auto mode uses it
only for large files **confirmed absent** at the destination. Existing or
uncertain large files always belong to rsync. Each mscp task runs with
`-W CHECKPOINT`; resume uses `-R CHECKPOINT` with the same working
directory and the same SSH options. Checkpoints are retained until
CatSync's own verification gate passes — mscp exiting zero is *not* the
completion criterion — and only then removed.

### rsync: the conservative fallback

`rsync --archive --partial --partial-dir=.catsync-rsync-partial --relative
--files-from=- --from0 -e ssh SRC/ DEST` with the file list on
NUL-delimited stdin. Never `--delete`, never `--whole-file` (delta updates
stay enabled), never `--inplace` (atomic rename into place stays enabled).

## Verification

- `--verify none`: a task completes when its backend exits zero and the
  source file is unchanged before and after the transfer.
- `--verify size` (default): after all stages, one batched remote manifest
  is collected and every transferred file's remote size is compared with
  the source size. A mismatch fails the task and the run. **Size equality
  is not content proof.**
- `--verify sha256`: **not implemented in this MVP.** It fails fast with a
  clear error instead of silently degrading to a size check.

## Examples

Linux VPS → Linux VPS (run on the source VPS):

```sh
catsync sync /srv/data/ deploy@203.0.113.10:/srv/data
```

IPv6-only VPS → IPv6-only VPS:

```sh
catsync sync --ipv6 /srv/data/ deploy@[2001:db8::10]:/srv/data
```

macOS → Linux VPS (small files parallelized, 8 jobs × 2 streams):

```sh
catsync sync --jobs 8 --streams 2 --large-threshold 1GiB ~/Pictures deploy@backup.example.com:/srv/pictures
```

Dry-run and plan:

```sh
catsync plan /srv/data/ deploy@203.0.113.10:/srv/data
catsync sync --dry-run /srv/data/ deploy@203.0.113.10:/srv/data
```

Resume and status:

```sh
catsync status                      # latest run
catsync resume                      # latest resumable run
catsync resume 20260101T120000Z-0123456789abcdef
catsync status 20260101T120000Z-0123456789abcdef --verbose
```

Forced backends (must preserve the safety policy or fail):

```sh
catsync sync --backend rsync /srv/data/ deploy@203.0.113.10:/srv/data
catsync sync --backend fpsync --large-threshold 1GiB /srv/small-files/ deploy@203.0.113.10:/srv/small-files
catsync sync --backend mscp /srv/isos/ deploy@203.0.113.10:/srv/isos   # only if none exist remotely
```

## Security notes

- All subprocesses run through `exec.CommandContext` with argument arrays;
  no local shell ever parses a command line. File lists travel on
  NUL-delimited stdin. Remote paths in SSH scripts are single-quote
  escaped.
- SSH host-key checking is never disabled; CatSync passes no
  `StrictHostKeyChecking` overrides. Configure `~/.ssh/config` as usual.
- No destination path ever has two backend writers: the plan is disjoint
  and stages execute sequentially.
- There is no deletion: no `--delete`, no fpsync `-E`, no remote cleanup.
- Credentials and private keys are never read or printed; the
  `CATSYNC_SSH`/`CATSYNC_RSYNC`/`CATSYNC_FPSYNC`/`CATSYNC_FPART`/
  `CATSYNC_MSCP` environment variables only override program *paths* for
  diagnostics and tests, and environment values never appear in logs.

## Known limitations

- **Mutating source tree**: CatSync does not snapshot the source. Files are
  re-stat'd immediately before and after their transfer, and a changed file
  fails its task — but a consistent point-in-time copy of a live tree is
  not promised. Sync quiescent trees or snapshot first.
- **Symlinks and special files** are reported as unsupported, never
  transferred or followed.
- **fpsync sees the whole tree**: files it does not own may be copied by
  its internal rsync when they differ (sequential stages keep this safe),
  and small files created *during* a run are copied but untracked.
- **mscp** transfers new files only; a modified large file always falls to
  rsync. mscp runs where the source lives — no remote-to-remote copies.
- **fpsync transport options** (e.g. IPv6-only) cannot be forced through
  fpsync; it uses rsync's defaults.
- **Progress reporting** is coarse: stage start/finish lines and the tools'
  own output. There is no aggregate byte-progress percentage; fpsync output
  is preserved in a log file inside the state directory for inspection.
- **Manifest buffering**: the remote manifest is buffered in memory during
  collection (tens of bytes per remote file).
- **sha256 verification** is not implemented in this MVP.
- Remote destinations must run a POSIX shell; the optimized manifest
  additionally needs GNU find, with a conservative rsync-only fallback
  otherwise.
