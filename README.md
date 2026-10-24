# CatSync

*Because one TCP connection is never enough.*

CatSync is a local orchestration CLI for moving large directory trees in
either direction between the local machine and a remote host over SSH. It
discovers the source tree, collects destination metadata, assigns every file exactly
one backend in an immutable **disjoint plan**, and drives battle-tested
external tools to move the data in parallel:

- **native parallel rsync** for the small-file stage: CatSync batches
  small files deterministically and runs up to `--jobs` rsync processes
  concurrently (no external fpsync/fpart),
- **native multistream** for large files confirmed new at the destination:
  several system-ssh SFTP sessions per file copy disjoint byte ranges into
  one same-directory temporary object, with an atomic checkpoint per chunk,
  then a no-clobber hardlink publish on the receiving side (no external mscp),
- **rsync** for everything else, as the conservative correctness fallback
  with delta transfer.

The remote host needs only SSH/SFTP, rsync, and GNU findutils; no CatSync
daemon runs remotely. Native uploads additionally need the
`hardlink@openssh.com` SFTP extension (OpenSSH ≥ 9.0) and GNU coreutils for
exact nanosecond metadata; native downloads publish locally and do not need
the remote hardlink extension.

CatSync is licensed under the AGPL-3.0-only; see [LICENSE](LICENSE).

## Architecture

```
catsync (local orchestrator)
  │ 1. stream source discovery (local Lstat or one remote GNU-find scan)
  │ 2. collect destination metadata (remote manifest or local Lstat walk)
  │ 3. plan: assign every relative path exactly one backend
  │ 4. persist the run atomically (state dir) before transferring
  │ 5. execute stages sequentially, so writers never overlap:
  │      native parallel-rsync (small files) → native multistream
  │      (new large files) → rsync (rest)
  │ 6. sync only: checksum-based full-tree archive reconciliation,
  │      with destination-only deletion delayed until transfers succeed
  │ 7. verify completed tasks (size plus optional remote SHA-256)
  │ 8. mark tasks completed; only then retire native checkpoints
  └ all subprocesses are exec'd with argument arrays — no local shell
```

A run is a versioned JSON document in the state directory (default
`$XDG_STATE_HOME/catsync` or `~/.local/state/catsync`), written
temp-file-plus-rename with fsync. State writes are part of correctness: a
task is marked running on disk *before* its subprocess starts (a failed save
aborts the launch), completion is persisted before a native checkpoint is
retired, and checkpoint references are cleared before their files are
removed, so interrupted cleanup leaves only a harmless orphan. A destination-scoped lock file prevents
two runs from writing the same destination tree at once. Interrupted runs are
recovered explicitly: `running` tasks return to `pending` and `resume`
revalidates and continues them.

## External dependencies

| Tool | Required | Purpose |
| --- | --- | --- |
| `rsync` | **yes** | conservative transfer backend; the native small-file stage also drives it, one process per batch |
| `ssh` | **yes** | remote transport, manifest, mkdir, and the native multistream large-file backend (system OpenSSH SFTP subsystem) |

No other external tool is needed: no fpsync, no fpart, no mscp.

CatSync does **not** use external `fpsync` or `fpart`; the small-file stage
is native. The remote host needs GNU findutils for the optimized manifest
collection (`find -printf`). If manifest collection fails — no GNU find, no
directory yet handled separately, SSH failure — auto mode plans
**conservatively**: every transferable file routes to rsync, and CatSync
says so. A missing destination directory is detected remotely (the script
exits 42) and counts as an authoritative *empty* destination, so first-time
syncs still use the native small-file and multistream stages.

`catsync doctor` checks rsync and ssh and reports versions; nothing else is
a runtime dependency.

## Build and install

```sh
make build     # writes bin/catsync with the git version baked in
make test      # go test ./...
make check     # gofmt, go vet, go test -race
make cross     # dist/ binaries: linux/amd64, linux/arm64, darwin/arm64
make clean     # remove bin/ and dist/
```

Install with `go install github.com/dotfrankruan/CatSync/cmd/catsync@latest`
or copy `bin/catsync` into your `PATH`.

## Commands

### `catsync doctor`

Probes rsync and ssh with bounded version probes; both are required. No
secrets or environment contents are printed.

### `catsync plan SOURCE DESTINATION`

Discovers the source, collects the remote manifest (or announces the
conservative fallback), runs the planner, and prints the summary: per-backend
counts, skipped/unsupported counts, and the logical source byte volume (a
source-side sum, **not** a network-traffic estimate). Nothing is
transferred and no run state is written. Unsafe forced-backend plans (for
example `--backend fpsync` with a large file) exit nonzero. `--backend
fpsync` is kept as a compatibility name; it selects the native
parallel-rsync small-file backend and never invokes external fpsync/fpart.

### `catsync copy SOURCE DESTINATION`

Executes the durable parallel transfer lifecycle without deleting anything
that exists only at the destination. This is the historical CatSync behavior
and the safe choice for additive backup/copy jobs.

### `catsync sync SOURCE DESTINATION`

Executes the same optimized transfer stages, then runs a final whole-tree
rsync archive reconciliation with checksums and delayed deletion. It preserves
directories, symlinks, hard links, ACLs, and extended attributes and removes
destination-only entries, making the destination an exact archive mirror of
the source when the source stays stable. The destructive reconciliation runs
only when every preceding transfer stage succeeded. Nonzero exit on any
transfer, reconciliation, or verification failure. A run
interrupted with Ctrl-C stays resumable. Exactly one operand must be remote:
`LOCAL [user@]host:/path` uploads and `[user@]host:/path LOCAL` downloads;
remote-to-remote and local-to-local forms are rejected.

### `catsync resume [RUN_ID]`

Resumes the named run, or the latest resumable run without an argument.
Reacquires the destination lock, revalidates source metadata (a changed
source file fails its task instead of transferring), rebuilds small-file
batches deterministically from pending tasks (legacy fpsync run IDs and
state directories in old runs are ignored and retired on completion),
resumes large files from their native multistream checkpoints, and reuses
rsync's partial directory. Retained checkpoints survive failed resumes.

### `catsync status [RUN_ID]`

Prints a run without mutating it: status, source/destination, timestamps,
configuration, per-status and per-backend task counts, retained checkpoint
count, and recorded errors. Legacy fpsync run IDs on old runs are shown for
diagnostics only. Missing or corrupt state exits nonzero.

### `catsync version`

Prints the version.

### Options

| Flag | Default | Meaning |
| --- | --- | --- |
| `--backend` | `auto` | `auto`, `fpsync` (native parallel-rsync; does not invoke fpsync/fpart), `mscp` (native multistream; never invokes external mscp), or `rsync` |
| `--dry-run` | off | transfer nothing; `sync` additionally runs rsync's itemized mirror preview, including deletions |
| `--verify` | `auto` | `auto`, `none`, `size`, or `sha256` (see verification policy below) |
| `--progress` | `auto` | `auto` (interactive terminal only), `always` (log snapshots), or `never` |
| `--jobs` | 4 | max concurrent rsync batches (small-file stage) or concurrent large-file transfers |
| `--streams` | 4 | SFTP sessions per large-file transfer |
| `--large-threshold` | `512MiB` | size at or above which a file counts as large |
| `--ipv6` | off | restrict ssh, rsync transport, and the native large-file backend to IPv6 |
| `--state-dir` | XDG state | run state directory |
| `--quiet` / `--verbose` | off | output control |

Options may appear before, between, or after the operands — `catsync sync
--jobs 8 SRC DEST` and `catsync sync SRC DEST --jobs 8` are equivalent — and
`--` ends option parsing. Raw `--delete` is rejected: select the complete
safety policy explicitly with `copy` (never delete) or `sync` (exact mirror).

The aggregate progress display combines concurrent rsync batches and native
multistream workers. It reports percentage, transferred logical bytes,
current smoothed speed, elapsed time, ETA, ETD (estimated local completion
clock time), and the active stage. It redraws one line on a terminal and is
silent when redirected unless `--progress always` is selected.

**SSH connection budget.** A run's estimated worst-case connection count is
`jobs × streams` (each of up to `jobs` large-file transfers holds `streams`
SFTP sessions; the native small-file stage uses up to `jobs` rsync+ssh
processes; the manifest and verification phases use one). Configurations
with `jobs × streams > 64` are rejected, so a typo cannot open hundreds of
connections.

## Backend selection

Auto mode assigns every regular file exactly one owner:

| Source / destination state | Backend |
| --- | --- |
| unchanged size and mtime (hint only) | skipped — no transfer |
| new file below the threshold | native parallel-rsync batches (backend name `fpsync` for compatibility) |
| new file at or above the threshold | native multistream (backend name `mscp` for compatibility) |
| existing, modified, or state unknown | rsync |
| symlink, special file, or scan error | unsupported — recorded distinctly, never followed |

### Unsupported entries

Symlinks, special files (devices, FIFOs, sockets), and entries whose scan
failed are recorded with the task status **unsupported**, which is distinct
from `skipped` and `failed`. In `copy`, they are never followed or written;
the final summary and `status` report them prominently. In `sync`, the final
full-tree rsync reconciliation owns entries outside the optimized regular-file
plan. If that reconciliation succeeds they are recorded completed; if rsync
cannot reproduce one because of platform or privilege limits, the entire sync
fails instead of claiming an exact mirror.

Size and mtime are planning *hints*, not proof of content equality; the
verification stage is the completion gate. Forced backends must preserve
the safety policy: `--backend mscp` rejects files that exist at the
destination or whose destination state is unknown, and `--backend fpsync`
rejects files at or above the threshold.

### Native small-file batching (replaces external fpsync/fpart)

CatSync packs the new small files (size < threshold) into deterministic
disjoint batches with a pure-Go next-fit partitioner (`internal/partition`,
max 2000 files and at most `threshold` logical bytes per batch; a batch's
byte sum never overflows). Then it runs up to `--jobs` rsync processes
concurrently, each receiving exactly its own batch as a NUL-delimited
`--files-from=-` list with `--relative --no-implied-dirs`.

Because batches share destination parent directories and `--relative` would
otherwise let each rsync update those parents' attributes concurrently,
CatSync creates receiving-side parents before launching any batch: one
batched remote `mkdir -p` for uploads, or component-by-component local
creation for downloads (rejecting symlink/non-directory parents). It then
runs the batches with `--no-implied-dirs` so rsync never creates or
attribute-updates an implied directory. This is verified with real rsync
3.5.1 runs: nested trees copy correctly, shared parents keep their
attributes, and two concurrent batches into one parent do not interfere.

Per batch, CatSync revalidates every source file right before launch (local
`Lstat` for uploads, a batched remote GNU-find manifest for downloads),
persists the whole batch's running intent in ONE atomic save (a failed save
blocks the launch and leaves every task pending, never a partial commit),
and re-checks the sources after rsync exits. Cancellation is honored at
every boundary: before a batch starts, during its pre-checks, before the
save, and between the save and the launch (where the batch reverts to
pending in one save and no process starts). The first unrecoverable batch
failure cancels the remaining in-flight batches and stops launching new
ones; cancelled batches' tasks revert to pending with a resumable note. Resume rebuilds batches deterministically from the
pending tasks — no fpsync run IDs or state directories are involved — and
rsync's per-file partial files survive batching boundaries because the
partial file lives next to the destination file, not per batch.

### Native multistream: new large files only

The native large-file backend has no delta mode and no remote-to-remote
mode, so auto mode uses it only for large files **confirmed absent** at the
destination. Existing or uncertain large files always belong to rsync.

An upload works like this:

1. The source is stat'ed (regular file, size, nanosecond mtime) and a
   deterministic chunk plan is built (16 MiB chunks).
2. An atomic local checkpoint (JSON, temp-file + fsync + rename + directory
   fsync, mode 0600, parent directories 0700) binds the schema version,
   source identity, destination identity, chunk size, the completed chunk
   set, and the unpredictable same-directory remote temp path — before any
   remote write.
3. `streams` system-ssh SFTP sessions are established **sequentially**, so
   interactive password/PIN/host-key prompts never pile up.
4. The temp object is created exclusively (`O_CREATE|O_EXCL`, mode 0600),
   truncated to the exact size, and synced; each session opens its own
   `O_WRONLY` handle.
5. Chunks are copied by range; after each chunk's full write the checkpoint
   is saved atomically. A checkpoint save failure cancels the copy
   immediately rather than claiming work that is not durably recorded.
6. After all chunks: writers sync and close, the source is re-validated
   (size and nanosecond mtime unchanged), permissions and the exact
   nanosecond mtime are applied to the temp (via a controlled ssh command —
   SFTP v3 carries only whole seconds), and the final path is published by
   a **no-clobber hardlink** (`hardlink@openssh.com`): if the final path
   exists in any form, the publish fails and nothing is overwritten. The
   final's size and mtime are verified.
7. Only after the task's completed state is durably saved is the local
   checkpoint retired, and only then is the remote temp removed (a temp
   removal failure is logged, never rolled back).

Resume never re-creates or re-truncates the temp: it validates it against
the checkpoint, skips every completed chunk, and writes only the pending
ranges. If the temp is missing but no chunk was ever completed, the
transfer restarts fresh. The crash window between a successful hardlink and
a recorded completion is recovered by proving final and temp are the same
inode (`test temp -ef final` over a controlled ssh command); a final that
cannot be proven to be the same object is a hard conflict and is never
overwritten or deleted.

The server needs OpenSSH ≥ 9.0's sftp-server (fsync and hardlink
extensions) and GNU coreutils on the remote. Auto mode probes the hardlink
capability before any remote write: a server that cleanly reports the
extension missing lets **never-started** large files fall back to the
conservative rsync stage, while a forced `--backend mscp` run fails with a
clear error. The fallback is deliberately restricted — a task that already
has a native or legacy checkpoint (or whose checkpoint state cannot be
proven clean) fails closed instead: rsync would bypass the no-clobber
publish and could overwrite a final created while the run was interrupted,
so the backend, checkpoint, temp object, and final path are all left
untouched. A probe *error* (as opposed to a clean "unsupported" answer)
never triggers a fallback; every task fails with the probe's cause.
`--backend mscp` is a compatibility name: it selects this native backend
and never executes an external mscp.

A download uses the same deterministic chunk plan, atomic checkpoint, fixed
worker memory, sequential system-ssh SFTP session startup, per-chunk
durability barrier, cancellation, and resume rules in the opposite
direction. Each SFTP session opens its own read handle on the exact remote
regular file; workers write ranges into an exclusive, pre-sized local temp
next to the final path. The remote source's size and **nanosecond** mtime are
checked against the immutable plan before copying and checked again after
all readers close. CatSync then applies mode and exact mtime locally and
publishes with `link(2)`, never rename, so an existing local final is not
overwritten. Completion is persisted before the extra local temp hardlink
is removed. Resume validates the remote source, checkpoint, local temp, and
destination binding and skips completed chunks. Native downloads need the
SFTP subsystem and GNU findutils, but not the remote hardlink extension or
GNU `touch`.

Legacy checkpoints written by the old external-mscp backend (binary,
relative-path format) are detected and fail closed: they are never parsed
as native checkpoints, the possibly partial destination is never touched,
and the task reports that the run must be recovered with an older catsync
release or inspected manually.

### rsync: the conservative fallback

Ordinary copy stages use `rsync --archive --partial --partial-dir=.catsync-rsync-partial --relative
--files-from=- --from0 -e ssh SRC/ DEST` with the file list on
NUL-delimited stdin. Never `--delete`, never `--whole-file` (delta updates
stay enabled), never `--inplace` (atomic rename into place stays enabled).

Only `catsync sync` adds a final full-tree reconciliation using
`--archive --checksum --hard-links --acls --xattrs --partial --delete-delay`. It has no
per-file filter, so it also represents empty directories, symlinks, and
directory metadata. Unlike ordinary stages it does not protect a CatSync
partial directory, which could otherwise survive as an extra destination
entry. `--delete-delay` defers destination-only removals until
rsync has completed its transfer work. The reconciliation intent and result
are persisted, so an interrupted or failed mirror remains resumable even if
all optimized per-file tasks already finished.

## Verification

- `--verify none`: a task completes when its backend exits zero and the
  source file is unchanged before and after the transfer.
- `--verify size`: after all stages, CatSync scans the receiving
  side (one remote manifest for uploads, one local walk for downloads) and
  compares every transferred file's size with the source size. A mismatch
  fails the task and the run. **Size equality is not content proof.**
- `--verify auto` (default): first performs the size check, then asks the
  remote endpoint to compute SHA-256 using the first available utility from
  `sha256sum`, `shasum`, `openssl`, or `busybox sha256sum`; CatSync computes
  the local endpoint's digest itself and compares the two. If the remote has
  none of those tools, the run is allowed to complete using size verification
  only and prints a prominent warning **after** the completion line saying
  that content verification was not performed and that an administrator must
  install a supported remote utility to enable it.
- `--verify sha256`: uses the same size-then-SHA-256 gate, but fails the run
  when the remote endpoint has no supported SHA-256 utility.

CatSync never downloads a remote file to hash it locally. Remote content is
read only by the remote hashing utility, and SSH returns only NUL-delimited
path/digest records. There is intentionally no readback fallback: install one
of the supported utilities on the remote endpoint if content verification is
required.

## Examples

Exact Linux VPS → Linux VPS mirror (run on the source VPS; deletes extras):

```sh
catsync sync /srv/data/ deploy@203.0.113.10:/srv/data
```

Non-deleting copy:

```sh
catsync copy /srv/data/ deploy@203.0.113.10:/srv/data
```

IPv6-only VPS → IPv6-only VPS:

```sh
catsync sync --ipv6 /srv/data/ deploy@[2001:db8::10]:/srv/data
```

macOS → Linux VPS (small files parallelized, 8 jobs × 2 streams):

```sh
catsync sync --jobs 8 --streams 2 --large-threshold 1GiB ~/Pictures deploy@backup.example.com:/srv/pictures
```

Linux VPS → macOS/Linux (remote-to-local; the operand order selects download):

```sh
catsync sync --jobs 4 --streams 2 deploy@backup.example.com:/srv/archive ~/Downloads/archive
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
catsync sync --backend mscp deploy@203.0.113.10:/srv/isos ~/Downloads/isos   # only if none exist locally
```

## SSH authentication and Smallstep certificates

CatSync deliberately delegates authentication to the **system `ssh`**. Every
manifest command, rsync transport, SFTP session, and metadata command inherits
the invoking process's environment, including `SSH_AUTH_SOCK`, and uses normal
`~/.ssh/config`, agent, `known_hosts`, `ProxyJump`, `ControlMaster`, PKCS#11,
and security-key behavior. CatSync does not add `-i`, does not set
`IdentitiesOnly=yes`, does not parse private keys, and does not require an SSH
certificate to exist as a file.

This directly supports Smallstep's agent-only flow:

```sh
step ssh login your-name                 # places the short-lived identity/certificate in ssh-agent
ssh user@host true                       # optional: prove the same shell can authenticate
catsync sync ./data user@host:/srv/data  # inherits that same SSH_AUTH_SOCK
```

Run `step ssh login` and CatSync from an environment connected to the same
agent. Avoid launching CatSync through `sudo` unless the agent socket and its
permissions are deliberately preserved. CatSync never invokes `step` or
refreshes certificates itself; if a short-lived certificate expires while a
run is opening new SSH sessions, authenticate again and use `catsync resume`.

## Security notes

- All subprocesses run through `exec.CommandContext` with argument arrays;
  no local shell ever parses a command line. File lists travel on
  NUL-delimited stdin. Remote paths in SSH scripts are single-quote
  escaped.
- SSH host-key checking is never disabled; CatSync passes no
  `StrictHostKeyChecking` overrides. Configure `~/.ssh/config` as usual.
- SSH credentials may live only in an inherited agent. CatSync neither
  requires nor manufactures on-disk identity/certificate files.
- No destination path ever has two writers at once: the plan assigns one
  owner per file, stages (native small-file batches, mscp, rsync) run
  strictly sequentially, and each small-file batch owns a disjoint file
  set, so concurrent rsync batches never touch the same file. Shared
  destination parents are pre-created and transferred with
  `--no-implied-dirs`, so concurrent batches never race on directory
  metadata either.
- `copy` never deletes destination-only data. `sync` deliberately performs
  checksum-based archive reconciliation with `--delete-delay`, but only after
  all preceding transfer stages succeed. Raw `--delete` is not accepted.
- CatSync itself never reads or prints credentials or private keys; no
  environment value is ever printed. System ssh remains responsible for its
  ordinary configured key and agent access. See the override scope table below.

### Program overrides (diagnostics and tests)

The `CATSYNC_*` environment variables override paths of programs CatSync
executes itself. They never change tool behavior, and their values are never
printed.

| Variable | Effective scope |
| --- | --- |
| `CATSYNC_SSH` | Every ssh invocation CatSync starts: remote manifest, batched mkdir, native multistream SFTP sessions, and the controlled metadata commands, **and** rsync's `-e` transport. Must be one token without whitespace or control characters (rsync splits `-e` on whitespace); `--ipv6` appends `-6`. Invalid values are a usage error. |
| `CATSYNC_RSYNC` | The rsync processes CatSync starts (conservative stage and every native small-file batch). |

`CATSYNC_FPSYNC`, `CATSYNC_FPART`, and `CATSYNC_MSCP` no longer exist:
CatSync never runs or probes those tools.

## Known limitations

- **Mutating source tree**: CatSync does not snapshot the source. Files are
  re-stat'd immediately before and after their transfer, and a changed file
  fails its task — but a consistent point-in-time copy of a live tree is
  not promised. Sync quiescent trees or snapshot first.
- **Symlinks, special files, and scan-error entries** are unsupported by the
  optimized per-file planner and remain untransferred in `copy`. A successful
  final `sync` reconciliation delegates the full tree to rsync, which handles
  directories, symlinks, and supported special files; platform privileges
  still determine whether device/special metadata can be reproduced.
- **Native multistream** transfers new files only in either local↔remote
  direction; a modified large file always falls to rsync. Remote-to-remote
  copies are not supported.
- **Progress totals are logical transfer bytes**, aggregated from successful
  rsync and native multistream writes. Delta transfer, already-completed
  resume chunks, checksum-only mirror work, metadata, and deletion do not map
  one-to-one to network bytes, so ETA/ETD are live estimates and may jump at
  stage boundaries.
- **Manifest buffering**: the remote manifest is buffered in memory during
  collection (tens of bytes per remote file).
- **Hash-tool availability**: default `--verify auto` falls back to size-only
  verification when the remote endpoint has no supported SHA-256 utility and
  prints a prominent post-completion warning. Use `--verify sha256` when this
  absence must fail the run. CatSync never downloads remote file contents as
  a verification fallback.
- Remote endpoints must run a POSIX shell and GNU findutils. Upload
  destination-manifest failure has a conservative rsync-only fallback;
  remote source discovery for a download cannot safely fall back and fails.
  Native uploads additionally need an OpenSSH ≥ 9.0 sftp-server (fsync and
  hardlink extensions) and GNU coreutils (nanosecond `touch`); native
  downloads use SFTP reads and a local no-clobber hardlink publish.
