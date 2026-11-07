# CatSync usage

CatSync moves files between one local machine and one remote host over SSH, in
either direction. This page is the complete user guide: commands, options,
backend selection, verification, authentication, and troubleshooting. For how it
works internally see [architecture.md](architecture.md).

## Contents

- [Installation](#installation)
- [Commands](#commands)
- [Sources and destinations](#sources-and-destinations)
- [Interruptions and resume](#interruptions-and-resume)
- [Expired certificates and re-authentication](#expired-certificates-and-re-authentication)
- [Options](#options)
- [Backend selection](#backend-selection)
- [Verification](#verification)
- [Examples](#examples)
- [SSH authentication and Smallstep certificates](#ssh-authentication-and-smallstep-certificates)
- [Security notes](#security-notes)
- [Program overrides](#program-overrides)
- [Troubleshooting](#troubleshooting)
- [Known limitations](#known-limitations)

## Installation

Homebrew, on macOS and Linux:

```sh
brew install dotfrankruan/catsync/catsync
```

The formula lives in
[dotfrankruan/homebrew-catsync](https://github.com/dotfrankruan/homebrew-catsync);
`dotfrankruan/catsync` is the tap name, which Homebrew maps to that repository.
The fully-qualified install auto-taps it and needs no trust step. If you tap
first and then install by short name, Homebrew asks you to trust the tap once:

```sh
brew tap dotfrankruan/catsync
brew trust dotfrankruan/catsync   # trusts the whole tap
brew install catsync
```

Prebuilt archives for linux/amd64, linux/arm64, darwin/amd64, and darwin/arm64
are on the [releases page](https://github.com/dotfrankruan/CatSync/releases),
each tag with a `SHA256SUMS` file. From source (Go 1.23+):

```sh
make install        # -> ~/.local/bin/catsync
```

or `go install github.com/dotfrankruan/CatSync/cmd/catsync@latest`.

## Commands

### `catsync doctor`

Probes `rsync` and `ssh` with bounded version probes; both are required. No
secrets or environment contents are printed.

### `catsync plan SOURCE DESTINATION`

Discovers the source, collects destination metadata (or announces the
conservative fallback), runs the planner, and prints the summary: per-backend
counts, skipped/unsupported counts, the logical source byte volume (a source-side
sum, **not** a network-traffic estimate), and whether an interrupted run of the
same job would be resumed. Nothing is transferred and no run state is written.
Unsafe forced-backend plans (for example `--backend fpsync` with a large file)
exit nonzero.

### `catsync copy SOURCE DESTINATION`

Executes the durable parallel transfer without deleting anything that exists
only at the destination. This is the safe choice for additive backup/copy jobs.

### `catsync sync SOURCE DESTINATION`

Executes the same optimized transfer stages, then runs a final whole-tree rsync
archive reconciliation with checksums and delayed deletion, making the
destination an exact archive mirror of the source when the source stays stable.
The destructive reconciliation runs only when every preceding transfer stage
succeeded. Nonzero exit on any transfer, reconciliation, or verification failure.

### `catsync resume [RUN_ID]`

Resumes the named run, or the latest resumable run without an argument.
Reacquires the destination lock, revalidates source metadata (a changed source
file fails its task instead of transferring), rebuilds small-file batches from
pending tasks, resumes large files from their checkpoints, and reuses rsync's
partial directory. Retained checkpoints survive failed resumes.

`--jobs` and `--streams` may change the parallelism of the resumed run, for
example after moving to a faster network:

```sh
catsync resume --streams 16                 # latest resumable run
catsync resume --jobs 2 --streams 8 RUN_ID
```

This is safe because the chunk plan, temporary path, and checkpoint are
independent of parallelism: transferred chunks are never re-sent, the same
temporary object continues, and the new values are persisted for later resumes.
Only these two options are accepted, because the rest of a run's strategy is
recorded and immutable.

### `catsync status [RUN_ID]`

Prints a run without mutating it: status, source/destination, timestamps,
configuration, per-status and per-backend task counts, retained checkpoint
count, and recorded errors. Fields recorded by older releases on old runs are
shown for diagnostics only. Missing or corrupt state exits nonzero.

### `catsync version`

Prints the version.

## Sources and destinations

Exactly one operand must be remote: `LOCAL [user@]host:/absolute/path` uploads
and `[user@]host:/absolute/path LOCAL` downloads. Remote paths must be absolute;
bracketed IPv6 literals are accepted. Remote-to-remote and local-to-local
transfers are rejected.

A source may name **one regular file** instead of a directory tree. The copy is
point-to-point: the file keeps its base name at the destination directory, and a
file at or above `--large-threshold` still uses the native multistream backend,
so a multi-gigabyte download runs over several SFTP sessions with a checkpoint
and resumability exactly like a tree transfer.

```sh
catsync plan user@host:/data/dataset.zip ./downloads
catsync copy user@host:/data/dataset.zip ./downloads
catsync copy ./dataset.zip user@host:/data
```

Both directions accept the form. The destination is always a directory; a
single-file source never renames the file, and a resumable run revalidates only
that file rather than rescanning its parent directory.

## Interruptions and resume

Ctrl-C, a dropped connection, or a failing stage leaves a **resumable run**:
`running` tasks return to `pending`, and everything already durably transferred
is kept (native large files keep a per-chunk checkpoint and their temporary
object, small files keep rsync's partial files).

There are two ways to continue, and normally you need neither in mind:

- **Automatic.** Running `copy` or `sync` again for the same job — same
  direction, source, destination, and command — resumes the interrupted run
  instead of re-planning it. The output names the run it continues.
- **Explicit.** `catsync resume` continues the latest resumable run, or the one
  you name.

```sh
catsync copy user@host:/data/dataset ./data         # interrupted
catsync copy user@host:/data/dataset ./data         # continues the same run
catsync copy --fresh user@host:/data/dataset ./data # starts over instead
```

A resumed run fast-forwards its progress bar to the bytes the checkpoints
confirm, then continues:

```
resume: continuing from 640.0 MiB of 18.3 GiB already durable (chunks confirmed by checkpoints)
[------------------------]   3.41%  640.0 MiB/18.3 GiB  ...  ETA 12:36
```

That figure can sit slightly below what the interrupted run displayed: bytes
that were written but not yet fsynced are not durable and are transferred again.
Before any stage runs, a resume also compares pending tasks with the destination
and marks a file complete when its size and nanosecond mtime already match the
plan, so a file that finished writing but was never recorded is not sent twice.

`--fresh` forces a new run, for example after rewriting the source tree or to
discard a checkpoint you no longer trust. A completed run, or one whose
endpoints differ at all, is never adopted.

## Expired certificates and re-authentication

SSH credentials can expire while a transfer runs: a short-lived certificate or
an agent identity can be valid at start and rejected minutes later. CatSync
distinguishes that class of failure (ssh "Permission denied", expired
certificate, exhausted identities) from ordinary errors and offers two options:

- **Keep the sessions you already have.** Open sessions keep transferring; an
  interrupted large file resumes from its checkpoint next time, so nothing
  already transferred is re-sent.
- **Re-authenticate and continue.** With `--on-auth-failure retry`, CatSync
  pauses on an authentication rejection, asks you to renew the credentials
  (re-run `ssh-add`, issue a fresh certificate), and retries the stage after you
  confirm. Answering no stops with the failure.

```sh
catsync copy --on-auth-failure retry SRC user@host:/dest
```

The default is `fail`: the run stops with an actionable message and stays
resumable, so `catsync resume` continues later. The prompt is only shown on a
terminal; without one the run fails instead of blocking.

## Options

| Flag | Default | Meaning |
| --- | --- | --- |
| `--backend` | `auto` | `auto`, `fpsync` (native parallel-rsync), `mscp` (native multistream), `sftp` (native single-file SFTP), or `rsync`; `fpsync`/`mscp` are kept as compatibility values |
| `--dry-run` | off | transfer nothing; `sync` additionally runs rsync's itemized mirror preview, including deletions |
| `--verify` | `auto` | `auto`, `none`, `size`, or `sha256` (see [Verification](#verification)) |
| `--progress` | `auto` | `auto` (interactive terminal only), `always` (log snapshots), or `never` |
| `--jobs` | 4 | max concurrent rsync batches (small-file stage) or concurrent large-file transfers |
| `--streams` | 4 | SFTP sessions per large-file transfer |
| `--large-threshold` | `512MiB` | size at or above which a file counts as large |
| `--ipv6` | off | restrict ssh, the rsync transport, and the native large-file backend to IPv6 |
| `--port` | ssh config | explicit SSH server port (`1..65535`), persisted for resume and applied to every SSH/rsync/SFTP path |
| `--on-auth-failure` | `fail` | `fail` (run stays resumable) or `retry` (ask to renew credentials, then retry the stage) |
| `--fresh` | off | start a new run instead of resuming an interrupted run for the same job |
| `--state-dir` | XDG state | run state directory |
| `--quiet` / `--verbose` | off | output control |

Options may appear before, between, or after the operands — `catsync sync
--jobs 8 SRC DEST` and `catsync sync SRC DEST --jobs 8` are equivalent — and
`--` ends option parsing. Raw `--delete` is rejected: select the complete safety
policy explicitly with `copy` (never delete) or `sync` (exact mirror).

`catsync resume` accepts only its own documented options (`--jobs`, `--streams`,
`--on-auth-failure`, `--state-dir`, `--progress`, `--quiet`, `--verbose`); the
rest of a resumed run's strategy is recorded and immutable.

**Parallelism.** `--jobs` bounds how many files transfer concurrently and
`--streams` bounds how many parallel SFTP sessions one large-file transfer
holds (both default to 4). The stages run sequentially, so the peak connection
count of a plan is the largest of `min(jobs, large files) × streams`,
`min(jobs, small files)` (one ssh per concurrent rsync batch), and the
conservative rsync stage. Parallelism is your choice: a single file can use many
streams (`--streams 32` with the default `--jobs 4` transfers one file over 32
sessions), and a wide plan is never refused. When the estimate exceeds the
64-connection guidance, CatSync prints one warning and proceeds. Each value is
capped at 64 on its own (`--jobs 65` or `--streams 65` is a usage error) to catch
typos.

**Progress.** The display has three parts, redrawn as one block on a terminal and
printed as snapshots when redirected with `--progress always`:

```
[============------------]  50.00%  8.2 GiB/16.4 GiB  Current Speed 28.3 MiB/s  Elapsed 04:12  ETA 04:51  ETD 17:44:10  native-large
ssh conns 7  |  files 12/40 done, 0 checked, 3 skipped
  …/data/big1.bin  4.1 GiB/9.0 GiB  21.0 MiB/s  ETA 03:58
  …/data/big2.bin  1.9 GiB/7.4 GiB   7.3 MiB/s  ETA 12:40
```

- the header aggregates every backend: percentage, transferred logical bytes,
  smoothed speed, elapsed time, ETA, ETD (estimated local completion clock time),
  and the active stage;
- the counter line shows the current SSH connection count and how many files are
  done, checked (verified), and skipped because the destination already matched;
- up to five active transfers follow, each with its own bytes, speed, and ETA.
  Native large-file and single-file SFTP transfers are per file; rsync batches
  are reported per batch with their file count, because rsync reports progress
  per invocation rather than per file.

## Backend selection

Auto mode assigns every regular file exactly one owner:

| Source / destination state | Backend |
| --- | --- |
| unchanged size and mtime (hint only) | skipped — no transfer |
| new file below the threshold | native parallel-rsync batches (backend name `fpsync` for compatibility), or native single-file SFTP transfers when rsync is not installed (backend `sftp`) |
| new file at or above the threshold | native multistream (backend name `mscp` for compatibility) |
| existing, modified, or state unknown | rsync |
| symlink, special file, or scan error | unsupported — recorded distinctly, never followed |

Size and mtime are planning *hints*, not proof of content equality; the
verification stage is the completion gate. Forced backends must preserve the
safety policy: `--backend mscp` rejects files that exist at the destination or
whose destination state is unknown, and `--backend fpsync` rejects files at or
above the threshold.

### Unsupported entries

Symlinks, special files (devices, FIFOs, sockets), and entries whose scan failed
are recorded as **unsupported**, distinct from `skipped` and `failed`. In
`copy` they are never followed or written, and the summary and `status` report
them prominently. In `sync` the final full-tree rsync reconciliation owns
entries outside the optimized regular-file plan: if it succeeds they are
recorded completed, and if rsync cannot reproduce one the entire sync fails
instead of claiming an exact mirror.

Files initially `skipped` because size and mtime matched are promoted to
`completed` after a successful checksum reconciliation, because sync has then
content-checked the whole tree rather than trusting the planning hint.

### Running without rsync

rsync is not required for a copy that only adds files: new small files go over
a single native SFTP session each (backend `sftp`) and new large files already
use native multistream, so a host with only `ssh` can copy a new tree. What
still needs rsync is updating files that already exist at the destination (delta
transfer), the mirror reconciliation of `sync`, and the parallel batches that
make many small files fast when rsync *is* installed. `catsync doctor` reports
rsync as optional and explains what it is used for; a run that needs it fails
with a clear message instead of guessing.

### Requirements on the remote host

- a POSIX shell and GNU findutils (manifest collection uses `find -printf`);
- `rsync` for the conservative stage and the native small-file batches.
  `catsync sync` additionally needs **GNU rsync** (it reconciles with
  `--acls --xattrs --hard-links`); macOS ships Apple openrsync at
  `/usr/bin/rsync`, so install GNU rsync (`brew install rsync`) and put it
  first in `PATH`. `catsync doctor` warns when the rsync in `PATH` is not GNU;
- for native **uploads**: an OpenSSH ≥ 9.0 `sftp-server` (fsync and
  `hardlink@openssh.com` extensions) and GNU coreutils (nanosecond `touch`);
- native **downloads** need the SFTP subsystem and GNU findutils, but not the
  remote hardlink extension or GNU `touch`.

If manifest collection fails (no GNU find, SSH failure), auto mode plans
conservatively: every transferable file routes to rsync, and CatSync says so. A
missing destination directory is detected remotely and counts as an authoritative
*empty* destination, so first-time syncs still use the native stages.

## Verification

- `--verify none`: a task completes when its backend exits zero and the source
  file is unchanged before and after the transfer.
- `--verify size`: after all stages, CatSync scans the receiving side (one remote
  manifest for uploads, one local walk for downloads) and compares every
  transferred file's size with the source size. A mismatch fails the task and the
  run. **Size equality is not content proof.**
- `--verify auto` (default): performs the size check, then asks the remote
  endpoint to compute SHA-256 with the first available utility from `sha256sum`,
  `shasum`, `openssl`, or `busybox sha256sum`, computes the local digest itself,
  and compares them. If the remote has none of those tools the run completes with
  size verification only and prints a prominent warning **after** the completion
  line stating that content verification was not performed.
- `--verify sha256`: the same size-then-SHA-256 gate, but it fails the run when
  the remote endpoint has no supported SHA-256 utility.

CatSync never downloads a remote file to hash it locally. Remote content is read
only by the remote hashing utility, and SSH returns NUL-delimited path/digest
records. There is intentionally no readback fallback.

## Examples

Exact Linux VPS → Linux VPS mirror (run on the source VPS; deletes extras):

```sh
catsync sync /srv/data/ deploy@203.0.113.10:/srv/data
```

Non-deleting copy:

```sh
catsync copy /srv/data/ deploy@203.0.113.10:/srv/data
```

IPv6-only:

```sh
catsync sync --ipv6 /srv/data/ deploy@[2001:db8::10]:/srv/data
```

Explicit non-default SSH port (applies to manifests, rsync, SFTP, metadata, and
verification):

```sh
catsync copy --port 2222 ./data deploy@backup.example.com:/srv/data
```

Without `--port`, port selection is left to the system `ssh`, so `Port` in
`~/.ssh/config` keeps working. The explicit port is recorded and reused by
`resume`.

Parallelism tuned per direction (small files parallelized, 8 jobs × 2 streams):

```sh
catsync sync --jobs 8 --streams 2 --large-threshold 1GiB ~/Pictures deploy@backup.example.com:/srv/pictures
```

Remote-to-local (operand order selects the direction):

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
catsync sync --backend mscp /srv/isos/ deploy@203.0.113.10:/srv/isos      # only if none exist remotely
catsync sync --backend mscp deploy@203.0.113.10:/srv/isos ~/Downloads/isos # only if none exist locally
```

## SSH authentication and Smallstep certificates

CatSync delegates authentication to the **system `ssh`**. Every manifest command,
rsync transport, SFTP session, and metadata command inherits the invoking
process's environment, including `SSH_AUTH_SOCK`, and uses normal `~/.ssh/config`,
agent, `known_hosts`, `ProxyJump`, `ControlMaster`, PKCS#11, and security-key
behavior. CatSync does not add `-i`, does not set `IdentitiesOnly=yes`, does not
parse private keys, and does not require a certificate to exist as a file.

This directly supports Smallstep's agent-only flow:

```sh
step ssh login your-name                 # places the short-lived identity/certificate in ssh-agent
ssh user@host true                       # optional: prove the same shell can authenticate
catsync sync ./data user@host:/srv/data  # inherits that same SSH_AUTH_SOCK
```

Run `step ssh login` and CatSync from an environment connected to the same agent.
Avoid launching CatSync through `sudo` unless the agent socket and its
permissions are deliberately preserved. CatSync never invokes `step` or refreshes
certificates itself; if a short-lived certificate expires while opening new SSH
sessions, use `--on-auth-failure retry` or authenticate again and resume.

## Security notes

- All subprocesses run through `exec.CommandContext` with argument arrays; no
  local shell ever parses a command line. File lists travel on NUL-delimited
  stdin. Remote paths in SSH scripts are single-quote escaped.
- SSH host-key checking is never disabled; CatSync passes no
  `StrictHostKeyChecking` overrides. Configure `~/.ssh/config` as usual.
- SSH credentials may live only in an inherited agent. CatSync neither requires
  nor manufactures on-disk identity/certificate files.
- No destination path ever has two writers at once: the plan assigns one owner
  per file, stages run strictly sequentially, and each small-file batch owns a
  disjoint file set.
- `copy` never deletes destination-only data. `sync` performs checksum-based
  archive reconciliation with `--delete-delay`, but only after all preceding
  transfer stages succeed. Raw `--delete` is not accepted.
- CatSync never reads or prints credentials or private keys; no environment
  value is ever printed.

## Program overrides

The `CATSYNC_*` environment variables override paths of programs CatSync executes
itself. They never change tool behavior, and their values are never printed.

| Variable | Effective scope |
| --- | --- |
| `CATSYNC_SSH` | Every ssh invocation CatSync starts: remote manifest, batched mkdir, native SFTP sessions, metadata commands, **and** rsync's `-e` transport. Must be one token without whitespace or control characters (rsync splits `-e` on whitespace); `--ipv6` appends `-6` and `--port` appends `-p PORT`. Invalid values are a usage error. |
| `CATSYNC_RSYNC` | The rsync processes CatSync starts (conservative stage and every native small-file batch). |

`CATSYNC_FPSYNC`, `CATSYNC_FPART`, and `CATSYNC_MSCP` no longer exist: CatSync
never runs or probes those tools.

## Troubleshooting

**A run stopped; what now?** Run the same `copy`/`sync` command again (it resumes
the same job automatically) or `catsync resume`. Nothing already durable is
re-sent.

**The progress bar went backwards after a resume?** It should not: a resume
starts at the durable byte count and prints that line. If you see a fresh run
instead, the endpoints differed or you passed `--fresh`.

**`Permission denied` after a while.** The credentials expired (typical for
short-lived certificates). Re-authenticate in the same shell/agent and resume, or
use `--on-auth-failure retry` next time.

**`--jobs`/`--streams` do not seem to matter.** For one file only `--streams`
matters; `--jobs` bounds concurrent *files*. Both are capped at 64 each, and an
estimate above 64 connections only warns.

**A large file was planned for rsync instead of the native backend.** Auto mode
uses native multistream only for large files confirmed absent at the destination;
existing or unknown destination state always falls back to rsync. `--backend
mscp` refuses such files rather than overwriting them.

**`rsync: unrecognized option '--acls'` on macOS.** The system `/usr/bin/rsync`
is Apple openrsync, which lacks GNU's ACL/xattr options that `sync` uses.
Install GNU rsync (`brew install rsync`) and make sure it comes first in
`PATH`; `catsync doctor` reports this.

**`sync` refuses to delete.** The reconciliation runs only after every transfer
stage succeeded; fix the transfer failure first.

**Where is the state?** `--state-dir`, defaulting to `$XDG_STATE_HOME/catsync` or
`~/.local/state/catsync`. Each run has `runs/<id>.json` plus checkpoints under
`checkpoints/<id>/`.

## Known limitations

- **Mutating source tree**: CatSync does not snapshot the source. Files are
  re-stat'ed immediately before and after transfer, and a changed file fails its
  task, but a consistent point-in-time copy of a live tree is not promised. Sync
  quiescent trees or snapshot first.
- **Symlinks, special files, and scan-error entries** are unsupported by the
  optimized planner and remain untransferred in `copy`. A successful final `sync`
  reconciliation delegates the whole tree to rsync, which handles directories,
  symlinks, and supported special files; platform privileges still decide whether
  device/special metadata can be reproduced.
- **Native multistream** transfers new files only; a modified large file always
  falls to rsync. Remote-to-remote copies are not supported.
- **Progress totals are logical transfer bytes** aggregated from successful
  writes. Delta transfer, already-completed resume chunks, checksum-only mirror
  work, metadata, and deletion do not map one-to-one to network bytes, so ETA/ETD
  are live estimates and may jump at stage boundaries.
- **Manifest buffering**: the remote manifest is buffered in memory during
  collection (tens of bytes per remote file).
- **Hash-tool availability**: default `--verify auto` falls back to size-only
  verification when the remote endpoint has no supported SHA-256 utility and
  prints a prominent post-completion warning. Use `--verify sha256` when this
  absence must fail the run.
- Remote endpoints must run a POSIX shell and GNU findutils. Upload
  destination-manifest failure has a conservative rsync-only fallback; remote
  source discovery for a download cannot safely fall back and fails.
