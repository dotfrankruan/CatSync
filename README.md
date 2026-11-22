# CatSync

*Because one TCP connection is never enough.*

CatSync moves large file trees — or a single file — between your machine and a
remote host over SSH, in either direction. It plans the transfer file by file,
runs small files in parallel batches and large files over several SFTP streams,
never overwrites a file that already exists at the destination, and can resume an
interrupted transfer from where it stopped. It is a single Go binary with no
daemon on the remote side.

[![CI](https://github.com/dotfrankruan/CatSync/actions/workflows/ci.yml/badge.svg)](https://github.com/dotfrankruan/CatSync/actions/workflows/ci.yml)
[![License: AGPL-3.0](https://img.shields.io/badge/license-AGPL--3.0-blue.svg)](LICENSE)

## Supported systems

| | |
| --- | --- |
| Local | Linux (amd64, arm64), macOS (amd64, arm64). Prebuilt archives are published on the [releases page](https://github.com/dotfrankruan/CatSync/releases); `make cross` builds all four locally. |
| Remote | Any POSIX host with `rsync`, an SSH server with the SFTP subsystem, and GNU findutils. |

## Requirements

| | |
| --- | --- |
| Local | Go 1.26+ to build. At runtime `ssh`, plus `rsync` for updated files and mirror sync; a copy that only adds files works with `ssh` alone. |
| Remote | `rsync`, `sshd` with `sftp-server`, and GNU findutils. Native uploads additionally need OpenSSH ≥ 9.0 (fsync and `hardlink@openssh.com`) and GNU coreutils; native downloads need only SFTP and GNU findutils. |
| Not required | No remote daemon and no extra tooling beyond the two programs above. |

`catsync doctor` verifies the local tools and prints their versions.

## Install

Homebrew (macOS and Linux):

```sh
brew install dotfrankruan/catsync/catsync
```

The formula lives in [dotfrankruan/homebrew-catsync](https://github.com/dotfrankruan/homebrew-catsync)
(`dotfrankruan/catsync` is the tap name; Homebrew maps it to that repository).
The fully-qualified install needs no extra trust step. If you prefer to tap
first and install by short name, Homebrew asks you to trust the tap once:

```sh
brew tap dotfrankruan/catsync
brew trust dotfrankruan/catsync   # trusts the tap, i.e. its formulae
brew install catsync
```

Upgrades arrive with `brew upgrade catsync`.

From a release archive:

```sh
curl -LO https://github.com/dotfrankruan/CatSync/releases/download/v0.2.0/catsync_v0.2.0_darwin_arm64.tar.gz
tar -xzf catsync_v0.2.0_darwin_arm64.tar.gz
install -m 0755 catsync_v0.2.0_darwin_arm64/catsync ~/.local/bin/catsync
```

From source: `make install` (Go 1.26+; installs `catsync` and `catsyncd`),
`go install github.com/dotfrankruan/CatSync/cmd/catsync@latest`, and separately
`go install github.com/dotfrankruan/CatSync/cmd/catsyncd@latest` if you want the
opt-in daemon transport.

## Daemon transport (opt-in)

CatSync can use a one-shot remote agent, `catsyncd`, instead of opening one
ssh data channel per SFTP stream or rsync batch:

```bash
catsync copy --transport daemon ~/src user@host:/data
# Optional QUIC diagnostics (qlog; owner-only files):
# catsync copy --transport daemon --daemon-transport quic \
#   --quic-qlog local.sqlog --remote-quic-qlog /tmp/remote.sqlog ...
```

The run starts exactly one ssh bootstrap session. Over that authenticated
channel the two sides exchange fresh in-memory ECDSA certificates, then use
pinned mutual TLS 1.3 for the data plane (SFTP streams, rsync exec, RPC).
For direct TCP and direct QUIC, the authenticated data control channel then
takes ownership: the SSH bootstrap exits and the transfer continues without
it; the remote one-shot worker exits on the final authenticated bye or a
bounded heartbeat timeout. `ssh-forward` still depends on its SSH tunnel.
An experimental QUIC data transport multiplexes all streams over one UDP
connection (`--daemon-transport quic`, ALPN `catsync-quic/1`, no 0-RTT).
The certificates and session nonces exist only for that run and are never
written to disk or state. `catsyncd` must already be installed by you on the
remote host and should be an absolute path or on the remote `PATH`; CatSync
never downloads, copies, or installs it. Default behavior without
`--transport daemon` is unchanged: plain ssh, exactly as before.

See [docs/usage.md](docs/usage.md#daemon-transport-one-shot-catsyncd) for
options, security model, resume behavior, and limitations. The lifecycle
handoff is illustrated in [docs/assets/ssh-handoff.svg](docs/assets/ssh-handoff.svg).

## Releases

Pushing a `v*` tag runs the release workflow: it re-runs the full check suite,
builds `linux/amd64`, `linux/arm64`, `darwin/amd64`, and `darwin/arm64`
archives containing `catsync` and the one-shot `catsyncd` agent, with the tag
baked in, and publishes them with `SHA256SUMS` and
generated notes on the GitHub releases page.

```sh
git tag -a v0.2.0 -m "v0.2.0"
git push origin v0.2.0
```

`make release VERSION=v0.2.0` builds the same archives locally, and
[docs/releasing.md](docs/releasing.md) lists the remaining steps: publishing the
tag and bumping the Homebrew formula.

## Quick start

```sh
make install            # builds and installs catsync and catsyncd to ~/.local/bin
catsync doctor          # check ssh and rsync

# Remote paths are always absolute (CATSync rejects relative paths and ~).
catsync plan ./data user@host:/srv/data       # what would be transferred
catsync copy ./data user@host:/srv/data       # add new/changed files, never delete
catsync sync ./data user@host:/srv/data       # exact mirror (deletes extras)

catsync copy user@host:/srv/data/archive.tar ./downloads   # single file, other direction
```

Interrupted? Run the same command again — CatSync resumes that job automatically
(`catsync resume` does the same explicitly, and `--fresh` starts over). Progress,
state, and every partial object are kept on disk until the transfer completes.

## CI/CD

GitHub Actions runs `gofmt`, `go vet`, the full test suite, the race suite, and
the cross build on Linux and macOS for every push and pull request
([`.github/workflows/ci.yml`](.github/workflows/ci.yml)). The same checks run
locally:

```sh
make test     # go test ./...
make check    # gofmt, go vet, go test -race
make cross    # dist/ binaries: linux/amd64, linux/arm64, darwin/amd64, darwin/arm64
make release  # versioned archives + SHA256SUMS, as published on a tag
```

## Acknowledgements

Earlier CatSync releases orchestrated the external `fpsync`, `fpart`, and `mscp`
tools. Those integrations have been replaced by native implementations and are no
longer included, invoked, or required; no code from those projects is part of
CatSync.

## Documentation

- [docs/usage.md](docs/usage.md) — commands, options, backend selection,
  verification, authentication, troubleshooting, limitations.
- [docs/architecture.md](docs/architecture.md) — how the transfer stages,
  checkpoints, resume, state model, and SSH lifecycle handoff work.
- [docs/benchmark.md](docs/benchmark.md) — measured transport performance,
  real SSH handoff acceptance, and known limitations.
- [third_party/](third_party/) — licenses of the linked Go modules (the main
  license is [AGPL-3.0-only](LICENSE)).
