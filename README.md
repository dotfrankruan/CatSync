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
| Local | Linux (amd64, arm64), macOS (arm64). Builds from source anywhere Go runs; `make cross` produces the three release binaries. |
| Remote | Any POSIX host with `rsync`, an SSH server with the SFTP subsystem, and GNU findutils. |

## Requirements

| | |
| --- | --- |
| Local | Go 1.23+ to build. At runtime only `ssh` and `rsync`. |
| Remote | `rsync`, `sshd` with `sftp-server`, and GNU findutils. Native uploads additionally need OpenSSH ≥ 9.0 (fsync and `hardlink@openssh.com`) and GNU coreutils; native downloads need only SFTP and GNU findutils. |
| Not required | No remote daemon and no extra tooling beyond the two programs above. |

`catsync doctor` verifies the local tools and prints their versions.

## Quick start

```sh
make install            # builds and installs to ~/.local/bin/catsync
catsync doctor          # check ssh and rsync

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
make cross    # dist/ binaries: linux/amd64, linux/arm64, darwin/arm64
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
  checkpoints, resume, and state model work.
- [third_party/](third_party/) — licenses of the linked Go modules (the main
  license is [AGPL-3.0-only](LICENSE)).
