# Releasing CatSync

CatSync publishes versioned archives on GitHub Releases and a formula in
[dotfrankruan/homebrew-catsync](https://github.com/dotfrankruan/homebrew-catsync).
The release workflow runs the full check suite before publishing anything.

## Before tagging

- [ ] `main` is green in CI.
- [ ] For a direct-lifecycle change, real SSH acceptance passes:

```bash
CATSYNC_E2E_HOST=catsync-test@example.invalid \
python3 scripts/tests/real-ssh-handoff.py --binary ./bin/catsync \
  --remote-catsyncd /home/catsync-test/.local/bin/catsyncd \
  --out docs/assets/benchmarks/lifecycle.json

CATSYNC_E2E_HOST=catsync-test@example.invalid \
python3 scripts/tests/worker-timeout.py --binary ./bin/catsync \
  --remote-catsyncd /home/catsync-test/.local/bin/catsyncd \
  --out docs/assets/benchmarks/cleanup.json
```

Both scripts assert a matching SHA-256, no second bootstrap SSH, and a
disappeared one-shot worker. Do not tag if either fails.

## Tag the release

```bash
git tag -a vX.Y.Z -m "vX.Y.Z"
git push origin vX.Y.Z
```

The tag starts [`.github/workflows/release.yml`](../.github/workflows/release.yml), which:

1. runs `make check` on the tagged commit;
2. builds `linux/amd64`, `linux/arm64`, `darwin/amd64`, and `darwin/arm64`
   archives containing both `catsync` and `catsyncd`;
3. verifies each archive and `SHA256SUMS`;
4. creates the GitHub Release with generated notes.

Re-running the workflow for an existing tag replaces the assets.

## Verify the release

```bash
gh run list --workflow=release.yml
gh release view vX.Y.Z
gh release download vX.Y.Z --pattern SHA256SUMS --clobber
sha256sum -c SHA256SUMS
```

`make release VERSION=vX.Y.Z` builds the same local archives if you need to
inspect them before tagging.

## Update the Homebrew tap

The tap is a separate repository and is intentionally not published
automatically. After the GitHub Release exists:

```bash
make release VERSION=vX.Y.Z
make brew-formula VERSION=vX.Y.Z > /tmp/catsync.rb
```

In a clone of `dotfrankruan/homebrew-catsync`:

```bash
cp /tmp/catsync.rb Formula/catsync.rb
brew style Formula/catsync.rb
brew audit --strict dotfrankruan/catsync/catsync
brew test dotfrankruan/catsync/catsync
git commit -am "catsync vX.Y.Z"
git push   # only when you intend to publish the tap
```

The rendered formula installs and tests both binaries. The tap is currently on
v0.2.0 and expects every archive to include `catsyncd`.

## Smoke test from the user side

On a clean machine, or after uninstalling the local package:

```bash
brew install dotfrankruan/catsync/catsync
catsync version
catsync doctor
```

The fully-qualified install auto-taps the repository. Installing by short name
after an explicit `brew tap` requires `brew trust dotfrankruan/catsync`, which
trusts the tap as a whole.
