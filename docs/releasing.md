# Releasing CatSync

CatSync ships as versioned archives on the GitHub releases page and as a
Homebrew formula in [dotfrankruan/homebrew-catsync](https://github.com/dotfrankruan/homebrew-catsync).

## 1. Tag the release

`main` must be green in CI first. Then:

```sh
git tag -a v0.1.0 -m "v0.1.0"
git push origin v0.1.0
```

`.github/workflows/release.yml` reacts to the tag: it re-runs `make check` on
exactly that commit, builds `linux/amd64`, `linux/arm64`, `darwin/amd64`, and
`darwin/arm64` archives with the tag baked into `catsync version`, and publishes
them with `SHA256SUMS` and generated notes. A re-run for an existing tag
replaces the assets instead of failing.

Watch it with:

```sh
gh run list --workflow=release.yml
gh release view v0.1.0
```

## 2. Bump the Homebrew formula

The formula pins the release URLs and their SHA-256 values, so it must be
updated for every tag. The checksums are the `SHA256SUMS` asset of the release:

```sh
gh release download v0.1.0 --pattern SHA256SUMS --clobber
cat SHA256SUMS
```

In a clone of `dotfrankruan/homebrew-catsync`, edit `Formula/catsync.rb`:

- replace every `v0.1.0` with the new tag in the four `url` lines;
- replace each `sha256` with the matching line from `SHA256SUMS`.

Then validate and publish:

```sh
brew tap dotfrankruan/catsync
brew fetch catsync
brew audit --strict catsync
brew test catsync
brew reinstall catsync

git commit -am "catsync 0.1.1"
git push
```

`brew test` runs `catsync version` and `catsync help`, so a completely broken
archive fails before users see it.

## 3. Verify the user experience

On a clean machine (or after `brew uninstall catsync && brew untap dotfrankruan/catsync`):

```sh
brew install dotfrankruan/catsync/catsync
catsync version
catsync doctor
```

The fully-qualified install auto-taps the repository and needs no trust step;
only installing by short name after an explicit `brew tap` requires
`brew trust dotfrankruan/catsync` (that command trusts the whole tap).
