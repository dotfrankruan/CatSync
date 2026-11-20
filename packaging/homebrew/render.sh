#!/bin/sh
# Render packaging/homebrew/catsync.rb from a release SHA256SUMS file.
# Usage: render.sh VERSION [SHA256SUMS] > catsync.rb
set -eu
raw="${1:?usage: render.sh VERSION [SHA256SUMS]}"
sums="${2:-dist/release/SHA256SUMS}"
# Accept either "1.2.3" or the release tag "v1.2.3": archives use the bare
# version and the download URL uses the tag.
version="${raw#v}"
tag="v${version}"
template="$(dirname "$0")/catsync.rb"
sha() {
  # SHA256SUMS lines end with the archive basename.
  awk -v name="$1" '$2 == name || $2 == "*"name { print $1; exit }' "$sums"
}
darm=$(sha "catsync_v${version}_darwin_arm64.tar.gz")
damd=$(sha "catsync_v${version}_darwin_amd64.tar.gz")
larm=$(sha "catsync_v${version}_linux_arm64.tar.gz")
lamd=$(sha "catsync_v${version}_linux_amd64.tar.gz")
for v in "$darm" "$damd" "$larm" "$lamd"; do
  [ -n "$v" ] || { echo "render.sh: missing archive in $sums" >&2; exit 1; }
done
sed \
  -e "s/__VERSION__/${version}/g" \
  -e "s/__SHA_DARWIN_ARM64__/${darm}/g" \
  -e "s/__SHA_DARWIN_AMD64__/${damd}/g" \
  -e "s/__SHA_LINUX_ARM64__/${larm}/g" \
  -e "s/__SHA_LINUX_AMD64__/${lamd}/g" \
  -e "s/__TAG__/${tag}/g" \
  "$template"
