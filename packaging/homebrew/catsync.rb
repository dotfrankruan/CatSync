# Formula source for the dotfrankruan/homebrew-catsync tap.
#
# Render it from the published SHA256SUMS file:
#   packaging/homebrew/render.sh VERSION dist/release/SHA256SUMS > catsync.rb
# then copy the result into the tap's Formula/catsync.rb. CatSync never
# publishes to the tap itself.
#
# Releases beginning with v0.2.0 ship both binaries in the same archive; the
# tap formula installs both unconditionally. Only the historical v0.1.0
# archive predates catsyncd.
class Catsync < Formula
  desc "Parallel, resumable file tree copier over SSH"
  homepage "https://github.com/dotfrankruan/CatSync"
  license "AGPL-3.0-only"

  on_macos do
    on_arm do
      url "https://github.com/dotfrankruan/CatSync/releases/download/__TAG__/catsync_v__VERSION___darwin_arm64.tar.gz"
      sha256 "__SHA_DARWIN_ARM64__"
    end
    on_intel do
      url "https://github.com/dotfrankruan/CatSync/releases/download/__TAG__/catsync_v__VERSION___darwin_amd64.tar.gz"
      sha256 "__SHA_DARWIN_AMD64__"
    end
  end

  on_linux do
    on_arm do
      url "https://github.com/dotfrankruan/CatSync/releases/download/__TAG__/catsync_v__VERSION___linux_arm64.tar.gz"
      sha256 "__SHA_LINUX_ARM64__"
    end
    on_intel do
      url "https://github.com/dotfrankruan/CatSync/releases/download/__TAG__/catsync_v__VERSION___linux_amd64.tar.gz"
      sha256 "__SHA_LINUX_AMD64__"
    end
  end

  def install
    # Both binaries ship in every archive from the daemon milestone on. catsyncd is the
    # opt-in one-shot daemon agent; installing it locally never installs or
    # copies anything on remote hosts.
    bin.install "catsync", "catsyncd"
  end

  test do
    assert_match "catsync v#{version}", shell_output("#{bin}/catsync version")
    assert_match "Usage: catsync", shell_output("#{bin}/catsync help")
    assert_match "catsyncd v#{version}", shell_output("#{bin}/catsyncd version")
    assert_match "\"transports\"", shell_output("#{bin}/catsyncd probe --json")
  end
end
