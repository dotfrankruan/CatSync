# Formula source for the dotfrankruan/homebrew-catsync tap.
#
# This file is not published automatically: render it with
#   packaging/homebrew/render.sh VERSION dist/release/SHA256SUMS > catsync.rb
# and commit the result to the tap. CatSync never publishes to the tap itself.
class Catsync < Formula
  desc "Crash-safe parallel file sync over ssh"
  homepage "https://github.com/dotfrankruan/CatSync"
  license "AGPL-3.0-only"
  version "__VERSION__"

  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/dotfrankruan/CatSync/releases/download/__TAG__/catsync_v__VERSION___darwin_arm64.tar.gz"
      sha256 "__SHA_DARWIN_ARM64__"
    else
      url "https://github.com/dotfrankruan/CatSync/releases/download/__TAG__/catsync_v__VERSION___darwin_amd64.tar.gz"
      sha256 "__SHA_DARWIN_AMD64__"
    end
  end

  on_linux do
    if Hardware::CPU.arm?
      url "https://github.com/dotfrankruan/CatSync/releases/download/__TAG__/catsync_v__VERSION___linux_arm64.tar.gz"
      sha256 "__SHA_LINUX_ARM64__"
    else
      url "https://github.com/dotfrankruan/CatSync/releases/download/__TAG__/catsync_v__VERSION___linux_amd64.tar.gz"
      sha256 "__SHA_LINUX_AMD64__"
    end
  end

  def install
    # Both binaries ship in every archive; catsyncd is the opt-in one-shot
    # daemon agent and is never installed on remote hosts by catsync itself.
    bin.install "catsync", "catsyncd"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/catsync version")
    assert_match version.to_s, shell_output("#{bin}/catsyncd version")
    assert_match "exec", shell_output("#{bin}/catsyncd probe --json")
  end
end
