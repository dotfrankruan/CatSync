// Command mockhelper is the standalone mock tool binary used by tests.
//
// Tests previously hard-linked the test binary itself as "ssh"/"rsync". Under
// -race that means every simulated SSH or SFTP session starts a race-instrumented
// binary (about 1.5 s of startup), which dominated the race test runtime. This
// tiny binary starts in milliseconds and reuses the same RunHelper dispatch, so
// mock behavior is unchanged.
package main

import (
	"os"

	"github.com/dotfrankruan/CatSync/internal/testutil/mockexec"
)

func main() {
	os.Exit(mockexec.RunHelper())
}
