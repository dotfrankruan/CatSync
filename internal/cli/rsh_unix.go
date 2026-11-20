//go:build unix

package cli

import (
	"fmt"
	"io"

	"github.com/dotfrankruan/CatSync/internal/rsh"
)

// runRshCommand implements the hidden `catsync rsh` transport helper. rsync
// spawns it as its remote shell; it is not a user-facing command.
func runRshCommand(args []string, stdout, stderr io.Writer) int {
	if len(args) == 1 && (args[0] == "help" || args[0] == "--help" || args[0] == "-h") {
		fmt.Fprintln(stderr, "catsync rsh: internal helper for the daemon transport; not for direct use")
		return ExitUsage
	}
	// The helper talks to rsync over the process's real stdin/stdout.
	return rsh.RunHelper(args)
}
