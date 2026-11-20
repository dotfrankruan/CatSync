//go:build !unix

package cli

import (
	"fmt"
	"io"
)

func runRshCommand(args []string, stdout, stderr io.Writer) int {
	fmt.Fprintln(stderr, "catsync rsh: unsupported platform")
	return ExitError
}
