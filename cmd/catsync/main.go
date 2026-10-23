// Command catsync orchestrates multi-backend file synchronization.
package main

import (
	"os"

	"github.com/dotfrankruan/CatSync/internal/cli"
)

// version is set at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	os.Exit(cli.Run(os.Args[1:], os.Stdout, os.Stderr, version))
}
