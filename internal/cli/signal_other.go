//go:build !unix

package cli

import (
	"context"
	"os"
	"os/signal"
)

// signalContext returns a context cancelled on interrupt, so an interrupted
// transfer terminates and reaps its subprocesses and leaves a resumable run
// behind.
func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt)
}
