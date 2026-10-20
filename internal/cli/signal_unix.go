//go:build unix

package cli

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/frankruan/catsync/internal/process"
)

// signalContext returns a context cancelled on SIGINT or SIGTERM, so an
// interrupted transfer terminates and reaps its subprocesses and leaves a
// resumable run behind. The received signal is recorded in a
// process.CancelSignal so subprocesses get the same signal forwarded to
// their process group rather than a generic termination.
func signalContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	slot := process.NewCancelSignal()
	ctx = process.WithCancelSignal(ctx, slot)
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		select {
		case sig := <-ch:
			slot.Set(sig)
			cancel()
		case <-ctx.Done():
		}
		signal.Stop(ch)
	}()
	return ctx, cancel
}
