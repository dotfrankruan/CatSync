//go:build !unix

package rsh

import (
	"context"
	"errors"

	"github.com/dotfrankruan/CatSync/internal/daemon"
)

// ErrUnsupported is returned on platforms without Unix sockets.
var ErrUnsupported = errors.New("rsh: unsupported platform")

type Agent struct{}

func Start(context.Context, string, string, *daemon.Client) (*Agent, error) {
	return nil, ErrUnsupported
}
func (a *Agent) Env() []string { return nil }
func (a *Agent) Close()        {}
