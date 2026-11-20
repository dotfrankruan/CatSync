package rsh

import "errors"

var (
	errNotUnix     = errors.New("rsh: not a unix socket")
	errUnsupported = errors.New("rsh: peer credentials are not supported on this platform")
)
