//go:build !linux && !darwin

package rsh

import "net"

var errNotUnix = errUnsupported

func peerUID(net.Conn) (int, error) { return -1, errUnsupported }
