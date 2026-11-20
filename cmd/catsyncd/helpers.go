package main

import (
	"net"

	"github.com/dotfrankruan/CatSync/internal/daemon/protocol"
)

// protocolLimits builds the server budget from CLI values.
func protocolLimits(sftp, exec, control, pending, conns int) protocol.Limits {
	return protocol.Limits{
		MaxSFTPConns:        sftp,
		MaxExecConns:        exec,
		MaxControlConns:     control,
		MaxPendingHandshake: pending,
		MaxConns:            conns,
	}
}

// netSplitHostPort wraps net.SplitHostPort for listen addresses.
func netSplitHostPort(addr string) (host, port string, err error) {
	return net.SplitHostPort(addr)
}
