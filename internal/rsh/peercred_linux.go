//go:build linux

package rsh

import (
	"net"

	"golang.org/x/sys/unix"
)

func peerUID(conn net.Conn) (int, error) {
	uc, ok := conn.(*net.UnixConn)
	if !ok {
		return -1, errNotUnix
	}
	raw, err := uc.SyscallConn()
	if err != nil {
		return -1, err
	}
	var uid int
	var cred *unix.Ucred
	var cerr error
	if err := raw.Control(func(fd uintptr) {
		cred, cerr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil {
		return -1, err
	}
	if cerr != nil {
		return -1, cerr
	}
	uid = int(cred.Uid)
	return uid, nil
}
