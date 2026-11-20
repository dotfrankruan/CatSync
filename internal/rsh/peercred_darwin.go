//go:build darwin

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
	var cerr error
	if err := raw.Control(func(fd uintptr) {
		xu, xerr := unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
		if xerr != nil {
			cerr = xerr
			return
		}
		uid = int(xu.Uid)
	}); err != nil {
		return -1, err
	}
	return uid, cerr
}
