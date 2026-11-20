package main

import (
	"testing"

	"github.com/dotfrankruan/CatSync/internal/daemon/protocol"
)

func TestPortFromListen(t *testing.T) {
	cases := map[string]int{
		":43411":      43411,
		"127.0.0.1:0": 0,
		"[::1]:43412": 43412,
		"nonsense":    0,
		":99999":      0,
	}
	for listen, want := range cases {
		if got := portFromListen(listen); got != want {
			t.Errorf("portFromListen(%q) = %d, want %d", listen, got, want)
		}
	}
}

func TestProtocolLimits(t *testing.T) {
	got := protocolLimits(1, 2, 3, 4, 5)
	want := protocol.Limits{MaxSFTPConns: 1, MaxExecConns: 2, MaxControlConns: 3, MaxPendingHandshake: 4, MaxConns: 5}
	if got != want {
		t.Errorf("limits = %+v, want %+v", got, want)
	}
}

func TestRunDispatch(t *testing.T) {
	if code := run(nil); code != 2 {
		t.Errorf("run(nil) = %d, want 2", code)
	}
	if code := run([]string{"bogus"}); code != 2 {
		t.Errorf("run(bogus) = %d, want 2", code)
	}
	if code := run([]string{"version"}); code != 0 {
		t.Errorf("run(version) = %d, want 0", code)
	}
	if code := run([]string{"serve"}); code != 2 {
		t.Errorf("run(serve without --once) = %d, want 2", code)
	}
}
