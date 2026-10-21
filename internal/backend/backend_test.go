package backend

import (
	"strings"
	"testing"

	"github.com/frankruan/catsync/internal/target"
)

func TestRsyncTransport(t *testing.T) {
	if got := (SSH{}).RsyncTransport(); got != "ssh" {
		t.Errorf("default transport = %q, want ssh", got)
	}
	if got := (SSH{IPv6: true}).RsyncTransport(); got != "ssh -6" {
		t.Errorf("IPv6 transport = %q, want ssh -6", got)
	}
}

func TestMSCPArgs(t *testing.T) {
	if got := (SSH{}).MSCPArgs(); len(got) != 0 {
		t.Errorf("default mscp args = %v, want none", got)
	}
	got := (SSH{IPv6: true}).MSCPArgs()
	if len(got) != 1 || got[0] != "-6" {
		t.Errorf("IPv6 mscp args = %v, want [-6]", got)
	}
}

func TestValidateDestination(t *testing.T) {
	good := target.Destination{User: "alice", Host: "example.com", Path: "/data"}
	if err := ValidateDestination(good); err != nil {
		t.Errorf("valid destination rejected: %v", err)
	}
	v6 := target.Destination{Host: "2001:db8::1", Path: "/data"}
	if err := ValidateDestination(v6); err != nil {
		t.Errorf("IPv6 destination rejected: %v", err)
	}

	bad := []struct {
		name string
		dest target.Destination
	}{
		{"empty host", target.Destination{Path: "/data"}},
		{"empty path", target.Destination{Host: "example.com"}},
		{"relative path", target.Destination{Host: "example.com", Path: "data"}},
		{"NUL in host", target.Destination{Host: "exa\x00mple", Path: "/data"}},
		{"NUL in path", target.Destination{Host: "example.com", Path: "/da\x00ta"}},
		{"NUL in user", target.Destination{User: "a\x00b", Host: "example.com", Path: "/data"}},
		// A dash-leading user renders a dash-leading operand, which a
		// tool could parse as options: rejected explicitly.
		{"dash user", target.Destination{User: "-evil", Host: "example.com", Path: "/data"}},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateDestination(tc.dest); err == nil {
				t.Errorf("destination %+v accepted, want error", tc.dest)
			}
		})
	}
}

func TestRemotePath(t *testing.T) {
	for _, tc := range []struct {
		base, rel, want string
	}{
		{"/dest", "a/b.bin", "/dest/a/b.bin"},
		{"/dest/", "a/b.bin", "/dest/a/b.bin"},
		{"/", "a", "/a"},
		{"/dest dir", "ünïcødé.bin", "/dest dir/ünïcødé.bin"},
	} {
		if got := RemotePath(tc.base, tc.rel); got != tc.want {
			t.Errorf("RemotePath(%q, %q) = %q, want %q", tc.base, tc.rel, got, tc.want)
		}
	}
}

func TestDestinationOperandNeverDashLeading(t *testing.T) {
	// Even a host starting with a dash cannot produce a dash-leading
	// operand once parsed by target, but a hand-built Destination could;
	// the validator is the backend's last line of defense.
	d := target.Destination{Host: "-h", Path: "/data"}
	err := ValidateDestination(d)
	if err == nil || !strings.Contains(err.Error(), "-") {
		t.Errorf("dash-leading operand error = %v, want explicit rejection", err)
	}
}

// TestRsyncTransportProgram pins the ssh program override in rsync's -e
// value: the executor passes CATSYNC_SSH here so the override reaches the
// transport instead of being silently ignored.
func TestRsyncTransportProgram(t *testing.T) {
	cases := []struct {
		ssh  SSH
		want string
	}{
		{SSH{}, "ssh"},
		{SSH{IPv6: true}, "ssh -6"},
		{SSH{Program: "/opt/mock/ssh"}, "/opt/mock/ssh"},
		{SSH{Program: "/opt/mock/ssh", IPv6: true}, "/opt/mock/ssh -6"},
		{SSH{Program: "ssh-custom"}, "ssh-custom"},
	}
	for _, tc := range cases {
		if got := tc.ssh.RsyncTransport(); got != tc.want {
			t.Errorf("RsyncTransport(%+v) = %q, want %q", tc.ssh, got, tc.want)
		}
	}
}

// TestValidateTransport rejects overrides rsync's -e value cannot represent
// (rsync splits it on whitespace without quoting).
func TestValidateTransport(t *testing.T) {
	for _, ok := range []string{"", "ssh", "/opt/homebrew/bin/ssh", "ssh-custom"} {
		if err := (SSH{Program: ok}).ValidateTransport(); err != nil {
			t.Errorf("ValidateTransport(%q) = %v, want nil", ok, err)
		}
	}
	for _, bad := range []string{"/opt/a b/ssh", "ssh\t-x", "ssh\nx", "-ssh", "a\x00b"} {
		if err := (SSH{Program: bad}).ValidateTransport(); err == nil {
			t.Errorf("ValidateTransport(%q) = nil, want error", bad)
		}
	}
}
