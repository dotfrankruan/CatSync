package daemon

import "testing"

func TestProbeFeaturesAreHonest(t *testing.T) {
	r := Probe("test")
	if r.Features.Exec != (r.Details.Rsync != "") {
		t.Errorf("exec feature %v does not match resolved rsync %q", r.Features.Exec, r.Details.Rsync)
	}
	if !r.Features.Manifest || !r.Features.Metadata || !r.Features.Hash || !r.Features.Stat || !r.Features.SameFile {
		t.Errorf("probe features = %+v", r.Features)
	}
	if (r.Features.SFTP) != (r.Details.SFTPServer != "") {
		t.Errorf("sftp feature %v does not match resolved path %q", r.Features.SFTP, r.Details.SFTPServer)
	}
	if len(r.Transports) != 2 || r.Transports[0] != "tcp" || r.Transports[1] != "quic" {
		t.Errorf("transports = %v", r.Transports)
	}
	if r.Protocol != 1 || r.DefaultPort != DefaultPort {
		t.Errorf("report = %+v", r)
	}
}
