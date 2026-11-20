package daemon

import "testing"

func TestProbeFeaturesAreHonest(t *testing.T) {
	r := Probe("test")
	if r.Features.Exec {
		t.Error("probe advertises exec in M1")
	}
	if !r.Features.Manifest || !r.Features.Metadata || !r.Features.Hash || !r.Features.Stat || !r.Features.SameFile {
		t.Errorf("probe features = %+v", r.Features)
	}
	if (r.Features.SFTP) != (r.Details.SFTPServer != "") {
		t.Errorf("sftp feature %v does not match resolved path %q", r.Features.SFTP, r.Details.SFTPServer)
	}
	if r.Protocol != 1 || r.DefaultPort != DefaultPort {
		t.Errorf("report = %+v", r)
	}
}
