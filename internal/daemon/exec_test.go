package daemon

import (
	"strings"
	"testing"

	"github.com/dotfrankruan/CatSync/internal/daemon/protocol"
)

func TestValidateExecStartGoldenArgv(t *testing.T) {
	cases := [][]string{
		// upload batch (rsync 3.5 capture)
		{"--server", "-logDtpRe.LsfxCIvu", "--partial-dir", ".catsync-rsync-partial", ".", "/data/"},
		// download batch
		{"--server", "--sender", "-logDtpRe.LsfxCIvu", "--files-from=-", "--from0", ".", "/data/"},
		// mirror upload
		{"--server", "-lHogDtpAXrce.iLsfxCIvu", "--delete-delay", "--partial", ".", "/data/"},
		// mirror download dry run
		{"--server", "--sender", "-nlHogDtpAXrce.iLsfxCIvu", ".", "/data/"},
		{"--server", "--sender", "--numeric-ids", "-logDtpre.iLsfxCIvu", ".", "/srv/"},
		{"--server", "-logDtpre.iLsfxCIvu", "--no-implied-dirs", ".", "/x"},
	}
	for _, args := range cases {
		if err := protocol.ValidateExecStart("rsync", args); err != nil {
			t.Errorf("golden argv rejected: %v: %v", args, err)
		}
	}
}

func TestValidateExecStartRejects(t *testing.T) {
	cases := []struct {
		program string
		args    []string
		want    string
	}{
		{"sh", []string{"-c", "id"}, "not allowed"},
		{"rsync", []string{"--server", "-log", ".", "relative/path"}, "absolute"},
		{"rsync", []string{"--server", "-log", ".", "/a\n/b"}, "control"},
		{"rsync", []string{"--server", "--rsh=sh", ".", "/a"}, "not allowed"},
		{"rsync", []string{"--server", "-log", "--", ".", "/a"}, "not allowed"},
		{"rsync", []string{"--server", "-log", ".", "/a", "/b"}, "exactly one path"},
		{"rsync", []string{"-log", ".", "/a"}, "--server"},
		{"rsync", []string{"--server", "-log"}, "separator"},
		{"rsync", []string{"--server", "-log", "", "/a"}, "unexpected"},
	}
	for _, tc := range cases {
		err := protocol.ValidateExecStart(tc.program, tc.args)
		if err == nil {
			t.Errorf("accepted %q %v", tc.program, tc.args)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("error %v lacks %q", err, tc.want)
		}
	}
}
