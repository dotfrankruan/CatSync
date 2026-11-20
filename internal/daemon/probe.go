package daemon

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"

	"github.com/dotfrankruan/CatSync/internal/daemon/protocol"
)

// ProbeReport is the read-only capability report `catsyncd probe --json`
// prints. Features reflect what this build can actually do right now; a
// missing executable downgrades the corresponding feature.
type ProbeReport struct {
	Version     string            `json:"version"`
	Protocol    int               `json:"protocol"`
	Platform    string            `json:"platform"`
	DefaultPort int               `json:"default_port"`
	Features    protocol.Features `json:"features"`
	Details     ProbeDetails      `json:"details"`
}

// ProbeDetails explains feature decisions.
type ProbeDetails struct {
	SFTPServer string `json:"sftp_server,omitempty"`
	Rsync      string `json:"rsync,omitempty"`
	Notes      string `json:"notes,omitempty"`
}

// DefaultPort is the fixed daemon port.
const DefaultPort = 43411

// ResolveSFTPPath finds the sftp-server executable: explicit flag first, then
// CATSYNC_SFTP_SERVER, then the platform locations, then PATH.
func ResolveSFTPPath(explicit string) string {
	if p := executablePath(explicit); p != "" {
		return p
	}
	if p := executablePath(os.Getenv("CATSYNC_SFTP_SERVER")); p != "" {
		return p
	}
	candidates := []string{"/usr/libexec/sftp-server", "/usr/lib/openssh/sftp-server", "/usr/lib/ssh/sftp-server"}
	for _, c := range candidates {
		if p := executablePath(c); p != "" {
			return p
		}
	}
	if p, err := exec.LookPath("sftp-server"); err == nil {
		return p
	}
	return ""
}

// ResolveRsyncPath finds rsync; exec is not implemented in M1, but probe
// reports availability so users can see what a later milestone will use.
func ResolveRsyncPath() string {
	if p, err := exec.LookPath("rsync"); err == nil {
		return p
	}
	return ""
}

func executablePath(p string) string {
	if p == "" {
		return ""
	}
	info, err := os.Stat(p)
	if err != nil || info.IsDir() || info.Mode()&0o111 == 0 {
		return ""
	}
	return filepath.Clean(p)
}

// Probe builds the capability report for this host.
func Probe(version string) ProbeReport {
	sftp := ResolveSFTPPath("")
	rsync := ResolveRsyncPath()
	notes := "exec is not implemented in this build"
	if rsync == "" {
		notes += "; rsync not found (needed only by later milestones)"
	}
	return ProbeReport{
		Version:     version,
		Protocol:    protocol.Version,
		Platform:    runtime.GOOS + "/" + runtime.GOARCH,
		DefaultPort: DefaultPort,
		Features: protocol.Features{
			SFTP:     sftp != "",
			Exec:     false,
			Manifest: true,
			Metadata: true,
			Hash:     true,
			Stat:     true,
			SameFile: true,
		},
		Details: ProbeDetails{SFTPServer: sftp, Rsync: rsync, Notes: notes},
	}
}
