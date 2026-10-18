package target

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestParseDestination(t *testing.T) {
	tests := []struct {
		in        string
		user      string
		host      string
		path      string
		canonical string
	}{
		{"host:/data", "", "host", "/data", "host:/data"},
		{"user@host:/data", "user", "host", "/data", "user@host:/data"},
		{"user.name-1@host.example.com:/a/b/c", "user.name-1", "host.example.com", "/a/b/c", "user.name-1@host.example.com:/a/b/c"},
		{"192.0.2.1:/srv", "", "192.0.2.1", "/srv", "192.0.2.1:/srv"},
		{"[2001:db8::1]:/srv", "", "2001:db8::1", "/srv", "[2001:db8::1]:/srv"},
		{"user@[2001:db8::1]:/srv", "user", "2001:db8::1", "/srv", "user@[2001:db8::1]:/srv"},
		{"user@[fe80::1%eth0]:/mnt/x", "user", "fe80::1%eth0", "/mnt/x", "user@[fe80::1%eth0]:/mnt/x"},
		{"host:/path with spaces/file", "", "host", "/path with spaces/file", "host:/path with spaces/file"},
		{"host:/", "", "host", "/", "host:/"},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			d, err := ParseDestination(tc.in)
			if err != nil {
				t.Fatalf("ParseDestination(%q) error: %v", tc.in, err)
			}
			if d.User != tc.user || d.Host != tc.host || d.Path != tc.path {
				t.Fatalf("ParseDestination(%q) = %+v, want user=%q host=%q path=%q",
					tc.in, d, tc.user, tc.host, tc.path)
			}
			if got := d.String(); got != tc.canonical {
				t.Fatalf("Destination.String() = %q, want %q", got, tc.canonical)
			}
		})
	}
}

func TestParseDestinationErrors(t *testing.T) {
	inputs := []string{
		"",                  // empty
		"/local/path",       // local absolute path
		"relative/path",     // local relative path
		"host",              // no path
		"host:",             // empty path
		":/data",            // empty host
		"host:relative",     // non-absolute remote path
		"host:~/data",       // tilde-relative remote path
		"@host:/data",       // empty user
		"us er@host:/data",  // space in user
		"user@ho st:/data",  // space in host
		"[2001:db8::1/srv",  // unterminated bracket
		"[2001:db8::1]/srv", // missing colon after bracket
		"user@/data",        // '@' after separator colon is path content; here host empty
		"ho/st:/data",       // slash in host
	}
	for _, in := range inputs {
		t.Run(in, func(t *testing.T) {
			if d, err := ParseDestination(in); err == nil {
				t.Fatalf("ParseDestination(%q) = %+v, want error", in, d)
			}
		})
	}
}

func TestParseDestinationAtInPath(t *testing.T) {
	// '@' after the separator colon belongs to the path, not a user part.
	d, err := ParseDestination("host:/data/@snapshots/daily")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d.User != "" || d.Host != "host" || d.Path != "/data/@snapshots/daily" {
		t.Fatalf("got %+v", d)
	}
}

func TestValidateSource(t *testing.T) {
	dir := t.TempDir()
	abs, err := ValidateSource(dir)
	if err != nil {
		t.Fatalf("ValidateSource(%q) error: %v", dir, err)
	}
	if !filepath.IsAbs(abs) {
		t.Fatalf("ValidateSource returned non-absolute path %q", abs)
	}

	// A relative spelling of the same directory must validate and return
	// an absolute path.
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	rel, err := filepath.Rel(wd, dir)
	if err != nil {
		t.Fatal(err)
	}
	abs2, err := ValidateSource(rel)
	if err != nil {
		t.Fatalf("ValidateSource(%q) error: %v", rel, err)
	}
	if abs2 != abs {
		t.Fatalf("relative spelling resolved to %q, want %q", abs2, abs)
	}

	// Symlink to a directory is accepted for the source root.
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(dir, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, err := ValidateSource(link); err != nil {
		t.Fatalf("ValidateSource(symlink) error: %v", err)
	}
}

func TestValidateSourceErrors(t *testing.T) {
	if _, err := ValidateSource(""); err == nil {
		t.Fatal("empty source: want error")
	}
	if _, err := ValidateSource(filepath.Join(t.TempDir(), "does-not-exist")); err == nil {
		t.Fatal("missing source: want error")
	}
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateSource(file); err == nil {
		t.Fatal("regular file source: want error")
	}
}

func TestValidateSourceUnreadable(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("chmod-based unreadable test is not portable to windows")
	}
	if os.Geteuid() == 0 {
		t.Skip("running as root; permission bits are not enforced")
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	if _, err := ValidateSource(dir); err == nil {
		t.Fatal("unreadable source: want error")
	}
}
