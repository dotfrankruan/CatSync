package discovery

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

// collect gathers every entry Walk emits, failing the test on a walk error.
func collect(t *testing.T, root string) []Entry {
	t.Helper()
	var entries []Entry
	err := Walk(root, func(e Entry) error {
		entries = append(entries, e)
		return nil
	})
	if err != nil {
		t.Fatalf("Walk(%q) error: %v", root, err)
	}
	return entries
}

// byPath indexes entries by path, failing on duplicates.
func byPath(t *testing.T, entries []Entry) map[string]Entry {
	t.Helper()
	m := make(map[string]Entry, len(entries))
	for _, e := range entries {
		if _, dup := m[e.Path]; dup {
			t.Fatalf("duplicate entry for path %q", e.Path)
		}
		m[e.Path] = e
	}
	return m
}

func writeFile(t *testing.T, dir, name string, size int) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(strings.Repeat("x", size)), 0o600); err != nil {
		t.Fatalf("write %q: %v", p, err)
	}
	return p
}

func TestWalkRegularFiles(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "top.txt", 10)
	if err := os.MkdirAll(filepath.Join(root, "sub", "dir"), 0o700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(root, "sub"), "mid.txt", 20)
	writeFile(t, filepath.Join(root, "sub", "dir"), "deep.txt", 30)

	entries := byPath(t, collect(t, root))
	if len(entries) != 3 {
		t.Fatalf("got %d entries, want 3: %v", len(entries), entries)
	}
	for path, wantSize := range map[string]int64{
		"top.txt":          10,
		"sub/mid.txt":      20,
		"sub/dir/deep.txt": 30,
	} {
		e, ok := entries[path]
		if !ok {
			t.Fatalf("missing entry %q", path)
		}
		if e.Kind != KindFile {
			t.Errorf("entry %q kind = %v, want file", path, e.Kind)
		}
		if e.Err != nil {
			t.Errorf("entry %q unexpected error: %v", path, e.Err)
		}
		if e.Size != wantSize {
			t.Errorf("entry %q size = %d, want %d", path, e.Size, wantSize)
		}
		if e.ModTime.IsZero() {
			t.Errorf("entry %q has zero mtime", path)
		}
		if strings.ContainsRune(e.Path, os.PathSeparator) && os.PathSeparator != '/' {
			t.Errorf("entry %q is not slash-normalized", path)
		}
	}
}

func TestWalkMtimeMatchesLstat(t *testing.T) {
	root := t.TempDir()
	p := writeFile(t, root, "stamped.txt", 5)
	mtime := time.Date(2020, 5, 4, 3, 2, 1, 123456789, time.UTC)
	if err := os.Chtimes(p, time.Now(), mtime); err != nil {
		t.Fatal(err)
	}
	entries := byPath(t, collect(t, root))
	e := entries["stamped.txt"]
	fi, err := os.Lstat(p)
	if err != nil {
		t.Fatal(err)
	}
	if !e.ModTime.Equal(fi.ModTime()) {
		t.Fatalf("entry mtime %v does not match Lstat mtime %v", e.ModTime, fi.ModTime())
	}
}

func TestWalkUnusualNames(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix-style special names")
	}
	root := t.TempDir()
	names := []string{
		"with space.txt",
		"日本語ファイル.txt",
		"emoji-🐈.txt",
		"line1\nline2.txt",
		"-leading-dash.txt",
		"--help.txt",
		"trailing space .txt",
		"quote'file.txt",
		`double"quote.txt`,
		"tab\tname.txt",
	}
	for _, n := range names {
		writeFile(t, root, n, len(n))
	}
	if err := os.MkdirAll(filepath.Join(root, "dir with spaces"), 0o700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(root, "dir with spaces"), "nested file.txt", 7)

	entries := byPath(t, collect(t, root))
	for _, n := range names {
		e, ok := entries[n]
		if !ok {
			t.Errorf("missing entry for %q", n)
			continue
		}
		if e.Kind != KindFile || e.Size != int64(len(n)) {
			t.Errorf("entry %q = %+v", n, e)
		}
	}
	if e, ok := entries["dir with spaces/nested file.txt"]; !ok || e.Kind != KindFile {
		t.Errorf("nested spaced entry = %+v, ok=%v", e, ok)
	}
	if len(entries) != len(names)+1 {
		t.Errorf("got %d entries, want %d", len(entries), len(names)+1)
	}
}

func TestWalkNeverFollowsSymlinks(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks need privileges on windows")
	}
	root := t.TempDir()
	writeFile(t, root, "real.txt", 3)
	outside := t.TempDir()
	writeFile(t, outside, "outside.txt", 100)

	links := map[string]string{
		"link-to-file": filepath.Join(root, "real.txt"),
		"link-to-dir":  outside,
		"dangling":     filepath.Join(root, "no-such-target"),
		"rel-link":     "real.txt",
		"link-to-link": "rel-link",
		"escape-up":    "../does-not-matter",
	}
	for name, target := range links {
		if err := os.Symlink(target, filepath.Join(root, name)); err != nil {
			t.Fatalf("symlink %q: %v", name, err)
		}
	}

	entries := byPath(t, collect(t, root))
	if len(entries) != 1+len(links) {
		t.Fatalf("got %d entries, want %d: %v", len(entries), 1+len(links), entries)
	}
	for name := range links {
		e, ok := entries[name]
		if !ok {
			t.Errorf("missing symlink entry %q", name)
			continue
		}
		if e.Kind != KindSymlink {
			t.Errorf("entry %q kind = %v, want symlink", name, e.Kind)
		}
		if e.Err != nil {
			t.Errorf("entry %q unexpected error: %v", name, e.Err)
		}
	}
	// The directory symlink must not be descended into.
	for path := range entries {
		if strings.HasPrefix(path, "link-to-dir/") {
			t.Errorf("walk descended into symlinked directory: %q", path)
		}
	}
}

func TestWalkSpecialFiles(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("no unix special files on windows")
	}
	root := t.TempDir()
	writeFile(t, root, "regular.txt", 1)
	if err := syscall.Mkfifo(filepath.Join(root, "pipe"), 0o600); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}

	entries := byPath(t, collect(t, root))
	e, ok := entries["pipe"]
	if !ok {
		t.Fatalf("missing fifo entry")
	}
	if e.Kind != KindSpecial {
		t.Errorf("fifo kind = %v, want special", e.Kind)
	}
	if entries["regular.txt"].Kind != KindFile {
		t.Errorf("regular.txt kind = %v, want file", entries["regular.txt"].Kind)
	}
}

func TestWalkReportsUnreadableDirectory(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("chmod-based permission test")
	}
	if os.Geteuid() == 0 {
		t.Skip("root bypasses permission checks")
	}
	root := t.TempDir()
	writeFile(t, root, "ok.txt", 1)
	blocked := filepath.Join(root, "blocked")
	if err := os.MkdirAll(blocked, 0o700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, blocked, "hidden.txt", 1)
	if err := os.Chmod(blocked, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(blocked, 0o700) })

	entries := collect(t, root)
	var errEntry *Entry
	for i := range entries {
		if entries[i].Path == "blocked" && entries[i].Err != nil {
			errEntry = &entries[i]
		}
	}
	if errEntry == nil {
		t.Fatalf("no error entry for unreadable directory: %v", entries)
	}
	// Sibling entries still appear: the walk continues past the failure.
	found := false
	for _, e := range entries {
		if e.Path == "ok.txt" && e.Kind == KindFile {
			found = true
		}
	}
	if !found {
		t.Errorf("walk did not continue past the unreadable directory: %v", entries)
	}
}

func TestWalkEmitErrorStopsPromptly(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < 50; i++ {
		writeFile(t, root, "file"+string(rune('a'+i%26))+string(rune('0'+i/26))+".txt", 1)
	}
	sentinel := errors.New("stop now")
	count := 0
	err := Walk(root, func(e Entry) error {
		count++
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Walk error = %v, want sentinel", err)
	}
	if count != 1 {
		t.Fatalf("emit called %d times after returning an error, want exactly 1", count)
	}
}

func TestWalkRootValidation(t *testing.T) {
	if _, err := collectExpectErr(t, ""); err == nil {
		t.Error("Walk(\"\") succeeded, want error")
	}
	file := filepath.Join(t.TempDir(), "file.txt")
	writeFile(t, filepath.Dir(file), filepath.Base(file), 1)
	if _, err := collectExpectErr(t, file); err == nil {
		t.Errorf("Walk on a regular file succeeded, want error")
	}
	if _, err := collectExpectErr(t, filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Errorf("Walk on a missing root succeeded, want error")
	}
	if _, err := collectExpectErr(t, "bad\x00root"); err == nil {
		t.Errorf("Walk on a NUL root succeeded, want error")
	}
}

func collectExpectErr(t *testing.T, root string) ([]Entry, error) {
	t.Helper()
	var entries []Entry
	err := Walk(root, func(e Entry) error {
		entries = append(entries, e)
		return nil
	})
	return entries, err
}

func TestWalkSymlinkedRoot(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks need privileges on windows")
	}
	real := t.TempDir()
	writeFile(t, real, "inner.txt", 4)
	link := filepath.Join(t.TempDir(), "root-link")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	// The explicitly named root anchor may be a symlink to a directory;
	// its contents are still Lstat'd.
	entries := byPath(t, collect(t, link))
	if e, ok := entries["inner.txt"]; !ok || e.Kind != KindFile || e.Size != 4 {
		t.Fatalf("entries through symlinked root = %v", entries)
	}
}

func TestStreamBoundedChannel(t *testing.T) {
	root := t.TempDir()
	const total = 20
	for i := 0; i < total; i++ {
		writeFile(t, root, strings.Repeat("f", i+1)+".txt", i)
	}
	entries, errc := Stream(root, 3)
	if cap(entries) != 3 {
		t.Fatalf("Stream channel capacity = %d, want 3", cap(entries))
	}
	count := 0
	for e := range entries {
		if e.Kind != KindFile {
			t.Errorf("unexpected entry %+v", e)
		}
		count++
	}
	if err := <-errc; err != nil {
		t.Fatalf("Stream error: %v", err)
	}
	if count != total {
		t.Fatalf("Stream delivered %d entries, want %d", count, total)
	}
}

func TestStreamErrorPropagation(t *testing.T) {
	entries, errc := Stream(filepath.Join(t.TempDir(), "missing"), 1)
	for range entries {
		t.Error("unexpected entry for a missing root")
	}
	if err := <-errc; err == nil {
		t.Fatal("Stream on a missing root reported success")
	}
}

func TestValidateRelPath(t *testing.T) {
	valid := []string{
		"a",
		"a/b/c",
		"with space.txt",
		"日本語/ファイル",
		"line1\nline2",
		"-dash",
		"--",
		"..x",
		"x..",
		"a.b/c.d",
		".hidden",
	}
	for _, p := range valid {
		if err := ValidateRelPath(p); err != nil {
			t.Errorf("ValidateRelPath(%q) = %v, want nil", p, err)
		}
	}

	invalid := []string{
		"",
		"/absolute",
		"/",
		"../escape",
		"a/../../escape",
		"a/./b",
		".",
		"..",
		"a//b",
		"a/",
		"has\x00nul",
	}
	for _, p := range invalid {
		if err := ValidateRelPath(p); err == nil {
			t.Errorf("ValidateRelPath(%q) = nil, want error", p)
		}
	}
}

func TestKindString(t *testing.T) {
	for k, want := range map[Kind]string{
		KindFile:    "file",
		KindSymlink: "symlink",
		KindSpecial: "special",
	} {
		if got := k.String(); got != want {
			t.Errorf("Kind(%d).String() = %q, want %q", int(k), got, want)
		}
	}
	if got := Kind(99).String(); !strings.Contains(got, "99") {
		t.Errorf("unknown kind string = %q", got)
	}
}
