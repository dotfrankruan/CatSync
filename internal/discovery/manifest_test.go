package discovery

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/frankruan/catsync/internal/target"
)

// manifestRecord renders one record in the wire format.
func manifestRecord(path, size, mtime string) string {
	return path + "\x00" + size + "\x00" + mtime + "\x00"
}

func TestParseManifestRoundTrip(t *testing.T) {
	var in strings.Builder
	in.WriteString(manifestRecord("plain.txt", "10", "1700000000.1234567890"))
	in.WriteString(manifestRecord("with space/inner file.bin", "0", "1700000001"))
	in.WriteString(manifestRecord("uni/日本語.txt", "4294967296", "1.5"))
	in.WriteString(manifestRecord("line1\nline2", "7", "9999999999.000000001"))
	in.WriteString(manifestRecord("-leading/dash", "88", "0"))
	in.WriteString(manifestRecord("quote'and\"double", "9", "1700000002.999999999"))

	var got []RemoteEntry
	err := ParseManifest(strings.NewReader(in.String()), func(e RemoteEntry) error {
		got = append(got, e)
		return nil
	})
	if err != nil {
		t.Fatalf("ParseManifest error: %v", err)
	}
	if len(got) != 6 {
		t.Fatalf("got %d records, want 6", len(got))
	}

	want := []RemoteEntry{
		{"plain.txt", 10, time.Unix(1700000000, 123456789).UTC()},
		{"with space/inner file.bin", 0, time.Unix(1700000001, 0).UTC()},
		{"uni/日本語.txt", 4294967296, time.Unix(1, 500000000).UTC()},
		{"line1\nline2", 7, time.Unix(9999999999, 1).UTC()},
		{"-leading/dash", 88, time.Unix(0, 0).UTC()},
		{"quote'and\"double", 9, time.Unix(1700000002, 999999999).UTC()},
	}
	for i := range want {
		if got[i].Path != want[i].Path || got[i].Size != want[i].Size {
			t.Errorf("record %d = %+v, want %+v", i+1, got[i], want[i])
		}
		if !got[i].ModTime.Equal(want[i].ModTime) {
			t.Errorf("record %d mtime = %v, want %v", i+1, got[i].ModTime, want[i].ModTime)
		}
	}
}

func TestParseManifestEmpty(t *testing.T) {
	count := 0
	err := ParseManifest(strings.NewReader(""), func(e RemoteEntry) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("empty manifest error: %v", err)
	}
	if count != 0 {
		t.Fatalf("empty manifest delivered %d records", count)
	}
	m, err := CollectManifest(strings.NewReader(""))
	if err != nil || len(m) != 0 {
		t.Fatalf("CollectManifest(\"\") = %v, %v", m, err)
	}
}

func TestParseManifestMalformed(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"missing terminators", "path", "truncated"},
		{"only path field", "path\x00", "truncated"},
		{"only path and size", "path\x0010\x00", "truncated"},
		{"size not numeric", manifestRecord("a", "abc", "1"), "invalid size"},
		{"size empty", manifestRecord("a", "", "1"), "size"},
		{"size negative", manifestRecord("a", "-1", "1"), "invalid size"},
		{"size overflows uint64", manifestRecord("a", "99999999999999999999999", "1"), "invalid size"},
		{"size overflows int64", manifestRecord("a", "9223372036854775808", "1"), "overflows int64"},
		{"mtime not numeric", manifestRecord("a", "1", "now"), "invalid mtime"},
		{"mtime empty", manifestRecord("a", "1", ""), "mtime"},
		{"mtime negative", manifestRecord("a", "1", "-5"), "invalid mtime"},
		{"mtime signed", manifestRecord("a", "1", "+5"), "invalid mtime"},
		{"mtime bare dot", manifestRecord("a", "1", "1."), "fraction"},
		{"mtime leading dot", manifestRecord("a", "1", ".5"), "invalid mtime"},
		{"mtime non-digit fraction", manifestRecord("a", "1", "1.123x"), "fraction"},
		{"mtime overflow", manifestRecord("a", "1", "99999999999999999999999"), "invalid mtime"},
		{"path absolute", manifestRecord("/abs", "1", "1"), "path"},
		{"path escapes root", manifestRecord("../x", "1", "1"), "path"},
		{"path nested escape", manifestRecord("a/../../x", "1", "1"), "path"},
		{"path empty", manifestRecord("", "1", "1"), "path"},
		{"path dot element", manifestRecord("a/./b", "1", "1"), "path"},
		{"path double slash", manifestRecord("a//b", "1", "1"), "path"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ParseManifest(strings.NewReader(tc.in), func(RemoteEntry) error { return nil })
			if err == nil {
				t.Fatalf("ParseManifest(%q) succeeded, want error", tc.in)
			}
			if !errors.Is(err, ErrMalformedManifest) {
				t.Errorf("error %v does not wrap ErrMalformedManifest", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
			if !strings.Contains(err.Error(), "record 1") {
				t.Errorf("error %q does not carry the record number", err)
			}
		})
	}
}

func TestParseManifestRecordNumbers(t *testing.T) {
	in := manifestRecord("good", "1", "1") + manifestRecord("also-good", "2", "2") + manifestRecord("bad", "x", "3")
	err := ParseManifest(strings.NewReader(in), func(RemoteEntry) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "record 3") {
		t.Fatalf("error = %v, want record 3", err)
	}
}

func TestParseManifestFieldBounds(t *testing.T) {
	// A path field exactly at the bound is accepted...
	ok := strings.Repeat("a", MaxManifestPathBytes)
	if err := ParseManifest(strings.NewReader(manifestRecord(ok, "1", "1")), func(RemoteEntry) error { return nil }); err != nil {
		t.Fatalf("max-size path rejected: %v", err)
	}
	// ...and one byte over is rejected.
	tooLong := strings.Repeat("a", MaxManifestPathBytes+1)
	err := ParseManifest(strings.NewReader(manifestRecord(tooLong, "1", "1")), func(RemoteEntry) error { return nil })
	if err == nil || !errors.Is(err, ErrMalformedManifest) {
		t.Fatalf("overlong path: err = %v", err)
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("overlong path error = %q, want a bound mention", err)
	}
	// Numeric fields are bounded too.
	bigNum := strings.Repeat("9", maxManifestNumBytes+1)
	err = ParseManifest(strings.NewReader(manifestRecord("a", bigNum, "1")), func(RemoteEntry) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("overlong size field: err = %v", err)
	}
}

// neverEndingReader yields 'a' forever, without any NUL terminator.
type neverEndingReader struct{}

func (neverEndingReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'a'
	}
	return len(p), nil
}

func TestParseManifestNeverTerminatingFieldIsBounded(t *testing.T) {
	// The parser must give up at the field bound instead of buffering an
	// unbounded field.
	err := ParseManifest(neverEndingReader{}, func(RemoteEntry) error {
		t.Fatal("emit called on an unterminated stream")
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("err = %v, want a field-bound error", err)
	}
}

func TestParseManifestEmitError(t *testing.T) {
	sentinel := errors.New("consumer stop")
	in := manifestRecord("a", "1", "1") + manifestRecord("b", "2", "2")
	count := 0
	err := ParseManifest(strings.NewReader(in), func(RemoteEntry) error {
		count++
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel", err)
	}
	if count != 1 {
		t.Fatalf("emit called %d times, want 1", count)
	}
}

func TestCollectManifestDuplicate(t *testing.T) {
	in := manifestRecord("dup.txt", "1", "1") + manifestRecord("other.txt", "2", "2") + manifestRecord("dup.txt", "3", "3")
	_, err := CollectManifest(strings.NewReader(in))
	if err == nil || !errors.Is(err, ErrMalformedManifest) {
		t.Fatalf("err = %v, want a duplicate error", err)
	}
	if !strings.Contains(err.Error(), "duplicate") || !strings.Contains(err.Error(), "dup.txt") {
		t.Fatalf("duplicate error = %q", err)
	}
}

func TestCollectManifestMap(t *testing.T) {
	in := manifestRecord("a.txt", "1", "10") + manifestRecord("dir/b.txt", "2", "20.25")
	m, err := CollectManifest(strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 2 {
		t.Fatalf("got %d entries, want 2", len(m))
	}
	if e := m["dir/b.txt"]; e.Size != 2 || !e.ModTime.Equal(time.Unix(20, 250000000).UTC()) {
		t.Fatalf("dir/b.txt = %+v", e)
	}
}

func TestManifestCommand(t *testing.T) {
	tests := []struct {
		name string
		dest target.Destination
		want []string
	}{
		{
			name: "host only",
			dest: target.Destination{Host: "host", Path: "/data"},
			want: []string{"ssh", "host", "find '/data' -type f -printf '%P\\0%s\\0%T@\\0'"},
		},
		{
			name: "user and host",
			dest: target.Destination{User: "u", Host: "example.com", Path: "/srv/tree"},
			want: []string{"ssh", "u@example.com", "find '/srv/tree' -type f -printf '%P\\0%s\\0%T@\\0'"},
		},
		{
			name: "ipv6 literal",
			dest: target.Destination{User: "u", Host: "2001:db8::1", Path: "/"},
			want: []string{"ssh", "u@2001:db8::1", "find '/' -type f -printf '%P\\0%s\\0%T@\\0'"},
		},
		{
			name: "hostile path",
			dest: target.Destination{Host: "h", Path: "/it's \"quoted\"/$HOME/`id`/;rm -rf"},
			want: []string{"ssh", "h", `find '/it'\''s "quoted"/$HOME/` + "`id`" + `/;rm -rf' -type f -printf '%P\0%s\0%T@\0'`},
		},
		{
			name: "newline and space path",
			dest: target.Destination{Host: "h", Path: "/a b/c\nd"},
			want: []string{"ssh", "h", "find '/a b/c\nd' -type f -printf '%P\\0%s\\0%T@\\0'"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ManifestCommand(tc.dest)
			if err != nil {
				t.Fatalf("ManifestCommand error: %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("argv = %q, want %q", got, tc.want)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Fatalf("argv[%d] = %q, want %q (full argv %q)", i, got[i], tc.want[i], got)
				}
			}
			// Exactly one ssh command, as an argument array: no local
			// shell, no concatenated shell line.
			if got[0] != "ssh" {
				t.Errorf("argv[0] = %q, want ssh", got[0])
			}
			for _, a := range got {
				if a == "sh" || a == "-c" {
					t.Errorf("argv %q invokes a local shell", got)
				}
			}
		})
	}
}

func TestManifestCommandErrors(t *testing.T) {
	bad := []target.Destination{
		{Host: "", Path: "/data"},
		{Host: "h", Path: ""},
		{Host: "h", Path: "relative"},
		{Host: "h", Path: "/bad\x00path"},
		{Host: "ba\x00d", Path: "/data"},
	}
	for _, d := range bad {
		if argv, err := ManifestCommand(d); err == nil {
			t.Errorf("ManifestCommand(%+v) = %q, want error", d, argv)
		}
	}
}

func TestManifestCommandSingleCommand(t *testing.T) {
	// One ssh invocation, one remote command string: the manifest needs a
	// single round trip.
	argv, err := ManifestCommand(target.Destination{User: "u", Host: "h", Path: "/d"})
	if err != nil {
		t.Fatal(err)
	}
	if len(argv) != 3 {
		t.Fatalf("argv has %d elements, want exactly 3 (ssh, target, script): %q", len(argv), argv)
	}
	if !strings.Contains(argv[2], "-printf") || !strings.Contains(argv[2], `\0`) {
		t.Fatalf("remote script %q does not emit NUL-delimited records", argv[2])
	}
}
