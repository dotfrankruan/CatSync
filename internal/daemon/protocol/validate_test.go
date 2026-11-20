package protocol

import (
	"strings"
	"testing"
)

func TestValidateAbsPath(t *testing.T) {
	good := []string{"/", "/tmp", "/tmp/a b", "/tmp/üñï"}
	for _, p := range good {
		if err := ValidateAbsPath(p); err != nil {
			t.Errorf("ValidateAbsPath(%q) = %v", p, err)
		}
	}
	bad := []string{"", "tmp", "/tmp/../etc", "/tmp/a\x00b", "/tmp/x/" + strings.Repeat("a", MaxPathBytes)}
	for _, p := range bad {
		if err := ValidateAbsPath(p); err == nil {
			t.Errorf("ValidateAbsPath(%q) = nil, want error", p)
		}
	}
	if err := ValidateAbsPath("/tmp/\xff"); err == nil {
		t.Error("invalid UTF-8 accepted")
	}
}

func TestValidateRelPath(t *testing.T) {
	for _, p := range []string{"a", "a/b", "a b", "üñï"} {
		if err := ValidateRelPath(p); err != nil {
			t.Errorf("ValidateRelPath(%q) = %v", p, err)
		}
	}
	for _, p := range []string{"", "/a", "a/../b", "a//b", "a/\x00b"} {
		if err := ValidateRelPath(p); err == nil {
			t.Errorf("ValidateRelPath(%q) = nil, want error", p)
		}
	}
	if err := ValidateRelPath("a/\xff"); err == nil {
		t.Error("invalid UTF-8 accepted")
	}
}

func TestHTTPStatus(t *testing.T) {
	cases := map[string]int{
		CodeNotFound: 404, CodePermission: 403, CodeInvalidArgument: 400,
		CodeTooLarge: 413, CodeInternal: 500, CodeConnectionLimit: 429,
	}
	for code, want := range cases {
		if got := HTTPStatus(code); got != want {
			t.Errorf("HTTPStatus(%q) = %d, want %d", code, got, want)
		}
	}
}
