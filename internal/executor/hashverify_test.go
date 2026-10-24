package executor

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

func TestParseHashRecords(t *testing.T) {
	digestBytes := sha256.Sum256([]byte("payload"))
	digest := hex.EncodeToString(digestBytes[:])
	out := map[string]string{}
	if err := parseHashRecords("dir/file with space\x00"+strings.ToUpper(digest)+"\x00", out); err != nil {
		t.Fatal(err)
	}
	if out["dir/file with space"] != digest {
		t.Fatalf("digest = %q", out["dir/file with space"])
	}
	for _, bad := range []string{
		"missing-terminator\x00" + digest,
		"../escape\x00" + digest + "\x00",
		"file\x00not-a-digest\x00",
		"file\x00" + digest + "\x00file\x00" + digest + "\x00",
	} {
		if err := parseHashRecords(bad, map[string]string{}); err == nil {
			t.Errorf("accepted malformed response %q", bad)
		}
	}
}

func TestHashCommandQuotesPathsAndReturnsOnlyRecords(t *testing.T) {
	cmd := hashCommand(hashSHA256Sum, "odd ' name\n.bin", "/remote/odd ' name\n.bin")
	for _, want := range []string{"sha256sum <", "'/remote/odd '\\'' name", "printf '%s\\000%s\\000'", "'odd '\\'' name"} {
		if !strings.Contains(cmd, want) {
			t.Errorf("command lacks %q: %s", want, cmd)
		}
	}
}
