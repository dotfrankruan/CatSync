package strictjson

import (
	"strings"
	"testing"

	"github.com/dotfrankruan/CatSync/internal/daemon/protocol"
)

type sample struct {
	Type string `json:"type"`
	Root string `json:"root"`
}

func TestDecodeAcceptsOneObject(t *testing.T) {
	var got sample
	if err := Decode([]byte(`{"type":"x","root":"/a"}`), &got, DefaultLimits); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Type != "x" || got.Root != "/a" {
		t.Errorf("decoded %+v", got)
	}
}

func TestDecodeRejectsViolations(t *testing.T) {
	cases := []struct {
		name string
		in   string
		code string
	}{
		{"duplicate key", `{"type":"a","type":"b"}`, protocol.CodeJSONDuplicateKey},
		{"duplicate nested", `{"type":"a","root":"/x","n":{"k":1,"k":2}}`, protocol.CodeJSONDuplicateKey},
		{"trailing object", `{"type":"a"}{"type":"b"}`, protocol.CodeJSONTrailingData},
		{"trailing garbage", `{"type":"a"} x`, protocol.CodeJSONTrailingData},
		{"unknown field", `{"type":"a","bogus":1}`, protocol.CodeJSONUnknownField},
		{"wrong type", `{"type":"a","root":5}`, protocol.CodeJSONTypeMismatch},
		{"non object", `[1,2]`, protocol.CodeJSONTypeMismatch},
		{"invalid utf8", "{\"type\":\"a\",\"root\":\"\xff\"}", protocol.CodeJSONTypeMismatch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got sample
			err := Decode([]byte(tc.in), &got, DefaultLimits)
			var pe *protocol.Error
			if err == nil || !asProtocolError(err, &pe) || pe.Code != tc.code {
				t.Fatalf("Decode(%q) = %v, want code %s", tc.in, err, tc.code)
			}
		})
	}
}

func TestDecodeDepth(t *testing.T) {
	var out map[string]any
	deep := strings.Repeat(`{"a":`, 40) + "1" + strings.Repeat("}", 40)
	if err := Decode([]byte(deep), &out, Limits{MaxBytes: 1 << 20, MaxDepth: 8}); err == nil {
		t.Fatal("deep nesting accepted")
	}
	ok := `{"a":{"b":{"c":1}}}`
	if err := Decode([]byte(ok), &out, Limits{MaxBytes: 1 << 20, MaxDepth: 8}); err != nil {
		t.Fatalf("shallow nesting rejected: %v", err)
	}
}

func TestDecodeSize(t *testing.T) {
	big := `{"type":"` + strings.Repeat("a", 200) + `"}`
	if err := Decode([]byte(big), &sample{}, Limits{MaxBytes: 64, MaxDepth: 8}); err == nil {
		t.Fatal("oversized message accepted")
	}
}

func asProtocolError(err error, out **protocol.Error) bool {
	pe, ok := err.(*protocol.Error)
	if ok {
		*out = pe
	}
	return ok
}
