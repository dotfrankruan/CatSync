package bytesize

import (
	"flag"
	"fmt"
	"math"
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		in   string
		want uint64
	}{
		{"0", 0},
		{"1", 1},
		{"1024", 1024},
		{"18446744073709551615", math.MaxUint64},
		{"1B", 1},
		{"512 B", 512},
		{"512B", 512},
		{"1K", 1024},
		{"1k", 1024},
		{"1KB", 1000},
		{"1kB", 1000},
		{"1KiB", 1024},
		{"2kib", 2048},
		{"1M", MiB},
		{"1MB", MB},
		{"1MiB", MiB},
		{"3G", 3 * GiB},
		{"3GB", 3 * GB},
		{"3GiB", 3 * GiB},
		{"1T", TiB},
		{"1TB", TB},
		{"1TiB", TiB},
		{"1P", PiB},
		{"1PiB", PiB},
		{"1E", EiB},
		{"1EiB", EiB},
		{"1.5KiB", 1536},
		{"1.5K", 1536},
		{"0.5MB", 500000},
		{"2.5 GiB", 2*GiB + GiB/2},
		{"  10  MiB  ", 10 * MiB},
		{"15EiB", 15 * EiB}, // largest integral EiB value that fits
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			got, err := Parse(tc.in)
			if err != nil {
				t.Fatalf("Parse(%q) unexpected error: %v", tc.in, err)
			}
			if uint64(got) != tc.want {
				t.Fatalf("Parse(%q) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

func TestParseErrors(t *testing.T) {
	inputs := []string{
		"",
		"   ",
		"-1",
		"+1",
		"-1GiB",
		"MiB",                  // no mantissa
		".",                    // no mantissa
		"1.2.3GiB",             // malformed float
		"1XB",                  // unknown unit
		"1foobar",              // unknown unit
		"10mibib",              // unknown unit
		"0x10",                 // hex digits leak into unit
		"1e3",                  // exponent parsed as unit
		"17EiB",                // overflow
		"18446744073709551616", // overflow of plain uint64
		"99999999999999999999999999TiB",
	}
	for _, in := range inputs {
		t.Run(in, func(t *testing.T) {
			if got, err := Parse(in); err == nil {
				t.Fatalf("Parse(%q) = %d, want error", in, got)
			}
		})
	}
}

func TestParseDoesNotPanicOnDotVariants(t *testing.T) {
	// Leading-dot fractions are accepted by ParseFloat; keep behavior stable.
	if got, err := Parse(".5K"); err != nil || got != 512 {
		t.Fatalf("Parse(.5K) = %d, %v; want 512, nil", got, err)
	}
	if _, err := Parse("1.K"); err != nil {
		t.Fatalf("Parse(1.K) unexpected error: %v", err)
	}
}

func TestString(t *testing.T) {
	tests := []struct {
		size Size
		want string
	}{
		{0, "0B"},
		{1, "1B"},
		{512, "512B"},
		{1023, "1023B"},
		{1024, "1KiB"},
		{1536, "1536B"}, // not integral in KiB
		{MiB, "1MiB"},
		{10 * GiB, "10GiB"},
		{TiB, "1TiB"},
		{PiB, "1PiB"},
		{EiB, "1EiB"},
	}
	for _, tc := range tests {
		if got := tc.size.String(); got != tc.want {
			t.Errorf("Size(%d).String() = %q, want %q", tc.size, got, tc.want)
		}
	}
}

func TestSetAsFlagValue(t *testing.T) {
	var s Size
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.Var(&s, "threshold", "usage")
	if err := fs.Parse([]string{"-threshold", "256MiB"}); err != nil {
		t.Fatalf("flag parse error: %v", err)
	}
	if s.Bytes() != 256*MiB {
		t.Fatalf("got %d bytes, want %d", s.Bytes(), 256*MiB)
	}
	if err := fs.Parse([]string{"-threshold", "bogus"}); err == nil {
		t.Fatal("expected error for bogus threshold")
	}
}

func TestRoundTripThroughString(t *testing.T) {
	for _, s := range []Size{0, 1, 512, 1024, 10 * GiB, TiB} {
		got, err := Parse(s.String())
		if err != nil {
			t.Fatalf("Parse(%q) error: %v", s.String(), err)
		}
		if got != s {
			t.Fatalf("round trip failed: %d -> %q -> %d", s, s.String(), got)
		}
	}
}

func ExampleParse() {
	n, _ := Parse("1.5GiB")
	fmt.Println(strings.TrimSuffix(n.String(), "B")) // print to keep example simple
	// Output: 1536Mi
}
