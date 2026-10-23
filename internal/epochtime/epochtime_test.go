package epochtime

import (
	"errors"
	"testing"
	"time"
)

func TestFormatParseRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		t    time.Time
		want string
	}{
		{"epoch zero", time.Unix(0, 0), "0.000000000"},
		{"positive", time.Unix(1_700_000_000, 123456789), "1700000000.123456789"},
		{"negative half", time.Unix(-1, 500000000), "-0.500000000"},
		{"negative one and a half", time.Unix(-2, 500000000), "-1.500000000"},
		{"negative integer", time.Unix(-1, 0), "-1.000000000"},
		{"negative whole seconds", time.Unix(-625776833, 0), "-625776833.000000000"},
		{"pre-epoch nanosecond", time.Unix(-625776833, 890123456), "-625776832.109876544"},
		{"negative tenth", time.Unix(-1, 900000000), "-0.100000000"},
		{"negative nine digits", time.Unix(-3, 123456789), "-2.876543211"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Format(tc.t)
			if got != tc.want {
				t.Errorf("Format(%v) = %q, want %q", tc.t, got, tc.want)
			}
			back, err := Parse(got)
			if err != nil {
				t.Fatalf("Parse(%q): %v", got, err)
			}
			if !back.Equal(tc.t) {
				t.Errorf("Parse(%q) = %v, want %v", got, back, tc.t)
			}
			if back.Nanosecond() != tc.t.Nanosecond() {
				t.Errorf("Parse(%q) nanoseconds = %d, want %d", got, back.Nanosecond(), tc.t.Nanosecond())
			}
		})
	}
}

// TestParseNegativeIsDecimal proves the signed-decimal model: "-0.5" is half
// a second BEFORE the epoch, i.e. time.Unix(-1, 500000000) — not
// time.Unix(0, -500000000) and not time.Unix(-1, 500000000) misread as -1.5.
func TestParseNegativeIsDecimal(t *testing.T) {
	got, err := Parse("-0.5")
	if err != nil {
		t.Fatal(err)
	}
	want := time.Unix(-1, 500000000).UTC()
	if !got.Equal(want) {
		t.Fatalf("Parse(-0.5) = %v, want %v", got, want)
	}
	if got.Unix() != -1 || got.Nanosecond() != 500000000 {
		t.Errorf("Parse(-0.5) = (%d, %d), want (-1, 500000000)", got.Unix(), got.Nanosecond())
	}
	// "-1.5" is one and a half seconds before the epoch.
	got, err = Parse("-1.5")
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Unix(-2, 500000000).UTC(); !got.Equal(want) {
		t.Fatalf("Parse(-1.5) = %v, want %v", got, want)
	}
}

func TestParseAcceptedForms(t *testing.T) {
	cases := []struct {
		in   string
		want time.Time
	}{
		{"0", time.Unix(0, 0)},
		{"0.0", time.Unix(0, 0)},
		{"1", time.Unix(1, 0)},
		{"1.", time.Time{}}, // rejected below
		{"0.1", time.Unix(0, 100000000)},
		{"0.000000001", time.Unix(0, 1)},
		{"0.123456789", time.Unix(0, 123456789)},
		{"-1", time.Unix(-1, 0)},
		{"-1.0", time.Unix(-1, 0)},
		{"-0.000000001", time.Unix(-1, 999999999)},
		{"-2.5", time.Unix(-3, 500000000)},
	}
	for _, tc := range cases {
		if tc.in == "1." {
			if _, err := Parse(tc.in); !errors.Is(err, ErrInvalid) {
				t.Errorf("Parse(%q) = nil error, want ErrInvalid", tc.in)
			}
			continue
		}
		got, err := Parse(tc.in)
		if err != nil {
			t.Errorf("Parse(%q): %v", tc.in, err)
			continue
		}
		if !got.Equal(tc.want) {
			t.Errorf("Parse(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestParseRejectsInvalid(t *testing.T) {
	bad := []string{
		"",
		"-",
		".",
		"-.5",
		"+1",
		"1.2.3",
		"1.1234567890", // ten fractional digits
		" 1",
		"1 ",
		"0x10",
		"1,5",
		"--1",
		"-0x1",
		"abc",
		"9223372036854775808",  // MaxInt64+1
		"-9223372036854775808", // magnitude exceeds MaxInt64
		"99999999999999999999",
	}
	for _, in := range bad {
		if _, err := Parse(in); !errors.Is(err, ErrInvalid) {
			t.Errorf("Parse(%q) = %v, want ErrInvalid", in, err)
		}
	}
}

func TestParseExtremes(t *testing.T) {
	// The largest representable whole second magnitude.
	got, err := Parse("9223372036854775807")
	if err != nil {
		t.Fatalf("Parse(MaxInt64): %v", err)
	}
	if got.Unix() != 9223372036854775807 {
		t.Errorf("Unix = %d, want MaxInt64", got.Unix())
	}
	got, err = Parse("-9223372036854775807")
	if err != nil {
		t.Fatalf("Parse(-MaxInt64): %v", err)
	}
	if got.Unix() != -9223372036854775807 {
		t.Errorf("Unix = %d, want -MaxInt64", got.Unix())
	}
	// Negative MaxInt64 with a fraction maps to MinInt64 seconds: it must
	// not wrap.
	got, err = Parse("-9223372036854775807.5")
	if err != nil {
		t.Fatalf("Parse(-MaxInt64.5): %v", err)
	}
	if got.Unix() != -9223372036854775808 {
		t.Errorf("Unix = %d, want MinInt64", got.Unix())
	}
	if got.Nanosecond() != 500000000 {
		t.Errorf("Nanosecond = %d, want 500000000", got.Nanosecond())
	}
}

// TestFormatMatchesParse keeps the two halves consistent for a spread of
// instants, including sub-second and pre-epoch values.
func TestFormatMatchesParse(t *testing.T) {
	bases := []int64{-1, 0, 1, 1_700_000_000, -625776833}
	fracs := []int64{0, 1, 5_000_000, 123456789, 999999999}
	for _, b := range bases {
		for _, f := range fracs {
			tt := time.Unix(b, f).UTC()
			s := Format(tt)
			back, err := Parse(s)
			if err != nil {
				t.Fatalf("Parse(Format(%v)=%q): %v", tt, s, err)
			}
			if !back.Equal(tt) {
				t.Errorf("round trip %v -> %q -> %v", tt, s, back)
			}
		}
	}
}
