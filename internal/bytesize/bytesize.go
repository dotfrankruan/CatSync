// Package bytesize parses and formats human-readable byte sizes such as
// "512MiB", "1.5 GB", or "1048576".
//
// Both IEC binary units (KiB, MiB, GiB, TiB, PiB, EiB; base 1024) and SI
// decimal units (kB, MB, GB, TB, PB, EB; base 1000) are accepted, as are the
// single-letter shorthands K, M, G, T, P, E, which follow the IEC convention
// (base 1024). A bare number is interpreted as bytes. Parsing is
// case-insensitive and tolerates surrounding and intermediate whitespace.
package bytesize

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Size is a byte count. It implements flag.Value so it can be bound directly
// to command-line flags.
type Size uint64

// unit multipliers.
const (
	KiB = 1 << 10
	MiB = 1 << 20
	GiB = 1 << 30
	TiB = 1 << 40
	PiB = 1 << 50
	EiB = 1 << 60

	KB = 1000
	MB = KB * 1000
	GB = MB * 1000
	TB = GB * 1000
	PB = TB * 1000
	EB = PB * 1000
)

// units maps lower-cased unit suffixes to multipliers. Longer suffixes must
// be tried before their prefixes, e.g. "mib" before "mi" before "m".
var units = []struct {
	suffix     string
	multiplier uint64
}{
	{"eib", EiB}, {"eb", EB},
	{"pib", PiB}, {"pb", PB},
	{"tib", TiB}, {"tb", TB},
	{"gib", GiB}, {"gb", GB},
	{"mib", MiB}, {"mb", MB},
	{"kib", KiB}, {"kb", KB},
	{"ei", EiB}, {"e", EiB},
	{"pi", PiB}, {"p", PiB},
	{"ti", TiB}, {"t", TiB},
	{"gi", GiB}, {"g", GiB},
	{"mi", MiB}, {"m", MiB},
	{"ki", KiB}, {"k", KiB},
	{"b", 1},
}

// maxUint64 is used for overflow detection.
const maxUint64 = math.MaxUint64

// Parse converts a human-readable byte size into a Size. It returns an error
// for empty input, negative values, unknown units, non-numeric mantissas, and
// values that overflow uint64.
func Parse(s string) (Size, error) {
	orig := s
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("bytesize: empty size string")
	}
	if strings.HasPrefix(s, "-") || strings.HasPrefix(s, "+") {
		return 0, fmt.Errorf("bytesize: %q: sign prefixes are not allowed", orig)
	}

	// Split into the leading numeric mantissa and the trailing unit.
	i := 0
	for i < len(s) && (isDigit(s[i]) || s[i] == '.') {
		i++
	}
	numPart := strings.TrimSpace(s[:i])
	unitPart := strings.TrimSpace(s[i:])
	if numPart == "" || numPart == "." {
		return 0, fmt.Errorf("bytesize: %q: missing numeric value", orig)
	}

	multiplier := uint64(1)
	if unitPart != "" {
		l := strings.ToLower(unitPart)
		matched := false
		for _, u := range units {
			if l == u.suffix {
				multiplier = u.multiplier
				matched = true
				break
			}
		}
		if !matched {
			return 0, fmt.Errorf("bytesize: %q: unknown unit %q", orig, unitPart)
		}
	}

	if strings.Contains(numPart, ".") {
		return parseFloat(orig, numPart, multiplier)
	}
	n, err := strconv.ParseUint(numPart, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("bytesize: %q: invalid number %q", orig, numPart)
	}
	if multiplier > 1 && n > maxUint64/multiplier {
		return 0, fmt.Errorf("bytesize: %q: value overflows 64-bit unsigned integer", orig)
	}
	return Size(n * multiplier), nil
}

// parseFloat handles fractional mantissas such as "1.5GiB". Fractional bytes
// are truncated, not rounded, so 1.5KiB is exactly 1536 bytes.
func parseFloat(orig, numPart string, multiplier uint64) (Size, error) {
	f, err := strconv.ParseFloat(numPart, 64)
	if err != nil {
		return 0, fmt.Errorf("bytesize: %q: invalid number %q", orig, numPart)
	}
	v := f * float64(multiplier)
	if v > float64(maxUint64) {
		return 0, fmt.Errorf("bytesize: %q: value overflows 64-bit unsigned integer", orig)
	}
	return Size(uint64(v)), nil
}

// Set implements flag.Value.
func (s *Size) Set(v string) error {
	n, err := Parse(v)
	if err != nil {
		return err
	}
	*s = n
	return nil
}

// String implements flag.Value and fmt.Stringer, rendering the size with the
// largest IEC unit that keeps the value integral, falling back to bytes.
func (s Size) String() string {
	n := uint64(s)
	for _, u := range []struct {
		suffix string
		mult   uint64
	}{
		{"EiB", EiB}, {"PiB", PiB}, {"TiB", TiB}, {"GiB", GiB}, {"MiB", MiB}, {"KiB", KiB},
	} {
		if n >= u.mult && n%u.mult == 0 {
			return fmt.Sprintf("%d%s", n/u.mult, u.suffix)
		}
	}
	return fmt.Sprintf("%dB", n)
}

// Bytes returns the size as a plain uint64 byte count.
func (s Size) Bytes() uint64 { return uint64(s) }

func isDigit(c byte) bool { return c >= '0' && c <= '9' }
