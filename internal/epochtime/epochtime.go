// Package epochtime formats and parses signed decimal Unix timestamps with
// nanosecond precision, as used by GNU tools ("%T@", "touch -d @SECONDS").
//
// The value is a true signed decimal number of seconds since the Unix epoch:
//
//	0.000000000
//	1700000000.123456789
//	-0.500000000        (half a second before the epoch)
//	-1.500000000
//
// Go's time.Unix splits a negative instant into a negative whole second and
// a non-negative nanosecond part — time.Unix(-1, 500000000) is -0.5s, not
// -1.5s — so formatting must not simply concatenate Unix() and Nanosecond().
package epochtime

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

// ErrInvalid reports a value that is not a valid signed decimal epoch
// timestamp, or one whose integer part cannot be represented.
var ErrInvalid = errors.New("epochtime: invalid epoch timestamp")

// Format renders t as a signed decimal epoch timestamp with exactly nine
// fractional digits, e.g. "1700000000.123456789", "-0.500000000",
// "-1.000000000", "0.000000000".
func Format(t time.Time) string {
	sec := t.Unix()
	ns := int64(t.Nanosecond()) // always 0..999999999
	if sec >= 0 {
		return fmt.Sprintf("%d.%09d", sec, ns)
	}
	if ns == 0 {
		return fmt.Sprintf("%d.000000000", sec)
	}
	// Negative non-integer instants: value = sec + ns/1e9, e.g. -1 + 0.5 =
	// -0.5. As a signed decimal magnitude that is (|sec|-1).(1e9-ns).
	return fmt.Sprintf("-%d.%09d", -sec-1, 1_000_000_000-ns)
}

// Parse parses a signed decimal epoch timestamp: an optional '-', at least
// one integer digit, and an optional '.' followed by one to nine fractional
// digits. A '+' sign, whitespace, a bare '.', more than nine fraction digits,
// or an integer part beyond int64 is an ErrInvalid error.
func Parse(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, fmt.Errorf("%w: empty value", ErrInvalid)
	}
	neg := false
	if s[0] == '-' {
		neg = true
		s = s[1:]
	}
	if s == "" {
		return time.Time{}, fmt.Errorf("%w: missing seconds", ErrInvalid)
	}
	intPart, fracPart, hasFrac := strings.Cut(s, ".")
	if !isDigits(intPart) {
		return time.Time{}, fmt.Errorf("%w: %q", ErrInvalid, s)
	}
	var nsec int64
	if hasFrac {
		if len(fracPart) == 0 || len(fracPart) > 9 || !isDigits(fracPart) {
			return time.Time{}, fmt.Errorf("%w: fraction %q", ErrInvalid, fracPart)
		}
		// Right-pad to nanosecond precision: ".5" means 500ms.
		frac := fracPart + strings.Repeat("0", 9-len(fracPart))
		v, err := parseInt64(frac)
		if err != nil {
			return time.Time{}, fmt.Errorf("%w: fraction %q", ErrInvalid, fracPart)
		}
		nsec = v
	}
	// The integer part is parsed as a non-negative magnitude, so an unsigned
	// parse catches |value| > MaxInt64 without wrapping.
	mag, err := parseUint64(intPart)
	if err != nil || mag > math.MaxInt64 {
		return time.Time{}, fmt.Errorf("%w: seconds out of range %q", ErrInvalid, intPart)
	}
	sec := int64(mag)
	if !neg {
		return time.Unix(sec, nsec).UTC(), nil
	}
	if nsec == 0 {
		return time.Unix(-sec, 0).UTC(), nil
	}
	// Magnitude m.f seconds maps to Unix(-(m+1), 1e9-fraction).
	// When m == MaxInt64 this is MinInt64, which is representable.
	return time.Unix(-sec-1, 1_000_000_000-nsec).UTC(), nil
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// parseUint64 parses a digit string, reporting overflow instead of wrapping.
func parseUint64(s string) (uint64, error) {
	var v uint64
	for i := 0; i < len(s); i++ {
		d := uint64(s[i] - '0')
		if v > (math.MaxUint64-d)/10 {
			return 0, errors.New("overflow")
		}
		v = v*10 + d
	}
	return v, nil
}

// parseInt64 parses at most nine digits, which always fits.
func parseInt64(s string) (int64, error) {
	v, err := parseUint64(s)
	if err != nil {
		return 0, err
	}
	return int64(v), nil
}
