// Package strictjson decodes security-sensitive JSON messages with rules that
// encoding/json does not enforce by itself: duplicate object keys, trailing
// data after the value, nesting depth, and byte size are all rejected.
package strictjson

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"

	"github.com/dotfrankruan/CatSync/internal/daemon/protocol"
)

// Limits bounds one message.
type Limits struct {
	MaxBytes int
	MaxDepth int
}

// DefaultLimits are the control-plane limits.
var DefaultLimits = Limits{MaxBytes: protocol.MaxControlLineBytes, MaxDepth: 32}

// errCode carries a protocol error code.
func errCode(code, format string, args ...any) error {
	return protocol.NewError(code, format, args...)
}

// Decode parses exactly one JSON object into dst. It rejects invalid UTF-8,
// duplicate keys (at any depth), trailing tokens, excessive nesting, unknown
// fields, and wrong field types. Nothing is written to dst on error.
func Decode(data []byte, dst any, lim Limits) error {
	if lim.MaxBytes <= 0 {
		lim.MaxBytes = protocol.MaxRPCBodyBytes
	}
	if lim.MaxDepth <= 0 {
		lim.MaxDepth = 32
	}
	if len(data) > lim.MaxBytes {
		return errCode(protocol.CodeJSONTooLarge, "message is %d bytes, limit %d", len(data), lim.MaxBytes)
	}
	if !utf8.Valid(data) {
		return errCode(protocol.CodeJSONTypeMismatch, "message is not valid UTF-8")
	}
	if err := scan(data, lim); err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return classifyDecodeError(err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return errCode(protocol.CodeJSONTrailingData, "unexpected data after message")
	}
	return nil
}

// scan walks the token stream to reject duplicate keys, trailing data, and
// excessive depth before the typed decode.
func scan(data []byte, lim Limits) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()

	tok, err := dec.Token()
	if err != nil {
		return classifyDecodeError(err)
	}
	if tok != json.Delim('{') {
		return errCode(protocol.CodeJSONTypeMismatch, "message root must be a JSON object")
	}
	if err := scanObject(dec, lim, 1); err != nil {
		return err
	}
	if _, err := dec.Token(); err != io.EOF {
		// The root value is complete; anything else (including a syntax
		// error at the next token) is trailing data.
		return errCode(protocol.CodeJSONTrailingData, "unexpected data after message")
	}
	return nil
}

// scanObject consumes the rest of an object whose opening brace was read.
func scanObject(dec *json.Decoder, lim Limits, depth int) error {
	seen := make(map[string]struct{})
	for {
		tok, err := dec.Token()
		if err != nil {
			return classifyDecodeError(err)
		}
		if tok == json.Delim('}') {
			return nil
		}
		key, ok := tok.(string)
		if !ok {
			return errCode(protocol.CodeJSONTypeMismatch, "object key is not a string")
		}
		if _, dup := seen[key]; dup {
			return errCode(protocol.CodeJSONDuplicateKey, "duplicate object key %q", key)
		}
		seen[key] = struct{}{}
		if err := scanValue(dec, lim, depth); err != nil {
			return err
		}
	}
}

// scanValue consumes one JSON value.
func scanValue(dec *json.Decoder, lim Limits, depth int) error {
	tok, err := dec.Token()
	if err != nil {
		return classifyDecodeError(err)
	}
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			if depth+1 > lim.MaxDepth {
				return errCode(protocol.CodeJSONTooDeep, "nesting exceeds depth %d", lim.MaxDepth)
			}
			return scanObject(dec, lim, depth+1)
		case '[':
			if depth+1 > lim.MaxDepth {
				return errCode(protocol.CodeJSONTooDeep, "nesting exceeds depth %d", lim.MaxDepth)
			}
			return scanArray(dec, lim, depth+1)
		default:
			return errCode(protocol.CodeJSONTypeMismatch, "unexpected delimiter %q", t)
		}
	case string, json.Number, bool, nil:
		return nil
	default:
		return errCode(protocol.CodeJSONTypeMismatch, "unsupported JSON value")
	}
}

// scanArray consumes the rest of an array whose opening bracket was read.
func scanArray(dec *json.Decoder, lim Limits, depth int) error {
	for {
		tok, err := dec.Token()
		if err != nil {
			return classifyDecodeError(err)
		}
		if tok == json.Delim(']') {
			return nil
		}
		// The token belongs to a value: push it back by scanning the value
		// from the already-read token.
		if err := scanValueFrom(dec, tok, lim, depth); err != nil {
			return err
		}
	}
}

// scanValueFrom is scanValue for an already-consumed first token.
func scanValueFrom(dec *json.Decoder, tok json.Token, lim Limits, depth int) error {
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			if depth+1 > lim.MaxDepth {
				return errCode(protocol.CodeJSONTooDeep, "nesting exceeds depth %d", lim.MaxDepth)
			}
			return scanObject(dec, lim, depth+1)
		case '[':
			if depth+1 > lim.MaxDepth {
				return errCode(protocol.CodeJSONTooDeep, "nesting exceeds depth %d", lim.MaxDepth)
			}
			return scanArray(dec, lim, depth+1)
		default:
			return errCode(protocol.CodeJSONTypeMismatch, "unexpected delimiter %q", t)
		}
	case string, json.Number, bool, nil:
		return nil
	default:
		return errCode(protocol.CodeJSONTypeMismatch, "unsupported JSON value")
	}
}

// classifyDecodeError maps encoding/json errors to protocol error codes.
func classifyDecodeError(err error) error {
	var pe *protocol.Error
	if errors.As(err, &pe) {
		return pe
	}
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		return errCode(protocol.CodeJSONTypeMismatch, "truncated JSON message")
	}
	if errors.Is(err, io.ErrNoProgress) {
		return errCode(protocol.CodeJSONTypeMismatch, "malformed JSON message")
	}
	msg := err.Error()
	switch {
	case contains(msg, "unknown field"):
		return errCode(protocol.CodeJSONUnknownField, "%s", msg)
	case contains(msg, "cannot unmarshal"):
		return errCode(protocol.CodeJSONTypeMismatch, "%s", msg)
	case contains(msg, "unexpected end"):
		return errCode(protocol.CodeJSONTypeMismatch, "truncated JSON message")
	}
	return errCode(protocol.CodeJSONTypeMismatch, "%s", msg)
}

func contains(s, sub string) bool { return bytes.Contains([]byte(s), []byte(sub)) }

// Encode marshals v and appends a newline so a control line can be written
// directly.
func Encode(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("strictjson: encode: %w", err)
	}
	return append(b, '\n'), nil
}
