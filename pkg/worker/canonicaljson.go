package worker

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
)

// Canonical JSON for flow-definition fingerprints (#60): the byte-exact Go
// counterpart of Python
// `json.dumps(doc, sort_keys=True, ensure_ascii=False, separators=(",",":"))`
// over the `json.loads`-parsed stored definition
// (plaita/server/flow_worker.py canonical_flow_definition_json).
//
// encoding/json's Marshal/Encoder cannot serve here — with escapeHTML on it
// rewrites <, >, & to \u003c-style escapes, it force-escapes U+2028/U+2029
// even with escapeHTML off, it emits \u0008/\u000c instead of \b/\f, it
// escapes DEL (0x7f), and json.Number round-trips float literals verbatim
// where Python normalizes them through float repr (1e-3 → 0.001,
// 1e2 → 100.0, 1.00 → 1.0). Each of those alone flips the fingerprint and
// trips the §7 resume hard gate in a mixed Go/Python fleet. The serializer
// below re-implements the CPython encoder rules instead; behavior is pinned
// against the live Python engine by pkg/worker/flow_hash_golden_test.go
// (testdata/flow_hash_golden.json, exported by scripts/export_flow_hash_golden.py).

// canonicalFlowDefinitionJSON decodes one strict JSON document (json.Number
// preserved) and renders it in the Python canonical form. Like Python's
// json.loads it accepts exactly one document: trailing data is an error, and
// so are the RFC-8259-invalid NaN/Infinity literals.
func canonicalFlowDefinitionJSON(raw []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var doc any
	if err := dec.Decode(&doc); err != nil {
		return nil, err
	}
	// json.loads parity: trailing data (a second document, garbage) is a
	// decode error, not silently-ignored input.
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return nil, errors.New("trailing data after flow definition JSON")
		}
		return nil, err
	}
	var buf []byte
	if err := appendCanonical(&buf, doc); err != nil {
		return nil, err
	}
	return buf, nil
}

func appendCanonical(b *[]byte, v any) error {
	switch x := v.(type) {
	case nil:
		*b = append(*b, "null"...)
	case bool:
		if x {
			*b = append(*b, "true"...)
		} else {
			*b = append(*b, "false"...)
		}
	case string:
		appendPythonString(b, x)
	case json.Number:
		s, err := canonicalJSONNumber(string(x))
		if err != nil {
			return err
		}
		*b = append(*b, s...)
	case []any:
		*b = append(*b, '[')
		for i, e := range x {
			if i > 0 {
				*b = append(*b, ',')
			}
			if err := appendCanonical(b, e); err != nil {
				return err
			}
		}
		*b = append(*b, ']')
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		// sort_keys=True: Python compares str keys by code point; UTF-8
		// byte order (Go string <) is the same order for valid Unicode.
		sort.Strings(keys)
		*b = append(*b, '{')
		for i, k := range keys {
			if i > 0 {
				*b = append(*b, ',')
			}
			appendPythonString(b, k)
			*b = append(*b, ':')
			if err := appendCanonical(b, x[k]); err != nil {
				return err
			}
		}
		*b = append(*b, '}')
	default:
		return fmt.Errorf("canonical json: unsupported value of type %T", v)
	}
	return nil
}

// appendPythonString mirrors CPython json.encoder's py_encode_basestring
// with ensure_ascii=False: the ESCAPE_DCT entries and nothing else. Every
// remaining code point — <, >, &, /, DEL, U+2028/U+2029, all non-ASCII — is
// emitted as raw UTF-8. Invalid UTF-8 cannot occur: the decoder coerces to
// U+FFFD on the only paths that could produce it.
func appendPythonString(b *[]byte, s string) {
	*b = append(*b, '"')
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '"':
			*b = append(*b, '\\', '"')
		case '\\':
			*b = append(*b, '\\', '\\')
		case '\b':
			*b = append(*b, '\\', 'b')
		case '\f':
			*b = append(*b, '\\', 'f')
		case '\n':
			*b = append(*b, '\\', 'n')
		case '\r':
			*b = append(*b, '\\', 'r')
		case '\t':
			*b = append(*b, '\\', 't')
		default:
			if c < 0x20 {
				const hexdigits = "0123456789abcdef"
				*b = append(*b, '\\', 'u', '0', '0', hexdigits[c>>4], hexdigits[c&0xf])
			} else {
				*b = append(*b, c)
			}
		}
	}
	*b = append(*b, '"')
}

// canonicalJSONNumber mirrors Python's number handling across
// json.loads → json.dumps. Integer literals become Python ints — exact
// decimal digits, with "-0" normalizing to "0". Float literals become IEEE
// doubles rendered by float.__repr__: shortest round-trip digits, fixed
// notation while the decimal-point position sits in (-4, 16], exponential
// form (≥2 exponent digits) outside it, a ".0" tail on integral values, and
// json.dumps's allow_nan spelling for out-of-range literals (1e400 →
// Infinity, 1e-400 → 0.0).
func canonicalJSONNumber(lit string) (string, error) {
	if !strings.ContainsAny(lit, ".eE") {
		if lit == "-0" {
			return "0", nil
		}
		return lit, nil
	}
	f, err := strconv.ParseFloat(lit, 64)
	if err != nil && !errors.Is(err, strconv.ErrRange) {
		return "", fmt.Errorf("canonical json: invalid number literal %q", lit)
	}
	return pythonFloatRepr(f), nil
}

// pythonFloatRepr renders f the way CPython float.__repr__ does
// (Python/pystrtod.c format_float_short, 'r' mode), followed by json.dumps's
// inf/nan spelling. The shortest round-trip digit string is unique per
// double, so Go's strconv (Grisu/Ryū) and CPython (David Gay's dtoa) always
// agree on the digits; only the presentation differs and is rebuilt here.
func pythonFloatRepr(f float64) string {
	if math.IsNaN(f) {
		return "NaN"
	}
	if math.IsInf(f, 1) {
		return "Infinity"
	}
	if math.IsInf(f, -1) {
		return "-Infinity"
	}
	sign := ""
	if math.Signbit(f) {
		sign = "-"
		f = -f
	}
	if f == 0 {
		return sign + "0.0"
	}
	// Shortest digits via 'e' format: d[.ddd]e±dd → digits + decpt, where
	// the value is 0.<digits> × 10^decpt.
	s := strconv.FormatFloat(f, 'e', -1, 64)
	mark := strings.IndexByte(s, 'e')
	exp, _ := strconv.Atoi(s[mark+1:])
	digits := strings.Replace(s[:mark], ".", "", 1)
	decpt := exp + 1

	var b strings.Builder
	b.WriteString(sign)
	switch {
	case decpt <= -4 || decpt > 16:
		b.WriteByte(digits[0])
		if len(digits) > 1 {
			b.WriteByte('.')
			b.WriteString(digits[1:])
		}
		b.WriteByte('e')
		e := decpt - 1
		if e < 0 {
			b.WriteByte('-')
			e = -e
		} else {
			b.WriteByte('+')
		}
		es := strconv.Itoa(e)
		if len(es) < 2 {
			b.WriteByte('0')
		}
		b.WriteString(es)
	case decpt <= 0:
		b.WriteString("0.")
		b.WriteString(strings.Repeat("0", -decpt))
		b.WriteString(digits)
	case decpt >= len(digits):
		b.WriteString(digits)
		b.WriteString(strings.Repeat("0", decpt-len(digits)))
		b.WriteString(".0")
	default:
		b.WriteString(digits[:decpt])
		b.WriteByte('.')
		b.WriteString(digits[decpt:])
	}
	return b.String()
}
