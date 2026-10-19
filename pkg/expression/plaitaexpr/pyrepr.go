package plaitaexpr

// pyrepr.go renders expression values the way Python's str()/repr() do
// (semantic authority: CPython str/repr for the JSON value domain: None,
// bool, int, float, str, list, dict). plaita's template interpolation is
// `str(value)` (expression_parser.py _eval_template) and `$F.concat` is
// `"".join(str(arg) ...)`, so every textual rendering of a structured value
// must go through this file to stay byte-compatible with Python.
//
// Documented divergence (unavoidable in Go): dict rendering order. Python
// dicts preserve insertion order; the Go engine's maps are unordered
// map[string]any (order is already lost at JSON decode). We render dict keys
// SORTED so output is deterministic, and document this as an intentional
// divergence from Python (see issues #8/#21/#22).

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// PyStr renders v the way Python str(v) does for the expression value domain.
// nil → "None", true/false → "True"/"False", strings verbatim, ints decimal,
// floats via Python float repr ("3.0", "1e+16"), lists/dicts via Python repr.
// It is the single rendering gate for template interpolation (`{% ... %}`),
// $F.concat and %s formatting.
func PyStr(v any) string {
	switch t := v.(type) {
	case nil:
		return "None"
	case bool:
		if t {
			return "True"
		}
		return "False"
	case string:
		return t
	}
	if i, ok := pyIntValue(v); ok {
		return strconv.FormatInt(i, 10)
	}
	if f, ok := pyFloatValue(v); ok {
		return pyFloatRepr(f)
	}
	switch t := v.(type) {
	case []any:
		parts := make([]string, len(t))
		for i, e := range t {
			parts[i] = PyRepr(e)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case map[string]any:
		return pyDictRepr(t)
	default:
		// Non-data values (engine-internal structs) have no Python
		// counterpart; fmt.Sprint is the pragmatic fallback.
		return fmt.Sprint(v)
	}
}

// PyRepr renders v the way Python repr(v) does: like PyStr, but strings are
// quoted (Python repr quoting: prefer single quotes) and containers render
// their elements repr'd.
func PyRepr(v any) string {
	switch t := v.(type) {
	case nil:
		return "None"
	case bool:
		if t {
			return "True"
		}
		return "False"
	case string:
		return pyStrQuote(t)
	}
	if i, ok := pyIntValue(v); ok {
		return strconv.FormatInt(i, 10)
	}
	if f, ok := pyFloatValue(v); ok {
		return pyFloatRepr(f)
	}
	switch t := v.(type) {
	case []any:
		parts := make([]string, len(t))
		for i, e := range t {
			parts[i] = PyRepr(e)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case map[string]any:
		return pyDictRepr(t)
	default:
		return fmt.Sprintf("%#v", v)
	}
}

func pyDictRepr(m map[string]any) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	// Divergence vs Python (insertion order): Go maps are unordered; sorted
	// keys keep the rendering deterministic. See the file comment.
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, pyStrQuote(k)+": "+PyRepr(m[k]))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// pyStrQuote quotes a string the way Python repr does: prefer single quotes,
// switch to double quotes when the text contains ' but no ", and escape
// backslashes, the chosen quote and control characters.
func pyStrQuote(s string) string {
	quote := byte('\'')
	if strings.Contains(s, "'") && !strings.Contains(s, "\"") {
		quote = '"'
	}
	var b strings.Builder
	b.WriteByte(quote)
	for _, r := range s {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if r == rune(quote) {
				b.WriteByte('\\')
				b.WriteRune(r)
				continue
			}
			if r < 0x20 || (r >= 0x7f && r < 0xa0) {
				b.WriteString(fmt.Sprintf(`\x%02x`, r))
				continue
			}
			b.WriteRune(r)
		}
	}
	b.WriteByte(quote)
	return b.String()
}

// pyFloatRepr formats a float64 the way Python repr(float) does:
//   - "inf"/"-inf"/"nan" for non-finite values;
//   - positional notation with a guaranteed ".0" tail when the decimal point
//     position is in (-4, 16] (0.0001 → "0.0001", 3.0 → "3.0",
//     1234567890123456.0 → "1234567890123456.0");
//   - scientific notation otherwise (1e-05 → "1e-05", 1e16 → "1e+16"),
//     exponent signed and zero-padded to two digits.
//
// Digits come from Go's shortest round-trip formatter, which matches Python's
// repr digit selection (both emit the shortest decimal string that round-trips
// the exact float64).
func pyFloatRepr(f float64) string {
	if math.IsNaN(f) {
		return "nan"
	}
	if math.IsInf(f, 1) {
		return "inf"
	}
	if math.IsInf(f, -1) {
		return "-inf"
	}
	neg := ""
	if math.Signbit(f) {
		neg = "-"
	}
	f = math.Abs(f)

	mant := strconv.FormatFloat(f, 'e', -1, 64) // "d[.ddddd]e±dd"
	expIdx := strings.IndexByte(mant, 'e')
	digits := strings.Replace(mant[:expIdx], ".", "", 1)
	exp10, _ := strconv.Atoi(mant[expIdx+1:])
	decpt := exp10 + 1 // value = 0.<digits> × 10^decpt

	if decpt <= -4 || decpt > 16 {
		// Scientific: mantissa "d[.ddd]" + "e" + sign + >=2 exponent digits.
		var b strings.Builder
		b.WriteString(neg)
		b.WriteByte(digits[0])
		if len(digits) > 1 {
			b.WriteByte('.')
			b.WriteString(digits[1:])
		}
		e := decpt - 1
		sign := "+"
		if e < 0 {
			sign = "-"
			e = -e
		}
		b.WriteString("e" + sign + fmt.Sprintf("%02d", e))
		return b.String()
	}
	// Positional with at least one fractional digit.
	if decpt <= 0 {
		return neg + "0." + strings.Repeat("0", -decpt) + digits
	}
	if decpt >= len(digits) {
		return neg + digits + strings.Repeat("0", decpt-len(digits)) + ".0"
	}
	return neg + digits[:decpt] + "." + digits[decpt:]
}

// pyIntValue reports whether v is an int-typed expression value (Python int
// domain: int, bool is an int subclass, integral json.Number from UseNumber
// decoding) and returns it as int64.
func pyIntValue(v any) (int64, bool) {
	switch t := v.(type) {
	case bool:
		if t {
			return 1, true
		}
		return 0, true
	case int:
		return int64(t), true
	case int8:
		return int64(t), true
	case int16:
		return int64(t), true
	case int32:
		return int64(t), true
	case int64:
		return t, true
	case uint:
		return int64(t), true
	case uint8:
		return int64(t), true
	case uint16:
		return int64(t), true
	case uint32:
		return int64(t), true
	case uint64:
		return int64(t), true
	case json.Number:
		if i, err := strconv.ParseInt(t.String(), 10, 64); err == nil {
			return i, true
		}
		return 0, false
	}
	return 0, false
}

// pyFloatValue reports whether v is a float-typed expression value (Python
// float domain: float32/64 and non-integral json.Number).
func pyFloatValue(v any) (float64, bool) {
	switch t := v.(type) {
	case float32:
		return float64(t), true
	case float64:
		return t, true
	case json.Number:
		if _, err := strconv.ParseInt(t.String(), 10, 64); err == nil {
			return 0, false // integral json.Number is an int, not a float
		}
		if f, err := strconv.ParseFloat(t.String(), 64); err == nil {
			return f, true
		}
		return 0, false
	}
	return 0, false
}

// pyNumericValue reports whether v is numeric (Python int or float domain —
// bool is an int subclass) and returns it as float64.
func pyNumericValue(v any) (float64, bool) {
	if i, ok := pyIntValue(v); ok {
		return float64(i), true
	}
	return pyFloatValue(v)
}
