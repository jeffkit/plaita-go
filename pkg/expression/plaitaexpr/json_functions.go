package plaitaexpr

// json_functions.go ports plaita's $F.json_loads / $F.json_dumps — Python's
// json.loads / json.dumps with DEFAULT arguments (expression.py
// _fn_json_loads/_fn_json_dumps):
//
//   - loads preserves number types: JSON integers decode to int (not
//     float64), floats to float (Python json parses 1 → int, 2.5 → float);
//   - dumps uses Python's defaults: separators (', ', ': ') —
//     '{"b": 2, "a": 1}' —, ensure_ascii (non-ASCII → \uXXXX) and no HTML
//     escaping (Go's encoding/json escapes <>& and emits compact output),
//     and float formatting follows Python's float repr ("2.0", "1e+16").
//
// Documented divergence (issue #22 acceptance note): Python dumps keeps dict
// INSERTION order; the Go engine's maps are unordered (order is lost at JSON
// decode), so keys are emitted SORTED to stay deterministic. Byte-level key
// order for dicts is therefore an intentional, documented divergence; the
// JSON semantics (values, escaping, number formatting) match Python.

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// jsonLoads is plaita $F.json_loads(a) — json.loads with number-type
// restoration (int stays int).
func jsonLoads(s string) (any, error) {
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	// Trailing garbage: json.loads raises Extra data.
	if dec.More() {
		return nil, fmt.Errorf("$F.json_loads: Extra data")
	}
	return restoreJSONNumbers(v), nil
}

func restoreJSONNumbers(v any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, e := range t {
			t[k] = restoreJSONNumbers(e)
		}
		return t
	case []any:
		for i, e := range t {
			t[i] = restoreJSONNumbers(e)
		}
		return t
	case json.Number:
		if i, err := strconv.ParseInt(t.String(), 10, 64); err == nil {
			return i
		}
		if f, err := strconv.ParseFloat(t.String(), 64); err == nil {
			return f
		}
		return t
	default:
		return v
	}
}

// jsonDumps is plaita $F.json_dumps(a) — json.dumps with Python defaults
// (separators, ensure_ascii) on the expression value domain.
func jsonDumps(obj any) (string, error) {
	var b strings.Builder
	if err := pyWriteJSON(&b, obj); err != nil {
		return "", err
	}
	return b.String(), nil
}

func pyWriteJSON(b *strings.Builder, v any) error {
	switch t := v.(type) {
	case nil:
		b.WriteString("null")
		return nil
	case bool:
		if t {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
		return nil
	case string:
		b.WriteString(pyJSONQuote(t))
		return nil
	}
	if i, ok := pyIntValue(v); ok {
		b.WriteString(strconv.FormatInt(i, 10))
		return nil
	}
	if f, ok := pyFloatValue(v); ok {
		b.WriteString(pyJSONFloat(f))
		return nil
	}
	switch t := v.(type) {
	case []any:
		b.WriteByte('[')
		for i, e := range t {
			if i > 0 {
				b.WriteString(", ")
			}
			if err := pyWriteJSON(b, e); err != nil {
				return err
			}
		}
		b.WriteByte(']')
		return nil
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys) // documented divergence: sorted, not insertion order
		b.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(pyJSONQuote(k))
			b.WriteString(": ")
			if err := pyWriteJSON(b, t[k]); err != nil {
				return err
			}
		}
		b.WriteByte('}')
		return nil
	default:
		return fmt.Errorf("$F.json_dumps: Object of type %s is not JSON serializable", pyTypeName(v))
	}
}

// pyJSONFloat renders a float the way Python's json encoder does: float repr
// digits with a ".0" tail kept for integral values ("2.0"), scientific via
// repr ("1e+16"), and the JSON-spec-exceeding Infinity/NaN spellings.
func pyJSONFloat(f float64) string {
	if math.IsInf(f, 1) {
		return "Infinity"
	}
	if math.IsInf(f, -1) {
		return "-Infinity"
	}
	if math.IsNaN(f) {
		return "NaN"
	}
	return pyFloatRepr(f)
}

// pyJSONQuote quotes s the way Python's json encoder does with
// ensure_ascii=True: ", \\ and control characters escaped, every non-ASCII
// code point as \uXXXX (astral plane as surrogate pairs).
func pyJSONQuote(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		default:
			if r < 0x20 {
				fmt.Fprintf(&b, `\u%04x`, r)
				continue
			}
			if r < 0x7f {
				b.WriteByte(byte(r))
				continue
			}
			// ensure_ascii: escape every non-ASCII rune.
			if r > utf8.MaxRune || (r >= 0xd800 && r <= 0xdfff) {
				r = utf8.RuneError
			}
			for _, u := range utf16.Encode([]rune{r}) {
				fmt.Fprintf(&b, `\u%04x`, u)
			}
		}
	}
	b.WriteByte('"')
	return b.String()
}
