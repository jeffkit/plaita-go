package plaitaexpr

// string_functions.go ports plaita's string $F functions with Python str
// semantics (expression.py _fn_lower/_fn_upper/_fn_capitalize/_fn_title/
// _fn_split/_fn_join/_fn_concat/_fn_isDigit):
//
//   - lower/upper do full Unicode case mapping via golang.org/x/text/cases
//     (Python 'straße'.upper() == 'STRASSE' — per-rune Go ToUpper would keep
//     the ß);
//   - capitalize uppercases the first character and LOWERCASES the rest
//     ('hello WORLD' → 'Hello world');
//   - title is str.title: every non-letter starts a new word, the first
//     letter of each word is uppercased and the rest lowercased
//     ("they're" → "They'Re", x/text's Title does not split on apostrophes);
//   - split('abc', '') is a ValueError (empty separator);
//   - join requires str elements (TypeError otherwise — the old cast based
//     variant silently stringified non-strings);
//   - concat accepts ANY values and renders them with Python str().

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

var pyCaser = cases.Upper(language.Und)
var pyLowerCaser = cases.Lower(language.Und)

func lower(s string) string {
	return pyLowerCaser.String(s)
}

func upper(s string) string {
	// Full Unicode uppercasing: 'straße' → 'STRASSE' (Python str.upper),
	// not per-rune strings.ToUpper which would keep the ß.
	return pyCaser.String(s)
}

func capitalize(s string) string {
	// Python str.capitalize: first character uppercased, the REST lowercased.
	if s == "" {
		return s
	}
	r, size := utf8.DecodeRuneInString(s)
	if r == utf8.RuneError && size <= 1 {
		return s
	}
	return pyCaser.String(string(r)) + pyLowerCaser.String(s[size:])
}

func title(s string) string {
	// Python str.title: a "word" starts after every non-alphabetic
	// character; its first letter is uppercased and the remaining letters
	// lowercased ("they're bill's" → "They'Re Bill'S").
	var b strings.Builder
	prevIsLetter := false
	for _, r := range s {
		if !prevIsLetter && unicode.IsLetter(r) {
			b.WriteString(pyCaser.String(string(r)))
		} else {
			b.WriteString(pyLowerCaser.String(string(r)))
		}
		prevIsLetter = unicode.IsLetter(r)
	}
	return b.String()
}

func strip(s string) string {
	return strings.TrimSpace(s)
}

func lstrip(s string) string {
	return strings.TrimLeftFunc(s, unicode.IsSpace)
}

func rstrip(s string) string {
	return strings.TrimRightFunc(s, unicode.IsSpace)
}

func replace(s, old, new string) string {
	return strings.ReplaceAll(s, old, new)
}

// split is plaita $F.split(s, sep) — Python str.split: an empty separator is
// a ValueError (Go strings.Split used to split into bytes instead).
func split(s, sep string) ([]any, error) {
	if sep == "" {
		return nil, fmt.Errorf("$F.split: empty separator")
	}
	parts := strings.Split(s, sep)
	out := make([]any, len(parts))
	for i, p := range parts {
		out[i] = p
	}
	return out, nil
}

// join is plaita $F.join(a, b) — Python `b.join(a)`: every element must be a
// str instance, anything else is a TypeError (the cast.ToString variant
// silently stringified 1 → "1", diverging from Python).
func join(items []any, sep string) (string, error) {
	strs := make([]string, len(items))
	for i, v := range items {
		s, ok := v.(string)
		if !ok {
			return "", fmt.Errorf("$F.join: sequence item %d: expected str instance, %s found",
				i, pyTypeName(v))
		}
		strs[i] = s
	}
	return strings.Join(strs, sep), nil
}

func startswith(s, prefix string) bool {
	return strings.HasPrefix(s, prefix)
}

func endswith(s, suffix string) bool {
	return strings.HasSuffix(s, suffix)
}

// concat is plaita $F.concat(*args) — Python
// `"".join(str(arg) for arg in args)`: every argument is rendered with
// Python str() semantics (numbers keep their Python shape, None is "None"),
// then concatenated. The typed (...string) variant panicked on
// `$F.concat(1, 'a')` at the reflect boundary instead.
func concat(args ...any) string {
	parts := make([]string, len(args))
	for i, arg := range args {
		parts[i] = PyStr(arg)
	}
	return strings.Join(parts, "")
}

func isDigit(s string) bool {
	if s == "" {
		return false
	}

	for _, r := range s {
		if !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}
