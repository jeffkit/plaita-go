package plaitaexpr

// string_functions.go ports plaita's string $F functions with Python str
// semantics (expression.py _fn_lower/_fn_upper/_fn_capitalize/_fn_title/
// _fn_split/_fn_join/_fn_concat/_fn_isDigit):
//
//   - lower/upper do full Unicode case mapping via golang.org/x/text/cases
//     (Python 'straße'.upper() == 'STRASSE' — per-rune Go ToUpper would keep
//     the ß);
//   - capitalize TITLECASES the first character (Python titlecase mapping,
//     issue #41: ß → "Ss" not "SS") and LOWERCASES the rest
//     ('hello WORLD' → 'Hello world');
//   - title is str.title: every non-letter starts a new word, the first
//     letter of each word is TITLECASED and the rest lowercased
//     ("they're" → "They'Re" — x/text's Title caser is only used per single
//     rune, its word segmentation does not split on apostrophes);
//   - split('abc', '') is a ValueError (empty separator);
//   - join requires str elements (TypeError otherwise — the old cast based
//     variant silently stringified non-strings);
//   - concat accepts ANY values and renders them with Python str().

import (
	"fmt"
	"math"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

var pyCaser = cases.Upper(language.Und)
var pyLowerCaser = cases.Lower(language.Und)

// pyTitleCaser provides the Unicode TITLECASE mapping for word-initial
// characters (Python str.capitalize/str.title use it for the first letter,
// NOT uppercase): ß → "Ss", ligature ﬀ → "Ff", Georgian stays put while
// upper maps it to Mtavruli. x/text's Title caser matches Python's titlecase
// mapping on every code point where upper ≠ title (135 of them, probe
// verified against Python 3.13); we only ever feed it single runes, so its
// word segmentation (which differs from str.title) never comes into play.
var pyTitleCaser = cases.Title(language.Und)

func lower(s string) string {
	return pyLowerCaser.String(s)
}

func upper(s string) string {
	// Full Unicode uppercasing: 'straße' → 'STRASSE' (Python str.upper),
	// not per-rune strings.ToUpper which would keep the ß.
	return pyCaser.String(s)
}

func capitalize(s string) string {
	// Python str.capitalize: first character TITLECASED (issue #41: ß →
	// "Ss", not "SS"), the REST lowercased ('hello WORLD' → 'Hello world').
	if s == "" {
		return s
	}
	r, size := utf8.DecodeRuneInString(s)
	if r == utf8.RuneError && size <= 1 {
		return s
	}
	return pyTitleCaser.String(string(r)) + pyLowerCaser.String(s[size:])
}

func title(s string) string {
	// Python str.title: a "word" starts after every non-alphabetic
	// character; its first letter is TITLECASED (issue #41) and the
	// remaining letters lowercased ("they're bill's" → "They'Re Bill'S").
	var b strings.Builder
	prevIsLetter := false
	for _, r := range s {
		if !prevIsLetter && unicode.IsLetter(r) {
			b.WriteString(pyTitleCaser.String(string(r)))
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

// replace is plaita $F.replace(a, b, c) — Python str.replace with the
// result-size gate of _fn_replace (expression.py :2881-2889) run BEFORE the
// allocation (#64): the hit count from strings.Count projects the result
// length (empty pattern inserts between every character), and an
// over-budget projection is a named refusal instead of a 100MB string.
// Lengths are runes, matching Python's "chars".
func replace(s, old, new string) (string, error) {
	la := int64(utf8.RuneCountInString(s))
	lb := int64(utf8.RuneCountInString(old))
	lc := int64(utf8.RuneCountInString(new))
	var hits, projected int64
	if lb == 0 {
		hits = la
		projected = mulClamped(la, lc+1)
		projected = addClamped(projected, lc)
	} else {
		hits = int64(strings.Count(s, old))
		if lc > lb {
			projected = addClamped(la, mulClamped(hits, lc-lb))
		} else {
			projected = la
		}
	}
	if err := checkResultSize(
		fmt.Sprintf("$F.replace(%d chars, %d hits)", la, hits),
		projected, addClamped(addClamped(la, lb), lc), "chars"); err != nil {
		return "", err
	}
	return strings.ReplaceAll(s, old, new), nil
}

// mulClamped/addClamped keep the projection arithmetic in int64 without
// wrapping on absurd inputs (a wrapped projection could sneak past the gate).
func mulClamped(a, b int64) int64 {
	p, ok := intMulOverflow(a, b)
	if ok {
		return math.MaxInt64
	}
	return p
}

func addClamped(a, b int64) int64 {
	s, ok := intAddOverflow(a, b)
	if ok {
		return math.MaxInt64
	}
	return s
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
// `$F.concat(1, 'a')` at the reflect boundary instead. The render size is
// bounded per argument BEFORE rendering (#64, _fn_concat's _render_size
// shape): the projection equals the input size, so only the hard cap binds.
func concat(args ...any) (string, error) {
	total := int64(0)
	for _, arg := range args {
		bound, ok := pyRenderBound(arg)
		if !ok {
			return "", fmt.Errorf("$F.concat cannot bound the rendered size of %s. Refusing to build an unbounded result.",
				pyTypeName(arg))
		}
		total = addClamped(total, bound)
	}
	if err := checkResultSize(fmt.Sprintf("$F.concat(%d args)", len(args)), total, total, "chars"); err != nil {
		return "", err
	}
	parts := make([]string, len(args))
	for i, arg := range args {
		parts[i] = PyStr(arg)
	}
	return strings.Join(parts, ""), nil
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
