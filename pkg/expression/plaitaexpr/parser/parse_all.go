package parser

import (
	"fmt"
	"strings"
)

// parse_all equivalence for the variable branch (#63): pyparsing's
// `parse_string(value, parse_all=True)` (plaita expression_parser.py
// _compile_prefix) refuses anything the variable grammar cannot consume
// completely. Parse used to swallow the leftover text into the last path
// segment, so `$INPUT.a + 1` / `$INPUT.age > 18` silently evaluated to nil
// while Python failed the node with a ParseException.

// nameTokenCharset mirrors plaita's `name_token = Word(alphanums + "_-$")`:
// the charset deliberately includes `-` ($INPUT.a-b is a field read) and `$`
// ($INPUT.a$b misses like any other field — it is NOT a parse error).
const nameTokenCharset = "_-$"

// indexSegmentMark marks a bracket-index segment already normalized by
// normalizeBrackets (`X[0]` → `X.#0`).
const indexSegmentMark = "#"

// validateVariablePath checks that expr is fully consumed by the plaita
// variable grammar: a `$`-rooted (or, for $PARENT rewrites, bare) name token
// followed by dotted segments (`.` field / `.N` dot-int / `.#N` normalized
// bracket index) or raw `[N]` bracket segments. Trailing text is a named
// parse error. Degenerate shapes that parseVariable already rejects
// (the empty string, a bare `$`) pass through so their historical errors stay.
func validateVariablePath(expr string) error {
	s := strings.TrimPrefix(expr, dollarToken)
	s = s[scanNameToken(s):]
	for s != "" {
		switch {
		case strings.HasPrefix(s, splitToken): // "." field / dot-int / "#N" index
			s = s[len(splitToken):]
			if strings.HasPrefix(s, indexSegmentMark) {
				s = s[len(indexSegmentMark):]
				rest, ok := cutSignedInt(s)
				if !ok {
					return trailingTextError(s)
				}
				s = rest
				continue
			}
			n := scanNameToken(s)
			if n == 0 {
				return trailingTextError(s)
			}
			s = s[n:]
		case strings.HasPrefix(s, "["): // raw bracket index, pre-normalization shape
			s = s[len("["):]
			rest, ok := cutBracketIndex(s)
			if !ok {
				return trailingTextError(s)
			}
			s = rest
		default:
			return trailingTextError(s)
		}
	}
	return nil
}

func trailingTextError(rest string) error {
	return fmt.Errorf("unable to parse expression: unexpected trailing text %q", rest)
}

// scanNameToken returns the length of the leading name-token run.
func scanNameToken(s string) int {
	i := 0
	for i < len(s) && isNameTokenByte(s[i]) {
		i++
	}
	return i
}

func isNameTokenByte(b byte) bool {
	switch {
	case b >= 'a' && b <= 'z', b >= 'A' && b <= 'Z', b >= '0' && b <= '9':
		return true
	}
	return strings.IndexByte(nameTokenCharset, b) >= 0
}

// cutSignedInt consumes an optional `-` followed by at least one digit and
// returns the remainder.
func cutSignedInt(s string) (string, bool) {
	if strings.HasPrefix(s, "-") {
		s = s[len("-"):]
	}
	n := scanDigits(s)
	if n == 0 {
		return s, false
	}
	return s[n:], true
}

// cutBracketIndex consumes `[-?]digits]` and returns the remainder.
func cutBracketIndex(s string) (string, bool) {
	digits := s
	if strings.HasPrefix(digits, "-") {
		digits = digits[len("-"):]
	}
	n := scanDigits(digits)
	if n == 0 || n >= len(digits) || digits[n] != ']' {
		return s, false
	}
	return digits[n+1:], true
}

func scanDigits(s string) int {
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	return i
}
