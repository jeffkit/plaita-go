package plaitaexpr

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/samber/lo"
	"github.com/spf13/cast"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

func lower(s string) string {
	return strings.ToLower(s)
}

func upper(s string) string {
	return strings.ToUpper(s)
}

func capitalize(s string) string {
	// capitalize the first letter of string
	r, size := utf8.DecodeRuneInString(s)
	if r == utf8.RuneError {
		return s
	}
	return string(unicode.ToTitle(r)) + s[size:]
}

func title(s string) string {
	return cases.Title(language.Und).String(s)
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

func split(s, sep string) []any {
	return lo.Map(strings.Split(s, sep), func(item string, index int) any {
		return item
	})
}

func join(s []any, sep string) string {
	strSlice := lo.Map(s, func(v any, _ int) string {
		return cast.ToString(v)
	})
	return strings.Join(strSlice, sep)
}

func startswith(s, prefix string) bool {
	return strings.HasPrefix(s, prefix)
}

func endswith(s, suffix string) bool {
	return strings.HasSuffix(s, suffix)
}

func concat(args ...string) string {
	var sb strings.Builder
	for _, arg := range args {
		sb.WriteString(arg)
	}
	return sb.String()
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
