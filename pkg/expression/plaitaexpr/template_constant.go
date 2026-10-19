package plaitaexpr

// TemplateConstant parses a plaita template constant — the `constant` rule of
// plaita's template grammar (expression_parser.py: boolean | string | number,
// with True/False/true/false/null/None spellings). It backs `{% ... %}`
// fragments that carry a bare literal ("{% true %}" → "True", "{% 1.5 %}" →
// "1.5", "{% None %}" → "None"). The second return reports whether inner is
// a constant at all; unparsable fragments stay literal in the template.
import (
	"strconv"
	"strings"
)

func TemplateConstant(inner string) (any, bool) {
	inner = strings.TrimSpace(inner)
	if inner == "" {
		return nil, false
	}
	switch inner {
	case "True", "true":
		return true, true
	case "False", "false":
		return false, true
	case "None", "null":
		return nil, true
	}
	if c := inner[0]; c == '\'' || c == '"' {
		if len(inner) >= 2 && inner[len(inner)-1] == c {
			return pyUnquote(inner[1 : len(inner)-1]), true
		}
		return nil, false
	}
	if i, err := strconv.ParseInt(inner, 10, 64); err == nil {
		return int(i), true
	}
	if f, err := strconv.ParseFloat(inner, 64); err == nil && looksNumeric(inner) {
		return f, true
	}
	return nil, false
}

// looksNumeric guards against strconv.ParseFloat accepting spellings Python's
// number rule would not ("Inf", "NaN", "0x1p-2").
func looksNumeric(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9':
		case c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-':
		default:
			return false
		}
	}
	return true
}

// pyUnquote resolves Python-style escapes inside a quoted template constant.
func pyUnquote(body string) string {
	if !strings.ContainsRune(body, '\\') {
		return body
	}
	var b strings.Builder
	for i := 0; i < len(body); i++ {
		if body[i] != '\\' || i+1 >= len(body) {
			b.WriteByte(body[i])
			continue
		}
		i++
		switch body[i] {
		case 'n':
			b.WriteByte('\n')
		case 't':
			b.WriteByte('\t')
		case 'r':
			b.WriteByte('\r')
		default:
			b.WriteByte(body[i])
		}
	}
	return b.String()
}
