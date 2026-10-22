package nodes

import (
	"fmt"
	"strconv"
	"strings"
)

// sqlalchemy text() bind-parameter parity (database.py:239
// `conn.execute(text(sql).execution_options(...), bind)`): flows carry
// `:name` placeholders and a params dict; the database/sql drivers do not
// rewrite those, so the node does it here — `:name` becomes `?` (mysql,
// sqlite) or `$n` (postgres, numbered by first appearance; a repeated name
// reuses its number and its argument).
//
// The scanner walks the statement like sqlalchemy's lexer does:
//   - '…' string literals (with '' doubling), "…" quoted identifiers;
//   - `--` line comments and `/* … */` block comments;
//   - postgres dollar-quoted strings `$$…$$` / `$tag$…$tag$`;
//   - `::` casts (a `:` is only a bind marker when followed by a name start);
//   - `\:` escapes a literal colon (sqlalchemy text() escape).
//
// Unknown escapes are left untouched. A `:name` without a value in params is
// a named error before anything is executed (sqlalchemy: "A value is required
// for bind parameter"); extra params not referenced by the statement are
// ignored (same as sqlalchemy).

type sqlParamStyle int

const (
	sqlPlaceholderQMark sqlParamStyle = iota
	sqlPlaceholderDollar
)

// rewriteNamedSQLParams rewrites `:name` bind parameters in sqlText for the
// target's placeholder style and returns the rewritten statement plus the
// positional argument list.
func rewriteNamedSQLParams(sqlText string, named map[string]any, style sqlParamStyle) (string, []any, error) {
	var out strings.Builder
	out.Grow(len(sqlText))
	args := make([]any, 0, len(named))
	position := make(map[string]int, len(named)) // dollar style: name → 1-based placeholder no.

	i := 0
	for i < len(sqlText) {
		c := sqlText[i]
		switch {
		case c == '\'' || c == '"':
			j := scanSQLQuoted(sqlText, i, c)
			out.WriteString(sqlText[i:j])
			i = j
		case c == '-' && i+1 < len(sqlText) && sqlText[i+1] == '-':
			j := strings.IndexByte(sqlText[i:], '\n')
			if j < 0 {
				j = len(sqlText)
			} else {
				j += i
			}
			out.WriteString(sqlText[i:j])
			i = j
		case c == '/' && i+1 < len(sqlText) && sqlText[i+1] == '*':
			j := strings.Index(sqlText[i+2:], "*/")
			if j < 0 {
				j = len(sqlText)
			} else {
				j += i + 4 // consume the closing "*/" as well
			}
			out.WriteString(sqlText[i:j])
			i = j
		case c == '$':
			if j, ok := scanSQLDollarQuoted(sqlText, i); ok {
				out.WriteString(sqlText[i:j])
				i = j
				continue
			}
			out.WriteByte(c)
			i++
		case c == '\\' && i+1 < len(sqlText) && sqlText[i+1] == ':':
			// sqlalchemy text() escape: the backslash is lexer syntax, the
			// colon is literal — emit only the colon.
			out.WriteByte(':')
			i += 2
		case c == ':' && i+1 < len(sqlText) && sqlText[i+1] == ':':
			out.WriteString("::")
			i += 2
		case c == ':' && i+1 < len(sqlText) && isSQLNameStart(sqlText[i+1]):
			name, j := scanSQLName(sqlText, i+1)
			v, ok := named[name]
			if !ok {
				return "", nil, fmt.Errorf("no value for bind parameter :%s", name)
			}
			switch style {
			case sqlPlaceholderDollar:
				n, seen := position[name]
				if !seen {
					n = len(position) + 1
					position[name] = n
					args = append(args, v)
				}
				out.WriteByte('$')
				out.WriteString(strconv.Itoa(n))
			default:
				args = append(args, v)
				out.WriteByte('?')
			}
			i = j
		default:
			out.WriteByte(c)
			i++
		}
	}
	return out.String(), args, nil
}

// scanSQLQuoted returns the index just past the quoted run starting at i
// (sqlText[i] is the quote char; the doubling ” / "" is the only escape,
// which is true for both SQL string literals and quoted identifiers).
func scanSQLQuoted(s string, i int, quote byte) int {
	for j := i + 1; j < len(s); j++ {
		if s[j] != quote {
			continue
		}
		if j+1 < len(s) && s[j+1] == quote {
			j++
			continue
		}
		return j + 1
	}
	return len(s) // unterminated: copy the rest verbatim
}

// scanSQLDollarQuoted recognizes `$$…$$` / `$tag$…$tag$` starting at i and
// returns the index just past the closing tag. A bare `$` (pg positional
// `$1`, json operators) is not a dollar quote.
func scanSQLDollarQuoted(s string, i int) (int, bool) {
	j := i + 1
	for j < len(s) && isSQLNameChar(s[j]) {
		j++
	}
	if j >= len(s) || s[j] != '$' {
		return 0, false
	}
	tag := s[i : j+1]
	rest := s[j+1:]
	end := strings.Index(rest, tag)
	if end < 0 {
		return len(s), true // unterminated: copy the rest verbatim
	}
	return j + 1 + end + len(tag), true
}

func isSQLNameStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isSQLNameChar(c byte) bool {
	return isSQLNameStart(c) || (c >= '0' && c <= '9')
}

// scanSQLName reads a bind name starting at i (s[i] is a name start) and
// returns the name plus the index just past it.
func scanSQLName(s string, i int) (string, int) {
	j := i + 1
	for j < len(s) && isSQLNameChar(s[j]) {
		j++
	}
	return s[i:j], j
}
