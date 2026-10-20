// Package parser defines the plaita expression parser
package parser

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/samber/lo"

	"github.com/jeffkit/plaita-go/pkg/model"
)

const (
	dollarToken = "$"
	splitToken  = "."
)

// ExprType expression type
type ExprType int

// expression types
const (
	ExprValue ExprType = iota
	ExprFunction
	ExprVariable
)

// Expr plaita expression interface
type Expr interface {
	Type() ExprType
	Statement() string
	Value() model.Value
	Variables() []VariableExpr
}

// ValueSource value source
type ValueSource string

// value sources
const (
	ValueSourceInput  ValueSource = "INPUT"
	ValueSourceNode   ValueSource = "NODE"
	ValueSourceParent ValueSource = "PARENT"
	ValueSourceERROR  ValueSource = "ERROR"
	ValueSourceENV    ValueSource = "ENV"
	ValueSourceGlobal ValueSource = "GLOBAL"
	// ValueSourceBranch is the branch target taken by the most recent
	// branching node (plaita system key $BRANCH / context.last_branch). It
	// is a scalar root: it has no sub-paths.
	ValueSourceBranch ValueSource = "BRANCH"
	ValueNULL         ValueSource = "NULL"
	ValueEmptyString  ValueSource = "EMPTYSTRING"
)

var builtinValueSources = []ValueSource{
	ValueSourceInput,
	ValueSourceNode,
	ValueSourceParent,
	ValueSourceERROR,
	ValueSourceENV,
	ValueSourceGlobal,
	ValueSourceBranch,
	ValueNULL,
}

// VariableExpr variable expression
type VariableExpr struct {
	Source                ValueSource
	Variable              string
	WithoutSourceVariable string
}

type funcExpr struct {
	expr      string
	variables []VariableExpr
}

func (f *funcExpr) Type() ExprType            { return ExprFunction }
func (f *funcExpr) Statement() string         { return f.expr }
func (f *funcExpr) Variables() []VariableExpr { return f.variables }
func (f *funcExpr) Value() model.Value        { return model.EmptyValue() }

type valueExpr struct{ v any }

func (v *valueExpr) Type() ExprType            { return ExprValue }
func (v *valueExpr) Statement() string         { return "" }
func (v *valueExpr) Value() model.Value        { return model.NewValue(v.v) }
func (v *valueExpr) Variables() []VariableExpr { return nil }

type variableExpr struct{ variable VariableExpr }

// NewVariableExpr creates a new variable expression
func NewVariableExpr(s string) (Expr, error) {
	return parseVariable(s)
}

func (v *variableExpr) Type() ExprType            { return ExprVariable }
func (v *variableExpr) Statement() string         { return "" }
func (v *variableExpr) Value() model.Value        { return model.EmptyValue() }
func (v *variableExpr) Variables() []VariableExpr { return []VariableExpr{v.variable} }

// Parse 解析字符串表达式,输出解析处理后的新表达式和提取的变量信息
// 表达式中的函数使用$F.前缀,如$F.add(1, 2)，变量使用$前缀，如$INPUT.a,
// 变量前缀目前只支持$INPUT、$NODE、$PARENT三种,函数表达式支持嵌套变量$表达式,
// 若表达式不以$开头，则直接返回表达式值.
// 表达式解析示例：
//  1. $F.add(1, 2) 					=> (`add(1, 2)`, [])
//  2. $F.add(1, $F.add(2, 3)) 			=> (`add(1, add(2, 3))`, [])
//  3. $INPUT.a 						=> (`INPUT.a`, [{"type": "INPUT", "vars": ["a"]}])
//  4. $NODE.a							=> (`NODE.a`, [{"type": "NODE", "vars": ["a"]}])
//  5. $PARENT.a 						=> (`PARENT.a`, [{"type": "PARENT", "vars": ["a"]}])
//  6. $F.add(1, $INPUT.a)				=> (`add(1, INPUT.a)`, [{"type": "INPUT", "vars": ["a"]}])
//  7. $F.add(1, $f.add($INPUT.a, 1))	=> (`add(1, add(INPUT.a, 1))`, [{"type": "INPUT", "vars": ["a"]}])
func Parse(expr string) (Expr, error) {
	expr = normalizeBrackets(expr)
	if countStartDollar(expr) > 1 {
		return nil, errors.New("unable to parse expression: most one start '$' is allowed")
	}
	if !strings.HasPrefix(expr, "$") {
		return &valueExpr{v: expr}, nil
	}

	segments := strings.Split(expr, splitToken)
	if len(segments) == 0 {
		return nil, errors.New("unable to parse expression: unknown format")
	}

	switch segments[0] {
	case "$F":
		return parseFunc(expr)
	default:
		return parseVariable(expr)
	}
}

var (
	dollarRegexp = regexp.MustCompile(`\$(\w+(?:\.(?:\w+|#-?\d+))*)`)
	funcRegexp   = regexp.MustCompile(`\$F\.`)

	// bracketIndexRe matches a bracket index segment of plaita's variable
	// syntax (`X[0]`, `X[-1]`).
	bracketIndexRe = regexp.MustCompile(`(\w+)\[(-?\d+)\]`)

	// quotedLiteralRe matches single- or double-quoted string literals (with
	// backslash escapes) so bracket-looking text inside literals is left
	// alone by the bracket normalization.
	quotedLiteralRe = regexp.MustCompile(`"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'`)

	// trailingCommaRe matches a comma right before a closing paren.
	trailingCommaRe = regexp.MustCompile(`,\s*\)`)
)

// normalizeBrackets rewrites plaita bracket index segments into marked dot
// segments the resolver understands: `X[0]` → `X.#0`, `X[-1]` → `X.#-1`.
// String literals are masked during the rewrite.
func normalizeBrackets(expr string) string {
	if !strings.Contains(expr, "[") {
		return expr
	}
	var masked []string
	mask := quotedLiteralRe.ReplaceAllStringFunc(expr, func(lit string) string {
		masked = append(masked, lit)
		return fmt.Sprintf("\x00%d\x00", len(masked)-1)
	})
	mask = bracketIndexRe.ReplaceAllString(mask, "$1.#$2")
	for i, lit := range masked {
		mask = strings.ReplaceAll(mask, fmt.Sprintf("\x00%d\x00", i), lit)
	}
	return mask
}

func parseFunc(expr string) (*funcExpr, error) {
	expr = funcRegexp.ReplaceAllString(expr, "")

	var variables []VariableExpr
	for _, loc := range dollarRegexp.FindAllStringSubmatchIndex(expr, -1) {
		name := expr[loc[0]+1 : loc[1]]
		if len(name) == 0 {
			return nil, errors.New("invalid function variable, empty variable name")
		}

		source := ValueSource(strings.Split(name, splitToken)[0])
		// Absorb `PARENT.` chains: in `$PARENT.$NODE.node1.a` the inner
		// `$NODE...` is not an independent variable of this runtime context —
		// it resolves through the parent context (goval navigation).
		start := loc[0]
		for source != ValueSourceParent && strings.HasSuffix(expr[:start], string(ValueSourceParent)+splitToken) {
			name = string(ValueSourceParent) + splitToken + name
			start -= len(string(ValueSourceParent)) + len(splitToken)
			source = ValueSourceParent
		}

		variables = append(variables, VariableExpr{
			Source:   source,
			Variable: name,
		})
	}

	statement := singleQuotedToDouble(freeDollar(expr))
	// plaita's grammar allows a trailing comma in argument lists
	// ("trailing-comma default"); goval does not — drop it.
	statement = trailingCommaRe.ReplaceAllString(statement, ")")
	return &funcExpr{expr: statement, variables: variables}, nil
}

// singleQuotedToDouble converts plaita's single-quoted string literals into
// goval's double-quoted form (goval has no single-quote token). Python-style
// escapes inside single quotes are normalized: \' and \" become plain
// characters, then double quotes are re-escaped. Double-quoted literals pass
// through untouched.
func singleQuotedToDouble(statement string) string {
	if !strings.Contains(statement, "'") {
		return statement
	}
	var masked []string
	mask := quotedLiteralRe.ReplaceAllStringFunc(statement, func(lit string) string {
		if !strings.HasPrefix(lit, "'") {
			masked = append(masked, lit)
			return fmt.Sprintf("\x00%d\x00", len(masked)-1)
		}
		inner := lit[1 : len(lit)-1]
		var b strings.Builder
		b.WriteByte('"')
		for i := 0; i < len(inner); i++ {
			ch := inner[i]
			if ch == '\\' && i+1 < len(inner) {
				i++
				switch inner[i] {
				case '\'', '"', '\\':
					ch = inner[i]
				default:
					b.WriteByte('\\')
				}
			}
			if ch == '"' {
				b.WriteByte('\\')
			}
			b.WriteByte(ch)
		}
		b.WriteByte('"')
		masked = append(masked, b.String())
		return fmt.Sprintf("\x00%d\x00", len(masked)-1)
	})
	for i, lit := range masked {
		mask = strings.ReplaceAll(mask, fmt.Sprintf("\x00%d\x00", i), lit)
	}
	return mask
}

func parseVariable(expr string) (*variableExpr, error) {
	expr = freeDollarPrefix(expr)

	if len(expr) == 0 {
		return nil, errors.New("unable to parse expression: invalid variable, must start with '$'")
	}

	segments := strings.Split(expr, splitToken)
	if lo.Contains(builtinValueSources, ValueSource(segments[0])) {
		return &variableExpr{variable: VariableExpr{
			Source:                ValueSource(segments[0]),
			Variable:              expr,
			WithoutSourceVariable: strings.Join(segments[1:], splitToken),
		}}, nil
	}
	return &variableExpr{variable: VariableExpr{
		Variable:              expr,
		WithoutSourceVariable: expr,
	}}, nil
}

func freeDollarPrefix(s string) string {
	return strings.TrimPrefix(s, dollarToken)
}

func freeDollar(s string) string {
	return strings.ReplaceAll(s, dollarToken, "")
}

func countStartDollar(s string) int {
	count := 0
	for _, ch := range s {
		if ch == '$' {
			count++
			continue
		}
		break
	}
	return count
}
