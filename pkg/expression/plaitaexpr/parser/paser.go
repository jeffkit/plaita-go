// Package parser defines plaita expression parser
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
	dollarRegexp = regexp.MustCompile(`\$(\w+(\.\w+)*)`)
	funcRegexp   = regexp.MustCompile(`\$F\.`)
)

func parseFunc(expr string) (*funcExpr, error) {
	expr = funcRegexp.ReplaceAllString(expr, "")

	var variables []VariableExpr
	for _, match := range dollarRegexp.FindAllStringSubmatch(expr, -1) {
		if len(match) < 2 {
			return nil, fmt.Errorf("unable to parse expression: invalid variable %s", match)
		}
		if len(match[1]) == 0 {
			return nil, errors.New("invalid function variable, empty variable name")
		}

		variables = append(variables, VariableExpr{
			Source:   ValueSource(strings.Split(match[1], splitToken)[0]),
			Variable: match[1],
		})
	}

	return &funcExpr{expr: freeDollar(expr), variables: variables}, nil
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
