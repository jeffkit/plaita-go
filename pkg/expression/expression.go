// Package expression provides expression evaluation
package expression

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/jeffkit/plaita-go/pkg/expression/plaitaexpr"
	"github.com/jeffkit/plaita-go/pkg/expression/plaitaexpr/parser"
	"github.com/jeffkit/plaita-go/pkg/model"
)

// Evaluator is a common interface for performing expression evaluations.
// It can be implemented for multiple different expression evaluation libraries, allowing for easy change or replacement
// of evaluation libraries within an application without modifying other code using the interface.
type Evaluator interface {
	// Eval method takes an expression string and an optional map of parameters (if the expression requires
	// parameters), and returns a result (usually of type `any` to accept any type of result) and an error (if
	// any occurs during the evaluation).
	Eval(context context.Context, expression string, parameters map[string]any) (model.Value, error)
}

var _ Evaluator = new(NoopEvaluator)

// NoopEvaluator is an evaluator that does nothing.
type NoopEvaluator struct{}

// Eval implement Evaluator interface for NoopEvaluator, but it does nothing.
func (NoopEvaluator) Eval(context.Context, string, map[string]any) (model.Value, error) {
	return model.EmptyValue(), nil
}

var defaultEvaluator Evaluator = plaitaexpr.NewEvaluator()

// expressionPrefix is the prefix that marks a whole string as an expression
// (plaita's expression prefix "$").
const expressionPrefix = "$"

// templateFragmentRe matches a single `{% ... %}` interpolation fragment.
// The fragment body must not contain braces — plaita's grammar only matches
// braced fragments whose inner expression parses, and a nested `{%` inside a
// fragment body is not valid interpolation syntax.
var templateFragmentRe = regexp.MustCompile(`\{%([^\{\}]+)%\}`)

// Eval evaluates the given expression following plaita's evaluation rules
// (see plaita/core/expression_parser.py, ExpressionParser._eval):
//
//   - a string starting with "$" is a whole expression: it is parsed and
//     evaluated, and the result keeps the expression's native type (maps,
//     lists and numbers are returned as-is, not stringified);
//   - a string containing `{% ... %}` fragments is a template: every fragment
//     whose inner expression starts with the "$" prefix is evaluated and
//     interpolated into the text as a string (multi-line strings and multiple
//     fragments per string are supported); fragments that do not start with
//     "$" or do not parse as an expression stay literal, mirroring plaita's
//     "interpolation only fires on a parsable prefixed expression" rule;
//   - any other string is plain text and is returned unchanged;
//   - maps and lists are evaluated recursively element by element;
//   - non-string scalars pass through unchanged.
func Eval(ctx context.Context, expr any) (any, error) {
	switch e := expr.(type) {
	case nil:
		return nil, nil
	case string:
		return evalString(ctx, e)
	case model.Value:
		return Eval(ctx, e.AsAny())
	case map[string]any:
		m := make(map[string]any, len(e))
		for k, v := range e {
			vv, err := Eval(ctx, v)
			if err != nil {
				return nil, fmt.Errorf("failed to eval expression %q: %v", v, err)
			}
			m[k] = vv
		}
		return m, nil
	case []any:
		res := make([]any, 0, len(e))
		for _, v := range e {
			vv, err := Eval(ctx, v)
			if err != nil {
				return nil, fmt.Errorf("failed to eval expression %q: %v", v, err)
			}
			res = append(res, vv)
		}
		return res, nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, bool:
		return e, nil
	default:
		return nil, fmt.Errorf("invalid expression type: %T", expr)
	}
}

// evalString dispatches a string value between whole-expression evaluation,
// template interpolation and plain-text passthrough (plaita
// ExpressionParser._eval / _eval_template semantics).
func evalString(ctx context.Context, s string) (any, error) {
	if strings.HasPrefix(s, expressionPrefix) {
		v, err := defaultEvaluator.Eval(ctx, s, nil)
		if err != nil {
			return nil, err
		}
		return v.AsAny(), nil
	}
	if strings.Contains(s, "{%") {
		return evalTemplateString(ctx, s)
	}
	return s, nil
}

// evalTemplateString interpolates `{% ... %}` fragments into the literal
// text: each fragment whose inner expression is a plaita expression (a `$`-
// prefixed variable/function, or a bare constant — true/false/None/null,
// numbers, quoted strings — mirroring plaita's `constant | function_call |
// variable` template grammar) is evaluated and spliced in as its Python
// str() rendering; every other fragment (unparsable or unclosed) stays
// literal, mirroring plaita's _eval_template which only rewrites matched
// interpolations and returns unmatched strings unchanged. An evaluation error
// of a *matched* fragment propagates (plaita lets the thunk's
// KeyError/TypeError escape the template as well).
func evalTemplateString(ctx context.Context, s string) (any, error) {
	var b strings.Builder
	last := 0
	for _, loc := range templateFragmentRe.FindAllStringSubmatchIndex(s, -1) {
		raw := s[loc[0]:loc[1]]
		b.WriteString(s[last:loc[0]])
		last = loc[1]
		inner := strings.TrimSpace(s[loc[2]:loc[3]])
		var v any
		switch {
		case strings.HasPrefix(inner, expressionPrefix):
			// A fragment that does not parse is not an interpolation match at
			// all (plaita's scanString skips it) — keep it literal.
			if _, err := parser.Parse(inner); err != nil {
				b.WriteString(raw)
				continue
			}
			evaluated, err := defaultEvaluator.Eval(ctx, inner, nil)
			if err != nil {
				return nil, err
			}
			v = evaluated.AsAny()
		default:
			// plaita's template grammar accepts bare constants too
			// ("{% true %}" → "True"); anything unparsable stays literal.
			constant, ok := plaitaexpr.TemplateConstant(inner)
			if !ok {
				b.WriteString(raw)
				continue
			}
			v = constant
		}
		// Python str(value): None → "None", True/False, dicts/lists as
		// Python repr — never cast.ToString's silent "" for containers.
		b.WriteString(plaitaexpr.PyStr(v))
	}
	b.WriteString(s[last:])
	return b.String(), nil
}
