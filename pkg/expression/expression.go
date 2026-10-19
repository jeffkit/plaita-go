// Package expression provides expression evaluation
package expression

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/spf13/cast"

	"github.com/jeffkit/plaita-go/pkg/expression/plaitaexpr"
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

// Eval evaluates the given expression.
func Eval(ctx context.Context, expr any) (any, error) {
	switch e := expr.(type) {
	case string:
		re := regexp.MustCompile(`\{%([^\{\}]+)\%}`)
		matches := re.FindAllString(e, -1)
		for _, match := range matches {
			tmp := strings.TrimPrefix(match, "{%")
			tmp = strings.TrimSuffix(tmp, "%}")
			result, err := defaultEvaluator.Eval(ctx, tmp, nil)
			if err != nil {
				return nil, err
			}
			e = strings.Replace(e, match, cast.ToString(result.AsAny()), 1)
		}
		v, err := defaultEvaluator.Eval(ctx, e, nil)
		if err != nil {
			return nil, err
		}
		return v.AsAny(), nil
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
