package plaitaexpr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/model"
)

func Test_and_eval(t *testing.T) {
	e := &Evaluator{}
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": true,
			"b": true,
			"c": false,
		},
	})
	// true
	result1, err1 := e.Eval(ctx, `$F.and($INPUT.a, $INPUT.b)`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue(true), result1)
	// false
	result2, err2 := e.Eval(ctx, `$F.and($INPUT.a, $INPUT.c)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue(false), result2)
}

func Test_or_eval(t *testing.T) {
	e := &Evaluator{}
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a":     true,
			"b":     false,
			"c":     false,
			"zero":  0,
			"empty": "",
		},
	})
	// true
	result1, err1 := e.Eval(ctx, `$F.or($INPUT.a, $INPUT.b, $INPUT.c)`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue(true), result1)
	// false
	result2, err2 := e.Eval(ctx, `$F.or($INPUT.b, $INPUT.c)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue(false), result2)
}

func Test_or_all_falsy_returns_false(t *testing.T) {
	// issue #26 acceptance: `$F.or(0,'')` returns the BOOL false (Python
	// _fn_or's next((arg for arg in args if arg), False)), not the last
	// argument's value.
	e := &Evaluator{}
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{"zero": 0, "empty": "", "n": nil},
	})
	cases := []struct {
		name string
		expr string
	}{
		{"two falsy args", `$F.or(0, '')`},
		{"three falsy args", `$F.or(0, '', '')`},
		{"nil and empty", `$F.or($INPUT.n, $INPUT.empty)`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := e.Eval(ctx, c.expr, nil)
			assert.NoError(t, err)
			assert.Equal(t, model.NewValue(false), got)
		})
	}
	// first-truthy still returns the VALUE
	got, err := e.Eval(ctx, `$F.or(0, 'x')`, nil)
	assert.NoError(t, err)
	assert.Equal(t, model.NewValue("x"), got)
	// middle truthy
	got2, err2 := e.Eval(ctx, `$F.or('', 'm', 'z')`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue("m"), got2)
	// all truthy → first
	got3, err3 := e.Eval(ctx, `$F.or('a', 'b')`, nil)
	assert.NoError(t, err3)
	assert.Equal(t, model.NewValue("a"), got3)
}

func Test_not_eval(t *testing.T) {
	e := &Evaluator{}
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": true,
			"b": false,
		},
	})
	// true
	result1, err1 := e.Eval(ctx, `$F.not($INPUT.a)`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue(false), result1)
	// false
	result2, err2 := e.Eval(ctx, `$F.not($INPUT.b)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue(true), result2)
}
