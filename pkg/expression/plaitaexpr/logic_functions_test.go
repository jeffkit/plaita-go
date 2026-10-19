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
			"a": true,
			"b": false,
			"c": false,
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
