package plaitaexpr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/model"
)

func Test_add_eval(t *testing.T) {
	e := &Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: 10,
	})
	result1, err1 := e.Eval(ctx1, `$F.add($INPUT, 20)`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue(30.0), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": 10,
			"b": 20,
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.add($INPUT.a, $INPUT.b)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue(30.0), result2)
}

func Test_sub_eval(t *testing.T) {
	e := &Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: 10,
	})
	result1, err1 := e.Eval(ctx1, `$F.sub($INPUT, 20)`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue(-10.0), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": 30,
			"b": 20,
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.sub($INPUT.a, $INPUT.b)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue(10.0), result2)
}

func Test_mul_eval(t *testing.T) {
	e := &Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: -10,
	})
	result1, err1 := e.Eval(ctx1, `$F.mul($INPUT, 20)`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue(-200.0), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": 10,
			"b": 20,
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.mul($INPUT.a, $INPUT.b)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue(200.0), result2)
}

func Test_div_eval(t *testing.T) {
	e := &Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: 10,
	})
	result1, err1 := e.Eval(ctx1, `$F.div($INPUT, 20)`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue(0.5), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": 10,
			"b": 20,
			"c": 0,
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.div($INPUT.a, $INPUT.b)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue(0.5), result2)
	// divide by zero
	result3, err3 := e.Eval(ctx2, `$F.div($INPUT.a, $INPUT.c)`, nil)
	assert.NoError(t, err3)
	assert.Equal(t, model.NewValue(0.0), result3)
}

func Test_mod_eval(t *testing.T) {
	e := &Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: 10,
	})
	result1, err1 := e.Eval(ctx1, `$F.mod($INPUT, 20)`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue(10.0), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": 7,
			"b": 3,
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.mod($INPUT.a, $INPUT.b)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue(1.0), result2)
}

func Test_pow_eval(t *testing.T) {
	e := &Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: 10,
	})
	result1, err1 := e.Eval(ctx1, `$F.pow($INPUT, 2)`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue(100.0), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": 7,
			"b": 3,
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.pow($INPUT.a, $INPUT.b)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue(343.0), result2)
}

func Test_abs_eval(t *testing.T) {
	e := &Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: -10,
	})
	result1, err1 := e.Eval(ctx1, `$F.abs($INPUT)`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue(10.0), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": -7,
			"b": 3,
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.abs($INPUT.b)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue(3.0), result2)
}

func Test_ceil_eval(t *testing.T) {
	e := &Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: 10.2,
	})
	result1, err1 := e.Eval(ctx1, `$F.ceil($INPUT)`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue(11.0), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": 7.2,
			"b": 3,
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.ceil($INPUT.a)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue(8.0), result2)
}

func Test_floor_eval(t *testing.T) {
	e := &Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: 10.2,
	})
	result1, err1 := e.Eval(ctx1, `$F.floor($INPUT)`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue(10.0), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": 7.2,
			"b": 3,
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.floor($INPUT.a)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue(7.0), result2)
}

func Test_round_eval(t *testing.T) {
	e := &Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: 10.2,
	})
	result1, err1 := e.Eval(ctx1, `$F.round($INPUT, 1)`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue(10.2), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": 7.6,
			"b": 3,
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.round($INPUT.a, 0)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue(8.0), result2)
}

func Test_trunc_eval(t *testing.T) {
	e := &Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: 10.2,
	})
	result1, err1 := e.Eval(ctx1, `$F.trunc($INPUT)`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue(10.0), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": 7.6,
			"b": 3,
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.trunc($INPUT.a)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue(7.0), result2)
}

func Test_sqrt_eval(t *testing.T) {
	e := &Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: 4,
	})
	result1, err1 := e.Eval(ctx1, `$F.sqrt($INPUT)`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue(2.0), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": 9,
			"b": 3,
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.sqrt($INPUT.a)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue(3.0), result2)
}
