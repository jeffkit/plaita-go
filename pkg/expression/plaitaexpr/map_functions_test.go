package plaitaexpr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/model"
)

func Test_keys_eval(t *testing.T) {
	e := &Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": 10,
			"b": 20,
		},
	})
	result1, err1 := e.Eval(ctx1, `$F.keys($INPUT)`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue([]any{"a", "b"}), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": map[string]any{
				"c": 20,
				"b": 10,
			},
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.keys($INPUT.a)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue([]any{"b", "c"}), result2)
}

func Test_values_eval(t *testing.T) {
	e := &Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": 10,
			"b": 20,
		},
	})
	result1, err1 := e.Eval(ctx1, `$F.values($INPUT)`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue([]any{10, 20}), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": map[string]any{
				"c": 20,
				"b": 10,
			},
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.values($INPUT.a)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue([]any{10, 20}), result2)
}

func Test_items_eval(t *testing.T) {
	e := &Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": 10,
			"b": 20,
		},
	})
	result1, err1 := e.Eval(ctx1, `$F.items($INPUT)`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue([][]any{
		{"a", 10},
		{"b", 20},
	}), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": map[string]any{
				"c": 20,
				"b": 10,
			},
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.items($INPUT.a)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue([][]any{
		{"b", 10},
		{"c", 20},
	}), result2)
}

func Test_get_eval(t *testing.T) {
	e := &Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": 10,
			"b": 20,
		},
	})
	result1, err1 := e.Eval(ctx1, `$F.get($INPUT, "a")`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue(10), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": map[string]any{
				"c": 20,
				"b": 10,
			},
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.get($INPUT.a, "e", 30)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue(30), result2)
}

func Test_set_eval(t *testing.T) {
	e := &Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": 10,
			"b": 20,
		},
	})
	_, err1 := e.Eval(ctx1, `$F.set($INPUT, "a", 30)`, nil)
	assert.NoError(t, err1)
	input, _ := runtime.WorkflowRuntimeContext(ctx1).WorkflowInput.(map[string]any)
	assert.Equal(t, 30, input["a"])
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": map[string]any{
				"c": 20,
				"b": 10,
			},
		},
	})
	_, err2 := e.Eval(ctx2, `$F.set($INPUT.a, "b", 30)`, nil)
	assert.NoError(t, err2)
	input, _ = runtime.WorkflowRuntimeContext(ctx2).WorkflowInput.(map[string]any)
	param, _ := input["a"].(map[string]any)
	assert.Equal(t, 30, param["b"])
}

func Test_delete_eval(t *testing.T) {
	e := &Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": 10,
			"b": 20,
		},
	})
	_, err1 := e.Eval(ctx1, `$F.delete($INPUT, "a")`, nil)
	assert.NoError(t, err1)
	input, _ := runtime.WorkflowRuntimeContext(ctx1).WorkflowInput.(map[string]any)
	assert.Equal(t, nil, input["a"])
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": map[string]any{
				"c": 20,
				"b": 10,
			},
		},
	})
	_, err2 := e.Eval(ctx2, `$F.delete($INPUT.a, "b")`, nil)
	assert.NoError(t, err2)
	input, _ = runtime.WorkflowRuntimeContext(ctx2).WorkflowInput.(map[string]any)
	param, _ := input["a"].(map[string]any)
	assert.Equal(t, nil, param["b"])
}

func Test_clear_eval(t *testing.T) {
	e := &Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": 10,
			"b": 20,
		},
	})
	_, err1 := e.Eval(ctx1, `$F.clear($INPUT)`, nil)
	assert.NoError(t, err1)
	input, _ := runtime.WorkflowRuntimeContext(ctx1).WorkflowInput.(map[string]any)
	assert.Equal(t, map[string]any{}, input)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": map[string]any{
				"c": 20,
				"b": 10,
			},
		},
	})
	_, err2 := e.Eval(ctx2, `$F.clear($INPUT.a)`, nil)
	assert.NoError(t, err2)
	input, _ = runtime.WorkflowRuntimeContext(ctx2).WorkflowInput.(map[string]any)
	param, _ := input["a"].(map[string]any)
	assert.Equal(t, map[string]any{}, param)
}
