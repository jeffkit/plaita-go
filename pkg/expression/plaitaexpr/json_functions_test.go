package plaitaexpr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/model"
)

func Test_jsonLoads_eval(t *testing.T) {
	e := &Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: `{"a": 10, "b": 20}`,
	})
	result1, err1 := e.Eval(ctx1, `$F.json_loads($INPUT)`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue(map[string]any{"a": float64(10), "b": float64(20)}), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": `{"b": 20, "c": 30}`,
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.json_loads($INPUT.a)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue(map[string]any{"b": float64(20), "c": float64(30)}), result2)
}

func Test_jsonDumps_eval(t *testing.T) {
	e := &Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": 10,
			"b": 20,
		},
	})
	result1, err1 := e.Eval(ctx1, `$F.json_dumps($INPUT)`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue(`{"a":10,"b":20}`), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": map[string]any{
				"b": 20,
				"c": 30,
			},
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.json_dumps($INPUT.a)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue(`{"b":20,"c":30}`), result2)
}
