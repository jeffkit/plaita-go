package plaitaexpr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/model"
)

func Test_length_eval(t *testing.T) {
	e := Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: []any{"1", "2"},
	})
	result1, err1 := e.Eval(ctx1, `$F.len($INPUT)`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue(2), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": []any{"1", "2"},
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.length($INPUT.a)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue(2), result2)
}

func Test_index_eval(t *testing.T) {
	e := Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: []any{"1", "2"},
	})
	result1, err1 := e.Eval(ctx1, `$F.index($INPUT, "1")`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue(0), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": []any{"1", "2"},
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.index($INPUT.a, "2")`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue(1), result2)
	// not exit item
	ctx3 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: []any{"1", "2"},
	})
	result3, err3 := e.Eval(ctx3, `$F.index($INPUT, "3")`, nil)
	assert.NoError(t, err3)
	assert.Equal(t, model.NewValue(-1), result3)
}

func Test_slice_eval(t *testing.T) {
	e := Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: []any{"1", "2"},
	})
	result1, err1 := e.Eval(ctx1, `$F.slice($INPUT, 0, 1)`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue([]any{"1"}), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": []any{"1", "2"},
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.slice($INPUT.a, 0, 1)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue([]any{"1"}), result2)
	// invalid start and end
	ctx3 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: []any{"1", "2"},
	})
	result3, err3 := e.Eval(ctx3, `$F.slice($INPUT, -1, -2)`, nil)
	assert.NoError(t, err3)
	assert.Equal(t, model.NewValue([]any(nil)), result3)
}

func Test_append_eval(t *testing.T) {
	e := Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: []any{"1", "2"},
	})
	result1, err1 := e.Eval(ctx1, `$F.append($INPUT, "3")`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue([]any{"1", "2", "3"}), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": []any{"1", "2"},
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.append($INPUT.a, "3")`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue([]any{"1", "2", "3"}), result2)
}

func Test_extend_eval(t *testing.T) {
	e := Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: []any{"1", "2"},
	})
	result1, err1 := e.Eval(ctx1, `$F.extend($INPUT, ["3"])`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue([]any{"1", "2", "3"}), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": []any{"1", "2"},
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.extend($INPUT.a, ["3"])`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue([]any{"1", "2", "3"}), result2)
}

func Test_insert_eval(t *testing.T) {
	e := Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: []any{"1", "2"},
	})
	result1, err1 := e.Eval(ctx1, `$F.insert($INPUT, 1, "3")`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue([]any{"1", "3", "2"}), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": []any{"1", "2"},
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.insert($INPUT.a, 1, "3")`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue([]any{"1", "3", "2"}), result2)
}

func Test_remove_eval(t *testing.T) {
	e := Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: []any{"1", "2"},
	})
	result1, err1 := e.Eval(ctx1, `$F.remove($INPUT, "1")`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue([]any{"2"}), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": []any{"1", "2"},
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.remove($INPUT.a, "1")`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue([]any{"2"}), result2)
}

func Test_sort_eval(t *testing.T) {
	e := Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: []any{"2", "1"},
	})
	result1, err1 := e.Eval(ctx1, `$F.sort($INPUT)`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue([]any{"1", "2"}), result1)
	// sort string
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": []any{"2", "1"},
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.sort($INPUT.a)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue([]any{"1", "2"}), result2)
	// sort int
	ctx3 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": []any{2, 1},
		},
	})
	result3, err3 := e.Eval(ctx3, `$F.sort($INPUT.a)`, nil)
	assert.NoError(t, err3)
	assert.Equal(t, model.NewValue([]any{1, 2}), result3)
	// sort float
	ctx4 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": []any{2.1, 1.1, 3},
		},
	})
	result4, err4 := e.Eval(ctx4, `$F.sort($INPUT.a, "", true)`, nil)
	assert.NoError(t, err4)
	assert.Equal(t, model.NewValue([]any{3, 2.1, 1.1}), result4)
	// sort map[string]any
	ctx5 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": []any{
				map[string]any{"a": 2, "b": 1},
				map[string]any{"a": 1, "b": 2},
			},
		},
	})
	result5, err5 := e.Eval(ctx5, `$F.sort($INPUT.a, "a", false)`, nil)
	assert.NoError(t, err5)
	assert.Equal(t, model.NewValue([]any{
		map[string]any{"a": 1, "b": 2},
		map[string]any{"a": 2, "b": 1},
	}), result5)
	// sort struct
	ctx6 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": []any{
				struct {
					A int
					B int
				}{A: 2, B: 1},
				struct {
					A int
					B int
				}{A: 1, B: 2},
			},
		},
	})
	result6, err6 := e.Eval(ctx6, `$F.sort($INPUT.a, "A", false)`, nil)
	assert.NoError(t, err6)
	assert.Equal(t, model.NewValue([]any{
		struct {
			A int
			B int
		}{A: 1, B: 2},
		struct {
			A int
			B int
		}{A: 2, B: 1},
	}), result6)
}

func Test_reverse_eval(t *testing.T) {
	e := Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: []any{"1", "2"},
	})
	result1, err1 := e.Eval(ctx1, `$F.reverse($INPUT)`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue([]any{"2", "1"}), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": []any{"1", "2"},
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.reverse($INPUT.a)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue([]any{"2", "1"}), result2)
}

func Test_getListItem_eval(t *testing.T) {
	e := Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: []any{"1", "2"},
	})
	result1, err1 := e.Eval(ctx1, `$F.getListItem($INPUT, 1)`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue("2"), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": []any{"1", "2"},
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.getListItem($INPUT.a, 1)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue("2"), result2)
}

func Test_setListItem_eval(t *testing.T) {
	e := Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: []any{"1", "2"},
	})
	_, err1 := e.Eval(ctx1, `$F.setListItem($INPUT, 1, "3")`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, []any{"1", "3"}, runtime.WorkflowRuntimeContext(ctx1).WorkflowInput)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": []any{"1", "2"},
		},
	})
	_, err2 := e.Eval(ctx2, `$F.setListItem($INPUT.a, 1, "3")`, nil)
	assert.NoError(t, err2)
	input := runtime.WorkflowRuntimeContext(ctx2).WorkflowInput.(map[string]any)
	assert.Equal(t, []any{"1", "3"}, input["a"])
}
