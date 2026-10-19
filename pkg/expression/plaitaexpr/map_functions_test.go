package plaitaexpr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/model"
)

// Semantics pinned here follow plaita's Python implementations
// (expression.py _fn_keys/_fn_values/_fn_items/_fn_get and the
// has_side_effects mutators _fn_set/_fn_delete/_fn_clear). Issue #9: mutators
// refuse live context roots with plaita#69's named error and operate on
// copies; delete/delDictValue is `a.pop(b)` — a missing key is a KeyError.
//
// Documented divergence (see map_functions.go): Python dict views follow
// insertion order; the Go engine's maps are unordered, so keys()/values()/
// items() emit sorted, deterministic order. The tests below only use
// already-sorted maps.

func evalMapExpr(t *testing.T, expr string, input any) (model.Value, error) {
	t.Helper()
	e := &Evaluator{}
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: input,
	})
	return e.Eval(ctx, expr, nil)
}

func Test_keys_eval(t *testing.T) {
	result1, err1 := evalMapExpr(t, `$F.keys($INPUT)`, map[string]any{"a": 10, "b": 20})
	require.NoError(t, err1)
	assert.Equal(t, model.NewValue([]any{"a", "b"}), result1)
	result2, err2 := evalMapExpr(t, `$F.keys($INPUT.a)`, map[string]any{
		"a": map[string]any{"c": 20, "b": 10},
	})
	require.NoError(t, err2)
	assert.Equal(t, model.NewValue([]any{"b", "c"}), result2)
}

func Test_values_eval(t *testing.T) {
	result1, err1 := evalMapExpr(t, `$F.values($INPUT)`, map[string]any{"a": 10, "b": 20})
	require.NoError(t, err1)
	assert.Equal(t, model.NewValue([]any{10, 20}), result1)
	result2, err2 := evalMapExpr(t, `$F.values($INPUT.a)`, map[string]any{
		"a": map[string]any{"c": 20, "b": 10},
	})
	require.NoError(t, err2)
	assert.Equal(t, model.NewValue([]any{10, 20}), result2)
}

func Test_items_eval(t *testing.T) {
	result1, err1 := evalMapExpr(t, `$F.items($INPUT)`, map[string]any{"a": 10, "b": 20})
	require.NoError(t, err1)
	assert.Equal(t, model.NewValue([][]any{{"a", 10}, {"b", 20}}), result1)
	result2, err2 := evalMapExpr(t, `$F.items($INPUT.a)`, map[string]any{
		"a": map[string]any{"c": 20, "b": 10},
	})
	require.NoError(t, err2)
	assert.Equal(t, model.NewValue([][]any{{"b", 10}, {"c", 20}}), result2)
}

func Test_get_eval(t *testing.T) {
	result1, err1 := evalMapExpr(t, `$F.get($INPUT, "a")`, map[string]any{"a": 10, "b": 20})
	require.NoError(t, err1)
	assert.Equal(t, model.NewValue(10), result1)
	result2, err2 := evalMapExpr(t, `$F.get($INPUT.a, "e", 30)`, map[string]any{
		"a": map[string]any{"c": 20, "b": 10},
	})
	require.NoError(t, err2)
	assert.Equal(t, model.NewValue(30), result2)
}

func Test_set_live_root_refused(t *testing.T) {
	// issue #9 acceptance row: `$F.set($INPUT.map, 'c', 3)` is plaita's
	// ValueError (live-root write protection), not a silent rewrite.
	e := &Evaluator{}
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{"a": 10, "b": 20},
	})
	_, err1 := e.Eval(ctx1, `$F.set($INPUT, "a", 30)`, nil)
	assert.ErrorContains(t, err1, "plaita#69 write protection")
	input, _ := runtime.WorkflowRuntimeContext(ctx1).WorkflowInput.(map[string]any)
	assert.Equal(t, 10, input["a"])

	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": map[string]any{"c": 20, "b": 10},
		},
	})
	_, err2 := e.Eval(ctx2, `$F.set($INPUT.a, "b", 30)`, nil)
	assert.ErrorContains(t, err2, "plaita#69 write protection")
	input2, _ := runtime.WorkflowRuntimeContext(ctx2).WorkflowInput.(map[string]any)
	param, _ := input2["a"].(map[string]any)
	assert.Equal(t, 10, param["b"])
}

func Test_mutators_on_fresh_targets_copy(t *testing.T) {
	// A mutator over a FRESH container (a pure producer result) stays legal —
	// Python allows it — but the mutation happens on a copy, so nothing
	// shared is ever written through (issue #9 建议 1+2 combined).
	e := &Evaluator{}
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{"list": []any{1, 2, 3}},
	})
	// pop of a fresh list produced in the same expression: legal, returns 1
	got, err := e.Eval(ctx, `$F.pop($F.append($INPUT.list, 4), 0)`, nil)
	require.NoError(t, err)
	assert.Equal(t, model.NewValue(1), got)
	// the original list is untouched
	input := runtime.WorkflowRuntimeContext(ctx).WorkflowInput.(map[string]any)
	assert.Equal(t, []any{1, 2, 3}, input["list"])
}

func Test_delete_python_semantics(t *testing.T) {
	// delete/delDictValue are Python `a.pop(b)`: a missing key is a KeyError
	// (the old variant silently ignored it). The fresh target here is a
	// json_loads result — the one legal (non-live) target shape.
	_, err1 := evalMapExpr(t, `$F.delete($F.json_loads('{"a": 1}'), "gone")`, map[string]any{})
	assert.ErrorContains(t, err1, "KeyError")

	_, err2 := evalMapExpr(t, `$F.delete($INPUT, "a")`, map[string]any{"a": 10})
	assert.ErrorContains(t, err2, "plaita#69 write protection")
}

func Test_clear_live_root_refused(t *testing.T) {
	// issue #9 acceptance row: `$F.clear($INPUT.map)` is plaita's ValueError.
	e := &Evaluator{}
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{"a": 10, "b": 20},
	})
	_, err1 := e.Eval(ctx1, `$F.clear($INPUT)`, nil)
	assert.ErrorContains(t, err1, "plaita#69 write protection")
	input, _ := runtime.WorkflowRuntimeContext(ctx1).WorkflowInput.(map[string]any)
	assert.Len(t, input, 2)

	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{"a": map[string]any{"c": 20, "b": 10}},
	})
	_, err2 := e.Eval(ctx2, `$F.clear($INPUT.a)`, nil)
	assert.ErrorContains(t, err2, "plaita#69 write protection")
	input2, _ := runtime.WorkflowRuntimeContext(ctx2).WorkflowInput.(map[string]any)
	assert.Len(t, input2["a"].(map[string]any), 2)
}
