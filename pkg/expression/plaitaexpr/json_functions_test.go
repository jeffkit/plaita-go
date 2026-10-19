package plaitaexpr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/model"
)

// JSON semantics pinned here follow plaita's Python implementations
// (expression.py _fn_json_loads = json.loads, _fn_json_dumps = json.dumps
// with default arguments). Issue #22: loads keeps int/float distinct, dumps
// uses Python separators (', ', ': '), ensure_ascii escaping and no HTML
// escaping. Dict KEY ORDER is a documented divergence: Python preserves
// insertion order, the Go engine's maps are unordered, so keys are emitted
// sorted for determinism.

func evalJSONExpr(t *testing.T, expr string, input any) (model.Value, error) {
	t.Helper()
	e := &Evaluator{}
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: input,
	})
	return e.Eval(ctx, expr, nil)
}

func Test_jsonLoads_eval(t *testing.T) {
	result1, err1 := evalJSONExpr(t, `$F.json_loads($INPUT)`, `{"a": 10, "b": 20}`)
	require.NoError(t, err1)
	// issue #22 acceptance: integers stay int (not float64)
	assert.Equal(t, model.NewValue(map[string]any{"a": int64(10), "b": int64(20)}), result1)
	result2, err2 := evalJSONExpr(t, `$F.json_loads($INPUT.a)`, map[string]any{
		"a": `{"b": 20, "c": 30}`,
	})
	require.NoError(t, err2)
	assert.Equal(t, model.NewValue(map[string]any{"b": int64(20), "c": int64(30)}), result2)
}

func Test_jsonLoads_number_types(t *testing.T) {
	// issue #22 acceptance row: json_loads('[1, 2.5]') → [1, 2.5] with the
	// int/float distinction preserved (the old decode collapsed to float64).
	got, err := jsonLoads("[1, 2.5]")
	require.NoError(t, err)
	assert.Equal(t, []any{int64(1), 2.5}, got)
}

func Test_jsonDumps_eval(t *testing.T) {
	// Python json.dumps({"a": 10, "b": 20}) == '{"a": 10, "b": 20}':
	// separators are (', ', ': ') — the old encoding/json output was compact.
	result1, err1 := evalJSONExpr(t, `$F.json_dumps($INPUT)`, map[string]any{"a": 10, "b": 20})
	require.NoError(t, err1)
	assert.Equal(t, model.NewValue(`{"a": 10, "b": 20}`), result1)
	result2, err2 := evalJSONExpr(t, `$F.json_dumps($INPUT.a)`, map[string]any{
		"a": map[string]any{"b": 20, "c": 30},
	})
	require.NoError(t, err2)
	assert.Equal(t, model.NewValue(`{"b": 20, "c": 30}`), result2)
}

func Test_jsonDumps_python_escaping(t *testing.T) {
	cases := []struct {
		name string
		obj  any
		want string
	}{
		// issue #22 acceptance row: ensure_ascii escapes non-ASCII and <&>
		// stay UNESCAPED (Go's encoding/json did the opposite on both).
		{name: "ensure_ascii", obj: "中文<&>", want: `"\u4e2d\u6587<&>"`},
		{name: "control chars", obj: "a\nb", want: `"a\nb"`},
		{name: "int stays int", obj: []any{1, 2.5}, want: "[1, 2.5]"},
		{name: "float keeps repr", obj: 2.0, want: "2.0"},
		{name: "bool", obj: true, want: "true"},
		{name: "null", obj: nil, want: "null"},
		{name: "nested", obj: map[string]any{"k": []any{1, "x"}}, want: `{"k": [1, "x"]}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := jsonDumps(c.obj)
			require.NoError(t, err)
			assert.Equal(t, c.want, got)
		})
	}
}
