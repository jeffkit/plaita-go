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
// (expression.py _fn_index/_fn_slice/_fn_getListItem/_fn_insert/_fn_remove
// and the pure-list-producer contracts). Issue #21: len/index/slice work on
// str/list/dict, misses raise instead of returning sentinels; issue #9: the
// pure producers never write through a shared backing array.

func evalSliceExpr(t *testing.T, expr string, input any) (model.Value, error) {
	t.Helper()
	e := Evaluator{}
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: input,
	})
	return e.Eval(ctx, expr, nil)
}

func Test_length_eval(t *testing.T) {
	// list input
	result1, err1 := evalSliceExpr(t, `$F.len($INPUT)`, []any{"1", "2"})
	require.NoError(t, err1)
	assert.Equal(t, model.NewValue(2), result1)
	// nested input
	result2, err2 := evalSliceExpr(t, `$F.length($INPUT.a)`, map[string]any{"a": []any{"1", "2"}})
	require.NoError(t, err2)
	assert.Equal(t, model.NewValue(2), result2)
}

func Test_len_python_domains(t *testing.T) {
	// issue #21 acceptance: len works on str AND dict; str counts runes.
	cases := []struct {
		name    string
		expr    string
		input   any
		want    any
		wantErr bool
	}{
		{name: "str len", expr: `$F.len('abc')`, input: map[string]any{}, want: 3},
		{name: "str len counts runes", expr: `$F.len('中文')`, input: map[string]any{}, want: 2},
		{name: "dict len", expr: `$F.len({'a': 1, 'b': 2})`, input: map[string]any{}, want: 2},
		{name: "int has no len", expr: `$F.len($INPUT.n)`, input: map[string]any{"n": 3}, wantErr: true},
		{name: "nil has no len", expr: `$F.len($INPUT.missing)`, input: map[string]any{}, wantErr: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := evalSliceExpr(t, c.expr, c.input)
			if c.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, model.NewValue(c.want), got)
		})
	}
}

func Test_index_eval(t *testing.T) {
	result1, err1 := evalSliceExpr(t, `$F.index($INPUT, "1")`, []any{"1", "2"})
	require.NoError(t, err1)
	assert.Equal(t, model.NewValue(0), result1)
	result2, err2 := evalSliceExpr(t, `$F.index($INPUT.a, "2")`, map[string]any{"a": []any{"1", "2"}})
	require.NoError(t, err2)
	assert.Equal(t, model.NewValue(1), result2)
}

func Test_index_miss_is_error(t *testing.T) {
	// issue #21 acceptance row: `$F.index(list,9)` → Python ValueError, the
	// Go -1 sentinel masked "not found" as a valid subscript.
	_, err := evalSliceExpr(t, `$F.index($INPUT, "3")`, []any{"1", "2"})
	assert.ErrorContains(t, err, "not in list")
}

func Test_index_python_domains(t *testing.T) {
	cases := []struct {
		name    string
		expr    string
		want    any
		wantErr bool
	}{
		// issue #21 acceptance row: `$F.index('abc','b')` == 1
		{name: "str substring index", expr: `$F.index('abc','b')`, want: 1},
		{name: "str miss", expr: `$F.index('abc','z')`, wantErr: true},
		{name: "int has no index", expr: `$F.index(42, 4)`, wantErr: true},
		// issue #40 acceptance rows: Python str.index counts CHARACTERS — the
		// strings.Index byte offset drifted with the text (é is two bytes,
		// 👍 four), mis-cutting downstream slice/getListItem.
		{name: "str index counts runes: accented", expr: `$F.index('héllo', 'l')`, want: 2},
		{name: "str index counts runes: emoji", expr: `$F.index('a👍b', 'b')`, want: 2},
		{name: "str index counts runes: cjk", expr: `$F.index('中文测试', '测')`, want: 2},
		{name: "str index multi-char needle", expr: `$F.index('héllo wörld', 'wör')`, want: 6},
	}
	e := Evaluator{}
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{},
	})
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := e.Eval(ctx, c.expr, nil)
			if c.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, model.NewValue(c.want), got)
		})
	}
}

func Test_slice_eval(t *testing.T) {
	result1, err1 := evalSliceExpr(t, `$F.slice($INPUT, 0, 1)`, []any{"1", "2"})
	require.NoError(t, err1)
	assert.Equal(t, model.NewValue([]any{"1"}), result1)
	result2, err2 := evalSliceExpr(t, `$F.slice($INPUT.a, 0, 1)`, map[string]any{"a": []any{"1", "2"}})
	require.NoError(t, err2)
	assert.Equal(t, model.NewValue([]any{"1"}), result2)
}

func Test_slice_python_semantics(t *testing.T) {
	cases := []struct {
		name    string
		expr    string
		want    any
		wantErr bool
	}{
		// issue #21 acceptance row: slice(list,0,9) == whole list (the old
		// out-of-range guard returned nil)
		{name: "end clamps", expr: `$F.slice($INPUT.list, 0, 9)`, want: []any{1, 2, 3}},
		{name: "negative start", expr: `$F.slice($INPUT.list, -2, 3)`, want: []any{2, 3}},
		{name: "start > end is empty", expr: `$F.slice($INPUT.list, -1, -2)`, want: []any{}},
		{name: "str slice", expr: `$F.slice('abc', 0, 2)`, want: "ab"},
		{name: "str negative slice", expr: `$F.slice('abc', -2, 99)`, want: "bc"},
		{name: "int not subscriptable", expr: `$F.slice(42, 0, 1)`, wantErr: true},
	}
	e := Evaluator{}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
				WorkflowInput: map[string]any{"list": []any{1, 2, 3}},
			})
			got, err := e.Eval(ctx, c.expr, nil)
			if c.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, model.NewValue(c.want), got)
		})
	}
}

func Test_append_no_write_through(t *testing.T) {
	// issue #9 acceptance row: addListItem returns a NEW list and the original
	// (and its backing array) stays untouched.
	original := []any{"1", "2"}
	result, err := evalSliceExpr(t, `$F.addListItem($INPUT, "3")`, original)
	require.NoError(t, err)
	assert.Equal(t, model.NewValue([]any{"1", "2", "3"}), result)
	assert.Equal(t, []any{"1", "2"}, original)

	result2, err2 := evalSliceExpr(t, `$F.append($INPUT.a, "3")`, map[string]any{"a": []any{"1", "2"}})
	require.NoError(t, err2)
	assert.Equal(t, model.NewValue([]any{"1", "2", "3"}), result2)
}

func Test_extend_eval(t *testing.T) {
	result1, err1 := evalSliceExpr(t, `$F.extend($INPUT, ["3"])`, []any{"1", "2"})
	require.NoError(t, err1)
	assert.Equal(t, model.NewValue([]any{"1", "2", "3"}), result1)
	// Python _fn_extend is `a + b`: str + str concatenates too.
	result2, err2 := evalSliceExpr(t, `$F.extend('ab', 'cd')`, map[string]any{})
	require.NoError(t, err2)
	assert.Equal(t, model.NewValue("abcd"), result2)
	// no write-through
	original := []any{"1", "2"}
	_, err3 := evalSliceExpr(t, `$F.extend($INPUT, ["3"])`, original)
	require.NoError(t, err3)
	assert.Equal(t, []any{"1", "2"}, original)
}

func Test_insert_python_semantics(t *testing.T) {
	cases := []struct {
		name string
		expr string
		want any
	}{
		{name: "middle insert", expr: `$F.insert($INPUT, 1, "3")`, want: []any{"1", "3", "2"}},
		// Python a[:b] + [c] + a[b:]: negative index counts from the end
		{name: "negative index", expr: `$F.insert([1, 2, 3], -1, 9)`, want: []any{1, 2, 9, 3}},
		// out-of-range clamps to the boundary
		{name: "overflow clamps", expr: `$F.insert([1, 2], 99, 9)`, want: []any{1, 2, 9}},
		{name: "underflow clamps", expr: `$F.insert([1, 2], -99, 9)`, want: []any{9, 1, 2}},
	}
	e := Evaluator{}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
				WorkflowInput: []any{"1", "2"},
			})
			got, err := e.Eval(ctx, c.expr, nil)
			require.NoError(t, err)
			assert.Equal(t, model.NewValue(c.want), got)
		})
	}
}

func Test_remove_python_semantics(t *testing.T) {
	result1, err1 := evalSliceExpr(t, `$F.remove($INPUT, "1")`, []any{"1", "2"})
	require.NoError(t, err1)
	assert.Equal(t, model.NewValue([]any{"2"}), result1)
	// Python list.remove raises ValueError on a miss; the old variant
	// silently returned the input unchanged.
	_, err2 := evalSliceExpr(t, `$F.remove($INPUT, "9")`, []any{"1", "2"})
	assert.ErrorContains(t, err2, "not in list")
	// no write-through
	original := []any{"1", "2"}
	_, err3 := evalSliceExpr(t, `$F.remove($INPUT, "1")`, original)
	require.NoError(t, err3)
	assert.Equal(t, []any{"1", "2"}, original)
}

func Test_sort_eval(t *testing.T) {
	result1, err1 := evalSliceExpr(t, `$F.sort($INPUT)`, []any{"2", "1"})
	require.NoError(t, err1)
	assert.Equal(t, model.NewValue([]any{"1", "2"}), result1)
	// sort int
	result3, err3 := evalSliceExpr(t, `$F.sort($INPUT.a)`, map[string]any{"a": []any{2, 1}})
	require.NoError(t, err3)
	assert.Equal(t, model.NewValue([]any{1, 2}), result3)
	// sort float desc
	result4, err4 := evalSliceExpr(t, `$F.sort($INPUT.a, "", true)`, map[string]any{"a": []any{2.1, 1.1, 3}})
	require.NoError(t, err4)
	assert.Equal(t, model.NewValue([]any{3, 2.1, 1.1}), result4)
	// sort map[string]any by key
	result5, err5 := evalSliceExpr(t, `$F.sort($INPUT.a, "a", false)`, map[string]any{
		"a": []any{
			map[string]any{"a": 2, "b": 1},
			map[string]any{"a": 1, "b": 2},
		},
	})
	require.NoError(t, err5)
	assert.Equal(t, model.NewValue([]any{
		map[string]any{"a": 1, "b": 2},
		map[string]any{"a": 2, "b": 1},
	}), result5)
	// no write-through: the input order survives (Python sorted())
	original := []any{"2", "1"}
	_, err6 := evalSliceExpr(t, `$F.sort($INPUT)`, original)
	require.NoError(t, err6)
	assert.Equal(t, []any{"2", "1"}, original)
}

func Test_reverse_eval(t *testing.T) {
	result1, err1 := evalSliceExpr(t, `$F.reverse($INPUT)`, []any{"1", "2"})
	require.NoError(t, err1)
	assert.Equal(t, model.NewValue([]any{"2", "1"}), result1)
	original := []any{"1", "2"}
	_, err2 := evalSliceExpr(t, `$F.reverse($INPUT)`, original)
	require.NoError(t, err2)
	assert.Equal(t, []any{"1", "2"}, original)
}

func Test_getListItem_python_semantics(t *testing.T) {
	cases := []struct {
		name            string
		expr            string
		input           any
		want            any
		wantErr         bool
		wantErrContains string
	}{
		{name: "list index", expr: `$F.getListItem($INPUT, 1)`, input: []any{"1", "2"}, want: "2"},
		// issue #21 acceptance row: negative index reads from the end
		// (the old variant returned nil)
		{name: "negative index", expr: `$F.getListItem($INPUT, -1)`, input: []any{"1", "2"}, want: "2"},
		{name: "out of range is IndexError", expr: `$F.getListItem($INPUT, 5)`, input: []any{"1", "2"}, wantErr: true},
		{name: "str index returns char", expr: `$F.getListItem('abc', 1)`, input: map[string]any{}, want: "b"},
		{name: "str negative index", expr: `$F.getListItem('abc', -1)`, input: map[string]any{}, want: "c"},
		{name: "str out of range", expr: `$F.getListItem('abc', 9)`, input: map[string]any{}, wantErr: true},
		// issue #42 acceptance rows: Python `a['k']` on a dict — the str-key
		// branch used to be unreachable because pyIndexArg rejected str first
		// ("'str' object cannot be interpreted as an integer").
		{name: "dict str key", expr: `$F.getListItem($INPUT.m, 'k')`, input: map[string]any{"m": map[string]any{"k": 5}}, want: 5},
		{name: "dict missing key is KeyError", expr: `$F.getListItem($INPUT.m, 'nope')`, input: map[string]any{"m": map[string]any{"k": 5}}, wantErrContains: "KeyError"},
		// a JSON dict never has non-str keys, so the int path keeps its
		// existing KeyError message; list IndexError rows above stay intact.
		{name: "dict int key stays KeyError", expr: `$F.getListItem($INPUT.m, 0)`, input: map[string]any{"m": map[string]any{"k": 5}}, wantErrContains: "KeyError"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := evalSliceExpr(t, c.expr, c.input)
			if c.wantErrContains != "" {
				assert.ErrorContains(t, err, c.wantErrContains)
				return
			}
			if c.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, model.NewValue(c.want), got)
		})
	}
}

func Test_setListItem_live_root_refused(t *testing.T) {
	// issue #9 acceptance row: `$F.setListItem($INPUT.list, 0, 9)` is plaita's
	// ValueError (live-root write protection), not a silent rewrite.
	e := Evaluator{}
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: []any{"1", "2"},
	})
	_, err1 := e.Eval(ctx1, `$F.setListItem($INPUT, 1, "3")`, nil)
	assert.ErrorContains(t, err1, "plaita#69 write protection")
	assert.Equal(t, []any{"1", "2"}, runtime.WorkflowRuntimeContext(ctx1).WorkflowInput)

	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{"a": []any{"1", "2"}},
	})
	_, err2 := e.Eval(ctx2, `$F.setListItem($INPUT.a, 1, "3")`, nil)
	assert.ErrorContains(t, err2, "plaita#69 write protection")
	input := runtime.WorkflowRuntimeContext(ctx2).WorkflowInput.(map[string]any)
	assert.Equal(t, []any{"1", "2"}, input["a"])
}
