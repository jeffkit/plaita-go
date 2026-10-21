package plaitaexpr

// Round-69 oracle acceptance rows (#44 sort TypeError / #45 mul size guard).
// Python 3.13 oracle, plaita expression_parser live runs (2026-10-21):
//
//	$F.sort([3,'x',1])             -> TypeError: '<' not supported between instances of 'str' and 'int'
//	$F.sort([1, None, 2])          -> TypeError: '<' not supported between instances of 'NoneType' and 'int'
//	$F.sort([{'k':2},{}], 'k')     -> TypeError: '<' not supported between instances of 'NoneType' and 'int' (x.get miss → None)
//	$F.sort([2,1,3.5])             -> [1, 2, 3.5] (int/float cross-type is fine)
//	$F.mul('a', 150000000)         -> ValueError: $F.mul(1 * 150000000) result too large: 150000000 chars (limit ...). Refusing to build an unbounded result.
//	$F.mul(['x'], 200000000)       -> same shape with "elements"
//	$F.mul('a', -5)                -> '' (negative/zero repeat stays empty, no error)

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/model"
)

func evalR69(t *testing.T, expr string, input any) (model.Value, error) {
	t.Helper()
	e := &Evaluator{}
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: input,
	})
	return e.Eval(ctx, expr, nil)
}

func Test_sort_mixed_types_type_error_oracle(t *testing.T) {
	cases := []struct {
		name    string
		expr    string
		input   any
		want    any
		errText string // empty = expect success with want
	}{
		// #44 acceptance rows: the first incomparable pair is a named
		// TypeError, not a silently reordered list.
		{
			name: "int/str mixed", expr: `$F.sort($INPUT.a)`,
			input:   map[string]any{"a": []any{3, "x", 1}},
			errText: "'<' not supported between instances of 'str' and 'int'",
		},
		{
			name: "None mixed", expr: `$F.sort($INPUT.a)`,
			input:   map[string]any{"a": []any{1, nil, 2}},
			errText: "'<' not supported between instances of 'NoneType' and 'int'",
		},
		{
			name: "dict missing key falls back to None", expr: `$F.sort($INPUT.a, 'k')`,
			input:   map[string]any{"a": []any{map[string]any{"k": 2}, map[string]any{}}},
			errText: "'<' not supported between instances of 'NoneType' and 'int'",
		},
		{
			name: "str/None mixed", expr: `$F.sort($INPUT.a)`,
			input:   map[string]any{"a": []any{"a", nil}},
			errText: "'<' not supported between instances of 'NoneType' and 'str'",
		},
		// same-type and int/float domains stay sortable (Python sorted).
		{name: "pure ints", expr: `$F.sort($INPUT.a)`, input: map[string]any{"a": []any{3, 1, 2}}, want: []any{1, 2, 3}},
		{name: "pure strs", expr: `$F.sort($INPUT.a)`, input: map[string]any{"a": []any{"b", "a", "c"}}, want: []any{"a", "b", "c"}},
		{name: "int+float", expr: `$F.sort($INPUT.a)`, input: map[string]any{"a": []any{2, 1, 3.5}}, want: []any{1, 2, 3.5}},
		{
			name: "desc ints", expr: `$F.sort($INPUT.a, "", true)`,
			input: map[string]any{"a": []any{3, 1, 2}}, want: []any{3, 2, 1},
		},
		{
			name: "dict by key", expr: `$F.sort($INPUT.a, 'k')`,
			input: map[string]any{"a": []any{map[string]any{"k": 2}, map[string]any{"k": 1}}},
			want:  []any{map[string]any{"k": 1}, map[string]any{"k": 2}},
		},
		{name: "empty list", expr: `$F.sort($INPUT.a)`, input: map[string]any{"a": []any{}}, want: []any{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := evalR69(t, c.expr, c.input)
			if c.errText != "" {
				require.Error(t, err, "mixed-type sort must fail like Python sorted(), got %v", got)
				assert.Contains(t, err.Error(), c.errText)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, model.NewValue(c.want), got, "got %v", got)
		})
	}
}

func Test_mul_repeat_size_guard_oracle(t *testing.T) {
	cases := []struct {
		name  string
		expr  string
		input any
		check func(t *testing.T, got model.Value, err error)
	}{
		// #45 acceptance rows: over-limit repetition is a named error, never a
		// silently emptied sequence. The limit itself stays the engine's 1e8
		// budget (issue #45 边界: lowering it to the Python ~100k soft limit is
		// a separate decision), so the message carries the engine limit.
		{
			name: "str repeat over limit errors",
			expr: `$F.len($F.mul('a', 150000000))`,
			check: func(t *testing.T, got model.Value, err error) {
				require.Error(t, err, "got %v", got)
				assert.Contains(t, err.Error(), "result too large")
				assert.Contains(t, err.Error(), "150000000 chars")
				assert.Contains(t, err.Error(), "Refusing to build an unbounded result")
			},
		},
		{
			name:  "list repeat over limit errors (elements unit)",
			expr:  `$F.len($F.mul($INPUT.a, 200000000))`,
			input: map[string]any{"a": []any{"x"}},
			check: func(t *testing.T, got model.Value, err error) {
				require.Error(t, err, "got %v", got)
				assert.Contains(t, err.Error(), "result too large")
				assert.Contains(t, err.Error(), "200000000 elements")
			},
		},
		{
			name: "int * str direction errors too",
			expr: `$F.len($F.mul(150000000, 'a'))`,
			check: func(t *testing.T, got model.Value, err error) {
				require.Error(t, err, "got %v", got)
				assert.Contains(t, err.Error(), "result too large")
			},
		},
		{
			name:  "projected size guards multi-char sequences",
			expr:  `$F.len($F.mul($INPUT, 50000001))`,
			input: "xy",
			check: func(t *testing.T, got model.Value, err error) {
				require.Error(t, err, "2 chars * 50000001 = 100000002 over the limit; got %v", got)
				assert.Contains(t, err.Error(), "100000002 chars")
			},
		},
		{
			name: "exactly at limit unchanged",
			expr: `$F.len($F.mul('a', 100000000))`,
			check: func(t *testing.T, got model.Value, err error) {
				require.NoError(t, err)
				assert.Equal(t, model.NewValue(100000000), got, "got %v", got)
			},
		},
		{
			name: "negative repeat stays empty",
			expr: `$F.mul('ab', -5)`,
			check: func(t *testing.T, got model.Value, err error) {
				require.NoError(t, err)
				assert.Equal(t, model.NewValue(""), got, "got %v", got)
			},
		},
		{
			name:  "zero repeat stays empty",
			expr:  `$F.mul($INPUT.a, 0)`,
			input: map[string]any{"a": []any{"ab"}},
			check: func(t *testing.T, got model.Value, err error) {
				require.NoError(t, err)
				assert.Equal(t, model.NewValue([]any{}), got, "got %v", got)
			},
		},
		{
			name: "numeric mul unaffected",
			expr: `$F.mul(6, 7)`,
			check: func(t *testing.T, got model.Value, err error) {
				require.NoError(t, err)
				assert.Equal(t, model.NewValue(42), got, "got %v", got)
			},
		},
		{
			name:  "small repeat unaffected",
			expr:  `$F.mul($INPUT.a, 3)`,
			input: map[string]any{"a": []any{"ab"}},
			check: func(t *testing.T, got model.Value, err error) {
				require.NoError(t, err)
				assert.Equal(t, model.NewValue([]any{"ab", "ab", "ab"}), got, "got %v", got)
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := evalR69(t, c.expr, c.input)
			c.check(t, got, err)
		})
	}
}
