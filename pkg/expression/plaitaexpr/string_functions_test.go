package plaitaexpr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/model"
)

// String semantics pinned here follow plaita's Python implementations
// (expression.py _fn_lower/_fn_upper/_fn_capitalize/_fn_title/_fn_split/
// _fn_join/_fn_concat/_fn_isDigit). Issue #21: Unicode full case mapping
// ('straße'.upper() == 'STRASSE'), capitalize lowercases the rest, title
// splits on every non-letter (like Python str.title), split('') and
// join(non-str) raise, concat renders any value with Python str().

func evalStringExpr(t *testing.T, expr string, input any) (model.Value, error) {
	t.Helper()
	e := Evaluator{}
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: input,
	})
	return e.Eval(ctx, expr, nil)
}

func Test_lower_eval(t *testing.T) {
	result1, err1 := evalStringExpr(t, `$F.lower($INPUT)`, "Hello World")
	require.NoError(t, err1)
	assert.Equal(t, model.NewValue("hello world"), result1)
}

func Test_upper_eval(t *testing.T) {
	result1, err1 := evalStringExpr(t, `$F.upper($INPUT)`, "Hello World")
	require.NoError(t, err1)
	assert.Equal(t, model.NewValue("HELLO WORLD"), result1)
	// issue #21 acceptance row: Python full case mapping ß → SS
	result2, err2 := evalStringExpr(t, `$F.upper('straße')`, map[string]any{})
	require.NoError(t, err2)
	assert.Equal(t, model.NewValue("STRASSE"), result2)
}

func Test_capitalize_python_semantics(t *testing.T) {
	cases := []struct {
		name string
		expr string
		want string
	}{
		// issue #21 acceptance row: 'hello WORLD'.capitalize() == 'Hello world'
		// (the rest is LOWERCASED — the old variant kept WORLD)
		{name: "rest lowercased", expr: `$F.capitalize('hello WORLD')`, want: "Hello world"},
		{name: "basic", expr: `$F.capitalize($INPUT)`, want: "Hello world"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := evalStringExpr(t, c.expr, "hello world")
			require.NoError(t, err)
			assert.Equal(t, model.NewValue(c.want), got)
		})
	}
}

func Test_title_python_semantics(t *testing.T) {
	cases := []struct {
		name string
		expr string
		want string
	}{
		{name: "basic", expr: `$F.title('hello world')`, want: "Hello World"},
		// Python str.title splits on EVERY non-letter (apostrophes included)
		{name: "apostrophe splits words", expr: `$F.title("they're")`, want: "They'Re"},
		{name: "digits split words", expr: `$F.title('abc1d')`, want: "Abc1D"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := evalStringExpr(t, c.expr, map[string]any{})
			require.NoError(t, err)
			assert.Equal(t, model.NewValue(c.want), got)
		})
	}
}

func Test_split_python_semantics(t *testing.T) {
	result1, err1 := evalStringExpr(t, `$F.split($INPUT, " ")`, "hello world")
	require.NoError(t, err1)
	assert.Equal(t, model.NewValue([]any{"hello", "world"}), result1)
	// issue #21 acceptance row: Python str.split('') is a ValueError
	_, err2 := evalStringExpr(t, `$F.split('abc','')`, map[string]any{})
	assert.ErrorContains(t, err2, "empty separator")
}

func Test_join_python_semantics(t *testing.T) {
	result1, err1 := evalStringExpr(t, `$F.join($INPUT, " ")`, []any{"hello", "world"})
	require.NoError(t, err1)
	assert.Equal(t, model.NewValue("hello world"), result1)
	// issue #21 acceptance row: `$F.join([1,2],'-')` is a Python TypeError —
	// the cast.ToString variant silently produced "1-2".
	_, err2 := evalStringExpr(t, `$F.join([1,2],'-')`, map[string]any{})
	assert.ErrorContains(t, err2, "expected str instance, int found")
}

func Test_concat_python_semantics(t *testing.T) {
	result1, err1 := evalStringExpr(t, `$F.concat($INPUT)`, "hello world")
	require.NoError(t, err1)
	assert.Equal(t, model.NewValue("hello world"), result1)
	// issue #21 acceptance row: `$F.concat(1,'a')` == '1a' (the typed variant
	// panicked at the reflect boundary)
	result2, err2 := evalStringExpr(t, `$F.concat(1,'a')`, map[string]any{})
	require.NoError(t, err2)
	assert.Equal(t, model.NewValue("1a"), result2)
	// None renders as Python str(None)
	result3, err3 := evalStringExpr(t, `$F.concat('v=', $INPUT.missing)`, map[string]any{})
	require.NoError(t, err3)
	assert.Equal(t, model.NewValue("v=None"), result3)
}

func Test_startswith_endswith_eval(t *testing.T) {
	result1, err1 := evalStringExpr(t, `$F.startswith($INPUT, "hello")`, "hello world")
	require.NoError(t, err1)
	assert.Equal(t, model.NewValue(true), result1)
	result2, err2 := evalStringExpr(t, `$F.endswith($INPUT, "world")`, "hello world")
	require.NoError(t, err2)
	assert.Equal(t, model.NewValue(true), result2)
}

func Test_isDigit_eval(t *testing.T) {
	result1, err1 := evalStringExpr(t, `$F.isDigit($INPUT)`, "123")
	require.NoError(t, err1)
	assert.Equal(t, model.NewValue(true), result1)
	result2, err2 := evalStringExpr(t, `$F.isDigit($INPUT.a)`, map[string]any{"a": "hello world"})
	require.NoError(t, err2)
	assert.Equal(t, model.NewValue(false), result2)
}
