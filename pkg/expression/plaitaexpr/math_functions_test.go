package plaitaexpr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/model"
)

// The semantics pinned here follow plaita's Python implementations
// (expression.py _fn_add/_fn_sub/_fn_mul/_fn_div/_fn_mod/_fn_pow and the
// builtin/math registrations abs/ceil/floor/round/trunc/sqrt). Issue #20:
// int/float typing follows Python (+ keeps int×int int, / is always float),
// str/list concatenation and repetition work, % formats strings, division by
// zero is a ZeroDivisionError and round is Python's banker's rounding.

// evalMath runs a $F expression against an int-typed input value.
func evalMath(t *testing.T, expr string, input any) (model.Value, error) {
	t.Helper()
	e := &Evaluator{}
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: input,
	})
	return e.Eval(ctx, expr, nil)
}

func Test_add_eval(t *testing.T) {
	// single input
	result1, err1 := evalMath(t, `$F.add($INPUT, 20)`, 10)
	require.NoError(t, err1)
	// Python: int + int stays int (issue #20)
	assert.Equal(t, model.NewValue(30), result1)
	// object input
	result2, err2 := evalMath(t, `$F.add($INPUT.a, $INPUT.b)`, map[string]any{"a": 10, "b": 20})
	require.NoError(t, err2)
	assert.Equal(t, model.NewValue(30), result2)
	// mixed int/float goes float, like Python 1 + 2.0
	result3, err3 := evalMath(t, `$F.add($INPUT.a, 2.5)`, map[string]any{"a": 1})
	require.NoError(t, err3)
	assert.Equal(t, model.NewValue(3.5), result3)
}

func Test_add_python_domains(t *testing.T) {
	cases := []struct {
		name    string
		expr    string
		want    any
		wantErr bool
	}{
		// $F.add('a','b') == 'ab' (issue #20 acceptance row)
		{name: "str concat", expr: `$F.add('a','b')`, want: "ab"},
		// $F.add([1],[2]) == [1,2]
		{name: "list concat", expr: `$F.add([1],[2])`, want: []any{1, 2}},
		{name: "str + int is TypeError", expr: `$F.add('a', 1)`, wantErr: true},
		{name: "list + str is TypeError", expr: `$F.add([1], 'a')`, wantErr: true},
		{name: "bool is int", expr: `$F.add(true, 1)`, want: int64(2)},
	}
	e := &Evaluator{}
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

func Test_sub_eval(t *testing.T) {
	result1, err1 := evalMath(t, `$F.sub($INPUT, 20)`, 10)
	require.NoError(t, err1)
	assert.Equal(t, model.NewValue(-10), result1)
	result2, err2 := evalMath(t, `$F.sub($INPUT.a, $INPUT.b)`, map[string]any{"a": 30, "b": 20})
	require.NoError(t, err2)
	assert.Equal(t, model.NewValue(10), result2)
	// Python: str - str is a TypeError
	_, err3 := evalMath(t, `$F.sub('abc', 'b')`, map[string]any{})
	assert.ErrorContains(t, err3, "unsupported operand type(s) for -")
}

func Test_mul_eval(t *testing.T) {
	result1, err1 := evalMath(t, `$F.mul($INPUT, 20)`, -10)
	require.NoError(t, err1)
	assert.Equal(t, model.NewValue(-200), result1)
	result2, err2 := evalMath(t, `$F.mul($INPUT.a, $INPUT.b)`, map[string]any{"a": 10, "b": 20})
	require.NoError(t, err2)
	assert.Equal(t, model.NewValue(200), result2)
}

func Test_mul_python_domains(t *testing.T) {
	cases := []struct {
		name    string
		expr    string
		want    any
		wantErr bool
	}{
		// $F.mul('ab',3) == 'ababab' (issue #20 acceptance row)
		{name: "str repeat", expr: `$F.mul('ab',3)`, want: "ababab"},
		{name: "int * str repeat", expr: `$F.mul(2, 'ab')`, want: "abab"},
		{name: "list repeat", expr: `$F.mul([1,2],2)`, want: []any{1, 2, 1, 2}},
		{name: "negative repeat empty", expr: `$F.mul('ab',-1)`, want: ""},
		{name: "str * str is TypeError", expr: `$F.mul('ab','cd')`, wantErr: true},
	}
	e := &Evaluator{}
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

func Test_div_eval(t *testing.T) {
	result1, err1 := evalMath(t, `$F.div($INPUT, 20)`, 10)
	require.NoError(t, err1)
	assert.Equal(t, model.NewValue(0.5), result1)
	result2, err2 := evalMath(t, `$F.div($INPUT.a, $INPUT.b)`, map[string]any{"a": 10, "b": 20})
	require.NoError(t, err2)
	assert.Equal(t, model.NewValue(0.5), result2)
}

func Test_div_by_zero_errors(t *testing.T) {
	// Python ZeroDivisionError (issue #20: `div(1,0)` 返回 error)
	_, err := evalMath(t, `$F.div($INPUT.a, $INPUT.c)`, map[string]any{"a": 10, "c": 0})
	assert.ErrorContains(t, err, "division by zero")
	// even int/int division is a float (Python 4/2 == 2.0)
	got, err := evalMath(t, `$F.div($INPUT.a, $INPUT.b)`, map[string]any{"a": 4, "b": 2})
	require.NoError(t, err)
	assert.Equal(t, model.NewValue(2.0), got)
}

func Test_mod_eval(t *testing.T) {
	result1, err1 := evalMath(t, `$F.mod($INPUT, 20)`, 10)
	require.NoError(t, err1)
	assert.Equal(t, model.NewValue(10), result1)
	result2, err2 := evalMath(t, `$F.mod($INPUT.a, $INPUT.b)`, map[string]any{"a": 7, "b": 3})
	require.NoError(t, err2)
	assert.Equal(t, model.NewValue(1), result2)
}

func Test_mod_python_semantics(t *testing.T) {
	cases := []struct {
		name    string
		expr    string
		want    any
		wantErr bool
	}{
		// Python sign rule: the result takes the divisor's sign
		{name: "negative dividend", expr: `$F.mod(-7, 3)`, want: int64(2)},
		{name: "negative divisor", expr: `$F.mod(7, -3)`, want: int64(-2)},
		{name: "float modulo", expr: `$F.mod(7.5, 2)`, want: 1.5},
		// printf-style string formatting (issue #20: `%s` 格式化)
		{name: "%s format", expr: `$F.mod('%s', 'x')`, want: "x"},
		{name: "%d format", expr: `$F.mod('%d apples', 3)`, want: "3 apples"},
		{name: "%r format", expr: `$F.mod('%r', 'x')`, want: "'x'"},
		{name: "tuple args", expr: `$F.mod('%s-%s', [1, 2])`, want: "1-2"},
		{name: "mapping args", expr: `$F.mod('%(k)s!', {'k': 'v'})`, want: "v!"},
		{name: "float precision", expr: `$F.mod('%.2f', 3.14159)`, want: "3.14"},
		{name: "percent literal", expr: `$F.mod('100%%', [1, 2])`, wantErr: true},
		// Python: a format that consumes no argument with one supplied is a
		// TypeError ("not all arguments converted")
		{name: "zero modulo errors", expr: `$F.mod(5, 0)`, wantErr: true},
		{name: "missing mapping key", expr: `$F.mod('%(gone)s', {})`, wantErr: true},
	}
	e := &Evaluator{}
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

func Test_pow_eval(t *testing.T) {
	result1, err1 := evalMath(t, `$F.pow($INPUT, 2)`, 10)
	require.NoError(t, err1)
	// Python: int ** int stays int
	assert.Equal(t, model.NewValue(100), result1)
	result2, err2 := evalMath(t, `$F.pow($INPUT.a, $INPUT.b)`, map[string]any{"a": 7, "b": 3})
	require.NoError(t, err2)
	assert.Equal(t, model.NewValue(343), result2)
	// int ** negative int is a float (Python 2 ** -1 == 0.5)
	result3, err3 := evalMath(t, `$F.pow(2, -1)`, map[string]any{})
	require.NoError(t, err3)
	assert.Equal(t, model.NewValue(0.5), result3)
	// 0 ** -1 is a ZeroDivisionError
	_, err4 := evalMath(t, `$F.pow(0, -1)`, map[string]any{})
	assert.Error(t, err4)
}

func Test_abs_eval(t *testing.T) {
	result1, err1 := evalMath(t, `$F.abs($INPUT)`, -10)
	require.NoError(t, err1)
	// Python: abs(-3) == 3, an int (issue #20 acceptance row)
	assert.Equal(t, model.NewValue(10), result1)
	result2, err2 := evalMath(t, `$F.abs($INPUT.b)`, map[string]any{"a": -7, "b": 3})
	require.NoError(t, err2)
	assert.Equal(t, model.NewValue(3), result2)
	// abs of a float stays float
	result3, err3 := evalMath(t, `$F.abs(-3.5)`, map[string]any{})
	require.NoError(t, err3)
	assert.Equal(t, model.NewValue(3.5), result3)
}

func Test_ceil_eval(t *testing.T) {
	result1, err1 := evalMath(t, `$F.ceil($INPUT)`, 10.2)
	require.NoError(t, err1)
	// Python math.ceil returns an int
	assert.Equal(t, model.NewValue(11), result1)
	result2, err2 := evalMath(t, `$F.ceil($INPUT.a)`, map[string]any{"a": 7.2, "b": 3})
	require.NoError(t, err2)
	assert.Equal(t, model.NewValue(8), result2)
}

func Test_floor_eval(t *testing.T) {
	result1, err1 := evalMath(t, `$F.floor($INPUT)`, 10.2)
	require.NoError(t, err1)
	assert.Equal(t, model.NewValue(10), result1)
	result2, err2 := evalMath(t, `$F.floor($INPUT.a)`, map[string]any{"a": 7.2, "b": 3})
	require.NoError(t, err2)
	assert.Equal(t, model.NewValue(7), result2)
}

func Test_round_python_semantics(t *testing.T) {
	cases := []struct {
		name string
		expr string
		want any
	}{
		// issue #20 acceptance rows: round(2.5) == 2 (banker's rounding),
		// round(2.5, 0) == 2.0 (float kept)
		{name: "round(2.5) -> int 2", expr: `$F.round(2.5)`, want: int64(2)},
		{name: "round(3.5) -> int 4", expr: `$F.round(3.5)`, want: int64(4)},
		{name: "round(0.5) -> int 0", expr: `$F.round(0.5)`, want: int64(0)},
		{name: "round(2.5,0) -> float 2.0", expr: `$F.round(2.5,0)`, want: 2.0},
		{name: "round(2.675,2) -> 2.67", expr: `$F.round(2.675,2)`, want: 2.67},
		{name: "round(5,2) -> int 5", expr: `$F.round(5,2)`, want: int64(5)},
		{name: "round(1234.5,-2) -> 1200.0", expr: `$F.round(1234.5,-2)`, want: 1200.0},
		{name: "round(10.2,1) -> 10.2", expr: `$F.round($INPUT.a,1)`, want: 10.2},
		{name: "round(7.6,0) -> 8.0", expr: `$F.round(7.6,0)`, want: 8.0},
	}
	e := &Evaluator{}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
				WorkflowInput: map[string]any{"a": 10.2},
			})
			got, err := e.Eval(ctx, c.expr, nil)
			require.NoError(t, err)
			assert.Equal(t, model.NewValue(c.want), got)
		})
	}
}

func Test_trunc_eval(t *testing.T) {
	result1, err1 := evalMath(t, `$F.trunc($INPUT)`, 10.2)
	require.NoError(t, err1)
	assert.Equal(t, model.NewValue(10), result1)
	result2, err2 := evalMath(t, `$F.trunc($INPUT.a)`, map[string]any{"a": 7.6, "b": 3})
	require.NoError(t, err2)
	assert.Equal(t, model.NewValue(7), result2)
}

func Test_sqrt_eval(t *testing.T) {
	result1, err1 := evalMath(t, `$F.sqrt($INPUT)`, 4)
	require.NoError(t, err1)
	// Python math.sqrt always returns a float (sqrt(4) == 2.0)
	assert.Equal(t, model.NewValue(2.0), result1)
	result2, err2 := evalMath(t, `$F.sqrt($INPUT.a)`, map[string]any{"a": 9, "b": 3})
	require.NoError(t, err2)
	assert.Equal(t, model.NewValue(3.0), result2)
	// negative operand: math domain error (Python ValueError), not NaN
	_, err3 := evalMath(t, `$F.sqrt(-1)`, map[string]any{})
	assert.ErrorContains(t, err3, "math domain error")
}

func Test_ceil_floor_float_to_int_guards(t *testing.T) {
	// Issue #70: ceil/floor bypassed the NaN/Inf/range guard that pyToInt
	// (trunc) already had — NaN silently became 0, ±Inf and finite
	// beyond-int64 floats became ±MaxInt64. The guard aligns the three with
	// CPython float→int semantics: NaN/±Inf are named errors and finite
	// floats outside int64 are refused (Python returns an arbitrary-precision
	// int; the engine's int64 value domain cannot — declared divergence).
	// Bounds use ±2^63, NOT f > math.MaxInt64: float64(MaxInt64) rounds up
	// to 2^63, so that comparison lets f == 2^63 slip through.
	cases := []struct {
		name    string
		expr    string
		input   any
		wantErr string
	}{
		{name: "ceil +Inf", expr: `$F.ceil($F.mul(1e308, 10))`, wantErr: "cannot convert float infinity to integer"},
		{name: "floor -Inf", expr: `$F.floor($F.mul(-1e308, 10))`, wantErr: "cannot convert float infinity to integer"},
		{name: "ceil NaN (Inf-Inf)", expr: `$F.ceil($F.sub($F.mul(1e308, 10), $F.mul(1e308, 10)))`, wantErr: "cannot convert float NaN to integer"},
		{name: "floor NaN (Inf-Inf)", expr: `$F.floor($F.sub($F.mul(1e308, 10), $F.mul(1e308, 10)))`, wantErr: "cannot convert float NaN to integer"},
		{name: "ceil finite beyond int64", expr: `$F.ceil(1e308)`, wantErr: "cannot convert float to integer"},
		{name: "ceil data-driven 1e308", expr: `$F.ceil($INPUT.x)`, input: map[string]any{"x": 1e308}, wantErr: "cannot convert float to integer"},
		{name: "ceil data-driven 1e308*10 = +Inf", expr: `$F.ceil($F.mul($INPUT.x, 10))`, input: map[string]any{"x": 1e308}, wantErr: "cannot convert float infinity to integer"},
		{name: "ceil 2^63 upper bound", expr: `$F.ceil(9223372036854775808.0)`, wantErr: "cannot convert float to integer"},
		{name: "floor below -2^63", expr: `$F.floor(-9223372036854777856.0)`, wantErr: "cannot convert float to integer"},
		// pyToInt now shares the guard: trunc's 2^63 hole (silent MaxInt64)
		// closes — an intentional tightening, pinned here.
		{name: "trunc 2^63 tightened", expr: `$F.trunc(9223372036854775808.0)`, wantErr: "cannot convert float to integer"},
		{name: "trunc +Inf keeps erroring", expr: `$F.trunc($F.mul(1e308, 10))`, wantErr: "cannot convert float infinity to integer"},
		{name: "trunc NaN keeps erroring", expr: `$F.trunc($F.sub($F.mul(1e308, 10), $F.mul(1e308, 10)))`, wantErr: "cannot convert float NaN to integer"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := evalMath(t, c.expr, c.input)
			require.Error(t, err)
			assert.ErrorContains(t, err, c.wantErr)
		})
	}
}

func Test_ceil_floor_boundaries_keep_working(t *testing.T) {
	// Exact MinInt64 is representable — the guard must not over-reject it.
	for _, fn := range []string{"ceil", "floor"} {
		got, err := evalMath(t, "$F."+fn+"(-9223372036854775808.0)", map[string]any{})
		require.NoError(t, err, fn)
		assert.Equal(t, model.NewValue(int64(-9223372036854775808)), got, fn)
	}
	// Normal values unchanged (pre-fix behaviour, Python math.ceil/floor).
	cases := []struct {
		name string
		expr string
		want any
	}{
		{name: "ceil(2.3)", expr: `$F.ceil(2.3)`, want: int64(3)},
		{name: "floor(-2.3)", expr: `$F.floor(-2.3)`, want: int64(-3)},
		{name: "ceil(2) stays int", expr: `$F.ceil(2)`, want: int64(2)},
		{name: "floor(2) stays int", expr: `$F.floor(2)`, want: int64(2)},
		{name: "ceil(10.2)", expr: `$F.ceil(10.2)`, want: int64(11)},
		{name: "floor(10.2)", expr: `$F.floor(10.2)`, want: int64(10)},
		{name: "ceil(7.2) via input", expr: `$F.ceil($INPUT.a)`, want: int64(8)},
		{name: "floor(7.2) via input", expr: `$F.floor($INPUT.a)`, want: int64(7)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := evalMath(t, c.expr, map[string]any{"a": 7.2})
			require.NoError(t, err)
			assert.Equal(t, model.NewValue(c.want), got)
		})
	}
}
