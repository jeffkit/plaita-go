package plaitaexpr

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/model"
)

// The semantics ported here follow plaita's Python implementations:
// plaita/core/expression.py — _fn_eq/_fn_ne/_fn_gt/_fn_gte/_fn_lt/_fn_lte,
// _fn_ifelse, _fn_notContains, _fn_pop, _fn_getDictValues.

func Test_eq(t *testing.T) {
	cases := []struct {
		name string
		l, r any
		want bool
	}{
		{"int int", 1, 1, true},
		{"int float cross-type", 1, 1.0, true},
		{"int float not equal", 1, 1.5, false},
		{"int64 float64", int64(2), 2.0, true},
		{"string exact", "abc", "abc", true},
		{"string differ", "abc", "abd", false},
		{"string vs int", "1", 1, false},
		{"nil nil", nil, nil, true},
		{"nil vs zero", nil, 0, false},
		{"nil vs empty string", nil, "", false},
		{"bool as int true", true, 1, true},
		{"bool as int false", false, 0, true},
		{"bool vs string", true, "1", false},
		{"slice deep equal", []any{1, 2}, []any{1.0, 2.0}, true},
		{"slice differ", []any{1, 2}, []any{2, 1}, false},
		{"slice length differ", []any{1}, []any{1, 1}, false},
		{"map deep equal", map[string]any{"a": 1}, map[string]any{"a": 1.0}, true},
		{"map key differ", map[string]any{"a": 1}, map[string]any{"b": 1}, false},
		{"map value differ", map[string]any{"a": 1}, map[string]any{"a": 2}, false},
		{"slice vs int", []any{1}, 1, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, eq(c.l, c.r))
			assert.Equal(t, !c.want, ne(c.l, c.r))
		})
	}
}

func Test_ordering(t *testing.T) {
	cases := []struct {
		name    string
		f       func(l, r any) (bool, error)
		l, r    any
		want    bool
		wantErr bool
	}{
		{"gt int", gt, 2, 1, true, false},
		{"gt int false", gt, 1, 2, false, false},
		{"gt float cross-type", gt, 2.0, 1, true, false},
		{"gt equal", gt, 1, 1, false, false},
		{"gt string lexicographic", gt, "b", "a", true, false},
		{"gt string false", gt, "a", "b", false, false},
		{"gt bool as int", gt, true, false, true, false},
		{"gt mixed types error", gt, 1, "2", false, true},
		{"gt nil error", gt, nil, nil, false, true},
		{"gt string vs int error", gt, "a", 1, false, true},
		{"gte equal", gte, 2, 2, true, false},
		{"gte cross-type equal", gte, 2, 2.0, true, false},
		{"gte greater", gte, 3, 2, true, false},
		{"gte less", gte, 1, 2, false, false},
		{"gte string", gte, "b", "a", true, false},
		{"gte mixed error", gte, "a", 1, false, true},
		{"lt less", lt, 1, 2, true, false},
		{"lt greater", lt, 2, 1, false, false},
		{"lt string", lt, "a", "b", true, false},
		{"lt cross-type", lt, 1.5, 2, true, false},
		{"lt mixed error", lt, nil, 1, false, true},
		{"lte equal", lte, 2, 2, true, false},
		{"lte cross-type equal", lte, 2.0, 2, true, false},
		{"lte less", lte, 1, 2, true, false},
		{"lte greater", lte, 2, 1, false, false},
		{"lte string", lte, "a", "b", true, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := c.f(c.l, c.r)
			if c.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, c.want, got)
		})
	}
}

func Test_ifelse(t *testing.T) {
	cases := []struct {
		name string
		cond any
		want any
	}{
		{"true", true, "x"},
		{"false", false, "y"},
		{"empty string falsy", "", "y"},
		{"non-empty string truthy", "a", "x"},
		{"zero falsy", 0, "y"},
		{"zero float falsy", 0.0, "y"},
		{"non-zero truthy", 1, "x"},
		{"nil falsy", nil, "y"},
		{"empty slice falsy", []any{}, "y"},
		{"non-empty slice truthy", []any{1}, "x"},
		{"empty map falsy", map[string]any{}, "y"},
		{"non-empty map truthy", map[string]any{"a": 1}, "x"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// plaita: a if c else b — Python truthiness on the condition.
			assert.Equal(t, c.want, ifelse(c.cond, "x", "y"))
		})
	}
}

func Test_notContains(t *testing.T) {
	cases := []struct {
		name      string
		container any
		needle    any
		want      bool
		wantErr   bool
	}{
		{"substring contained", "hello world", "lo wo", false, false},
		{"substring not contained", "hello", "xyz", true, false},
		{"substring equal", "hello", "hello", false, false},
		{"slice member", []any{1, 2}, 2, false, false},
		{"slice member cross-type", []any{1.0, 2}, 1, false, false},
		{"slice not member", []any{1, 2}, 3, true, false},
		{"slice nil member", []any{1, nil}, nil, false, false},
		{"dict key contained", map[string]any{"a": 1}, "a", false, false},
		{"dict key not contained", map[string]any{"a": 1}, "b", true, false},
		{"dict value is not a key", map[string]any{"a": 1}, 1, true, false},
		{"dict non-string needle", map[string]any{"1": 1}, 1, true, false},
		{"string needle non-string", "hello", 1, false, true},
		{"nil container", nil, "a", false, true},
		{"int container", 42, "4", false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := notContains(c.container, c.needle)
			if c.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, c.want, got)
		})
	}
}

func Test_pop_list(t *testing.T) {
	t.Run("pop first", func(t *testing.T) {
		l := []any{1, 2, 3}
		v, err := pop(l, 0)
		assert.NoError(t, err)
		assert.Equal(t, 1, v)
		// in-place: backing array shifted, tail zeroed
		assert.Equal(t, []any{2, 3, nil}, l)
	})
	t.Run("pop middle", func(t *testing.T) {
		l := []any{"a", "b", "c"}
		v, err := pop(l, 1)
		assert.NoError(t, err)
		assert.Equal(t, "b", v)
		assert.Equal(t, []any{"a", "c", nil}, l)
	})
	t.Run("negative index counts from end", func(t *testing.T) {
		l := []any{1, 2, 3}
		v, err := pop(l, -1)
		assert.NoError(t, err)
		assert.Equal(t, 3, v)
		assert.Equal(t, []any{1, 2, nil}, l)
	})
	t.Run("index out of range", func(t *testing.T) {
		_, err := pop([]any{1, 2, 3}, 5)
		assert.ErrorContains(t, err, "out of range")
	})
	t.Run("negative index out of range", func(t *testing.T) {
		_, err := pop([]any{1, 2, 3}, -5)
		assert.ErrorContains(t, err, "out of range")
	})
	t.Run("integral float index accepted", func(t *testing.T) {
		l := []any{1, 2, 3}
		v, err := pop(l, 1.0)
		assert.NoError(t, err)
		assert.Equal(t, 2, v)
	})
	t.Run("fractional index rejected", func(t *testing.T) {
		_, err := pop([]any{1, 2, 3}, 1.5)
		assert.ErrorContains(t, err, "interpreted as an integer")
	})
	t.Run("json.Number index", func(t *testing.T) {
		l := []any{1, 2, 3}
		v, err := pop(l, json.Number("1"))
		assert.NoError(t, err)
		assert.Equal(t, 2, v)
	})
	t.Run("string index rejected", func(t *testing.T) {
		_, err := pop([]any{1, 2, 3}, "1")
		assert.Error(t, err)
	})
}

func Test_pop_dict(t *testing.T) {
	t.Run("pop key returns value and deletes", func(t *testing.T) {
		m := map[string]any{"a": 1, "b": 2}
		v, err := pop(m, "a")
		assert.NoError(t, err)
		assert.Equal(t, 1, v)
		assert.Equal(t, map[string]any{"b": 2}, m)
	})
	t.Run("missing key", func(t *testing.T) {
		_, err := pop(map[string]any{"a": 1}, "z")
		assert.ErrorContains(t, err, "KeyError")
	})
	t.Run("non-string key", func(t *testing.T) {
		_, err := pop(map[string]any{"a": 1}, 1)
		assert.ErrorContains(t, err, "KeyError")
	})
	t.Run("empty map", func(t *testing.T) {
		_, err := pop(map[string]any{}, "a")
		assert.ErrorContains(t, err, "KeyError")
	})
}

func Test_pop_unsupported(t *testing.T) {
	for _, c := range []struct {
		name      string
		container any
	}{
		{"nil", nil},
		{"string", "abc"},
		{"int", 42},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := pop(c.container, 0)
			assert.Error(t, err)
		})
	}
}

func Test_getDictValues(t *testing.T) {
	// plaita $F.getDictValues = dict.values() materialised to a list;
	// Go order follows the existing keys/values convention (sorted keys).
	assert.Equal(t, []any{1, 2}, values(map[string]any{"a": 1, "b": 2}))
	assert.Equal(t, []any{}, values(map[string]any{}))
}

func Test_plaita_functions_eval(t *testing.T) {
	e := &Evaluator{}
	newCtx := func(input map[string]any) context.Context {
		return runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
			WorkflowInput: input,
		})
	}

	t.Run("eq/ne", func(t *testing.T) {
		ctx := newCtx(map[string]any{"a": 1, "b": 1.0, "c": "1", "list": []any{1, 2}})
		r, err := e.Eval(ctx, `$F.eq($INPUT.a, $INPUT.b)`, nil)
		assert.NoError(t, err)
		assert.Equal(t, model.NewValue(true), r)
		r, err = e.Eval(ctx, `$F.eq($INPUT.a, $INPUT.c)`, nil)
		assert.NoError(t, err)
		assert.Equal(t, model.NewValue(false), r)
		r, err = e.Eval(ctx, `$F.eq($INPUT.list, $F.slice($INPUT.list, 0, 2))`, nil)
		assert.NoError(t, err)
		assert.Equal(t, model.NewValue(true), r)
		r, err = e.Eval(ctx, `$F.ne($INPUT.a, $INPUT.c)`, nil)
		assert.NoError(t, err)
		assert.Equal(t, model.NewValue(true), r)
	})

	t.Run("gt/lt/gte/lte", func(t *testing.T) {
		ctx := newCtx(map[string]any{"n": 2.5, "s": "b"})
		r, err := e.Eval(ctx, `$F.gt($INPUT.n, 2)`, nil)
		assert.NoError(t, err)
		assert.Equal(t, model.NewValue(true), r)
		r, err = e.Eval(ctx, `$F.lt($INPUT.n, 2)`, nil)
		assert.NoError(t, err)
		assert.Equal(t, model.NewValue(false), r)
		r, err = e.Eval(ctx, `$F.gte($INPUT.s, "a")`, nil)
		assert.NoError(t, err)
		assert.Equal(t, model.NewValue(true), r)
		r, err = e.Eval(ctx, `$F.lte($INPUT.s, "a")`, nil)
		assert.NoError(t, err)
		assert.Equal(t, model.NewValue(false), r)
		// plaita propagates the native TypeError for incomparable operands
		_, err = e.Eval(ctx, `$F.gt($INPUT.s, 1)`, nil)
		assert.Error(t, err)
	})

	t.Run("ifelse", func(t *testing.T) {
		ctx := newCtx(map[string]any{"flag": true, "empty": "", "n": 0})
		r, err := e.Eval(ctx, `$F.ifelse($INPUT.flag, "yes", "no")`, nil)
		assert.NoError(t, err)
		assert.Equal(t, model.NewValue("yes"), r)
		r, err = e.Eval(ctx, `$F.ifelse($INPUT.empty, "yes", "no")`, nil)
		assert.NoError(t, err)
		assert.Equal(t, model.NewValue("no"), r)
		r, err = e.Eval(ctx, `$F.ifelse($INPUT.n, "yes", "no")`, nil)
		assert.NoError(t, err)
		assert.Equal(t, model.NewValue("no"), r)
	})

	t.Run("notContains", func(t *testing.T) {
		ctx := newCtx(map[string]any{"s": "hello", "list": []any{1, 2}, "m": map[string]any{"k": 1}})
		r, err := e.Eval(ctx, `$F.notContains($INPUT.s, "ell")`, nil)
		assert.NoError(t, err)
		assert.Equal(t, model.NewValue(false), r)
		r, err = e.Eval(ctx, `$F.notContains($INPUT.list, 3)`, nil)
		assert.NoError(t, err)
		assert.Equal(t, model.NewValue(true), r)
		r, err = e.Eval(ctx, `$F.notContains($INPUT.m, "k")`, nil)
		assert.NoError(t, err)
		assert.Equal(t, model.NewValue(false), r)
	})

	t.Run("getDictValues", func(t *testing.T) {
		ctx := newCtx(map[string]any{"m": map[string]any{"a": 1, "b": 2}})
		r, err := e.Eval(ctx, `$F.getDictValues($INPUT.m)`, nil)
		assert.NoError(t, err)
		assert.Equal(t, model.NewValue([]any{1, 2}), r)
	})

	t.Run("pop list", func(t *testing.T) {
		ctx := newCtx(map[string]any{"list": []any{"x", "y", "z"}})
		r, err := e.Eval(ctx, `$F.pop($INPUT.list, 0)`, nil)
		assert.NoError(t, err)
		assert.Equal(t, model.NewValue("x"), r)
	})

	t.Run("pop dict", func(t *testing.T) {
		m := map[string]any{"a": 1, "b": 2}
		ctx := newCtx(map[string]any{"m": m})
		r, err := e.Eval(ctx, `$F.pop($INPUT.m, "a")`, nil)
		assert.NoError(t, err)
		assert.Equal(t, model.NewValue(1), r)
		// dict.pop mutates in place
		assert.Equal(t, map[string]any{"b": 2}, m)
	})

	t.Run("pop errors propagate", func(t *testing.T) {
		ctx := newCtx(map[string]any{"list": []any{1}, "m": map[string]any{}})
		_, err := e.Eval(ctx, `$F.pop($INPUT.list, 5)`, nil)
		assert.ErrorContains(t, err, "out of range")
		_, err = e.Eval(ctx, `$F.pop($INPUT.m, "missing")`, nil)
		assert.ErrorContains(t, err, "KeyError")
	})
}

func Test_plaita_functions_registered(t *testing.T) {
	for _, name := range []string{
		"eq", "ne", "gt", "gte", "lt", "lte",
		"ifelse", "notContains", "getDictValues", "pop",
	} {
		_, ok := GetFunction(name)
		assert.True(t, ok, "function %q should be registered", name)
	}
}
