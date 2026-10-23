package plaitaexpr

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jeffkit/plaita-go/internal/runtime"
)

// evalCatching runs Eval with a recover guard and reports whether the call
// panicked — #56's contract is that it never does.
func evalCatching(t *testing.T, expr string, input map[string]any) (val any, err error) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Eval panicked on %q: %v (#56: runtime errors must surface as named errors)", expr, r)
		}
	}()
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: input,
	})
	v, evalErr := NewEvaluator().Eval(ctx, expr, nil)
	return v.AsAny(), evalErr
}

// TestIntegerDivModZeroNamedError (#56): an integer divide/modulo by zero
// inside a flow expression is re-panicked by goval as a runtime.Error; Eval
// must convert it to a named error instead of letting it escape into the
// worker process (an uncaught panic kills the process and the PEL message
// re-crashes every reclaiming worker).
func TestIntegerDivModZeroNamedError(t *testing.T) {
	cases := []struct {
		name string
		expr string
	}{
		{"literal divide", "$F.add(1 / 0, 1)"},
		{"literal modulo", "$F.add(1 % 0, 1)"},
		{"parenthesized", "$F.add((1/0), 1)"},
		{"input-sourced", "$F.add($INPUT.a/$INPUT.z, 1)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := evalCatching(t, tc.expr, map[string]any{"a": 1, "z": 0})
			if err == nil {
				t.Fatalf("Eval(%q) = nil error, want a named runtime error", tc.expr)
			}
			if !errors.Is(err, ErrExpressionRuntime) {
				t.Fatalf("Eval(%q) error = %v, want errors.Is(err, ErrExpressionRuntime)", tc.expr, err)
			}
			if !strings.Contains(err.Error(), "integer divide by zero") {
				t.Fatalf("Eval(%q) error = %q, want the original runtime text preserved", tc.expr, err.Error())
			}
		})
	}
}

// TestIntegerDivModZeroErrorTypeIsDeterministic: the recovered error is the
// named *RuntimeError type — the engine-side deterministic bridge classifies
// it by type name (same input divides by zero again; retrying only re-fails).
func TestIntegerDivModZeroErrorTypeIsDeterministic(t *testing.T) {
	_, err := evalCatching(t, "$F.add(1 / 0, 1)", nil)
	if err == nil {
		t.Fatal("nil error, want a named runtime error")
	}
	var re *RuntimeError
	if !errors.As(err, &re) {
		t.Fatalf("error %v (%T) is not *RuntimeError", err, err)
	}
}
