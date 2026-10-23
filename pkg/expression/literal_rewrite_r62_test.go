package expression

import (
	"context"
	"strings"
	"testing"

	"github.com/jeffkit/plaita-go/internal/runtime"
)

// #62 negative controls: the text rewrites applied to `$F.` statements before
// goval (prefix strip, `$` strip, trailing-comma drop, synthetic-identifier
// rewrite) must not reach inside quoted string arguments. Python leaves the
// same expressions byte-identical; every row below reproduces a HEAD bug
// (silent value change or `__pvN` leak) and pins the Python value.

func TestR62LiteralRewriteIsolation(t *testing.T) {
	ctx := runtime.WithWorkflowRuntimeContext(context.TODO(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a":    5,
			"list": []any{1, 2},
		},
	})
	tests := []struct {
		name string
		expr string
		want string
	}{
		{"dollar literal double quoted", `$F.concat("$", "5")`, "$5"},
		{"dollar literal single quoted", `$F.concat('$', '5')`, "$5"},
		{"ghost variable text", `$F.concat("see $INPUT.a here", $INPUT.a)`, "see $INPUT.a here5"},
		{"ghost variable bracket path", `$F.concat("see $INPUT.list[9] here", "x")`, "see $INPUT.list[9] herex"},
		{"trailing comma shape in literal", `$F.concat("a, )b", "x")`, "a, )bx"},
		{"env style dollar braces", `$F.concat("${HOME}", "")`, "${HOME}"},
		{"nested func call as text", `$F.concat("$F.add(1,2)", "")`, "$F.add(1,2)"},
		{"currency dollar", `$F.concat("cost: $100", "!")`, "cost: $100!"},
		{"home path dollar", `$F.concat("$HOME/bin", "")`, "$HOME/bin"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Eval(ctx, tt.expr)
			if err != nil {
				t.Fatalf("Eval() error = %v", err)
			}
			s, ok := got.(string)
			if !ok {
				t.Fatalf("Eval() = %T (%v), want string", got, got)
			}
			if s != tt.want {
				t.Errorf("Eval() = %q, want %q", s, tt.want)
			}
			if strings.Contains(s, "__pv") {
				t.Errorf("Eval() leaked the engine's internal identifier: %q", s)
			}
		})
	}
}

// Reverse controls (pre-fix green, must stay green): real trailing commas in
// argument lists keep being dropped, and literals that LOOK like variables but
// resolve fine keep evaluating.
func TestR62RewriteControls(t *testing.T) {
	ctx := runtime.WithWorkflowRuntimeContext(context.TODO(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a":    5,
			"test": map[string]any{"value": "x"},
		},
	})
	tests := []struct {
		name string
		expr string
		want string
	}{
		{"comma space inside literal stays", `$F.concat("a, b", "x")`, "a, bx"},
		{"trailing comma in args still dropped", `$F.getDictValue($INPUT.test, 'value',)`, "x"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Eval(ctx, tt.expr)
			if err != nil {
				t.Fatalf("Eval() error = %v", err)
			}
			if s, _ := got.(string); s != tt.want {
				t.Errorf("Eval() = %q, want %q", s, tt.want)
			}
		})
	}
}
