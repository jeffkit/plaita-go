package expression

import (
	"context"
	"reflect"
	"testing"

	"github.com/jeffkit/plaita-go/internal/runtime"
)

// evalStringHelper evaluates a single string expr against a runtime context.
func evalStringHelper(t *testing.T, runtimeCtx *runtime.Context, expr string) (any, error) {
	t.Helper()
	ctx := runtime.WithWorkflowRuntimeContext(context.TODO(), runtimeCtx)
	return Eval(ctx, expr)
}

// TestEval_PlaitaStringDispatch pins the facade dispatch (plaita
// ExpressionParser._eval): "$..." is a whole expression and keeps the
// expression's native type, strings containing "{% ... %}" are templates, and
// everything else is plain text returned unchanged.
func TestEval_PlaitaStringDispatch(t *testing.T) {
	baseCtx := func() *runtime.Context {
		return &runtime.Context{
			WorkflowInput: map[string]any{
				"a": 1.0,
				"list": []any{
					map[string]any{"name": "hello"},
				},
			},
			CurrentNodeContext: map[string]any{
				"a": map[string]any{"b": 2.0},
			},
		}
	}
	tests := []struct {
		name    string
		expr    string
		want    any
		wantErr bool
	}{
		{
			name: "whole expression keeps native type (map)",
			expr: "$INPUT",
			want: map[string]any{"a": 1.0, "list": []any{map[string]any{"name": "hello"}}},
		},
		{
			name: "whole expression keeps native type (number)",
			expr: "$INPUT.a",
			want: 1.0,
		},
		{
			name: "whole expression function call keeps native type",
			expr: "$F.add($a.b, $INPUT.a)",
			want: 3.0,
		},
		{
			name: "plain text returned unchanged",
			expr: "hello world, nothing to evaluate here",
			want: "hello world, nothing to evaluate here",
		},
		{
			name: "plain text with dollar sign but no braces returned unchanged",
			expr: "cost is $100 total",
			want: "cost is $100 total",
		},
		{
			name: "single interpolation stringifies value",
			expr: "v: {% $INPUT.a %}",
			want: "v: 1.0", // Python str(1.0) == "1.0" (issue #8)
		},
		{
			name: "whole-string template is still a string (not native type)",
			expr: "{% $INPUT.a %}",
			want: "1.0",
		},
		{
			name: "multi interpolation",
			expr: "{%$INPUT.a%} and {%$a.b%}",
			want: "1.0 and 2.0",
		},
		{
			name: "multi-line template with multiple fragments",
			expr: "line1\nname={%$INPUT.list.0.name%}\nsum={% $F.add($INPUT.a, $a.b) %}",
			want: "line1\nname=hello\nsum=3.0",
		},
		{
			name: "fragment without dollar prefix stays literal",
			expr: "hi {%world%}",
			want: "hi {%world%}",
		},
		{
			name: "wrong-prefix fragment stays literal",
			expr: "x {%@INPUT.a%} y",
			want: "x {%@INPUT.a%} y",
		},
		{
			name: "unclosed fragment stays literal",
			expr: "a {% b",
			want: "a {% b",
		},
		{
			name: "trailing unclosed fragment stays literal after valid one",
			expr: "a {% $INPUT.a %} b {%",
			want: "a 1.0 b {%",
		},
		{
			name: "empty fragment stays literal",
			expr: "x {% %} y",
			want: "x {% %} y",
		},
		{
			name: "curly braces without percent markers are not interpolation",
			expr: "{user.name}",
			want: "{user.name}",
		},
		{
			name:    "unknown function inside fragment is an error",
			expr:    "x {% $F.definitely_not_registered(1) %}",
			wantErr: true,
		},
		{
			name:    "missing root inside fragment is an error",
			expr:    "x {% $GONE.y %}",
			wantErr: true,
		},
		{
			name:    "missing root whole expression is an error",
			expr:    "$GONE.y",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := evalStringHelper(t, baseCtx(), tt.expr)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Eval() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Eval() got = %#v, want %#v", got, tt.want)
			}
		})
	}
}

// TestEval_TemplateInterpolationErrorPropagates pins that an evaluation
// error of a matched fragment propagates (plaita lets the thunk error escape
// the template), while unparsable fragments stay literal.
func TestEval_TemplateInterpolationErrorPropagates(t *testing.T) {
	runtimeCtx := &runtime.Context{
		WorkflowInput: map[string]any{"a": 1.0},
	}
	// Valid fragment + erroring fragment: the error wins.
	_, err := evalStringHelper(t, runtimeCtx, "ok {% $INPUT.a %} bad {% $GONE.x %}")
	if err == nil {
		t.Fatal("Eval() expected error for erroring fragment, got nil")
	}
}

// TestEval_MapAndListRecursion keeps pinning container recursion.
func TestEval_MapAndListRecursion(t *testing.T) {
	runtimeCtx := &runtime.Context{
		WorkflowInput: map[string]any{"a": 1.0},
	}
	got, err := evalStringHelper(t, runtimeCtx, "missing field stays nil: {% $INPUT.missing.deep %}")
	if err != nil {
		t.Fatalf("Eval() error = %v", err)
	}
	// Python str(None) == "None" (issue #8: the old cast.ToString rendered "")
	if got != "missing field stays nil: None" {
		t.Errorf("Eval() got = %#v", got)
	}
}

// TestEval_TemplatePythonStr pins issue #8's acceptance table: template
// interpolation renders values with Python str() semantics — None → "None",
// True/False, dicts/lists as Python repr — and bare-constant fragments
// (true/None/numbers/strings) evaluate like plaita's template grammar
// (`constant | function_call | variable`), instead of staying literal.
func TestEval_TemplatePythonStr(t *testing.T) {
	runtimeCtx := &runtime.Context{
		WorkflowInput: map[string]any{
			"map":     map[string]any{"b": 2.0, "a": 1.0},
			"list":    []any{1.0, 2.0, 3.0},
			"missing": nil,
			"s":       "txt",
		},
	}
	tests := []struct {
		name string
		expr string
		want string
	}{
		// the acceptance table of issue #8 (dict key order is the documented
		// Go divergence: Go maps are unordered, rendering is sorted)
		{"map renders as Python repr", "{% $INPUT.map %}", "{'a': 1.0, 'b': 2.0}"},
		{"list renders as Python repr", "{% $INPUT.list %}", "[1.0, 2.0, 3.0]"},
		{"None renders as None", "{% $INPUT.missing %}", "None"},
		{"bool literal fragment", "{% true %}", "True"},
		{"False literal fragment", "{% false %}", "False"},
		{"None literal fragment", "{% None %}", "None"},
		{"null literal fragment", "{% null %}", "None"},
		{"int literal fragment", "{% 1 %}", "1"},
		{"float literal fragment", "{% 2.5 %}", "2.5"},
		{"int kept int-shaped", "{% $F.add(1, 2) %}", "3"},
		{"bool renders capitalized", "{% $F.eq(1, 1) %}", "True"},
		{"quoted string fragment", "{% 'txt' %}", "txt"},
		{"plain text unchanged", "hello {% world %}", "hello {% world %}"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := evalStringHelper(t, runtimeCtx, tt.expr)
			if err != nil {
				t.Fatalf("Eval() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("Eval() got = %#v, want %#v", got, tt.want)
			}
		})
	}
}

// TestEval_TemplateStructuredContentE2E pins the issue #8 agentrun-prompt
// scenario: an upstream node's structured result interpolated into a prompt
// keeps its content instead of silently collapsing to "".
func TestEval_TemplateStructuredContentE2E(t *testing.T) {
	runtimeCtx := &runtime.Context{
		ExecuteNodeResults: map[string]any{
			"previous": map[string]any{
				"items": []any{"alpha", "beta"},
				"count": 2.0,
			},
		},
	}
	got, err := evalStringHelper(t, runtimeCtx,
		"Summarize this: {% $NODE.previous %}")
	if err != nil {
		t.Fatalf("Eval() error = %v", err)
	}
	want := "Summarize this: {'count': 2.0, 'items': ['alpha', 'beta']}"
	if got != want {
		t.Errorf("Eval() got = %#v, want %#v", got, want)
	}
}

// TestEval_MutatorLiveRootRefused pins issue #9's write protection at the
// facade level: mutator calls over live context roots are named errors, the
// context object is untouched.
func TestEval_MutatorLiveRootRefused(t *testing.T) {
	runtimeCtx := &runtime.Context{
		WorkflowInput: map[string]any{
			"m":    map[string]any{"a": 1.0},
			"list": []any{1.0, 2.0, 3.0},
		},
	}
	for _, expr := range []string{
		`$F.set($INPUT.m, 'c', 3)`,
		`$F.clear($INPUT.m)`,
		`$F.setListItem($INPUT.list, 0, 9)`,
		`$F.pop($INPUT.list, 0)`,
		`$F.delete($INPUT.m, 'a')`,
	} {
		t.Run(expr, func(t *testing.T) {
			_, err := evalStringHelper(t, runtimeCtx, expr)
			if err == nil {
				t.Fatal("Eval() expected a plaita#69 write-protection error, got nil")
			}
		})
	}
	// the live context is unchanged after all the refusals
	m := runtimeCtx.WorkflowInput.(map[string]any)["m"].(map[string]any)
	if len(m) != 1 {
		t.Errorf("live map was mutated: %#v", m)
	}
	l := runtimeCtx.WorkflowInput.(map[string]any)["list"].([]any)
	if !reflect.DeepEqual(l, []any{1.0, 2.0, 3.0}) {
		t.Errorf("live list was mutated: %#v", l)
	}
}
