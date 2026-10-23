package expression

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/jeffkit/plaita-go/internal/runtime"
)

// #63 negative controls: the variable branch of parser.Parse has no
// parse_all=True equivalent — trailing text outside the grammar
// (`$INPUT.a + 1`, `$INPUT.age > 18`, ...) used to be swallowed into the path
// and silently evaluated to nil. Python raises ParseException for every row
// below; the Go engine must refuse with its `unable to parse` prefix.

func TestR63VariablePathFullMatch(t *testing.T) {
	ctx := runtime.WithWorkflowRuntimeContext(context.TODO(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a":   5,
			"n":   "tom",
			"age": 30,
		},
	})
	tests := []struct {
		name string
		expr string
	}{
		{"arithmetic", "$INPUT.a + 1"},
		{"comparison", "$INPUT.a > 1"},
		{"comparison named field", "$INPUT.age > 18"},
		{"logical or", `$INPUT.n || "x"`},
		{"extra token", "$INPUT.a 1"},
		{"dangling dot", "$INPUT."},
		{"extra text after bracket index", "$INPUT.list[0] extra"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Eval(ctx, tt.expr)
			if err == nil {
				t.Fatalf("Eval() = %v, err = nil, want parse error", got)
			}
			if !strings.Contains(err.Error(), "unable to parse") {
				t.Errorf("Eval() error = %q, want `unable to parse` prefix", err)
			}
		})
	}
}

// Template fragments that do not parse stay literal — the facade's documented
// contract (expression.go), which only becomes reachable once Parse enforces
// full-match (HEAD rendered this as "x None y").
func TestR63TemplateFragmentStaysLiteral(t *testing.T) {
	ctx := runtime.WithWorkflowRuntimeContext(context.TODO(), &runtime.Context{
		WorkflowInput: map[string]any{"a": 5},
	})
	got, err := Eval(ctx, "x {% $INPUT.a + 1 %} y")
	if err != nil {
		t.Fatalf("Eval() error = %v", err)
	}
	if s, _ := got.(string); s != "x {% $INPUT.a + 1 %} y" {
		t.Errorf("Eval() = %q, want literal template", s)
	}
}

// Reverse controls (green before and after): legal shapes the validator must
// keep accepting, with the exact values Python produces.
func TestR63VariablePathControls(t *testing.T) {
	users := []any{
		map[string]any{"name": "u0"},
		map[string]any{"name": "u1"},
	}
	ctx := runtime.WithWorkflowRuntimeContext(context.TODO(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a":     5,
			"n":     "tom",
			"age":   30,
			"a-b":   9,
			"list":  []any{1, 2},
			"m":     map[string]any{"k": "v"},
			"users": users,
		},
	})
	tests := []struct {
		name string
		expr string
		want any
	}{
		{"dash is a field character", "$INPUT.a-b", 9},
		{"dollar is a field character (missing key yields nil)", "$INPUT.a$b", nil},
		{"bracket index", "$INPUT.list[0]", 1},
		{"dot int index", "$INPUT.list.0", 1},
		{"negative bracket index", "$INPUT.list[-1]", 2},
		{"chained bracket then field", "$INPUT.users[0].name", "u0"},
		{"negative bracket then field", "$INPUT.users[-1].name", "u1"},
		{"users dot int index", "$INPUT.users.0", users[0]},
		{"whole input map is legal", "$INPUT.n", "tom"},
		{"missing dollar-ish key yields nil not parse error", "$INPUT.missing$key", nil},
		{"trailing comma in func args still fine", `$F.getDictValue($INPUT.m, 'k',)`, "v"},
		{"func argument arithmetic stays goval territory", "$F.add($INPUT.a, 1)", 6},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Eval(ctx, tt.expr)
			if err != nil {
				t.Fatalf("Eval() error = %v", err)
			}
			if tt.want == nil {
				if got != nil {
					t.Errorf("Eval() = %v, want nil", got)
				}
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Eval() = %#v, want %#v", got, tt.want)
			}
		})
	}
}
