package plaitaexpr

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/jeffkit/plaita-go/internal/runtime"
)

// evalHelper evaluates expr against a runtime context.
func evalHelper(t *testing.T, runtimeCtx *runtime.Context, expr string) (any, error) {
	t.Helper()
	e := NewEvaluator()
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), runtimeCtx)
	v, err := e.Eval(ctx, expr, nil)
	if err != nil {
		return nil, err
	}
	return v.AsAny(), nil
}

// TestUnknownFunctionErrors pins the plaita unknown-function semantics of
// the default registry: an unregistered $F.<name> call is a loud error
// (plaita raises NameError with did-you-mean; goval panics "no such
// function", recovered as an error), and a failing argument errors before
// the function resolves (arguments evaluate first, as in plaita).
func TestUnknownFunctionErrors(t *testing.T) {
	runtimeCtx := &runtime.Context{
		WorkflowInput: map[string]any{
			"a": 1.0,
		},
	}
	tests := []struct {
		name    string
		expr    string
		want    any
		wantErr bool
	}{
		{
			name:    "unknown function errors",
			expr:    "$F.definitely_not_registered(1, 2)",
			wantErr: true,
		},
		{
			name:    "unknown function nested in known call errors",
			expr:    `$F.concat($F.definitely_not_registered("a"), "-b")`,
			wantErr: true,
		},
		{
			name: "unknown function with failing argument errors",
			expr: "$F.definitely_not_registered($GONE.x)",
			// $GONE is not a context root -> root-miss error, matching
			// plaita's KeyError raised while evaluating the argument.
			wantErr: true,
		},
		{
			name: "unknown function still shadows nothing registered",
			expr: "$F.add(1.0, 2.0)",
			want: 3.0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := evalHelper(t, runtimeCtx, tt.expr)
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

// TestNodeRawAccessor pins plaita#130's `$NODE.raw.<node>[.path...]`
// verbatim accessor: the value is returned as stored, and a node literally
// named "raw" keeps the plain path semantics (collision rule).
func TestNodeRawAccessor(t *testing.T) {
	runtimeCtx := &runtime.Context{
		WorkflowInput: map[string]any{
			"x": "INPUT",
		},
		ExecuteNodeResults: map[string]any{
			"step1": map[string]any{
				"data": "text with $INPUT.x and {% $F.add(1.0, 1.0) %}",
			},
		},
	}
	tests := []struct {
		name    string
		ctx     *runtime.Context
		expr    string
		want    any
		wantErr bool
	}{
		{
			name: "raw accessor returns stored value verbatim",
			ctx:  runtimeCtx,
			expr: "$NODE.raw.step1.data",
			want: "text with $INPUT.x and {% $F.add(1.0, 1.0) %}",
		},
		{
			name: "plain NODE path returns the same stored string (go runtime never re-evaluates)",
			ctx:  runtimeCtx,
			expr: "$NODE.step1.data",
			want: "text with $INPUT.x and {% $F.add(1.0, 1.0) %}",
		},
		{
			name: "raw accessor miss yields nil without error",
			ctx:  runtimeCtx,
			expr: "$NODE.raw.step1.missing",
			want: nil,
		},
		{
			name: "raw accessor on unknown node yields nil without error",
			ctx:  runtimeCtx,
			expr: "$NODE.raw.unknown_node",
			want: nil,
		},
		{
			name: "raw with single segment is a plain node lookup",
			ctx: &runtime.Context{
				ExecuteNodeResults: map[string]any{
					"raw": "raw-node-value",
				},
			},
			expr: "$NODE.raw",
			want: "raw-node-value",
		},
		{
			name: "node named raw wins the collision",
			ctx: &runtime.Context{
				ExecuteNodeResults: map[string]any{
					"raw":   map[string]any{"step1": "from-raw-node"},
					"step1": "plain-step1",
				},
			},
			expr: "$NODE.raw.step1",
			want: "from-raw-node",
		},
		{
			name: "numeric second segment is not the raw modifier",
			ctx: &runtime.Context{
				ExecuteNodeResults: map[string]any{
					"0": "zeroth",
				},
			},
			expr:    "$NODE.raw.0",
			wantErr: true, // resolves node "raw" (absent root miss) -> error
		},
		{
			name: "raw accessor through index reads stored element",
			ctx: &runtime.Context{
				ExecuteNodeResults: map[string]any{
					"step1": map[string]any{
						"list": []any{"$INPUT.x", "second"},
					},
				},
			},
			expr: "$NODE.raw.step1.list.1",
			want: "second",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := evalHelper(t, tt.ctx, tt.expr)
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

// TestMissingKeySemantics pins the plaita missing-key mapping: a missing
// field on an existing object yields nil (and field access through nil keeps
// yielding nil), while index misses and absent roots are errors.
func TestMissingKeySemantics(t *testing.T) {
	tests := []struct {
		name    string
		ctx     *runtime.Context
		expr    string
		want    any
		wantErr bool
	}{
		{
			name: "missing field on existing node result yields nil",
			ctx: &runtime.Context{
				ExecuteNodeResults: map[string]any{},
			},
			expr: "$NODE.missing",
			want: nil,
		},
		{
			name: "field access through nil keeps yielding nil",
			ctx: &runtime.Context{
				ExecuteNodeResults: map[string]any{},
			},
			expr: "$NODE.missing.deep",
			want: nil,
		},
		{
			name: "field access on scalar yields nil",
			ctx: &runtime.Context{
				WorkflowInput: map[string]any{
					"s": "a string",
				},
			},
			expr: "$INPUT.s.field",
			want: nil,
		},
		{
			name: "missing input field yields nil",
			ctx: &runtime.Context{
				WorkflowInput: map[string]any{"a": 1.0},
			},
			expr: "$INPUT.missing",
			want: nil,
		},
		{
			name: "missing global field yields nil",
			ctx: &runtime.Context{
				GlobalVariables: map[string]any{"set": 1.0},
			},
			expr: "$GLOBAL.unset",
			want: nil,
		},
		{
			name: "index out of bounds is an error",
			ctx: &runtime.Context{
				WorkflowInput: map[string]any{
					"list": []any{1.0, 2.0},
				},
			},
			expr:    "$INPUT.list.5",
			wantErr: true,
		},
		{
			name: "index through nil is an error",
			ctx: &runtime.Context{
				WorkflowInput: map[string]any{},
			},
			expr:    "$INPUT.missing.0",
			wantErr: true,
		},
		{
			name: "index miss on map is an error",
			ctx: &runtime.Context{
				WorkflowInput: map[string]any{
					"m": map[string]any{"0": "zero"},
				},
			},
			expr:    "$INPUT.m.1",
			wantErr: true,
		},
		{
			name: "numeric key on map still reads by key",
			ctx: &runtime.Context{
				WorkflowInput: map[string]any{
					"m": map[string]any{"0": "zero"},
				},
			},
			expr: "$INPUT.m.0",
			want: "zero",
		},
		{
			name:    "nil root container is an absent root error",
			ctx:     &runtime.Context{},
			expr:    "$GLOBAL.anything",
			wantErr: true,
		},
		{
			name: "bare root returns whole container",
			ctx: &runtime.Context{
				GlobalVariables: map[string]any{"a": 1.0},
			},
			expr: "$GLOBAL",
			want: map[string]any{"a": 1.0},
		},
		{
			name: "parameter root miss is an error",
			ctx: &runtime.Context{
				CurrentNodeContext: map[string]any{"myvar": map[string]any{"a": 1.0}},
			},
			expr:    "$nosuchroot.x",
			wantErr: true,
		},
		{
			name: "parameter field miss yields nil",
			ctx: &runtime.Context{
				CurrentNodeContext: map[string]any{"myvar": map[string]any{"a": 1.0}},
			},
			expr: "$myvar.missing",
			want: nil,
		},
		{
			name: "env field miss yields nil",
			ctx: &runtime.Context{
				EnvVariables: map[string]any{"a": "10"},
			},
			expr: "$ENV.missing",
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := evalHelper(t, tt.ctx, tt.expr)
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

// TestRawAccessorInFunctionArg checks the raw modifier survives statement
// compilation ($F.* args are resolved by goval from the exec args map).
func TestRawAccessorInFunctionArg(t *testing.T) {
	runtimeCtx := &runtime.Context{
		ExecuteNodeResults: map[string]any{
			"step1": map[string]any{"data": "raw text"},
		},
	}
	got, err := evalHelper(t, runtimeCtx, `$F.concat("pre-", $NODE.raw.step1.data)`)
	if err != nil {
		t.Fatalf("Eval() error = %v", err)
	}
	if got != "pre-raw text" {
		t.Errorf("Eval() got = %#v, want %#v", got, "pre-raw text")
	}
}

// TestUnknownFunctionErrorMessage checks the error names the function,
// mirroring plaita's NameError naming the missing callable.
func TestUnknownFunctionErrorMessage(t *testing.T) {
	_, err := evalHelper(t, &runtime.Context{}, "$F.definitely_not_registered()")
	if err == nil {
		t.Fatal("expected an error for an unknown function")
	}
	if !strings.Contains(err.Error(), "definitely_not_registered") {
		t.Errorf("error %q does not name the unknown function", err)
	}
}
