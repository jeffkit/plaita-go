package expression

import (
	"context"
	"reflect"
	"testing"

	"github.com/jeffkit/plaita-go/internal/runtime"
)

func TestEval(t *testing.T) {
	tests := []struct {
		name       string
		runtimeCtx *runtime.Context
		expr       any
		want       any
		wantErr    bool
	}{
		{
			name: "test regexp string expr",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"a": 1.0,
				},
				CurrentNodeContext: map[string]any{
					"a": map[string]any{
						"b": 1.0,
					},
				},
			},
			expr:    "you need know 1 + 1 = {%$F.add($a.b, $INPUT.a)%}",
			want:    "you need know 1 + 1 = 2",
			wantErr: false,
		},
		{
			name: "test multiple regexp string expr",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"a": 1.0,
				},
				CurrentNodeContext: map[string]any{
					"a": map[string]any{
						"b": 1.0,
					},
				},
			},
			expr:    "you need know {%$INPUT.a%} + {%$INPUT.a%} = {%$F.add($a.b, $INPUT.a)%}",
			want:    "you need know 1 + 1 = 2",
			wantErr: false,
		},
		{
			name: "test string expr",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"a": 1.0,
				},
				CurrentNodeContext: map[string]any{
					"a": map[string]any{
						"b": 1.0,
					},
				},
			},
			expr:    "$F.add($a.b, $INPUT.a)",
			want:    2.0,
			wantErr: false,
		},
		{
			name: "test map expr",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"a": 1,
				},
			},
			expr: map[string]any{
				"a": "$INPUT.a",
				"b": `$F.upper("abc")`,
			},
			want: map[string]any{
				"a": 1,
				"b": "ABC",
			},
			wantErr: false,
		},
		{
			name: "test slice expr",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"a": 1,
				},
			},
			expr: []any{
				"$F.add(1.0, 2.0)",
				"$INPUT.a",
			},
			want:    []any{3.0, 1},
			wantErr: false,
		},
		{
			name: "test string error source",
			runtimeCtx: &runtime.Context{
				ExecuteNodeErrors: map[string]any{
					"a": "param invalid",
				},
			},
			expr:    "$ERROR.a",
			want:    "param invalid",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := runtime.WithWorkflowRuntimeContext(context.TODO(), tt.runtimeCtx)
			got, err := Eval(ctx, tt.expr)
			if (err != nil) != tt.wantErr {
				t.Errorf("Eval() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Eval() got = %v, want %v", got, tt.want)
			}
		})
	}
}
