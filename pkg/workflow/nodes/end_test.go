package nodes

import (
	"context"
	"reflect"
	"testing"

	"github.com/jeffkit/plaita-go/internal/runtime"

	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func TestEnd_Execute(t *testing.T) {
	tests := []struct {
		name       string
		runtimeCtx *runtime.Context
		endNode    *End
		want       *workflow.ExecuteResult
		wantErr    bool
	}{
		{
			name: "test simple end node",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"a": 1,
				},
			},
			endNode: &End{
				BaseNode: BaseNode{
					ID: "end",
					Output: map[string]any{
						"result": "$INPUT.a",
					},
					OutputType: workflow.Property{
						DataType: workflow.Object,
						Properties: map[string]workflow.Property{
							"result": {
								DataType: workflow.Integer,
							},
						},
					},
				},
			},
			want:    &workflow.ExecuteResult{Result: map[string]any{"result": 1}},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.endNode.Execute(runtime.WithWorkflowRuntimeContext(context.TODO(), tt.runtimeCtx))
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Execute() got = %v, want %v", got, tt.want)
			}
		})
	}
}
