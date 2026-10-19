package nodes

import (
	"context"
	"reflect"
	"testing"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func TestChildrenFlow_Execute(t *testing.T) {
	tests := []struct {
		name         string
		runtimeCtx   *runtime.Context
		childrenNode *Child
		want         *workflow.ExecuteResult
		wantErr      bool
	}{
		{
			name:       "test simple child workflow node",
			runtimeCtx: &runtime.Context{},
			childrenNode: &Child{
				BaseNode: BaseNode{
					ID:   "child",
					Next: "next-node",
				},
				ChildFlow: &workflow.Workflow{
					ID: "test-workflow",
					InputType: workflow.Property{
						DataType: workflow.Object,
						Properties: map[string]workflow.Property{
							"name": {
								DataType: workflow.String,
							},
							"age": {
								DataType: workflow.Integer,
							},
						},
					},
					OutputType: workflow.Property{
						DataType: workflow.String,
					},
					Nodes: workflow.Nodes{
						&Start{BaseNode{ID: "start", Next: "end"}},
						&End{BaseNode: BaseNode{ID: "end", Output: "children workflow execute completed"}},
					},
				},
				Input: map[string]any{
					"name": "John",
					"age":  18,
				},
			},
			want: &workflow.ExecuteResult{
				Result:   "children workflow execute completed",
				NextNode: "next-node",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.childrenNode.Execute(runtime.WithWorkflowRuntimeContext(context.Background(), tt.runtimeCtx))
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
