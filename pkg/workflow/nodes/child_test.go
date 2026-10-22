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

// plaita 实测（r78）：child 的 input 缺省（求值为 None）→ 子流程继承调用方
// $INPUT——child 与 reference 同款语义（Python 子执行 params=None 走继承）。
func TestChildrenFlow_AbsentInputInheritsCallerInput(t *testing.T) {
	childNode := &Child{
		BaseNode: BaseNode{ID: "child", Next: "next-node"},
		ChildFlow: &workflow.Workflow{
			ID: "test-workflow",
			Nodes: workflow.Nodes{
				&Start{BaseNode{ID: "start", Next: "end"}},
				&End{BaseNode: BaseNode{ID: "end", Output: "$INPUT"}},
			},
		},
	}
	parentCtx := runtime.WithWorkflowRuntimeContext(context.Background(),
		&runtime.Context{WorkflowInput: map[string]any{"z": 42}})
	got, err := childNode.Execute(parentCtx)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	want := map[string]any{"z": 42}
	if !reflect.DeepEqual(got.Result, want) {
		t.Errorf("Execute() got = %v, want %v", got.Result, want)
	}
}
