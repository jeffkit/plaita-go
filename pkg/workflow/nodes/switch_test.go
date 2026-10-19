package nodes

import (
	"context"
	"reflect"
	"testing"

	"github.com/jeffkit/plaita-go/internal/runtime"

	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func TestSwitch_Execute(t *testing.T) {
	tests := []struct {
		name       string
		runtimeCtx *runtime.Context
		switchNode *Switch
		want       *workflow.ExecuteResult
		wantErr    bool
	}{
		{
			name: "test normal switch node with single branch match condition",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"a": 10,
				},
			},
			switchNode: &Switch{
				BaseNode: BaseNode{},
				Branches: []*Branch{
					{
						Name: "b1",
						Condition: Condition{
							LogicCondition: LogicCondition{
								Field:    "$INPUT.a",
								Operator: Gt,
								Value:    5,
							},
						},
						Next: "next-node",
					},
					{
						Name:      "default",
						IsDefault: true,
					},
				},
			},
			want: &workflow.ExecuteResult{
				NextNode: "next-node",
			},
			wantErr: false,
		},
		{
			name: "test switch node with single branch and mismatch condition",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"a": 1,
				},
			},
			switchNode: &Switch{
				BaseNode: BaseNode{},
				Branches: []*Branch{
					{
						Name: "b1",
						Condition: Condition{
							LogicCondition: LogicCondition{
								Field:    "$INPUT.a",
								Operator: Gt,
								Value:    5,
							},
						},
						Next: "next-node",
					},
					{
						Name:      "default",
						IsDefault: true,
						Next:      "default-node",
					},
				},
			},
			want: &workflow.ExecuteResult{
				NextNode: "default-node",
			},
			wantErr: false,
		},
		{
			name: "test multi branch match condition",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"a": 10,
					"b": map[string]any{
						"c": "abc",
					},
				},
			},
			switchNode: &Switch{
				BaseNode: BaseNode{},
				Branches: []*Branch{
					{
						Name: "b1",
						Condition: Condition{
							LogicCondition: LogicCondition{
								Field:    "$INPUT.a",
								Operator: Gt,
								Value:    5,
							},
						},
						Next: "next-node1",
					},
					{
						Name: "b2",
						Condition: Condition{
							LogicCondition: LogicCondition{
								Field:    "$INPUT.b.c",
								Operator: Eq,
								Value:    "abc",
							},
						},
						Next: "next-node2",
					},
					{
						Name:      "default",
						IsDefault: true,
						Next:      "default-node",
					},
				},
			},
			want: &workflow.ExecuteResult{
				NextNode: "next-node1",
			},
			wantErr: false,
		},
		{
			name: "test multi branch and mismatch condition",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"a": 1,
					"b": map[string]any{
						"c": "abcd",
					},
				},
			},
			switchNode: &Switch{
				BaseNode: BaseNode{},
				Branches: []*Branch{
					{
						Name: "b1",
						Condition: Condition{
							LogicCondition: LogicCondition{
								Field:    "$INPUT.a",
								Operator: Gt,
								Value:    5,
							},
						},
						Next: "next-node1",
					},
					{
						Name: "b2",
						Condition: Condition{
							LogicCondition: LogicCondition{
								Field:    "$INPUT.b.c",
								Operator: Eq,
								Value:    "abc",
							},
						},
						Next: "next-node2",
					},
					{
						Name:      "default",
						IsDefault: true,
						Next:      "default-node",
					},
				},
			},
			want: &workflow.ExecuteResult{
				NextNode: "default-node",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.switchNode.Execute(runtime.WithWorkflowRuntimeContext(context.Background(), tt.runtimeCtx))
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
