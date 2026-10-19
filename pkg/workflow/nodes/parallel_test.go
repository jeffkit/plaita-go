package nodes

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func TestParallel_Execute(t *testing.T) {
	tests := []struct {
		name         string
		runtimeCtx   *runtime.Context
		parallelNode *Parallel
		want         *workflow.ExecuteResult
		wantErr      bool
	}{
		{
			name: "test simple all branches completed",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"a": 1,
				},
			},
			parallelNode: &Parallel{
				BaseNode: BaseNode{ID: "parallel", Next: "end"},
				Branches: []ParallelBranch{
					{
						Branch: Branch{Name: "branch1"},
						Flow: workflow.Workflow{
							ID: "simple-workflow-1",
							Nodes: []workflow.Node{
								&Start{BaseNode{ID: "start", Next: "sleep"}},
								&Sleep{BaseNode: BaseNode{ID: "sleep", Next: "end"}, Duration: 50 * time.Millisecond},
								&End{
									BaseNode: BaseNode{
										ID:         "end",
										OutputType: workflow.Property{DataType: workflow.String},
										Output:     "finished",
									},
								},
							},
						},
						Input: nil,
					},
					{
						Branch: Branch{Name: "branch2"},
						Flow: workflow.Workflow{
							ID: "simple-workflow-2",
							Nodes: []workflow.Node{
								&Start{BaseNode{ID: "start", Next: "sleep"}},
								&Sleep{BaseNode: BaseNode{ID: "sleep", Next: "end"}, Duration: 150 * time.Millisecond},
								&End{
									BaseNode: BaseNode{
										ID:         "end",
										OutputType: workflow.Property{DataType: workflow.Integer},
										Output:     "$INPUT.a",
									},
								},
							},
						},
						Input: map[string]any{
							"a": "$INPUT.a",
						},
					},
					{
						Branch: Branch{Name: "branch3"},
						Flow: workflow.Workflow{
							ID: "simple-workflow-3",
							Nodes: []workflow.Node{
								&Start{BaseNode{ID: "start", Next: "sleep"}},
								&Sleep{BaseNode: BaseNode{ID: "sleep", Next: "end"}, Duration: 100 * time.Millisecond},
								&End{
									BaseNode: BaseNode{
										ID:         "end",
										OutputType: workflow.Property{DataType: workflow.String},
										Output:     "hello",
									},
								},
							},
						},
						Input: nil,
					},
				},
			},
			want: &workflow.ExecuteResult{
				Result: map[string]any{
					"branch1": "finished",
					"branch2": 1,
					"branch3": "hello",
				},
				NextNode: "end",
			},
			wantErr: false,
		},
		{
			name: "test branches completed with condition",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"a": 1,
					"b": "yes",
				},
			},
			parallelNode: &Parallel{
				BaseNode: BaseNode{Next: "end"},
				Branches: []ParallelBranch{
					{
						Branch: Branch{
							Name: "branch1",
							Condition: Condition{
								ConditionType: conditionTypeLogicCondition,
								LogicCondition: LogicCondition{
									Field:    "$INPUT.b",
									Operator: Eq,
									Value:    "yes",
								},
							},
						},
						Flow: workflow.Workflow{
							ID: "simple-workflow",
							Nodes: []workflow.Node{
								&Start{BaseNode{ID: "start", Next: "sleep"}},
								&Sleep{BaseNode: BaseNode{ID: "sleep", Next: "end"}, Duration: 50 * time.Millisecond},
								&End{
									BaseNode: BaseNode{
										ID:         "end",
										OutputType: workflow.Property{DataType: workflow.String},
										Output:     "finished",
									},
								},
							},
						},
						Input: map[string]any{
							"a": "$INPUT.a",
						},
					},
					{
						Branch: Branch{
							Name: "branch2",
							Condition: Condition{
								ConditionType: conditionTypeLogicCondition,
								LogicCondition: LogicCondition{
									Field:    "$INPUT.b",
									Operator: Eq,
									Value:    "yes",
								},
							},
						},
						Flow: workflow.Workflow{
							ID: "simple-workflow",
							Nodes: []workflow.Node{
								&Start{BaseNode{ID: "start", Next: "sleep"}},
								&Sleep{BaseNode: BaseNode{ID: "sleep", Next: "end"}, Duration: 150 * time.Millisecond},
								&End{
									BaseNode: BaseNode{
										ID:         "end",
										OutputType: workflow.Property{DataType: workflow.Integer},
										Output:     "$INPUT.a",
									},
								},
							},
						},
						Input: map[string]any{
							"a": "$INPUT.a",
						},
					},
					{
						Branch: Branch{
							Name: "branch3",
							Condition: Condition{
								ConditionType: conditionTypeLogicCondition,
								LogicCondition: LogicCondition{
									Field:    "$INPUT.b",
									Operator: Ne,
									Value:    "yes",
								},
							},
						},
						Flow: workflow.Workflow{
							ID: "simple-workflow",
							Nodes: []workflow.Node{
								&Start{BaseNode{ID: "start", Next: "sleep"}},
								&Sleep{BaseNode: BaseNode{ID: "sleep", Next: "end"}, Duration: 100 * time.Millisecond},
								&End{
									BaseNode: BaseNode{
										ID:         "end",
										OutputType: workflow.Property{DataType: workflow.String},
										Output:     "hello",
									},
								},
							},
						},
						Input: nil,
					},
				},
				IsConditional: true,
			},
			want: &workflow.ExecuteResult{
				Result: map[string]any{
					"branch1": "finished",
					"branch2": 1,
				},
				NextNode: "end",
			},
			wantErr: false,
		},
		{
			name: "test branches completed with condition and join branches",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"a": 1,
					"b": "yes",
				},
			},
			parallelNode: &Parallel{
				BaseNode: BaseNode{Next: "end"},
				Branches: []ParallelBranch{
					{
						Branch: Branch{
							Name: "branch1",
							Condition: Condition{
								ConditionType: conditionTypeLogicCondition,
								LogicCondition: LogicCondition{
									Field:    "$INPUT.b",
									Operator: Eq,
									Value:    "yes",
								},
							},
						},
						Flow: workflow.Workflow{
							ID: "simple-workflow",
							Nodes: []workflow.Node{
								&Start{BaseNode{ID: "start", Next: "sleep"}},
								&Sleep{BaseNode: BaseNode{ID: "sleep", Next: "end"}, Duration: 50 * time.Millisecond},
								&End{
									BaseNode: BaseNode{
										ID:         "end",
										OutputType: workflow.Property{DataType: workflow.String},
										Output:     "finished",
									},
								},
							},
						},
						Input: nil,
					},
					{
						Branch: Branch{
							Name: "branch2",
							Condition: Condition{
								ConditionType: conditionTypeLogicCondition,
								LogicCondition: LogicCondition{
									Field:    "$INPUT.b",
									Operator: Eq,
									Value:    "yes",
								},
							},
						},
						Flow: workflow.Workflow{
							ID: "simple-workflow",
							Nodes: []workflow.Node{
								&Start{BaseNode{ID: "start", Next: "sleep"}},
								&Sleep{BaseNode: BaseNode{ID: "sleep", Next: "end"}, Duration: 150 * time.Millisecond},
								&End{
									BaseNode: BaseNode{
										ID:         "end",
										OutputType: workflow.Property{DataType: workflow.Integer},
										Output:     "$INPUT.a",
									},
								},
							},
						},
						Input: map[string]any{
							"b": "$INPUT.b",
						},
					},
					{
						Branch: Branch{
							Name: "branch3",
							Condition: Condition{
								ConditionType: conditionTypeLogicCondition,
								LogicCondition: LogicCondition{
									Field:    "$INPUT.b",
									Operator: Ne,
									Value:    "yes",
								},
							},
						},
						Flow: workflow.Workflow{
							ID: "simple-workflow",
							Nodes: []workflow.Node{
								&Start{BaseNode{ID: "start", Next: "sleep"}},
								&Sleep{BaseNode: BaseNode{ID: "sleep", Next: "end"}, Duration: 100 * time.Millisecond},
								&End{
									BaseNode: BaseNode{
										ID:         "end",
										OutputType: workflow.Property{DataType: workflow.String},
										Output:     "hello",
									},
								},
							},
						},
						Input: nil,
					},
				},
				IsConditional: true,
				JoinBranches:  []string{"branch1"},
			},
			want: &workflow.ExecuteResult{
				Result: map[string]any{
					"branch1": "finished",
				},
				NextNode: "end",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.parallelNode.Execute(runtime.WithWorkflowRuntimeContext(context.TODO(), tt.runtimeCtx))
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
