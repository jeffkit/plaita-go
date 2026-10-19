package nodes

import (
	"context"
	"reflect"
	"testing"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/providers/memory"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func TestReference_Execute_with_childFlow(t *testing.T) {
	childFlow := &workflow.Workflow{
		ID:      "test-flow",
		Version: "v1.0",
		Nodes: []workflow.Node{
			&Start{
				BaseNode: BaseNode{ID: "start", Next: "end"},
			},
			&End{
				BaseNode: BaseNode{
					ID: "end",
					OutputType: workflow.Property{
						DataType: workflow.Integer,
					},
					Output: "$INPUT.number",
				},
			},
		},
	}
	referenceNode := &Reference{
		BaseNode:   BaseNode{ID: "reference", Next: "end"},
		ChildFlow:  childFlow,
		FlowParams: map[string]any{"number": 1},
	}
	got, err := referenceNode.Execute(runtime.WithWorkflowRuntimeContext(context.TODO(), &runtime.Context{}))
	if err != nil {
		t.Errorf("Execute() error = %v, wantErr %v", err, false)
	}

	if !reflect.DeepEqual(got, &workflow.ExecuteResult{NextNode: "end", Result: 1}) {
		t.Errorf("Execute() got = %v, want %v", got, &workflow.ExecuteResult{NextNode: "end", Result: 1})
	}
}

func TestReference_Execute(t *testing.T) {
	testProvider := memory.FromWorkflows([]*workflow.Workflow{
		{
			ID:      "test-flow",
			Version: "v1.0",
			Nodes: []workflow.Node{
				&Start{
					BaseNode: BaseNode{ID: "start", Next: "end"},
				},
				&End{
					BaseNode: BaseNode{
						ID: "end",
						OutputType: workflow.Property{
							DataType: workflow.Integer,
						},
						Output: "$INPUT.number",
					},
				},
			},
		},
	}...)

	tests := []struct {
		name          string
		runtimeCtx    *runtime.Context
		referenceNode *Reference
		want          *workflow.ExecuteResult
		wantErr       bool
	}{
		{
			name:       "test simple reference node",
			runtimeCtx: &runtime.Context{},
			referenceNode: &Reference{
				BaseNode:    BaseNode{ID: "reference", Next: "end"},
				FlowID:      "test-flow",
				FlowVersion: "v1.0",
				FlowParams:  map[string]any{"number": 1},

				workflowsProvider: testProvider,
			},
			want:    &workflow.ExecuteResult{NextNode: "end", Result: 1},
			wantErr: false,
		},
		{
			name:       "test reference workflow not found",
			runtimeCtx: &runtime.Context{},
			referenceNode: &Reference{
				BaseNode:    BaseNode{Next: "end"},
				FlowID:      "test-flow",
				FlowVersion: "v2.0",
				FlowParams:  map[string]any{"number": 1},

				workflowsProvider: testProvider,
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.referenceNode.Execute(runtime.WithWorkflowRuntimeContext(context.TODO(), tt.runtimeCtx))
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
