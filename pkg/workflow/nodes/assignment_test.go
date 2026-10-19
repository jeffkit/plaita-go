package nodes

import (
	"context"
	"reflect"
	"testing"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func TestAssignment_Execute(t *testing.T) {
	tests := []struct {
		name       string
		runtimeCtx *runtime.Context
		assignment *Assignment
		want       *workflow.ExecuteResult
		wantErr    bool
	}{
		{
			name: "test no upstream output",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"value": "Hello World!",
				},
			},
			assignment: &Assignment{
				BaseNode: BaseNode{
					ID: "test",
					OutputType: workflow.Property{
						DataType: workflow.String,
						Name:     "assignmentValue",
					},
					Output: "$INPUT.value",
					Next:   "assignment-next",
				},
				UpstreamOutput: nil,
			},
			want:    &workflow.ExecuteResult{Result: "Hello World!", NextNode: "assignment-next"},
			wantErr: false,
		},
		{
			name: "test assignment node with upstream output",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"value": "Hello World!",
				},
				LastNodeID: "previous-node",
			},
			assignment: &Assignment{
				BaseNode: BaseNode{
					ID: "test",
					OutputType: workflow.Property{
						DataType: workflow.String,
						Name:     "assignmentValue",
					},
					Output: "$INPUT.value",
					Next:   "assignment-next",
				},
				UpstreamOutput: []UpstreamValue{
					{
						Upstream: "previous-node",
						Value:    "$INPUT.value",
					},
				},
			},
			want:    &workflow.ExecuteResult{Result: "Hello World!", NextNode: "assignment-next"},
			wantErr: false,
		},
		{
			name: "test assignment node with unknown upstream output",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"value": "Hello World!",
				},
				LastNodeID: "unknown-node",
			},
			assignment: &Assignment{
				BaseNode: BaseNode{
					ID: "test",
					OutputType: workflow.Property{
						DataType: workflow.String,
						Name:     "assignmentValue",
					},
					Output: "$INPUT.value",
					Next:   "assignment-next",
				},
				UpstreamOutput: []UpstreamValue{
					{
						Upstream: "previous-node",
						Value:    "$INPUT.value",
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.assignment.Execute(runtime.WithWorkflowRuntimeContext(context.TODO(), tt.runtimeCtx))
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
