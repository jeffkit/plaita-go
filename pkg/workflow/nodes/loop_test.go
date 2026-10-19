package nodes

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jeffkit/plaita-go/internal/runtime"

	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func TestLoop_Execute(t *testing.T) {
	tests := []struct {
		name       string
		runtimeCtx *runtime.Context
		loopNode   *Loop
		want       *workflow.ExecuteResult
		wantErr    bool
	}{
		{
			name: "test string expr loop node",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"collection": []any{
						map[string]any{"number": 1},
						map[string]any{"number": 2},
						map[string]any{"number": 3},
					},
					"number": 2,
				},
			},
			loopNode: &Loop{
				Child: Child{
					BaseNode: BaseNode{
						ID:         "loop",
						OutputType: workflow.Property{DataType: workflow.Integer},
						Output:     "$LOOP-RESULT",
					},
					ChildFlow: workflow.Workflow{
						ID: "workflow",
						Nodes: workflow.Nodes{
							&Start{BaseNode{ID: "start", Next: "end"}},
							&End{
								BaseNode: BaseNode{
									ID:         "end",
									OutputType: workflow.Property{DataType: workflow.Integer},
									Output:     "$INPUT.item.number",
								},
							},
						},
					},
				},
				Collection: loopCollection{expr: "$INPUT.collection"},
				Condition: Condition{
					ConditionType: conditionTypeLogicCondition,
					LogicCondition: LogicCondition{
						Field:    "$LOOP-ITEM.number",
						Operator: Lte,
						Value:    2,
					},
				},
			},
			want:    &workflow.ExecuteResult{Result: 3},
			wantErr: false,
		},
		{
			name: "test collection loop node",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"collection": []any{
						map[string]any{"number": 1},
						map[string]any{"number": 2},
						map[string]any{"number": 3},
					},
					"number": 2,
				},
			},
			loopNode: &Loop{
				Child: Child{
					BaseNode: BaseNode{
						ID:         "loop",
						OutputType: workflow.Property{DataType: workflow.Integer},
						Output:     "$LOOP-RESULT",
					},
					ChildFlow: workflow.Workflow{
						ID: "flow",
						Nodes: workflow.Nodes{
							&Start{BaseNode{ID: "start", Next: "end"}},
							&End{
								BaseNode: BaseNode{
									ID:         "end",
									OutputType: workflow.Property{DataType: workflow.Integer},
									Output:     "$INPUT.item",
								},
							},
						},
					},
				},
				Collection: loopCollection{array: []any{1, 2, 3}},
				ItemType:   workflow.Property{DataType: workflow.Integer},
				Condition: Condition{
					ConditionType: conditionTypeLogicCondition,
					LogicCondition: LogicCondition{
						Field:    "$LOOP-ITEM",
						Operator: Lte,
						Value:    2,
					},
				},
			},
			want:    &workflow.ExecuteResult{Result: 3},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.loopNode.Execute(runtime.WithWorkflowRuntimeContext(context.Background(), tt.runtimeCtx))
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

func TestLoop_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		data     string
		wantLoop *Loop
		wantErr  bool
	}{
		{
			name: "test loop collection for string expression",
			data: `
			{
				"id": "loop",
				"collection": "$INPUT.collection"
			}
			`,
			wantLoop: &Loop{
				Child:      Child{BaseNode: BaseNode{ID: "loop"}},
				Collection: loopCollection{expr: "$INPUT.collection"},
			},
			wantErr: false,
		},
		{
			name: "test loop collection for array",
			data: `
			{
				"id": "loop",
				"collection": [{"a": 1}, {"a": 2}, {"a": 3}]
			}
			`,
			wantLoop: &Loop{
				Child: Child{BaseNode: BaseNode{ID: "loop"}},
				Collection: loopCollection{
					array: []any{
						map[string]any{"a": float64(1)},
						map[string]any{"a": float64(2)},
						map[string]any{"a": float64(3)},
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := new(Loop)
			if err := got.UnmarshalJSON([]byte(tt.data)); (err != nil) != tt.wantErr {
				t.Errorf("UnmarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
			}
			require.Equal(t, tt.wantLoop, got)
		})
	}
}
