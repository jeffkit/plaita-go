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
					ChildFlow: &workflow.Workflow{
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
					ChildFlow: &workflow.Workflow{
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

// TestLoopCollection_Elements：collection 归一对齐 Python loop._coerce_collection
// （issue #27）：JSON 数组字符串按数组解析、普通字符串按单元素集合、nil 空集合、
// 不可迭代标量报错。
func TestLoopCollection_Elements(t *testing.T) {
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{})

	t.Run("json array string parses as array", func(t *testing.T) {
		got, err := loopCollection{expr: "[1,2,3]"}.Elements(ctx)
		require.NoError(t, err)
		require.Equal(t, []any{float64(1), float64(2), float64(3)}, got)
	})
	t.Run("json array string with spaces parses as array", func(t *testing.T) {
		got, err := loopCollection{expr: " [1, 2] "}.Elements(ctx)
		require.NoError(t, err)
		require.Equal(t, []any{float64(1), float64(2)}, got)
	})
	t.Run("plain string is a single-element collection", func(t *testing.T) {
		got, err := loopCollection{expr: "abc"}.Elements(ctx)
		require.NoError(t, err)
		require.Equal(t, []any{"abc"}, got)
	})
	t.Run("broken json array falls back to single element", func(t *testing.T) {
		got, err := loopCollection{expr: "[1,2"}.Elements(ctx)
		require.NoError(t, err)
		require.Equal(t, []any{"[1,2"}, got)
	})
	t.Run("nil expression result is an empty collection", func(t *testing.T) {
		got, err := loopCollection{expr: "$NULL"}.Elements(ctx)
		require.NoError(t, err)
		require.Nil(t, got)
	})
	t.Run("array passthrough", func(t *testing.T) {
		got, err := loopCollection{array: []any{1, 2, 3}}.Elements(ctx)
		require.NoError(t, err)
		require.Equal(t, []any{1, 2, 3}, got)
	})
	t.Run("map value iterates keys like Python list(dict)", func(t *testing.T) {
		got, err := loopCollection{expr: "$INPUT.m"}.Elements(runtime.WithWorkflowRuntimeContext(context.Background(),
			&runtime.Context{WorkflowInput: map[string]any{"m": map[string]any{"a": 1, "b": 2}}}))
		require.NoError(t, err)
		require.Len(t, got, 2)
	})
	t.Run("non-iterable scalar errors like Python TypeError", func(t *testing.T) {
		_, err := loopCollection{expr: "$INPUT.n"}.Elements(runtime.WithWorkflowRuntimeContext(context.Background(),
			&runtime.Context{WorkflowInput: map[string]any{"n": 3.0}}))
		require.Error(t, err)
		require.Contains(t, err.Error(), "not iterable")
	})
}

// TestLoop_Execute_StringCollections：issue #27 实测矩阵走整节点执行路径。
func TestLoop_Execute_StringCollections(t *testing.T) {
	newLoop := func(expr string) *Loop {
		return &Loop{
			Child: Child{
				BaseNode: BaseNode{ID: "loop"},
				ChildFlow: &workflow.Workflow{
					ID: "workflow",
					Nodes: workflow.Nodes{
						&Start{BaseNode{ID: "start", Next: "end"}},
						&End{BaseNode: BaseNode{ID: "end", Output: "$INPUT.item"}},
					},
				},
			},
			Collection: loopCollection{expr: expr},
		}
	}

	t.Run("json array string iterates 3 times, result is last item", func(t *testing.T) {
		got, err := newLoop("[1,2,3]").Execute(runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{}))
		require.NoError(t, err)
		require.Equal(t, float64(3), got.Result)
	})
	t.Run("plain string iterates once, result is the string", func(t *testing.T) {
		got, err := newLoop("abc").Execute(runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{}))
		require.NoError(t, err)
		require.Equal(t, "abc", got.Result)
	})
	t.Run("nil collection iterates zero times, result is nil", func(t *testing.T) {
		got, err := newLoop("$NULL").Execute(runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{}))
		require.NoError(t, err)
		require.Nil(t, got.Result)
	})
}
