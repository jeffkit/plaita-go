package nodes

import (
	"context"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

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
				Result:   "next-node",
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
				Result:   "default-node",
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
				Result:   "next-node1",
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
				Result:   "default-node",
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

// TestSwitch_Execute_BranchNameAsTarget：分支未声明 next 时按
// branch_name_as_target 语义回落 name 作为目标与节点结果
// （Python decide.py resolve_branch_target，issue #6）。
func TestSwitch_Execute_BranchNameAsTarget(t *testing.T) {
	sw := &Switch{
		BaseNode: BaseNode{ID: "sw"},
		Branches: []*Branch{
			{
				Name: "e1",
				Condition: Condition{
					LogicCondition: LogicCondition{Field: "$INPUT.a", Operator: Gt, Value: 5},
				},
				Next: "", // 未声明 next
			},
			{
				Name:      "default",
				IsDefault: true,
			},
		},
	}
	got, err := sw.Execute(runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{"a": 10},
	}))
	require.NoError(t, err)
	require.Equal(t, "e1", string(got.NextNode))
	require.Equal(t, "e1", got.Result)
}

// TestSwitch_Execute_NoMatchNoDefault：无匹配且无 default 仍报错、不写结果
// （issue #6 验收）。
func TestSwitch_Execute_NoMatchNoDefault(t *testing.T) {
	sw := &Switch{
		BaseNode: BaseNode{ID: "sw"},
		Branches: []*Branch{
			{
				Name: "b1",
				Condition: Condition{
					LogicCondition: LogicCondition{Field: "$INPUT.a", Operator: Gt, Value: 5},
				},
				Next: "next-node",
			},
		},
	}
	got, err := sw.Execute(runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{"a": 1},
	}))
	require.Error(t, err)
	require.Nil(t, got)
	require.Contains(t, err.Error(), "no branch matched")
}

// TestSwitch_Execute_NoTarget：命中分支既无 next 也无 name，显式报错而非静默。
func TestSwitch_Execute_NoTarget(t *testing.T) {
	sw := &Switch{
		BaseNode: BaseNode{ID: "sw"},
		Branches: []*Branch{
			{
				Name:      "",
				Condition: Condition{},
			},
		},
	}
	got, err := sw.Execute(runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{}))
	require.Error(t, err)
	require.Nil(t, got)
	require.Contains(t, err.Error(), "no next target")
}

// TestSwitch_Execute_ConcurrentSortSafety：并发执行同一共享 Workflow（真实
// 引擎路径：每次 Execute 自建 runtime context），排序只发生在副本上——
// Branches 顺序执行前后不变、命中结果稳定（issue #17）。runtime.Context
// 自身的并发读写保护属 internal/runtime 所有权（见报告联动项）。
func TestSwitch_Execute_ConcurrentSortSafety(t *testing.T) {
	sw := &Switch{
		BaseNode: BaseNode{ID: "sw"},
		Branches: []*Branch{
			{
				Name: "low",
				Condition: Condition{
					LogicCondition: LogicCondition{Field: "$INPUT.a", Operator: Gt, Value: 5},
				},
				Priority: 0,
				Next:     "end-low",
			},
			{
				Name: "high",
				Condition: Condition{
					LogicCondition: LogicCondition{Field: "$INPUT.a", Operator: Gt, Value: 5},
				},
				Priority: 100,
				Next:     "end-high",
			},
			{
				Name:      "default",
				IsDefault: true,
				Next:      "end-low",
			},
		},
	}
	before := make([]string, 0, len(sw.Branches))
	for _, b := range sw.Branches {
		before = append(before, b.Name)
	}

	// 两个 end 各自输出本路径标记：并发下 priority 排序必须稳定命中 high
	// 分支；$NODE.sw 同时验证 switch 的节点结果语义（#6）。
	//
	// 语义更新（engine-wiring，#17 重入保护接线后）：Workflow 实例不再可重入
	// （同实例并发 Execute 返回 ErrWorkflowReentry），但节点定义仍然是共享的
	// ——每个 goroutine 建各自的 Workflow 实例、指向同一组节点指针，保住本测
	// 试的真不变量：同一 *Switch 定义被并发执行时排序走副本、Branches 顺序
	// 不被改写。
	nodes := workflow.Nodes{
		&Start{BaseNode{ID: "start", Next: "sw"}},
		sw,
		&End{BaseNode: BaseNode{ID: "end-high", Output: "$NODE.sw"}},
		&End{BaseNode: BaseNode{ID: "end-low", Output: "$NODE.sw"}},
	}
	newFlow := func() *workflow.Workflow {
		return &workflow.Workflow{
			ID:      "switch-race-flow",
			Runtime: workflow.RuntimeGo,
			Version: "v1.0",
			Nodes:   nodes,
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := newFlow().Execute(context.Background(), map[string]any{"a": 10})
			require.NoError(t, err)
			require.Equal(t, "end-high", got, "concurrent execution must pick the same (priority-sorted) branch")
		}()
	}
	wg.Wait()

	after := make([]string, 0, len(sw.Branches))
	for _, b := range sw.Branches {
		after = append(after, b.Name)
	}
	require.Equal(t, before, after, "shared Branches order must not be mutated by Execute")
}

// TestSwitch_Execute_NodeResultWrittenToContext：命中分支目标写入节点结果，
// 下游以 $NODE.<switch> 读到（Python decide.py 输出契约，issue #6）。
func TestSwitch_Execute_NodeResultWrittenToContext(t *testing.T) {
	sw := &Switch{
		BaseNode: BaseNode{ID: "sw"},
		Branches: []*Branch{
			{
				Name: "yes",
				Condition: Condition{
					LogicCondition: LogicCondition{Field: "$INPUT.a", Operator: Gt, Value: 5},
				},
				Next: "e1",
			},
			{
				Name:      "no",
				IsDefault: true,
				Next:      "e2",
			},
		},
	}
	flow := &workflow.Workflow{
		ID:      "switch-result-flow",
		Runtime: workflow.RuntimeGo,
		Version: "v1.0",
		Nodes: workflow.Nodes{
			&Start{BaseNode{ID: "start", Next: "sw"}},
			sw,
			&End{BaseNode: BaseNode{ID: "e1", Output: "$NODE.sw"}},
		},
	}
	got, err := flow.Execute(context.Background(), map[string]any{"a": 10})
	require.NoError(t, err)
	require.Equal(t, "e1", got, "$NODE.sw must be the selected branch target (Python parity)")
}
