package nodes

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/duration"
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
			name: "test simple all branches joined",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"a": 1,
				},
			},
			parallelNode: &Parallel{
				BaseNode: BaseNode{ID: "parallel", Next: "end"},
				Branches: []*ParallelBranch{
					{
						Branch: Branch{Name: "branch1"},
						Flow: &workflow.Workflow{
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
						Flow: &workflow.Workflow{
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
						Flow: &workflow.Workflow{
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
				JoinBranches: []string{"branch1", "branch2", "branch3"},
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
				Branches: []*ParallelBranch{
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
						Flow: &workflow.Workflow{
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
						Flow: &workflow.Workflow{
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
						Flow: &workflow.Workflow{
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
				// 空 JoinBranches = 全部后台（fire-and-forget）——为保留"条件
				// 命中的分支都 join"的原断言，这里显式列出要 join 的分支。
				JoinBranches: []string{"branch1", "branch2"},
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
				Branches: []*ParallelBranch{
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
						Flow: &workflow.Workflow{
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
						Flow: &workflow.Workflow{
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
						Flow: &workflow.Workflow{
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

// newBranchFlow 构造一个「start → sleep → end(output)」的分支子流程。
func newBranchFlow(id string, sleep time.Duration, output any, outputType workflow.DataType) *workflow.Workflow {
	return &workflow.Workflow{
		ID: workflow.ID(id),
		Nodes: workflow.Nodes{
			&Start{BaseNode{ID: "start", Next: "sleep"}},
			&Sleep{BaseNode: BaseNode{ID: "sleep", Next: "end"}, Duration: sleep},
			&End{BaseNode: BaseNode{ID: "end", OutputType: workflow.Property{DataType: outputType}, Output: output}},
		},
	}
}

// TestParallel_Execute_EmptyJoinIsAllBackground：无 joinBranches 的 parallel
// 全部后台执行、立即返回 {}（Python _split_branches 语义，issue #7）。
func TestParallel_Execute_EmptyJoinIsAllBackground(t *testing.T) {
	p := &Parallel{
		BaseNode: BaseNode{ID: "parallel", Next: "end"},
		Branches: []*ParallelBranch{
			{Branch: Branch{Name: "a"}, Flow: newBranchFlow("flow-a", 300*time.Millisecond, "a-done", workflow.String)},
			{Branch: Branch{Name: "b"}, Flow: newBranchFlow("flow-b", 300*time.Millisecond, "b-done", workflow.String)},
		},
	}
	start := time.Now()
	got, err := p.Execute(runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{"x": 5},
	}))
	require.NoError(t, err)
	require.Empty(t, got.Result, "no join branches: result must be an empty map")
	require.Less(t, time.Since(start), 200*time.Millisecond, "parallel must not block on background branches")
}

// TestParallel_BackgroundBranchesExecute：后台分支确实被执行，且未显式声明
// input 的分支继承父 $INPUT（issue #7 验收：后台分支 $INPUT.x 可见）。
func TestParallel_BackgroundBranchesExecute(t *testing.T) {
	p := &Parallel{
		BaseNode: BaseNode{ID: "parallel", Next: "end"},
		Branches: []*ParallelBranch{
			// 无显式 input → 继承父 $INPUT
			{Branch: Branch{Name: "bg1"}, Flow: newBranchFlow("flow-bg1", 10*time.Millisecond, "$INPUT.x", workflow.Integer)},
			// 显式 input 按表达式求值，不受影响
			{
				Branch: Branch{Name: "bg2"},
				Flow:   newBranchFlow("flow-bg2", 10*time.Millisecond, "$INPUT.a", workflow.Integer),
				Input:  map[string]any{"a": "$INPUT.x"},
			},
		},
	}
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{"x": 5},
	})
	future := p.backgroundExecBranches(ctx, p.Branches)

	require.Eventually(t, func() bool {
		future.mutex.Lock()
		defer future.mutex.Unlock()
		return len(future.results) == 2 && future.results["bg1"] == 5 && future.results["bg2"] == 5
	}, 2*time.Second, 10*time.Millisecond)
}

// TestParallel_Execute_JoinFailureSentinel：join 分支失败写
// {"__parallel_error__", "__branch__"} 哨兵并继续汇总，节点本身不失败
// （Python _process_future_result 契约，issue #7）。
func TestParallel_Execute_JoinFailureSentinel(t *testing.T) {
	errorFlow := &workflow.Workflow{
		ID: "flow-error",
		Nodes: workflow.Nodes{
			&Start{BaseNode{ID: "start", Next: "end"}},
			&End{
				BaseNode:   BaseNode{ID: "end"},
				ResultType: ResultTypeError,
				Error:      &Error{Code: "400", Message: "boom"},
			},
		},
	}
	p := &Parallel{
		BaseNode: BaseNode{ID: "parallel", Next: "end"},
		Branches: []*ParallelBranch{
			{Branch: Branch{Name: "a"}, Flow: errorFlow},
			{Branch: Branch{Name: "b"}, Flow: newBranchFlow("flow-b", 0, "ok", workflow.String)},
		},
		JoinBranches: []string{"a", "b"},
	}
	got, err := p.Execute(runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{"x": 5},
	}))
	require.NoError(t, err, "failed join branch must not fail the node")
	result, ok := got.Result.(map[string]any)
	require.True(t, ok)
	require.Equal(t, "ok", result["b"])
	sentinel, ok := result["a"].(map[string]any)
	require.True(t, ok, "failed branch slot must carry the sentinel map")
	require.Equal(t, "a", sentinel["__branch__"])
	require.Contains(t, sentinel["__parallel_error__"], "boom")
}

// TestParallel_Execute_SharedTimeoutNotMutated：parallel 不改写共享子
// Workflow 的 Timeout——超时兜底只落在每次执行的副本上（issue #17）；副本
// 同时携带子流程的全局变量快照（$GLOBAL 在分支内可见）。
func TestParallel_Execute_SharedTimeoutNotMutated(t *testing.T) {
	p := &Parallel{
		BaseNode: BaseNode{ID: "parallel", Next: "end", Timeout: duration.MustParseDuration("PT1S")},
		Branches: []*ParallelBranch{
			{
				Branch: Branch{Name: "a"},
				Flow:   newBranchFlow("flow-a", 0, "$GLOBAL.gk", workflow.String),
			},
			{Branch: Branch{Name: "b"}, Flow: newBranchFlow("flow-b", 0, "done", workflow.String)},
		},
		JoinBranches: []string{"a"},
	}
	p.Branches[0].Flow.SetGlobalVariables(map[string]any{"gk": "gv"})

	got, err := p.Execute(runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{"x": 5},
	}))
	require.NoError(t, err)
	require.Equal(t, map[string]any{"a": "gv"}, got.Result, "branch flow globals must be visible in the branch view")
	for _, br := range p.Branches {
		require.True(t, br.Flow.Timeout.IsZero(), "shared child flow Timeout must stay untouched")
	}
}

// TestParallel_Execute_ConcurrentRaceSafety：同一共享 Parallel 节点（含共享子
// Workflow）被并发 Execute——子流程 Timeout 兜底不写共享对象，-race 下无
// 数据竞争、结果稳定（issue #17）。
func TestParallel_Execute_ConcurrentRaceSafety(t *testing.T) {
	p := &Parallel{
		BaseNode: BaseNode{ID: "parallel", Next: "end", Timeout: duration.MustParseDuration("PT5S")},
		Branches: []*ParallelBranch{
			{Branch: Branch{Name: "a"}, Flow: newBranchFlow("flow-a", time.Millisecond, "$INPUT.x", workflow.Integer)},
			{Branch: Branch{Name: "b"}, Flow: newBranchFlow("flow-b", time.Millisecond, "done", workflow.String)},
		},
		JoinBranches: []string{"a", "b"},
	}
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{"x": 5},
	})
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := p.Execute(ctx)
			require.NoError(t, err)
			require.Equal(t, map[string]any{"a": 5, "b": "done"}, got.Result)
		}()
	}
	wg.Wait()
	for _, br := range p.Branches {
		require.True(t, br.Flow.Timeout.IsZero(), "shared child flow Timeout must stay untouched")
	}
}
