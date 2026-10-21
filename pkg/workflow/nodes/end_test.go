package nodes

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jeffkit/plaita-go/internal/runtime"

	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func TestEnd_Execute(t *testing.T) {
	tests := []struct {
		name       string
		runtimeCtx *runtime.Context
		endNode    *End
		want       *workflow.ExecuteResult
		wantErr    bool
	}{
		{
			name: "test simple end node",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"a": 1,
				},
			},
			endNode: &End{
				BaseNode: BaseNode{
					ID: "end",
					Output: map[string]any{
						"result": "$INPUT.a",
					},
					OutputType: workflow.Property{
						DataType: workflow.Object,
						Properties: map[string]workflow.Property{
							"result": {
								DataType: workflow.Integer,
							},
						},
					},
				},
			},
			want:    &workflow.ExecuteResult{Result: map[string]any{"result": 1}},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.endNode.Execute(runtime.WithWorkflowRuntimeContext(context.TODO(), tt.runtimeCtx))
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

// TestEnd_Execute_ErrorResultType：resultType=error 抛 FlowResultError（带
// code/message），且不求值 output（Python end.py error 分支直接 raise，#5）。
func TestEnd_Execute_ErrorResultType(t *testing.T) {
	end := &End{
		BaseNode: BaseNode{
			ID: "end",
			// 若被求值，int 输出过不了 String 校验——error 分支必须完全
			// 不碰 output。
			Output:     12345,
			OutputType: workflow.Property{DataType: workflow.String},
		},
		ResultType: ResultTypeError,
		Error:      &Error{Code: "400", Message: "boom"},
	}
	got, err := end.Execute(runtime.WithWorkflowRuntimeContext(context.TODO(), &runtime.Context{}))
	require.Nil(t, got)
	require.Error(t, err)

	var flowErr *FlowResultError
	require.ErrorAs(t, err, &flowErr)
	require.Equal(t, "400", flowErr.Code)
	require.Equal(t, "boom", flowErr.Message)
	require.Contains(t, err.Error(), "boom")
	require.Contains(t, err.Error(), "400")
}

// TestEnd_Execute_ErrorResultTypeWithoutErrorInfo：error end 未声明 error
// 信息时仍抛错（空 code/message），不静默成功。
func TestEnd_Execute_ErrorResultTypeWithoutErrorInfo(t *testing.T) {
	end := &End{BaseNode: BaseNode{ID: "end"}, ResultType: ResultTypeError}
	got, err := end.Execute(runtime.WithWorkflowRuntimeContext(context.TODO(), &runtime.Context{}))
	require.Nil(t, got)
	var flowErr *FlowResultError
	require.ErrorAs(t, err, &flowErr)
	require.Empty(t, flowErr.Code)
	require.Empty(t, flowErr.Message)
}

// TestEnd_Execute_NoopResultType：nop 静默结束、结果为 nil（IR 层拒载，
// 直连引擎时保留 Python 语义）。
func TestEnd_Execute_NoopResultType(t *testing.T) {
	end := &End{
		BaseNode:   BaseNode{ID: "end", Output: "ignored"},
		ResultType: ResultTypeNoop,
	}
	got, err := end.Execute(runtime.WithWorkflowRuntimeContext(context.TODO(), &runtime.Context{}))
	require.NoError(t, err)
	require.Nil(t, got.Result)
}

// TestEnd_ErrorResultType_FlowLevel：resultType=error 的 flow 返回非 nil
// error，信息含 boom 与 400（issue #5 验收）。
func TestEnd_ErrorResultType_FlowLevel(t *testing.T) {
	flow := &workflow.Workflow{
		ID:      "error-end-flow",
		Runtime: workflow.RuntimeGo,
		Version: "v1.0",
		Nodes: workflow.Nodes{
			&Start{BaseNode{ID: "start", Next: "end"}},
			&End{
				BaseNode:   BaseNode{ID: "end", Output: "x"},
				ResultType: ResultTypeError,
				Error:      &Error{Code: "400", Message: "boom"},
			},
		},
	}
	result, err := flow.Execute(context.Background(), nil)
	require.Nil(t, result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "boom")
	require.Contains(t, err.Error(), "400")
}

// TestEnd_ErrorResultType_PropagatesThroughChild：error end 在 child 内向上
// 传播为子节点错误（issue #5 验收；parallel 内则按 #7 变哨兵）。
func TestEnd_ErrorResultType_PropagatesThroughChild(t *testing.T) {
	errorEnd := &End{
		BaseNode:   BaseNode{ID: "end", Output: "x"},
		ResultType: ResultTypeError,
		Error:      &Error{Code: "400", Message: "boom"},
	}
	childFlow := &workflow.Workflow{
		ID:      "child-flow",
		Runtime: workflow.RuntimeGo,
		Version: "v1.0",
		Nodes: workflow.Nodes{
			&Start{BaseNode{ID: "start", Next: "end"}},
			errorEnd,
		},
	}
	flow := &workflow.Workflow{
		ID:      "parent-flow",
		Runtime: workflow.RuntimeGo,
		Version: "v1.0",
		Nodes: workflow.Nodes{
			&Start{BaseNode{ID: "start", Next: "child"}},
			&Child{BaseNode: BaseNode{ID: "child", Next: "parent-end"}, ChildFlow: childFlow},
			&End{BaseNode: BaseNode{ID: "parent-end", Output: "done"}},
		},
	}
	result, err := flow.Execute(context.Background(), nil)
	require.Nil(t, result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "boom")
}

// TestEndErrorResultNotRetryable (issue #43): resultType=error is the flow's
// decision, not a transient fault — plaita converts FlowResultError to
// ErrorResultException inside the `except FlowResultError` clause, which
// escapes run_node's retry loop entirely. The construction point therefore
// wraps the error as NonRetryable; text and the *FlowResultError contract
// (errors.As through the wrapper) stay intact.
func TestEndErrorResultNotRetryable(t *testing.T) {
	end := &End{
		BaseNode:   BaseNode{ID: "end"},
		ResultType: ResultTypeError,
		Error:      &Error{Code: "400", Message: "boom"},
	}
	_, err := end.Execute(runtime.WithWorkflowRuntimeContext(context.TODO(), &runtime.Context{}))
	require.Error(t, err)
	if workflow.IsRetryableError(err) {
		t.Fatalf("end error result must be deterministic, got retryable: %v", err)
	}
	var flowErr *FlowResultError
	require.ErrorAs(t, err, &flowErr, "FlowResultError contract must survive the wrapper")
	require.Equal(t, "400", flowErr.Code)
	require.Equal(t, "boom", flowErr.Message)
	require.Contains(t, err.Error(), "FlowResultError: code=400, message=boom")
}
