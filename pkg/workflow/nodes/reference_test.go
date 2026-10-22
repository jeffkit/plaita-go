package nodes

import (
	"context"
	"strings"
	"testing"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/providers/memory"
	"github.com/jeffkit/plaita-go/pkg/workflow"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func intEndFlow(id workflow.ID, output any) *workflow.Workflow {
	return &workflow.Workflow{
		ID:      id,
		Version: "v1.0",
		Nodes: []workflow.Node{
			&Start{BaseNode: BaseNode{ID: "start", Next: "end"}},
			&End{
				BaseNode: BaseNode{
					ID:     "end",
					Output: output,
				},
			},
		},
	}
}

// plaita 实测语义（r78 探针，golden 钉）：input 求值后注入、缺省继承调用方
// $INPUT、$PARENT 隔离（子流程读不到父执行数据）、$GLOBAL 继承。
func TestReference_ExecutePlaitaSemantics(t *testing.T) {
	newRef := func(input any, child *workflow.Workflow) *Reference {
		return &Reference{
			BaseNode:  BaseNode{ID: "r", Next: "after"},
			Input:     input,
			ChildFlow: child,
		}
	}

	t.Run("input dict is evaluated element-wise", func(t *testing.T) {
		child := intEndFlow("sub", map[string]any{"echo": "$INPUT.prefix"})
		ref := newRef(map[string]any{"prefix": "$INPUT.name"}, child)
		parentCtx := runtime.WithWorkflowRuntimeContext(context.Background(),
			&runtime.Context{WorkflowInput: map[string]any{"name": "kong"}})
		got, err := ref.Execute(parentCtx)
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"echo": "kong"}, got.Result)
		assert.Equal(t, "after", string(got.NextNode))
	})

	t.Run("absent input inherits the caller input", func(t *testing.T) {
		child := intEndFlow("sub", "$INPUT")
		ref := newRef(nil, child)
		parentCtx := runtime.WithWorkflowRuntimeContext(context.Background(),
			&runtime.Context{WorkflowInput: map[string]any{"z": int64(42)}})
		got, err := ref.Execute(parentCtx)
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"z": int64(42)}, got.Result)
	})

	t.Run("parent execution data is invisible across the boundary", func(t *testing.T) {
		// child.py：引用逻辑不能使用父逻辑上下文的数据（实测 $PARENT.x → null）。
		child := intEndFlow("sub", map[string]any{"px": "$PARENT.x", "pin": "$PARENT.n"})
		ref := newRef(map[string]any{}, child)
		parentRC := &runtime.Context{WorkflowInput: map[string]any{"x": "top-x"}}
		parentRC.WithCurrentNodeContext("n", "parent-node-result")
		parentCtx := runtime.WithWorkflowRuntimeContext(context.Background(), parentRC)
		got, err := ref.Execute(parentCtx)
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"px": nil, "pin": nil}, got.Result)
	})

	t.Run("globals are inherited", func(t *testing.T) {
		child := intEndFlow("sub", "$GLOBAL.token")
		ref := newRef(map[string]any{}, child)
		parentRC := &runtime.Context{}
		parentRC.WithGlobalVariables(map[string]any{"token": "top-token"})
		parentCtx := runtime.WithWorkflowRuntimeContext(context.Background(), parentRC)
		got, err := ref.Execute(parentCtx)
		require.NoError(t, err)
		assert.Equal(t, "top-token", got.Result)
	})
}

// 裸引擎路径：执行期 WorkflowProvider 兜底解析（IR 路径在加载期注入，不走
// 这里）。
func TestReference_ExecuteWithProvider(t *testing.T) {
	provider := memory.FromWorkflows(intEndFlow("test-flow", "$INPUT.number"))

	ref := &Reference{
		BaseNode:          BaseNode{ID: "reference", Next: "end"},
		Input:             map[string]any{"number": int64(1)},
		FlowID:            "test-flow",
		FlowVersion:       "v1.0",
		workflowsProvider: provider,
	}
	got, err := ref.Execute(runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{}))
	require.NoError(t, err)
	assert.EqualValues(t, 1, got.Result)

	_, err = (&Reference{
		BaseNode:          BaseNode{Next: "end"},
		FlowID:            "test-flow",
		FlowVersion:       "v2.0",
		workflowsProvider: provider,
	}).Execute(runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{}))
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "test-flow"), err.Error())
}

// 无 childFlow 且无 provider：具名错误（plaita 的「调度器尚未注入」形态）。
func TestReference_ExecuteWithoutChildFlowOrProvider(t *testing.T) {
	ref := &Reference{BaseNode: BaseNode{ID: "r", Next: "end"}, FlowID: "ghost"}
	_, err := ref.Execute(runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "childFlow is missing")
}

// 子流程错误直接冒泡。
func TestReference_ErrorPropagation(t *testing.T) {
	child := &workflow.Workflow{
		ID: "boom",
		Nodes: []workflow.Node{
			&Start{BaseNode: BaseNode{ID: "start", Next: "end"}},
			&End{BaseNode: BaseNode{ID: "end"}, ResultType: ResultTypeError,
				Error: &Error{Code: "501", Message: "sub-boom"}},
		},
	}
	ref := &Reference{BaseNode: BaseNode{ID: "r", Next: "after"}, ChildFlow: child}
	_, err := ref.Execute(runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sub-boom")
}
