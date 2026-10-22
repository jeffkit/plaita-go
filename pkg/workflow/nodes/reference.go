package nodes

import (
	"context"
	"fmt"

	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/providers"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func init() {
	workflow.RegisterNodeType(
		types.NodeTypeReference,
		func(ctx context.Context) workflow.Node { return new(Reference) },
	)
}

// Reference 落地 plaita 的 ReferenceFlow（plaita/node/child.py，引用逻辑）：
// 性质相当于调用另一个独立函数。childFlow 由「调度器」按 flowId/flowVersion
// 注入——IR 部署路径里这一步发生在加载期（pkg/ir 的 FlowProvider 解析后内联，
// 注入的子流程照样过全量静态校验）；裸引擎路径保留执行期 WorkflowProvider
// 兜底。
//
// plaita 实测语义（r78 探针，golden 钉）：
//   - input 求值（dict 逐元素）后作为子流程 $INPUT；缺省/求值为 None → 子流程
//     继承调用方 $INPUT（child 节点同款）；
//   - 子流程继承 $GLOBAL/$ENV 与执行身份（$FLOW_ID/$EXECUTION_ID），但看不到
//     父执行数据：$PARENT.<x> 求值为 null（child.py「引用逻辑不能使用父逻辑
//     上下文的数据」的实测口径——plain-run 路径不给子流程播种父快照）。Go 以
//     ambient-carrier 种子上下文实现：只携带 global/env/身份，不带 input/node；
//   - 子流程错误直接冒泡，节点内零兜底。
type Reference struct {
	BaseNode
	Input       any                `json:"input,omitempty"`
	ChildFlow   *workflow.Workflow `json:"childFlow,omitempty"`
	FlowID      workflow.ID        `json:"flowId,omitempty"`
	FlowVersion workflow.Version   `json:"flowVersion,omitempty"`

	// bare-engine 执行期解析兜底（IR 路径已在加载期注入 ChildFlow，不走这里）。
	workflowsProvider providers.WorkflowProvider
}

// GetType returns the reference node type.
func (r *Reference) GetType() workflow.NodeType {
	return types.NodeTypeReference
}

// Execute resolves the referenced workflow and runs it with the evaluated
// input. plaita 的「调度器尚未注入」形态在这里具名报错（IR 路径不会出现：
// 加载期已完成注入）。
func (r *Reference) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	input, err := expression.Eval(ctx, r.Input)
	if err != nil {
		return nil, fmt.Errorf("reference node %q: failed to evaluate input: %w", r.GetID(), err)
	}
	if input == nil {
		// plaita：evaluate(None) → None，子执行继承调用方 $INPUT（child 同款）。
		input = runtime.WorkflowRuntimeContext(ctx).GetWorkflowInput()
	}

	target := r.ChildFlow
	if target == nil {
		if r.workflowsProvider == nil {
			return nil, fmt.Errorf("reference node %q: childFlow is missing (plaita injects it from flowId/flowVersion via the scheduler; this engine resolves it at load time via ir.LoadWithFlowProvider)", r.GetID())
		}
		target, err = r.workflowsProvider.GetWorkflow(ctx, r.FlowID, r.FlowVersion)
		if err != nil {
			return nil, fmt.Errorf("reference node %q: failed to resolve workflow %s: %w", r.GetID(), r.FlowID, err)
		}
	}

	if err := target.InputType.ValidateValue(input); err != nil {
		return nil, fmt.Errorf("reference node %q: input not match required: %w", r.GetID(), err)
	}

	result, err := target.Execute(r.referenceChildContext(ctx), input)
	if err != nil {
		return nil, fmt.Errorf("reference node %q: failed to execute referenced workflow (%q): %w", r.GetID(), target.ID, err)
	}
	return &workflow.ExecuteResult{Result: result, NextNode: r.NextNode()}, nil
}

// referenceChildContext builds the child execution seed: ambient roots only.
// The carrier pair makes $GLOBAL/$ENV/执行身份 inherit (plaita 实测口径) while
// $PARENT resolves against a context with no input/node data — so $PARENT.<x>
// evaluates to null across the reference boundary, never the parent's data.
func (r *Reference) referenceChildContext(ctx context.Context) context.Context {
	parent := runtime.WorkflowRuntimeContext(ctx)

	ambient := runtime.NewContext()
	ambient.WithGlobalVariables(parent.GetGlobalVariables())
	ambient.WithEnvVariables(parent.GetEnvVariables())
	ambient.FlowDeadline = parent.FlowDeadline
	ambientCtx := runtime.WithWorkflowRuntimeContext(ctx, ambient)

	seed := &runtime.Context{
		ParentWorkflowContext: ambientCtx,
		FlowID:                parent.FlowID,
		ExecutionID:           parent.ExecutionID,
	}
	return runtime.WithWorkflowRuntimeContext(ambientCtx, seed)
}
