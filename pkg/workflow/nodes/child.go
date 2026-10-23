package nodes

import (
	"context"
	"fmt"

	validation "github.com/go-ozzo/ozzo-validation"
	"github.com/spf13/cast"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func init() {
	workflow.RegisterNodeType(
		types.NodeTypeChild,
		func(ctx context.Context) workflow.Node { return new(Child) },
	)
}

// Child node is used to execute a children workflow.
type Child struct {
	BaseNode
	ChildFlow *workflow.Workflow `json:"childFlow"`
	Input     any                `json:"input,omitempty"`
}

// Validate validates the children workflow node.
func (c *Child) Validate() error {
	return validation.ValidateStruct(c,
		validation.Field(&c.ChildFlow, validation.Required))
}

// GetType returns the children flow node type.
func (c *Child) GetType() workflow.NodeType {
	return types.NodeTypeChild
}

// Execute executes child flow node.
func (c *Child) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	input, err := expression.Eval(ctx, c.Input)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate input for child node: %w", err)
	}
	if input == nil {
		// plaita 实测（r78）：input 缺省/求值为 None 时子流程继承调用方
		// $INPUT（evaluate(None) → None，子执行 params=None 走继承路径）——
		// child 与 reference 同款语义。
		input = runtime.WorkflowRuntimeContext(ctx).GetWorkflowInput()
	}

	if err = c.ChildFlow.InputType.ValidateValue(input); err != nil {
		return nil, fmt.Errorf("failed to validate input for children workflow (%q): %w", c.ChildFlow.ID, err)
	}

	childCtx := runtime.WithWorkflowRuntimeContext(ctx, &runtime.Context{
		WorkflowInput:         cast.ToStringMap(input),
		ParentWorkflowContext: ctx,
		// 嵌套挂起 v1：子执行帧标注（作用域闸的链行走依据之一）。
		FrameKind: runtime.FrameKindChildCall,
	})
	childFlowResult, err := c.ChildFlow.Execute(childCtx, input)
	if err != nil {
		// 嵌套挂起 v1 传播转换点（主闸，rs suspension_carrier_or_reject 同款）：
		// 调用方帧 kind 是真实帧（不受 reference ambient 父链遮蔽影响）——
		// 迭代/分支帧上的载体在此具名拒绝并注销已注册订阅；child/reference
		// 调用链放行，载体原样上抛（executeNode 的拦截臂保证不进任何策略链）。
		if susp := workflow.AsSuspend(err); susp != nil {
			if rc := runtime.WorkflowRuntimeContext(ctx); !rc.FrameKind.AllowsSuspension() {
				err = workflow.RejectSuspendedCarrier(ctx, susp, rc.FrameKind)
			}
		}
		return nil, fmt.Errorf("failed to exec children workflow (%q) : %w", c.ChildFlow.ID, err)
	}

	if verr := c.OutputType.ValidateValue(childFlowResult); verr != nil {
		return nil, fmt.Errorf("validate output error for children workflow (%q): %w", c.ChildFlow.ID, verr)
	}

	return &workflow.ExecuteResult{
		Result:   childFlowResult,
		NextNode: c.NextNode(),
	}, nil
}
