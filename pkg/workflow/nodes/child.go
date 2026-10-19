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

	if err = c.ChildFlow.InputType.ValidateValue(input); err != nil {
		return nil, fmt.Errorf("failed to validate input for children workflow (%q): %w", c.ChildFlow.ID, err)
	}

	childCtx := runtime.WithWorkflowRuntimeContext(ctx, &runtime.Context{
		WorkflowInput:         cast.ToStringMap(input),
		ParentWorkflowContext: ctx,
	})
	childFlowResult, err := c.ChildFlow.Execute(childCtx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to exec children workflow (%q) : %w", c.ChildFlow.ID, err)
	}

	if c.OutputType.ValidateValue(childFlowResult) != nil {
		return nil, fmt.Errorf("validate output error for children workflow (%q): %w", c.ChildFlow.ID, err)
	}

	return &workflow.ExecuteResult{
		Result:   childFlowResult,
		NextNode: c.NextNode(),
	}, nil
}
