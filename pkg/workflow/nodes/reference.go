package nodes

import (
	"context"
	"errors"
	"fmt"
	"github.com/jeffkit/plaita-go/pkg/expression"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/providers"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func init() {
	workflow.RegisterNodeType(
		types.NodeTypeReference,
		func(ctx context.Context) workflow.Node { return new(Reference) },
	)
}

// Reference node is used to reference a flow from another flow.
type Reference struct {
	BaseNode
	FlowID      workflow.ID      `json:"flowId"`
	FlowVersion workflow.Version `json:"flowVersion,omitempty"`
	FlowParams  any              `json:"flowParams,omitempty"`

	workflowsProvider providers.WorkflowProvider
	ChildFlow         *workflow.Workflow `json:"childFlow,omitempty"`
}

// GetType returns the reference node type.
func (r *Reference) GetType() workflow.NodeType {
	return types.NodeTypeReference
}

// Execute executes the reference node.
func (r *Reference) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	if r.workflowsProvider == nil && r.ChildFlow == nil {
		return nil, errors.New("workflowsProvider or childFlow must be set")
	}
	// 如果childFlow有值，则把childFlow的值赋给targetWorkflow
	targetWorkflow := r.ChildFlow
	var err error
	if r.ChildFlow == nil {
		targetWorkflow, err = r.workflowsProvider.GetWorkflow(ctx, r.FlowID, r.FlowVersion)
		if err != nil {
			return nil, fmt.Errorf("failed to get workflow %s: %w", r.FlowID, err)
		}
	}
	flowParams, err := expression.Eval(ctx, r.FlowParams)
	if err != nil {
		return nil, fmt.Errorf("failed to eval flow params: %w", err)
	}
	referenceCtx := runtime.WithWorkflowInput(ctx, flowParams)
	result, err := targetWorkflow.Execute(referenceCtx, flowParams)
	if err != nil {
		return nil, fmt.Errorf("failed to execute workflow %s: %w", r.FlowID, err)
	}

	return &workflow.ExecuteResult{NextNode: r.NextNode(), Result: result}, nil
}
