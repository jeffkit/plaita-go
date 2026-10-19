package nodes

import (
	"context"
	"fmt"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func init() {
	workflow.RegisterNodeType(
		types.NodeTypeAssignment,
		func(ctx context.Context) workflow.Node { return new(Assignment) },
	)
}

// UpstreamValue represents the value of an upstream node.
type UpstreamValue struct {
	Upstream types.NodeID `json:"upstream"`
	Value    any          `json:"value"`
}

// Assignment node assigns a value to a variable.
type Assignment struct {
	BaseNode
	UpstreamOutput []UpstreamValue `json:"upstreamOutput,omitempty"`
}

// GetType returns the assignment node type.
func (a *Assignment) GetType() types.NodeType {
	return types.NodeTypeAssignment
}

func (a *Assignment) upstreamValueByNodeID(nodeID types.NodeID) (any, bool) {
	for _, upstreamValue := range a.UpstreamOutput {
		if upstreamValue.Upstream == nodeID {
			return upstreamValue.Value, true
		}
	}
	return nil, false
}

// Execute executes the `assignment` node logic.
func (a *Assignment) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	value := a.Output
	if len(a.UpstreamOutput) > 0 {
		lastNodeID := runtime.LastNodeID(ctx)
		v, found := a.upstreamValueByNodeID(lastNodeID)
		if !found {
			return nil, fmt.Errorf("upstream value not found for last node id: %s", lastNodeID)
		}
		value = v
	}

	// Evaluate and validate the value according to the OutputType
	evaluatedValue, err := expression.Eval(ctx, value)
	if err != nil {
		return nil, err
	}
	if err := a.OutputType.ValidateValue(evaluatedValue); err != nil {
		return nil, fmt.Errorf("failed to validate value: %w", err)
	}

	return &workflow.ExecuteResult{Result: evaluatedValue, NextNode: a.NextNode()}, nil
}
