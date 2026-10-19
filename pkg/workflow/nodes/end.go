package nodes

import (
	"context"
	"fmt"

	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func init() {
	workflow.RegisterNodeType(
		types.NodeTypeEnd,
		func(ctx context.Context) workflow.Node { return new(End) },
	)
}

// Error represents error information.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ResultType represents the type of the result.
type ResultType string

// ResultType enums
const (
	ResultTypeSuccess ResultType = "success"
	ResultTypeError   ResultType = "error"
	ResultTypeNoop    ResultType = "nop"
)

// End node is the exit point of a flow. It produces a result.
type End struct {
	BaseNode
	ResultType ResultType `json:"resultType"`
	Error      *Error     `json:"error,omitempty"`
}

// GetType returns the end node type.
func (e *End) GetType() workflow.NodeType {
	return types.NodeTypeEnd
}

// Execute executes the end node.
func (e *End) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	evaluatedOutput, err := expression.Eval(ctx, e.Output)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate end node output: %v", err)
	}

	// Validate the evaluated output using the OutputType Property
	if err := e.OutputType.ValidateValue(evaluatedOutput); err != nil {
		return nil, fmt.Errorf("failed to validate end node output: %v", err)
	}
	return &workflow.ExecuteResult{Result: evaluatedOutput}, nil
}
