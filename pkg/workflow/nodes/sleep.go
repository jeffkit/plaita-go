package nodes

import (
	"context"
	"time"

	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func init() {
	workflow.RegisterNodeType(
		types.NodeTypeSleep,
		func(ctx context.Context) workflow.Node { return new(Sleep) },
	)
}

// Sleep node is used to pause the execution of the flow for a specified duration.
type Sleep struct {
	BaseNode
	Duration time.Duration `json:"duration"`
}

// GetType returns the sleep node type.
func (s *Sleep) GetType() workflow.NodeType {
	return types.NodeTypeSleep
}

// Execute executes the sleep node.
func (s *Sleep) Execute(_ context.Context) (*workflow.ExecuteResult, error) {
	time.Sleep(s.Duration)
	return &workflow.ExecuteResult{NextNode: s.NextNode()}, nil
}
