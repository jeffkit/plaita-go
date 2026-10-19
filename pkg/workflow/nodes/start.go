package nodes

import (
	"context"

	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func init() {
	workflow.RegisterNodeType(
		types.NodeTypeStart,
		func(ctx context.Context) workflow.Node { return new(Start) },
	)
}

// Start node is the entry point of a flow. It does not produce any result.
type Start struct {
	BaseNode
}

// GetType returns the start node type.
func (s *Start) GetType() workflow.NodeType {
	return types.NodeTypeStart
}
