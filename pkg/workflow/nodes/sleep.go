package nodes

import (
	"context"
	"fmt"
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

// Execute sleeps for the configured duration, waking immediately when the
// context is cancelled (#48): the bare time.Sleep made this node ctx-blind,
// so one oversized Duration pinned the consumer past the execution budget and
// the deadline terminal never became reachable. The budget itself stays the
// occupancy bound — Duration keeps no additional cap, mirroring plaita where
// sleep has no node-level ceiling either (worker-deployed flows cannot carry
// this node at all: the IR rejects "sleep" as not part of the plaita dialect).
func (s *Sleep) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	timer := time.NewTimer(s.Duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return &workflow.ExecuteResult{NextNode: s.NextNode()}, nil
	case <-ctx.Done():
		// Wrap ctx.Err() rather than a bespoke sentinel: context.Canceled is
		// deliberately outside contextTimeoutError (DeadlineExceeded only), so
		// a budget/worker cancellation stays a node failure instead of being
		// absorbed by a continue-with timeout handler, while a node-level
		// timeout (WithTimeout → DeadlineExceeded) keeps its usual policy.
		return nil, fmt.Errorf("sleep node %q cancelled: %w", s.ID, ctx.Err())
	}
}
