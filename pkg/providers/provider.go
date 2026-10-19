package providers

import (
	"context"
	"errors"

	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// ErrWorkflowNotFound is error for workflow not found
var ErrWorkflowNotFound = errors.New("workflow not found")

// WorkflowProvider is a workflow provider
type WorkflowProvider interface {
	GetWorkflow(ctx context.Context, flowID workflow.ID, version workflow.Version) (*workflow.Workflow, error)
}
