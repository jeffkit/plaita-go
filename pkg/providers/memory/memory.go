// Package memory provides in-memory workflow provider
package memory

import (
	"context"
	"errors"

	"github.com/jeffkit/plaita-go/pkg/providers"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

type provider struct {
	workflows map[workflow.ID]map[workflow.Version]workflow.Workflow
}

// NewProvider returns a new in-memory workflow provider.
func NewProvider() providers.WorkflowProvider {
	return &provider{workflows: make(map[workflow.ID]map[workflow.Version]workflow.Workflow)}
}

// FromWorkflows returns a new in-memory workflow provider with the given workflows.
func FromWorkflows(workflows ...*workflow.Workflow) providers.WorkflowProvider {
	p := NewProvider()
	for _, wf := range workflows {
		if err := p.(*provider).RegisterWorkflow(wf); err != nil {
			panic(err)
		}
	}
	return p
}

// GetWorkflow returns a workflow by its ID and version.
func (p *provider) GetWorkflow(
	_ context.Context,
	id workflow.ID,
	version workflow.Version,
) (*workflow.Workflow, error) {
	versionWorkflows, ok := p.workflows[id]
	if !ok {
		return nil, providers.ErrWorkflowNotFound
	}

	wf, ok := versionWorkflows[version]
	if !ok {
		return nil, providers.ErrWorkflowNotFound
	}
	return &wf, nil
}

// RegisterWorkflow registers a workflow.
func (p *provider) RegisterWorkflow(wf *workflow.Workflow) error {
	if wf == nil || len(wf.ID) == 0 || len(wf.Version) == 0 {
		return errors.New("invalid workflow: must set workflow id and version")

	}
	if _, ok := p.workflows[wf.ID]; !ok {
		p.workflows[wf.ID] = make(map[workflow.Version]workflow.Workflow)
	}
	p.workflows[wf.ID][wf.Version] = *wf
	return nil
}
