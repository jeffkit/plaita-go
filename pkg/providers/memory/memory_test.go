package memory

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func TestMemoryProvider(t *testing.T) {
	p := FromWorkflows([]*workflow.Workflow{
		{ID: "test1", Version: "v1"},
		{ID: "test2", Version: "v2"},
	}...)
	require.NotNil(t, p)

	wf1, err := p.GetWorkflow(context.TODO(), "test1", "v1")
	require.NoError(t, err)
	require.NotNil(t, wf1)

	wf2, err := p.GetWorkflow(context.TODO(), "test2", "v2")
	require.NoError(t, err)
	require.NotNil(t, wf2)
}
