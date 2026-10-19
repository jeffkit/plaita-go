package filesystem

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	_ "github.com/jeffkit/plaita-go/pkg/workflow/nodes"
)

func TestFileSystemProvider(t *testing.T) {
	p, err := NewWorkflowProvider(WithWorkflowDirPath("./testdata"))
	require.NoError(t, err)
	require.NotNil(t, p)

	addWorkflow, err := p.GetWorkflow(context.Background(), "addition-flow", "v1.0.0")
	require.NoError(t, err)
	require.NotNil(t, addWorkflow)

	subtractWorkflow, err := p.GetWorkflow(context.Background(), "subtract-flow", "v1.0.0")
	require.NoError(t, err)
	require.NotNil(t, subtractWorkflow)
}
