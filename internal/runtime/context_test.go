package runtime

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jeffkit/plaita-go/internal/types"
)

func TestCloneContext(t *testing.T) {
	runtimeCtx := &Context{
		WorkflowInput:         map[string]any{"a": 1, "b": map[string]any{"c": "d"}},
		ExecuteNodeResults:    map[string]any{"node1": 1},
		ParentWorkflowContext: nil,
		CurrentNodeContext:    map[string]any{"node1": map[string]any{"a": 1}},
		LastNodeID:            "prev-node",
		ExecuteNodeErrors:     map[string]any{},
		EnvVariables:          map[string]any{},
		GlobalVariables:       map[string]any{},
	}

	gotCtx := CloneContext(WithWorkflowRuntimeContext(context.Background(), runtimeCtx))
	require.NotNil(t, gotCtx)

	gotRuntimeCtx := WorkflowRuntimeContext(gotCtx)
	require.Equal(t, runtimeCtx, gotRuntimeCtx)
}

func TestContext_WithExecuteNodeError(t *testing.T) {
	type args struct {
		nodeID types.NodeID
		error  any
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "test with node error",
			args: args{
				nodeID: "a",
				error:  "param invalid",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Context{
				ExecuteNodeErrors: make(map[string]any),
			}
			c.WithExecuteNodeError(tt.args.nodeID, tt.args.error)
			got := c.GetExecuteNodeError(tt.args.nodeID)
			if got != tt.args.error {
				t.Errorf("WithExecuteNodeError() = %v, want %v", got, tt.args.nodeID)
			}
		})
	}
}
