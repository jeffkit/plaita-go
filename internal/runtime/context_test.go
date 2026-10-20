package runtime

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

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

	sourceCtx := WithWorkflowRuntimeContext(context.Background(), runtimeCtx)
	gotCtx := CloneContext(sourceCtx)
	require.NotNil(t, gotCtx)

	gotRuntimeCtx := WorkflowRuntimeContext(gotCtx)
	// $PARENT of a cloned branch points at the SOURCE execution (the flow
	// containing the parallel node), not at the source's own parent.
	require.Equal(t, sourceCtx, gotRuntimeCtx.ParentWorkflowContext)
	// deep-copied per-execution state (parent linkage excluded, asserted above)
	gotRuntimeCtx.ParentWorkflowContext = nil
	gotRuntimeCtx.FlowDeadline = time.Time{}
	require.Equal(t, runtimeCtx, gotRuntimeCtx)
}

func TestWorkflowRuntimeContext_NilCtx(t *testing.T) {
	// A nil context (e.g. $PARENT of a root flow) must yield an empty runtime
	// context instead of panicking.
	require.NotNil(t, WorkflowRuntimeContext(nil))
	require.NotPanics(t, func() {
		_ = WorkflowRuntimeContext(nil).GetGlobalVariable("x")
	})

	runtimeCtx := NewContext()
	runtimeCtx.WithGlobalVariable("g", 5)
	ctx := WithWorkflowRuntimeContext(context.Background(), runtimeCtx)
	got, ok := LookupWorkflowRuntimeContext(ctx)
	require.True(t, ok)
	require.Equal(t, 5, got.GetGlobalVariable("g"))

	_, ok = LookupWorkflowRuntimeContext(context.Background())
	require.False(t, ok)
	_, ok = LookupWorkflowRuntimeContext(nil)
	require.False(t, ok)
}

func TestWithEnvVariablesContext_Allowlist(t *testing.T) {
	t.Setenv("PLAITA_TEST_ALLOWED", "v1=with=equals")
	t.Setenv("PLAITA_TEST_SECRET", "should-not-leak")

	c := NewContext()
	c.WithEnvVariablesContext("PLAITA_TEST_ALLOWED")
	require.Equal(t, "v1=with=equals", c.EnvVariables["PLAITA_TEST_ALLOWED"],
		"value keeps everything after the first '='")
	require.NotContains(t, c.EnvVariables, "PLAITA_TEST_SECRET")
	// allowlist misses (typos) are silently dropped, not errors.
	c = NewContext()
	c.WithEnvVariablesContext("PLAITA_TEST_MISSING")
	require.Empty(t, c.EnvVariables)
	// an explicitly empty env exposes nothing (fail-closed).
	c = NewContext()
	c.WithEnvVariables(nil)
	require.Empty(t, c.EnvVariables)
}

func TestNewExecutionRuntimeContext_ChildLinkage(t *testing.T) {
	// parent execution: globals + env live here
	parentRuntime := NewContext()
	parentRuntime.WithGlobalVariable("dry_run", true)
	parentRuntime.WithEnvVariables(map[string]any{"HOME": "/Users/x"})
	parentCtx := WithWorkflowRuntimeContext(context.Background(), parentRuntime)
	parentCtx = WithWorkflowInput(parentCtx, map[string]any{"x": 5})

	// child seed as built by the child/loop nodes
	seed := WithWorkflowRuntimeContext(parentCtx, &Context{
		WorkflowInput:         map[string]any{"item": 1},
		ParentWorkflowContext: parentCtx,
	})

	fresh := NewExecutionRuntimeContext(seed)
	// $PARENT resolves to the parent EXECUTION context (not the seed): the
	// child's $PARENT.INPUT.x must see the parent input.
	parentRuntimeCtx := WorkflowRuntimeContext(fresh.ParentWorkflowContext)
	require.Equal(t, map[string]any{"x": 5}, parentRuntimeCtx.WorkflowInput)
	// $GLOBAL inherited from the parent execution.
	require.Equal(t, true, fresh.GetGlobalVariable("dry_run"))
	// per-execution state starts clean ($INPUT is set by Workflow.Execute
	// from the call args, not from the seed).
	require.Empty(t, fresh.WorkflowInput)
	require.Empty(t, fresh.ExecuteNodeResults)
}

func TestNewExecutionRuntimeContext_Root(t *testing.T) {
	fresh := NewExecutionRuntimeContext(context.Background())
	require.Nil(t, fresh.ParentWorkflowContext)
	require.Empty(t, fresh.GlobalVariables)
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

func TestContext_LastBranch(t *testing.T) {
	c := NewContext()
	// plaita: last_branch is None until a branching node has run.
	require.Nil(t, c.GetLastBranch())
	c.WithLastBranch("e1")
	require.Equal(t, "e1", c.GetLastBranch())
	c.WithLastBranch(nil)
	require.Nil(t, c.GetLastBranch())
	// clones carry the branch (parallel branch snapshot semantics).
	c.WithLastBranch("yes")
	clone := WorkflowRuntimeContext(CloneContext(
		WithWorkflowRuntimeContext(context.Background(), c)))
	require.Equal(t, "yes", clone.GetLastBranch())
}

// TestContext_ConcurrentReadWrite is the -race harness for issue #17: a
// parent flow keeps writing node results/globals/current-node state while
// background parallel branches snapshot and clone the same context (the
// pre-lock failure was a parent write racing a branch's $PARENT evaluation).
func TestContext_ConcurrentReadWrite(t *testing.T) {
	ctx := NewContext()
	ctx.WithGlobalVariable("seed", 0)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				ctx.WithExecuteNodeResult(types.NodeID(fmt.Sprintf("n%d-%d", i, j)), j)
				ctx.WithExecuteNodeError(types.NodeID(fmt.Sprintf("e%d-%d", i, j)), "x")
				ctx.WithCurrentNodeContext(fmt.Sprintf("c%d-%d", i, j), map[string]any{"v": j})
				ctx.WithGlobalVariable(fmt.Sprintf("g%d-%d", i, j), j)
				ctx.WithLastNodeID(types.NodeID(fmt.Sprintf("n%d-%d", i, j)))
				ctx.WithLastBranch(fmt.Sprintf("b%d-%d", i, j))
				ctx.ResetCurrentNodeContext()
			}
		}(i)
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				_ = ctx.GetExecuteNodeResult("n0-1")
				_ = ctx.GetExecuteNodeError("e0-1")
				_ = ctx.GetCurrentNodeContext("c0-1")
				_ = ctx.GetGlobalVariable("g0-1")
				_ = ctx.GetLastNodeID()
				_ = ctx.GetLastBranch()
				_ = ctx.GetExecuteNodeResults()
				_ = ctx.GetExecuteNodeErrors()
				_ = ctx.GetCurrentNodeContexts()
				_ = ctx.GetGlobalVariables()
				_ = ctx.GetEnvVariables()
				_ = ctx.GetWorkflowInput()
			}
		}()
	}
	// background branch clones race the writers too (CloneContext holds the
	// source read lock across its deep copy).
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			parent := WithWorkflowRuntimeContext(context.Background(), ctx)
			for j := 0; j < 100; j++ {
				clone := WorkflowRuntimeContext(CloneContext(parent))
				_ = clone.GetExecuteNodeResults()
			}
		}()
	}
	wg.Wait()
}
