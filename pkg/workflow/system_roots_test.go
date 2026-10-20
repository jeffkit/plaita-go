package workflow

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/duration"
	"github.com/jeffkit/plaita-go/pkg/expression"
)

// Workflow-level system roots (#11): $FLOW_ID / $FLOW / $GLOBAL.flow_id /
// $EXECUTION_ID / $LAST_NODE / $BRANCH / $EMPTYSTRING evaluated inside a real
// execution, plus the child-flow rules and the checkpoint round trip.

// exprNode evaluates one plaita expression and returns its value as the node
// result. With typ=end it doubles as the flow's end node, so the flow output
// IS the probed expression value.
type exprNode struct {
	id   string
	typ  NodeType
	next NodeID
	expr string
}

func (n *exprNode) GetID() NodeID                 { return NodeID(n.id) }
func (n *exprNode) GetType() NodeType             { return n.typ }
func (n *exprNode) GetName() string               { return n.id }
func (n *exprNode) GetTimeout() duration.Duration { return duration.Duration{} }
func (n *exprNode) GetErrorHandlePolicy() RecoverableErrorHandlePolicy {
	return RecoverableErrorHandlePolicy{}
}
func (n *exprNode) GetTimeoutHandlePolicy() TimeoutHandlePolicy { return TimeoutHandlePolicy{} }
func (n *exprNode) NextNode() NodeID                            { return n.next }
func (n *exprNode) GetMetadata() map[string]string              { return nil }
func (n *exprNode) MoreInfoAfterExecute(context.Context) map[string]interface{} {
	return map[string]interface{}{}
}
func (n *exprNode) Execute(ctx context.Context) (*ExecuteResult, error) {
	v, err := expression.Eval(ctx, n.expr)
	if err != nil {
		return nil, err
	}
	return &ExecuteResult{Result: v, NextNode: n.next}, nil
}

// spawningNode executes a child flow exactly the way nodes/child.go does
// (raw seed context chaining $PARENT at the spawning execution).
type spawningNode struct {
	id    string
	child *Workflow
}

func (n *spawningNode) GetID() NodeID                 { return NodeID(n.id) }
func (n *spawningNode) GetType() NodeType             { return types.NodeTypeChild }
func (n *spawningNode) GetName() string               { return n.id }
func (n *spawningNode) GetTimeout() duration.Duration { return duration.Duration{} }
func (n *spawningNode) GetErrorHandlePolicy() RecoverableErrorHandlePolicy {
	return RecoverableErrorHandlePolicy{}
}
func (n *spawningNode) GetTimeoutHandlePolicy() TimeoutHandlePolicy { return TimeoutHandlePolicy{} }
func (n *spawningNode) NextNode() NodeID                            { return "probe" }
func (n *spawningNode) GetMetadata() map[string]string              { return nil }
func (n *spawningNode) MoreInfoAfterExecute(context.Context) map[string]interface{} {
	return map[string]interface{}{}
}
func (n *spawningNode) Execute(ctx context.Context) (*ExecuteResult, error) {
	childCtx := runtime.WithWorkflowRuntimeContext(ctx, &runtime.Context{
		ParentWorkflowContext: ctx,
	})
	out, err := n.child.Execute(childCtx, map[string]any{})
	if err != nil {
		return nil, err
	}
	return &ExecuteResult{Result: out, NextNode: n.NextNode()}, nil
}

// probeFlow builds start → chain → probe(end) where the end node evaluates
// the probe expression; the chain's fakeNodes are wired in order.
func probeFlow(flowID string, chain []Node, probeExpr string) *Workflow {
	nodes := Nodes{&fakeNode{id: "s", typ: types.NodeTypeStart, next: "probe"}}
	nodes = append(nodes, chain...)
	nodes = append(nodes, &exprNode{id: "probe", typ: types.NodeTypeEnd, expr: probeExpr})
	for i, n := range nodes {
		if i+1 < len(nodes) {
			if f, ok := n.(*fakeNode); ok {
				f.next = nodes[i+1].GetID()
			}
		}
	}
	return &Workflow{ID: ID(flowID), Runtime: RuntimeGo, Nodes: nodes}
}

func TestExecuteSystemRoots(t *testing.T) {
	t.Run("$FLOW_ID / $FLOW / $GLOBAL.flow_id", func(t *testing.T) {
		for _, expr := range []string{"$FLOW_ID", "$FLOW", "$GLOBAL.flow_id"} {
			result, err := probeFlow("probe-flow", nil, expr).Execute(context.Background(), nil)
			require.NoError(t, err, expr)
			assert.Equal(t, "probe-flow", result, expr)
		}
	})

	t.Run("$EXECUTION_ID: host-injected id wins, bare engine mints a uuid", func(t *testing.T) {
		ctx := runtime.WithExecutionID(context.Background(), "exec-fixed")
		result, err := probeFlow("probe-flow", nil, "$EXECUTION_ID").Execute(ctx, nil)
		require.NoError(t, err)
		assert.Equal(t, "exec-fixed", result)

		result, err = probeFlow("probe-flow", nil, "$EXECUTION_ID").Execute(context.Background(), nil)
		require.NoError(t, err)
		if id, ok := result.(string); !ok || len(id) != 36 {
			t.Fatalf("$EXECUTION_ID = %v, want a dashed uuid4", result)
		}
	})

	t.Run("$EMPTYSTRING evaluates to the empty string", func(t *testing.T) {
		result, err := probeFlow("probe-flow", nil, "$EMPTYSTRING").Execute(context.Background(), nil)
		require.NoError(t, err)
		assert.Empty(t, result)
	})

	t.Run("$LAST_NODE is the last completed node", func(t *testing.T) {
		chain := []Node{&fakeNode{id: "n1", typ: types.NodeTypeAssignment}}
		result, err := probeFlow("probe-flow", chain, "$LAST_NODE").Execute(context.Background(), nil)
		require.NoError(t, err)
		assert.Equal(t, "n1", result)
	})

	t.Run("$BRANCH mirrors the switch target (plaita last_branch)", func(t *testing.T) {
		chain := []Node{&fakeNode{id: "sw", typ: types.NodeTypeSwitch}}
		// the fake switch's result is the string "ok" — the branch target.
		result, err := probeFlow("probe-flow", chain, "$BRANCH").Execute(context.Background(), nil)
		require.NoError(t, err)
		assert.Equal(t, "ok", result)
	})

	t.Run("child flow: own $FLOW_ID, shared $EXECUTION_ID (child-node seed pattern)", func(t *testing.T) {
		cases := []struct {
			expr string
			want any
		}{
			{"$FLOW_ID", "child-flow"},
			{"$FLOW", "child-flow"},
			{"$GLOBAL.flow_id", "child-flow"},
			{"$EXECUTION_ID", "exec-root"},
		}
		for _, tc := range cases {
			childFlow := probeFlow("child-flow", nil, tc.expr)
			parent := &Workflow{
				ID:      ID("parent-flow"),
				Runtime: RuntimeGo,
				Nodes: Nodes{
					&fakeNode{id: "s", typ: types.NodeTypeStart, next: "spawn"},
					&spawningNode{id: "spawn", child: childFlow},
					&exprNode{id: "probe", typ: types.NodeTypeEnd, expr: "$NODE.spawn"},
				},
			}
			ctx := runtime.WithExecutionID(context.Background(), "exec-root")
			result, err := parent.Execute(ctx, nil)
			require.NoError(t, err, tc.expr)
			// the child has its own id: the flow roots are the child's, while
			// the execution id correlates across root and child.
			assert.Equal(t, tc.want, result, tc.expr)
		}
	})

	t.Run("flow without its own id falls back to the parent ($FLOW_ID rule)", func(t *testing.T) {
		// Validate rejects an empty id, but the fallback rule lives in the
		// context preparation: a flow without its own id keeps the parent
		// execution's $FLOW_ID (plaita child semantics).
		flow := &Workflow{Runtime: RuntimeGo, Nodes: Nodes{
			&fakeNode{id: "s", typ: types.NodeTypeStart, next: "end"},
			&exprNode{id: "end", typ: types.NodeTypeEnd, expr: "$FLOW_ID"},
		}}
		parentCtx := runtime.WithWorkflowRuntimeContext(context.Background(), runtime.NewContext())
		runtime.WorkflowRuntimeContext(parentCtx).WithFlowID("parent-flow")
		ctx := flow.prepareExecutionRuntimeContext(parentCtx, nil)
		rc := runtime.WorkflowRuntimeContext(ctx)
		assert.Equal(t, "parent-flow", rc.GetFlowID())
		// the effective id lands in $GLOBAL.flow_id too.
		assert.Equal(t, "parent-flow", rc.GetGlobalVariable("flow_id"))
	})
}

func TestSystemRootsSurviveCheckpointRoundTrip(t *testing.T) {
	chain := []Node{&fakeNode{id: "n1", typ: types.NodeTypeAssignment}}
	w := probeFlow("probe-flow", chain, "$FLOW_ID")
	require.NoError(t, w.Validate())
	ctx := runtime.WithExecutionID(context.Background(), "exec-42")
	execCtx := w.prepareExecutionRuntimeContext(ctx, nil)
	// n1 "completed": stage its bookkeeping, then snapshot.
	rc := runtime.WorkflowRuntimeContext(execCtx)
	rc.WithExecuteNodeResult("n1", "ok")
	rc.WithLastNodeID("n1")
	snap, err := SnapshotRuntimeContext(execCtx, &SuspendOptions{FlowID: "probe-flow", ExecutionID: "exec-42"})
	require.NoError(t, err)

	restored, err := RestoreRuntimeContext(context.Background(), snap)
	require.NoError(t, err)
	rc = runtime.WorkflowRuntimeContext(restored)
	assert.Equal(t, "probe-flow", rc.GetFlowID())
	assert.Equal(t, "exec-42", rc.GetExecutionID())
	assert.Equal(t, "n1", string(rc.GetLastNodeID()))

	// the restored context evaluates the roots like the original did.
	got, err := expression.Eval(restored, "$EXECUTION_ID")
	require.NoError(t, err)
	assert.Equal(t, "exec-42", got)
	got, err = expression.Eval(restored, "$FLOW_ID")
	require.NoError(t, err)
	assert.Equal(t, "probe-flow", got)
}
