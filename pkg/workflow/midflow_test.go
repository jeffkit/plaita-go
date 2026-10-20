package workflow

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeffkit/plaita-go/internal/types"
)

// Per-step checkpoints + mid-flow entry (#32): a continue/retry carrier
// enters a live execution at the checkpoint's recorded position instead of
// replaying completed nodes.

// effectNode is a fakeNode with a side effect (the non-idempotent node of the
// acceptance criteria): every Execute appends to effects.
type effectNode struct {
	fakeNode
	effects *[]string
}

func (n *effectNode) Execute(ctx context.Context) (*ExecuteResult, error) {
	res, err := n.fakeNode.Execute(ctx)
	if err == nil {
		*n.effects = append(*n.effects, "run:"+n.id)
	}
	return res, err
}

// suspendingNode parks the flow (plaita is_suspending): Execute stages a
// pending result, IsSuspending is true.
type suspendingNode struct {
	fakeNode
}

func (n *suspendingNode) IsSuspending() bool { return true }

func (n *suspendingNode) Execute(ctx context.Context) (*ExecuteResult, error) {
	return &ExecuteResult{
		Result:   map[string]any{"status": "pending"},
		NextNode: n.next,
	}, nil
}

func midFlowFixture() (*Workflow, *MemoryCheckpointStore, *[]string) {
	effects := []string{}
	mk := func(id string, execErrs ...error) *effectNode {
		return &effectNode{
			fakeNode: fakeNode{
				id:       id,
				typ:      types.NodeTypeAssignment,
				execErrs: execErrs,
			},
			effects: &effects,
		}
	}
	// n3 fails its first attempt deterministically (never retried in-run).
	w := &Workflow{
		ID:      ID("midflow"),
		Runtime: RuntimeGo,
		Nodes: Nodes{
			&fakeNode{id: "s", typ: types.NodeTypeStart},
			mk("n1"), mk("n2"), mk("n3", NonRetryable(errors.New("boom"))),
			&fakeNode{id: "e", typ: types.NodeTypeEnd},
		},
	}
	linkNodes(w.Nodes, "s", "n1", "n2", "n3", "e")
	store := NewMemoryCheckpointStore()
	return w, store, &effects
}

// linkNodes chains the named nodes in order (fakeNode/effectNode carriers).
func linkNodes(nodes Nodes, ids ...string) {
	byID := map[NodeID]Node{}
	for _, n := range nodes {
		byID[n.GetID()] = n
	}
	for i, id := range ids {
		if i+1 >= len(ids) {
			continue
		}
		switch n := byID[NodeID(id)].(type) {
		case *fakeNode:
			n.next = NodeID(ids[i+1])
		case *effectNode:
			n.next = NodeID(ids[i+1])
		}
	}
}

func midFlowCtx(ctx context.Context, store *MemoryCheckpointStore) context.Context {
	return WithSuspendOptions(ctx, &SuspendOptions{
		ExecutionID:    "exec-mid",
		Store:          store,
		StepCheckpoint: 1,
	})
}

func TestPerStepCheckpointMidFlowResume(t *testing.T) {
	w, store, effects := midFlowFixture()
	ctx := midFlowCtx(context.Background(), store)

	// First run: n1/n2 succeed (side effects land), n3 fails → Execute errors,
	// but the per-step checkpoints recorded the live position.
	_, execErr := w.Execute(ctx, nil)
	require.Error(t, execErr)
	assert.Contains(t, execErr.Error(), "boom")
	require.Len(t, *effects, 2, "n1+n2 side effects from the first run only")

	cp, err := store.LoadCheckpoint(ctx, "", "exec-mid")
	require.NoError(t, err)
	require.NotNil(t, cp)
	assert.Equal(t, CheckpointKindPerStep, cp.Kind)
	assert.Equal(t, NodeID("n2"), cp.SuspendedNode, "last completed node")
	assert.Equal(t, NodeID("n3"), cp.NextNode, "the entry position")
	assert.Equal(t, "midflow", cp.FlowID)

	// Mid-flow entry: only the remaining nodes execute. The completed n1/n2
	// are NOT replayed (counter assertion), so the non-idempotent n2 side
	// effect happens exactly once.
	result, err := w.ResumeFrom(ctx, cp, "")
	require.NoError(t, err)
	assert.Equal(t, "ok", result)
	assert.Equal(t, []string{"run:n1", "run:n2", "run:n3"}, *effects)

	// The continuation advanced the recorded position: n3 completed, end next.
	cp, err = store.LoadCheckpoint(ctx, "", "exec-mid")
	require.NoError(t, err)
	require.NotNil(t, cp)
	assert.Equal(t, NodeID("n3"), cp.SuspendedNode)
}

func TestResumeFromExplicitNextNode(t *testing.T) {
	w, store, effects := midFlowFixture()
	ctx := midFlowCtx(context.Background(), store)

	_, execErr := w.Execute(ctx, nil)
	require.Error(t, execErr)
	cp, err := store.LoadCheckpoint(ctx, "", "exec-mid")
	require.NoError(t, err)

	// An explicit nextNodeID overrides the checkpoint's next_node: retry
	// semantics re-execute a chosen node (here n2, whose earlier side effect
	// then happens twice — the explicit contract of naming it).
	result, err := w.ResumeFrom(ctx, cp, "n2")
	require.NoError(t, err)
	assert.Equal(t, "ok", result)
	// n3 never completed its first attempt, so its effect is absent from the
	// first run; the continuation runs n2 again and steps on through n3.
	assert.Equal(t, []string{"run:n1", "run:n2", "run:n2", "run:n3"}, *effects)
}

func TestResumeFromGuards(t *testing.T) {
	w, _ /* store */, _ := midFlowFixture()
	ctx := context.Background()
	guard := func(err error) *ResumeGuardError {
		t.Helper()
		var g *ResumeGuardError
		require.ErrorAs(t, err, &g)
		return g
	}

	_, err := w.ResumeFrom(ctx, nil, "")
	assert.NotNil(t, guard(err))

	_, err = w.ResumeFrom(ctx, &Checkpoint{
		SchemaVersion: CheckpointSchemaVersion,
		Kind:          CheckpointKindSuspend,
		SuspendedNode: "n2",
		NextNode:      "n3",
		Context:       map[string]any{"$INPUT": map[string]any{}, "$NODE": map[string]any{}, "$GLOBAL": map[string]any{}, "$ENV": map[string]any{}},
	}, "")
	assert.Contains(t, guard(err).Error(), "Workflow.Resume")

	_, err = w.ResumeFrom(ctx, &Checkpoint{SchemaVersion: 99}, "")
	assert.NotNil(t, guard(err))

	_, err = w.ResumeFrom(ctx, &Checkpoint{
		SchemaVersion: CheckpointSchemaVersion,
		NextNode:      "ghost",
		Context:       map[string]any{},
	}, "")
	assert.Contains(t, guard(err).Error(), "ghost")

	_, err = w.ResumeFrom(ctx, &Checkpoint{SchemaVersion: CheckpointSchemaVersion}, "")
	require.Error(t, err, "a checkpoint without a context cannot be restored")
}

func TestPerStepCheckpointInterval(t *testing.T) {
	w := &Workflow{
		ID:      ID("interval"),
		Runtime: RuntimeGo,
		Nodes: Nodes{
			&fakeNode{id: "s", typ: types.NodeTypeStart},
			&fakeNode{id: "n1", typ: types.NodeTypeAssignment},
			&fakeNode{id: "e", typ: types.NodeTypeEnd},
		},
	}
	linkNodes(w.Nodes, "s", "n1", "e")

	store := NewMemoryCheckpointStore()
	ctx := WithSuspendOptions(context.Background(), &SuspendOptions{
		ExecutionID:    "exec-int",
		Store:          store,
		StepCheckpoint: 2, // every 2nd completed node
	})
	_, err := w.Execute(ctx, nil)
	require.NoError(t, err)

	// completed nodes: s (step 1, skipped), n1 (step 2, checkpointed).
	cp, err := store.LoadCheckpoint(ctx, "", "exec-int")
	require.NoError(t, err)
	require.NotNil(t, cp, "the 2nd completed node (n1) is checkpointed")
	assert.Equal(t, NodeID("n1"), cp.SuspendedNode)
	assert.Equal(t, CheckpointKindPerStep, cp.Kind)
}

func TestPerStepCheckpointSkipsStepFailure(t *testing.T) {
	// A node that fails does not produce a per-step checkpoint for itself —
	// the position stays at the last COMPLETED node (or the worker's failure
	// boundary snapshot supplies the retry position).
	w, store, _ := midFlowFixture()
	ctx := midFlowCtx(context.Background(), store)
	_, execErr := w.Execute(ctx, nil)
	require.Error(t, execErr)
	cp, err := store.LoadCheckpoint(ctx, "", "exec-mid")
	require.NoError(t, err)
	require.NotNil(t, cp)
	assert.Equal(t, NodeID("n2"), cp.SuspendedNode, "n3 failed and is not a position")
}

func TestPerStepCheckpointNestedFlowSkipped(t *testing.T) {
	// A child flow's nodes never produce per-step checkpoints: ResumeFrom
	// re-enters the ROOT flow, so a nested position cannot be expressed.
	effects := []string{}
	child := &Workflow{
		ID:      ID("child-flow"),
		Runtime: RuntimeGo,
		Nodes: Nodes{
			&fakeNode{id: "cs", typ: types.NodeTypeStart},
			&effectNode{fakeNode: fakeNode{id: "cn", typ: types.NodeTypeAssignment}, effects: &effects},
			&fakeNode{id: "ce", typ: types.NodeTypeEnd},
		},
	}
	linkNodes(child.Nodes, "cs", "cn", "ce")

	w := &Workflow{
		ID:      ID("root"),
		Runtime: RuntimeGo,
		Nodes: Nodes{
			&fakeNode{id: "s", typ: types.NodeTypeStart},
			&spawnNextNode{spawningNode: spawningNode{id: "spawn", child: child}, next: "e"},
			&fakeNode{id: "e", typ: types.NodeTypeEnd},
		},
	}
	linkNodes(w.Nodes, "s", "spawn", "e")

	store := NewMemoryCheckpointStore()
	ctx := midFlowCtx(context.Background(), store)
	_, err := w.Execute(ctx, nil)
	require.NoError(t, err)

	cp, err := store.LoadCheckpoint(ctx, "", "exec-mid")
	require.NoError(t, err)
	require.NotNil(t, cp)
	assert.Equal(t, CheckpointKindPerStep, cp.Kind)
	assert.Equal(t, NodeID("spawn"), cp.SuspendedNode, "only root-flow positions are checkpointed")
	assert.Equal(t, NodeID("e"), cp.NextNode)
}

// spawnNextNode is a spawningNode with a configurable next pointer (its own
// Execute: the promoted spawningNode.Execute would resolve NextNode on the
// embedded receiver).
type spawnNextNode struct {
	spawningNode
	next NodeID
}

func (n *spawnNextNode) NextNode() NodeID { return n.next }

func (n *spawnNextNode) Execute(ctx context.Context) (*ExecuteResult, error) {
	res, err := n.spawningNode.Execute(ctx)
	if res != nil {
		res.NextNode = n.next
	}
	return res, err
}

func TestResumeFromCarriesIntoSuspend(t *testing.T) {
	// A mid-flow continuation whose remaining nodes park the flow surfaces
	// the SuspendError and the kind=suspend boundary takes over the key; a
	// later ResumeFrom on that parked checkpoint is refused (Workflow.Resume
	// owns decision-carrier wake-ups).
	effects := []string{}
	n1 := &effectNode{fakeNode: fakeNode{id: "n1", typ: types.NodeTypeAssignment}, effects: &effects}
	w := &Workflow{
		ID:      ID("park"),
		Runtime: RuntimeGo,
		Nodes: Nodes{
			&fakeNode{id: "s", typ: types.NodeTypeStart},
			n1,
			&suspendingNode{fakeNode: fakeNode{id: "park", typ: types.NodeTypeAssignment}},
			&fakeNode{id: "e", typ: types.NodeTypeEnd},
		},
	}
	linkNodes(w.Nodes, "s", "n1", "park", "e")

	store := NewMemoryCheckpointStore()
	ctx := midFlowCtx(context.Background(), store)

	_, execErr := w.Execute(ctx, nil)
	susp := AsSuspend(execErr)
	require.NotNil(t, susp, "flow parks at the event node: %v", execErr)
	require.NotNil(t, susp.Checkpoint)
	assert.Equal(t, CheckpointKindSuspend, susp.Checkpoint.Kind)
	assert.Equal(t, NodeID("park"), susp.Checkpoint.SuspendedNode)

	cp, err := store.LoadCheckpoint(ctx, "", "exec-mid")
	require.NoError(t, err)
	require.NotNil(t, cp)
	assert.Equal(t, CheckpointKindSuspend, cp.Kind, "the parked boundary overwrites the per-step position")

	_, err = w.ResumeFrom(ctx, cp, "")
	var guard *ResumeGuardError
	require.ErrorAs(t, err, &guard)
}
