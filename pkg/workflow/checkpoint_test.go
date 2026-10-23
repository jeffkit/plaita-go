package workflow

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/expression"
)

// --- snapshot semantics (plaita state.py _snapshot_value) -------------------

func TestSnapshotValueRebuildsFreshContainers(t *testing.T) {
	child := map[string]any{"leaf": 1.0}
	src := map[string]any{
		"map":  child,
		"list": []any{map[string]any{"a": 1.0}},
	}

	snapAny, err := SnapshotValue(src)
	if err != nil {
		t.Fatalf("SnapshotValue: %v", err)
	}
	snap := snapAny.(map[string]any)

	// Mutating the live containers must not change the snapshot.
	child["leaf"] = 2.0
	src["list"].([]any)[0].(map[string]any)["a"] = 9.0
	if got := snap["map"].(map[string]any)["leaf"]; got != 1.0 {
		t.Fatalf("snapshot aliased the live map: leaf = %v", got)
	}
	if got := snap["list"].([]any)[0].(map[string]any)["a"]; got != 1.0 {
		t.Fatalf("snapshot aliased the live list element: a = %v", got)
	}
}

func TestSnapshotValueTruncatesCycleWithPath(t *testing.T) {
	root := map[string]any{}
	root["a"] = map[string]any{"self": root}
	root["tail"] = 1.0

	snapAny, err := SnapshotValue(root)
	if err != nil {
		t.Fatalf("SnapshotValue: %v", err)
	}
	snap := snapAny.(map[string]any)
	got := snap["a"].(map[string]any)["self"]
	if got != "<recursive: $['a']['self']>" {
		t.Fatalf("cycle placeholder = %v, want <recursive: $['a']['self']>", got)
	}
}

func TestSnapshotValueSharedDAGIsNotACycle(t *testing.T) {
	shared := map[string]any{"x": 1.0}
	root := map[string]any{"left": shared, "right": shared}

	snapAny, err := SnapshotValue(root)
	if err != nil {
		t.Fatalf("SnapshotValue: %v", err)
	}
	snap := snapAny.(map[string]any)
	for _, key := range []string{"left", "right"} {
		m, ok := snap[key].(map[string]any)
		if !ok || m["x"] != 1.0 {
			t.Fatalf("%s was not materialised: %v", key, snap[key])
		}
	}
}

func TestSnapshotValueDepthLimit(t *testing.T) {
	root := map[string]any{}
	cur := root
	for i := 0; i < SnapshotMaxDepth+2; i++ {
		next := map[string]any{}
		cur["n"] = next
		cur = next
	}
	_, err := SnapshotValue(root)
	if err == nil {
		t.Fatal("expected a depth error")
	}
	var snapErr *StateSnapshotError
	if !errors.As(err, &snapErr) {
		t.Fatalf("error type = %T, want *StateSnapshotError", err)
	}
	if !strings.Contains(err.Error(), "exceeds limit") {
		t.Fatalf("error = %v, want an exceeds-limit message", err)
	}
}

func TestSnapshotValueRejectsNonStringMapKey(t *testing.T) {
	_, err := SnapshotValue(map[int]any{1: "x"})
	if err == nil {
		t.Fatal("expected a map key error")
	}
	var snapErr *StateSnapshotError
	if !errors.As(err, &snapErr) {
		t.Fatalf("error type = %T, want *StateSnapshotError", err)
	}
}

// --- checkpoint round trip --------------------------------------------------

func TestCheckpointContextRoundTrip(t *testing.T) {
	rc := runtime.NewContext()
	rc.WorkflowInput = map[string]any{"who": "roundtrip"}
	rc.ExecuteNodeResults["step1"] = map[string]any{"v": 1.0}
	rc.GlobalVariables["g"] = "G"
	rc.EnvVariables["E"] = "V"
	rc.LastNodeID = "step1"
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), rc)

	snap, err := SnapshotRuntimeContext(ctx, &SuspendOptions{ExecutionID: "exec-1", FlowID: "flow-1"})
	if err != nil {
		t.Fatalf("SnapshotRuntimeContext: %v", err)
	}
	if snap["$EXECUTION_ID"] != "exec-1" || snap["$FLOW_ID"] != "flow-1" {
		t.Fatalf("checkpoint ids wrong: %v", snap)
	}

	restored, err := RestoreRuntimeContext(context.Background(), snap)
	if err != nil {
		t.Fatalf("RestoreRuntimeContext: %v", err)
	}
	cases := []struct {
		expr string
		want any
	}{
		{"$INPUT.who", "roundtrip"},
		{"$NODE.step1.v", 1.0},
		{"$GLOBAL.g", "G"},
		{"$ENV.E", "V"},
	}
	for _, tc := range cases {
		got, err := expression.Eval(restored, tc.expr)
		if err != nil {
			t.Fatalf("%s: %v", tc.expr, err)
		}
		if got != tc.want {
			t.Fatalf("%s = %v, want %v", tc.expr, got, tc.want)
		}
	}
	if got := runtime.WorkflowRuntimeContext(restored).LastNodeID; got != "step1" {
		t.Fatalf("$LAST_NODE = %q, want step1", got)
	}
}

// --- engine suspend / resume ------------------------------------------------

type fakeSubscriptionRegistry struct {
	mu           sync.Mutex
	registered   []SubscriptionRequest
	unregistered []string
}

func (r *fakeSubscriptionRegistry) Register(_ context.Context, req *SubscriptionRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if req.SubscriptionID == "" {
		req.SubscriptionID = "sub-1"
	}
	r.registered = append(r.registered, *req)
	return nil
}

func (r *fakeSubscriptionRegistry) Unregister(_ context.Context, subscriptionID, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unregistered = append(r.unregistered, subscriptionID)
	return nil
}

type suspendingTestNode struct {
	fakeNode
	resumedType string
	resumedData map[string]any
}

func (n *suspendingTestNode) IsSuspending() bool { return true }

func (n *suspendingTestNode) Execute(context.Context) (*ExecuteResult, error) {
	return &ExecuteResult{
		Result:   map[string]any{"status": "pending", "event_type": "test.event"},
		NextNode: n.next,
	}, nil
}

func (n *suspendingTestNode) SuspendSpec(context.Context, *ExecuteResult) (*SuspendSpec, error) {
	return &SuspendSpec{Subscription: &SubscriptionRequest{EventType: "test.event"}}, nil
}

func (n *suspendingTestNode) Resume(_ context.Context, resumeType string, data map[string]any) (*ExecuteResult, error) {
	n.resumedType = resumeType
	n.resumedData = data
	return &ExecuteResult{
		Result:   map[string]any{"status": "completed"},
		NextNode: n.next,
	}, nil
}

type suspendFixture struct {
	flow     *Workflow
	wait     *suspendingTestNode
	store    *MemoryCheckpointStore
	registry *fakeSubscriptionRegistry
	opts     *SuspendOptions
}

func newSuspendFixture(t *testing.T) *suspendFixture {
	t.Helper()
	start := &fakeNode{id: "start", typ: types.NodeTypeStart, next: "wait"}
	wait := &suspendingTestNode{fakeNode: fakeNode{id: "wait", typ: NodeType("test_suspend"), next: "end"}}
	end := &fakeNode{id: "end", typ: types.NodeTypeEnd}
	flow := &Workflow{ID: "suspend-flow", Nodes: Nodes{start, wait, end}}
	store := NewMemoryCheckpointStore()
	registry := &fakeSubscriptionRegistry{}
	return &suspendFixture{
		flow:     flow,
		wait:     wait,
		store:    store,
		registry: registry,
		opts: &SuspendOptions{
			ExecutionID:   "exec-susp",
			TenantID:      "",
			FlowID:        "suspend-flow",
			Store:         store,
			Subscriptions: registry,
		},
	}
}

func (f *suspendFixture) suspend(t *testing.T) *SuspendError {
	t.Helper()
	ctx := WithSuspendOptions(context.Background(), f.opts)
	_, err := f.flow.Execute(ctx, map[string]any{"x": 1.0})
	susp := AsSuspend(err)
	if susp == nil {
		t.Fatalf("Execute did not suspend: err = %v", err)
	}
	return susp
}

func TestSuspendWritesCheckpointAndSubscription(t *testing.T) {
	f := newSuspendFixture(t)
	susp := f.suspend(t)

	if susp.NodeID != "wait" {
		t.Fatalf("suspended node = %q, want wait", susp.NodeID)
	}
	cp, err := f.store.LoadCheckpoint(context.Background(), "", "exec-susp")
	if err != nil || cp == nil {
		t.Fatalf("checkpoint not stored: %v (cp=%v)", err, cp)
	}
	if cp.SuspendedNode != "wait" || cp.FlowID != "suspend-flow" {
		t.Fatalf("checkpoint header wrong: %+v", cp)
	}
	nodeState := cp.Context["$NODE"].(map[string]any)["wait"].(map[string]any)
	if nodeState["status"] != "pending" {
		t.Fatalf("pending node state not snapshotted: %v", nodeState)
	}
	if got := nodeState["subscription_id"]; got != "sub-1" {
		t.Fatalf("subscription_id = %v, want sub-1", got)
	}
	if len(f.registry.registered) != 1 {
		t.Fatalf("registered subscriptions = %d, want 1", len(f.registry.registered))
	}
	req := f.registry.registered[0]
	if req.EventType != "test.event" || req.CorrelationID != "exec-susp" || req.FlowID != "suspend-flow" || req.NodeID != "wait" {
		t.Fatalf("subscription request wrong: %+v", req)
	}
}

func TestResumeDispatchesEventTimeoutCancel(t *testing.T) {
	for _, resumeType := range []string{ResumeEvent, ResumeTimeout, ResumeCancel} {
		t.Run(resumeType, func(t *testing.T) {
			f := newSuspendFixture(t)
			susp := f.suspend(t)

			ctx := WithSuspendOptions(context.Background(), f.opts)
			result, err := f.flow.Resume(ctx, susp.Checkpoint, resumeType, map[string]any{
				"subscription_id": "sub-1",
				"event_type":      "test.event",
				"event_data":      map[string]any{"ok": true},
			})
			if err != nil {
				t.Fatalf("Resume(%s): %v", resumeType, err)
			}
			if result != "ok" {
				t.Fatalf("flow result = %v, want ok", result)
			}
			if f.wait.resumedType != resumeType {
				t.Fatalf("node saw resume_type %q, want %q", f.wait.resumedType, resumeType)
			}
			if len(f.registry.unregistered) != 1 || f.registry.unregistered[0] != "sub-1" {
				t.Fatalf("subscription not consumed: %v", f.registry.unregistered)
			}
		})
	}
}

func TestResumeRejectsStaleSubscriptionBinding(t *testing.T) {
	f := newSuspendFixture(t)
	susp := f.suspend(t)

	ctx := WithSuspendOptions(context.Background(), f.opts)
	_, err := f.flow.Resume(ctx, susp.Checkpoint, ResumeEvent, map[string]any{
		"subscription_id": "stale-sub",
		"event_type":      "test.event",
	})
	var guard *ResumeGuardError
	if !errors.As(err, &guard) {
		t.Fatalf("error = %v (%T), want *ResumeGuardError", err, err)
	}
	if f.wait.resumedType != "" {
		t.Fatal("stale resume must not reach the node")
	}
}

func TestResumeRejectsContinueOnPendingNode(t *testing.T) {
	f := newSuspendFixture(t)
	susp := f.suspend(t)

	ctx := WithSuspendOptions(context.Background(), f.opts)
	_, err := f.flow.Resume(ctx, susp.Checkpoint, ResumeContinue, nil)
	var guard *ResumeGuardError
	if !errors.As(err, &guard) {
		t.Fatalf("error = %v (%T), want *ResumeGuardError", err, err)
	}
}

func TestSuspendRefusesWithoutSubscriptionRegistry(t *testing.T) {
	start := &fakeNode{id: "start", typ: types.NodeTypeStart, next: "wait"}
	wait := &suspendingTestNode{fakeNode: fakeNode{id: "wait", typ: NodeType("test_suspend"), next: "end"}}
	end := &fakeNode{id: "end", typ: types.NodeTypeEnd}
	flow := &Workflow{ID: "no-registry", Nodes: Nodes{start, wait, end}}
	ctx := WithSuspendOptions(context.Background(), &SuspendOptions{
		ExecutionID: "exec-nr",
		FlowID:      "no-registry",
		Store:       NewMemoryCheckpointStore(),
	})
	_, err := flow.Execute(ctx, nil)
	if err == nil || AsSuspend(err) != nil {
		t.Fatalf("expected a hard error (never suspend without a wake-up source), got %v", err)
	}
	if !strings.Contains(err.Error(), "subscription registry") {
		t.Fatalf("error = %v, want a subscription-registry message", err)
	}
}

func TestSuspendInParentLinkedContextSuspends(t *testing.T) {
	// 嵌套挂起 v1 语义改写（原 TestSuspendInsideNestedFlowFailsClosed）：
	// 作用域闸从「单层 parent 判定」改为「帧 kind 判定」——child/reference
	// 调用链放行，仅迭代/分支帧具名拒。裸 Execute 携带父链 ctx 时本执行帧
	// 仍是 Root kind（prepareExecutionRuntimeContext 为本流新建执行上下文），
	// 挂起照常成立、无调用栈帧；真正的 child 链挂起/迭代帧拒/分支帧拒见
	// nodes 包 nested_suspend_test.go（rs 对拍镜像测试集）。
	f := newSuspendFixture(t)
	// A child flow's runtime context carries the enclosing execution as its
	// parent (child.go / CloneContext set ParentWorkflowContext).
	parent := runtime.NewContext()
	parent.ParentWorkflowContext = context.Background()
	nested := runtime.WithWorkflowRuntimeContext(context.Background(), parent)
	nested = WithSuspendOptions(nested, f.opts)
	_, err := f.flow.Execute(nested, nil)
	susp := AsSuspend(err)
	if susp == nil {
		t.Fatalf("root-frame execution suspends regardless of parent linkage, got err = %v", err)
	}
	if len(susp.CallStack) != 0 {
		t.Fatalf("bare Execute carries no call frames, got %d", len(susp.CallStack))
	}
	if susp.Flow != f.flow || susp.Ctx == nil {
		t.Fatalf("carrier must hold the live frames: flow=%v ctx=%v", susp.Flow, susp.Ctx)
	}
}
