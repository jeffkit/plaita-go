package nodes

// 嵌套挂起 v1（NS-M2，jeffkit/plaita-go#88）专项测试——rs#16
// crates/plaita-engine/tests/nested_suspend.rs 的对拍镜像：child/reference
// 调用链的挂起穿透与沿栈恢复（决策步→返回步）、恢复中再挂起的栈合并、
// 深位订阅绑定、迭代帧/分支帧的具名拒绝与孤儿订阅防线。
//
// 与 rs 用例的对应关系（命名对齐）：
//   - TestNestedSuspendChildChainParksAndResumes        ← grandchild_chain（单层版）
//   - TestNestedSuspendGrandchildChainTwoFrames         ← grandchild_chain_parks_and_resumes_through_two_frames
//   - TestNestedSuspendReferenceChainIsolation          ← reference_chain_isolation_preserved_through_suspension
//   - TestNestedSuspendLoopDirectNamedRejected          ← loop_boundary（直连：闸在注册前拒，register=0）
//   - TestNestedSuspendLoopRejectionNotSwallowed        ← loop_boundary_rejection_is_not_swallowed_by_error_handler
//   - TestNestedSuspendLoopReferenceBypassNoOrphan      ← loop_reference_bypass_is_named_rejected_without_orphan_subscription
//   - TestNestedSuspendParallelJoinBranchStaticReject   ← parallel_background_branch_is_statically_rejected（join 面）
//   - TestNestedSuspendParallelBackgroundStaticReject   ← parallel_background_branch_is_statically_rejected（background 面）
//   - TestNestedSuspendReSuspensionMergesCallStacks     ← resume_continuation_re_suspension_merges_call_stacks
//   - TestNestedSuspendDeepBindingAndTimeoutAtDepth     ← deep_binding_enforcement_and_timeout_at_depth
//   （rs resume_during_cancel_is_refused 无 go 对应：go 引擎没有进程内
//   cancel 面——取消经 ctx 超时由宿主层表达，恢复入口取消门留 worker 接线单。）

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// countingRegistry records register/unregister counts (the orphan-subscription
// assertion surface).
type countingRegistry struct {
	mu           sync.Mutex
	registered   int
	unregistered []string
}

func (r *countingRegistry) Register(_ context.Context, req *workflow.SubscriptionRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.registered++
	if req.SubscriptionID == "" {
		req.SubscriptionID = fmt.Sprintf("sub-%d", r.registered)
	}
	return nil
}

func (r *countingRegistry) Unregister(_ context.Context, subscriptionID, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unregistered = append(r.unregistered, subscriptionID)
	return nil
}

func nsStart(id, next string) *Start {
	return &Start{BaseNode: BaseNode{ID: workflow.NodeID(id), Next: workflow.NodeID(next)}}
}

func nsEnd(id string, output any) *End {
	return &End{BaseNode: BaseNode{ID: workflow.NodeID(id), Output: output}}
}

func nsEndNoOutput(id string) *End {
	return &End{BaseNode: BaseNode{ID: workflow.NodeID(id)}}
}

func nsEvent(id, eventType, next string) *Event {
	return &Event{BaseNode: BaseNode{ID: workflow.NodeID(id), Next: workflow.NodeID(next)}, EventType: eventType}
}

func nsChild(id, next string, childFlow *workflow.Workflow) *Child {
	return &Child{BaseNode: BaseNode{ID: workflow.NodeID(id), Next: workflow.NodeID(next)}, ChildFlow: childFlow}
}

// nsReference 不声明 input：子流程继承调用方 $INPUT（child 同款语义）。
func nsReference(id, next string, childFlow *workflow.Workflow) *Reference {
	return &Reference{BaseNode: BaseNode{ID: workflow.NodeID(id), Next: workflow.NodeID(next)}, ChildFlow: childFlow}
}

// nestedSuspendCtx wires the suspend options (memory store + counting
// registry) the way the worker injects them on the start/resume paths.
func nestedSuspendCtx(t *testing.T) (context.Context, *countingRegistry) {
	t.Helper()
	reg := &countingRegistry{}
	ctx := workflow.WithSuspendOptions(context.Background(), &workflow.SuspendOptions{
		ExecutionID:   "exec-nested",
		TenantID:      "t1",
		FlowID:        "root",
		Store:         workflow.NewMemoryCheckpointStore(),
		Subscriptions: reg,
	})
	return ctx, reg
}

func nsSuspendOf(t *testing.T, err error) *workflow.SuspendError {
	t.Helper()
	susp := workflow.AsSuspend(err)
	if susp == nil {
		t.Fatalf("expected a suspension, got err = %v", err)
	}
	return susp
}

func nsBound(t *testing.T, root *workflow.Workflow, ctx context.Context, susp *workflow.SuspendError, resumeType string, data map[string]any) (any, error) {
	t.Helper()
	if data == nil {
		data = map[string]any{}
	}
	if _, ok := data["subscription_id"]; !ok {
		// 绑定到该载体挂起期登记的那条订阅（再挂起后是新订阅 id）。
		data["subscription_id"] = nsCarrierSubID(t, susp)
	}
	return root.ResumeSuspension(ctx, susp, resumeType, data)
}

// nsCarrierSubID reads the subscription id the carrier registered at its
// suspend boundary (from the checkpoint's pending node state).
func nsCarrierSubID(t *testing.T, susp *workflow.SuspendError) string {
	t.Helper()
	nodes, _ := susp.Checkpoint.Context["$NODE"].(map[string]any)
	state, _ := nodes[string(susp.NodeID)].(map[string]any)
	id, _ := state["subscription_id"].(string)
	require.NotEmpty(t, id, "carrier has no registered subscription id")
	return id
}

// TestNestedSuspendChildChainParksAndResumes：child 链挂起（单帧调用栈路径
// 断言）+ 沿栈恢复到根完成。
func TestNestedSuspendChildChainParksAndResumes(t *testing.T) {
	sub := &workflow.Workflow{ID: "sub", Nodes: workflow.Nodes{
		nsStart("a", "ev"),
		nsEvent("ev", "t", "bend"),
		nsEnd("bend", map[string]any{"status": "$NODE.ev.status", "data": "$NODE.ev.event_data"}),
	}}
	root := &workflow.Workflow{ID: "root", Nodes: workflow.Nodes{
		nsStart("s", "c"),
		nsChild("c", "o", sub),
		nsEnd("o", "$NODE.c"),
	}}
	ctx, reg := nestedSuspendCtx(t)

	_, err := root.Execute(ctx, nil)
	susp := nsSuspendOf(t, err)
	assert.Equal(t, workflow.NodeID("ev"), susp.NodeID)
	// 调用栈路径：[(root, c)]——最内父帧在前。
	path := susp.CallStackPath()
	require.Len(t, path, 1)
	assert.Equal(t, "root", path[0].FlowID)
	assert.Equal(t, workflow.NodeID("c"), path[0].NodeID)

	result, err := nsBound(t, root, ctx, susp, workflow.ResumeEvent, map[string]any{"k": "v"})
	require.NoError(t, err)
	// 根 end 输出 $NODE.c = sub 返回值 = bend 输出；$NODE.ev 是完整恢复态。
	out, ok := result.(map[string]any)
	require.True(t, ok, "result = %T", result)
	assert.Equal(t, "completed", out["status"])
	data, ok := out["data"].(map[string]any)
	require.True(t, ok, "data = %T", out["data"])
	assert.Equal(t, "v", data["k"])
	// 订阅账目：挂起注册 1 条，恢复消费注销同一条。
	assert.Equal(t, 1, reg.registered)
	assert.Equal(t, []string{"sub-1"}, reg.snapshotUnregistered())
}

// TestNestedSuspendGrandchildChainTwoFrames：孙代双帧栈（rs grandchild 同款
// 断言面：路径、恢复终值、恢复态全键）。
func TestNestedSuspendGrandchildChainTwoFrames(t *testing.T) {
	inner := &workflow.Workflow{ID: "c2.childFlow", Nodes: workflow.Nodes{
		nsStart("b", "ev"),
		nsEvent("ev", "deep", "bend"),
		nsEnd("bend", map[string]any{"from": "deep", "answer": "$NODE.ev"}),
	}}
	mid := &workflow.Workflow{ID: "c1.childFlow", Nodes: workflow.Nodes{
		nsStart("a", "c2"),
		nsChild("c2", "aend", inner),
		nsEnd("aend", "$NODE.c2"),
	}}
	root := &workflow.Workflow{ID: "root", Nodes: workflow.Nodes{
		nsStart("s", "c1"),
		nsChild("c1", "o", mid),
		nsEnd("o", "$NODE.c1"),
	}}
	ctx, _ := nestedSuspendCtx(t)

	_, err := root.Execute(ctx, nil)
	susp := nsSuspendOf(t, err)
	assert.Equal(t, workflow.NodeID("ev"), susp.NodeID)
	// 调用栈 = [c1 帧(root), c2 帧(c1.childFlow)]。
	path := susp.CallStackPath()
	require.Len(t, path, 2)
	assert.Equal(t, "c1.childFlow", path[0].FlowID)
	assert.Equal(t, workflow.NodeID("c2"), path[0].NodeID)
	assert.Equal(t, "root", path[1].FlowID)
	assert.Equal(t, workflow.NodeID("c1"), path[1].NodeID)

	result, err := nsBound(t, root, ctx, susp, workflow.ResumeEvent, map[string]any{"k": "v"})
	require.NoError(t, err)
	out, ok := result.(map[string]any)
	require.True(t, ok, "result = %T", result)
	assert.Equal(t, "deep", out["from"])
	// $NODE.ev 是完整恢复态（挂起态全量保留 + status 翻转 + event_data）。
	ev, ok := out["answer"].(map[string]any)
	require.True(t, ok, "answer = %T", out["answer"])
	assert.Equal(t, "completed", ev["status"])
	assert.Equal(t, "deep", ev["event_type"])
	assert.Equal(t, "t1", ev["subscription_tenant"])
	assert.Contains(t, ev["event_id"], "event_ev_")
	keys := make([]string, 0, len(ev))
	for k := range ev {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	assert.Equal(t, []string{
		"event_data", "event_filter", "event_id", "event_type",
		"is_async", "status", "subscription_id", "subscription_tenant",
	}, keys)
}

// TestNestedSuspendReferenceChainIsolation：reference 链隔离保持（$PARENT
// 不可见父状态）+ 挂起穿透。
func TestNestedSuspendReferenceChainIsolation(t *testing.T) {
	sub := &workflow.Workflow{ID: "sub", Nodes: workflow.Nodes{
		nsStart("a", "ev"),
		nsEvent("ev", "sub-event", "b"),
		nsEnd("b", map[string]any{"got": "$INPUT", "parent_leak": "$PARENT.n"}),
	}}
	root := &workflow.Workflow{ID: "rootf", Nodes: workflow.Nodes{
		nsStart("s", "r"),
		&Reference{
			BaseNode:  BaseNode{ID: "r", Next: "o"},
			Input:     map[string]any{"n": "$INPUT.name"},
			ChildFlow: sub,
		},
		nsEnd("o", "$NODE.r"),
	}}
	ctx, reg := nestedSuspendCtx(t)

	// 走真实入口：root 的 $INPUT 注入由 Execute args 携带。
	_, err := root.Execute(ctx, map[string]any{"name": "kong"})
	susp := nsSuspendOf(t, err)
	assert.Equal(t, workflow.NodeID("ev"), susp.NodeID)
	path := susp.CallStackPath()
	require.Len(t, path, 1)
	assert.Equal(t, "rootf", path[0].FlowID)
	assert.Equal(t, workflow.NodeID("r"), path[0].NodeID)

	result, err := nsBound(t, root, ctx, susp, workflow.ResumeEvent, map[string]any{"x": 1})
	require.NoError(t, err)
	// 隔离保持：$PARENT.n → null（reference 隔离），$INPUT 是注入的 {n}。
	assert.Equal(t, map[string]any{"got": map[string]any{"n": "kong"}, "parent_leak": nil}, result)
	assert.Equal(t, 1, reg.registered)
	assert.Equal(t, 1, len(reg.snapshotUnregistered()))
}

// TestNestedSuspendLoopDirectNamedRejected：loop 直连——作用域闸（advisory
// 快路径）在订阅注册**之前**具名拒绝：register=0、无孤儿。
func TestNestedSuspendLoopDirectNamedRejected(t *testing.T) {
	body := &workflow.Workflow{ID: "loop-body", Nodes: workflow.Nodes{
		nsStart("a", "ev"),
		nsEvent("ev", "t", "b"),
		nsEndNoOutput("b"),
	}}
	root := &workflow.Workflow{ID: "root", Nodes: workflow.Nodes{
		nsStart("s", "lp"),
		&Loop{
			Child:      Child{BaseNode: BaseNode{ID: "lp", Next: "o"}, ChildFlow: body},
			Collection: loopCollection{array: []any{1}},
		},
		nsEndNoOutput("o"),
	}}
	ctx, reg := nestedSuspendCtx(t)

	_, err := root.Execute(ctx, nil)
	require.Error(t, err)
	assert.Nil(t, workflow.AsSuspend(err), "rejection must not surface as a resumable suspend")
	text := err.Error()
	assert.Contains(t, text, "loop-iteration")
	assert.Contains(t, text, "suspends inside")
	assert.Equal(t, 0, reg.registered, "gate rejects before subscription registration")
	assert.Empty(t, reg.snapshotUnregistered())
}

// TestNestedSuspendLoopRejectionNotSwallowed：loop 节点自带 errorHandler
// continue 时，闸拒绝必须仍以终态上抛（rs 修正清单 #3——SuspendRejected
// 拦截臂，continue 不得吞）。
func TestNestedSuspendLoopRejectionNotSwallowed(t *testing.T) {
	body := &workflow.Workflow{ID: "loop-body", Nodes: workflow.Nodes{
		nsStart("a", "ev"),
		nsEvent("ev", "t", "b"),
		nsEndNoOutput("b"),
	}}
	root := &workflow.Workflow{ID: "root", Nodes: workflow.Nodes{
		nsStart("s", "lp"),
		&Loop{
			Child: Child{
				BaseNode: BaseNode{
					ID:                "lp",
					Next:              "o",
					ErrorHandlePolicy: workflow.RecoverableErrorHandlePolicy{Strategy: workflow.HandleStrategyContinue},
				},
				ChildFlow: body,
			},
			Collection: loopCollection{array: []any{1}},
		},
		nsEndNoOutput("o"),
	}}
	ctx, _ := nestedSuspendCtx(t)

	_, err := root.Execute(ctx, nil)
	require.Error(t, err, "rejection must not be swallowed by errorHandler continue")
	assert.Contains(t, err.Error(), "loop-iteration")
}

// TestNestedSuspendLoopReferenceBypassNoOrphan：验证报告 Q4 旁路组合
// loop → reference → event——reference 的 ambient 父链遮蔽闸的链行走，订阅
// 注册后由传播转换点（主闸）具名拒绝**并注销**（register=1 unregister=1，
// 无孤儿）。
func TestNestedSuspendLoopReferenceBypassNoOrphan(t *testing.T) {
	sub := &workflow.Workflow{ID: "sub", Nodes: workflow.Nodes{
		nsStart("x", "ev"),
		nsEvent("ev", "t", "y"),
		nsEndNoOutput("y"),
	}}
	body := &workflow.Workflow{ID: "loop-body", Nodes: workflow.Nodes{
		nsStart("a", "r"),
		nsReference("r", "b", sub),
		nsEndNoOutput("b"),
	}}
	root := &workflow.Workflow{ID: "root", Nodes: workflow.Nodes{
		nsStart("s", "lp"),
		&Loop{
			Child:      Child{BaseNode: BaseNode{ID: "lp", Next: "o"}, ChildFlow: body},
			Collection: loopCollection{array: []any{1}},
		},
		nsEndNoOutput("o"),
	}}
	ctx, reg := nestedSuspendCtx(t)

	_, err := root.Execute(ctx, nil)
	require.Error(t, err)
	text := err.Error()
	assert.Contains(t, text, "loop-iteration", "named loop-boundary rejection expected")
	assert.Contains(t, text, "crossed")
	assert.Equal(t, 1, reg.registered, "advisory gate is blinded by the reference ambient chain")
	assert.Equal(t, []string{"sub-1"}, reg.snapshotUnregistered(), "no orphan subscription")
}

// TestNestedSuspendParallelJoinBranchStaticReject：join 分支图含挂起型节点
// ——派发前静态具名拒。
func TestNestedSuspendParallelJoinBranchStaticReject(t *testing.T) {
	branchFlow := &workflow.Workflow{ID: "branch", Nodes: workflow.Nodes{
		nsStart("bs", "be"),
		nsEvent("be", "t", "bend"),
		nsEndNoOutput("bend"),
	}}
	root := &workflow.Workflow{ID: "root", Nodes: workflow.Nodes{
		nsStart("s", "p"),
		&Parallel{
			BaseNode:     BaseNode{ID: "p", Next: "o"},
			Branches:     []*ParallelBranch{{Branch: Branch{Name: "j"}, Flow: branchFlow}},
			JoinBranches: []string{"j"},
		},
		nsEndNoOutput("o"),
	}}
	ctx, reg := nestedSuspendCtx(t)

	_, err := root.Execute(ctx, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "contains a suspending node")
	assert.Equal(t, 0, reg.registered)
}

// TestNestedSuspendParallelBackgroundStaticReject：后台分支（不在
// joinBranches）同样静态具名拒——后台的动态拒绝是异步的，静态闸覆盖。
func TestNestedSuspendParallelBackgroundStaticReject(t *testing.T) {
	branchFlow := &workflow.Workflow{ID: "bg-branch", Nodes: workflow.Nodes{
		nsStart("bs", "be"),
		nsEvent("be", "t", "bend"),
		nsEndNoOutput("bend"),
	}}
	root := &workflow.Workflow{ID: "root", Nodes: workflow.Nodes{
		nsStart("s", "p"),
		&Parallel{
			BaseNode: BaseNode{ID: "p", Next: "o"},
			Branches: []*ParallelBranch{{Branch: Branch{Name: "bg"}, Flow: branchFlow}},
		},
		nsEndNoOutput("o"),
	}}
	ctx, reg := nestedSuspendCtx(t)

	_, err := root.Execute(ctx, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "contains a suspending node")
	assert.Equal(t, 0, reg.registered)
}

// TestNestedSuspendReSuspensionMergesCallStacks：恢复中再挂起——栈合并
// （新现场在前、剩余在后），二次恢复到根完成。
func TestNestedSuspendReSuspensionMergesCallStacks(t *testing.T) {
	inner := &workflow.Workflow{ID: "c2.childFlow", Nodes: workflow.Nodes{
		nsStart("b", "evB"),
		nsEvent("evB", "second", "bend"),
		nsEnd("bend", "done"),
	}}
	mid := &workflow.Workflow{ID: "c1.childFlow", Nodes: workflow.Nodes{
		nsStart("a", "evA"),
		nsEvent("evA", "first", "c2"),
		nsChild("c2", "aend", inner),
		nsEnd("aend", "$NODE.c2"),
	}}
	root := &workflow.Workflow{ID: "root", Nodes: workflow.Nodes{
		nsStart("s", "c1"),
		nsChild("c1", "o", mid),
		nsEnd("o", "$NODE.c1"),
	}}
	ctx, _ := nestedSuspendCtx(t)

	// 第一次挂起：evA，单帧栈。
	_, err := root.Execute(ctx, nil)
	first := nsSuspendOf(t, err)
	assert.Equal(t, workflow.NodeID("evA"), first.NodeID)
	require.Len(t, first.CallStackPath(), 1)

	// 第一次恢复：决策步后内层继续 → c2 → evB 再次挂起（栈合并）。
	result, err := nsBound(t, root, ctx, first, workflow.ResumeEvent, map[string]any{})
	require.Error(t, err)
	assert.Nil(t, result)
	second := nsSuspendOf(t, err)
	assert.Equal(t, workflow.NodeID("evB"), second.NodeID)
	// 栈合并：新现场（c2 帧，位于 c1.childFlow）在前，剩余（c1 帧，root）在后。
	path := second.CallStackPath()
	require.Len(t, path, 2)
	assert.Equal(t, "c1.childFlow", path[0].FlowID)
	assert.Equal(t, workflow.NodeID("c2"), path[0].NodeID)
	assert.Equal(t, "root", path[1].FlowID)
	assert.Equal(t, workflow.NodeID("c1"), path[1].NodeID)

	// 第二次恢复：一路返回步到根。
	out, err := nsBound(t, root, ctx, second, workflow.ResumeEvent, map[string]any{})
	require.NoError(t, err)
	assert.Equal(t, "done", out)
}

// TestNestedSuspendDeepBindingAndTimeoutAtDepth：深位订阅绑定（ResumeGuard
// 语义保持）+ timeout 载体在深位的返回步回传。
func TestNestedSuspendDeepBindingAndTimeoutAtDepth(t *testing.T) {
	sub := &workflow.Workflow{ID: "sub", Nodes: workflow.Nodes{
		nsStart("a", "ev"),
		nsEvent("ev", "t", "b"),
		nsEnd("b", "$NODE.ev"),
	}}
	root := &workflow.Workflow{ID: "root", Nodes: workflow.Nodes{
		nsStart("s", "c"),
		nsChild("c", "o", sub),
		nsEnd("o", "$NODE.c"),
	}}
	ctx, _ := nestedSuspendCtx(t)

	// 错误绑定 → ResumeGuard（深位照常准入校验），执行保持挂起。
	_, err := root.Execute(ctx, nil)
	susp := nsSuspendOf(t, err)
	_, err = root.ResumeSuspension(ctx, susp, workflow.ResumeEvent, map[string]any{"subscription_id": "wrong"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not bound to the suspended subscription")
	var guard *workflow.ResumeGuardError
	assert.ErrorAs(t, err, &guard)

	// timeout 载体在深位：内层节点状态翻 timeout，返回步带回父帧到根。
	_, err = root.Execute(ctx, nil)
	susp = nsSuspendOf(t, err)
	out, err := nsBound(t, root, ctx, susp, workflow.ResumeTimeout, map[string]any{})
	require.NoError(t, err)
	m, ok := out.(map[string]any)
	require.True(t, ok, "out = %T", out)
	assert.Equal(t, "timeout", m["status"])
}

// snapshotUnregistered 是计数注册表的读面（测试断言用）。
func (r *countingRegistry) snapshotUnregistered() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.unregistered))
	copy(out, r.unregistered)
	return out
}
