package workflow

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jeffkit/plaita-go/internal/runtime"
)

// Suspend/resume (M6, checkpoint v1.5).
//
// The engine follows plaita's distributed strategy:
//
//   - a node declares itself suspending (`is_suspending` in Python). After its
//     Execute returns a result whose status is "pending", the engine does NOT
//     advance: it registers the node's wake-up subscription, snapshots the
//     execution context and returns a *SuspendError. The worker turns that
//     into a `suspended` state row;
//   - `Resume` restores the snapshot, asks the suspended node to interpret the
//     resume carrier (`Resume(resumeType, data)`) and continues from the
//     node's next pointer — the same polymorphic dispatch as
//     `DistributedStrategy._handle_resume` + `EventNode.resume`.
//
// Scope (nested suspend v1, rs 语义契约对齐): a suspending node inside a
// child/reference CALL CHAIN suspends the whole tree — the live call stack
// travels on the SuspendError carrier and ResumeSuspension walks it back to
// the root. A suspension on a loop/map/filter/find/reduce/while iteration
// frame or a parallel branch frame is named-rejected (SuspendRejectedError):
// its resume position needs iteration-state snapshotting, a future milestone.

// Resume carrier types (§9.2 / plaita ResumeType).
const (
	ResumeEvent    = "event"
	ResumeTimeout  = "timeout"
	ResumeCancel   = "cancel"
	ResumeContinue = "continue"
	ResumeRetry    = "retry"
)

// resumeBindingKey is the private carrier key a manual resume can use to bind
// itself to the suspended subscription without polluting the user payload
// (plaita `RESUME_BINDING_KEY`).
const resumeBindingKey = "__plaita_subscription_id"

// Suspender is the node capability behind plaita's `is_suspending` class
// attribute: executing it parks the flow.
type Suspender interface {
	IsSuspending() bool
}

// Resumer is the polymorphic resume entry of a suspending node (plaita
// `Node.resume`). resumeType is one of ResumeEvent/ResumeTimeout/ResumeCancel
// for suspending nodes; data is the resume carrier payload.
type Resumer interface {
	Resume(ctx context.Context, resumeType string, data map[string]any) (*ExecuteResult, error)
}

// SuspendSpec describes what the host must set up before the flow may
// suspend. A nil spec means "no external wake-up needed" (a purely
// host-driven suspension).
type SuspendSpec struct {
	// Subscription, when non-nil, is registered before the checkpoint is
	// written. A failed registration refuses the suspension (plaita: never
	// park an execution without a wake-up source).
	Subscription *SubscriptionRequest
}

// SubscriptionRequest is one suspended node's wake-up subscription
// (plaita EventSubscription core fields). The registry fills SubscriptionID
// when empty.
type SubscriptionRequest struct {
	SubscriptionID string         `json:"subscription_id"`
	EventType      string         `json:"event_type"`
	Filter         map[string]any `json:"filter,omitempty"`
	CorrelationID  string         `json:"correlation_id,omitempty"`
	FlowID         string         `json:"flow_id,omitempty"`
	NodeID         string         `json:"node_id,omitempty"`
	TenantID       string         `json:"tenant_id,omitempty"`
	// TimeoutSeconds > 0 arms the subscription timeout: the host enqueues a
	// resume_type=timeout carrier when it elapses.
	TimeoutSeconds float64 `json:"timeout,omitempty"`
}

// SuspendDescriptor lets a suspending node contribute its wake-up spec.
type SuspendDescriptor interface {
	SuspendSpec(ctx context.Context, result *ExecuteResult) (*SuspendSpec, error)
}

// SubscriptionRegistry is the host-side wake-up subscription store (the
// Redis-backed implementation lives in pkg/worker; engine tests use fakes).
type SubscriptionRegistry interface {
	Register(ctx context.Context, req *SubscriptionRequest) error
	Unregister(ctx context.Context, subscriptionID, tenantID string) error
}

// SuspendOptions is the per-execution host wiring for suspend/resume:
// checkpoint persistence + subscription registration. The worker injects it
// on both the start and the resume path.
type SuspendOptions struct {
	ExecutionID   string
	TenantID      string
	FlowID        string
	Store         CheckpointStore
	Subscriptions SubscriptionRegistry
	// StepCheckpoint > 0 enables per-step checkpoints (#32): after every
	// N-th completed ROOT-flow node the engine snapshots the execution
	// context and saves a kind=per-step checkpoint (N=1: every step). The
	// per-step checkpoint shares the execution's checkpoint key — it is the
	// resume position a continue/retry carrier enters at (Workflow.
	// ResumeFrom); a suspend boundary overwrites it while parked. 0 (the
	// zero value) keeps the M6 behavior: checkpoints at suspend boundaries
	// only.
	StepCheckpoint int
}

type suspendOptionsKey struct{}

// WithSuspendOptions attaches suspend/resume host wiring to ctx.
func WithSuspendOptions(ctx context.Context, opts *SuspendOptions) context.Context {
	if opts == nil {
		return ctx
	}
	return context.WithValue(ctx, suspendOptionsKey{}, opts)
}

// SuspendOptionsFromContext returns the host wiring attached by
// WithSuspendOptions, or nil.
func SuspendOptionsFromContext(ctx context.Context) *SuspendOptions {
	if ctx == nil {
		return nil
	}
	opts, _ := ctx.Value(suspendOptionsKey{}).(*SuspendOptions)
	return opts
}

// SuspendError is the engine's "this execution parked" signal. It is
// deliberately an error: nested flow nodes wrap and propagate errors, so a
// suspension physically cannot be mistaken for flow output on the way up.
// The worker detects it with AsSuspend before classifying failures.
type SuspendError struct {
	NodeID     NodeID
	Checkpoint *Checkpoint
	// SubscriptionID is the registered wake-up subscription, when any.
	SubscriptionID string
	// 嵌套挂起 v1（进程内载体，rs SuspendedExecution 同形）：Flow 是挂起节点
	// 所在的 flow，Ctx 是该帧的活运行态；CallStack 是待返回的父帧，index 0 =
	// 最内父帧，递增到根（空栈 = 根流程挂起）。Checkpoint 保持扁平单帧快照
	// （worker 持久化层如何序列化调用栈路径是后续单的规格面，见 jeffkit/
	// plaita-go#88 备注）。
	Flow      *Workflow
	Ctx       *runtime.Context
	CallStack []CallFrame
}

// CallFrame is one frame of a nested suspension's call stack: the parent
// execution awaiting the return of its child node (rs CallFrame 同形).
type CallFrame struct {
	// NodeID is the child node id inside the parent flow.
	NodeID NodeID
	// Workflow is the parent flow containing the child node.
	Workflow *Workflow
	// Ctx is the parent frame's live runtime context.
	Ctx *runtime.Context
}

// CallStackPathEntry is one (flow_id, node_id) pair of the suspension's call
// stack path (the observation surface, rs call_stack_path 同形).
type CallStackPathEntry struct {
	FlowID string
	NodeID NodeID
}

// CallStackPath returns the suspension's call stack path: the flow_id/node_id
// chain from the innermost parent frame up to the root (observation only).
func (e *SuspendError) CallStackPath() []CallStackPathEntry {
	if e == nil {
		return nil
	}
	out := make([]CallStackPathEntry, 0, len(e.CallStack))
	for _, frame := range e.CallStack {
		flowID := ""
		if frame.Workflow != nil {
			flowID = string(frame.Workflow.ID)
		}
		out = append(out, CallStackPathEntry{FlowID: flowID, NodeID: frame.NodeID})
	}
	return out
}

// carrierTenant reads the registration tenant recorded in the carrier's
// snapshot (resume/rejection deregisters the exact namespaced key).
func (e *SuspendError) carrierTenant() (string, bool) {
	if e == nil || e.Checkpoint == nil {
		return "", false
	}
	nodes, _ := e.Checkpoint.Context["$NODE"].(map[string]any)
	state, _ := nodes[string(e.NodeID)].(map[string]any)
	tenant, ok := state["subscription_tenant"].(string)
	return tenant, ok
}

func (e *SuspendError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("flow suspended at node %s (checkpoint schema v%d)", e.NodeID, CheckpointSchemaVersion)
}

// AsSuspend returns the *SuspendError carried by err (possibly wrapped), or
// nil.
func AsSuspend(err error) *SuspendError {
	var se *SuspendError
	if errors.As(err, &se) {
		return se
	}
	return nil
}

// SuspendRejectedError is the named scope-gate rejection of nested suspend v1
// (rs FlowError::SuspendRejected / ErrorClass::SuspendRejected 同形): the
// suspension cannot be expressed as a resumable position (iteration frame /
// parallel branch frame). Fail-closed: a terminal error — never retried,
// never swallowed by any errorHandler/timeoutHandler policy, no pending state
// and no subscription left behind.
type SuspendRejectedError struct{ Message string }

func (e *SuspendRejectedError) Error() string { return e.Message }

// IsSuspendRejected reports whether err carries a scope-gate rejection
// (possibly wrapped).
func IsSuspendRejected(err error) bool {
	var sre *SuspendRejectedError
	return errors.As(err, &sre)
}

// RejectSuspendedCarrier named-rejects a suspension carrier that crossed a
// blocking frame boundary (rs suspension_carrier_or_reject 的拒绝臂同形) and
// unregisters the subscription it already registered — the subscription was
// registered before the conversion point could see the blocking frame (the
// reference ambient chain blinds the advisory gate), so deregistering here is
// what keeps the rejection orphan-free.
func RejectSuspendedCarrier(ctx context.Context, susp *SuspendError, blocker runtime.FrameKind) error {
	if susp != nil && susp.SubscriptionID != "" {
		if opts := SuspendOptionsFromContext(ctx); opts != nil && opts.Subscriptions != nil {
			tenant := opts.TenantID
			if stored, ok := susp.carrierTenant(); ok {
				tenant = stored
			}
			_ = opts.Subscriptions.Unregister(ctx, susp.SubscriptionID, tenant)
		}
	}
	return &SuspendRejectedError{Message: fmt.Sprintf(
		"suspension carrier crossed a %s frame boundary: suspension v1 supports only "+
			"child/reference call chains (subscription deregistered)", blocker)}
}

// frameChainBlocker walks the frame chain from rc to the root and returns the
// first frame kind that cannot carry a suspension (advisory fast path, rs
// frame_chain_blocker 同形). The chain walk can be blinded by the reference
// node's ambient parent link — this check only guarantees "rejected BEFORE the
// subscription registration" (no orphans); correctness is owned by the
// caller-frame-kind check at the propagation conversion points (nodes package).
func frameChainBlocker(rc *runtime.Context) (runtime.FrameKind, bool) {
	for rc != nil {
		if !rc.FrameKind.AllowsSuspension() {
			return rc.FrameKind, true
		}
		parent, ok := runtime.LookupWorkflowRuntimeContext(rc.ParentWorkflowContext)
		if !ok || parent == nil {
			break
		}
		rc = parent
	}
	return runtime.FrameKindRoot, false
}

// ResumeGuardError is plaita's ResumeGuardError: the resume carrier cannot be
// applied to the current execution state (no suspended node, non-pending
// status, unsupported resume type, stale subscription binding). The execution
// stays suspended; the carrier is not a valid decision.
type ResumeGuardError struct{ Message string }

func (e *ResumeGuardError) Error() string { return e.Message }

// ResumeError is plaita's ResumeError: the carrier was admissible but the
// node's resume handler failed.
type ResumeError struct{ Message string }

func (e *ResumeError) Error() string { return e.Message }

// Resume restores cp and applies a resume carrier to the suspended node,
// then continues the flow from that node's next pointer.
//
// Supported resume types for a suspending node are event / timeout / cancel
// (plaita `_handle_resume`); continue/retry are stepping carriers for
// non-suspended states and are rejected here with a ResumeGuardError.
//
// This is the checkpoint-carried (flat, root-flow) resume path — a nested
// suspension resumes through ResumeSuspension with the live-frame carrier.
func (f *Workflow) Resume(ctx context.Context, cp *Checkpoint, resumeType string, data map[string]any) (result any, err error) {
	if cp == nil {
		return nil, &ResumeGuardError{Message: "resume requires a checkpoint, none was provided"}
	}
	if cp.SchemaVersion <= 0 || cp.SchemaVersion > CheckpointSchemaVersion {
		return nil, &ResumeGuardError{Message: fmt.Sprintf(
			"checkpoint schema version %d is not supported by this engine (supported: 1..%d)",
			cp.SchemaVersion, CheckpointSchemaVersion)}
	}
	if err := f.Validate(); err != nil {
		return nil, err
	}

	ctx, err = RestoreRuntimeContext(ctx, cp.Context)
	if err != nil {
		return nil, err
	}
	// The engine's OnFlowEnd listeners are the host's capture point for the
	// post-resume runtime context (node results, last node). A resume has no
	// flow start, so this is the only coherent "this distributed step ended"
	// signal; a suspension is reported without an error, exactly like the
	// start path.
	defer func() {
		if susp := AsSuspend(err); susp != nil {
			triggerFlowEnd(ctx, f, nil, nil)
			return
		}
		triggerFlowEnd(ctx, f, result, err)
	}()
	rc := runtime.WorkflowRuntimeContext(ctx)

	nodeID := cp.SuspendedNode
	if nodeID == "" {
		nodeID = rc.LastNodeID
	}
	next, err := f.resumeStep(ctx, f, nodeID, resumeType, data)
	if err != nil {
		return nil, err
	}
	return f.runNodes(ctx, next)
}

// resumeStep applies a decision carrier to the suspended node: admission
// checks (node exists and is suspending, status pending, carrier type legal,
// subscription binding — plaita#221) → the node's resume → result bookkeeping
// → triggerNodeEnd → subscription deregistration. It returns the node the
// continuation enters. flow is the flow holding the suspended node (the root
// for the flat checkpoint path, the carrier's innermost frame for the nested
// path).
func (f *Workflow) resumeStep(ctx context.Context, flow *Workflow, nodeID NodeID, resumeType string, data map[string]any) (Node, error) {
	if nodeID == "" {
		return nil, &ResumeGuardError{Message: "no suspended node found for resume"}
	}
	node, err := flow.findNodeByID(nodeID)
	if err != nil {
		return nil, &ResumeGuardError{Message: fmt.Sprintf("suspended node %q is not part of flow %q", nodeID, flow.ID)}
	}
	suspender, ok := node.(Suspender)
	if !ok || !suspender.IsSuspending() {
		return nil, &ResumeGuardError{Message: fmt.Sprintf("node %s is not suspending", nodeID)}
	}
	resumer, ok := node.(Resumer)
	if !ok {
		return nil, &ResumeGuardError{Message: fmt.Sprintf("suspending node %s has no resume handler", nodeID)}
	}
	switch resumeType {
	case ResumeEvent, ResumeTimeout, ResumeCancel:
	case ResumeContinue, ResumeRetry:
		return nil, &ResumeGuardError{Message: fmt.Sprintf(
			"resume_type=%q cannot wake a suspending node; use event/timeout/cancel", resumeType)}
	default:
		return nil, &ResumeGuardError{Message: fmt.Sprintf("unsupported resume type %q", resumeType)}
	}

	rc := runtime.WorkflowRuntimeContext(ctx)
	pending := nodeStateResult(rc, nodeID)
	if status, _ := pending["status"].(string); status != "pending" {
		return nil, &ResumeGuardError{Message: fmt.Sprintf(
			"suspending node %s is not in pending status: %q", nodeID, status)}
	}
	if err := checkResumeBinding(pending, resumeType, data); err != nil {
		return nil, &ResumeGuardError{Message: fmt.Sprintf("stale %s resume for node %s: %v; execution stays suspended",
			resumeType, nodeID, err)}
	}
	cleanData := stripResumeBindingKey(data)

	nodeResult, err := resumer.Resume(ctx, resumeType, cleanData)
	if err != nil {
		return nil, &ResumeError{Message: fmt.Sprintf("resume node %s: %v", nodeID, err)}
	}
	if nodeResult == nil {
		return nil, &ResumeError{Message: fmt.Sprintf("resume node %s returned no result", nodeID)}
	}
	rc.WithExecuteNodeResult(nodeID, nodeResult.Result)
	rc.WithLastNodeID(nodeID)
	// plaita's _handle_resume reports on_node_end for the resumed node (and
	// does NOT re-run on_node_start). The worker uses this to close the
	// suspended node's timing entry and append its node_end timeline event.
	triggerNodeEnd(ctx, flow, node, nodeResult.Result, nil, nil)

	// The wake-up subscription is consumed: deregister it (plaita
	// _unregister_suspended_subscription; a failure only warns and never
	// changes the resume outcome).
	if subID, _ := pending["subscription_id"].(string); subID != "" {
		if opts := SuspendOptionsFromContext(ctx); opts != nil && opts.Subscriptions != nil {
			tenant := opts.TenantID
			if stored, ok := pending["subscription_tenant"].(string); ok {
				tenant = stored
			}
			_ = opts.Subscriptions.Unregister(ctx, subID, tenant)
		}
	}

	if nodeResult.NextNode == "" {
		return nil, &ResumeError{Message: fmt.Sprintf("resume node %s has no next node", nodeID)}
	}
	next, err := flow.findNodeByID(nodeResult.NextNode)
	if err != nil {
		return nil, fmt.Errorf("resume node %s: next node %q: %w", nodeID, nodeResult.NextNode, err)
	}
	return next, nil
}

// ResumeSuspension is the nested-suspend resume entry (嵌套挂起 v1): it applies
// a decision carrier to the suspended node carried by susp and walks the live
// call stack back to the root — decision step (the suspended node's resume)
// → the inner flow continues → each completing flow returns its value to its
// parent frame ($NODE[child]=value + outputType validation + continue at
// child.next, a fresh per-frame budget each step) → until the root flow
// completes. A re-suspension along the way merges the call stacks (new, deeper
// frames first) and re-parks the execution.
//
// A carrier without live frames (a deserialized root-flow suspension) falls
// back to the flat checkpoint path (Resume). The workflow instance's reentry
// guard applies (same contract as Execute/ResumeFrom).
func (f *Workflow) ResumeSuspension(ctx context.Context, susp *SuspendError, resumeType string, data map[string]any) (result any, err error) {
	if susp == nil {
		return nil, &ResumeGuardError{Message: "resume requires a suspension carrier, none was provided"}
	}
	if susp.Flow == nil || susp.Ctx == nil || susp.Checkpoint == nil {
		// No live frames on the carrier: the flat checkpoint path (a
		// root-flow suspension resurrected by a host).
		return f.Resume(ctx, susp.Checkpoint, resumeType, data)
	}
	for _, frame := range susp.CallStack {
		if frame.Workflow == nil || frame.Ctx == nil {
			return nil, &ResumeGuardError{Message: "suspension carrier has an incomplete call frame"}
		}
	}
	if !f.running.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("workflow %q: %w", f.ID, ErrWorkflowReentry)
	}
	defer f.running.Store(false)

	if err := f.Validate(); err != nil {
		return nil, err
	}
	if err := susp.Flow.Validate(); err != nil {
		return nil, err
	}

	// Same end-of-step contract as Resume: the root's FlowEnd is emitted here
	// (a suspension without an error); the inner frames' FlowEnds are emitted
	// by the continuation loop below. A resume session has no FlowStart.
	defer func() {
		if susp := AsSuspend(err); susp != nil {
			triggerFlowEnd(ctx, f, nil, nil)
			return
		}
		triggerFlowEnd(ctx, f, result, err)
	}()
	return f.resumeNested(ctx, susp, resumeType, data)
}

// resumeNested is the resume session body: the decision step on the carrier's
// innermost frame followed by the continuation loop (return steps up the call
// stack / re-suspension stack merge / per-frame finalize chain).
func (f *Workflow) resumeNested(ctx context.Context, susp *SuspendError, resumeType string, data map[string]any) (any, error) {
	// Per-frame budgets accumulate one cancel per frame; all are released when
	// the session ends.
	cancels := make([]context.CancelFunc, 0, len(susp.CallStack)+1)
	defer func() {
		for _, cancel := range cancels {
			cancel()
		}
	}()

	// 决策步作用域 = 挂起帧：活运行态 + 每帧新鲜预算（程式一④——挂起租约已
	// 结束，恢复会话以 flow 自有 timeout 重挂；go 引擎没有缺省执行预算常量，
	// TODO(worker): worker 层执行级缺省预算口径（plaita
	// DEFAULT_EXECUTION_DEADLINE 的对应物）落地后在此接入，引擎以 flow.Timeout
	// 为准）。
	frameCtx, cancel := f.withFrameBudget(runtime.WithWorkflowRuntimeContext(ctx, susp.Ctx), susp.Ctx, susp.Flow)
	if cancel != nil {
		cancels = append(cancels, cancel)
	}

	next, err := f.resumeStep(frameCtx, susp.Flow, susp.NodeID, resumeType, data)
	if err != nil {
		return nil, err
	}

	// -- 续跑循环：从最内层帧开始推进；每层 flow 完成时作为「调用返回」把值
	//    交给父帧（$NODE[child]=返回值 + outputType 校验 + 续跑 child.next），
	//    直到根流程完成；续跑途中再次挂起则合并调用栈重新入挂起态。
	currentFlow := susp.Flow
	currentCtx := frameCtx
	stack := susp.CallStack

	for {
		contResult, contErr := currentFlow.runNodes(currentCtx, next)
		if susp2 := AsSuspend(contErr); susp2 != nil {
			// 再挂起：本帧 FlowEnd(nil,nil)（rs finalize 同形——再深帧的
			// FlowEnd 已由传播路径各 flow 的 Execute 发出）。新现场（更深）
			// 的调用栈在前，本会话剩余栈在后拼接。
			if currentFlow != f {
				triggerFlowEnd(currentCtx, currentFlow, nil, nil)
			}
			susp2.CallStack = append(susp2.CallStack, stack...)
			return nil, susp2
		}
		if contErr != nil {
			// 帧收尾链（rs advance_and_finalize 对应面）：本帧失败与首跑同链
			// 而非绕过——FlowEnd 必发；SuspendRejected 终态直抛；节点自有
			// 超时按 abort 传播时仍咨询本帧 timeoutHandler（与 Execute 的流级
			// 尾链同形）；其余错误终态直抛。
			if currentFlow != f {
				triggerFlowEnd(currentCtx, currentFlow, contResult, contErr)
			}
			if IsSuspendRejected(contErr) {
				return nil, contErr
			}
			if contextTimeoutError(contErr) {
				contResult, contErr = currentFlow.TimeoutHandlePolicy.Handle(contErr)
				if contErr != nil {
					return nil, contErr
				}
				// continue/continue-with：以默认值视为本帧完成 → 走返回步
				//（FlowEnd 已随错误发射；FlowEnd 可重复、最后一次为准）。
			} else {
				return nil, contErr
			}
		} else if currentFlow != f {
			// 本帧完成：FlowEnd(result)（根帧的 FlowEnd 由外层 defer 发——
			// 恢复方向无 FlowStart，FlowEnd 可重复、最后一次为准）。
			triggerFlowEnd(currentCtx, currentFlow, contResult, nil)
		}

		if len(stack) == 0 {
			return contResult, nil
		}
		// 返回步：按传播序消费最深帧（index 0）——值回填父帧后从 child.next
		// 续跑父 flow。
		frame := stack[0]
		stack = stack[1:]
		childNode, nerr := frame.Workflow.findNodeByID(frame.NodeID)
		if nerr != nil {
			return nil, &ResumeError{Message: fmt.Sprintf("call frame node %q not found in flow %q",
				frame.NodeID, frame.Workflow.ID)}
		}
		// 返回步同款 outputType 校验（rs validate_child_return_output 同形）。
		if typer, ok := childNode.(childOutputTyper); ok {
			outputType := typer.GetOutputType()
			if verr := outputType.ValidateValue(contResult); verr != nil {
				return nil, &ResumeError{Message: fmt.Sprintf("call frame node %s: %v", frame.NodeID, verr)}
			}
		}
		// 返回步同样新鲜发放预算（帧的原租约已结束）。
		frameCtx, cancel := f.withFrameBudget(runtime.WithWorkflowRuntimeContext(ctx, frame.Ctx), frame.Ctx, frame.Workflow)
		if cancel != nil {
			cancels = append(cancels, cancel)
		}
		frameRC := runtime.WorkflowRuntimeContext(frameCtx)
		// 回填细则：$NODE[child]=v、$LAST_NODE=child、不写 $ERROR。
		frameRC.WithExecuteNodeResult(childNode.GetID(), contResult)
		frameRC.WithLastNodeID(childNode.GetID())
		triggerNodeEnd(frameCtx, frame.Workflow, childNode, contResult, nil, nil)
		currentFlow = frame.Workflow
		currentCtx = frameCtx
		next, nerr = currentFlow.findNodeByID(childNode.NextNode())
		if nerr != nil {
			return nil, &ResumeError{Message: fmt.Sprintf("resume node %s: next node %q: %v",
				childNode.GetID(), childNode.NextNode(), nerr)}
		}
	}
}

// withFrameBudget issues one frame's fresh resume-session budget (程式一④):
// with a flow-level timeout it re-wraps ctx with the deadline and refreshes
// rc.FlowDeadline (the flowBudgetBinding attribution input); without one the
// frame runs unbudgeted — TODO(worker): the worker-level default execution
// budget (plaita DEFAULT_EXECUTION_DEADLINE counterpart) plugs in here; the
// engine treats flow.Timeout as authoritative.
func (f *Workflow) withFrameBudget(ctx context.Context, rc *runtime.Context, flow *Workflow) (context.Context, context.CancelFunc) {
	if flow == nil || flow.Timeout.IsZero() {
		return ctx, nil
	}
	d := flow.Timeout.TimeDuration()
	if rc != nil {
		rc.FlowDeadline = time.Now().Add(d)
	}
	return context.WithTimeout(ctx, d)
}

// nodeStateResult returns the stored result map of one node.
func nodeStateResult(rc *runtime.Context, nodeID NodeID) map[string]any {
	if rc == nil || rc.ExecuteNodeResults == nil {
		return map[string]any{}
	}
	value, ok := rc.ExecuteNodeResults[string(nodeID)]
	if !ok {
		return map[string]any{}
	}
	if m, ok := value.(map[string]any); ok {
		return m
	}
	return map[string]any{}
}

// checkResumeBinding ports plaita#221: an event/timeout carrier must be bound
// to the subscription the suspended node registered. A checkpoint without a
// subscription id (host-driven suspend or legacy snapshot) has nothing to
// compare and is allowed.
func checkResumeBinding(pending map[string]any, resumeType string, data map[string]any) error {
	if resumeType != ResumeEvent && resumeType != ResumeTimeout {
		return nil
	}
	expected, _ := pending["subscription_id"].(string)
	if expected == "" {
		return nil
	}
	got, _ := data["subscription_id"].(string)
	if got == "" {
		got, _ = data[resumeBindingKey].(string)
	}
	if got != expected {
		return fmt.Errorf("resume carrier is not bound to the suspended subscription (expected %q, got %q)",
			expected, got)
	}
	if resumeType == ResumeEvent {
		wantType, _ := pending["event_type"].(string)
		gotType, _ := data["event_type"].(string)
		if wantType != "" && gotType != "" && wantType != gotType {
			return fmt.Errorf("event type mismatch (suspended on %q, carrier carries %q)", wantType, gotType)
		}
	}
	return nil
}

func stripResumeBindingKey(data map[string]any) map[string]any {
	if data == nil {
		return nil
	}
	if _, ok := data[resumeBindingKey]; !ok {
		return data
	}
	clean := make(map[string]any, len(data))
	for k, v := range data {
		if k == resumeBindingKey {
			continue
		}
		clean[k] = v
	}
	return clean
}

// suspend parks the flow at node: it registers the node's wake-up
// subscription, snapshots the execution context, persists the checkpoint and
// surfaces a *SuspendError carrying the live frames (nested suspend v1).
func (f *Workflow) suspend(ctx context.Context, node Node, result *ExecuteResult) error {
	rc := runtime.WorkflowRuntimeContext(ctx)
	// 作用域闸（嵌套挂起 v1）：child/reference 调用链允许（恢复位置由载体的
	// 活帧调用栈表达）；迭代帧/分支帧具名拒绝（恢复需要迭代状态快照，v2）。
	// 本检查是注册订阅**之前**的快路径（防孤儿订阅）；reference 帧的 ambient
	// 父链会遮蔽链行走，正确性由传播转换点的调用方帧 kind 检查兜底（nodes 包，
	// 越界载体在转换点具名拒并注销已注册订阅）。
	if blocker, blocked := frameChainBlocker(rc); blocked {
		return &SuspendRejectedError{Message: fmt.Sprintf(
			"node %s suspends inside a %s frame: suspension v1 supports only child/reference "+
				"call chains (iteration-state snapshotting is a future milestone; see docs/PROGRAM.md)",
			node.GetID(), blocker)}
	}
	opts := SuspendOptionsFromContext(ctx)

	state, _ := result.Result.(map[string]any)
	if state == nil {
		state = map[string]any{}
	}
	if status, _ := state["status"].(string); status != "pending" {
		return fmt.Errorf("suspending node %s returned status %q, want pending", node.GetID(), status)
	}

	subscriptionID := ""
	if desc, ok := node.(SuspendDescriptor); ok {
		spec, err := desc.SuspendSpec(ctx, result)
		if err != nil {
			return fmt.Errorf("suspend spec for node %s: %w", node.GetID(), err)
		}
		if spec != nil && spec.Subscription != nil {
			// plaita refuses to suspend without an active subscription:
			// otherwise the execution sits suspended with no wake-up source.
			if opts == nil || opts.Subscriptions == nil {
				return fmt.Errorf("node %s declares an event subscription but no subscription registry is configured", node.GetID())
			}
			req := *spec.Subscription
			if req.NodeID == "" {
				req.NodeID = string(node.GetID())
			}
			if req.CorrelationID == "" {
				req.CorrelationID = opts.ExecutionID
			}
			if req.FlowID == "" {
				req.FlowID = opts.FlowID
			}
			if req.FlowID == "" {
				req.FlowID = string(f.ID)
			}
			if req.TenantID == "" {
				req.TenantID = opts.TenantID
			}
			if err := opts.Subscriptions.Register(ctx, &req); err != nil {
				return fmt.Errorf("event subscription failed for node %s; refusing to suspend without an active subscription: %w",
					node.GetID(), err)
			}
			if req.SubscriptionID == "" {
				return fmt.Errorf("subscription registry did not assign an id for node %s", node.GetID())
			}
			subscriptionID = req.SubscriptionID
			state["subscription_id"] = subscriptionID
			// The registration tenant travels with the snapshot so resume
			// deregisters the exact namespaced key even when the carrier's
			// tenant routing differs from the start task's.
			state["subscription_tenant"] = req.TenantID
			rc.WithExecuteNodeResult(node.GetID(), state)
		}
	}

	cp, err := f.buildCheckpoint(ctx, node.GetID(), result.NextNode, state)
	if err != nil {
		if subscriptionID != "" && opts != nil && opts.Subscriptions != nil {
			_ = opts.Subscriptions.Unregister(ctx, subscriptionID, opts.TenantID)
		}
		return err
	}
	if opts != nil && opts.Store != nil {
		if err := opts.Store.SaveCheckpoint(ctx, cp); err != nil {
			if subscriptionID != "" && opts.Subscriptions != nil {
				_ = opts.Subscriptions.Unregister(ctx, subscriptionID, opts.TenantID)
			}
			return fmt.Errorf("checkpoint persist for node %s: %w", node.GetID(), err)
		}
	}
	return &SuspendError{
		NodeID:         node.GetID(),
		Checkpoint:     cp,
		SubscriptionID: subscriptionID,
		Flow:           f,
		Ctx:            rc,
	}
}

// buildCheckpoint snapshots the runtime context at the suspend boundary.
func (f *Workflow) buildCheckpoint(ctx context.Context, nodeID, next NodeID, state map[string]any) (*Checkpoint, error) {
	opts := SuspendOptionsFromContext(ctx)
	snapCtx, err := SnapshotRuntimeContext(ctx, opts)
	if err != nil {
		return nil, err
	}
	cp := &Checkpoint{
		SchemaVersion: CheckpointSchemaVersion,
		Kind:          CheckpointKindSuspend,
		SuspendedNode: nodeID,
		NextNode:      next,
		ResumeType:    ResumeEvent,
		Context:       snapCtx,
		CreatedAt:     isoNow(),
	}
	if opts != nil {
		cp.ExecutionID = opts.ExecutionID
		cp.TenantID = opts.TenantID
		cp.FlowID = opts.FlowID
	}
	if cp.FlowID == "" {
		cp.FlowID = string(f.ID)
	}
	if state != nil {
		if rt, ok := state["resume_type"].(string); ok && rt != "" {
			cp.ResumeType = rt
		}
	}
	return cp, nil
}

// --- per-step checkpoints + mid-flow entry (#32) ---------------------------
//
// M6 checkpoints only suspend boundaries; a continue/retry carrier advancing
// a RUNNING execution had no engine entry point and re-entered at the flow
// start, replaying every completed node (non-idempotent nodes repeated their
// side effects). Two additions close that gap:
//
//   - stepCheckpoint: after each completed root-flow node (per the
//     SuspendOptions.StepCheckpoint interval) the engine snapshots the
//     execution context into a kind=per-step checkpoint whose next_node is
//     the upcoming node — the resumable position of a live execution;
//   - ResumeFrom: restore such a checkpoint and continue at its next node —
//     only the remaining nodes execute.

// stepCheckpoint is the per-step checkpoint hook runNodes calls after each
// successfully completed node. step is the 1-based ordinal of completed nodes
// within the current stepping session (a resumed session restarts the count —
// the interval is a write-amplification knob, not a correctness knob).
// Root-flow positions only: ResumeFrom re-enters the root flow, so a nested
// (child/loop/parallel) position cannot be expressed and is not checkpointed
// (same scope rule as suspend v1.5).
func (f *Workflow) stepCheckpoint(ctx context.Context, completed Node, next NodeID, step int) error {
	opts := SuspendOptionsFromContext(ctx)
	if opts == nil || opts.Store == nil || opts.StepCheckpoint <= 0 {
		return nil
	}
	if step%opts.StepCheckpoint != 0 {
		return nil
	}
	rc := runtime.WorkflowRuntimeContext(ctx)
	if rc.ParentWorkflowContext != nil {
		return nil
	}
	snap, err := SnapshotRuntimeContext(ctx, opts)
	if err != nil {
		return err
	}
	flowID := opts.FlowID
	if flowID == "" {
		flowID = string(f.ID)
	}
	cp := &Checkpoint{
		SchemaVersion: CheckpointSchemaVersion,
		Kind:          CheckpointKindPerStep,
		ExecutionID:   opts.ExecutionID,
		TenantID:      opts.TenantID,
		FlowID:        flowID,
		SuspendedNode: completed.GetID(),
		NextNode:      next,
		ResumeType:    ResumeContinue,
		Context:       snap,
		CreatedAt:     isoNow(),
	}
	if err := opts.Store.SaveCheckpoint(ctx, cp); err != nil {
		return fmt.Errorf("per-step checkpoint after node %s: %w", completed.GetID(), err)
	}
	return nil
}

// ResumeFrom is the mid-flow entry (#32): it restores cp (a kind=per-step
// checkpoint written by stepCheckpoint, or an equivalent failure-boundary
// snapshot) and continues the flow at nextNodeID — falling back to the
// checkpoint's own next_node when nextNodeID is empty. Only the remaining
// nodes execute: the completed predecessors carried by the snapshot's $NODE
// are never replayed, so a non-idempotent node's side effect happens exactly
// once.
//
// The sibling entries: Workflow.Execute starts a flow, Workflow.Resume wakes
// a suspending node with a decision carrier (event/timeout/cancel), and
// ResumeFrom continues a live execution from a recorded position. A
// kind=suspend checkpoint is rejected (use Resume); a missing next node is a
// ResumeGuardError, not a silent restart.
//
// Like Execute, ResumeFrom holds the instance's reentry guard: a concurrent
// run on the same Workflow object is rejected with ErrWorkflowReentry.
func (f *Workflow) ResumeFrom(ctx context.Context, cp *Checkpoint, nextNodeID NodeID) (result any, err error) {
	if !f.running.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("workflow %q: %w", f.ID, ErrWorkflowReentry)
	}
	defer f.running.Store(false)

	if cp == nil {
		return nil, &ResumeGuardError{Message: "mid-flow resume requires a checkpoint, none was provided"}
	}
	if cp.Kind == CheckpointKindSuspend {
		return nil, &ResumeGuardError{Message: fmt.Sprintf(
			"checkpoint is parked at suspending node %s; use Workflow.Resume with a decision carrier",
			cp.SuspendedNode)}
	}
	if cp.SchemaVersion <= 0 || cp.SchemaVersion > CheckpointSchemaVersion {
		return nil, &ResumeGuardError{Message: fmt.Sprintf(
			"checkpoint schema version %d is not supported by this engine (supported: 1..%d)",
			cp.SchemaVersion, CheckpointSchemaVersion)}
	}
	if err := f.Validate(); err != nil {
		return nil, err
	}

	ctx, err = RestoreRuntimeContext(ctx, cp.Context)
	if err != nil {
		return nil, err
	}
	// Same end-of-step contract as Resume: the host's OnFlowEnd listener is
	// the capture point for the post-continuation runtime context; a fresh
	// suspension inside the remaining nodes is reported without an error.
	defer func() {
		if susp := AsSuspend(err); susp != nil {
			triggerFlowEnd(ctx, f, nil, nil)
			return
		}
		triggerFlowEnd(ctx, f, result, err)
	}()

	nodeID := nextNodeID
	if nodeID == "" {
		nodeID = cp.NextNode
	}
	if nodeID == "" {
		return nil, &ResumeGuardError{
			Message: "mid-flow resume has no entry node: pass nextNodeID or a checkpoint with next_node"}
	}
	node, err := f.findNodeByID(nodeID)
	if err != nil {
		return nil, &ResumeGuardError{Message: fmt.Sprintf(
			"mid-flow entry node %q is not part of flow %q", nodeID, f.ID)}
	}
	return f.runNodes(ctx, node)
}
