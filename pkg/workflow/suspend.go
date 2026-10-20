package workflow

import (
	"context"
	"errors"
	"fmt"

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
// Scope (deliberate, fail-closed): only the ROOT flow can suspend. A
// suspending node inside a child/loop/parallel sub-flow returns an explicit
// engine error instead of parking a nested execution whose resume position
// the checkpoint cannot express.

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
	if nodeID == "" {
		return nil, &ResumeGuardError{Message: "no suspended node found for resume"}
	}
	node, err := f.findNodeByID(nodeID)
	if err != nil {
		return nil, &ResumeGuardError{Message: fmt.Sprintf("suspended node %q is not part of flow %q", nodeID, f.ID)}
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
	triggerNodeEnd(ctx, f, node, nodeResult.Result, nil, nil)

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
	next, err := f.findNodeByID(nodeResult.NextNode)
	if err != nil {
		return nil, fmt.Errorf("resume node %s: next node %q: %w", nodeID, nodeResult.NextNode, err)
	}
	return f.runNodes(ctx, next)
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
// surfaces a *SuspendError.
func (f *Workflow) suspend(ctx context.Context, node Node, result *ExecuteResult) error {
	rc := runtime.WorkflowRuntimeContext(ctx)
	if rc.ParentWorkflowContext != nil {
		return fmt.Errorf("node %s suspended inside a nested flow: checkpoint v1.5 only supports suspend/resume in the root flow",
			node.GetID())
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
	return &SuspendError{NodeID: node.GetID(), Checkpoint: cp, SubscriptionID: subscriptionID}
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
