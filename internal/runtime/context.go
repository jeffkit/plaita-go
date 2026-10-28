package runtime

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jinzhu/copier"

	"github.com/jeffkit/plaita-go/internal/types"
)

type contextKey string

const (
	workflowRuntimeContextContextKey contextKey = "workflowRuntimeContext"
)

// FrameKind is the execution frame type — the scope-gate input of nested
// suspend v1 (rs FrameKind 同形). Suspension may only pierce Root / ChildCall /
// ReferenceCall chains; a suspension on any LoopIteration / ParallelBranch
// frame is named-rejected (its resume position needs iteration-state
// snapshotting, a future milestone).
type FrameKind int

const (
	// FrameKindRoot is the zero value: a root execution frame (also the
	// default for contexts built before this field existed).
	FrameKindRoot FrameKind = iota
	// FrameKindChildCall is a child-node sub-flow execution frame.
	FrameKindChildCall
	// FrameKindReferenceCall is a reference-node sub-flow execution frame
	// (isolated context).
	FrameKindReferenceCall
	// FrameKindLoopIteration is a loop/map/filter/find/reduce/while
	// iteration-body frame.
	FrameKindLoopIteration
	// FrameKindParallelBranch is a parallel branch frame.
	FrameKindParallelBranch
)

// String renders the frame kind the way the engine's rejection messages name
// it (rs FrameKind::as_str alignment).
func (k FrameKind) String() string {
	switch k {
	case FrameKindChildCall:
		return "child"
	case FrameKindReferenceCall:
		return "reference"
	case FrameKindLoopIteration:
		return "loop-iteration"
	case FrameKindParallelBranch:
		return "parallel-branch"
	default:
		return "root"
	}
}

// AllowsSuspension reports whether this frame may carry a suspension piercing
// through it (the call-chain frames).
func (k FrameKind) AllowsSuspension() bool {
	return k == FrameKindRoot || k == FrameKindChildCall || k == FrameKindReferenceCall
}

// Context represents the context of workflow execution.
//
// Concurrency: a Context is shared between a flow and its background
// parallel branches (the branch clones its own per-execution state but
// resolves $PARENT against the source execution), so every field is guarded
// by mu. Readers that need to iterate a map must take a snapshot accessor
// (GetExecuteNodeResults / GetGlobalVariables / …) — the getters copy under
// the read lock, callers never iterate the live map.
type Context struct {
	mu                    sync.RWMutex
	WorkflowInput         any             `json:"input"`
	ExecuteNodeResults    map[string]any  `json:"nodes"`
	ParentWorkflowContext context.Context `json:"parent"`
	CurrentNodeContext    map[string]any  `json:"current"`
	LastNodeID            types.NodeID    `json:"last_node_id"`
	// LastBranch is the branch target taken by the most recent branching
	// node (plaita runner: context.last_branch = result for node.branching
	// nodes; nil until one has run). Read by expressions as $BRANCH.
	LastBranch any `json:"last_branch,omitempty"`
	// FlowID is the id of the flow this execution context belongs to
	// (plaita setup_flow: $FLOW_ID). A child flow without its own id keeps
	// the parent's value (plaita child fallback). Read by expressions as
	// $FLOW_ID / $FLOW.
	FlowID string `json:"flow_id,omitempty"`
	// ExecutionID is the distributed execution id (plaita setup_flow:
	// $EXECUTION_ID). The root execution gets it from the worker's ctx
	// injection (runtime.WithExecutionID) or mints a uuid; child/loop/
	// parallel sub-executions inherit the root's value, so every level of
	// one execution correlates under the same id.
	ExecutionID       string         `json:"execution_id,omitempty"`
	ExecuteNodeErrors map[string]any `json:"errors"`
	EnvVariables      map[string]any `json:"env_variables"`
	GlobalVariables   map[string]any `json:"global_variables"`

	// FlowDeadline is the deadline imposed by the current flow's own timeout
	// budget. It stays zero for flows without an explicit timeout. It is used
	// to attribute a context deadline error to the flow budget instead of the
	// node budget (plaita FLOW_ERROR vs NODE_ERROR); it is runtime bookkeeping
	// and never serialized.
	FlowDeadline time.Time `json:"-"`
	// FrameKind is the execution frame type of this context (nested suspend
	// v1 scope gate). Seeded by the sub-flow entrypoints (child/reference/
	// collection iteration/parallel branch); a root execution keeps the zero
	// value FrameKindRoot. Runtime bookkeeping, never serialized.
	FrameKind FrameKind `json:"-"`
	// FramePath / FrameKey carry the span-model frame state (plaita#344):
	// the frame node-id chain root→leaf and the in-frame qualifier
	// (loop-family iteration index / parallel branch name; nil at the root
	// and for child/reference frames). Seeded alongside FrameKind by the
	// same entrypoints. Runtime bookkeeping, never serialized.
	FramePath []string `json:"-"`
	FrameKey  any      `json:"-"`
}

func newContext() *Context {
	return &Context{
		WorkflowInput:      make(map[string]any),
		ExecuteNodeResults: make(map[string]any),
		CurrentNodeContext: make(map[string]any),
		ExecuteNodeErrors:  make(map[string]any),
		EnvVariables:       make(map[string]any),
		GlobalVariables:    make(map[string]any),
	}
}

// NewContext returns a fresh workflow runtime context with empty per-execution
// state ($INPUT/$NODE/$ERROR/current node). Use NewExecutionRuntimeContext to
// seed one with child-flow linkage.
func NewContext() *Context {
	return newContext()
}

// WithWorkflowInput sets the workflow input in workflow runtime context and return new context.
func WithWorkflowInput(ctx context.Context, input any) context.Context {
	runtimeCtx := WorkflowRuntimeContext(ctx)
	runtimeCtx.WithWorkflowInput(input)
	return WithWorkflowRuntimeContext(ctx, runtimeCtx)
}

// WithWorkflowInput sets the workflow input in workflow runtime context.
func (c *Context) WithWorkflowInput(input any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.WorkflowInput = input
}

// GetWorkflowInput returns the workflow input from workflow runtime context.
func (c *Context) GetWorkflowInput() any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.WorkflowInput
}

// WithEnvVariablesContext populates the env variables from the process
// environment. With an allowlist, only the listed keys are copied (plaita
// 0.5.0 allowlist model): keys missing from the environment are silently
// dropped (typos must not crash the flow) and values keep everything after
// the first '=' (SplitN, not Split).
//
// The zero-argument legacy form copies the FULL process environment — the
// workflow engine never uses it (Workflow.Execute assembles $ENV through the
// ExposeEnv allowlist); it exists only for callers that explicitly want the
// whole environment.
func (c *Context) WithEnvVariablesContext(allowed ...string) {
	env := map[string]any{}
	if len(allowed) > 0 {
		want := make(map[string]struct{}, len(allowed))
		for _, k := range allowed {
			want[k] = struct{}{}
		}
		for _, entry := range os.Environ() {
			k, v, ok := strings.Cut(entry, "=")
			if !ok {
				continue
			}
			if _, hit := want[k]; hit {
				env[k] = v
			}
		}
	} else {
		for _, entry := range os.Environ() {
			k, v, ok := strings.Cut(entry, "=")
			if !ok {
				continue
			}
			env[k] = v
		}
	}
	c.mu.Lock()
	c.EnvVariables = env
	c.mu.Unlock()
}

// WithEnvVariables replaces the env variables in context with the given map.
// It is the inheritance path: a child flow without its own exposeEnv
// declaration sees exactly the (already filtered) env of its parent
// execution (plaita#141).
func (c *Context) WithEnvVariables(variables map[string]any) {
	env := make(map[string]any, len(variables))
	for k, v := range variables {
		env[k] = v
	}
	c.mu.Lock()
	c.EnvVariables = env
	c.mu.Unlock()
}

// WithParentWorkflowContext sets the parent workflow context in context.
func (c *Context) WithParentWorkflowContext(ctx context.Context) {
	c.ParentWorkflowContext = ctx
}

// GetParentWorkflowContext returns the parent workflow context from context.
func (c *Context) GetParentWorkflowContext() context.Context {
	return c.ParentWorkflowContext
}

// WithExecuteNodeResult sets the execute node result in context.
func (c *Context) WithExecuteNodeResult(nodeID types.NodeID, result any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ExecuteNodeResults == nil {
		c.ExecuteNodeResults = make(map[string]any)
	}

	c.ExecuteNodeResults[string(nodeID)] = result
}

// GetExecuteNodeResults returns a snapshot of the executed node results
// ($NODE). The copy is taken under the read lock: callers may iterate it
// (expression evaluation) while the owning flow keeps writing.
func (c *Context) GetExecuteNodeResults() map[string]any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.ExecuteNodeResults == nil {
		return nil
	}
	out := make(map[string]any, len(c.ExecuteNodeResults))
	for k, v := range c.ExecuteNodeResults {
		out[k] = v
	}
	return out
}

// WithExecuteNodeError sets the execute node error in context.
func (c *Context) WithExecuteNodeError(nodeID types.NodeID, err any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ExecuteNodeErrors == nil {
		c.ExecuteNodeErrors = make(map[string]any)
	}
	c.ExecuteNodeErrors[string(nodeID)] = err
}

// GetExecuteNodeError returns the execute node error from context.
func (c *Context) GetExecuteNodeError(nodeID types.NodeID) any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ExecuteNodeErrors[string(nodeID)]
}

// GetExecuteNodeErrors returns a snapshot of the executed node errors
// ($ERROR).
func (c *Context) GetExecuteNodeErrors() map[string]any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.ExecuteNodeErrors == nil {
		return nil
	}
	out := make(map[string]any, len(c.ExecuteNodeErrors))
	for k, v := range c.ExecuteNodeErrors {
		out[k] = v
	}
	return out
}

// GetExecuteNodeResult returns the execute node result from context.
func (c *Context) GetExecuteNodeResult(nodeID types.NodeID) any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ExecuteNodeResults[string(nodeID)]
}

// WithCurrentNodeContext sets the current node context in context.
func (c *Context) WithCurrentNodeContext(key string, nodeContext any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.CurrentNodeContext == nil {
		c.CurrentNodeContext = make(map[string]any)
	}
	c.CurrentNodeContext[key] = nodeContext
}

// ResetCurrentNodeContext resets the current node context in context.
func (c *Context) ResetCurrentNodeContext() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.CurrentNodeContext = make(map[string]any)
}

// GetCurrentNodeContext returns the current node context from context.
func (c *Context) GetCurrentNodeContext(key string) any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.CurrentNodeContext[key]
}

// GetCurrentNodeContexts returns a snapshot of the current node context
// (per-node extra results such as a switch's matchInfos).
func (c *Context) GetCurrentNodeContexts() map[string]any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.CurrentNodeContext == nil {
		return nil
	}
	out := make(map[string]any, len(c.CurrentNodeContext))
	for k, v := range c.CurrentNodeContext {
		out[k] = v
	}
	return out
}

// WithLastNodeID sets the last node id in workflow runtime context.
func (c *Context) WithLastNodeID(nodeID types.NodeID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.LastNodeID = nodeID
}

// GetLastNodeID returns the last node id from workflow runtime context.
func (c *Context) GetLastNodeID() types.NodeID {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.LastNodeID
}

// WithLastBranch records the branch target taken by the most recent
// branching node (plaita runner: context.last_branch).
func (c *Context) WithLastBranch(branch any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.LastBranch = branch
}

// GetLastBranch returns the branch target taken by the most recent branching
// node ($BRANCH); nil until a branching node has run (plaita None).
func (c *Context) GetLastBranch() any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.LastBranch
}

// WithFlowID records the flow id of the execution this context belongs to
// (plaita setup_flow: $FLOW_ID).
func (c *Context) WithFlowID(flowID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.FlowID = flowID
}

// GetFlowID returns the flow id ($FLOW_ID / $FLOW); empty before the flow
// set it (plaita: the root always has one, a child falls back to its parent).
func (c *Context) GetFlowID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.FlowID
}

// WithExecutionID records the distributed execution id ($EXECUTION_ID).
func (c *Context) WithExecutionID(executionID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ExecutionID = executionID
}

// GetExecutionID returns the distributed execution id ($EXECUTION_ID).
func (c *Context) GetExecutionID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ExecutionID
}

// WithGlobalVariables sets the global variables in context.
func (c *Context) WithGlobalVariables(variables map[string]any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.GlobalVariables == nil {
		c.GlobalVariables = make(map[string]any)
	}
	// merge global variables
	for k, v := range variables {
		c.GlobalVariables[k] = v
	}
}

// WithGlobalVariable sets the global variable in context.
func (c *Context) WithGlobalVariable(key string, value any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.GlobalVariables == nil {
		c.GlobalVariables = make(map[string]any)
	}
	c.GlobalVariables[key] = value
}

// GetGlobalVariable returns the global variables from context.
func (c *Context) GetGlobalVariable(key string) any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.GlobalVariables[key]
}

// GetGlobalVariables returns a snapshot of the global variables from context
// ($GLOBAL). The copy keeps callers safe while another execution path merges
// globals; the historical live-map return had no mutating caller.
func (c *Context) GetGlobalVariables() map[string]any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.GlobalVariables == nil {
		return nil
	}
	out := make(map[string]any, len(c.GlobalVariables))
	for k, v := range c.GlobalVariables {
		out[k] = v
	}
	return out
}

// GetEnvVariables returns a snapshot of the env variables from context
// ($ENV).
func (c *Context) GetEnvVariables() map[string]any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.EnvVariables == nil {
		return nil
	}
	out := make(map[string]any, len(c.EnvVariables))
	for k, v := range c.EnvVariables {
		out[k] = v
	}
	return out
}

// LastNodeID returns the last node id from workflow runtime context.
func LastNodeID(ctx context.Context) types.NodeID {
	return WorkflowRuntimeContext(ctx).GetLastNodeID()
}

// executionIDContextKey carries the host's execution id for the current
// engine execution (the worker injects it via WithExecutionID before
// Execute/Resume so $EXECUTION_ID reflects the real distributed id instead
// of a bare-engine uuid).
type executionIDContextKey struct{}

// WithExecutionID attaches the host's execution id to ctx. Workflow.Execute
// reads it when seeding the root execution context ($EXECUTION_ID); child /
// loop / parallel sub-executions inherit the value from the root context.
func WithExecutionID(ctx context.Context, executionID string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, executionIDContextKey{}, executionID)
}

// ExecutionIDFromContext returns the execution id attached by
// WithExecutionID, or "".
func ExecutionIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	id, _ := ctx.Value(executionIDContextKey{}).(string)
	return id
}

// DefaultTenantID is plaita's default tenant id (plaita.tenant_context
// DEFAULT_TENANT_ID). An empty tenant id maps to it.
const DefaultTenantID = "default"

// tenantIDContextKey carries the worker's tenant id for the current engine
// execution. The worker injects it alongside the execution id; child / loop /
// parallel executions inherit it through the context chain.
type tenantIDContextKey struct{}

// WithTenantID attaches the tenant id to ctx. Host-executing nodes read it
// via TenantIDFromContext to apply the fail-closed tenant gate (plaita
// refuse_non_default_tenant).
func WithTenantID(ctx context.Context, tenantID string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, tenantIDContextKey{}, tenantID)
}

// TenantIDFromContext returns the tenant id attached by WithTenantID, or ""
// when the host attached none. An empty id is the bare-engine / single-tenant
// deployment (plaita's ImportError fallback in tenant_context).
func TenantIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	id, _ := ctx.Value(tenantIDContextKey{}).(string)
	return id
}

// IsNonDefaultTenant reports whether tenantID identifies a tenant other than
// the default. The empty id is the default (plaita current_tenant() falls back
// to DEFAULT_TENANT_ID).
func IsNonDefaultTenant(tenantID string) bool {
	return tenantID != "" && tenantID != DefaultTenantID
}

// WithWorkflowRuntimeContext returns a new context with workflow runtime context.
func WithWorkflowRuntimeContext(ctx context.Context, workflowRuntimeContext *Context) context.Context {
	if workflowRuntimeContext == nil {
		return ctx
	}
	return context.WithValue(ctx, workflowRuntimeContextContextKey, workflowRuntimeContext)
}

// WorkflowRuntimeContext returns the workflow runtime context from context.
// A nil context (or one without a runtime context) yields a fresh empty one
// instead of panicking — $PARENT on a root flow evaluates against this empty
// context and must not crash (see evaluator's parent fallback).
func WorkflowRuntimeContext(ctx context.Context) *Context {
	if ctx == nil {
		return newContext()
	}
	if runtimeCtx, _ := ctx.Value(workflowRuntimeContextContextKey).(*Context); runtimeCtx != nil {
		return runtimeCtx
	}
	return newContext()
}

// LookupWorkflowRuntimeContext returns the workflow runtime context carried by
// ctx and reports whether one was actually present. Callers that need to
// distinguish "no runtime context" from "empty runtime context" (flow
// execution setup) must use this instead of WorkflowRuntimeContext.
func LookupWorkflowRuntimeContext(ctx context.Context) (*Context, bool) {
	if ctx == nil {
		return nil, false
	}
	runtimeCtx, _ := ctx.Value(workflowRuntimeContextContextKey).(*Context)
	return runtimeCtx, runtimeCtx != nil
}

// ResetWorkflowRuntimeContext resets the workflow runtime context from context.
func ResetWorkflowRuntimeContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, workflowRuntimeContextContextKey, newContext())
}

// NewExecutionRuntimeContext returns a fresh runtime context for a new flow
// execution. Child-flow state carried by ctx ($PARENT linkage and $GLOBAL)
// is preserved — child/loop/parallel sub-flows keep their parent chain —
// while the per-execution state ($INPUT/$NODE/$ERROR/current node) starts
// clean:
//
//   - ParentWorkflowContext points at the parent execution context found in
//     ctx (its runtime context), so $PARENT.<...> resolves in the flow that
//     spawned this execution; a root flow has no parent and $PARENT stays nil.
//   - The parent's global variables are copied in ($GLOBAL falls back to the
//     parent in plaita; the copy is the Go encoding of that fallback).
//   - The parent's flow id and execution id are inherited ($FLOW_ID /
//     $EXECUTION_ID child fallback, plaita setup_flow): a sub-flow with its
//     own id overrides the flow id afterwards, the execution id stays shared
//     so every level of one distributed execution correlates.
func NewExecutionRuntimeContext(ctx context.Context) *Context {
	fresh := newContext()
	parent, ok := LookupWorkflowRuntimeContext(ctx)
	if !ok {
		return fresh
	}
	// The frame kind travels with the execution (nested suspend v1): the
	// sub-flow entrypoints (child/reference/collection iteration/parallel
	// branch) annotate the seed context, and the fresh execution context built
	// here IS that frame — the scope gate and the conversion points read it.
	fresh.FrameKind = parent.FrameKind
	// The span-model frame state travels the same way (plaita#344): the
	// entrypoint already extended the path and set the iteration key — the
	// fresh execution context is that frame, so its nodes' spans carry them.
	fresh.FramePath = append([]string(nil), parent.FramePath...)
	fresh.FrameKey = parent.FrameKey
	if parent.ParentWorkflowContext != nil {
		fresh.ParentWorkflowContext = parent.ParentWorkflowContext
	}
	// The parent chain has up to two levels (child/loop nodes wrap the parent
	// execution in a raw seed context): the immediate rc and, through the
	// seed's $PARENT link, the real parent execution. Both carry inheritable
	// identity; the immediate one wins.
	if parent.FlowID != "" {
		fresh.FlowID = parent.FlowID
	}
	if parent.ExecutionID != "" {
		fresh.ExecutionID = parent.ExecutionID
	}
	if parentRuntime := WorkflowRuntimeContext(parent.ParentWorkflowContext); parentRuntime != nil {
		fresh.WithGlobalVariables(parentRuntime.GetGlobalVariables())
		if fresh.FlowID == "" {
			fresh.FlowID = parentRuntime.FlowID
		}
		if fresh.ExecutionID == "" {
			fresh.ExecutionID = parentRuntime.ExecutionID
		}
	}
	return fresh
}

// NewExecutionUUID mints a uuid4 in the canonical dashed form (plaita
// execution_id = str(uuid.uuid4())). The bare-engine fallback for a root
// execution whose host did not inject one via WithExecutionID.
func NewExecutionUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// CloneContext deep clone the workflow runtime context from context. The
// clone's parent linkage points at the SOURCE execution (ctx): a parallel
// branch is a child of the flow that contains the parallel node, so
// $PARENT inside the branch must resolve against that flow, matching
// plaita's child-execution parent chain.
//
// The source maps are deep-copied under the source's read lock: a background
// branch clones while the parent flow keeps writing node results.
func CloneContext(ctx context.Context) context.Context {
	runtimeCtx := WorkflowRuntimeContext(ctx)
	newRuntimeCtx := newContext()
	runtimeCtx.mu.RLock()
	copyOpts := copier.Option{DeepCopy: true}
	_ = copier.CopyWithOption(&newRuntimeCtx.WorkflowInput, runtimeCtx.WorkflowInput, copyOpts)
	_ = copier.CopyWithOption(&newRuntimeCtx.ExecuteNodeResults, runtimeCtx.ExecuteNodeResults, copyOpts)
	_ = copier.CopyWithOption(&newRuntimeCtx.CurrentNodeContext, runtimeCtx.CurrentNodeContext, copyOpts)
	_ = copier.CopyWithOption(&newRuntimeCtx.EnvVariables, runtimeCtx.EnvVariables, copyOpts)
	_ = copier.CopyWithOption(&newRuntimeCtx.GlobalVariables, runtimeCtx.GlobalVariables, copyOpts)
	newRuntimeCtx.LastNodeID = runtimeCtx.LastNodeID
	newRuntimeCtx.LastBranch = runtimeCtx.LastBranch
	newRuntimeCtx.FlowDeadline = runtimeCtx.FlowDeadline
	newRuntimeCtx.FlowID = runtimeCtx.FlowID
	newRuntimeCtx.ExecutionID = runtimeCtx.ExecutionID
	// The frame kind travels with the clone (the branch entrypoint overrides
	// it to FrameKindParallelBranch; map's concurrent path relies on the copy
	// before its iteration seed replaces the context).
	newRuntimeCtx.FrameKind = runtimeCtx.FrameKind
	// The frame state travels with the clone: a parallel branch (the only
	// CloneContext consumer) is a frame — its entrypoint overrides the key
	// and extends the path with the branch node's id.
	newRuntimeCtx.FramePath = append([]string(nil), runtimeCtx.FramePath...)
	newRuntimeCtx.FrameKey = runtimeCtx.FrameKey
	runtimeCtx.mu.RUnlock()
	newRuntimeCtx.ParentWorkflowContext = ctx
	return WithWorkflowRuntimeContext(ctx, newRuntimeCtx)
}
