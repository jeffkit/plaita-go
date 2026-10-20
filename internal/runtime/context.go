package runtime

import (
	"context"
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
	LastBranch        any            `json:"last_branch,omitempty"`
	ExecuteNodeErrors map[string]any `json:"errors"`
	EnvVariables      map[string]any `json:"env_variables"`
	GlobalVariables   map[string]any `json:"global_variables"`

	// FlowDeadline is the deadline imposed by the current flow's own timeout
	// budget. It stays zero for flows without an explicit timeout. It is used
	// to attribute a context deadline error to the flow budget instead of the
	// node budget (plaita FLOW_ERROR vs NODE_ERROR); it is runtime bookkeeping
	// and never serialized.
	FlowDeadline time.Time `json:"-"`
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
func NewExecutionRuntimeContext(ctx context.Context) *Context {
	fresh := newContext()
	parent, ok := LookupWorkflowRuntimeContext(ctx)
	if !ok {
		return fresh
	}
	if parent.ParentWorkflowContext != nil {
		fresh.ParentWorkflowContext = parent.ParentWorkflowContext
	}
	if parentRuntime := WorkflowRuntimeContext(parent.ParentWorkflowContext); parentRuntime != nil {
		fresh.WithGlobalVariables(parentRuntime.GetGlobalVariables())
	}
	return fresh
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
	runtimeCtx.mu.RUnlock()
	newRuntimeCtx.ParentWorkflowContext = ctx
	return WithWorkflowRuntimeContext(ctx, newRuntimeCtx)
}
