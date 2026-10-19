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
type Context struct {
	mu                    sync.RWMutex
	WorkflowInput         any             `json:"input"`
	ExecuteNodeResults    map[string]any  `json:"nodes"`
	ParentWorkflowContext context.Context `json:"parent"`
	CurrentNodeContext    map[string]any  `json:"current"`
	LastNodeID            types.NodeID    `json:"last_node_id"`
	ExecuteNodeErrors     map[string]any  `json:"errors"`
	EnvVariables          map[string]any  `json:"env_variables"`
	GlobalVariables       map[string]any  `json:"global_variables"`

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
	c.WorkflowInput = input
}

// GetWorkflowInput returns the workflow input from workflow runtime context.
func (c *Context) GetWorkflowInput() any {
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
	if c.ExecuteNodeResults == nil {
		c.ExecuteNodeResults = make(map[string]any)
	}

	c.ExecuteNodeResults[string(nodeID)] = result
}

// WithExecuteNodeError sets the execute node error in context.
func (c *Context) WithExecuteNodeError(nodeID types.NodeID, err any) {
	if c.ExecuteNodeErrors == nil {
		c.ExecuteNodeErrors = make(map[string]any)
	}
	c.ExecuteNodeErrors[string(nodeID)] = err
}

// GetExecuteNodeError returns the execute node error from context.
func (c *Context) GetExecuteNodeError(nodeID types.NodeID) any {
	return c.ExecuteNodeErrors[string(nodeID)]
}

// GetExecuteNodeResult returns the execute node result from context.
func (c *Context) GetExecuteNodeResult(nodeID types.NodeID) any {
	return c.ExecuteNodeResults[string(nodeID)]
}

// WithCurrentNodeContext sets the current node context in context.
func (c *Context) WithCurrentNodeContext(key string, nodeContext any) {
	if c.CurrentNodeContext == nil {
		c.CurrentNodeContext = make(map[string]any)
	}
	c.CurrentNodeContext[key] = nodeContext
}

// ResetCurrentNodeContext resets the current node context in context.
func (c *Context) ResetCurrentNodeContext() {
	c.CurrentNodeContext = make(map[string]any)
}

// GetCurrentNodeContext returns the current node context from context.
func (c *Context) GetCurrentNodeContext(key string) any {
	return c.CurrentNodeContext[key]
}

// WithLastNodeID sets the last node id in workflow runtime context.
func (c *Context) WithLastNodeID(nodeID types.NodeID) {
	c.LastNodeID = nodeID
}

// GetLastNodeID returns the last node id from workflow runtime context.
func (c *Context) GetLastNodeID() types.NodeID {
	return c.LastNodeID
}

// WithGlobalVariables sets the global variables in context.
func (c *Context) WithGlobalVariables(variables map[string]any) {
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
	if c.GlobalVariables == nil {
		c.GlobalVariables = make(map[string]any)
	}
	c.GlobalVariables[key] = value
}

// GetGlobalVariable returns the global variables from context.
func (c *Context) GetGlobalVariable(key string) any {
	return c.GlobalVariables[key]
}

// GetGlobalVariables returns the global variables from context.
func (c *Context) GetGlobalVariables() map[string]any {
	return c.GlobalVariables
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
func CloneContext(ctx context.Context) context.Context {
	runtimeCtx := WorkflowRuntimeContext(ctx)
	newRuntimeCtx := newContext()
	copyOpts := copier.Option{DeepCopy: true}
	_ = copier.CopyWithOption(&newRuntimeCtx.WorkflowInput, runtimeCtx.WorkflowInput, copyOpts)
	_ = copier.CopyWithOption(&newRuntimeCtx.ExecuteNodeResults, runtimeCtx.ExecuteNodeResults, copyOpts)
	_ = copier.CopyWithOption(&newRuntimeCtx.CurrentNodeContext, runtimeCtx.CurrentNodeContext, copyOpts)
	_ = copier.CopyWithOption(&newRuntimeCtx.EnvVariables, runtimeCtx.EnvVariables, copyOpts)
	_ = copier.CopyWithOption(&newRuntimeCtx.GlobalVariables, runtimeCtx.GlobalVariables, copyOpts)
	newRuntimeCtx.ParentWorkflowContext = ctx
	newRuntimeCtx.LastNodeID = runtimeCtx.LastNodeID
	newRuntimeCtx.FlowDeadline = runtimeCtx.FlowDeadline
	return WithWorkflowRuntimeContext(ctx, newRuntimeCtx)
}
