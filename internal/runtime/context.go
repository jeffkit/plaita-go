package runtime

import (
	"context"
	"os"
	"strings"
	"sync"

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

// WithEnvVariablesContext sets the env variables in context.
func (c *Context) WithEnvVariablesContext() {
	if c.EnvVariables == nil {
		c.EnvVariables = map[string]any{}
	}
	environ := os.Environ()
	for i := 0; i < len(environ); i++ {
		e := strings.Split(environ[i], "=")
		if len(e) < 2 {
			continue
		}
		c.mu.Lock()
		c.EnvVariables[e[0]] = e[1]
		c.mu.Unlock()
	}
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
func WorkflowRuntimeContext(ctx context.Context) *Context {
	if runtimeCtx, _ := ctx.Value(workflowRuntimeContextContextKey).(*Context); runtimeCtx != nil {
		return runtimeCtx
	}
	return newContext()
}

// ResetWorkflowRuntimeContext resets the workflow runtime context from context.
func ResetWorkflowRuntimeContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, workflowRuntimeContextContextKey, newContext())
}

// CloneContext deep clone the workflow runtime context from context.
func CloneContext(ctx context.Context) context.Context {
	runtimeCtx := WorkflowRuntimeContext(ctx)
	newRuntimeCtx := newContext()
	copyOpts := copier.Option{DeepCopy: true}
	_ = copier.CopyWithOption(&newRuntimeCtx.WorkflowInput, runtimeCtx.WorkflowInput, copyOpts)
	_ = copier.CopyWithOption(&newRuntimeCtx.ExecuteNodeResults, runtimeCtx.ExecuteNodeResults, copyOpts)
	_ = copier.CopyWithOption(&newRuntimeCtx.CurrentNodeContext, runtimeCtx.CurrentNodeContext, copyOpts)
	_ = copier.CopyWithOption(&newRuntimeCtx.EnvVariables, runtimeCtx.EnvVariables, copyOpts)
	_ = copier.CopyWithOption(&newRuntimeCtx.GlobalVariables, runtimeCtx.GlobalVariables, copyOpts)
	newRuntimeCtx.ParentWorkflowContext = runtimeCtx.ParentWorkflowContext
	newRuntimeCtx.LastNodeID = runtimeCtx.LastNodeID
	return WithWorkflowRuntimeContext(ctx, newRuntimeCtx)
}
