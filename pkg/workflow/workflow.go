// Package workflow provides the plaita-go workflow engine
package workflow

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/fatih/structs"
	validation "github.com/go-ozzo/ozzo-validation"
	"github.com/samber/lo"
	"github.com/sethvargo/go-retry"

	"github.com/jeffkit/plaita-go/internal/errs"
	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/duration"
)

// RuntimeType represents the runtime type of a plaita-go workflow.
type RuntimeType string

// RuntimeType supported by plaita-go.
const (
	RuntimeGo         = "go"
	RuntimeGEngine    = "gengine"
	RuntimeTypeScript = "typescript"
	RuntimePython     = "python"
	RuntimeErrorCode  = 10000
)

var supportedRuntime = []RuntimeType{RuntimeGo, RuntimeGEngine, RuntimeTypeScript, RuntimePython}

// Version is type for workflow version.
type Version string

// ID is type for workflow id
type ID string

// Step represents the workflow step result.
type Step struct {
	Result    *ExecuteResult   `json:"result"`
	NextNode  NodeID           `json:"next"`
	Type      NodeType         `json:"type"`
	Name      string           `json:"name"`
	ID        NodeID           `json:"id"`
	Context   *runtime.Context `json:"context"`
	Error     error
	EndOfFlow bool
}

// Executor represents the workflow executor. execute step by step.
type Executor struct {
	results  <-chan Step
	nextStep chan struct{}
}

// NewExecutor returns a new workflow executor
func NewExecutor() *Executor {
	return &Executor{
		nextStep: make(chan struct{}, 1),
	}
}

// NextStep returns the next step
func (e *Executor) NextStep() Step {
	e.nextStep <- struct{}{}
	// 从results中读取下一步的结果
	return <-e.results
}

// Workflow represents the plaita-go Workflow DSL definition.
type Workflow struct {
	ID                  ID                           `json:"id"`                       // workflow id
	Name                string                       `json:"name,omitempty"`           // workflow名称
	Desc                string                       `json:"desc,omitempty"`           // workflow描述
	Runtime             RuntimeType                  `json:"runtime,omitempty"`        // 运行时
	Version             Version                      `json:"version,omitempty"`        // workflow版本
	Author              string                       `json:"author,omitempty"`         // workflow作者
	Nodes               Nodes                        `json:"nodes,omitempty"`          // workflow逻辑节点
	Timeout             duration.Duration            `json:"timeout,omitempty"`        // 超时时间，ISO 8601格式
	InputType           Property                     `json:"inputType,omitempty"`      // workflow输入数据格式
	OutputType          Property                     `json:"outputType,omitempty"`     // workflow输出数据格式
	ErrorHandlePolicy   RecoverableErrorHandlePolicy `json:"errorHandler,omitempty"`   // 错误处理策略
	TimeoutHandlePolicy TimeoutHandlePolicy          `json:"timeoutHandler,omitempty"` // 超时处理策略
	Metadata            map[string]any               `json:"metadata,omitempty"`       // workflow元数据
	Context             Context                      `json:"context,omitempty"`        // workflow上下文配置
	// running is the reentry guard: one Workflow instance runs at most one
	// Execute at a time (Python _reentry_error semantics). Zero value ready.
	running atomic.Bool
	// ExposeEnv is the $ENV allowlist (plaita 0.5.0 allowlist model,
	// fail-closed): only the listed environment variables are visible to flow
	// expressions. An empty/absent list exposes nothing. A child flow without
	// its own declaration inherits the parent execution's (already filtered)
	// env. Wildcards ("*"), blank or whitespace-padded entries are rejected.
	ExposeEnv []string `json:"exposeEnv,omitempty"`
}

// Validate validates the workflow.
func (f *Workflow) Validate() error {
	if err := validation.ValidateStruct(f,
		validation.Field(&f.ID, validation.Required),
		validation.Field(&f.Runtime, validation.In(lo.ToAnySlice(supportedRuntime)...)),
		validation.Field(&f.Version, validation.Length(1, 100)),
		validation.Field(&f.Nodes, validation.Each(), validation.Required)); err != nil {
		return err
	}

	if err := ValidateExposeEnv(f.ExposeEnv); err != nil {
		return err
	}

	// #49: the engine layer of the budget caps (budget.go). These explicit
	// calls are the wiring — without them RecoverableErrorHandlePolicy.Validate
	// / TimeoutHandlePolicy.Validate were dead code (ozzo does not dispatch
	// pointer-receiver Validatable on value struct fields), so retryTimes=1e9
	// and bogus strategies loaded clean at this entrypoint. The flow-level
	// Timeout stays uncapped by decision (an operator budget that composes
	// with the worker execution deadline at runtime).
	if err := f.ErrorHandlePolicy.Validate(); err != nil {
		return err
	}
	if err := f.TimeoutHandlePolicy.Validate(); err != nil {
		return err
	}

	if _, ok := f.findStartNode(); !ok {
		return ErrStartNodeNotFound
	}

	if _, ok := f.findEndNode(); !ok {
		return ErrEndNodeNotFound
	}

	return nil
}

// ValidateExposeEnv validates a $ENV allowlist (plaita _safe_environment
// rules): entries must be exact environment variable names — no wildcard,
// no blank, no whitespace padding. Go's []string cannot carry non-strings.
func ValidateExposeEnv(allowed []string) error {
	for _, k := range allowed {
		if k == "" || strings.TrimSpace(k) != k || strings.Contains(k, "*") {
			return fmt.Errorf("exposeEnv contains invalid entry %q: no wildcards, no blank or whitespace-padded keys; list exact environment variable names", k)
		}
	}
	return nil
}

// ExecuteArgs is type for workflow execute args
type ExecuteArgs any

func toMap(v any) (map[string]any, error) {
	if v == nil {
		return nil, nil
	}

	if m, ok := v.(map[string]any); ok {
		return m, nil
	}

	if kind := reflect.Indirect(reflect.ValueOf(v)).Kind(); kind == reflect.Struct {
		return structs.Map(v), nil
	}
	return nil, errors.New("args must be map[string]any or struct")
}

// Execute executes the flow with the given input.
// Variable `args` can be pass by struct or map[string]any.
//
// A Workflow instance is not reentrant (Python executor._begin_run /
// _reentry_error): one Workflow object carries no per-execution state, but a
// second concurrent run would still interleave on shared node definitions
// and event callbacks. Concurrent/reentrant Execute on the same instance is
// rejected with ErrWorkflowReentry; create a new Workflow (or a child flow)
// per concurrent run. The flag is released when the execution returns.
func (f *Workflow) Execute(ctx context.Context, args ExecuteArgs) (any, error) {
	if !f.running.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("workflow %q: %w", f.ID, ErrWorkflowReentry)
	}
	defer f.running.Store(false)

	if err := f.Validate(); err != nil {
		return nil, err
	}

	if !f.Timeout.IsZero() {
		var cancelFunc context.CancelFunc
		ctx, cancelFunc = context.WithTimeout(ctx, f.Timeout.TimeDuration())
		defer cancelFunc()
	}

	ctx = f.prepareExecutionRuntimeContext(ctx, args)

	var result any
	var err error
	retryFunc := func(ctx context.Context) error {
		triggerFlowStart(ctx, f)
		// An explicit zero budget (or an already-expired caller context) fails
		// immediately instead of letting ctx-ignoring node bodies run to
		// completion — plaita treats 0 as the strictest budget (plaita#138).
		if ctxErr := ctx.Err(); ctxErr != nil {
			result, err = nil, ctxErr
		} else {
			result, err = f.execute(ctx, args)
		}
		// A suspension is a normal park, not a failed attempt: no flow-end
		// error marker (which would make the worker append a failed
		// run_end(final=false)) and no flow-level retry — re-executing a
		// parked flow would re-run the nodes before the suspend point. The
		// error still travels up so the worker can write the suspended row.
		if susp := AsSuspend(err); susp != nil {
			triggerFlowEnd(ctx, f, nil, nil)
			return susp
		}
		triggerFlowEnd(ctx, f, result, err)
		if err == nil {
			return nil
		}

		if contextTimeoutError(err) {
			result, err = f.TimeoutHandlePolicy.Handle(err)
			return err
		}

		// plaita node-F2 at flow level: a deterministic node error (wired
		// through IsRetryableError) must not buy full-flow re-executions
		// either — retryTimes only multiplies attempts for transient faults.
		if !IsRetryableError(err) {
			return err
		}
		return RetryableError(err)
	}

	// flow-level retry stays a cheap engine-level re-execution (10ms fixed);
	// the plaita exponential backoff and retry classification live at the
	// node level (RecoverableErrorHandlePolicy.Retrier / executeNode).
	flowRetrier := newLowDelayRetrier(
		retry.WithMaxRetries(uint64(f.ErrorHandlePolicy.RetryTimes), retry.NewConstant(10*time.Millisecond)))
	return result, flowRetrier.Do(ctx, retryFunc)
}

// prepareExecutionRuntimeContext builds the runtime context for one flow
// execution (both Execute and GetDebugExecute):
//
//   - a FRESH context replaces any inherited one, but the parent execution
//     found in ctx is kept reachable as $PARENT and its $GLOBAL/$ENV are
//     inherited (child/loop/parallel sub-flow semantics, plaita
//     get_global_variable fallback + plaita#141 expose_env inheritance);
//   - $ENV is built from the process environment through the ExposeEnv
//     allowlist — an undeclared allowlist (nil) inherits the parent
//     execution's filtered env, a declared one re-filters os.Environ, and
//     with neither $ENV stays empty (fail-closed);
//   - the flow's own globals override the inherited ones;
//   - $FLOW_ID (alias $FLOW) is the flow's own id; a sub-flow without one
//     keeps the parent's (plaita child fallback) — and the effective flow id
//     is also written into $GLOBAL.flow_id (plaita setup_flow);
//   - $EXECUTION_ID is the host-injected execution id (runtime.
//     WithExecutionID, wired by the worker), inherited from the parent
//     execution when the host did not inject one at this level, or a fresh
//     uuid for a bare-engine root run.
func (f *Workflow) prepareExecutionRuntimeContext(ctx context.Context, args ExecuteArgs) context.Context {
	fresh := runtime.NewExecutionRuntimeContext(ctx)
	_, hadParent := runtime.LookupWorkflowRuntimeContext(ctx)
	ctx = runtime.WithWorkflowRuntimeContext(ctx, fresh)
	ctx = runtime.WithWorkflowInput(ctx, args)
	fresh.WithGlobalVariables(f.Context.ParseGlobal())

	// $FLOW_ID / $GLOBAL.flow_id (plaita setup_flow): the flow's own id wins;
	// NewExecutionRuntimeContext already inherited the parent's, so a
	// sub-flow without its own id keeps it. The effective id lands in
	// $GLOBAL.flow_id too (an explicit global of the same name is the flow
	// config's, which setup_flow overwrites).
	if flowID := string(f.ID); flowID != "" {
		fresh.WithFlowID(flowID)
	}
	if flowID := fresh.GetFlowID(); flowID != "" {
		fresh.WithGlobalVariable("flow_id", flowID)
	}

	// $EXECUTION_ID: the host's injected id wins (worker wiring); otherwise
	// the value inherited from the parent execution stands (child/loop/
	// parallel sub-executions correlate under the root's id); a root run
	// without either mints a uuid.
	if execID := runtime.ExecutionIDFromContext(ctx); execID != "" {
		fresh.WithExecutionID(execID)
	} else if fresh.GetExecutionID() == "" {
		fresh.WithExecutionID(runtime.NewExecutionUUID())
	}

	// $ENV assembly (plaita 0.5.0 allowlist model, fail-closed):
	//   - declared allowlist (non-empty) → filter os.Environ through it;
	//   - declared EMPTY allowlist → expose nothing;
	//   - undeclared (nil) → inherit the parent execution's filtered env
	//     (plaita#141), or expose nothing at the root.
	switch {
	case f.ExposeEnv != nil && len(f.ExposeEnv) > 0:
		fresh.WithEnvVariablesContext(f.ExposeEnv...)
	case f.ExposeEnv != nil:
		fresh.WithEnvVariables(nil)
	case hadParent && fresh.ParentWorkflowContext != nil:
		fresh.WithEnvVariables(runtime.WorkflowRuntimeContext(fresh.ParentWorkflowContext).GetEnvVariables())
	default:
		fresh.WithEnvVariables(nil)
	}

	// own timeout budget → record its deadline for FLOW/NODE timeout
	// attribution; without an own budget the enclosing flow's deadline (if
	// any) stays authoritative — the context chain already enforces the min.
	if !f.Timeout.IsZero() {
		if dl, ok := ctx.Deadline(); ok {
			fresh.FlowDeadline = dl
		}
	} else if fresh.ParentWorkflowContext != nil {
		fresh.FlowDeadline = runtime.WorkflowRuntimeContext(fresh.ParentWorkflowContext).FlowDeadline
	}
	return ctx
}

func (f *Workflow) execute(ctx context.Context, _ ExecuteArgs) (result any, err error) {
	startNode, ok := f.findStartNode()
	if !ok {
		return nil, ErrStartNodeNotFound
	}
	return f.runNodes(ctx, startNode)
}

// runNodes advances the flow from node. It is shared by the start path
// (execute), the resume path (Resume continues from the resumed node's next
// pointer) and the mid-flow entry (ResumeFrom continues at the checkpoint's
// next node), so all three use exactly the same step bookkeeping, per-step
// checkpointing and suspend detection.
func (f *Workflow) runNodes(ctx context.Context, node Node) (any, error) {
	completed := 0
	for node != nil {
		result, err := f.executeNode(ctx, node)
		if err != nil {
			// 嵌套挂起 v1：内层挂起沿子节点体上抛到本层——把本帧压入其调用
			// 栈后继续向上（rs advance_from 的 push_frame 同形）。挂起不是本
			// 层节点的失败；作用域闸拒绝（SuspendRejectedError）不是载体，
			// 不压帧。
			if susp := AsSuspend(err); susp != nil {
				susp.CallStack = append(susp.CallStack, CallFrame{
					NodeID:   node.GetID(),
					Workflow: f,
					Ctx:      runtime.WorkflowRuntimeContext(ctx),
				})
			}
			return nil, err
		}

		// plaita `is_suspending`: an executing node parks the flow BEFORE the
		// end-node check and before advancing. The node result (status
		// "pending") is already staged in $NODE by suspend(), so the
		// checkpoint carries the admission state a resume will verify.
		if suspender, ok := node.(Suspender); ok && suspender.IsSuspending() {
			return nil, f.suspend(ctx, node, result)
		}

		if isEndNode(node) {
			return result.Result, nil
		}

		runtimeCtx := runtime.WorkflowRuntimeContext(ctx)
		runtimeCtx.WithExecuteNodeResult(node.GetID(), result.Result)
		runtimeCtx.WithLastNodeID(node.GetID())

		// #32 per-step checkpoint: record the live execution's position
		// (completed node + upcoming node) per the host's policy, before
		// advancing. A failure fails the step: a silently stale checkpoint
		// would misroute a later continue carrier.
		completed++
		if err := f.stepCheckpoint(ctx, node, result.NextNode, completed); err != nil {
			return nil, err
		}

		node, err = f.findNodeByID(result.NextNode)
		if err != nil {
			return nil, err
		}
	}

	return nil, errors.New("unreachable: end node not found")
}

// ExecuteGenerator executes the flow with the given input.
func (f *Workflow) ExecuteGenerator(ctx context.Context, executor *Executor) {
	results := make(chan Step)
	executor.results = results

	go func() {
		defer close(results)

		startNode, ok := f.findStartNode()
		if !ok {
			results <- Step{Error: ErrStartNodeNotFound}
			return
		}

		for node := startNode; node != nil; {
			result, err := f.executeNode(ctx, node)
			if err != nil {
				results <- Step{Error: err}
				return
			}

			// The debug generator only advances step by step inside one
			// process; it has no checkpoint store and no resume entry. A
			// suspending node would be silently stepped past, so fail closed.
			if suspender, ok := node.(Suspender); ok && suspender.IsSuspending() {
				results <- Step{Error: fmt.Errorf(
					"node %s suspends; the debug generator cannot park a flow (use Workflow.Execute/Resume)", node.GetID())}
				return
			}

			if isEndNode(node) {
				results <- Step{
					Result:    result,
					ID:        node.GetID(),
					EndOfFlow: true,
					Type:      node.GetType(),
					Name:      node.GetName(),
					Context:   runtime.WorkflowRuntimeContext(ctx),
				}
				return
			}

			runtimeCtx := runtime.WorkflowRuntimeContext(ctx)
			runtimeCtx.WithExecuteNodeResult(node.GetID(), result.Result)
			runtimeCtx.WithLastNodeID(node.GetID())

			results <- Step{
				Result:   result,
				ID:       node.GetID(),
				NextNode: result.NextNode,
				Type:     node.GetType(),
				Name:     node.GetName(),
				Context:  runtime.WorkflowRuntimeContext(ctx),
			}

			<-executor.nextStep // 等待下一个步骤的信号
			node, err = f.findNodeByID(result.NextNode)
			if err != nil {
				results <- Step{Error: err}
				return
			}
		}

		results <- Step{Error: errors.New("unreachable: end node not found")}
	}()
}

// GetDebugExecute returns the executor for debug.
func (f *Workflow) GetDebugExecute(ctx context.Context, args ExecuteArgs) (*Executor, error) {
	if err := f.Validate(); err != nil {
		return nil, err
	}

	ctx = f.prepareExecutionRuntimeContext(ctx, args)

	executor := NewExecutor()
	f.ExecuteGenerator(ctx, executor)
	return executor, nil
}

// SetEnvName sets the env name of workflow
func (f *Workflow) SetEnvName(envName string) {
	f.Context.SetEnvName(envName)
}

// SetFlowGlobalVariables sets the global variables in workflow.
//
// Deprecated: Use SetGlobalVariables instead.
func (f *Workflow) SetFlowGlobalVariables(ctx context.Context, globalVariables map[string]any) context.Context {
	f.SetGlobalVariables(globalVariables)
	return ctx
}

// SetGlobalVariables sets the global variables in workflow.
func (f *Workflow) SetGlobalVariables(globalVariables map[string]any) {
	f.Context.SetGlobalVariables(globalVariables)
}

// SetFlowGlobalVariable sets the global variable in workflow runtime context.
//
// Deprecated: Use SetGlobalVariable instead.
func (f *Workflow) SetFlowGlobalVariable(ctx context.Context, key string, value any) context.Context {
	f.Context.SetGlobalVariable(key, value)
	return ctx
}

// SetGlobalVariable sets the global variable in workflow.
func (f *Workflow) SetGlobalVariable(key string, value any) {
	f.Context.SetGlobalVariable(key, value)
}

func (f *Workflow) executeNode(ctx context.Context, node Node) (result *ExecuteResult, err error) {
	if nodeTimeout := node.GetTimeout(); !nodeTimeout.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, nodeTimeout.TimeDuration())
		defer cancel()
	}

	retryFunc := func(ctx context.Context) error {
		runtimeCtx := runtime.WorkflowRuntimeContext(ctx)
		runtimeCtx.ResetCurrentNodeContext()
		triggerNodeStart(ctx, f, node)
		// fail fast on an expired context: a node body that ignores ctx must
		// not run to completion once the budget is gone (plaita runs every
		// node under the remaining budget and raises NodeTimeoutError).
		if ctxErr := ctx.Err(); ctxErr != nil {
			result, err = nil, ctxErr
		} else {
			result, err = node.Execute(ctx)
		}
		args := node.MoreInfoAfterExecute(ctx)
		ret := any(nil)
		if result != nil {
			ret = result.Result
		}
		triggerNodeEnd(ctx, f, node, ret, err, args) // TODO 从node中获取更多用于暴露给外部的信息。
		if err == nil {
			return nil
		}
		// 嵌套挂起 v1：挂起载体与作用域闸拒绝先于一切策略链（预算归属/超时/
		// 重试/errorHandler）原样上抛，与取消同待遇——挂起不是节点失败。
		// 不带 RetryableError 标记返回：IsRetryableError 对未知错误默认可
		// 重试，标记会让 retrier 重跑整个子流。
		if AsSuspend(err) != nil {
			return err
		}
		if IsSuspendRejected(err) {
			return err
		}
		if contextTimeoutError(err) {
			if flowBudgetBinding(runtimeCtx, ctx) {
				// The flow-level budget is the binding constraint — this is a
				// FLOW_ERROR, not the node's timeout to handle: bubble the
				// error up so the flow's timeout policy decides.
				return err
			}
			timeoutHandlePolicy := node.GetTimeoutHandlePolicy()
			defaultValue, handledErr := timeoutHandlePolicy.Handle(err)
			result = &ExecuteResult{Result: defaultValue, NextNode: node.NextNode()}
			return handledErr
		}
		// plaita node-F2: deterministic errors (1001 request generation, the
		// ValueError class) never retry; transient ones do.
		if !IsRetryableError(err) {
			return err
		}
		return RetryableError(err)
	}
	errorHandlePolicy := node.GetErrorHandlePolicy()
	if err = errorHandlePolicy.Retrier().Do(ctx, retryFunc); err != nil {
		// 嵌套挂起 v1：挂起载体/作用域闸拒绝不可被 errorHandler continue 吞
		//（rs 修正清单 #3——continue 策略不得把挂起洗成默认值，fail-closed）。
		if AsSuspend(err) != nil {
			return nil, err
		}
		if IsSuspendRejected(err) {
			return nil, err
		}
		// 当错误策略是继续执行时，把错误信息存到Context中
		if errorHandlePolicy.IsContinue() {
			runtimeCtx := runtime.WorkflowRuntimeContext(ctx)
			runtimeCtx.ResetCurrentNodeContext()

			errInfo, parseErr := toMap(errs.Internal(err))
			if parseErr != nil {
				return nil, parseErr
			}
			runtimeCtx.WithExecuteNodeError(node.GetID(), errInfo)
		}
		// 执行错误处理逻辑
		defaultResult, finalErr := errorHandlePolicy.HandleFinalError(err)
		result = &ExecuteResult{Result: defaultResult, NextNode: node.NextNode()}
		return result, finalErr
	}
	// plaita runner: the branch taken by the most recent branching node is
	// recorded as context.last_branch ($BRANCH) — only on a successful
	// attempt (Python run_node writes it after run, errors keep the previous
	// branch). The switch's result IS the resolved branch target, so the
	// value mirrors Python resolve_branch_target (next, falling back to the
	// branch name).
	if node.GetType() == types.NodeTypeSwitch && result != nil {
		if branch, ok := result.Result.(string); ok {
			runtime.WorkflowRuntimeContext(ctx).WithLastBranch(branch)
		}
	}
	return result, nil
}

// flowBudgetBinding reports whether the context-deadline error was caused by
// the flow-level budget rather than the node's own: the recorded flow
// deadline has passed and the node ctx has no strictly tighter deadline.
// (plaita: time_limit_by_flow → NodeTimeoutError with FLOW_ERROR type.)
func flowBudgetBinding(runtimeCtx *runtime.Context, ctx context.Context) bool {
	if runtimeCtx == nil || runtimeCtx.FlowDeadline.IsZero() {
		return false
	}
	if time.Now().Before(runtimeCtx.FlowDeadline) {
		return false
	}
	if dl, ok := ctx.Deadline(); ok && dl.Before(runtimeCtx.FlowDeadline) {
		return false
	}
	return true
}

func (f *Workflow) findStartNode() (Node, bool) {
	return lo.Find(f.Nodes, func(item Node) bool { return item.GetType() == types.NodeTypeStart })
}

func (f *Workflow) findEndNode() (Node, bool) {
	return lo.Find(f.Nodes, func(item Node) bool { return item.GetType() == types.NodeTypeEnd })
}

func (f *Workflow) findNodeByID(id NodeID) (Node, error) {
	node, ok := lo.Find(f.Nodes, func(item Node) bool {
		return item.GetID() == id
	})
	if ok {
		return node, nil
	}

	return nil, ErrNodeNotFound
}
