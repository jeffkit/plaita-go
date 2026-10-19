// Package workflow provides plaita workflow
package workflow

import (
	"context"
	"errors"
	"reflect"

	"github.com/fatih/structs"
	validation "github.com/go-ozzo/ozzo-validation"
	"github.com/samber/lo"

	"github.com/jeffkit/plaita-go/internal/errs"
	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/duration"
)

// RuntimeType represents the runtime type of the plaita workflow.
type RuntimeType string

// RuntimeType supported by Plaita.
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

// Workflow represents Plaita Workflow DSL definition.
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
	Metadata            map[string]string            `json:"metadata,omitempty"`       // workflow元数据
	Context             Context                      `json:"context,omitempty"`        // workflow上下文配置
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

	if _, ok := f.findStartNode(); !ok {
		return ErrStartNodeNotFound
	}

	if _, ok := f.findEndNode(); !ok {
		return ErrEndNodeNotFound
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
func (f *Workflow) Execute(ctx context.Context, args ExecuteArgs) (any, error) {
	if err := f.Validate(); err != nil {
		return nil, err
	}

	if !f.Timeout.IsZero() {
		var cancelFunc context.CancelFunc
		ctx, cancelFunc = context.WithTimeout(ctx, f.Timeout.TimeDuration())
		defer cancelFunc()

		// TODO(scottzhlin): 计算剩余的超时时间并存入context向下传递 node的超时时间应该受到workflow超时时间的影响
	}

	ctx = runtime.ResetWorkflowRuntimeContext(ctx)
	ctx = runtime.WithWorkflowInput(ctx, args)
	runtimeCtx := runtime.WorkflowRuntimeContext(ctx)
	runtimeCtx.WithEnvVariablesContext()
	globalVariables := f.Context.ParseGlobal()
	runtimeCtx.WithGlobalVariables(globalVariables)

	var result any
	var err error
	retryFunc := func(ctx context.Context) error {
		triggerFlowStart(ctx, f)
		result, err = f.execute(ctx, args)
		triggerFlowEnd(ctx, f, result, err)
		if err == nil {
			return nil
		}

		if contextTimeoutError(err) {
			result, err = f.TimeoutHandlePolicy.Handle(err)
			return err
		}

		return RetryableError(err)
	}

	return result, f.ErrorHandlePolicy.Retrier().Do(ctx, retryFunc)
}

func (f *Workflow) execute(ctx context.Context, _ ExecuteArgs) (result any, err error) {
	startNode, ok := f.findStartNode()
	if !ok {
		return nil, ErrStartNodeNotFound
	}

	for node := startNode; node != nil; {
		result, err := f.executeNode(ctx, node)
		if err != nil {
			return nil, err
		}

		if isEndNode(node) {
			return result.Result, nil
		}

		runtimeCtx := runtime.WorkflowRuntimeContext(ctx)
		runtimeCtx.WithExecuteNodeResult(node.GetID(), result.Result)
		runtimeCtx.WithLastNodeID(node.GetID())

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

	ctx = runtime.ResetWorkflowRuntimeContext(ctx)
	ctx = runtime.WithWorkflowInput(ctx, args)
	runtimeCtx := runtime.WorkflowRuntimeContext(ctx)
	runtimeCtx.WithEnvVariablesContext()
	globalVariables := f.Context.ParseGlobal()
	runtimeCtx.WithGlobalVariables(globalVariables)

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
	timeout := node.GetTimeout().TimeDuration()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, node.GetTimeout().TimeDuration())
		defer cancel()
	}

	retryFunc := func(ctx context.Context) error {
		runtimeCtx := runtime.WorkflowRuntimeContext(ctx)
		runtimeCtx.ResetCurrentNodeContext()
		triggerNodeStart(ctx, f, node)
		result, err = node.Execute(ctx)
		args := node.MoreInfoAfterExecute(ctx)
		ret := any(nil)
		if result != nil {
			ret = result.Result
		}
		triggerNodeEnd(ctx, f, node, ret, err, args) // TODO 从node中获取更多用于暴露给外部的信息。
		if err == nil {
			return nil
		}
		if contextTimeoutError(err) {
			timeoutHandlePolicy := node.GetTimeoutHandlePolicy()
			defaultValue, handledErr := timeoutHandlePolicy.Handle(err)
			result = &ExecuteResult{Result: defaultValue, NextNode: node.NextNode()}
			return handledErr
		}
		return RetryableError(err)
	}
	errorHandlePolicy := node.GetErrorHandlePolicy()
	if err = errorHandlePolicy.Retrier().Do(ctx, retryFunc); err != nil {
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
	return result, nil
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
