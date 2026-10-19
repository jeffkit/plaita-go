package nodes

import (
	"context"
	"maps"

	validation "github.com/go-ozzo/ozzo-validation"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/duration"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// BaseNode basic node information
type BaseNode struct {
	ID                  types.NodeID                          `json:"id"`                       // 描述逻辑节点的id，全局唯一
	Type                types.NodeType                        `json:"type"`                     // 逻辑节点的类型
	Name                string                                `json:"name,omitempty"`           // 逻辑节点的名称
	Desc                string                                `json:"desc,omitempty"`           // 逻辑节点的描述
	Async               bool                                  `json:"async,omitempty"`          // 是否异步节点，默认为true
	Output              any                                   `json:"output,omitempty"`         // 节点返回值
	External            map[string]any                        `json:"external,omitempty"`       // 扩展参数
	Timeout             duration.Duration                     `json:"timeout,omitempty"`        // 超时时间，ISO 8601格式
	ErrorHandlePolicy   workflow.RecoverableErrorHandlePolicy `json:"errorHandler,omitempty"`   // 节点执行错误处理策略
	TimeoutHandlePolicy workflow.TimeoutHandlePolicy          `json:"timeoutHandler,omitempty"` // 节点执行超时处理策略
	Next                workflow.NodeID                       `json:"next,omitempty"`           // 描述下一个逻辑节点的id
	OutputType          workflow.Property                     `json:"outputType,omitempty"`     // 节点输出参数的格式定义
	Metadata            map[string]string                     `json:"metadata,omitempty"`       // 节点元数据

	ExtraResult map[string]interface{}
}

// Validate validates the node info.
func (n *BaseNode) Validate() error {
	return validation.ValidateStruct(n,
		validation.Field(&n.ID, validation.Required.Error("node id is required")),
		validation.Field(&n.ErrorHandlePolicy),
		validation.Field(&n.TimeoutHandlePolicy))
}

// GetID returns the id of the node.
func (n *BaseNode) GetID() workflow.NodeID {
	return n.ID
}

// GetType returns the type of the node.
func (n *BaseNode) GetType() workflow.NodeType {
	return "noop"
}

// GetTimeout returns the timeout duration of the node.
func (n *BaseNode) GetTimeout() duration.Duration {
	return n.Timeout
}

// GetName returns the name of the node.
func (n *BaseNode) GetName() string {
	return n.Name
}

// GetErrorHandlePolicy returns the error handle policy of the node.
func (n *BaseNode) GetErrorHandlePolicy() workflow.RecoverableErrorHandlePolicy {
	return n.ErrorHandlePolicy
}

// GetTimeoutHandlePolicy returns the timeout handle policy of the node.
func (n *BaseNode) GetTimeoutHandlePolicy() workflow.TimeoutHandlePolicy {
	return n.TimeoutHandlePolicy
}

// NextNode returns the next node id of the node.
func (n *BaseNode) NextNode() workflow.NodeID {
	return n.Next
}

// Execute executes the node.
func (n *BaseNode) Execute(_ context.Context) (*workflow.ExecuteResult, error) {
	return &workflow.ExecuteResult{NextNode: n.NextNode()}, nil
}

// GetMetadata returns the metadata of the node.
func (n *BaseNode) GetMetadata() map[string]string {
	return maps.Clone(n.Metadata)
}

const extraResultKey = "node.extra.result"

// MoreInfoAfterExecute returns the more info after execute.
func (n *BaseNode) MoreInfoAfterExecute(ctx context.Context) map[string]interface{} {
	runtimeCtx := runtime.WorkflowRuntimeContext(ctx)
	value := runtimeCtx.GetCurrentNodeContext(extraResultKey)
	extraResult, _ := value.(map[string]any)
	if extraResult == nil {
		return make(map[string]any)
	}
	return maps.Clone(extraResult)
}
