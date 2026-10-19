package nodes

import (
	"context"
	"fmt"

	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func init() {
	workflow.RegisterNodeType(
		types.NodeTypeEnd,
		func(ctx context.Context) workflow.Node { return new(End) },
	)
}

// Error represents error information.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ResultType represents the type of the result.
type ResultType string

// ResultType enums
const (
	ResultTypeSuccess ResultType = "success"
	ResultTypeError   ResultType = "error"
	ResultTypeNoop    ResultType = "nop"
)

// End node is the exit point of a flow. It produces a result.
type End struct {
	BaseNode
	ResultType ResultType `json:"resultType"`
	Error      *Error     `json:"error,omitempty"`
}

// FlowResultError 对齐 plaita 的 plaita_errors.FlowResultError：end 节点以
// error 结果结束时抛出（Python plaita/node/end.py END_TYPE_ERROR 分支），
// code/message 取自 end.error，是 plaita 的错误契约（engine 错误码口径见
// internal/errs：这里的 code 是业务错误码字符串，如 "400"）。
type FlowResultError struct {
	Code    string
	Message string
}

// Error implements the error interface.
func (e *FlowResultError) Error() string {
	return fmt.Sprintf("FlowResultError: code=%s, message=%s", e.Code, e.Message)
}

// GetType returns the end node type.
func (e *End) GetType() workflow.NodeType {
	return types.NodeTypeEnd
}

// Execute executes the end node.
//
// 按 ResultType 分流（Python end.py execute）：
//   - error：直接抛 FlowResultError（带 error.code/error.message），不求值
//     output——Python 在 error 分支直接 raise，output 不参与；
//   - nop：静默结束、结果为 nil（IR 层已 fail-closed 拒载，直连引擎时保留
//     Python 语义）；
//   - success（含未知值的默认归一，对齐 Python setup_end）：求值并校验
//     output，作为流程返回值。
func (e *End) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	switch e.ResultType {
	case ResultTypeError:
		errInfo := e.Error
		if errInfo == nil {
			errInfo = &Error{}
		}
		return nil, &FlowResultError{Code: errInfo.Code, Message: errInfo.Message}
	case ResultTypeNoop:
		return &workflow.ExecuteResult{}, nil
	}

	evaluatedOutput, err := expression.Eval(ctx, e.Output)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate end node output: %v", err)
	}

	// Validate the evaluated output using the OutputType Property
	if err := e.OutputType.ValidateValue(evaluatedOutput); err != nil {
		return nil, fmt.Errorf("failed to validate end node output: %v", err)
	}
	return &workflow.ExecuteResult{Result: evaluatedOutput}, nil
}
