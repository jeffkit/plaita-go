package nodes

import (
	"context"

	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func init() {
	workflow.RegisterNodeType(
		types.NodeTypeMock,
		func(ctx context.Context) workflow.Node { return new(Mock) },
	)
}

// Mock 落地 plaita 的 Mock 节点（plaita/node/mock.py，数据固定 pin 占位）：
// 原样返回 “value“ 字段作为节点输出。console 侧把被固定的节点替换成本类
// 型，使后续试跑以固定值跳过真实执行（如 HTTP 调用）反复调试下游；也可手工
// 作为测试桩。
//
// plaita 实测语义（r78 探针/golden）：
//   - value 不做表达式求值——“execute“ 直接 “return self.value“，连
//     "$INPUT.x" 这样的表达式串都字面成为节点结果（pin 语义即字面固定）；
//   - output 字段被忽略（runner 不对 mock 求 output）。
type Mock struct {
	BaseNode
	Value any `json:"value"`
}

// GetType returns the node type.
func (m *Mock) GetType() workflow.NodeType {
	return types.NodeTypeMock
}

// Execute returns the pinned value untouched.
func (m *Mock) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	return &workflow.ExecuteResult{Result: m.Value, NextNode: m.NextNode()}, nil
}
