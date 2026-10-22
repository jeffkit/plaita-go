package nodes

import (
	"context"
	"testing"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/workflow"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// plaita/node/mock.py：execute 直接 return self.value——value 不求值（连
// "$INPUT.x" 表达式串都字面返回），output 字段被忽略（r78 探针/golden 钉）。
func TestMockExecute(t *testing.T) {
	tests := []struct {
		name  string
		node  *Mock
		setup func(context.Context) context.Context
		want  any
	}{
		{
			name: "expression string stays literal (pin semantics)",
			node: &Mock{BaseNode: BaseNode{ID: "m"}, Value: "$INPUT.x"},
			setup: func(ctx context.Context) context.Context {
				return runtime.WithWorkflowRuntimeContext(ctx, &runtime.Context{WorkflowInput: map[string]any{"x": "hello"}})
			},
			want: "$INPUT.x",
		},
		{
			name: "dict pinned as-is",
			node: &Mock{BaseNode: BaseNode{ID: "m"}, Value: map[string]any{"k": []any{int64(1), int64(2)}}},
			want: map[string]any{"k": []any{int64(1), int64(2)}},
		},
		{
			name: "absent value is nil",
			node: &Mock{BaseNode: BaseNode{ID: "m"}},
			want: nil,
		},
		{
			name: "output field is ignored by the node",
			node: &Mock{BaseNode: BaseNode{ID: "m", Output: "$INPUT.x"}, Value: "pinned"},
			setup: func(ctx context.Context) context.Context {
				return runtime.WithWorkflowRuntimeContext(ctx, &runtime.Context{WorkflowInput: map[string]any{"x": "eval-output"}})
			},
			want: "pinned",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			if tc.setup != nil {
				ctx = tc.setup(ctx)
			}
			got, err := tc.node.Execute(ctx)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got.Result)
			assert.Equal(t, tc.node.NextNode(), got.NextNode)
		})
	}
}

func TestMockGetType(t *testing.T) {
	assert.Equal(t, workflow.NodeType("mock"), new(Mock).GetType())
}
