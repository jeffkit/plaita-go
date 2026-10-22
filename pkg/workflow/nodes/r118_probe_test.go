package nodes

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/duration"
	"github.com/jeffkit/plaita-go/pkg/workflow"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// #81 负控探针（A1 r118）：map 并发路径的 childFlowView 曾漏拷
// Workflow.Timeout——同一子流程，顺序路径 flow 级超时生效、concurrent 路径
// 静默失效（跑满全程，50ms 预算从未存在）。先例 parallel.branchFlowView 专
// 门拷贝 Timeout（issue #17）。探针：子流程 Timeout=PT0.05S + 单 sleep 300ms
// 节点 ×2 元素——两条路径都必须报 flow 级超时。
func TestR118_ChildTimeout_SequentialVsConcurrent(t *testing.T) {
	newMap := func(concurrent bool) *Map {
		return &Map{
			Child: Child{
				BaseNode: BaseNode{ID: "m"},
				ChildFlow: &workflow.Workflow{
					ID:      "cf",
					Timeout: duration.MustParseDuration("PT0.05S"),
					Nodes: workflow.Nodes{
						&Start{BaseNode: BaseNode{ID: "s", Next: "sl"}},
						&Sleep{BaseNode: BaseNode{ID: "sl", Next: "e"}, Duration: 300 * time.Millisecond},
						&End{BaseNode: BaseNode{ID: "e", Output: "$INPUT.index"}},
					},
				},
			},
			Collection: loopCollection{array: []any{1, 2}},
			Concurrent: concurrent,
		}
	}

	assertChildTimeout := func(t *testing.T, err error) {
		t.Helper()
		require.Error(t, err, "child flow Timeout=50ms must bound a 300ms sleep")
		assert.True(t, errors.Is(err, context.DeadlineExceeded),
			"error must be the flow-level deadline, got: %v", err)
	}

	t.Run("sequential_timeout_applies", func(t *testing.T) {
		ctx := runtime.WithWorkflowRuntimeContext(context.Background(), runtime.NewContext())
		_, err := newMap(false).Execute(ctx)
		assertChildTimeout(t, err)
	})

	t.Run("concurrent_timeout_applies", func(t *testing.T) {
		ctx := runtime.WithWorkflowRuntimeContext(context.Background(), runtime.NewContext())
		_, err := newMap(true).Execute(ctx)
		assertChildTimeout(t, err)
	})
}
