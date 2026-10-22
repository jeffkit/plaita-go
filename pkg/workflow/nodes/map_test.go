package nodes

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/workflow"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 并发峰值探针（对齐 Python test_map_max_concurrent 的峰值断言思路）：
// 记录同时在跑的子流程实例数峰值，用于钉 maxConcurrent 的 gate 语义与
// 结果按集合顺序落位。仅测试二进制注册。
type mapProbeState struct {
	mu   sync.Mutex
	cur  int
	peak int
}

func (s *mapProbeState) enter() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cur++
	if s.cur > s.peak {
		s.peak = s.cur
	}
}

func (s *mapProbeState) leave() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cur--
}

func (s *mapProbeState) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cur, s.peak = 0, 0
}

func (s *mapProbeState) peakValue() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peak
}

type concurrencyProbe struct {
	BaseNode
}

var mapProbe mapProbeState

func (p *concurrencyProbe) GetType() workflow.NodeType { return "concurrency_probe" }

func (p *concurrencyProbe) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	mapProbe.enter()
	defer mapProbe.leave()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(15 * time.Millisecond):
	}
	input, _ := runtime.WorkflowRuntimeContext(ctx).GetWorkflowInput().(map[string]any)
	idx, _ := input["index"].(int)
	return &workflow.ExecuteResult{Result: map[string]any{"index": idx}, NextNode: p.NextNode()}, nil
}

func init() {
	workflow.RegisterNodeType("concurrency_probe",
		func(ctx context.Context) workflow.Node { return &concurrencyProbe{} })
}

// maxConcurrent=2、6 元素：峰值必须 ≤2（gate 生效）且 >1（真并发）；结果
// 严格按集合顺序落位（Python ThreadParallelExecutor.map 的有序契约）。
func TestMap_ConcurrentPeakAndOrder(t *testing.T) {
	mapProbe.reset()
	items := make([]any, 6)
	for i := range items {
		items[i] = i
	}
	m := &Map{
		Child: Child{
			BaseNode: BaseNode{ID: "m"},
			ChildFlow: &workflow.Workflow{
				ID: "cf",
				Nodes: workflow.Nodes{
					&Start{BaseNode: BaseNode{ID: "s", Next: "p"}},
					&concurrencyProbe{BaseNode: BaseNode{ID: "p", Next: "e"}},
					&End{BaseNode: BaseNode{ID: "e", Output: "$INPUT.index"}},
				},
			},
		},
		Collection:    loopCollection{array: items},
		Concurrent:    true,
		MaxConcurrent: ptrInt(2),
	}
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), runtime.NewContext())
	res, err := m.Execute(ctx)
	require.NoError(t, err)

	results, ok := res.Result.([]any)
	require.True(t, ok)
	require.Len(t, results, 6)
	for i, r := range results {
		// 子流程结果 = end 输出（$INPUT.index），非探针的 map
		idx, _ := r.(int)
		assert.Equal(t, i, idx, "results must land in collection order")
	}
	peak := mapProbe.peakValue()
	assert.Greater(t, peak, 1, "elements must actually run concurrently")
	assert.LessOrEqual(t, peak, 2, "peak concurrency must respect maxConcurrent")
}

// maxConcurrent 缺省 = 不设限（Python `or None`）：峰值允许超过 2（这里 6
// 个元素全部并发，峰值 == 6）。
func TestMap_ConcurrentUnboundedDefault(t *testing.T) {
	mapProbe.reset()
	items := make([]any, 6)
	for i := range items {
		items[i] = i
	}
	m := &Map{
		Child: Child{
			BaseNode: BaseNode{ID: "m"},
			ChildFlow: &workflow.Workflow{
				ID: "cf",
				Nodes: workflow.Nodes{
					&Start{BaseNode: BaseNode{ID: "s", Next: "p"}},
					&concurrencyProbe{BaseNode: BaseNode{ID: "p", Next: "e"}},
					&End{BaseNode: BaseNode{ID: "e", Output: "$INPUT.index"}},
				},
			},
		},
		Collection: loopCollection{array: items},
		Concurrent: true,
	}
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), runtime.NewContext())
	_, err := m.Execute(ctx)
	require.NoError(t, err)
	assert.Equal(t, 6, mapProbe.peakValue())
}

// 并发路径错误传播：最小失败下标的错误冒泡（Python ThreadPoolExecutor.map
// 按序产出，最早失败先抛）。
func TestMap_ConcurrentErrorPropagates(t *testing.T) {
	items := []any{1, 2, 3}
	m := &Map{
		Child: Child{
			BaseNode: BaseNode{ID: "m"},
			ChildFlow: &workflow.Workflow{
				ID: "cf",
				Nodes: workflow.Nodes{
					&Start{BaseNode: BaseNode{ID: "s", Next: "e"}},
					&End{BaseNode: BaseNode{ID: "e"}, ResultType: ResultTypeError,
						Error: &Error{Code: "500", Message: "boom"}},
				},
			},
		},
		Collection: loopCollection{array: items},
		Concurrent: true,
	}
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), runtime.NewContext())
	_, err := m.Execute(ctx)
	require.Error(t, err)
	var frErr *FlowResultError
	assert.True(t, errors.As(err, &frErr))
}

// 顺序路径错误传播：Python 串行 map 在首个错误处即停。
func TestMap_SequentialStopsAtFirstError(t *testing.T) {
	items := []any{1, 2, 3}
	m := &Map{
		Child: Child{
			BaseNode: BaseNode{ID: "m"},
			ChildFlow: &workflow.Workflow{
				ID: "cf",
				Nodes: workflow.Nodes{
					&Start{BaseNode: BaseNode{ID: "s", Next: "e"}},
					&End{BaseNode: BaseNode{ID: "e"}, ResultType: ResultTypeError,
						Error: &Error{Code: "500", Message: "boom"}},
				},
			},
		},
		Collection: loopCollection{array: items},
	}
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), runtime.NewContext())
	_, err := m.Execute(ctx)
	require.Error(t, err)
}

func ptrInt(i int) *int { return &i }
