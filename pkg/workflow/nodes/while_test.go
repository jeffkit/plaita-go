package nodes

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/workflow"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 取消传播探针：阻塞在 ctx.Done 上（#48 ctx 反弹矩阵——循环体每步的子执行
// 都是 ctx-aware，取消必须从子执行一路冒泡出 While.Execute）。
type cancelProbe struct {
	BaseNode
}

func (p *cancelProbe) GetType() workflow.NodeType { return "cancel_probe" }

func (p *cancelProbe) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(time.Hour):
		return &workflow.ExecuteResult{Result: "done"}, nil
	}
}

func init() {
	workflow.RegisterNodeType("cancel_probe",
		func(ctx context.Context) workflow.Node { return &cancelProbe{} })
}

// 取消传播负控：恒真条件 + 大上限的 while，父 ctx 取消后 Execute 必须即时
// 返回 context.Canceled（不能吞掉、不能继续下一轮）。
func TestWhile_CancelPropagates(t *testing.T) {
	maxIter := 100000
	w := &While{
		Child: Child{
			BaseNode: BaseNode{ID: "w"},
			ChildFlow: &workflow.Workflow{
				ID:    "cf",
				Nodes: workflow.Nodes{&Start{BaseNode: BaseNode{ID: "s", Next: "p"}}, &cancelProbe{BaseNode: BaseNode{ID: "p", Next: "e"}}, &End{BaseNode: BaseNode{ID: "e"}}},
			},
		},
		// 零值 Condition（直构不走 Unmarshal，singleRound=false）→ 恒 match
		MaxIterations: &maxIter,
	}
	ctx, cancel := context.WithCancel(runtime.WithWorkflowRuntimeContext(context.Background(), runtime.NewContext()))
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	done := make(chan error, 1)
	start := time.Now()
	go func() {
		_, err := w.Execute(ctx)
		done <- err
	}()
	select {
	case err := <-done:
		require.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled), "got %v", err)
		assert.Less(t, time.Since(start), 5*time.Second)
	case <-time.After(10 * time.Second):
		t.Fatal("while did not return after ctx cancellation")
	}
}

// 取消落在两轮之间：循环头部的 ctx.Err() fail-fast 让 while 不再发起新一轮。
func TestWhile_CancelBetweenRoundsFailFast(t *testing.T) {
	maxIter := 100000
	w := &While{
		Child: Child{
			BaseNode: BaseNode{ID: "w"},
			ChildFlow: &workflow.Workflow{
				ID:    "cf",
				Nodes: workflow.Nodes{&Start{BaseNode: BaseNode{ID: "s", Next: "p"}}, &cancelProbe{BaseNode: BaseNode{ID: "p", Next: "e"}}, &End{BaseNode: BaseNode{ID: "e"}}},
			},
		},
		MaxIterations: &maxIter,
	}
	ctx, cancel := context.WithCancel(runtime.WithWorkflowRuntimeContext(context.Background(), runtime.NewContext()))
	cancel() // 先取消再执行：第一轮循环头即返回

	_, err := w.Execute(ctx)
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled), "got %v", err)
}
