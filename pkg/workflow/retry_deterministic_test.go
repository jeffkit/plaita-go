package workflow_test

// Issue #43 negative controls: deterministic node errors execute the node
// exactly once regardless of retryTimes (plaita node-F2 / A50 classification).
// Pre-fix both deterministic probes re-executed the node retryTimes+1 times
// (measured: sidecar ValueError with retryTimes=2 ran 3×, end-error with
// retryTimes=3 ran 4×); the transient probe pins that 1002-class errors keep
// retrying.

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/jeffkit/plaita-go/internal/errs"
	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/workflow"
	"github.com/jeffkit/plaita-go/pkg/workflow/nodes"
)

// probeNode returns queued errors before succeeding; executed counts attempts.
type probeNode struct {
	nodes.BaseNode
	execErrs []error
	executed int
}

func (n *probeNode) GetType() workflow.NodeType { return "r69probe" }

func (n *probeNode) Execute(context.Context) (*workflow.ExecuteResult, error) {
	i := n.executed
	n.executed++
	if i < len(n.execErrs) {
		return nil, n.execErrs[i]
	}
	return &workflow.ExecuteResult{Result: "ok", NextNode: n.NextNode()}, nil
}

var probeRegisterOnce sync.Once

func registerProbeNode(t *testing.T) {
	t.Helper()
	probeRegisterOnce.Do(func() {
		workflow.RegisterNodeType("r69probe", func(context.Context) workflow.Node {
			return &probeNode{}
		})
	})
}

// nodeExecutions runs flow and counts OnNodeStart firings for nodeID.
func nodeExecutions(t *testing.T, flow *workflow.Workflow, nodeID workflow.NodeID) int {
	t.Helper()
	var mu sync.Mutex
	count := 0
	workflow.OnNodeStart(func(_ context.Context, _ *workflow.Workflow, node workflow.Node) {
		if node.GetID() == nodeID {
			mu.Lock()
			count++
			mu.Unlock()
		}
	})
	runtimeCtx := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{})
	_, _ = flow.Execute(runtimeCtx, nil)
	return count
}

// TestDeterministicSidecarErrorExecutesOnce: a code node whose user code
// raises ValueError fails deterministically — the sidecar runs once, the
// retryTimes budget is not burned (plaita node-F2 ValueError class). The
// sidecar spawn per attempt makes this probe live (python3 on PATH).
func TestDeterministicSidecarErrorExecutesOnce(t *testing.T) {
	flow := &workflow.Workflow{
		ID:      "r69-sidecar-deterministic",
		Runtime: workflow.RuntimeGo,
		Nodes: workflow.Nodes{
			&nodes.Start{BaseNode: nodes.BaseNode{ID: "start", Next: "code"}},
			&nodes.Code{
				BaseNode: nodes.BaseNode{
					ID:   "code",
					Next: "end",
					ErrorHandlePolicy: workflow.RecoverableErrorHandlePolicy{
						Strategy:   workflow.HandleStrategyAbort,
						RetryTimes: 2,
					},
				},
				Language: nodes.LanguagePython,
				Code:     "def run(input):\n    raise ValueError('boom')",
			},
			&nodes.End{BaseNode: nodes.BaseNode{ID: "end"}},
		},
	}
	if got := nodeExecutions(t, flow, "code"); got != 1 {
		t.Fatalf("deterministic sidecar error executed %d times, want exactly 1 (retryTimes=2)", got)
	}
}

// TestDeterministicEndErrorExecutesOnce: resultType=error is the flow's
// DECISION (plaita converts FlowResultError to ErrorResultException inside the
// except clause, bypassing the retry loop entirely) — never retried.
func TestDeterministicEndErrorExecutesOnce(t *testing.T) {
	flow := &workflow.Workflow{
		ID:      "r69-end-deterministic",
		Runtime: workflow.RuntimeGo,
		Nodes: workflow.Nodes{
			&nodes.Start{BaseNode: nodes.BaseNode{ID: "start", Next: "boom"}},
			&nodes.End{
				BaseNode: nodes.BaseNode{
					ID:   "boom",
					Next: "never",
					ErrorHandlePolicy: workflow.RecoverableErrorHandlePolicy{
						Strategy:   workflow.HandleStrategyAbort,
						RetryTimes: 3,
					},
				},
				ResultType: nodes.ResultTypeError,
				Error:      &nodes.Error{Code: "400", Message: "business failure"},
			},
		},
	}
	if got := nodeExecutions(t, flow, "boom"); got != 1 {
		t.Fatalf("end error result executed %d times, want exactly 1 (retryTimes=3)", got)
	}
}

// TestExpressionRuntimeErrorExecutesOnce (#56): an integer divide by zero in
// an end-node output expression is deterministic — the same input divides by
// zero again, so the retryTimes budget must not burn (plaitaexpr's recovered
// RuntimeError is bridge-classified deterministic, ValueError-family
// treatment per #43). Pre-fix the goval runtime.Error panics straight through
// Execute.
func TestExpressionRuntimeErrorExecutesOnce(t *testing.T) {
	flow := &workflow.Workflow{
		ID:      "r56-expr-divzero-deterministic",
		Runtime: workflow.RuntimeGo,
		Nodes: workflow.Nodes{
			&nodes.Start{BaseNode: nodes.BaseNode{ID: "start", Next: "boom"}},
			&nodes.End{
				BaseNode: nodes.BaseNode{
					ID:   "boom",
					Next: "never",
					ErrorHandlePolicy: workflow.RecoverableErrorHandlePolicy{
						Strategy:   workflow.HandleStrategyAbort,
						RetryTimes: 2,
					},
					Output: map[string]any{"x": "$F.add(1 / 0, 1)"},
				},
				ResultType: nodes.ResultTypeSuccess,
			},
		},
	}
	if got := nodeExecutions(t, flow, "boom"); got != 1 {
		t.Fatalf("divide-by-zero output executed %d times, want exactly 1 (retryTimes=2)", got)
	}
}

// TestTransientErrorStillRetries: the 1002 (send-phase) class stays
// retryable — the #43 change must not damp transient retries.
func TestTransientErrorStillRetries(t *testing.T) {
	registerProbeNode(t)
	node := &probeNode{
		BaseNode: nodes.BaseNode{
			ID:   "probe",
			Next: "end",
			ErrorHandlePolicy: workflow.RecoverableErrorHandlePolicy{
				Strategy:   workflow.HandleStrategyAbort,
				RetryTimes: 2,
			},
		},
		execErrs: []error{
			errs.Wrap(errors.New("dial fail"), 1002, "send"),
			errs.Wrap(errors.New("dial fail"), 1002, "send"),
		},
	}
	flow := &workflow.Workflow{
		ID:      "r69-transient-retries",
		Runtime: workflow.RuntimeGo,
		Nodes: workflow.Nodes{
			&nodes.Start{BaseNode: nodes.BaseNode{ID: "start", Next: "probe"}},
			node,
			&nodes.End{BaseNode: nodes.BaseNode{ID: "end"}},
		},
	}
	if got := nodeExecutions(t, flow, "probe"); got != 3 {
		t.Fatalf("transient 1002 executed %d times, want 3 (1 + retryTimes=2)", got)
	}
}
