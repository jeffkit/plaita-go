package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jeffkit/plaita-go/internal/errs"
	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/duration"
	"github.com/jeffkit/plaita-go/pkg/expression"
)

// wiring_test.go — the engine-wiring wave: $BRANCH system root (#6/#17),
// Workflow reentry guard (#17), runtime-context locking (#17) and the
// retry classification wiring (#19 / AGENTS.md 接线缺口①).

// wiringNode is a configurable Node for these tests: result is the Execute
// output, expr (when non-empty) is evaluated against the runtime context and
// becomes the output (lets a node read $BRANCH/$NODE mid-flow).
type wiringNode struct {
	id       string
	typ      NodeType
	next     NodeID
	result   any
	expr     string
	errs     []error // returned once per Execute attempt, then result
	executed atomic.Int64
	sleep    time.Duration
}

func (n *wiringNode) GetID() NodeID     { return NodeID(n.id) }
func (n *wiringNode) GetType() NodeType { return n.typ }
func (n *wiringNode) GetName() string   { return n.id }
func (n *wiringNode) GetTimeout() duration.Duration {
	return duration.Duration{}
}
func (n *wiringNode) GetErrorHandlePolicy() RecoverableErrorHandlePolicy {
	return RecoverableErrorHandlePolicy{}
}
func (n *wiringNode) GetTimeoutHandlePolicy() TimeoutHandlePolicy { return TimeoutHandlePolicy{} }
func (n *wiringNode) NextNode() NodeID                            { return n.next }
func (n *wiringNode) GetMetadata() map[string]string              { return nil }
func (n *wiringNode) MoreInfoAfterExecute(context.Context) map[string]interface{} {
	return map[string]interface{}{}
}
func (n *wiringNode) Validate() error { return nil }
func (n *wiringNode) Execute(ctx context.Context) (*ExecuteResult, error) {
	n.executed.Add(1)
	if n.sleep > 0 {
		time.Sleep(n.sleep)
	}
	if i := int(n.executed.Load()) - 1; i < len(n.errs) {
		return nil, n.errs[i]
	}
	if n.expr != "" {
		v, err := expression.Eval(ctx, n.expr)
		if err != nil {
			return nil, err
		}
		return &ExecuteResult{Result: v, NextNode: n.next}, nil
	}
	return &ExecuteResult{Result: n.result, NextNode: n.next}, nil
}

// --- #17: Workflow reentry guard ---

func TestWorkflowExecuteReentryGuard(t *testing.T) {
	newFlow := func() *Workflow {
		return &Workflow{
			ID: "reentry-flow",
			Nodes: Nodes{
				&wiringNode{id: "start", typ: types.NodeTypeStart, next: "end", sleep: 50 * time.Millisecond},
				&wiringNode{id: "end", typ: types.NodeTypeEnd, result: "done"},
			},
		}
	}

	t.Run("concurrent Execute on one instance: exactly one wins", func(t *testing.T) {
		f := newFlow()
		const n = 10
		var wg sync.WaitGroup
		var okCount atomic.Int64
		var reentryCount atomic.Int64
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := f.Execute(context.Background(), nil)
				switch {
				case err == nil:
					okCount.Add(1)
				case errors.Is(err, ErrWorkflowReentry):
					reentryCount.Add(1)
				default:
					t.Errorf("unexpected error: %v", err)
				}
			}()
		}
		wg.Wait()
		require.Equal(t, int64(1), okCount.Load(), "exactly one concurrent run may execute")
		require.Equal(t, int64(n-1), reentryCount.Load(), "the rest must get ErrWorkflowReentry")
	})

	t.Run("flag released after the run: sequential re-run works", func(t *testing.T) {
		f := newFlow()
		f.Nodes[0].(*wiringNode).sleep = 0
		got, err := f.Execute(context.Background(), nil)
		require.NoError(t, err)
		require.Equal(t, "done", got)
		got, err = f.Execute(context.Background(), nil)
		require.NoError(t, err, "second sequential Execute must not be rejected")
		require.Equal(t, "done", got)
	})

	t.Run("flag released after a failed run", func(t *testing.T) {
		f := &Workflow{
			ID: "reentry-fail-flow",
			Nodes: Nodes{
				&wiringNode{id: "start", typ: types.NodeTypeStart, next: "end",
					errs: []error{errors.New("boom")}},
				&wiringNode{id: "end", typ: types.NodeTypeEnd, result: "done"},
			},
		}
		_, err := f.Execute(context.Background(), nil)
		require.Error(t, err)
		_, err = f.Execute(context.Background(), nil)
		require.NoError(t, err, "Execute after a failed run must not be rejected")
	})
}

// --- #6/#17: $BRANCH system root ---

func TestExecuteWritesLastBranch(t *testing.T) {
	// start → switch (no-op fake with a branch target result) → end reads
	// $BRANCH. The engine records last_branch in executeNode for branching
	// nodes (plaita runner: context.last_branch), and the end node sees it
	// as the $BRANCH expression root.
	sw := &wiringNode{id: "sw", typ: types.NodeTypeSwitch, next: "end", result: "e1"}
	end := &wiringNode{id: "end", typ: types.NodeTypeEnd, expr: "$BRANCH"}
	f := &Workflow{
		ID: "branch-flow",
		Nodes: Nodes{
			&wiringNode{id: "start", typ: types.NodeTypeStart, next: "sw"},
			sw,
			end,
		},
	}
	got, err := f.Execute(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, "e1", got, "$BRANCH must resolve to the hit branch target")

	// a non-branching flow leaves $BRANCH untouched (nil, plaita None).
	end2 := &wiringNode{id: "end", typ: types.NodeTypeEnd, expr: "$BRANCH"}
	f2 := &Workflow{
		ID: "no-branch-flow",
		Nodes: Nodes{
			&wiringNode{id: "start", typ: types.NodeTypeStart, next: "end"},
			end2,
		},
	}
	got, err = f2.Execute(context.Background(), nil)
	require.NoError(t, err)
	require.Nil(t, got, "$BRANCH stays nil without a branching node")
}

func TestLastBranchCheckpointRoundTrip(t *testing.T) {
	rc := runtime.NewContext()
	rc.WithLastBranch("e1")
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), rc)

	snap, err := SnapshotRuntimeContext(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, "e1", snap["$BRANCH"], "checkpoint must carry $BRANCH (plaita SYSTEM_KEY_NAMES)")

	restored, err := RestoreRuntimeContext(context.Background(), snap)
	require.NoError(t, err)
	require.Equal(t, "e1", runtime.WorkflowRuntimeContext(restored).GetLastBranch())

	// a context without a branch does not invent the key.
	rc2 := runtime.NewContext()
	snap2, err := SnapshotRuntimeContext(
		runtime.WithWorkflowRuntimeContext(context.Background(), rc2), nil)
	require.NoError(t, err)
	require.NotContains(t, snap2, "$BRANCH")
}

// --- #19 / 接线缺口①: retry classification wiring ---

// fakeAPIError stands in for *llm.APIError: classification crosses the
// engine boundary through the structural Retryable() bool contract, not an
// import.
type fakeAPIError struct{ status int }

func (e *fakeAPIError) Error() string { return "LLM 请求失败" }

func (e *fakeAPIError) Retryable() bool {
	return e.status >= 500 || e.status == 408 || e.status == 425 || e.status == 429
}

// HitlError stands in for nodes.HitlError: the engine bridges node-side
// deterministic types by type NAME (import cycle).
type HitlError struct{ msg string }

func (e *HitlError) Error() string { return e.msg }

func TestIsRetryableErrorTypedClassification(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"APIError 500 transient", &fakeAPIError{status: 500}, true},
		{"APIError 429 transient", &fakeAPIError{status: 429}, true},
		{"APIError 401 deterministic", &fakeAPIError{status: 401}, false},
		{"APIError 404 deterministic", &fakeAPIError{status: 404}, false},
		{"wrapped APIError verdict surfaces", fmt.Errorf("node: %w", &fakeAPIError{status: 403}), false},
		{"deterministic wrapper wins over inner classifier",
			fmt.Errorf("ctx: %w", NonRetryable(&fakeAPIError{status: 500})), false},
		{"HitlError (server rejection) deterministic", &HitlError{msg: "hitl-server 发送失败"}, false},
		{"wrapped HitlError deterministic", fmt.Errorf("send: %w", &HitlError{msg: "x"}), false},
		{"1001 stays deterministic", errs.Wrap(errors.New("bad url"), 1001, "gen"), false},
		{"1002 stays transient", errs.Wrap(errors.New("dial"), 1002, "send"), true},
	}
	for _, tt := range tests {
		if got := IsRetryableError(tt.err); got != tt.want {
			t.Errorf("%s: IsRetryableError = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestExecuteNodeTypedClassificationCounts(t *testing.T) {
	origSleep := retrySleep
	defer func() { retrySleep = origSleep }()
	retrySleep = func(context.Context, time.Duration) error { return nil }

	t.Run("self-classified deterministic error executes once despite retryTimes", func(t *testing.T) {
		node := &fakeNode{
			id: "n",
			errH: RecoverableErrorHandlePolicy{
				Strategy:   HandleStrategyAbort,
				RetryTimes: 5,
			},
			execErrs: []error{&fakeAPIError{status: 401}},
		}
		f := &Workflow{ID: "fake-flow"}
		_, err := f.executeNode(context.Background(), node)
		require.Error(t, err)
		require.Equal(t, 1, node.executed, "401 is deterministic — no retry")
	})

	t.Run("self-classified transient error retries with backoff", func(t *testing.T) {
		var sleeps []time.Duration
		retrySleep = func(_ context.Context, d time.Duration) error {
			sleeps = append(sleeps, d)
			return nil
		}
		node := &fakeNode{
			id: "n",
			errH: RecoverableErrorHandlePolicy{
				Strategy:   HandleStrategyAbort,
				RetryTimes: 2,
			},
			execErrs: []error{
				&fakeAPIError{status: 500},
				&fakeAPIError{status: 500},
				&fakeAPIError{status: 500},
			},
		}
		f := &Workflow{ID: "fake-flow"}
		_, err := f.executeNode(context.Background(), node)
		require.Error(t, err)
		require.Equal(t, 3, node.executed, "initial + 2 retries")
		require.Equal(t, []time.Duration{time.Second, 2 * time.Second}, sleeps, "exponential backoff 1s/2s")
	})

	t.Run("HitlError executes once despite retryTimes", func(t *testing.T) {
		node := &fakeNode{
			id: "n",
			errH: RecoverableErrorHandlePolicy{
				Strategy:   HandleStrategyAbort,
				RetryTimes: 3,
			},
			execErrs: []error{&HitlError{msg: "hitl-server 拒绝"}},
		}
		f := &Workflow{ID: "fake-flow"}
		_, err := f.executeNode(context.Background(), node)
		require.Error(t, err)
		require.Equal(t, 1, node.executed)
	})
}

func TestFlowRetrySkipsDeterministicErrors(t *testing.T) {
	t.Run("deterministic node error: no full-flow re-execution", func(t *testing.T) {
		node := &fakeNode{
			id:   "n",
			typ:  "fake",
			next: "end",
			errH: RecoverableErrorHandlePolicy{Strategy: HandleStrategyAbort, RetryTimes: 0},
			execErrs: []error{
				errs.Wrap(errors.New("bad url"), 1001, "gen"),
				errs.Wrap(errors.New("bad url"), 1001, "gen"),
				errs.Wrap(errors.New("bad url"), 1001, "gen"),
				errs.Wrap(errors.New("bad url"), 1001, "gen"),
			},
		}
		f := &Workflow{
			ID:                "flow-retry",
			ErrorHandlePolicy: RecoverableErrorHandlePolicy{Strategy: HandleStrategyAbort, RetryTimes: 3},
			Nodes: Nodes{
				&fakeNode{id: "start", typ: "start", next: "n"},
				node,
				&fakeNode{id: "end", typ: "end"},
			},
		}
		_, err := f.Execute(context.Background(), nil)
		require.Error(t, err)
		require.Equal(t, 1, node.executed,
			"deterministic error must not buy flow-level re-executions")
	})

	t.Run("transient node error: flow re-executes RetryTimes times", func(t *testing.T) {
		node := &fakeNode{
			id:   "n",
			typ:  "fake",
			next: "end",
			errH: RecoverableErrorHandlePolicy{Strategy: HandleStrategyAbort, RetryTimes: 0},
			execErrs: []error{
				errs.Wrap(errors.New("dial fail"), 1002, "send"),
				errs.Wrap(errors.New("dial fail"), 1002, "send"),
				errs.Wrap(errors.New("dial fail"), 1002, "send"),
				errs.Wrap(errors.New("dial fail"), 1002, "send"),
			},
		}
		f := &Workflow{
			ID:                "flow-retry",
			ErrorHandlePolicy: RecoverableErrorHandlePolicy{Strategy: HandleStrategyAbort, RetryTimes: 2},
			Nodes: Nodes{
				&fakeNode{id: "start", typ: "start", next: "n"},
				node,
				&fakeNode{id: "end", typ: "end"},
			},
		}
		_, err := f.Execute(context.Background(), nil)
		require.Error(t, err)
		require.Equal(t, 3, node.executed, "initial run + 2 flow retries")
	})
}
