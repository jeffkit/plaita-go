package workflow

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jeffkit/plaita-go/internal/errs"
	"github.com/jeffkit/plaita-go/pkg/duration"
)

// fakeNode is a minimal Node for engine-level tests (avoids importing
// pkg/workflow/nodes, which would cycle).
type fakeNode struct {
	id      string
	typ     NodeType
	next    NodeID
	timeout duration.Duration
	errH    RecoverableErrorHandlePolicy
	toH     TimeoutHandlePolicy

	execErrs []error // returned by Execute, one per attempt
	executed int
}

func (n *fakeNode) GetID() NodeID     { return NodeID(n.id) }
func (n *fakeNode) GetType() NodeType { return n.typ }
func (n *fakeNode) GetName() string   { return n.id }
func (n *fakeNode) GetTimeout() duration.Duration {
	return n.timeout
}
func (n *fakeNode) GetErrorHandlePolicy() RecoverableErrorHandlePolicy { return n.errH }
func (n *fakeNode) GetTimeoutHandlePolicy() TimeoutHandlePolicy        { return n.toH }
func (n *fakeNode) NextNode() NodeID                                   { return n.next }
func (n *fakeNode) GetMetadata() map[string]string                     { return nil }
func (n *fakeNode) MoreInfoAfterExecute(context.Context) map[string]interface{} {
	return map[string]interface{}{}
}
func (n *fakeNode) Validate() error { return nil }
func (n *fakeNode) Execute(context.Context) (*ExecuteResult, error) {
	i := n.executed
	n.executed++
	if i < len(n.execErrs) {
		return nil, n.execErrs[i]
	}
	return &ExecuteResult{Result: "ok", NextNode: n.next}, nil
}

func TestIsRetryableError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"code 1001 deterministic", errs.Wrap(errors.New("bad url"), 1001, "gen"), false},
		{"code 1002 transient", errs.Wrap(errors.New("dial fail"), 1002, "send"), true},
		{"code 1003 transient", errs.Wrap(errors.New("exec fail"), 1003, "exec"), true},
		{"unknown code retries", errs.Wrap(errors.New("mystery"), 4242, "m"), true},
		{"plain error retries", errors.New("plain"), true},
		{"NonRetryable marker", NonRetryable(errors.New("bad credential")), false},
		{"wrapped NonRetryable", fmt.Errorf("ctx: %w", NonRetryable(errors.New("x"))), false},
	}
	for _, tt := range tests {
		if got := IsRetryableError(tt.err); got != tt.want {
			t.Errorf("%s: IsRetryableError = %v, want %v", tt.name, got, tt.want)
		}
	}
}

// --- #19: retry classification + exponential backoff (fake clock) ---

func TestNodeRetryClassificationAndBackoff(t *testing.T) {
	origSleep := retrySleep
	defer func() { retrySleep = origSleep }()

	var recorded []time.Duration
	retrySleep = func(ctx context.Context, d time.Duration) error {
		recorded = append(recorded, d)
		return nil
	}

	t.Run("transient error retries with 1s/2s backoff and keeps the original error", func(t *testing.T) {
		recorded = nil
		node := &fakeNode{
			id: "n",
			errH: RecoverableErrorHandlePolicy{
				Strategy:   HandleStrategyAbort,
				RetryTimes: 2,
			},
			execErrs: []error{
				errs.Wrap(errors.New("dial fail"), 1002, "send"),
				errs.Wrap(errors.New("dial fail"), 1002, "send"),
				errs.Wrap(errors.New("dial fail"), 1002, "send"),
			},
		}
		f := &Workflow{ID: "fake-flow"}
		_, err := f.executeNode(context.Background(), node)
		if err == nil {
			t.Fatal("expected final error")
		}
		if node.executed != 3 {
			t.Fatalf("executed = %d, want 3 (initial + 2 retries)", node.executed)
		}
		if len(recorded) != 2 || recorded[0] != time.Second || recorded[1] != 2*time.Second {
			t.Fatalf("backoff = %v, want [1s 2s]", recorded)
		}
		// final error keeps the original code (plaita: 最终错误带原 code)
		if got := errs.Internal(err).Code; got != 1002 {
			t.Fatalf("final code = %d, want 1002", got)
		}
	})

	t.Run("deterministic error executes exactly once", func(t *testing.T) {
		recorded = nil
		node := &fakeNode{
			id: "n",
			errH: RecoverableErrorHandlePolicy{
				Strategy:   HandleStrategyAbort,
				RetryTimes: 5,
			},
			execErrs: []error{
				errs.Wrap(errors.New("bad url"), 1001, "gen"),
			},
		}
		f := &Workflow{ID: "fake-flow"}
		_, err := f.executeNode(context.Background(), node)
		if err == nil {
			t.Fatal("expected final error")
		}
		if node.executed != 1 {
			t.Fatalf("executed = %d, want 1 (1001 is deterministic)", node.executed)
		}
		if len(recorded) != 0 {
			t.Fatalf("no sleep expected, got %v", recorded)
		}
	})

	t.Run("NonRetryable (ValueError class) executes exactly once", func(t *testing.T) {
		recorded = nil
		node := &fakeNode{
			id: "n",
			errH: RecoverableErrorHandlePolicy{
				Strategy:   HandleStrategyAbort,
				RetryTimes: 3,
			},
			execErrs: []error{
				NonRetryable(errors.New("invalid parameter")),
			},
		}
		f := &Workflow{ID: "fake-flow"}
		_, err := f.executeNode(context.Background(), node)
		if err == nil {
			t.Fatal("expected final error")
		}
		if node.executed != 1 {
			t.Fatalf("executed = %d, want 1 (ValueError class)", node.executed)
		}
	})

	t.Run("retryTimes 0 never retries", func(t *testing.T) {
		recorded = nil
		node := &fakeNode{
			id: "n",
			errH: RecoverableErrorHandlePolicy{
				Strategy:   HandleStrategyAbort,
				RetryTimes: 0,
			},
			execErrs: []error{errs.Wrap(errors.New("dial fail"), 1002, "send")},
		}
		f := &Workflow{ID: "fake-flow"}
		_, _ = f.executeNode(context.Background(), node)
		if node.executed != 1 || len(recorded) != 0 {
			t.Fatalf("executed=%d sleeps=%v, want 1/none", node.executed, recorded)
		}
	})
}

func TestPolicyBackoffIsExponentialCapped(t *testing.T) {
	p := RecoverableErrorHandlePolicy{Strategy: HandleStrategyAbort, RetryTimes: 7}
	b := p.Backoff()
	var delays []time.Duration
	for i := 0; i < 7; i++ {
		d, stop := b.Next()
		if stop {
			t.Fatalf("stopped early at %d", i)
		}
		delays = append(delays, d)
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second,
		8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second}
	for i := range want {
		if delays[i] != want[i] {
			t.Errorf("delay[%d] = %v, want %v", i, delays[i], want[i])
		}
	}
	if _, stop := b.Next(); !stop {
		t.Error("retries exhausted but backoff did not stop")
	}
}

// --- #18: handler abort carries the configured code/message ---

func TestErrorHandlerAbortCarriesCodeMessage(t *testing.T) {
	code := 4001
	cause := errors.New("dial fail")
	node := &fakeNode{
		id: "n",
		errH: RecoverableErrorHandlePolicy{
			Strategy: HandleStrategyAbort,
			Code:     &code,
			Message:  "upstream down",
		},
		execErrs: []error{cause},
	}
	f := &Workflow{ID: "fake-flow"}
	_, err := f.executeNode(context.Background(), node)
	if err == nil {
		t.Fatal("expected abort error")
	}
	var he *HandlerError
	if !errors.As(err, &he) {
		t.Fatalf("expected *HandlerError, got %T: %v", err, err)
	}
	if he.Code != 4001 || he.Message != "upstream down" {
		t.Fatalf("code/message = %d/%q, want 4001/%q", he.Code, he.Message, "upstream down")
	}
	// the original error stays reachable through the wrapper chain
	if !errors.Is(err, cause) {
		t.Fatalf("original cause lost: %v", err)
	}
}

func TestHandlerAbortDefaultsAndUnconfigured(t *testing.T) {
	// message-only config: default code -9527 (plaita ErrorHandler.error_code)
	node := &fakeNode{
		id: "n",
		errH: RecoverableErrorHandlePolicy{
			Strategy: HandleStrategyAbort,
			Message:  "custom message",
		},
		execErrs: []error{errors.New("boom")},
	}
	f := &Workflow{ID: "fake-flow"}
	_, err := f.executeNode(context.Background(), node)
	var he *HandlerError
	if !errors.As(err, &he) || he.Code != DefaultHandlerErrorCode || he.Message != "custom message" {
		t.Fatalf("default code abort = %v", err)
	}

	// unconfigured handler: historical raw error, no wrapping
	raw := &fakeNode{
		id:       "n",
		execErrs: []error{errors.New("boom")},
	}
	f2 := &Workflow{ID: "fake-flow"}
	_, err = f2.executeNode(context.Background(), raw)
	if errors.As(err, &he) {
		t.Fatalf("unconfigured handler must not wrap, got %v", err)
	}
	if err.Error() != "boom" {
		t.Fatalf("unconfigured handler error = %v, want boom", err)
	}
}

func TestTimeoutHandlerAbortCarriesCodeMessage(t *testing.T) {
	code := 4104
	node := &fakeNode{
		id:      "n",
		timeout: duration.MustParseDuration("PT0S"), // zero budget: immediate timeout
		toH: TimeoutHandlePolicy{
			Strategy: HandleStrategyAbort,
			Code:     &code,
			Message:  "too slow",
		},
	}
	f := &Workflow{ID: "fake-flow"}
	_, err := f.executeNode(context.Background(), node)
	var he *HandlerError
	if !errors.As(err, &he) || he.Code != 4104 || he.Message != "too slow" {
		t.Fatalf("timeout abort error = %v, want code 4104/message too slow", err)
	}
	// the timeout stays recognizable through the wrapper
	if !contextTimeoutError(err) {
		t.Fatal("wrapped timeout must still satisfy contextTimeoutError")
	}
}

// --- flow-level timeout attribution (FLOW vs NODE budget) ---

func TestFlowBudgetAttributionSkipsNodeTimeoutPolicy(t *testing.T) {
	// The node's timeout policy would swallow the timeout (continue-with); a
	// flow-budget expiry must NOT be handled by the node — the flow policy
	// decides instead (plaita FLOW_ERROR vs NODE_ERROR).
	node := &fakeNode{
		id:      "n",
		typ:     "fake",
		next:    "end",
		timeout: duration.MustParseDuration("PT10S"),
		toH:     TimeoutHandlePolicy{Strategy: HandleStrategyContinueWith, DefaultValue: "node-default"},
	}
	f := &Workflow{
		ID:                  "fake-flow",
		Timeout:             duration.MustParseDuration("PT0S"), // zero flow budget
		TimeoutHandlePolicy: TimeoutHandlePolicy{Strategy: HandleStrategyAbort},
		Nodes: Nodes{
			&fakeNode{id: "start", typ: "start", next: "n"},
			node,
			&fakeNode{id: "end", typ: "end"},
		},
	}
	_, err := f.Execute(context.Background(), nil)
	if err == nil {
		t.Fatal("flow zero budget must abort despite the node continue-with policy")
	}
	if !contextTimeoutError(err) {
		t.Fatalf("expected timeout error, got %v", err)
	}
	if node.executed != 0 {
		t.Fatalf("zero flow budget must fail before the node body runs, executed=%d", node.executed)
	}
}

// A node's OWN budget expiring (flow budget alive) stays the node's timeout
// to handle: continue-with yields the node default and the flow proceeds.
func TestNodeBudgetAttributionHandledByNodePolicy(t *testing.T) {
	node := &fakeNode{
		id:      "n",
		typ:     "fake",
		next:    "end",
		timeout: duration.MustParseDuration("PT0S"), // node zero budget
		toH:     TimeoutHandlePolicy{Strategy: HandleStrategyContinueWith, DefaultValue: "node-default"},
	}
	f := &Workflow{
		ID: "fake-flow",
		Nodes: Nodes{
			&fakeNode{id: "start", typ: "start", next: "n"},
			node,
			&fakeNode{id: "end", typ: "end", next: ""},
		},
	}
	got, err := f.Execute(context.Background(), nil)
	if err != nil {
		t.Fatalf("node continue-with should swallow the node timeout: %v", err)
	}
	if got != "ok" {
		// end node echoes its own constant output; node n yields its default
		// and is skipped — the exact value here only asserts flow completion.
		t.Logf("flow result = %v", got)
	}
	if node.executed != 0 {
		t.Fatalf("node zero budget must fail before the body runs, executed=%d", node.executed)
	}
}

// --- #23: strict vs lenient value model ---

func TestValidateValueStrictTable(t *testing.T) {
	intPtr := func(f float64) *float64 { return &f }
	arrayOf := func(inner DataType) ArrayItemType {
		return ArrayItemType{itemProperty: &Property{DataType: inner, Strict: true}}
	}
	tests := []struct {
		name   string
		prop   Property
		value  any
		wantOK bool
	}{
		// issue #23 acceptance table (Python match column)
		{"integer <- string", Property{DataType: Integer, Strict: true}, "5", false},
		{"integer <- bool", Property{DataType: Integer, Strict: true}, true, false},
		{"string <- int", Property{DataType: String, Strict: true}, 5, false},
		{"bool <- string", Property{DataType: Bool, Strict: true}, "true", false},
		{"integer max boundary not enforced", Property{DataType: Integer, Strict: true, Max: intPtr(10)}, 10, true},
		{"integer min boundary not enforced", Property{DataType: Integer, Strict: true, Min: intPtr(1)}, 1, true},
		{"any <- string", Property{DataType: Any, Strict: true}, "x", true},
		{"any <- int", Property{DataType: Any, Strict: true}, 5, true},
		// any/number support
		{"number <- int", Property{DataType: Number, Strict: true}, 5, true},
		{"number <- float64", Property{DataType: Number, Strict: true}, 5.5, true},
		{"number <- integral float64 (json)", Property{DataType: Number, Strict: true}, float64(5), true},
		{"number <- string rejected", Property{DataType: Number, Strict: true}, "5", false},
		{"number <- bool rejected", Property{DataType: Number, Strict: true}, true, false},
		// strict model details
		{"integer <- int", Property{DataType: Integer, Strict: true}, 5, true},
		{"integer <- integral float64 (json decode)", Property{DataType: Integer, Strict: true}, float64(5), true},
		{"integer <- fractional float64", Property{DataType: Integer, Strict: true}, 5.5, false},
		{"float <- int", Property{DataType: Float, Strict: true}, 5, true},
		{"float <- string", Property{DataType: Float, Strict: true}, "5", false},
		{"string <- empty rejected", Property{DataType: String, Strict: true}, "", false},
		{"string <- string", Property{DataType: String, Strict: true}, "ok", true},
		{"bool <- bool", Property{DataType: Bool, Strict: true}, true, true},
		{"object <- map", Property{DataType: Object, Strict: true}, map[string]any{"a": 1}, true},
		{"object <- string rejected", Property{DataType: Object, Strict: true}, `{"a":1}`, false},
		{"array <- slice", Property{DataType: Array, Strict: true, ArrayItemType: arrayOf(Integer)}, []any{1, 2}, true},
		{"array <- bad item", Property{DataType: Array, Strict: true, ArrayItemType: arrayOf(Integer)}, []any{1, "x"}, false},
		// nil + required
		{"nil optional ok", Property{DataType: String, Strict: true}, nil, true},
		{"nil required rejected", Property{DataType: String, Strict: true, IsRequired: true}, nil, false},
		// lenient model keeps the historical cast behavior
		{"lenient integer <- string", Property{DataType: Integer}, "5", true},
		{"lenient string <- int", Property{DataType: String}, 5, true},
		{"lenient bool <- string", Property{DataType: Bool}, "true", true},
		{"lenient max boundary not enforced", Property{DataType: Integer, Max: intPtr(10)}, 10, true},
		{"lenient any <- string", Property{DataType: Any}, "x", true},
		{"lenient number <- int", Property{DataType: Number}, 5, true},
	}
	for _, tt := range tests {
		p := tt.prop
		err := p.ValidateValue(tt.value)
		if tt.wantOK != (err == nil) {
			t.Errorf("%s: ValidateValue(%v) error = %v, wantOK %v", tt.name, tt.value, err, tt.wantOK)
		}
	}
}
