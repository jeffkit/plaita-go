package workflow

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jeffkit/plaita-go/pkg/duration"
)

// #49: the engine-layer budget caps must be wired into Workflow.Validate.
// RecoverableErrorHandlePolicy.Validate existed but nothing called it
// (ozzo does not dispatch pointer-receiver Validatable on value struct
// fields), so retryTimes=1e9 and bogus strategies passed Validate at the
// documented bare-engine entrypoints.

func capsProbeFlow(errH RecoverableErrorHandlePolicy, toH TimeoutHandlePolicy) *Workflow {
	return &Workflow{
		ID:                  "budget-probe",
		Runtime:             RuntimeGo,
		Version:             "1",
		ErrorHandlePolicy:   errH,
		TimeoutHandlePolicy: toH,
		Nodes: Nodes{
			&fakeNode{id: "start", typ: "start", next: "n"},
			&fakeNode{id: "n", typ: "noop", next: "end"},
			&fakeNode{id: "end", typ: "end"},
		},
	}
}

func TestWorkflowValidateCapsFlowRetryTimes(t *testing.T) {
	t.Run("retryTimes=1e9 is a named rejection", func(t *testing.T) {
		err := capsProbeFlow(RecoverableErrorHandlePolicy{Strategy: HandleStrategyAbort, RetryTimes: 1_000_000_000}, TimeoutHandlePolicy{}).Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "exceeds the cap 10")
	})
	t.Run("retryTimes=11 is a named rejection", func(t *testing.T) {
		err := capsProbeFlow(RecoverableErrorHandlePolicy{Strategy: HandleStrategyAbort, RetryTimes: 11}, TimeoutHandlePolicy{}).Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "cap 10")
	})
	t.Run("retryTimes=10 is legal", func(t *testing.T) {
		require.NoError(t, capsProbeFlow(RecoverableErrorHandlePolicy{Strategy: HandleStrategyAbort, RetryTimes: MaxRetryTimes}, TimeoutHandlePolicy{}).Validate())
	})
}

func TestWorkflowValidateCapsHandlerStrategies(t *testing.T) {
	t.Run("flow errorHandler strategy=bogus is rejected", func(t *testing.T) {
		err := capsProbeFlow(RecoverableErrorHandlePolicy{Strategy: "bogus"}, TimeoutHandlePolicy{}).Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "strategy")
	})
	t.Run("flow timeoutHandler strategy=bogus is rejected", func(t *testing.T) {
		err := capsProbeFlow(RecoverableErrorHandlePolicy{}, TimeoutHandlePolicy{Strategy: "bogus"}).Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "strategy")
	})
}

// The flow-level timeout is deliberately NOT capped (#49 keeps the existing
// decision): it is an operator budget that composes with the worker
// execution deadline at runtime. A legal flow (caps respected) still loads
// and executes.
func TestWorkflowValidateLegalFlowExecutes(t *testing.T) {
	f := capsProbeFlow(
		RecoverableErrorHandlePolicy{Strategy: HandleStrategyAbort, RetryTimes: MaxRetryTimes},
		TimeoutHandlePolicy{},
	)
	f.Timeout = duration.FromTimeDuration(MaxNodeTimeout)
	require.NoError(t, f.Validate())
	_, err := f.Execute(context.Background(), nil)
	require.NoError(t, err)
}
