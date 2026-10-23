package nodes

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jeffkit/plaita-go/pkg/duration"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// #49: BaseNode.Validate must enforce the budget caps itself — ozzo does not
// dispatch pointer-receiver Validatable on value struct fields, so leaving
// the policies as bare Field() rules meant neither retryTimes nor the
// node-timeout cap ever ran at this layer.

func TestBaseNodeValidateBudgetCaps(t *testing.T) {
	t.Run("node retryTimes=11 is a named rejection", func(t *testing.T) {
		n := &BaseNode{ID: "n", ErrorHandlePolicy: workflow.RecoverableErrorHandlePolicy{Strategy: workflow.HandleStrategyAbort, RetryTimes: 11}}
		err := n.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "cap 10")
	})
	t.Run("node retryTimes=1e9 is a named rejection", func(t *testing.T) {
		n := &BaseNode{ID: "n", ErrorHandlePolicy: workflow.RecoverableErrorHandlePolicy{Strategy: workflow.HandleStrategyAbort, RetryTimes: 1_000_000_000}}
		err := n.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "exceeds the cap 10")
	})
	t.Run("node timeout beyond MaxNodeTimeout is a named rejection", func(t *testing.T) {
		n := &BaseNode{ID: "n", Timeout: duration.FromTimeDuration(workflow.MaxNodeTimeout + time.Second)}
		err := n.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), workflow.MaxNodeTimeout.String())
	})
	t.Run("caps-edge node (retryTimes=10, timeout=24h) is legal", func(t *testing.T) {
		n := &BaseNode{
			ID:                "n",
			Timeout:           duration.FromTimeDuration(workflow.MaxNodeTimeout),
			ErrorHandlePolicy: workflow.RecoverableErrorHandlePolicy{Strategy: workflow.HandleStrategyContinue, RetryTimes: workflow.MaxRetryTimes},
		}
		require.NoError(t, n.Validate())
	})
	t.Run("node errorHandler strategy=bogus is rejected", func(t *testing.T) {
		n := &BaseNode{ID: "n", ErrorHandlePolicy: workflow.RecoverableErrorHandlePolicy{Strategy: "bogus"}}
		err := n.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "strategy")
	})
	t.Run("node timeoutHandler strategy=bogus is rejected", func(t *testing.T) {
		n := &BaseNode{ID: "n", TimeoutHandlePolicy: workflow.TimeoutHandlePolicy{Strategy: "bogus"}}
		err := n.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "strategy")
	})
	t.Run("zero-value node stays legal", func(t *testing.T) {
		require.NoError(t, (&BaseNode{ID: "n"}).Validate())
	})
}
