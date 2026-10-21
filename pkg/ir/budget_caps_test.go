package ir

// Budget caps at the deployment-form boundary (#35, plaita#267): a legal
// flow must not be able to express an unbounded occupancy of a shared
// single-consumer worker. plaita caps these at the model layer (r47:
// retryTimes ≤ 10; #267: node.timeout ≤ 24h); the IR loader is this repo's
// model boundary, so the same caps reject at load time with named errors.
// The flow-level timeout is deliberately NOT capped (explicit operator
// budget; the worker execution deadline caps occupancy at runtime).

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func irDocWith(fragment string) []byte {
	return []byte(`{
		"nodes": [
			{"type": "start", "id": "s", "next": "a"},
			{"type": "assignment", "id": "a", "next": "e"` + fragment + `},
			{"type": "end", "id": "e"}
		]
	}`)
}

func TestLoadBudgetCaps(t *testing.T) {
	t.Run("node retryTimes over cap rejected", func(t *testing.T) {
		_, err := Load([]byte(`{
			"errorHandler": {"strategy": "retry", "retryTimes": 11},
			"nodes": [
				{"type": "start", "id": "s", "next": "a"},
				{"type": "assignment", "id": "a", "errorHandler": {"strategy": "retry", "retryTimes": 11}},
				{"type": "end", "id": "e"}
			]
		}`))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "retryTimes 11 exceeds the cap 10")
	})

	t.Run("flow retryTimes over cap rejected", func(t *testing.T) {
		_, err := Load([]byte(`{
			"errorHandler": {"strategy": "retry", "retryTimes": 1000000000},
			"nodes": [
				{"type": "start", "id": "s", "next": "a"},
				{"type": "assignment", "id": "a"},
				{"type": "end", "id": "e"}
			]
		}`))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "retryTimes 1000000000 exceeds the cap 10")
	})

	t.Run("retryTimes at the cap loads", func(t *testing.T) {
		_, err := Load([]byte(`{
			"errorHandler": {"strategy": "retry", "retryTimes": 10},
			"nodes": [
				{"type": "start", "id": "s", "next": "a"},
				{"type": "assignment", "id": "a", "next": "e", "errorHandler": {"strategy": "retry", "retryTimes": 10}},
				{"type": "end", "id": "e"}
			]
		}`))
		require.NoError(t, err)
	})

	t.Run("node timeout past 24h rejected", func(t *testing.T) {
		_, err := Load(irDocWith(`,"timeout": "PT90000S"`)) // 25h
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exceeds the 24h0m0s cap")
	})

	t.Run("node timeout 24h+1ms via millis rejected", func(t *testing.T) {
		_, err := Load(irDocWith(`,"timeout": 86400001`))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exceeds the 24h0m0s cap")
	})

	t.Run("node timeout at 24h loads", func(t *testing.T) {
		_, err := Load(irDocWith(`,"timeout": "PT86400S"`))
		require.NoError(t, err)
	})

	t.Run("node zero timeout (strictest budget) loads", func(t *testing.T) {
		_, err := Load(irDocWith(`,"timeout": "PT0S"`))
		require.NoError(t, err)
	})

	t.Run("flow-level timeout is not capped (worker deadline caps at runtime)", func(t *testing.T) {
		_, err := Load([]byte(`{
			"timeout": "PT876000H",
			"nodes": [
				{"type": "start", "id": "s", "next": "a"},
				{"type": "assignment", "id": "a", "next": "e"},
				{"type": "end", "id": "e"}
			]
		}`))
		require.NoError(t, err)
	})
}

// The engine model validates the same cap (plaita caps at the pydantic model;
// the engine policy struct is this repo's model), covering non-IR callers.
func TestEngineRetryTimesCap(t *testing.T) {
	over := workflow.RecoverableErrorHandlePolicy{Strategy: workflow.HandleStrategyAbort, RetryTimes: 11}
	assert.Error(t, over.Validate())

	at := workflow.RecoverableErrorHandlePolicy{Strategy: workflow.HandleStrategyAbort, RetryTimes: 10}
	assert.NoError(t, at.Validate())
}
