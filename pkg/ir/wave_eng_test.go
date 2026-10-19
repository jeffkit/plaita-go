package ir

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// --- #1 exposeEnv: load contract ---

func TestLoadExposeEnv(t *testing.T) {
	data := []byte(`{
		"flow_id": "env-flow",
		"expose_env": ["PLAITA_TEST_HOME"],
		"nodes": [
			{"type": "start", "id": "s", "next": "e"},
			{"type": "end", "id": "e", "resultType": "success", "output": "$ENV.PLAITA_TEST_SECRET"}
		]
	}`)
	wf, err := Load(data)
	require.NoError(t, err)
	// both spellings fold onto the engine field; the secret must not be in $ENV.
	require.Equal(t, []string{"PLAITA_TEST_HOME"}, wf.ExposeEnv)

	t.Setenv("PLAITA_TEST_HOME", "/home/x")
	t.Setenv("PLAITA_TEST_SECRET", "TOP-SECRET-VALUE")
	got, err := wf.Execute(t.Context(), map[string]any{})
	require.NoError(t, err)
	assert.Nil(t, got, "$ENV.PLAITA_TEST_SECRET must not be readable without the allowlist entry")

	// allowlisted key is readable
	wf2, err := Load([]byte(`{
		"flow_id": "env-flow",
		"exposeEnv": ["PLAITA_TEST_HOME"],
		"nodes": [
			{"type": "start", "id": "s", "next": "e"},
			{"type": "end", "id": "e", "resultType": "success", "output": "$ENV.PLAITA_TEST_HOME"}
		]
	}`))
	require.NoError(t, err)
	got, err = wf2.Execute(t.Context(), map[string]any{})
	require.NoError(t, err)
	assert.Equal(t, "/home/x", got)

	// undeclared allowlist → nothing is exposed (fail-closed)
	wf3, err := Load([]byte(`{
		"flow_id": "env-flow",
		"nodes": [
			{"type": "start", "id": "s", "next": "e"},
			{"type": "end", "id": "e", "resultType": "success", "output": "$F.getDictValues($ENV)"}
		]
	}`))
	require.NoError(t, err)
	got, err = wf3.Execute(t.Context(), map[string]any{})
	require.NoError(t, err)
	assert.Empty(t, got, "undeclared exposeEnv must expose nothing")
}

func TestLoadExposeEnvRejectsIllegalEntries(t *testing.T) {
	for _, bad := range []string{`["*"]`, `["HOME *"]`, `[" HOME"]`, `[""]`} {
		data := []byte(`{
			"flow_id": "env-flow",
			"exposeEnv": ` + bad + `,
			"nodes": [
				{"type": "start", "id": "s", "next": "e"},
				{"type": "end", "id": "e", "resultType": "success", "output": 1}
			]
		}`)
		_, err := Load(data)
		assert.Error(t, err, "exposeEnv %s must be rejected at load", bad)
	}
}

// plaita#141: a child flow without its own exposeEnv inherits the parent's
// filtered env; a declared one re-filters the process env.
func TestChildEnvInheritance(t *testing.T) {
	t.Setenv("PLAITA_TEST_PARENT", "from-parent")
	t.Setenv("PLAITA_TEST_CHILD", "child-only")
	wf, err := Load([]byte(`{
		"flow_id": "env-parent",
		"exposeEnv": ["PLAITA_TEST_PARENT"],
		"inputType": {"dataType": "object"},
		"nodes": [
			{"type": "start", "id": "s", "next": "c"},
			{"type": "child", "id": "c", "input": {"k": "v"}, "next": "ret",
				"childFlow": {
					"flow_id": "env-child",
					"nodes": [
						{"type": "start", "id": "s", "next": "e"},
						{"type": "end", "id": "e", "resultType": "success",
						 "output": {"p": "$ENV.PLAITA_TEST_PARENT", "c": "$ENV.PLAITA_TEST_CHILD"}}
					]
				}},
			{"type": "end", "id": "ret", "resultType": "success", "output": "$NODE.c"}
		]
	}`))
	require.NoError(t, err)
	got, err := wf.Execute(t.Context(), map[string]any{})
	require.NoError(t, err)
	// child inherits the parent's allowlist result; the undeclared child key
	// stays invisible even though it exists in the process env.
	assert.Equal(t, map[string]any{"p": "from-parent", "c": nil}, got)
}

// --- #2 sub-flow context: $PARENT / $GLOBAL ---

func TestSubflowParentAndGlobal(t *testing.T) {
	load := func(t *testing.T, doc string) *workflow.Workflow {
		t.Helper()
		wf, err := Load([]byte(doc))
		require.NoError(t, err)
		return wf
	}

	t.Run("child sees $PARENT.INPUT and $GLOBAL", func(t *testing.T) {
		wf := load(t, `{
			"flow_id": "p-root", "globalContext": {"g": 5},
			"inputType": {"dataType": "object"},
			"nodes": [
				{"type": "start", "id": "s", "next": "c"},
				{"type": "child", "id": "c", "input": {"k": 1}, "next": "ret",
					"childFlow": {"flow_id": "p-child", "nodes": [
						{"type": "start", "id": "s", "next": "e"},
						{"type": "end", "id": "e", "resultType": "success",
						 "output": {"px": "$PARENT.INPUT.x", "g": "$GLOBAL.g", "own": "$INPUT.k"}}
					]}
				},
				{"type": "end", "id": "ret", "resultType": "success", "output": "$NODE.c"}
			]
		}`)
		got, err := wf.Execute(t.Context(), map[string]any{"x": 5})
		require.NoError(t, err)
		// $PARENT.INPUT is the live parent input (Go map, int 5); $GLOBAL comes
		// from the IR context (decoded number → float64); $INPUT.k is the
		// evaluated child input (IR number → float64).
		assert.Equal(t, map[string]any{"px": 5, "g": float64(5), "own": float64(1)}, got)
	})

	t.Run("loop sees $PARENT.INPUT and $GLOBAL", func(t *testing.T) {
		wf := load(t, `{
			"flow_id": "p-loop-root", "globalContext": {"g": 7},
			"inputType": {"dataType": "object"},
			"nodes": [
				{"type": "start", "id": "s", "next": "l"},
				{"type": "loop", "id": "l", "collection": "$INPUT.items", "next": "ret",
					"childFlow": {"flow_id": "p-loop-child", "nodes": [
						{"type": "start", "id": "s", "next": "e"},
						{"type": "end", "id": "e", "resultType": "success",
						 "output": "$F.add($PARENT.INPUT.x, $GLOBAL.g)"}
					]}
				},
				{"type": "end", "id": "ret", "resultType": "success", "output": "$NODE.l"}
			]
		}`)
		got, err := wf.Execute(t.Context(), map[string]any{"x": 5, "items": []any{1, 2}})
		require.NoError(t, err)
		assert.Equal(t, float64(12), got, "5 + 7 per iteration; loop output = last child result")
	})

	t.Run("parallel branch sees $PARENT.INPUT and $GLOBAL", func(t *testing.T) {
		// Join branches are the observable ones; both read the parent chain.
		// (Background branches clone the parent runtime context concurrently
		// with the parent writing node results — a pre-existing lockless-clone
		// race upstream, out of this wave's scope; $PARENT/$GLOBAL wiring for
		// those branches is the same code path as join branches.)
		wf := load(t, `{
			"flow_id": "p-par-root", "globalContext": {"g": 3},
			"inputType": {"dataType": "object"},
			"nodes": [
				{"type": "start", "id": "s", "next": "p"},
				{"type": "parallel", "id": "p",
					"branches": [
						{"name": "a", "flow": {"flow_id": "p-par-a", "nodes": [
							{"type": "start", "id": "s", "next": "e"},
							{"type": "end", "id": "e", "resultType": "success",
							 "output": "$F.add($PARENT.INPUT.x, $GLOBAL.g)"}
						]}},
						{"name": "b", "flow": {"flow_id": "p-par-b", "nodes": [
							{"type": "start", "id": "s", "next": "e"},
							{"type": "end", "id": "e", "resultType": "success",
							 "output": "$GLOBAL.g"}
						]}}
					],
					"joinBranches": ["a", "b"], "next": "ret"},
				{"type": "end", "id": "ret", "resultType": "success", "output": "$NODE.p"}
			]
		}`)
		got, err := wf.Execute(t.Context(), map[string]any{"x": 5})
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"a": float64(8), "b": float64(3)}, got)
	})

	t.Run("root $PARENT does not panic", func(t *testing.T) {
		wf := load(t, `{
			"flow_id": "p-root",
			"nodes": [
				{"type": "start", "id": "s", "next": "e"},
				{"type": "end", "id": "e", "resultType": "success", "output": "$PARENT.INPUT.x"}
			]
		}`)
		// A root flow has no parent: $PARENT.INPUT resolves against an empty
		// parent execution (missing field → nil). It used to panic on a nil
		// context deref; now it must complete with a nil value.
		var got any
		var err error
		require.NotPanics(t, func() {
			got, err = wf.Execute(t.Context(), map[string]any{})
		})
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("agentrun in loop honors inherited dry_run global", func(t *testing.T) {
		wf := load(t, `{
			"flow_id": "dry-loop-root", "globalContext": {"dry_run": true},
			"inputType": {"dataType": "object"},
			"nodes": [
				{"type": "start", "id": "s", "next": "l"},
				{"type": "loop", "id": "l", "collection": "$INPUT.items", "next": "ret",
					"childFlow": {"flow_id": "dry-loop-child", "nodes": [
						{"type": "start", "id": "s", "next": "a"},
						{"type": "agentrun", "id": "a", "agent": "echo-agent",
						 "prompt": "hi", "next": "e"},
						{"type": "end", "id": "e", "resultType": "success", "output": "$NODE.a.text"}
					]}
				},
				{"type": "end", "id": "ret", "resultType": "success", "output": "$NODE.l"}
			]
		}`)
		got, err := wf.Execute(t.Context(), map[string]any{"items": []any{1}})
		require.NoError(t, err)
		text, ok := got.(string)
		require.True(t, ok)
		assert.True(t, strings.HasPrefix(text, "[dry-run]"),
			"agent must dry-run (inherit dry_run global), got %q", text)
	})
}

// --- #4 timeout: parse and budget semantics ---

func TestNormalizeTimeoutTable(t *testing.T) {
	tests := []struct {
		in      any
		want    string
		wantErr bool
	}{
		{nil, "", false},
		{"", "", false},
		{json.Number("0"), "PT0S", false},
		{"0", "PT0S", false},
		{"PT0S", "PT0S", false},
		{"PT", "PT", false},
		{json.Number("300"), "PT0.3S", false},
		{"PT0.3S", "PT0.3S", false},
		{json.Number("1500"), "PT1.5S", false},
		{"PT1.5S", "PT1.5S", false},
		{"P1D", "P1D", false},
		{"P1Y", "", true},
		{json.Number("-5"), "", true},
		{"-5", "", true},
		{"soon", "", true},
	}
	for _, tt := range tests {
		got, err := normalizeTimeout(tt.in)
		if tt.wantErr {
			assert.Error(t, err, "normalizeTimeout(%v)", tt.in)
			continue
		}
		require.NoError(t, err, "normalizeTimeout(%v)", tt.in)
		assert.Equal(t, tt.want, got, "normalizeTimeout(%v)", tt.in)
	}
}

func TestLoadTimeoutBudgets(t *testing.T) {
	doc := func(timeout any) []byte {
		raw, _ := json.Marshal(map[string]any{
			"flow_id": "t-flow", "timeout": timeout,
			"nodes": []any{
				map[string]any{"type": "start", "id": "s", "next": "e"},
				map[string]any{"type": "end", "id": "e", "resultType": "success", "output": "ok"},
			},
		})
		return raw
	}

	// sub-second / second budgets load (previously: load failure)
	for _, ts := range []any{300, "PT0.3S", 1500, "PT1.5S", "P1D"} {
		wf, err := Load(doc(ts))
		require.NoError(t, err, "timeout %v must load", ts)
		assert.False(t, wf.Timeout.IsZero(), "timeout %v must be set", ts)
	}

	// zero budget = strictest limit: the flow fails immediately, both spellings
	for _, ts := range []any{0, "PT0S"} {
		wf, err := Load(doc(ts))
		require.NoError(t, err)
		got, err := wf.Execute(t.Context(), map[string]any{})
		assert.Error(t, err, "timeout %v must fail immediately", ts)
		assert.Nil(t, got)
	}

	// zero budget + continue policy → flow "completes" with nil per the handler
	data := []byte(`{
		"flow_id": "t-zero-continue", "timeout": "PT0S",
		"timeoutHandler": {"strategy": "continue"},
		"nodes": [
			{"type": "start", "id": "s", "next": "e"},
			{"type": "end", "id": "e", "resultType": "success", "output": "ok"}
		]
	}`)
	wf, err := Load(data)
	require.NoError(t, err)
	got, err := wf.Execute(t.Context(), map[string]any{})
	assert.NoError(t, err)
	assert.Nil(t, got)

	// negative budgets are rejected at load
	_, err = Load(doc(-5))
	assert.Error(t, err)
	_, err = Load(doc("soon"))
	assert.Error(t, err)
}

// --- #18 handler code/message folding ---

func TestNormalizeHandlerCodeMessage(t *testing.T) {
	wf, err := Load([]byte(`{
		"flow_id": "h-flow",
		"nodes": [
			{"type": "start", "id": "s", "next": "e",
			 "errorHandler": {"strategy": "abort", "code": 4001, "message": "upstream down"},
			 "timeoutHandler": {"strategy": "abort", "error_code": 4004, "error_message": "too slow"}},
			{"type": "end", "id": "e", "resultType": "success", "output": 1}
		]
	}`))
	require.NoError(t, err)

	start := wf.Nodes[0]
	errPolicy := start.GetErrorHandlePolicy()
	require.Equal(t, workflow.HandleStrategyAbort, errPolicy.Strategy)
	require.NotNil(t, errPolicy.Code)
	assert.Equal(t, 4001, *errPolicy.Code)
	assert.Equal(t, "upstream down", errPolicy.Message)

	timeoutPolicy := start.GetTimeoutHandlePolicy()
	require.NotNil(t, timeoutPolicy.Code)
	assert.Equal(t, 4004, *timeoutPolicy.Code)
	assert.Equal(t, "too slow", timeoutPolicy.Message)
}

// --- #23 required list folding ---

func TestNormalizePropertyRequiredList(t *testing.T) {
	p := map[string]any{
		"type":     "object",
		"required": []any{"a"},
		"children": map[string]any{
			"a": map[string]any{"type": "string"},
			"b": map[string]any{"type": "string"},
		},
	}
	require.NoError(t, normalizeProperty(p))
	inner := p["properties"].(map[string]any)
	assert.Equal(t, true, inner["a"].(map[string]any)["isRequired"])
	_, hasB := inner["b"].(map[string]any)["isRequired"]
	assert.False(t, hasB)
	assert.NotContains(t, p, "required")

	pb := map[string]any{"type": "string", "required": false}
	require.NoError(t, normalizeProperty(pb))
	assert.Equal(t, false, pb["isRequired"])
}
