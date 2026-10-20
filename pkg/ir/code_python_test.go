package ir

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeffkit/plaita-go/pkg/workflow"
	"github.com/jeffkit/plaita-go/pkg/workflow/nodes"
)

// translateCode python admission (2026-10 python sidecar): deployment-form
// python code nodes load and execute through the subprocess sidecar;
// sandbox_backend values the Go engine cannot honour stay fail-closed.

func pyCodeNode(t *testing.T, wf *workflow.Workflow) *nodes.Code {
	t.Helper()
	for _, n := range wf.Nodes {
		if c, ok := n.(*nodes.Code); ok {
			return c
		}
	}
	t.Fatal("no code node in workflow")
	return nil
}

func pyCodeFlow(code, backend string) []byte {
	doc := map[string]any{
		"flow_id": "py-code-smoke",
		"nodes": []any{
			map[string]any{"type": "start", "id": "s", "next": "c"},
			map[string]any{
				"type": "code", "id": "c", "language": "python",
				"code":  code,
				"input": "$INPUT", "next": "e",
				"outputType": map[string]any{"dataType": "object"},
			},
			map[string]any{"type": "end", "id": "e", "resultType": "success", "output": "$NODE.c"},
		},
	}
	if backend != "" {
		doc["nodes"].([]any)[1].(map[string]any)["sandbox_backend"] = backend
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		panic(err)
	}
	return raw
}

const pyEchoCode = "def run(input):\n    return {\"echo\": input[\"msg\"], \"n\": 7}\n"

func TestLoadPythonCodeNode(t *testing.T) {
	for _, backend := range []string{"", "subprocess"} {
		wf, err := Load(pyCodeFlow(pyEchoCode, backend))
		require.NoError(t, err, "backend %q", backend)

		// the folded camelCase field rides into the engine node
		node := pyCodeNode(t, wf)
		assert.Equal(t, "python", string(node.Language))
		assert.Equal(t, backend, node.SandboxBackend)

		got, err := wf.Execute(t.Context(), map[string]any{"msg": "hello"})
		require.NoError(t, err)
		// integers survive the sidecar envelope as json.Number
		assert.Equal(t, map[string]any{"echo": "hello", "n": json.Number("7")}, got)
	}
}

func TestLoadPythonCodeNodeDefaultsLanguage(t *testing.T) {
	// plaita model_validator: language None → python (production flows carry
	// such nodes, e.g. cancel-drill-flow:1.0.0/slow)
	doc := `{"flow_id":"py-default","nodes":[
		{"type":"start","id":"s","next":"c"},
		{"type":"code","id":"c","code":"def run(input):\n    return input","input":"$INPUT","next":"e",
		 "outputType":{"dataType":"object"}},
		{"type":"end","id":"e","resultType":"success","output":"$NODE.c"}]}`
	wf, err := Load([]byte(doc))
	require.NoError(t, err)
	node := pyCodeNode(t, wf)
	assert.Equal(t, "python", string(node.Language))
}

func TestLoadPythonCodeNodeRejectsBackends(t *testing.T) {
	for _, backend := range []string{"restricted", "docker", "unsafe"} {
		t.Run(backend, func(t *testing.T) {
			_, err := Load(pyCodeFlow(pyEchoCode, backend))
			require.Error(t, err)
			assert.Contains(t, err.Error(), backend)
			assert.Contains(t, err.Error(), "plaita runtime")
		})
	}
	t.Run("unknown value", func(t *testing.T) {
		_, err := Load(pyCodeFlow(pyEchoCode, "containerish"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid for language python")
	})
}

func TestLoadJavascriptCodeNodeBackendStillRejected(t *testing.T) {
	doc := `{"nodes":[{"type":"start","id":"s","next":"c"},
		{"type":"code","id":"c","language":"javascript","code":"function run(i){return i}","sandbox_backend":"docker","next":"e"},
		{"type":"end","id":"e","resultType":"success","output":"$NODE.c"}]}`
	_, err := Load([]byte(doc))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "docker")
}

func TestLoadUnknownCodeLanguageRejected(t *testing.T) {
	doc := `{"nodes":[{"type":"start","id":"s","next":"c"},
		{"type":"code","id":"c","language":"ruby","code":"def run(i)\nend","next":"e"},
		{"type":"end","id":"e","resultType":"success","output":"$NODE.c"}]}`
	_, err := Load([]byte(doc))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ruby")
}
