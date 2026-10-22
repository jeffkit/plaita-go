package ir

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// The fixtures under testdata/ are real plaita artifacts: assignment_flow.json
// is a console export (legacy go-era property spellings); loop_flow /
// parallel_flow / child_flow are compiled by plaita's codeflow compiler from
// @flow sources (they exercise the embedded-flow path).

func mustLoadFile(t *testing.T, name string) *workflow.Workflow {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	require.NoError(t, err)
	wf, err := Load(data)
	require.NoError(t, err)
	return wf
}

func TestLoadHelloWorld(t *testing.T) {
	wf := mustLoadFile(t, "hello_world.json")
	assert.Equal(t, workflow.ID("hello_world_go"), wf.ID)

	got, err := wf.Execute(t.Context(), map[string]any{})
	require.NoError(t, err)
	assert.Equal(t, "Hello, world!", got)
}

func TestLoadAssignmentFixture(t *testing.T) {
	wf := mustLoadFile(t, "assignment_flow.json")

	// condition branch: $INPUT.bb eq "123" (priority 1) beats default
	got, err := wf.Execute(t.Context(), map[string]any{"bb": "123"})
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"dd": "456"}, got)

	// default branch passes the input through
	got, err = wf.Execute(t.Context(), map[string]any{"bb": "zzz"})
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"dd": "zzz"}, got)
}

func TestLoadLoopFixture(t *testing.T) {
	wf := mustLoadFile(t, "loop_flow.json")

	// plaita semantics: loop output is the last child result
	got, err := wf.Execute(t.Context(), map[string]any{"items": []any{1, 2, 3}})
	require.NoError(t, err)
	assert.Equal(t, 6, got)
}

func TestLoadParallelFixture(t *testing.T) {
	wf := mustLoadFile(t, "parallel_flow.json")

	got, err := wf.Execute(t.Context(), map[string]any{"x": 5})
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"a": 10, "b": 15}, got)
}

func TestLoadChildFixture(t *testing.T) {
	wf := mustLoadFile(t, "child_flow.json")

	got, err := wf.Execute(t.Context(), map[string]any{"name": "plaita"})
	require.NoError(t, err)
	assert.Equal(t, "hello plaita", got)
}

func TestNormalizeTimeout(t *testing.T) {
	ms, err := normalizeTimeout("3000")
	require.NoError(t, err)
	assert.Equal(t, "PT3S", ms)

	frac, err := normalizeTimeout(1500)
	require.NoError(t, err)
	assert.Equal(t, "PT1.5S", frac)

	iso, err := normalizeTimeout("PT2S")
	require.NoError(t, err)
	assert.Equal(t, "PT2S", iso)

	empty, err := normalizeTimeout("")
	require.NoError(t, err)
	assert.Equal(t, "", empty)

	_, err = normalizeTimeout("soon")
	assert.Error(t, err)
}

func TestNormalizePropertyLegacyKeys(t *testing.T) {
	p := map[string]any{
		"type":     "object",
		"children": map[string]any{"x": map[string]any{"type": "string", "default": "s", "is_required": true}},
	}
	require.NoError(t, normalizeProperty(p))
	assert.Equal(t, "object", p["dataType"])
	inner := p["properties"].(map[string]any)["x"].(map[string]any)
	assert.Equal(t, "string", inner["dataType"])
	assert.Equal(t, "s", inner["defaultValue"])
	assert.Equal(t, true, inner["isRequired"])
	assert.NotContains(t, inner, "default")
}

// plaita flow_id wins over legacy id; source_line lands in metadata.
func TestTopLevelAliasesAndMetadata(t *testing.T) {
	data := []byte(`{
		"id": "legacy",
		"flow_id": "canonical",
		"nodes": [
			{"type": "start", "id": "s", "next": "e", "source_line": 7},
			{"type": "end", "id": "e", "resultType": "success", "output": 1}
		]
	}`)
	wf, err := Load(data)
	require.NoError(t, err)
	assert.Equal(t, workflow.ID("canonical"), wf.ID)

	var startNode interface {
		GetID() workflow.NodeID
		GetMetadata() map[string]string
	}
	for _, n := range wf.Nodes {
		if n.GetID() == "s" {
			startNode = n
		}
	}
	require.NotNil(t, startNode)
	assert.Equal(t, "7", startNode.GetMetadata()["sourceLine"])
}

func TestLoadRejects(t *testing.T) {
	cases := []struct {
		name string
		doc  string
		want string
	}{
		{
			// mock 随 #80 phase 2 转正；未注册类型负控换 calculate（r78 实测
			// plaita 运行时同样拒载：unRecognized node type）。
			name: "unsupported known type",
			doc:  `{"nodes":[{"type":"start","id":"s","next":"d"},{"type":"calculate","id":"d","expression":{}}]}`,
			want: "calculate",
		},
		{
			name: "unknown type",
			doc:  `{"nodes":[{"type":"start","id":"s","next":"x"},{"type":"wat","id":"x"}]}`,
			want: "not supported",
		},
		{
			name: "missing start",
			doc:  `{"nodes":[{"type":"end","id":"e"}]}`,
			want: "exactly one start",
		},
		{
			name: "dangling next",
			doc:  `{"nodes":[{"type":"start","id":"s","next":"gone"},{"type":"end","id":"e"}]}`,
			want: "missing node",
		},
		{
			name: "unreachable node",
			doc:  `{"nodes":[{"type":"start","id":"s","next":"e"},{"type":"end","id":"e"},{"type":"end","id":"orphan"}]}`,
			want: "not reachable",
		},
		{
			// python code nodes are admitted (2026-10: python sidecar); what
			// stays fail-closed is an isolation tier the Go engine cannot
			// honour — translateCode must reject it, never silently downgrade.
			name: "python code node with restricted backend",
			doc:  `{"nodes":[{"type":"start","id":"s","next":"c"},{"type":"code","id":"c","language":"python","sandbox_backend":"restricted","code":"def run(input):\n    return input"}]}`,
			want: "restricted",
		},
		{
			name: "nop end",
			doc:  `{"nodes":[{"type":"start","id":"s","next":"e"},{"type":"end","id":"e","resultType":"nop"}]}`,
			want: "nop",
		},
		{
			name: "http ssrf guard",
			doc:  `{"nodes":[{"type":"start","id":"s","next":"h"},{"type":"http","id":"h","url":"http://x","blockPrivateNetworks":true}]}`,
			want: "blockPrivateNetworks",
		},
		{
			name: "reference child",
			doc:  `{"nodes":[{"type":"start","id":"s","next":"c"},{"type":"child","id":"c","flowId":"abc"}]}`,
			want: "reference-style",
		},
		{
			name: "unknown join branch",
			doc:  `{"nodes":[{"type":"start","id":"s","next":"p"},{"type":"parallel","id":"p","branches":[{"name":"a","flow":{"nodes":[{"type":"start","id":"start","next":"e"},{"type":"end","id":"e"}]}}],"joinBranches":["typo"]}]}`,
			want: "unknown branch",
		},
		{
			name: "embedded flow missing end",
			doc:  `{"nodes":[{"type":"start","id":"s","next":"c"},{"type":"child","id":"c","childFlow":{"nodes":[{"type":"start","id":"start","next":"a"},{"type":"assignment","id":"a"}]}}]}`,
			want: "never reaches an end",
		},
		{
			// A1 r132 card G-3: Python's loader takes a mistyped branches
			// (pydantic lax + #128 warnings only); the typed unmarshal into
			// []*nodes.Branch refuses here. Mirrored on the go-vs-rust axis
			// by the plaita-rs diffcheck load/branches-str case.
			name: "switch branches mistyped string",
			doc:  `{"flow_id":"probe","nodes":[{"id":"s","type":"start","next":"sw"},{"id":"sw","type":"switch","next":"e","branches":"oops"},{"id":"e","type":"end"}]}`,
			want: "cannot unmarshal string into Go struct field Workflow.nodes.branches",
		},
		{
			// A1 r132 card G-3: a non-string next slips past unmarshal but
			// forms no edge, so the reachability gate refuses — Python loads
			// the same shape and fails at run with a named NodeNotFoundError.
			// Mirrored by the plaita-rs diffcheck load/next-int case.
			name: "next mistyped int",
			doc:  `{"flow_id":"probe","nodes":[{"id":"s","type":"start","next":123},{"id":"e","type":"end"}]}`,
			want: "not reachable from start",
		},
		{
			// A1 r132 card G-1: a lone surrogate escape is outside the UTF-8
			// hash domain (Python compute_flow_definition_hash refuses the
			// definition at .encode); the decoder would fold it to U+FFFD and
			// let the pool run what Python can never hash. Same pin lives on
			// the worker fingerprint path (flow_hash golden goerr_lone_*).
			name: "lone surrogate escape high",
			doc:  `{"flow_id":"probe","nodes":[{"id":"s","type":"start","next":"e","name":"a\ud800b"},{"id":"e","type":"end"}]}`,
			want: "lone surrogate",
		},
		{
			name: "lone surrogate escape low",
			doc:  `{"flow_id":"probe","nodes":[{"id":"s","type":"start","next":"e","name":"a\udc00b"},{"id":"e","type":"end"}]}`,
			want: "lone surrogate",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load([]byte(tc.doc))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

// plaita allows a switch without a default branch; it loads here and the
// engine reports "no branch matched" at execution time if nothing matches.
func TestSwitchWithoutDefaultBranchLoads(t *testing.T) {
	data := []byte(`{
		"nodes": [
			{"type": "start", "id": "s", "next": "sw"},
			{"type": "switch", "id": "sw", "branches": [
				{"name": "yes", "priority": 1, "condition": {"field": "$INPUT.v", "operator": "eq", "value": "1"}, "next": "e1"},
				{"name": "no", "priority": 0, "condition": {"field": "$INPUT.v", "operator": "eq", "value": "0"}, "next": "e0"}
			]},
			{"type": "end", "id": "e1", "resultType": "success", "output": "one"},
			{"type": "end", "id": "e0", "resultType": "success", "output": "zero"}
		]
	}`)
	wf, err := Load(data)
	require.NoError(t, err)

	got, err := wf.Execute(t.Context(), map[string]any{"v": "1"})
	require.NoError(t, err)
	assert.Equal(t, "one", got)

	_, err = wf.Execute(t.Context(), map[string]any{"v": "42"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no branch matched")
}

// plaita condition operators this engine gained: contains/notContains/truthy/falsy.
func TestPlaitaConditionOperators(t *testing.T) {
	flowWith := func(op string, value any) *workflow.Workflow {
		cond := map[string]any{"field": "$INPUT.v", "operator": op}
		if value != nil {
			cond["value"] = value
		}
		data, err := json.Marshal(map[string]any{
			"nodes": []any{
				map[string]any{"type": "start", "id": "s", "next": "sw"},
				map[string]any{"type": "switch", "id": "sw", "branches": []any{
					map[string]any{"name": "hit", "priority": 1, "condition": cond, "next": "hit"},
					map[string]any{"name": "miss", "isDefault": true, "next": "miss"},
				}},
				map[string]any{"type": "end", "id": "hit", "resultType": "success", "output": "hit"},
				map[string]any{"type": "end", "id": "miss", "resultType": "success", "output": "miss"},
			},
		})
		require.NoError(t, err)
		wf, err := Load(data)
		require.NoError(t, err)
		return wf
	}

	run := func(t *testing.T, wf *workflow.Workflow, input map[string]any) string {
		t.Helper()
		got, err := wf.Execute(t.Context(), input)
		require.NoError(t, err)
		return got.(string)
	}

	assert.Equal(t, "hit", run(t, flowWith("contains", "ell"), map[string]any{"v": "hello"}))
	assert.Equal(t, "miss", run(t, flowWith("contains", "xyz"), map[string]any{"v": "hello"}))
	assert.Equal(t, "hit", run(t, flowWith("notContains", "xyz"), map[string]any{"v": "hello"}))
	assert.Equal(t, "hit", run(t, flowWith("contains", "k"), map[string]any{"v": map[string]any{"k": 1}}))
	assert.Equal(t, "hit", run(t, flowWith("truthy", nil), map[string]any{"v": "x"}))
	assert.Equal(t, "miss", run(t, flowWith("truthy", nil), map[string]any{"v": ""}))
	assert.Equal(t, "hit", run(t, flowWith("falsy", nil), map[string]any{"v": 0}))
}

func TestLoadAgentRun(t *testing.T) {
	data := []byte(`{
		"flow_id": "agent_flow",
		"nodes": [
			{"type": "start", "id": "s", "next": "run"},
			{"type": "agentrun", "id": "run", "agent": "echo", "prompt": "$INPUT.q",
			 "timeout_secs": 90, "next": "end"},
			{"type": "end", "id": "end", "resultType": "success", "output": "$NODE.run.text"}
		]
	}`)
	wf, err := Load(data)
	require.NoError(t, err)

	var agentNode interface {
		GetID() workflow.NodeID
		GetMetadata() map[string]string
	}
	found := false
	for _, n := range wf.Nodes {
		if n.GetID() == "run" {
			agentNode, found = n.(interface {
				GetID() workflow.NodeID
				GetMetadata() map[string]string
			}), true
		}
	}
	require.True(t, found, "agentrun node not loaded")
	require.NotNil(t, agentNode)
}

func TestLoadAgentRunRejects(t *testing.T) {
	_, err := Load([]byte(`{"nodes":[
		{"type":"start","id":"s","next":"r"},
		{"type":"agentrun","id":"r","agent":"a","workspace":"/sbx","repo":"/host"}
	]}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "workspace")

	_, err = Load([]byte(`{"nodes":[
		{"type":"start","id":"s","next":"r"},
		{"type":"agent","id":"r","agent":"a"}
	]}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "agentrun")
}

// Workspace is now a supported branch; a literal name must be registered in
// the infra registry (fail-closed), while an expression is checked at runtime
// after evaluation.
func TestLoadAgentRunWorkspaceRegistry(t *testing.T) {
	home := t.TempDir()
	t.Setenv("PLAITA_HOME", home)
	require.NoError(t, os.WriteFile(filepath.Join(home, "sandboxes.json"), []byte(`{
		"sandboxes": {
			"sbx": {"driver": "docker", "image": "alpine@sha256:deadbeef"}
		}
	}`), 0o600))

	flow := func(workspace string) []byte {
		return []byte(`{"flow_id":"ws_flow","nodes":[
			{"type":"start","id":"s","next":"r"},
			{"type":"agentrun","id":"r","agent":"a","prompt":"p","workspace":"` + workspace + `","next":"e"},
			{"type":"end","id":"e","resultType":"success","output":"$NODE.r.text"}
		]}`)
	}

	_, err := Load(flow("sbx"))
	require.NoError(t, err, "a registered workspace must load")

	_, err = Load(flow("nope"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not registered")

	_, err = Load(flow("$INPUT.ws"))
	require.NoError(t, err, "dynamic workspace names are validated at runtime")
}

// plaita-nodes self-contained nodes load through the same contract: camel
// aliases fold, the chain builds.
func TestLoadPlaitaNodesFlow(t *testing.T) {
	data := []byte(`{
		"flow_id": "nodes_flow",
		"nodes": [
			{"type": "start", "id": "s", "next": "g"},
			{"type": "gate", "id": "g", "gate_name": "check", "command": "true",
			 "timeout_secs": 30, "max_retries": 1, "dryRun": false, "next": "p"},
			{"type": "parse_json", "id": "p", "text": "$NODE.g.stdout", "choices": ["yes"],
			 "join_fields": ["tags"], "next": "w"},
			{"type": "writefile", "id": "w", "path": "out/result.json",
			 "content": "$NODE.p", "json": true, "next": "e"},
			{"type": "end", "id": "e", "resultType": "success", "output": "$NODE.p.parse_ok"}
		]
	}`)
	wf, err := Load(data)
	require.NoError(t, err)
	types := map[string]bool{}
	for _, n := range wf.Nodes {
		types[string(n.GetType())] = true
	}
	assert.True(t, types["gate"] && types["parse_json"] && types["writefile"])
}

func TestLoadNotifyFamilyAccepted(t *testing.T) {
	for _, typ := range []string{"notify", "feishu_webhook", "dingtalk_webhook",
		"slack_webhook", "wecom_webhook", "generic_webhook", "email_send",
		"api_request", "rate_limit", "report"} {
		data := []byte(`{"nodes":[
			{"type":"start","id":"s","next":"n"},
			{"type":"` + typ + `","id":"n","timeoutSecs":5}
		]}`)
		_, err := Load(data)
		require.NoError(t, err, "type %s should load", typ)
	}
}

func TestLoadWave25NodesAccepted(t *testing.T) {
	for _, typ := range []string{"capture", "git_publish", "github_comment",
		"llm", "decision", "hitl"} {
		data := []byte(`{"nodes":[
			{"type":"start","id":"s","next":"n"},
			{"type":"` + typ + `","id":"n","timeoutSecs":5}
		]}`)
		_, err := Load(data)
		require.NoError(t, err, "type %s should load", typ)
	}
}
