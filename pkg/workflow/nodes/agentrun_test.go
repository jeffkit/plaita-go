package nodes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/agentproc"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func agentRegistryForTest(t *testing.T) *agentproc.Registry {
	t.Helper()
	dir := t.TempDir()
	// The echo double reads its turn line before emitting the result — the
	// same readiness gate protocolDetailsRegistry uses. A bridge that never
	// consumes stdin makes agentproc's stdin.Write race the child's exit:
	// on a loaded CI runner the child wins and the write dies with EPIPE
	// ("write turn: broken pipe" flake, 807829f first run). Real bridges
	// read the turn first; the double must be equally faithful.
	agents := `{"agents": {
		"echo": {"executor": "testcli", "model": "test-model",
			"command": "/bin/sh", "args": ["-c", "IFS= read -r _ || true; printf '%s\\n' '{\"type\":\"result\",\"text\":\"reply-from-agent\"}'"]}
	}}`
	providers := `{"providers": {"p1": {"type": "anthropic", "apiBase": "https://x", "apiKey": "k", "model": "test-model"}}}`
	if err := os.WriteFile(filepath.Join(dir, "agents.json"), []byte(agents), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "providers.json"), []byte(providers), 0o600); err != nil {
		t.Fatal(err)
	}
	return &agentproc.Registry{Dir: dir}
}

func agentRunContext(t *testing.T, input map[string]any) context.Context {
	t.Helper()
	return runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput:      input,
		ExecuteNodeResults: map[string]any{},
		EnvVariables:       map[string]any{},
		GlobalVariables:    map[string]any{},
	})
}

func TestAgentRunExecute(t *testing.T) {
	node := &AgentRun{
		BaseNode: BaseNode{ID: "ag1"},
		Agent:    "$INPUT.agent",
		Prompt:   "$INPUT.prompt",
		Registry: agentRegistryForTest(t),
	}
	res, err := node.Execute(agentRunContext(t, map[string]any{
		"agent": "echo", "prompt": "do the thing",
	}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out, ok := res.Result.(map[string]any)
	if !ok {
		t.Fatalf("result type %T", res.Result)
	}
	if out["text"] != "reply-from-agent" {
		t.Errorf("text = %v", out["text"])
	}
	if out["cli"] != "echo" {
		t.Errorf("cli = %v", out["cli"])
	}
	if out["model"] != "test-model" {
		t.Errorf("model = %v", out["model"])
	}
}

func TestAgentRunDryRun(t *testing.T) {
	long := make([]byte, 120)
	for i := range long {
		long[i] = 'x'
	}
	node := &AgentRun{
		BaseNode: BaseNode{ID: "ag1"},
		Agent:    "echo",
		Prompt:   string(long),
		DryRun:   true,
		Registry: agentRegistryForTest(t),
	}
	res, err := node.Execute(agentRunContext(t, map[string]any{}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out := res.Result.(map[string]any)
	text, _ := out["text"].(string)
	if len(text) > 200 || text[:9] != "[dry-run]" {
		t.Errorf("dry-run text = %q", text)
	}
	if dry, _ := out["dry_run"].(bool); !dry {
		t.Errorf("dry_run flag missing: %v", out)
	}
}

func TestAgentRunWorkspaceRejected(t *testing.T) {
	node := &AgentRun{
		BaseNode:  BaseNode{ID: "ag1"},
		Agent:     "echo",
		Workspace: "/sandbox/place",
		Registry:  agentRegistryForTest(t),
	}
	_, err := node.Execute(agentRunContext(t, map[string]any{}))
	if err == nil {
		t.Fatal("workspace must be rejected at runtime")
	}
}

func TestAgentRunJSONRoundTrip(t *testing.T) {
	doc := `{"type":"agentrun","id":"a1","agent":"echo","prompt":"$INPUT.p",
		"timeout_secs": 90, "dry_run": true, "timeoutSecsAlt": null}`
	node := new(AgentRun)
	if err := json.Unmarshal([]byte(doc), node); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if node.Agent != "echo" || node.Prompt != "$INPUT.p" {
		t.Errorf("fields = %v %v", node.Agent, node.Prompt)
	}
}

// protocolDetailsRegistry returns a registry whose agent is a fake protocol
// bridge emitting claude-compatible stream-json NDJSON events.
func protocolDetailsRegistry(t *testing.T) *agentproc.Registry {
	t.Helper()
	dir := t.TempDir()
	script := `IFS= read -r _ || true
printf '%s\n' \
'{"type":"system","subtype":"init"}' \
'{"type":"assistant","message":{"model":"claude-x","content":[{"type":"tool_use","id":"t1","name":"read_file","input":{"path":"a.go"}}]}}' \
'{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"t1","content":[{"type":"text","text":"file body"}]}]}}' \
'{"type":"assistant","message":{"model":"claude-x","content":[{"type":"text","text":"reading the file"}]}}' \
'{"type":"result","text":"final reply","session_id":"s-9","usage":{"input_tokens":3}}'`
	agents := `{"agents": {
		"proto": {"executor": "claude-code", "model": "m", "command": "/bin/sh", "args": ["-c", ` + strconv.Quote(script) + `]}
	}}`
	if err := os.WriteFile(filepath.Join(dir, "agents.json"), []byte(agents), 0o600); err != nil {
		t.Fatal(err)
	}
	return &agentproc.Registry{Dir: dir}
}

// TestAgentRunDetailsTrueObservations pins issue #16: details=true must emit
// `observations` — structured {type,name,input,output} entries per the
// Python parse_stream_details contract — not raw protocol lines.
func TestAgentRunDetailsTrueObservations(t *testing.T) {
	node := &AgentRun{
		BaseNode: BaseNode{ID: "ag"},
		Agent:    "proto",
		Prompt:   "p",
		Details:  true,
		Registry: protocolDetailsRegistry(t),
	}
	res, err := node.Execute(agentRunContext(t, map[string]any{}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out := res.Result.(map[string]any)
	if _, has := out["details"]; has {
		t.Errorf("raw `details` key must not appear: %v", out)
	}
	obs, ok := out["observations"].([]map[string]any)
	if !ok {
		t.Fatalf("observations type %T, want []map[string]any: %v", out["observations"], out)
	}
	if len(obs) != 2 {
		t.Fatalf("observations = %v, want 2 entries", obs)
	}
	span := obs[0]
	if span["type"] != "span" || span["name"] != "tool:read_file" {
		t.Errorf("span = %v", span)
	}
	input, _ := span["input"].(map[string]any)
	if input == nil || input["path"] != "a.go" {
		t.Errorf("span input = %v", span["input"])
	}
	if span["output"] != "file body" {
		t.Errorf("span output = %v (content blocks must join to text)", span["output"])
	}
	gen := obs[1]
	if gen["type"] != "generation" || gen["name"] != "turn:1" || gen["model"] != "claude-x" || gen["output"] != "reading the file" {
		t.Errorf("generation = %v", gen)
	}
}

// TestAgentRunDetailsFalseNoExtraKeys pins: details=false must not add
// `observations` (nor the legacy `details`).
func TestAgentRunDetailsFalseNoExtraKeys(t *testing.T) {
	node := &AgentRun{
		BaseNode: BaseNode{ID: "ag"},
		Agent:    "proto",
		Prompt:   "p",
		Registry: protocolDetailsRegistry(t),
	}
	res, err := node.Execute(agentRunContext(t, map[string]any{}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out := res.Result.(map[string]any)
	for _, key := range []string{"observations", "details"} {
		if _, has := out[key]; has {
			t.Errorf("details=false must not carry %q: %v", key, out)
		}
	}
}

// TestParseStreamDetailsCapAndSkip pins the parser edge rules: system /
// stream_event / result lines skipped, unmatched tool_results dropped, and
// the 50-entry cap enforced.
func TestParseStreamDetailsCapAndSkip(t *testing.T) {
	lines := []string{
		`{"type":"system","subtype":"init"}`,
		`{"type":"stream_event","x":1}`,
		`{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"orphan","content":"boom"}]}}`,
		`{"type":"result","text":"done"}`,
		`not json at all`,
	}
	if obs := parseStreamDetails(lines); len(obs) != 0 {
		t.Errorf("observations = %v, want empty", obs)
	}

	capped := make([]string, 0, detailsCap+5)
	for i := 0; i < detailsCap+5; i++ {
		capped = append(capped, fmt.Sprintf(`{"type":"assistant","message":{"model":"m","content":[{"type":"text","text":"t%d"}]}}`, i))
	}
	obs := parseStreamDetails(capped)
	if len(obs) != detailsCap {
		t.Fatalf("capped observations = %d, want %d", len(obs), detailsCap)
	}
	if obs[detailsCap-1]["name"] != fmt.Sprintf("turn:%d", detailsCap) {
		t.Errorf("last kept observation = %v", obs[detailsCap-1])
	}
}

// TestParseStreamDetailsContentBlockJoin pins the tool_result content-array
// → "\n"-joined text rule and the plain-string passthrough.
func TestParseStreamDetailsContentBlockJoin(t *testing.T) {
	lines := []string{
		`{"type":"assistant","message":{"content":[{"type":"tool_use","id":"t1","name":"run","input":{"cmd":"ls"}}]}}`,
		`{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"t1","content":[{"type":"text","text":"line1"},{"type":"text","text":"line2"}]}]}}`,
		`{"type":"assistant","message":{"content":[{"type":"tool_use","id":"t2","name":"raw","input":{}}]}}`,
		`{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"t2","content":"plain result"}]}}`,
	}
	obs := parseStreamDetails(lines)
	if len(obs) != 2 {
		t.Fatalf("observations = %v", obs)
	}
	if obs[0]["output"] != "line1\nline2" {
		t.Errorf("block-array output = %v", obs[0]["output"])
	}
	if obs[1]["output"] != "plain result" {
		t.Errorf("string output = %v", obs[1]["output"])
	}
}

// recursiveDetailsRegistry returns a registry whose agent uses the
// recursive-direct executor backed by a fake `recursive` binary that prints
// stream-json NDJSON (what the node switches --output-format to when
// details=true).
func recursiveDetailsRegistry(t *testing.T) *agentproc.Registry {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "fake-recursive")
	script := `#!/bin/sh
printf '%s\n' \
'{"type":"assistant","message":{"model":"rm","content":[{"type":"text","text":"thinking out loud"}]}}' \
'{"type":"result","result":"recursive reply","session_id":"rc-9","usage":{"input_tokens":7}}'`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	agents := `{"agents": {"rec": {"executor": "recursive", "model": "rm", "env": {"RECURSIVE_BIN": ` + strconv.Quote(bin) + `}}}}`
	if err := os.WriteFile(filepath.Join(dir, "agents.json"), []byte(agents), 0o600); err != nil {
		t.Fatal(err)
	}
	return &agentproc.Registry{Dir: dir}
}

// TestAgentRunRecursiveDirectDetails pins the recursive-direct plain path:
// details=true switches --output-format to stream-json and observations are
// parsed from the transcript (Python parses reply.splitlines() the same way).
func TestAgentRunRecursiveDirectDetails(t *testing.T) {
	node := &AgentRun{
		BaseNode: BaseNode{ID: "ag"},
		Agent:    "rec",
		Prompt:   "p",
		Details:  true,
		Registry: recursiveDetailsRegistry(t),
	}
	res, err := node.Execute(agentRunContext(t, map[string]any{}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["text"] != "recursive reply" {
		t.Errorf("text = %v (trailing result object must still be extracted)", out["text"])
	}
	if out["session_id"] != "rc-9" {
		t.Errorf("session_id = %v", out["session_id"])
	}
	obs, ok := out["observations"].([]map[string]any)
	if !ok || len(obs) != 1 {
		t.Fatalf("observations = %v", out["observations"])
	}
	if obs[0]["type"] != "generation" || obs[0]["output"] != "thinking out loud" || obs[0]["model"] != "rm" {
		t.Errorf("observation = %v", obs[0])
	}
}

// TestAgentRunHostRunTenantGate pins A62-02: a non-default tenant may not
// spawn an agent CLI on the worker host. The gate must fire before spawn so
// the agent has zero execution chance.
func TestAgentRunHostRunTenantGate(t *testing.T) {
	node := &AgentRun{
		BaseNode: BaseNode{ID: "ag"},
		Agent:    "echo",
		Prompt:   "p",
		Repo:     t.TempDir(), // existing dir: without the gate the spawn would succeed
		Registry: agentRegistryForTest(t),
	}
	ctx := runtime.WithTenantID(agentRunContext(t, map[string]any{}), "acme")
	res, err := node.Execute(ctx)
	if err == nil {
		t.Fatalf("non-default tenant must be refused, got result %v", res)
	}
	if !errors.Is(err, ErrHostRunTenantDenied) {
		t.Fatalf("error = %v, want ErrHostRunTenantDenied", err)
	}
	if !strings.Contains(err.Error(), "workspace") {
		t.Errorf("error should point at the sandbox path: %v", err)
	}
	if workflow.IsRetryableError(err) {
		t.Error("a tenant policy refusal must be deterministic (no retry storm)")
	}
}

// TestAgentRunDefaultTenantHostRunAllowed is the positive control for the
// tenant gate: the default tenant keeps the historical host-run semantics.
func TestAgentRunDefaultTenantHostRunAllowed(t *testing.T) {
	node := &AgentRun{
		BaseNode: BaseNode{ID: "ag"},
		Agent:    "echo",
		Prompt:   "p",
		Registry: agentRegistryForTest(t),
	}
	ctx := runtime.WithTenantID(agentRunContext(t, map[string]any{}), runtime.DefaultTenantID)
	res, err := node.Execute(ctx)
	if err != nil {
		t.Fatalf("default tenant host run must be allowed: %v", err)
	}
	if out := res.Result.(map[string]any); out["text"] != "reply-from-agent" {
		t.Errorf("text = %v", out["text"])
	}
}

// TestAgentRunBareEngineHostRunAllowed pins the no-tenant-layer deployment:
// without a worker there is no tenant context (plaita's single-tenant
// ImportError fallback), so the gate stays out of the way.
func TestAgentRunBareEngineHostRunAllowed(t *testing.T) {
	node := &AgentRun{
		BaseNode: BaseNode{ID: "ag"},
		Agent:    "echo",
		Prompt:   "p",
		Registry: agentRegistryForTest(t),
	}
	if _, err := node.Execute(agentRunContext(t, map[string]any{})); err != nil {
		t.Fatalf("bare engine host run must be allowed: %v", err)
	}
}
