package nodes

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/agentproc"
)

func agentRegistryForTest(t *testing.T) *agentproc.Registry {
	t.Helper()
	dir := t.TempDir()
	agents := `{"agents": {
		"echo": {"executor": "recursive", "provider": "p1",
			"command": "/bin/sh", "args": ["-c", "printf '%s\\n' '{\"type\":\"result\",\"text\":\"reply-from-agent\"}'"]}
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
