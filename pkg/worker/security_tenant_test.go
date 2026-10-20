package worker

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestAgentRunHostTenantGateThroughWorker is the end-to-end negative/positive
// control for A62-02: the gate reads the tenant the WORKER injects
// (runtime.WithTenantID), not one a test sets by hand.
//
//   - tenant acme: the carrier is processed, the agentrun host path refuses
//     before spawn and the execution terminates as a deterministic error; the
//     bridge's sentinel file must not exist (zero execution chance);
//   - tenant default (same flow): the bridge runs and the execution completes.
func TestAgentRunHostTenantGateThroughWorker(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()

	home := t.TempDir()
	t.Setenv("PLAITA_HOME", home)
	sentinel := filepath.Join(t.TempDir(), "agent-ran")
	writeAgentRegistry(t, home, sentinel)

	flow := []byte(`{
		"flow_id": "tenant-gate",
		"inputType": {"dataType": "object"},
		"nodes": [
			{"type": "start", "id": "start", "next": "run"},
			{"type": "agentrun", "id": "run", "agent": "echo", "prompt": "$INPUT.q",
			 "timeout_secs": 30, "next": "end"},
			{"type": "end", "id": "end", "resultType": "success", "output": "$NODE.run.text"}
		]
	}`)
	for _, tenant := range []string{"", "acme"} {
		if err := w.flows.PresetFlowDefinition(ctx, tenant, "tenant-gate", "latest", flow); err != nil {
			t.Fatalf("preset flow for tenant %q: %v", tenant, err)
		}
	}

	startPayload := func(tenant string) (string, map[string]any) {
		id := newExecutionID()
		return id, map[string]any{
			"type":         "start",
			"flow_id":      "tenant-gate",
			"execution_id": id,
			"params":       map[string]any{"q": "hi"},
			"tenant_id":    tenant,
			"timestamp":    nowISO(),
		}
	}

	// Non-default tenant: refused before the bridge spawns.
	acmeID, acmePayload := startPayload("acme")
	enqueueTask(t, rdb, cfg, acmePayload, "")
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne(acme): %v", err)
	}
	acmeState := loadTenantStateRow(t, rdb, cfg, "acme", acmeID)
	if acmeState.Status != StatusError {
		t.Fatalf("acme status = %q, want %q (state=%+v)", acmeState.Status, StatusError, acmeState)
	}
	if msg, _ := acmeState.Error["message"].(string); !strings.Contains(msg, "non-default tenant") {
		t.Errorf("acme error = %v, want the host-run tenant refusal", acmeState.Error)
	}
	if _, err := os.Stat(sentinel); !os.IsNotExist(err) {
		t.Fatalf("the agent bridge ran for a non-default tenant (sentinel exists)")
	}

	// Default tenant: same flow, historical semantics preserved.
	defaultID, defaultPayload := startPayload("")
	enqueueTask(t, rdb, cfg, defaultPayload, "")
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne(default): %v", err)
	}
	defaultState := loadStateRow(t, rdb, cfg, defaultID)
	if defaultState.Status != StatusCompleted {
		t.Fatalf("default status = %q, want %q (state=%+v)", defaultState.Status, StatusCompleted, defaultState)
	}
	if _, err := os.Stat(sentinel); err != nil {
		t.Fatalf("the agent bridge must run for the default tenant: %v", err)
	}
}

// writeAgentRegistry writes a 0600 agents.json whose bridge touches sentinel
// and emits one protocol result event.
func writeAgentRegistry(t *testing.T, home, sentinel string) {
	t.Helper()
	script := "touch '" + strings.ReplaceAll(sentinel, "'", `'\''`) + "'\n" +
		`printf '{"type":"result","text":"ran"}\n'`
	doc := map[string]any{"agents": map[string]any{
		"echo": map[string]any{
			"executor": "testcli",
			"model":    "test-model",
			"command":  "/bin/sh",
			"args":     []any{"-c", script},
		},
	}}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, "agents.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
}
