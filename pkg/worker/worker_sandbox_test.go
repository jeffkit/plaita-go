package worker

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/jeffkit/plaita-go/pkg/sandbox"
)

// Worker → sandbox execution-scope wiring (pkg/sandbox resolution order:
// ctx value > PLAITA_EXECUTION_ID env > $GLOBAL). executeAndFinalize threads
// the REAL execution id into the engine context, so the agentrun workspace
// branch derives its sandbox resource names from the execution identity and a
// retried/resumed carrier re-attaches to the same workspace
// ({execution_id}:{ws_key}) instead of falling back to the (node, input)
// hash scope (sandbox.DeriveExecutionID's local-<sha256> names).

type recordedEnsure struct {
	ExecutionID   string
	WSKey         string
	ContainerName string
}

type recordedExec struct {
	ExecutionIDEnv string
}

type recordingSandboxDriver struct {
	mu      sync.Mutex
	ensures []recordedEnsure
	execs   []recordedExec
}

func (d *recordingSandboxDriver) Name() string { return "recording" }

func (d *recordingSandboxDriver) Ensure(_ context.Context, _ *sandbox.WorkspaceSpec, executionID, wsKey string) (*sandbox.WorkspaceHandle, error) {
	d.mu.Lock()
	d.ensures = append(d.ensures, recordedEnsure{
		ExecutionID:   executionID,
		WSKey:         wsKey,
		ContainerName: sandbox.ResourceName(executionID, wsKey, "container"),
	})
	d.mu.Unlock()
	return &sandbox.WorkspaceHandle{
		Driver:      "recording",
		ID:          sandbox.HandleID(executionID, wsKey),
		Path:        sandbox.SandboxPath,
		Key:         wsKey,
		ExecutionID: executionID,
	}, nil
}

func (d *recordingSandboxDriver) Exec(_ context.Context, _ *sandbox.WorkspaceHandle, opts sandbox.ExecOptions) (*sandbox.ExecResult, error) {
	d.mu.Lock()
	d.execs = append(d.execs, recordedExec{ExecutionIDEnv: opts.Env["PLAITA_EXECUTION_ID"]})
	d.mu.Unlock()
	return &sandbox.ExecResult{ExitCode: 0, Stdout: `{"result":"sandbox-ok","session_id":"s-1"}`}, nil
}

func (d *recordingSandboxDriver) InjectFile(context.Context, *sandbox.WorkspaceHandle, string, []byte, int64) error {
	return nil
}

func (d *recordingSandboxDriver) RemoveFile(context.Context, *sandbox.WorkspaceHandle, string) error {
	return nil
}

func (d *recordingSandboxDriver) Enforce(context.Context, *sandbox.WorkspaceHandle) error { return nil }

func (d *recordingSandboxDriver) Release(context.Context, *sandbox.WorkspaceHandle, sandbox.ReleaseOptions) error {
	return nil
}

// TestWorkerThreadsExecutionIDIntoSandboxScope: the start carrier's
// execution_id reaches the sandbox layer as the workspace scope — container
// resource names derive from the real execution id (re-attach semantics
// hold), never from the (node, input) hash fallback.
func TestWorkerThreadsExecutionIDIntoSandboxScope(t *testing.T) {
	home := t.TempDir()
	files := map[string]string{
		"sandboxes.json": `{"sandboxes": {
			"sbx": {"driver": "recording", "image": "img", "allow_unpinned": true}
		}}`,
		"agents.json": `{"agents": {
			"runner": {"executor": "recursive-direct", "model": "m1",
			           "env": {"RECURSIVE_API_KEY": "secret-token-value"}}
		}}`,
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(home, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PLAITA_HOME", home)
	t.Setenv("TMPDIR", t.TempDir())

	driver := &recordingSandboxDriver{}
	sandbox.RegisterDriver("recording", func() sandbox.Driver { return driver })

	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	flow := `{
	  "flow_id": "sandbox_scope_flow",
	  "nodes": [
	    {"type": "start", "id": "start", "next": "ag"},
	    {"type": "agentrun", "id": "ag", "agent": "runner", "prompt": "turn",
	     "workspace": "sbx", "next": "end"},
	    {"type": "end", "id": "end", "resultType": "success", "output": "$NODE.ag.text"}
	  ]
	}`
	presetFlowJSON(t, w, "sandbox_scope_flow", "1", flow)

	executionID := newExecutionID()
	payload := baseStartPayload(executionID)
	payload["flow_id"] = "sandbox_scope_flow"
	enqueueTask(t, rdb, cfg, payload, "")

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	state := loadStateRow(t, rdb, cfg, executionID)
	if state.Status != StatusCompleted {
		t.Fatalf("status = %q, want completed (state error: %v)", state.Status, state.Error)
	}

	if len(driver.ensures) != 1 {
		t.Fatalf("ensure calls = %d, want 1", len(driver.ensures))
	}
	ensure := driver.ensures[0]
	if ensure.WSKey != "sbx" {
		t.Fatalf("ws key = %q, want sbx", ensure.WSKey)
	}
	if ensure.ExecutionID != executionID {
		t.Fatalf("sandbox execution scope = %q, want the carrier execution_id %q", ensure.ExecutionID, executionID)
	}
	if strings.HasPrefix(ensure.ExecutionID, "local-") {
		t.Fatalf("sandbox scope fell back to the derived hash scope: %q", ensure.ExecutionID)
	}
	// The deterministic resource name carries the real execution id, so a
	// retried carrier re-attaches to the same workspace.
	wantName := sandbox.ResourceName(executionID, "sbx", "container")
	if ensure.ContainerName != wantName || !strings.Contains(wantName, executionID) {
		t.Fatalf("container name = %q, want %q (derived from the real execution id)", ensure.ContainerName, wantName)
	}

	if len(driver.execs) != 1 {
		t.Fatalf("exec calls = %d, want 1", len(driver.execs))
	}
	if got := driver.execs[0].ExecutionIDEnv; got != executionID {
		t.Fatalf("PLAITA_EXECUTION_ID inside the sandbox = %q, want %q", got, executionID)
	}
	if text, _ := state.Context["$NODE"].(map[string]any)["ag"].(map[string]any)["text"].(string); text != "sandbox-ok" {
		t.Fatalf("agentrun text = %v", state.Context["$NODE"])
	}
}
