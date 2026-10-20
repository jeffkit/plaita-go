package worker

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestSubprocessNodesTenantGateThroughWorker is the end-to-end negative /
// positive control for #34 (pn#18 / A42-06): the gate reads the tenant the
// WORKER injects (runtime.WithTenantID), not one a test sets by hand, and it
// covers all three host-subprocess nodes — gate, capture and the code python
// sidecar.
//
//   - tenant acme: the execution terminates as a deterministic error carrying
//     the PLAITA_SUBPROCESS_TENANT_ISOLATION_UNAVAILABLE marker, and the
//     command's sentinel file must not exist (zero execution chance);
//   - tenant default (same flow): the command runs and the execution completes.
func TestSubprocessNodesTenantGateThroughWorker(t *testing.T) {
	cases := []struct {
		name string
		node func(sentinel string) map[string]any
		skip func(t *testing.T)
	}{
		{
			name: "gate",
			node: func(sentinel string) map[string]any {
				return map[string]any{
					"type": "gate", "id": "n", "gate_name": "tenant-gate",
					"command": []any{"sh", "-c", "touch " + sentinel},
					"next":    "end",
				}
			},
		},
		{
			name: "capture",
			node: func(sentinel string) map[string]any {
				return map[string]any{
					"type": "capture", "id": "n",
					"command": []any{"sh", "-c", "touch " + sentinel},
					"next":    "end",
				}
			},
		},
		{
			name: "code-python",
			node: func(sentinel string) map[string]any {
				return map[string]any{
					"type": "code", "id": "n", "language": "python",
					"code": "def run(input):\n    open(" + jsonQuote(sentinel) + ", 'w').write('ran')\n    return 'ok'\n",
					"next": "end",
				}
			},
			skip: func(t *testing.T) {
				if _, err := exec.LookPath("python3"); err != nil {
					t.Skipf("python3 not on PATH: %v", err)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip != nil {
				tc.skip(t)
			}
			w, rdb, _, cfg := newTestWorker(t)
			ctx := context.Background()

			flowID := "tenant-gate-" + tc.name
			sentinel := filepath.Join(t.TempDir(), "ran")
			flow := map[string]any{
				"flow_id":   flowID,
				"inputType": map[string]any{"dataType": "object"},
				"nodes": []any{
					map[string]any{"type": "start", "id": "start", "next": "n"},
					tc.node(sentinel),
					map[string]any{"type": "end", "id": "end", "resultType": "success",
						"output": "$NODE.n"},
				},
			}
			blob, err := json.Marshal(flow)
			if err != nil {
				t.Fatal(err)
			}
			for _, tenant := range []string{"", "acme"} {
				if err := w.flows.PresetFlowDefinition(ctx, tenant, flowID, "latest", blob); err != nil {
					t.Fatalf("preset flow for tenant %q: %v", tenant, err)
				}
			}

			startPayload := func(tenant string) (string, map[string]any) {
				id := newExecutionID()
				return id, map[string]any{
					"type":         "start",
					"flow_id":      flowID,
					"execution_id": id,
					"params":       map[string]any{},
					"tenant_id":    tenant,
					"timestamp":    nowISO(),
				}
			}

			// Non-default tenant: refused before the command spawns.
			acmeID, acmePayload := startPayload("acme")
			enqueueTask(t, rdb, cfg, acmePayload, "")
			if _, err := w.ProcessOne(ctx); err != nil {
				t.Fatalf("ProcessOne(acme): %v", err)
			}
			acmeState := loadTenantStateRow(t, rdb, cfg, "acme", acmeID)
			if acmeState.Status != StatusError {
				t.Fatalf("acme status = %q, want %q (state=%+v)", acmeState.Status, StatusError, acmeState)
			}
			msg, _ := acmeState.Error["message"].(string)
			if !strings.Contains(msg, "PLAITA_SUBPROCESS_TENANT_ISOLATION_UNAVAILABLE") {
				t.Errorf("acme error = %v, want the tenant-isolation marker", acmeState.Error)
			}
			if !strings.Contains(msg, `"acme"`) {
				t.Errorf("acme error = %v, want the refused tenant named", acmeState.Error)
			}
			if _, err := os.Stat(sentinel); !os.IsNotExist(err) {
				t.Fatalf("the %s command ran for a non-default tenant (sentinel exists)", tc.name)
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
				t.Fatalf("the %s command must run for the default tenant: %v", tc.name, err)
			}
		})
	}
}

// jsonQuote renders a Go string as a JSON string literal for embedding in a
// python source line.
func jsonQuote(s string) string {
	raw, _ := json.Marshal(s)
	return string(raw)
}
