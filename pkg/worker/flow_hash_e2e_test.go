package worker

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/redis/go-redis/v9"
)

// overwriteStateHash rewrites the state row's fingerprint fields, simulating
// the row as the OTHER engine wrote it: the mixed-fleet deployment shares
// `{ns}:execution:{id}` between the Python worker and this one
// (docs/WORKER_PROTOCOL.md §5.1, plaita-go#60).
func overwriteStateHash(t *testing.T, rdb *redis.Client, cfg *Config, executionID, flowHash, flowHashAlgo string) {
	t.Helper()
	ctx := context.Background()
	key := stateKeyOf(cfg, executionID)
	raw, err := rdb.Get(ctx, key).Result()
	if err != nil {
		t.Fatalf("state row read: %v", err)
	}
	var row map[string]any
	if err := json.Unmarshal([]byte(raw), &row); err != nil {
		t.Fatalf("state row decode: %v", err)
	}
	row["flow_hash"] = flowHash
	row["flow_hash_algo"] = flowHashAlgo
	encoded, err := json.Marshal(row)
	if err != nil {
		t.Fatalf("state row encode: %v", err)
	}
	if err := rdb.Set(ctx, key, string(encoded), 0).Err(); err != nil {
		t.Fatalf("state row write: %v", err)
	}
}

// TestMixedFleetResumeWithPythonHash is the #60 end-to-end negative control:
// a Python engine starts an execution and writes its fingerprint (label
// "flow-definition-json-sortkeys-v2", hash computed by Python) into the
// shared state row; the execution suspends; THIS Go worker picks up the
// resume carrier. The definition is unchanged — the resume must proceed, not
// die as flow_hash_mismatch. Pre-fix this test is red: the old Go
// canonicalization (json.Marshal escapeHTML + json.Number passthrough) could
// not reproduce Python's fingerprint for a definition containing '&' /
// exponent literals, and the hard gate terminal-errored the run.
func TestMixedFleetResumeWithPythonHash(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()

	raw, pythonHash := goldenCase(t, "e2e_mixed_fleet_flow")
	presetFlowJSON(t, w, "mixed_fleet_flow", "1", raw)

	executionID := newExecutionID()
	enqueueTask(t, rdb, cfg, startPayloadFor("mixed_fleet_flow", "1", executionID), "")
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("start ProcessOne: %v", err)
	}
	state := loadStateRow(t, rdb, cfg, executionID)
	if state.Status != StatusSuspended {
		t.Fatalf("status = %q, want suspended", state.Status)
	}
	subscriptionID, _ := pendingNodeState(t, state, "wait")["subscription_id"].(string)
	if subscriptionID == "" {
		t.Fatal("subscription id missing")
	}

	// The Python engine's fingerprint lands in the shared row (flow_worker.py
	// flow_hash=self.get_flow_definition_hash(...) + FLOW_HASH_ALGO label).
	overwriteStateHash(t, rdb, cfg, executionID, pythonHash, "flow-definition-json-sortkeys-v2")

	enqueueTask(t, rdb, cfg, map[string]any{
		"type":         "resume",
		"flow_id":      "mixed_fleet_flow",
		"execution_id": executionID,
		"resume_type":  "event",
		"timestamp":    nowISO(),
		"data": map[string]any{
			"subscription_id": subscriptionID,
			"event_type":      "approval",
		},
	}, "")
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("resume ProcessOne: %v", err)
	}
	final := loadStateRow(t, rdb, cfg, executionID)
	if final.Status == StatusError && final.Error["error_type"] == "flow_hash_mismatch" {
		t.Fatalf("mixed-fleet resume was refused: status=%s error=%v (go hash %q vs python %q)",
			final.Status, final.Error, FlowHashAlgoV2, "flow-definition-json-sortkeys-v2")
	}
	if final.Status != StatusCompleted {
		t.Fatalf("status = %q, want completed after cross-engine resume", final.Status)
	}
}

// TestMixedFleetResumeStillRejectsRealChange: the gate itself must stay
// shut — with identical algorithm labels and a genuinely different
// fingerprint (definition actually changed), the resume still terminal-errors
// with flow_hash_mismatch. #60 fixes the fingerprint parity, not the guard.
func TestMixedFleetResumeStillRejectsRealChange(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()

	raw, _ := goldenCase(t, "e2e_mixed_fleet_flow")
	presetFlowJSON(t, w, "mixed_fleet_flow", "1", raw)

	executionID := newExecutionID()
	enqueueTask(t, rdb, cfg, startPayloadFor("mixed_fleet_flow", "1", executionID), "")
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("start ProcessOne: %v", err)
	}
	state := loadStateRow(t, rdb, cfg, executionID)
	subscriptionID, _ := pendingNodeState(t, state, "wait")["subscription_id"].(string)
	if subscriptionID == "" {
		t.Fatal("subscription id missing")
	}

	// A definition actually changed under the same flow/version: the stored
	// hash no longer matches what this worker computes now.
	forged := sha256Hex([]byte(`{"changed":"definition"}`))
	overwriteStateHash(t, rdb, cfg, executionID, forged, FlowHashAlgoV2)

	enqueueTask(t, rdb, cfg, map[string]any{
		"type":         "resume",
		"flow_id":      "mixed_fleet_flow",
		"execution_id": executionID,
		"resume_type":  "event",
		"timestamp":    nowISO(),
		"data": map[string]any{
			"subscription_id": subscriptionID,
			"event_type":      "approval",
		},
	}, "")
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("resume ProcessOne: %v", err)
	}
	final := loadStateRow(t, rdb, cfg, executionID)
	if final.Status != StatusError || final.Error["error_type"] != "flow_hash_mismatch" {
		t.Fatalf("state = %s/%v, want error/flow_hash_mismatch", final.Status, final.Error)
	}
	details, _ := final.Error["details"].(map[string]any)
	if details != nil {
		if suspected, _ := details["upgrade_suspected"].(bool); suspected {
			t.Fatalf("upgrade_suspected must be false for same-algo mismatch: %v", details)
		}
	}
}

// TestMixedFleetLegacyGoRowExplainsUpgrade: an in-flight row written by a
// PRE-#60 Go build carries the old shared label and a mis-computed hash.
// After the label rename the resume gate must flag upgrade_suspected=true so
// operations sees the algorithm-lineage split instead of a silent
// "definition changed" (issue #60 boundary note, option (a)).
func TestMixedFleetLegacyGoRowExplainsUpgrade(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()

	raw, pythonHash := goldenCase(t, "e2e_mixed_fleet_flow")
	presetFlowJSON(t, w, "mixed_fleet_flow", "1", raw)

	executionID := newExecutionID()
	enqueueTask(t, rdb, cfg, startPayloadFor("mixed_fleet_flow", "1", executionID), "")
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("start ProcessOne: %v", err)
	}
	state := loadStateRow(t, rdb, cfg, executionID)
	subscriptionID, _ := pendingNodeState(t, state, "wait")["subscription_id"].(string)
	if subscriptionID == "" {
		t.Fatal("subscription id missing")
	}

	// Pre-#60 Go row: old shared label + a hash computed by the broken
	// canonicalization (here: any value that differs from the correct one —
	// use the Python hash to guarantee the old-Go value differs).
	overwriteStateHash(t, rdb, cfg, executionID, strings_xor_hash(pythonHash), "flow-definition-json-sortkeys-v2")

	enqueueTask(t, rdb, cfg, map[string]any{
		"type":         "resume",
		"flow_id":      "mixed_fleet_flow",
		"execution_id": executionID,
		"resume_type":  "event",
		"timestamp":    nowISO(),
		"data": map[string]any{
			"subscription_id": subscriptionID,
			"event_type":      "approval",
		},
	}, "")
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("resume ProcessOne: %v", err)
	}
	final := loadStateRow(t, rdb, cfg, executionID)
	if final.Status != StatusError || final.Error["error_type"] != "flow_hash_mismatch" {
		t.Fatalf("state = %s/%v, want error/flow_hash_mismatch", final.Status, final.Error)
	}
	details, _ := final.Error["details"].(map[string]any)
	if details == nil {
		t.Fatal("mismatch terminal must carry details")
	}
	if suspected, _ := details["upgrade_suspected"].(bool); !suspected {
		t.Fatalf("legacy-Go row must set upgrade_suspected=true (label split), got %v", details)
	}
}

// strings_xor_hash perturbs a hash deterministically (keeps hex shape,
// guaranteed different from the input).
func strings_xor_hash(h string) string {
	b := []byte(h)
	if b[0] == '0' {
		b[0] = '1'
	} else {
		b[0] = '0'
	}
	return string(b)
}
