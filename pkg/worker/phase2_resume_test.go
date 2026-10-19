package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

// TestResumeSuspendedContinueCompensatesDispatch (plaita#233, A47-F2): a
// suspended + continue carrier carries no progress semantics and takes no
// lease, but it MUST re-dispatch a pending suspend service task and clear the
// credential before acking.
func TestResumeSuspendedContinueCompensatesDispatch(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	executionID := newExecutionID()

	stateRow := map[string]any{
		"execution_id": executionID,
		"status":       StatusSuspended,
		"context":      map[string]any{"$INPUT": map[string]any{}},
		"dispatch_pending": map[string]any{
			"pending":        true,
			"service_type":   "delay",
			"service_config": map[string]any{"type": "delay", "delay_seconds": 5},
			"node_id":        "wait",
			"recorded_at":    nowISO(),
		},
	}
	blob, _ := json.Marshal(stateRow)
	rdb.Set(ctx, stateKeyOf(cfg, executionID), string(blob), 0)

	payload := baseStartPayload(executionID)
	payload["type"] = "resume"
	payload["resume_type"] = "continue"
	enqueueTask(t, rdb, cfg, payload, "")

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}

	// The service task landed on {prefix}:{subtype}:queue with execution +
	// tenant attached.
	queueKey := cfg.KeyPrefix + ":delay:queue"
	entries, err := rdb.LRange(ctx, queueKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("service queue read: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("service queue entries = %d, want 1", len(entries))
	}
	var serviceTask map[string]any
	if err := json.Unmarshal([]byte(entries[0]), &serviceTask); err != nil {
		t.Fatalf("service task decode: %v", err)
	}
	if serviceTask["execution_id"] != executionID || serviceTask["type"] != "delay" {
		t.Fatalf("service task shape wrong: %v", serviceTask)
	}

	// Credential cleared, status still suspended, message acked.
	state := loadStateRow(t, rdb, cfg, executionID)
	if state.Status != StatusSuspended {
		t.Fatalf("status = %q, want suspended", state.Status)
	}
	if state.DispatchPending != nil {
		t.Fatalf("dispatch_pending must be cleared, got %v", state.DispatchPending)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0 (idempotent ack)", n)
	}
	if v := rdb.Get(ctx, cfg.KeyPrefix+":execution:lease:"+executionID).Val(); v != "" {
		t.Fatalf("lease must be released after compensation, got %q", v)
	}

	// A second identical carrier must not dispatch again (credential gone).
	enqueueTask(t, rdb, cfg, payload, "")
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("second ProcessOne: %v", err)
	}
	if n, _ := rdb.LLen(ctx, queueKey).Result(); n != 1 {
		t.Fatalf("service queue entries after duplicate carrier = %d, want 1", n)
	}
}

// TestResumeSuspendedContinueNoDispatchCredential: the plain #33 short-circuit
// takes no lease and writes nothing.
func TestResumeSuspendedContinueNoDispatchCredential(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	executionID := newExecutionID()
	rdb.Set(ctx, stateKeyOf(cfg, executionID),
		fmt.Sprintf(`{"execution_id":%q,"status":"suspended"}`, executionID), 0)

	payload := baseStartPayload(executionID)
	payload["type"] = "resume"
	payload["resume_type"] = "continue"
	enqueueTask(t, rdb, cfg, payload, "")

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0", n)
	}
	if v := rdb.Get(ctx, cfg.KeyPrefix+":execution:lease:"+executionID).Val(); v != "" {
		t.Fatalf("no lease expected, got %q", v)
	}
	if v := rdb.Get(ctx, cfg.KeyPrefix+":execution:fencegen:"+executionID).Val(); v != "" {
		t.Fatalf("no fence generation may be minted, got %q", v)
	}
}

// TestResumeEventOnRunningNormalizesToContinue (§9.2 / #258): an event carrier
// that missed the suspend point is replaced by a continue carrier (enqueue →
// ack), inheriting the source envelope version.
func TestResumeEventOnRunningNormalizesToContinue(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	executionID := newExecutionID()
	rdb.Set(ctx, stateKeyOf(cfg, executionID),
		fmt.Sprintf(`{"execution_id":%q,"status":"running"}`, executionID), 0)

	payload := baseStartPayload(executionID)
	payload["type"] = "resume"
	payload["resume_type"] = "event"
	enqueueTask(t, rdb, cfg, payload, "")

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	msgs := rdb.XRange(ctx, cfg.QueueName, "-", "+").Val()
	if len(msgs) != 1 {
		t.Fatalf("queue entries = %d, want 1 normalized carrier", len(msgs))
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(msgs[0].Values["payload"].(string)), &body); err != nil {
		t.Fatalf("normalized carrier decode: %v", err)
	}
	if body["type"] != "resume" || body["resume_type"] != "continue" {
		t.Fatalf("normalized carrier = %v, want resume/continue", body)
	}
	if body["execution_id"] != executionID {
		t.Fatalf("execution_id = %v, want %s", body["execution_id"], executionID)
	}
	if msgs[0].Values["schema_version"] != fmt.Sprintf("%d", TaskSchemaVersion) {
		t.Fatalf("schema_version = %v, want inherited v%d", msgs[0].Values["schema_version"], TaskSchemaVersion)
	}
	// The original was acked; the state row was not terminalized by the
	// normalization step itself.
	if state := loadStateRow(t, rdb, cfg, executionID); state.Status != StatusRunning {
		t.Fatalf("status = %q, want running (normalization writes nothing)", state.Status)
	}
}

// TestResumeSuspendedWithoutCheckpointFailsDeterministically: M6 resumes a
// suspended execution from its checkpoint. A suspended row with no checkpoint
// can never advance — the carrier is acked with an observable error terminal
// (not left pending forever).
func TestResumeSuspendedWithoutCheckpointFailsDeterministically(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	presetHelloWorld(t, w)
	executionID := newExecutionID()
	rdb.Set(ctx, stateKeyOf(cfg, executionID),
		fmt.Sprintf(`{"execution_id":%q,"flow_id":"hello_world_go","status":"suspended"}`, executionID), 0)

	payload := baseStartPayload(executionID)
	payload["type"] = "resume"
	payload["resume_type"] = "timeout"
	enqueueTask(t, rdb, cfg, payload, "")

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	state := loadStateRow(t, rdb, cfg, executionID)
	if state.Status != StatusError || state.Error["error_type"] != "resume_checkpoint_missing" {
		t.Fatalf("state = %+v, want error/resume_checkpoint_missing", state)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0", n)
	}
	if v := rdb.Get(ctx, cfg.KeyPrefix+":execution:lease:"+executionID).Val(); v != "" {
		t.Fatalf("lease must be released, got %q", v)
	}
}

// TestResumeCancelCarrierOnLiveLeaseNeverAcked: a cancel decision carrier in
// the lease-conflict window must survive for the holder's release (A47-F2).
func TestResumeCancelCarrierOnLiveLeaseNeverAcked(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	executionID := newExecutionID()
	rdb.Set(ctx, stateKeyOf(cfg, executionID),
		fmt.Sprintf(`{"execution_id":%q,"status":"suspended"}`, executionID), 0)
	rdb.Set(ctx, cfg.KeyPrefix+":execution:lease:"+executionID,
		"start:other-worker:0123456789abcdef:7", time.Minute)
	rdb.Set(ctx, cfg.KeyPrefix+":registry:flow_worker:other-worker",
		`{"instance_id":"other-worker","status":"running"}`, time.Minute)

	payload := baseStartPayload(executionID)
	payload["type"] = "resume"
	payload["resume_type"] = "cancel"
	enqueueTask(t, rdb, cfg, payload, "")

	if _, err := w.ProcessOne(ctx); err == nil {
		t.Fatal("decision carriers must not be acked while another worker holds the lease")
	}
	if n := pendingCount(t, rdb, cfg); n != 1 {
		t.Fatalf("PEL = %d, want 1 (pending for reclaim)", n)
	}
}
