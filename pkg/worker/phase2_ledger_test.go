package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestDispatchLedgerEnqueueWithLedger (§6.3): the message body and the ledger
// entry derive their target from the same input, and the queue/ledger lengths
// match after the dispatch.
func TestDispatchLedgerEnqueueWithLedger(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()

	task := map[string]any{
		"type":         "start",
		"flow_id":      "hello_world_go",
		"execution_id": newExecutionID(),
		"tenant_id":    "",
		"timestamp":    nowISO(),
	}
	entry := &LedgerEntry{
		IssueID:          "issue-42",
		Decision:         DecisionTargeted,
		Reason:           "free disk + lowest active tasks",
		TargetInstanceID: "worker-a",
		CandidateCount:   2,
		Allowed:          []map[string]any{{"instance_id": "worker-a"}},
	}
	out, err := w.ledger.EnqueueWithLedger(ctx, w.queue, task, entry)
	if err != nil {
		t.Fatalf("EnqueueWithLedger: %v", err)
	}
	if !out.Enqueued || !out.LedgerRecorded || out.MessageID == "" || out.LedgerID == "" {
		t.Fatalf("unexpected result: %+v", out)
	}

	// Message body carries the routed target.
	msgs := rdb.XRange(ctx, cfg.QueueName, "-", "+").Val()
	if len(msgs) != 1 {
		t.Fatalf("queue entries = %d, want 1", len(msgs))
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(msgs[0].Values["payload"].(string)), &body); err != nil {
		t.Fatal(err)
	}
	if body["target_instance_id"] != "worker-a" {
		t.Fatalf("message target = %v, want worker-a", body["target_instance_id"])
	}

	entries, err := w.ledger.Read(ctx, 0)
	if err != nil {
		t.Fatalf("ledger read: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("ledger entries = %d, want 1", len(entries))
	}
	got := entries[0]
	if got.TargetInstanceID != "worker-a" || got.Decision != DecisionTargeted ||
		got.MessageID != out.MessageID || got.IssueID != "issue-42" || !got.Enqueued {
		t.Fatalf("ledger entry mismatch: %+v", got)
	}
	if got.LedgerSchemaVersion != ledgerSchemaVersion || got.DecidedAt == "" || got.DecidedBy == "" {
		t.Fatalf("ledger schema fields missing: %+v", got)
	}

	parity, ledgerN, queueN, err := w.ledger.Parity(ctx, cfg.QueueName)
	if err != nil || !parity || ledgerN != 1 || queueN != 1 {
		t.Fatalf("parity = %v (%d/%d) err=%v, want true (1/1)", parity, ledgerN, queueN, err)
	}
}

// TestHandoverToLiveTarget (§9.2): a task routed at another, live instance is
// re-enqueued (target kept) and the original acked; the handover is bounded by
// the affinityhandover counter and recorded in the ledger.
func TestHandoverToLiveTarget(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	executionID := newExecutionID()
	rdb.Set(ctx, cfg.KeyPrefix+":registry:flow_worker:worker-b",
		`{"instance_id":"worker-b","status":"running"}`, time.Minute)

	payload := baseStartPayload(executionID)
	payload["target_instance_id"] = "worker-b"
	enqueueTask(t, rdb, cfg, payload, "")

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	msgs := rdb.XRange(ctx, cfg.QueueName, "-", "+").Val()
	if len(msgs) != 1 {
		t.Fatalf("queue entries = %d, want 1 handover copy", len(msgs))
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(msgs[0].Values["payload"].(string)), &body); err != nil {
		t.Fatal(err)
	}
	if body["target_instance_id"] != "worker-b" {
		t.Fatalf("handover copy target = %v, want worker-b", body["target_instance_id"])
	}
	if n := pendingCount(t, rdb, cfg); n != 0 {
		t.Fatalf("PEL = %d, want 0 (original acked)", n)
	}
	if counter, _ := rdb.Get(ctx, cfg.KeyPrefix+":execution:affinityhandover:"+executionID).Int(); counter != 1 {
		t.Fatalf("handover counter = %d, want 1", counter)
	}

	entries, _ := w.ledger.Read(ctx, 0)
	if len(entries) != 1 || entries[0].Decision != DecisionTargeted ||
		entries[0].Reason != ReasonHandover || !entries[0].Enqueued {
		t.Fatalf("handover ledger entry mismatch: %+v", entries)
	}
	// The ledger's target matches the enqueued body (never forks).
	if entries[0].TargetInstanceID != body["target_instance_id"] {
		t.Fatalf("ledger target %q != body target %v", entries[0].TargetInstanceID, body["target_instance_id"])
	}
}

// TestHandoverTargetUnavailableBroadcasts (A46-02): a target that is no longer
// running is stripped and the task is re-enqueued as a broadcast, so a
// runnable task is not churned to the handover ceiling.
func TestHandoverTargetUnavailableBroadcasts(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	executionID := newExecutionID()
	// Registry entry exists but the worker is draining: not a valid target.
	rdb.Set(ctx, cfg.KeyPrefix+":registry:flow_worker:worker-c",
		`{"instance_id":"worker-c","status":"draining"}`, time.Minute)

	payload := baseStartPayload(executionID)
	payload["target_instance_id"] = "worker-c"
	enqueueTask(t, rdb, cfg, payload, "")

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	msgs := rdb.XRange(ctx, cfg.QueueName, "-", "+").Val()
	if len(msgs) != 1 {
		t.Fatalf("queue entries = %d, want 1 broadcast copy", len(msgs))
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(msgs[0].Values["payload"].(string)), &body); err != nil {
		t.Fatal(err)
	}
	if _, present := body["target_instance_id"]; present {
		t.Fatalf("broadcast copy still has target_instance_id: %v", body)
	}
	if n := pendingCount(t, rdb, cfg); n != 0 {
		t.Fatalf("PEL = %d, want 0 (original acked)", n)
	}
	entries, _ := w.ledger.Read(ctx, 0)
	if len(entries) != 1 || entries[0].Decision != DecisionBroadcast ||
		entries[0].Reason != ReasonTargetUnavailable {
		t.Fatalf("broadcast ledger entry mismatch: %+v", entries)
	}
}

// TestHandoverBudgetExhausted: the counter is the only termination condition;
// at the ceiling the execution is terminalized and the message is left pending
// (never handed over again, never DLQ'd by this branch).
func TestHandoverBudgetExhausted(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	executionID := newExecutionID()
	rdb.Set(ctx, cfg.KeyPrefix+":registry:flow_worker:worker-b",
		`{"instance_id":"worker-b","status":"running"}`, time.Minute)
	rdb.Set(ctx, stateKeyOf(cfg, executionID),
		fmt.Sprintf(`{"execution_id":%q,"status":"running"}`, executionID), 0)
	// The next INCR makes the counter exceed the ceiling.
	rdb.Set(ctx, cfg.KeyPrefix+":execution:affinityhandover:"+executionID,
		fmt.Sprintf("%d", AffinityHandoverMax), 7*24*time.Hour)

	payload := baseStartPayload(executionID)
	payload["target_instance_id"] = "worker-b"
	enqueueTask(t, rdb, cfg, payload, "")

	got, err := w.ProcessOne(ctx)
	if err != nil || !got {
		t.Fatalf("ProcessOne = (%v,%v)", got, err)
	}
	// No new copy, original left pending.
	if n := streamLen(t, rdb, cfg.QueueName); n != 1 {
		t.Fatalf("queue XLEN = %d, want 1 (original only)", n)
	}
	if n := pendingCount(t, rdb, cfg); n != 1 {
		t.Fatalf("PEL = %d, want 1 (left pending)", n)
	}
	state := loadStateRow(t, rdb, cfg, executionID)
	if state.Status != StatusError {
		t.Fatalf("status = %q, want error", state.Status)
	}
	if state.Error["type"] != "AffinityHandoverExhausted" {
		t.Fatalf("error type = %v, want AffinityHandoverExhausted", state.Error["type"])
	}
	if state.Error["affinity_handovers"] != float64(AffinityHandoverMax+1) {
		t.Fatalf("affinity_handovers = %v, want %d", state.Error["affinity_handovers"], AffinityHandoverMax+1)
	}
	if n := streamLen(t, rdb, cfg.DLQKey); n != 0 {
		t.Fatalf("DLQ XLEN = %d, want 0 (handover never dead-letters)", n)
	}
}

// TestHandoverExhaustedDoesNotClobberLiveHolder (#53): the exhaustion
// terminal write must never go out as a fenceGen=0 plain write over a
// running row another worker holds — that bypasses §3.4 and (via the
// terminal-save choke point) releases a quota slot the live execution still
// occupies. With a live holder the write is refused and the message stays
// pending for the real holder to finalize.
func TestHandoverExhaustedDoesNotClobberLiveHolder(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	executionID := newExecutionID()
	rdb.Set(ctx, cfg.KeyPrefix+":registry:flow_worker:worker-b",
		`{"instance_id":"worker-b","status":"running"}`, time.Minute)
	// running row + live lease + fence, all owned by worker-b's generation.
	rdb.Set(ctx, stateKeyOf(cfg, executionID),
		fmt.Sprintf(`{"execution_id":%q,"flow_id":"f9","status":"running"}`, executionID), 0)
	rdb.Set(ctx, cfg.KeyPrefix+":execution:lease:"+executionID,
		"start:worker-b:0123456789abcdef", time.Minute)
	rdb.Set(ctx, cfg.KeyPrefix+":execution:fence:"+executionID, "7", 7*24*time.Hour)
	rdb.Set(ctx, cfg.KeyPrefix+":execution:fencegen:"+executionID, "7", 0)
	// The live execution occupies its tenant start-quota slot (#30).
	rdb.ZAdd(ctx, cfg.KeyPrefix+":tenant_running:"+cfg.DefaultTenantID,
		redis.Z{Score: float64(time.Now().Add(time.Hour).Unix()), Member: executionID})
	// The next INCR exceeds the ceiling.
	rdb.Set(ctx, cfg.KeyPrefix+":execution:affinityhandover:"+executionID,
		fmt.Sprintf("%d", AffinityHandoverMax), 7*24*time.Hour)

	payload := baseStartPayload(executionID)
	payload["target_instance_id"] = "worker-b"
	enqueueTask(t, rdb, cfg, payload, "")

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	state := loadStateRow(t, rdb, cfg, executionID)
	if state.Status != StatusRunning {
		t.Fatalf("status = %q, want running (terminal write refused)", state.Status)
	}
	if h := rdb.Get(ctx, cfg.KeyPrefix+":execution:lease:"+executionID).Val(); h != "start:worker-b:0123456789abcdef" {
		t.Fatalf("lease = %q, want the live holder untouched", h)
	}
	if n := rdb.ZCard(ctx, cfg.KeyPrefix+":tenant_running:"+cfg.DefaultTenantID).Val(); n != 1 {
		t.Fatalf("tenant_running ZCARD = %d, want 1 (quota slot kept)", n)
	}
	if n := pendingCount(t, rdb, cfg); n != 1 {
		t.Fatalf("PEL = %d, want 1 (left pending for the holder)", n)
	}
}

// TestHandoverExhaustedTerminalizesUnownedRowFenced (#53 green side): with no
// live holder the terminal write goes out under a freshly claimed fence
// generation (§3.4) instead of a fenceGen=0 plain write, and the claim lease
// is released right after.
func TestHandoverExhaustedTerminalizesUnownedRowFenced(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	executionID := newExecutionID()
	rdb.Set(ctx, cfg.KeyPrefix+":registry:flow_worker:worker-b",
		`{"instance_id":"worker-b","status":"running"}`, time.Minute)
	rdb.Set(ctx, stateKeyOf(cfg, executionID),
		fmt.Sprintf(`{"execution_id":%q,"flow_id":"f9","status":"running"}`, executionID), 0)
	// Dead generation trail, no live lease (§3.1: fence outlives the lease).
	rdb.Set(ctx, cfg.KeyPrefix+":execution:fence:"+executionID, "2", 7*24*time.Hour)
	rdb.Set(ctx, cfg.KeyPrefix+":execution:fencegen:"+executionID, "2", 0)
	rdb.Set(ctx, cfg.KeyPrefix+":execution:affinityhandover:"+executionID,
		fmt.Sprintf("%d", AffinityHandoverMax), 7*24*time.Hour)

	payload := baseStartPayload(executionID)
	payload["target_instance_id"] = "worker-b"
	enqueueTask(t, rdb, cfg, payload, "")

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	state := loadStateRow(t, rdb, cfg, executionID)
	if state.Status != StatusError || state.Error["type"] != "AffinityHandoverExhausted" {
		t.Fatalf("status = %q error = %v, want AffinityHandoverExhausted", state.Status, state.Error)
	}
	if gen := rdb.Get(ctx, cfg.KeyPrefix+":execution:fencegen:"+executionID).Val(); gen == "" || gen == "2" {
		t.Fatalf("fencegen = %q, want a freshly claimed generation past 2", gen)
	}
	if h := rdb.Get(ctx, cfg.KeyPrefix+":execution:lease:"+executionID).Val(); h != "" {
		t.Fatalf("claim lease not released: %q", h)
	}
	if n := pendingCount(t, rdb, cfg); n != 1 {
		t.Fatalf("PEL = %d, want 1 (terminalized row leaves the message pending)", n)
	}
}
