package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// writeDLQ appends one dead-letter envelope and returns its stream id.
func writeDLQ(t *testing.T, rdb *redis.Client, cfg *Config, envelope map[string]any) string {
	t.Helper()
	blob, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	id, err := rdb.XAdd(context.Background(), &redis.XAddArgs{
		Stream: cfg.DLQKey,
		Values: map[string]any{"payload": string(blob), "schema_version": "1"},
	}).Result()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func dlqEnvelopeFor(body map[string]any, schema any) map[string]any {
	return map[string]any{
		"reason":           ReasonMaxDeliveries,
		"source_stream":    "plaita-go-shadow:flow:queue",
		"source_id":        "1-1",
		"delivery_count":   1,
		"dead_lettered_at": floatEpoch(),
		"schema_version":   schema,
		"payload":          body,
	}
}

// TestDLQReplayIdempotent (§2.4): one marker per (dlq_key, dlq_id); a second
// replay returns already_replayed and does not enqueue; both attempts are
// audited; the DLQ original is never deleted.
func TestDLQReplayIdempotent(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	body := baseStartPayload(newExecutionID())
	body["tenant_id"] = "default"
	dlqID := writeDLQ(t, rdb, cfg, dlqEnvelopeFor(body, 1))

	first := w.ReplayDeadLetter(ctx, ReplayOptions{DLQID: dlqID, Actor: "tester"})
	if !first.Replayed || first.Reason != ReplayReasonReplayed {
		t.Fatalf("first replay = %+v, want replayed", first)
	}
	if first.ExecutionID != body["execution_id"] || first.TargetMessageID == "" {
		t.Fatalf("first replay fields wrong: %+v", first)
	}
	msgs := rdb.XRange(ctx, cfg.QueueName, "-", "+").Val()
	if len(msgs) != 1 {
		t.Fatalf("queue entries = %d, want 1", len(msgs))
	}
	if msgs[0].Values["schema_version"] != "1" {
		t.Fatalf("inherited schema_version = %v, want 1", msgs[0].Values["schema_version"])
	}
	var replayBody map[string]any
	if err := json.Unmarshal([]byte(msgs[0].Values["payload"].(string)), &replayBody); err != nil {
		t.Fatal(err)
	}
	if replayBody["tenant_id"] != "default" || replayBody["execution_id"] != body["execution_id"] {
		t.Fatalf("replayed body wrong: %v", replayBody)
	}

	second := w.ReplayDeadLetter(ctx, ReplayOptions{DLQID: dlqID, Actor: "tester"})
	if second.Replayed || second.Reason != ReplayReasonAlready {
		t.Fatalf("second replay = %+v, want already_replayed", second)
	}
	if second.TargetMessageID != first.TargetMessageID {
		t.Fatalf("already_replayed target = %q, want %q", second.TargetMessageID, first.TargetMessageID)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 1 {
		t.Fatalf("queue XLEN = %d, want 1 (no duplicate)", n)
	}
	if n := streamLen(t, rdb, cfg.DLQKey); n != 1 {
		t.Fatalf("DLQ XLEN = %d, want 1 (evidence kept)", n)
	}
	if n := streamLen(t, rdb, cfg.KeyPrefix+":dlq:replay:audit"); n != 2 {
		t.Fatalf("audit XLEN = %d, want 2", n)
	}
}

// TestDLQReplaySchemaTooNew (A53-03): the source envelope version is
// inherited, and an over-version carrier is rejected before enqueue.
func TestDLQReplaySchemaTooNew(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	body := baseStartPayload(newExecutionID())
	body["tenant_id"] = "default"
	dlqID := writeDLQ(t, rdb, cfg, dlqEnvelopeFor(body, 2))

	res := w.ReplayDeadLetter(ctx, ReplayOptions{DLQID: dlqID, Actor: "tester"})
	if res.Replayed || res.Reason != ReplayReasonSchemaNew {
		t.Fatalf("replay = %+v, want schema_too_new", res)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0", n)
	}
	if n := streamLen(t, rdb, cfg.KeyPrefix+":dlq:replay:audit"); n != 1 {
		t.Fatalf("audit XLEN = %d, want 1", n)
	}
}

// TestDLQReplayTruncated: a truncated dead-letter payload cannot be replayed
// safely.
func TestDLQReplayTruncated(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	envelope := dlqEnvelopeFor(map[string]any{"type": "start"}, 1)
	envelope["truncated"] = true
	dlqID := writeDLQ(t, rdb, cfg, envelope)

	res := w.ReplayDeadLetter(ctx, ReplayOptions{DLQID: dlqID, Actor: "tester"})
	if res.Replayed || res.Reason != ReplayReasonNotReplay {
		t.Fatalf("replay = %+v, want not_replayable", res)
	}
}

// TestDLQReplayDeterministicExecutionID (A53-02): a start carrier without an
// execution id gets uuid5(dlq_key, dlq_id) — a forced replay still converges on
// one execution.
func TestDLQReplayDeterministicExecutionID(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	body := map[string]any{
		"type":      "start",
		"flow_id":   "hello_world_go",
		"tenant_id": "default",
		"timestamp": nowISO(),
	}
	dlqID := writeDLQ(t, rdb, cfg, dlqEnvelopeFor(body, 1))

	first := w.ReplayDeadLetter(ctx, ReplayOptions{DLQID: dlqID, Actor: "tester"})
	if !first.Replayed || !first.UsedDeterministic {
		t.Fatalf("first replay = %+v, want replayed with a minted id", first)
	}
	want := deterministicReplayExecutionID(cfg.DLQKey, dlqID)
	if first.ExecutionID != want {
		t.Fatalf("minted execution id = %q, want %q", first.ExecutionID, want)
	}
	msgs := rdb.XRange(ctx, cfg.QueueName, "-", "+").Val()
	var replayBody map[string]any
	_ = json.Unmarshal([]byte(msgs[0].Values["payload"].(string)), &replayBody)
	if replayBody["execution_id"] != want {
		t.Fatalf("carrier execution_id = %v, want %s", replayBody["execution_id"], want)
	}
	if replayBody["dedup_key"] != "dlq-replay:"+want {
		t.Fatalf("dedup_key = %v, want dlq-replay:%s", replayBody["dedup_key"], want)
	}

	second := w.ReplayDeadLetter(ctx, ReplayOptions{DLQID: dlqID, Actor: "tester", Force: true})
	if !second.Replayed || second.ExecutionID != want {
		t.Fatalf("forced replay = %+v, want the same execution id", second)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 2 {
		t.Fatalf("queue XLEN = %d, want 2 (forced replay enqueues again)", n)
	}
}

// TestDLQReplayRequiresTenant: a carrier with no tenant cannot be attributed,
// so the replay requires an explicit one (fail-closed).
func TestDLQReplayRequiresTenant(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	body := baseStartPayload(newExecutionID()) // tenant_id = ""
	dlqID := writeDLQ(t, rdb, cfg, dlqEnvelopeFor(body, 1))

	res := w.ReplayDeadLetter(ctx, ReplayOptions{DLQID: dlqID, Actor: "tester"})
	if res.Replayed || res.Reason != ReplayReasonNotReplay {
		t.Fatalf("replay = %+v, want not_replayable", res)
	}
	// With the explicit tenant the same entry replays.
	res = w.ReplayDeadLetter(ctx, ReplayOptions{DLQID: dlqID, Actor: "tester", TenantID: "default"})
	if !res.Replayed {
		t.Fatalf("explicit-tenant replay = %+v, want replayed", res)
	}
}

// TestDLQReplayDeletedExecution: a tombstone blocks resurrection (A53-06).
func TestDLQReplayDeletedExecution(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	body := baseStartPayload(newExecutionID())
	body["tenant_id"] = "default"
	rdb.Set(ctx, cfg.KeyPrefix+":exec_tombstone:"+body["execution_id"].(string),
		`{"tenant":"default","reason":"deleted by test"}`, 0)
	dlqID := writeDLQ(t, rdb, cfg, dlqEnvelopeFor(body, 1))

	res := w.ReplayDeadLetter(ctx, ReplayOptions{DLQID: dlqID, Actor: "tester"})
	if res.Replayed || res.Reason != ReplayReasonDeleted {
		t.Fatalf("replay = %+v, want execution_deleted", res)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0", n)
	}
}

// TestDeadLetterGuardRecoveryClone (§9.2 last row): a non-terminal execution
// with no lease gets a recovery carrier before the original is dead-lettered.
func TestDeadLetterGuardRecoveryClone(t *testing.T) {
	_, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	cfg.MaxDeliveries = 1
	w := New(cfg, rdb, silentLogger{})
	q := w.queue
	if err := q.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}

	executionID := newExecutionID()
	rdb.Set(ctx, stateKeyOf(cfg, executionID),
		fmt.Sprintf(`{"execution_id":%q,"status":"running"}`, executionID), 0)
	seedPending(t, rdb, cfg, baseStartPayload(executionID))
	time.Sleep(120 * time.Millisecond)

	delivery, err := q.Read(ctx)
	if err != nil || delivery != nil {
		t.Fatalf("queue read = (%v,%v), want a dead-letter", delivery, err)
	}
	if n := streamLen(t, rdb, cfg.DLQKey); n != 1 {
		t.Fatalf("DLQ XLEN = %d, want 1", n)
	}
	msgs := rdb.XRange(ctx, cfg.QueueName, "-", "+").Val()
	if len(msgs) != 1 {
		t.Fatalf("queue entries = %d, want 1 recovery clone", len(msgs))
	}
	var clone map[string]any
	if err := json.Unmarshal([]byte(msgs[0].Values["payload"].(string)), &clone); err != nil {
		t.Fatal(err)
	}
	if clone["execution_id"] != executionID || clone["type"] != "start" {
		t.Fatalf("recovery clone wrong: %v", clone)
	}
	if msgs[0].Values["schema_version"] != "1" {
		t.Fatalf("clone schema_version = %v, want inherited 1", msgs[0].Values["schema_version"])
	}
	if state := loadStateRow(t, rdb, cfg, executionID); state.Status != StatusRunning {
		t.Fatalf("guard must not terminalize a resumable run, got %q", state.Status)
	}
}

// TestDeadLetterGuardBudgetExhaustedTerminalizes (§9.2): running + retry budget
// reached → error terminal, then the dead letter (no recovery clone).
func TestDeadLetterGuardBudgetExhaustedTerminalizes(t *testing.T) {
	_, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	cfg.MaxDeliveries = 1
	w := New(cfg, rdb, silentLogger{})
	q := w.queue
	if err := q.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}

	executionID := newExecutionID()
	rdb.Set(ctx, stateKeyOf(cfg, executionID),
		fmt.Sprintf(`{"execution_id":%q,"status":"running"}`, executionID), 0)
	rdb.Set(ctx, cfg.KeyPrefix+":execution:noderetry:"+executionID+":boom", "1", time.Hour)
	seedPending(t, rdb, cfg, baseStartPayload(executionID))
	time.Sleep(120 * time.Millisecond)

	if _, err := q.Read(ctx); err != nil {
		t.Fatalf("queue read: %v", err)
	}
	if n := streamLen(t, rdb, cfg.DLQKey); n != 1 {
		t.Fatalf("DLQ XLEN = %d, want 1", n)
	}
	// No clone: the flow itself must not be re-enqueued once its budget is
	// exhausted (otherwise: clone → exhaust → clone forever).
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0 (no recovery clone)", n)
	}
	state := loadStateRow(t, rdb, cfg, executionID)
	if state.Status != StatusError {
		t.Fatalf("status = %q, want error", state.Status)
	}
	if state.Error["error_type"] != "node_retry_budget_exhausted" {
		t.Fatalf("error_type = %v, want node_retry_budget_exhausted", state.Error["error_type"])
	}
}
