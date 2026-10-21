package worker

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func oldTimestamp(d time.Duration) string {
	return time.Now().Add(-d).Format("2006-01-02T15:04:05.999999")
}

// TestTaskAgeGateExpiredCarrierDropped (#29): a start past the horizon is
// acked WITHOUT executing, never dead-lettered, and the drop is observable
// (counter + live execution event).
func TestTaskAgeGateExpiredCarrierDropped(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	w.cfg.TaskMaxAgeSeconds = 3600 // 1h horizon
	rec := newRecordingMetrics()
	w.SetMetrics(rec)
	ctx := context.Background()
	presetHelloWorld(t, w)

	executionID := newExecutionID()
	channel := cfg.KeyPrefix + ":execution:events:" + executionID
	pubsub := rdb.Subscribe(ctx, channel)
	defer pubsub.Close()
	if _, err := pubsub.Receive(ctx); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	payload := baseStartPayload(executionID)
	payload["timestamp"] = oldTimestamp(2 * time.Hour)
	enqueueTask(t, rdb, cfg, payload, "")

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	// Dropped: acked + XDELed, no execution, no DLQ.
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0 (acked)", n)
	}
	if n := pendingCount(t, rdb, cfg); n != 0 {
		t.Fatalf("PEL = %d, want 0", n)
	}
	if n := streamLen(t, rdb, cfg.DLQKey); n != 0 {
		t.Fatalf("DLQ XLEN = %d, want 0 (drop is not a dead letter)", n)
	}
	if _, err := rdb.Get(ctx, stateKeyOf(cfg, executionID)).Result(); err == nil {
		t.Fatal("state row must not exist (expired task must not execute)")
	}
	if got := rec.total(MetricTaskExpiredTotal); got != 1 {
		t.Fatalf("task_expired_total = %d, want 1", got)
	}
	msg, err := pubsub.ReceiveTimeout(ctx, 2*time.Second)
	if err != nil {
		t.Fatalf("expected a live task_expired event: %v", err)
	}
	redisMsg, ok := msg.(*redis.Message)
	if !ok || !strings.Contains(redisMsg.Payload, "task_expired") {
		t.Fatalf("unexpected pubsub message %T: %v", msg, redisMsg)
	}
}

// TestTaskAgeGateDisabledProcessesAncient (#29): a negative horizon disables
// the gate — ancient carriers execute.
func TestTaskAgeGateDisabledProcessesAncient(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	w.cfg.TaskMaxAgeSeconds = -1
	rec := newRecordingMetrics()
	w.SetMetrics(rec)
	ctx := context.Background()
	presetHelloWorld(t, w)

	executionID := newExecutionID()
	payload := baseStartPayload(executionID)
	payload["timestamp"] = oldTimestamp(45 * 24 * time.Hour)
	enqueueTask(t, rdb, cfg, payload, "")
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	if state := loadStateRow(t, rdb, cfg, executionID); state.Status != StatusCompleted {
		t.Fatalf("status = %q, want completed (gate disabled)", state.Status)
	}
	if got := rec.total(MetricTaskExpiredTotal); got != 0 {
		t.Fatalf("task_expired_total = %d, want 0", got)
	}
}

// TestTaskAgeGateStreamIDMaxWins (#29): a fresh payload timestamp cannot
// whitewash an old Stream ID (dual-source max) — the XADD id is 2001.
func TestTaskAgeGateStreamIDMaxWins(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	rec := newRecordingMetrics()
	w.SetMetrics(rec)
	ctx := context.Background()
	presetHelloWorld(t, w)

	executionID := newExecutionID()
	payload := baseStartPayload(executionID)
	payload["timestamp"] = nowISO() // fresh, forged
	if err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: cfg.QueueName,
		ID:     "1000000000000-0", // 2001-09-09
		Values: map[string]any{
			"payload":        string(mustJSON(t, payload)),
			"schema_version": "1",
		},
	}).Err(); err != nil {
		t.Fatalf("xadd explicit id: %v", err)
	}
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	if _, err := rdb.Get(ctx, stateKeyOf(cfg, executionID)).Result(); err == nil {
		t.Fatal("state row must not exist (old stream id must not be washed by a fresh timestamp)")
	}
	if got := rec.total(MetricTaskExpiredTotal); got != 1 {
		t.Fatalf("task_expired_total = %d, want 1", got)
	}
}

// TestTaskAgeGateUnknownAgeFailClosed (#29): neither source determinable →
// drop + dedicated counter.
func TestTaskAgeGateUnknownAgeFailClosed(t *testing.T) {
	w, _, _, _ := newTestWorker(t)
	rec := newRecordingMetrics()
	w.SetMetrics(rec)
	task := &Task{Type: "start"} // no timestamp
	if !w.carrierTooOld(task, "not-a-stream-id") {
		t.Fatal("undeterminable age must fail closed (drop)")
	}
	if got := rec.total(MetricTaskAgeUnknownTotal); got != 1 {
		t.Fatalf("task_age_unknown_total = %d, want 1", got)
	}
	// Sanity of the source parsers.
	if _, ok := taskAgeFromStreamID("1712345678901-0"); !ok {
		t.Fatal("numeric stream id must yield an age")
	}
	if _, ok := taskAgeFromTimestamp(""); ok {
		t.Fatal("empty timestamp must be undeterminable")
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	blob, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return blob
}

// TestDuplicateStartTerminalNotOverwritten (#29): a duplicate start on a
// terminal execution is an idempotent ack — the terminal row is untouched and
// the duplicate never reaches the DLQ.
func TestDuplicateStartTerminalNotOverwritten(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	rec := newRecordingMetrics()
	w.SetMetrics(rec)
	ctx := context.Background()
	presetHelloWorld(t, w)

	executionID := newExecutionID()
	enqueueTask(t, rdb, cfg, baseStartPayload(executionID), "")
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("first ProcessOne: %v", err)
	}
	first := loadStateRow(t, rdb, cfg, executionID)
	if first.Status != StatusCompleted {
		t.Fatalf("first run status = %q, want completed", first.Status)
	}

	// Duplicate start: same execution_id, fresh message.
	payload := baseStartPayload(executionID)
	payload["timestamp"] = nowISO()
	enqueueTask(t, rdb, cfg, payload, "")
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("duplicate ProcessOne: %v", err)
	}

	second := loadStateRow(t, rdb, cfg, executionID)
	if second.EndTime != first.EndTime {
		t.Fatalf("terminal row overwritten: end_time %q → %q", first.EndTime, second.EndTime)
	}
	if second.Status != StatusCompleted {
		t.Fatalf("status = %q, want completed", second.Status)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0 (duplicate acked)", n)
	}
	if n := streamLen(t, rdb, cfg.DLQKey); n != 0 {
		t.Fatalf("DLQ XLEN = %d, want 0 (duplicate start never dead-letters)", n)
	}
	if n := pendingCount(t, rdb, cfg); n != 0 {
		t.Fatalf("PEL = %d, want 0", n)
	}
	if got := rec.total(MetricTaskDedupHitTotal); got != 1 {
		t.Fatalf("task_dedup_hit_total = %d, want 1", got)
	}
	if got := rec.labelOf(MetricTaskDedupHitTotal, "status"); got != StatusCompleted {
		t.Fatalf("dedup hit status label = %q, want completed", got)
	}
}

// TestDuplicateStartRunningRowNoOverwrite (#29) was retired by #47: its
// premise — a redelivered start on a running row is an idempotent ack — is
// exactly the stranding bug (#47). The running branch now routes through the
// §9.1 conversion; the replacement negative controls live in
// idempotent_gate_routing_test.go (a genuinely-live execution never reaches
// the gate: TestWorkerLeaseConflict covers the lease-conflict ack).

// TestDuplicateStartFlowConflictPoisonAck (#29): an execution id owned by
// another flow is poison-acked (ExecutionIdConflict parity) — no overwrite,
// no DLQ.
func TestDuplicateStartFlowConflictPoisonAck(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	rec := newRecordingMetrics()
	w.SetMetrics(rec)
	ctx := context.Background()
	presetHelloWorld(t, w)

	executionID := newExecutionID()
	seeded := &ExecutionState{
		ExecutionID: executionID,
		FlowID:      "another_flow",
		Status:      StatusCompleted,
		StartTime:   nowISO(),
		EndTime:     nowISO(),
		Context:     map[string]any{},
	}
	if err := w.store.SaveExecutionState(ctx, "", seeded, 0); err != nil {
		t.Fatal(err)
	}

	enqueueTask(t, rdb, cfg, baseStartPayload(executionID), "")
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	row := loadStateRow(t, rdb, cfg, executionID)
	if row.FlowID != "another_flow" || row.Status != StatusCompleted {
		t.Fatalf("row overwritten: flow=%s status=%s", row.FlowID, row.Status)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0 (poison ack)", n)
	}
	if n := streamLen(t, rdb, cfg.DLQKey); n != 0 {
		t.Fatalf("DLQ XLEN = %d, want 0", n)
	}
	if got := rec.labelOf(MetricTaskDedupHitTotal, "reason"); got != "flow_conflict" {
		t.Fatalf("dedup hit reason = %q, want flow_conflict", got)
	}
}

// TestDuplicateStartSuspendedStaysSuspended (#29): the M6 suspended short
// circuit now flows through the idempotency gate — no replay, no overwrite.
func TestDuplicateStartSuspendedStaysSuspended(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	presetHelloWorld(t, w)

	executionID := newExecutionID()
	seeded := &ExecutionState{
		ExecutionID: executionID,
		FlowID:      "hello_world_go",
		Status:      StatusSuspended,
		StartTime:   nowISO(),
		Context:     map[string]any{"$LAST_NODE": "await"},
	}
	if err := w.store.SaveExecutionState(ctx, "", seeded, 0); err != nil {
		t.Fatal(err)
	}

	enqueueTask(t, rdb, cfg, baseStartPayload(executionID), "")
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	row := loadStateRow(t, rdb, cfg, executionID)
	if row.Status != StatusSuspended {
		t.Fatalf("status = %q, want suspended (start must not replay a suspended flow)", row.Status)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0 (idempotent ack)", n)
	}
}

// TestRedeliveredDuplicateStartExecutesOnce (#29 验收「同一 execution 重复
// start 只执行一次」): two queued copies of the same start produce exactly one
// execution; the redelivered copy is the idempotent ack.
func TestRedeliveredDuplicateStartExecutesOnce(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	presetHelloWorld(t, w)

	executionID := newExecutionID()
	enqueueTask(t, rdb, cfg, baseStartPayload(executionID), "")
	enqueueTask(t, rdb, cfg, baseStartPayload(executionID), "")

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("first ProcessOne: %v", err)
	}
	first := loadStateRow(t, rdb, cfg, executionID)
	if first.Status != StatusCompleted {
		t.Fatalf("status = %q, want completed", first.Status)
	}
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("duplicate ProcessOne: %v", err)
	}

	second := loadStateRow(t, rdb, cfg, executionID)
	if second.EndTime != first.EndTime || second.Status != StatusCompleted {
		t.Fatalf("second copy re-executed: end_time %q → %q", first.EndTime, second.EndTime)
	}
	// Exactly one run: a single run_start on the timeline.
	events, _ := second.Timeline["events"].([]any)
	runs := 0
	for _, raw := range events {
		ev, _ := raw.(map[string]any)
		if ev["type"] == "run_start" {
			runs++
		}
	}
	if runs != 1 {
		t.Fatalf("run_start count = %d, want 1 (duplicate must not re-run)", runs)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0", n)
	}
	if n := pendingCount(t, rdb, cfg); n != 0 {
		t.Fatalf("PEL = %d, want 0", n)
	}
}
