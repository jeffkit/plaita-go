package worker

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestQueueMalformedPayloadDLQ: non-JSON payload → DLQ + ack at the parse
// point, with raw_fields + truncated flag (§2.4).
func TestQueueMalformedPayloadDLQ(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	rdb.XAdd(ctx, redisXAddArgs(cfg, "{oops-not-json", "1"))

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	reason, entry := dlqReason(t, rdb, cfg)
	if reason != ReasonInvalidPayload {
		t.Fatalf("reason = %q, want %q", reason, ReasonInvalidPayload)
	}
	if entry["schema_version"] != float64(1) {
		t.Fatalf("dlq schema_version = %v, want 1", entry["schema_version"])
	}
	if entry["source_stream"] != cfg.QueueName || entry["source_id"] == "" {
		t.Fatalf("dlq source fields wrong: %v", entry)
	}
	rawFields, ok := entry["raw_fields"].(map[string]any)
	if !ok {
		t.Fatalf("raw_fields missing: %v", entry)
	}
	if truncated, _ := entry["truncated"].(bool); truncated {
		t.Fatal("short fields must not be truncated")
	}
	if rawFields["payload"] != "{oops-not-json" {
		t.Fatalf("raw payload = %v", rawFields["payload"])
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0 (DLQ + ack)", n)
	}
	if n := pendingCount(t, rdb, cfg); n != 0 {
		t.Fatalf("PEL = %d, want 0", n)
	}
}

// TestQueueTruncatedRawFields: >500-char field values are cut to 500 and
// flagged (§2.4).
func TestQueueTruncatedRawFields(t *testing.T) {
	long := make([]byte, 900)
	for i := range long {
		long[i] = 'x'
	}
	got := truncatedRawFields(map[string]any{"payload": string(long)})
	if got["payload"].(string)[:1] != "x" {
		t.Fatal("unexpected truncated content")
	}
	if len(got["payload"].(string)) != 500 {
		t.Fatalf("truncated len = %d, want 500", len(got["payload"].(string)))
	}
	if got["truncated"] != true {
		t.Fatal("truncated flag must be true")
	}
}

// TestQueueOverschemaDLQ: schema_version above the build version → reject
// into DLQ (§2.2), same verdict on the reclaim path.
func TestQueueOverschemaDLQ(t *testing.T) {
	t.Run("first delivery", func(t *testing.T) {
		w, rdb, _, cfg := newTestWorker(t)
		ctx := context.Background()
		executionID := newExecutionID()
		enqueueTask(t, rdb, cfg, baseStartPayload(executionID), "2")

		if _, err := w.ProcessOne(ctx); err != nil {
			t.Fatalf("ProcessOne: %v", err)
		}
		reason, _ := dlqReason(t, rdb, cfg)
		if reason != ReasonUnsupportedSchema {
			t.Fatalf("reason = %q, want %q", reason, ReasonUnsupportedSchema)
		}
		if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
			t.Fatalf("queue XLEN = %d, want 0", n)
		}
	})

	t.Run("missing schema_version = v1 accepted", func(t *testing.T) {
		_, rdb, _, cfg := newTestWorker(t)

		env := &Envelope{MessageID: "1-1", Fields: map[string]any{"payload": `{"type":"start"}`}}
		q := NewTaskQueue(rdb, cfg, silentLogger{})
		task, err := q.parseEnvelope(env)
		if err != nil {
			t.Fatalf("parseEnvelope: %v", err)
		}
		if env.SchemaVersion != 1 || task.EnvelopeSchemaVer != 1 {
			t.Fatalf("schema version = %d/%d, want 1 (missing = v1)", env.SchemaVersion, task.EnvelopeSchemaVer)
		}
	})

	t.Run("garbage schema_version falls back to v1", func(t *testing.T) {
		_, rdb, _, cfg := newTestWorker(t)
		env := &Envelope{MessageID: "1-1", Fields: map[string]any{"payload": `{"type":"start"}`, "schema_version": "abc"}}
		q := NewTaskQueue(rdb, cfg, silentLogger{})
		if _, err := q.parseEnvelope(env); err != nil {
			t.Fatalf("parseEnvelope: %v", err)
		}
		if env.SchemaVersion != 1 {
			t.Fatalf("schema version = %d, want 1", env.SchemaVersion)
		}
	})
}

// TestQueueMaxDeliveriesDLQ: reclaim path with an exhausted delivery budget →
// guarded dead-letter (§2.4).
func TestQueueMaxDeliveriesDLQ(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	cfg.MaxDeliveries = 1
	ctx := context.Background()

	executionID := newExecutionID()
	payload := baseStartPayload(executionID)
	payload["target_instance_id"] = "someone-else" // MVP: leaves pending untouched
	enqueueTask(t, rdb, cfg, payload, "")

	// First round delivers (count=1) but the target check leaves it pending.
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne first round: %v", err)
	}
	if n := pendingCount(t, rdb, cfg); n != 1 {
		t.Fatalf("PEL = %d, want 1", n)
	}

	// Age past the claim threshold → reclaim sees delivery_count=1 >= max=1.
	time.Sleep(120 * time.Millisecond) // real-time idle (miniredis PEL idle ignores FastForward)
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne reclaim round: %v", err)
	}
	reason, entry := dlqReason(t, rdb, cfg)
	if reason != ReasonMaxDeliveries {
		t.Fatalf("reason = %q, want %q", reason, ReasonMaxDeliveries)
	}
	if entry["delivery_count"] != float64(1) {
		t.Fatalf("delivery_count = %v, want 1", entry["delivery_count"])
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0", n)
	}
}

// TestReclaimLeaseGuard: a pending message whose execution lease is still
// active is never reclaimed (§2.4 #47).
func TestReclaimLeaseGuard(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()

	executionID := newExecutionID()
	payload := baseStartPayload(executionID)
	payload["target_instance_id"] = "someone-else"
	enqueueTask(t, rdb, cfg, payload, "")

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne first round: %v", err)
	}
	rdb.Set(ctx, cfg.KeyPrefix+":execution:lease:"+executionID,
		"start:busy-worker:0123456789abcdef:3", time.Minute)

	time.Sleep(120 * time.Millisecond) // real-time idle (miniredis PEL idle ignores FastForward)
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne reclaim round: %v", err)
	}
	if n := pendingCount(t, rdb, cfg); n != 1 {
		t.Fatalf("PEL = %d, want 1 (lease guard kept it)", n)
	}
	if n := streamLen(t, rdb, cfg.DLQKey); n != 0 {
		t.Fatalf("DLQ XLEN = %d, want 0", n)
	}
	// The claim count must not have grown.
	pe, err := rdb.XPendingExt(ctx, redisXPendingExtArgs(cfg)).Result()
	if err != nil || len(pe) != 1 || pe[0].RetryCount != 1 {
		t.Fatalf("pending entry = %+v err=%v, want single retry_count=1", pe, err)
	}
}

// TestAckDeletesEntry: ack removes the PEL entry AND the stream entry (§2.4).
func TestAckDeletesEntry(t *testing.T) {
	_, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	id := enqueueTask(t, rdb, cfg, map[string]any{"type": "start"}, "")
	q := NewTaskQueue(rdb, cfg, silentLogger{})
	if err := q.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	res := rdb.XReadGroup(ctx, redisXReadGroupArgs(cfg)).Val()
	if len(res) != 1 || len(res[0].Messages) != 1 {
		t.Fatalf("read: %v", res)
	}
	if err := q.Ack(ctx, id); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0 (XDEL after ack)", n)
	}
	if n := pendingCount(t, rdb, cfg); n != 0 {
		t.Fatalf("PEL = %d, want 0", n)
	}
}

// TestDLQTrim: DLQ entries beyond DLQMaxLen are trimmed exactly (§2.1).
func TestDLQTrim(t *testing.T) {
	_, rdb, _, cfg := newTestWorker(t)
	cfg.DLQMaxLen = 3
	ctx := context.Background()
	w := New(cfg, rdb, silentLogger{})
	for i := 0; i < 5; i++ {
		rdb.XAdd(ctx, redisXAddArgs(cfg, fmt.Sprintf("bad-%d", i), "1"))
		if _, err := w.ProcessOne(ctx); err != nil {
			t.Fatalf("ProcessOne: %v", err)
		}
	}
	if n := streamLen(t, rdb, cfg.DLQKey); n != 3 {
		t.Fatalf("DLQ XLEN = %d, want 3 (exact trim)", n)
	}
}

// silentLogger drops worker logs in tests that don't assert on them.
type silentLogger struct{}

func (silentLogger) Info(string, ...any)  {}
func (silentLogger) Warn(string, ...any)  {}
func (silentLogger) Error(string, ...any) {}
