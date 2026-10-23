package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// dlqID0Minus1 XADDs a DLQ envelope under the explicit id "0-1" (millisecond
// prefix 0 — unparseable by taskAgeFromStreamID) to lock the fail-closed
// contract of #67 case B.
func dlqID0Minus1(t *testing.T, rdb *redis.Client, cfg *Config, envelope map[string]any) {
	t.Helper()
	blob, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rdb.XAdd(context.Background(), &redis.XAddArgs{
		Stream: cfg.DLQKey,
		ID:     "0-1",
		Values: map[string]any{"payload": string(blob), "schema_version": "1"},
	}).Result(); err != nil {
		t.Fatalf("XADD 0-1: %v", err)
	}
}

func replayAuditLastRow(t *testing.T, rdb *redis.Client, cfg *Config) map[string]any {
	t.Helper()
	msgs := rdb.XRange(context.Background(), cfg.KeyPrefix+":dlq:replay:audit", "-", "+").Val()
	if len(msgs) == 0 {
		t.Fatal("expected replay audit rows, got none")
	}
	return msgs[len(msgs)-1].Values
}

// TestDLQReplayBackfillsCarrierAgeFromDeadLetterTime (#67 case A): a payload
// without a timestamp must inherit the DLQ entry's age. Before the fix the
// replay reset the age to ~0 (fresh stream id) and the 40-day-old start was
// EXECUTED — the exact scenario the 30-day age gate exists to prevent.
func TestDLQReplayBackfillsCarrierAgeFromDeadLetterTime(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	w.cfg.TaskMaxAgeSeconds = 3600 // 1h horizon: the 40d-old carrier must trip it
	rec := newRecordingMetrics()
	w.SetMetrics(rec)
	ctx := context.Background()
	presetHelloWorld(t, w)

	// The most common replay object: a start carrier with no execution_id
	// (deterministic id gets minted → no state row can exist → would really
	// run) and no timestamp (the legacy shape the issue is about).
	body := map[string]any{
		"type":      "start",
		"flow_id":   "hello_world_go",
		"tenant_id": "default",
	}
	envelope := dlqEnvelopeFor(body, 1)
	envelope["dead_lettered_at"] = floatEpoch() - 40*86400
	dlqID := writeDLQ(t, rdb, cfg, envelope)

	res := w.ReplayDeadLetter(ctx, ReplayOptions{DLQID: dlqID, Actor: "tester"})
	if !res.Replayed {
		t.Fatalf("replay = %+v, want replayed (backfill must admit a resolvable age)", res)
	}

	channel := cfg.KeyPrefix + ":execution:events:" + res.ExecutionID
	pubsub := rdb.Subscribe(ctx, channel)
	defer pubsub.Close()
	if _, err := pubsub.Receive(ctx); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}

	// Expired, not executed: acked + XDELed, no state row, observable drop.
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0", n)
	}
	if n := pendingCount(t, rdb, cfg); n != 0 {
		t.Fatalf("PEL = %d, want 0", n)
	}
	if _, err := rdb.Get(ctx, stateKeyOf(cfg, res.ExecutionID)).Result(); err == nil {
		t.Fatal("state row exists — the 40-day-old start was EXECUTED (age gate bypassed by replay)")
	}
	if got := rec.total(MetricTaskExpiredTotal); got != 1 {
		t.Fatalf("task_expired_total = %d, want 1", got)
	}
	msg, err := pubsub.ReceiveTimeout(ctx, 2*time.Second)
	if err != nil {
		t.Fatalf("task_expired event: %v", err)
	}
	redisMsg, ok := msg.(*redis.Message)
	if !ok || !strings.Contains(redisMsg.Payload, `"type":"task_expired"`) {
		t.Fatalf("event payload = %T %v, want task_expired", msg, msg)
	}
	// The backfilled timestamp must be visible on the replayed carrier.
	enqueued := rdb.XRange(ctx, cfg.QueueName, "-", "+").Val()
	if len(enqueued) != 0 {
		var replayed map[string]any
		if err := json.Unmarshal([]byte(enqueued[0].Values["payload"].(string)), &replayed); err != nil {
			t.Fatal(err)
		}
		ts, _ := replayed["timestamp"].(string)
		if _, ok := parseProducerTimestamp(ts); !ok {
			t.Fatalf("backfilled timestamp %q is not parseable by the age gate", ts)
		}
	}
}

// TestDLQReplayRejectsUnresolvableCarrierTimestamp (#67 case B): no
// dead_lettered_at in the envelope and a DLQ stream id whose millisecond
// prefix is unparseable ("0-1") ⇒ named fail-closed rejection — never a
// now() fallback. Practically unreachable via real XADDs; the case pins the
// contract so it cannot drift into a silent wash.
func TestDLQReplayRejectsUnresolvableCarrierTimestamp(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()

	body := map[string]any{
		"type":      "start",
		"flow_id":   "hello_world_go",
		"tenant_id": "default",
	}
	envelope := dlqEnvelopeFor(body, 1)
	delete(envelope, "dead_lettered_at")
	dlqID0Minus1(t, rdb, cfg, envelope)

	res := w.ReplayDeadLetter(ctx, ReplayOptions{DLQID: "0-1", Actor: "tester"})
	if res.Replayed || res.Reason != ReplayReasonCarrierTimestampUnknown {
		t.Fatalf("replay = %+v, want carrier_timestamp_unknown", res)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0 (rejected entry must not enqueue)", n)
	}
	// No replay marker: fixing the entry's data must allow a retry.
	if raw, err := rdb.Get(ctx, w.replayMarkerKey(cfg.DLQKey, "0-1")).Result(); err == nil && raw != "" {
		t.Fatalf("rejection wrote a replay marker: %s", raw)
	}
	// Audit row: same shape as the other named rejections.
	audit := replayAuditLastRow(t, rdb, cfg)
	if audit["result"] != ReplayReasonCarrierTimestampUnknown {
		t.Fatalf("audit result = %v, want carrier_timestamp_unknown", audit["result"])
	}
	if audit["dlq_id"] != "0-1" || audit["source_id"] != envelope["source_id"] {
		t.Fatalf("audit dlq_id/source_id = %v/%v, want 0-1/%v", audit["dlq_id"], audit["source_id"], envelope["source_id"])
	}
}

// TestDLQReplayPreservesExistingCarrierTimestamp (#67 case C, negative
// control — green before AND after): a payload-carried timestamp is the
// authority. It must survive the replay byte-for-byte and still drive the age
// gate.
func TestDLQReplayPreservesExistingCarrierTimestamp(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	w.cfg.TaskMaxAgeSeconds = 3600
	rec := newRecordingMetrics()
	w.SetMetrics(rec)
	ctx := context.Background()

	original := oldTimestamp(40 * 24 * time.Hour)
	body := map[string]any{
		"type":         "start",
		"flow_id":      "hello_world_go",
		"tenant_id":    "default",
		"execution_id": newExecutionID(),
		"timestamp":    original,
	}
	envelope := dlqEnvelopeFor(body, 1)
	envelope["dead_lettered_at"] = floatEpoch() // deliberately different from the payload ts
	dlqID := writeDLQ(t, rdb, cfg, envelope)

	res := w.ReplayDeadLetter(ctx, ReplayOptions{DLQID: dlqID, Actor: "tester"})
	if !res.Replayed {
		t.Fatalf("replay = %+v, want replayed", res)
	}
	msgs := rdb.XRange(ctx, cfg.QueueName, "-", "+").Val()
	if len(msgs) != 1 {
		t.Fatalf("queue entries = %d, want 1", len(msgs))
	}
	var replayed map[string]any
	if err := json.Unmarshal([]byte(msgs[0].Values["payload"].(string)), &replayed); err != nil {
		t.Fatal(err)
	}
	if replayed["timestamp"] != original {
		t.Fatalf("timestamp = %v, want the original %q verbatim (no dead_lettered_at overwrite)", replayed["timestamp"], original)
	}

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	if got := rec.total(MetricTaskExpiredTotal); got != 1 {
		t.Fatalf("task_expired_total = %d, want 1 (the inherited age must still gate)", got)
	}
	if _, err := rdb.Get(ctx, stateKeyOf(cfg, res.ExecutionID)).Result(); err == nil {
		t.Fatal("state row exists — the 40-day-old carrier was executed")
	}
}

// TestDeadLetterCarrierTimestampSources (#67): the two-source resolution
// order — dead_lettered_at (JSON number / numeric string / ISO string), then
// the DLQ stream id's millisecond prefix — and the fail-closed corners.
func TestDeadLetterCarrierTimestampSources(t *testing.T) {
	epoch := floatEpoch() - 40*86400
	cases := []struct {
		name       string
		envelope   map[string]any
		dlqID      string
		wantOK     bool
		wantAbsAge func(t *testing.T, ts string)
	}{
		{
			name:     "json number epoch",
			envelope: map[string]any{"dead_lettered_at": epoch},
			dlqID:    "1792609839452-0",
			wantOK:   true,
			wantAbsAge: func(t *testing.T, ts string) {
				parsed, ok := parseProducerTimestamp(ts)
				if !ok {
					t.Fatalf("backfill %q unparseable", ts)
				}
				if ageDays := time.Since(parsed).Hours() / 24; ageDays < 39.9 || ageDays > 40.1 {
					t.Fatalf("age = %.1f days, want ~40", ageDays)
				}
			},
		},
		{
			name:     "numeric string epoch",
			envelope: map[string]any{"dead_lettered_at": fmt.Sprintf("%.6f", epoch)},
			dlqID:    "1792609839452-0",
			wantOK:   true,
			wantAbsAge: func(t *testing.T, ts string) {
				parsed, ok := parseProducerTimestamp(ts)
				if !ok {
					t.Fatalf("backfill %q unparseable", ts)
				}
				if ageDays := time.Since(parsed).Hours() / 24; ageDays < 39.9 || ageDays > 40.1 {
					t.Fatalf("age = %.1f days, want ~40", ageDays)
				}
			},
		},
		{
			name:     "iso string",
			envelope: map[string]any{"dead_lettered_at": oldTimestamp(10 * 24 * time.Hour)},
			dlqID:    "1792609839452-0",
			wantOK:   true,
			wantAbsAge: func(t *testing.T, ts string) {
				parsed, ok := parseProducerTimestamp(ts)
				if !ok {
					t.Fatalf("backfill %q unparseable", ts)
				}
				if age := time.Since(parsed).Hours(); age < 239.9 || age > 240.1 {
					t.Fatalf("age = %.1f days, want ~10", age/24)
				}
			},
		},
		{
			name:     "missing dead_lettered_at falls to dlq stream id",
			envelope: map[string]any{},
			dlqID:    "1792609839452-0",
			wantOK:   true,
			wantAbsAge: func(t *testing.T, ts string) {
				parsed, ok := parseProducerTimestamp(ts)
				if !ok {
					t.Fatalf("backfill %q unparseable", ts)
				}
				if want := time.UnixMilli(1792609839452); !parsed.Equal(want) {
					t.Fatalf("backfill = %v, want the stream id time %v", parsed, want)
				}
			},
		},
		{
			name:     "absurd epoch falls to dlq stream id (python overflow 口径)",
			envelope: map[string]any{"dead_lettered_at": 1e30},
			dlqID:    "1792609839452-0",
			wantOK:   true,
			wantAbsAge: func(t *testing.T, ts string) {
				parsed, _ := parseProducerTimestamp(ts)
				if want := time.UnixMilli(1792609839452); !parsed.Equal(want) {
					t.Fatalf("backfill = %v, want the stream id time %v", parsed, want)
				}
			},
		},
		{
			name:       "both sources unresolvable",
			envelope:   map[string]any{"dead_lettered_at": "not-a-time"},
			dlqID:      "0-1",
			wantOK:     false,
			wantAbsAge: func(t *testing.T, ts string) {},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ts, ok := deadLetterCarrierTimestamp(tc.envelope, tc.dlqID)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v (ts=%q), want %v", ok, ts, tc.wantOK)
			}
			if ok {
				tc.wantAbsAge(t, ts)
			}
		})
	}
}
