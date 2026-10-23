package worker

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// #59: the old carrierAgeSeconds sentinel (best := -1.0, `a > best`) merged
// "both sources unparsable" and "both sources in the future" into one
// known=false verdict, so a worker with a behind clock fail-closed dropped
// carriers whose real age was inside the clock skew — then acked+XDELed them
// (unrecoverable). Python's _start_carrier_too_old lets negative ages compete
// in max(ages) and admits them.

func futureTimestamp(d time.Duration) string {
	return time.Now().Add(d).Format("2006-01-02T15:04:05.999999")
}

func futureStreamID(d time.Duration) string {
	return fmt.Sprintf("%d-0", time.Now().Add(d).UnixMilli())
}

// 用例 A (unit): both sources now+2s (minimal behind-clock shape) → the age
// must be KNOWN and clamped to 0 ("just enqueued"), not reported unknown.
func TestCarrierAgeFutureClampedKnown(t *testing.T) {
	task := &Task{Type: "start", Timestamp: futureTimestamp(2 * time.Second)}
	age, known := carrierAgeSeconds(task, futureStreamID(2*time.Second))
	if !known {
		t.Fatalf("future-dated carrier (worker clock behind) must be age-known, got known=false age=%v", age)
	}
	if age != 0 {
		t.Fatalf("age = %v, want 0 (negative max clamped to just-enqueued)", age)
	}
}

// 用例 B (unit): both sources now+1h → same code path, known must be true
// (the value is not pinned — any non-positive max means "newer than us").
func TestCarrierAgeFarFutureKnown(t *testing.T) {
	task := &Task{Type: "start", Timestamp: futureTimestamp(time.Hour)}
	age, known := carrierAgeSeconds(task, futureStreamID(time.Hour))
	if !known {
		t.Fatalf("far-future carrier must be age-known, got known=false age=%v", age)
	}
	if age > 0 {
		t.Fatalf("age = %v, want ≤ 0 (clamped)", age)
	}
}

// 用例 A/B (gate): carrierTooOld admits future-dated carriers, and the
// fail-closed counter stays untouched.
func TestCarrierTooOldFutureAdmits(t *testing.T) {
	w, _, _, _ := newTestWorker(t)
	rec := newRecordingMetrics()
	w.SetMetrics(rec)
	w.cfg.TaskMaxAgeSeconds = 3600
	for _, d := range []time.Duration{2 * time.Second, time.Hour} {
		task := &Task{Type: "start", Timestamp: futureTimestamp(d)}
		if w.carrierTooOld(task, futureStreamID(d)) {
			t.Fatalf("carrier %v newer than the worker clock must be admitted (clamped to 0), not dropped", d)
		}
	}
	if got := rec.total(MetricTaskAgeUnknownTotal); got != 0 {
		t.Fatalf("task_age_unknown_total = %d, want 0", got)
	}
}

// 用例 A (end-to-end): a start carrier with BOTH sources at now+2s must
// execute — not vanish through carrierTooOld's ack+XDEL with no state row.
func TestAgeGateFutureCarrierExecutes(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	w.cfg.TaskMaxAgeSeconds = 3600
	rec := newRecordingMetrics()
	w.SetMetrics(rec)
	ctx := context.Background()
	presetHelloWorld(t, w)

	executionID := newExecutionID()
	payload := baseStartPayload(executionID)
	payload["timestamp"] = futureTimestamp(2 * time.Second)
	if err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: cfg.QueueName,
		ID:     futureStreamID(2 * time.Second),
		Values: map[string]any{
			"payload":        string(mustJSON(t, payload)),
			"schema_version": "1",
		},
	}).Err(); err != nil {
		t.Fatalf("xadd future id: %v", err)
	}
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}

	state := loadStateRow(t, rdb, cfg, executionID)
	if state.Status != StatusRunning && state.Status != StatusCompleted {
		t.Fatalf("status = %q, want running/completed (future-dated carrier must execute, not be acked)", state.Status)
	}
	if got := rec.total(MetricTaskExpiredTotal); got != 0 {
		t.Fatalf("task_expired_total = %d, want 0", got)
	}
	if got := rec.total(MetricTaskAgeUnknownTotal); got != 0 {
		t.Fatalf("task_age_unknown_total = %d, want 0", got)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0", n)
	}
	if n := pendingCount(t, rdb, cfg); n != 0 {
		t.Fatalf("PEL = %d, want 0", n)
	}
}

// 日志断言: the future-dated shape must no longer be logged as
// "no timestamp, non-numeric stream id" (both sources were perfectly legal —
// the old canned message sent debugging toward the producer format).
func TestAgeGateFutureCarrierNoUnknownLog(t *testing.T) {
	w, _, _, _ := newTestWorker(t)
	var buf bytes.Buffer
	w.logger = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	rec := newRecordingMetrics()
	w.SetMetrics(rec)
	w.cfg.TaskMaxAgeSeconds = 3600
	task := &Task{Type: "start", Timestamp: futureTimestamp(2 * time.Second)}
	if w.carrierTooOld(task, futureStreamID(2*time.Second)) {
		t.Fatal("future-dated carrier must not be dropped fail-closed")
	}
	if got := rec.total(MetricTaskAgeUnknownTotal); got != 0 {
		t.Fatalf("task_age_unknown_total = %d, want 0", got)
	}
	if logs := buf.String(); strings.Contains(logs, "no timestamp, non-numeric stream id") {
		t.Fatalf("future-dated carrier must not be logged as age-unknown, got: %s", logs)
	}
}
