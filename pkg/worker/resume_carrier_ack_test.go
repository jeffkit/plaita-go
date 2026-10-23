package worker

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// staleEventResumeTask is the #68 carrier shape: an event decision carrier
// bound to a subscription the suspended node never registered.
func staleEventResumeTask(flowID, executionID string) map[string]any {
	return map[string]any{
		"type":         "resume",
		"flow_id":      flowID,
		"execution_id": executionID,
		"resume_type":  "event",
		"timestamp":    nowISO(),
		"data": map[string]any{
			"subscription_id": "stale-sub",
			"event_type":      "approval",
		},
	}
}

// suspendedEventFlowExecution parks a fresh execution on the event node and
// returns once the row is suspended with the lease released.
func suspendedEventFlowExecution(t *testing.T, w *Worker, rdb *redis.Client, cfg *Config, ctx context.Context, flowID string) string {
	t.Helper()
	presetFlowJSON(t, w, flowID, "1", testEventFlow(flowID, ""))
	executionID := newExecutionID()
	enqueueTask(t, rdb, cfg, startPayloadFor(flowID, "1", executionID), "")
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("start ProcessOne: %v", err)
	}
	state := loadStateRow(t, rdb, cfg, executionID)
	if state.Status != StatusSuspended {
		t.Fatalf("precondition: status = %q, want suspended", state.Status)
	}
	return executionID
}

// TestStaleResumeCarrierAcksWithoutCloneLoop (#68 case A): a permanently
// rejected (ResumeGuardError) event carrier is acked, NOT left pending. Before
// the fix the pending carrier rode the reclaim → dead-letter → recovery-clone
// loop: every MaxDeliveries exhausts produced a fresh clone of the same
// poisoned payload and a DLQ entry, while the execution stayed suspended
// forever (queue XLEN pinned at 1, DLQ growing unboundedly).
func TestStaleResumeCarrierAcksWithoutCloneLoop(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	executionID := suspendedEventFlowExecution(t, w, rdb, cfg, ctx, "stale_ack_flow")

	enqueueTask(t, rdb, cfg, staleEventResumeTask("stale_ack_flow", executionID), "")

	// ≥ 2*MaxDeliveries rounds; enough (with ClaimMinIdleMS=20ms) to cross the
	// reclaim ceiling several times — the pre-fix loop never drains.
	for i := 0; i < 4*cfg.MaxDeliveries; i++ {
		if _, err := w.ProcessOne(ctx); err != nil {
			t.Fatalf("round %d: stale carrier must not keep erroring: %v", i, err)
		}
		if streamLen(t, rdb, cfg.QueueName) == 0 && pendingCount(t, rdb, cfg) == 0 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0 (carrier acked; a lingering entry means the clone loop survives)", n)
	}
	if n := pendingCount(t, rdb, cfg); n != 0 {
		t.Fatalf("PEL = %d, want 0", n)
	}
	if n := streamLen(t, rdb, cfg.DLQKey); n != 0 {
		t.Fatalf("DLQ XLEN = %d, want 0 (a permanent rejection must never dead-letter, let alone clone)", n)
	}
	// The execution itself keeps waiting for its real decision carrier.
	state := loadStateRow(t, rdb, cfg, executionID)
	if state.Status != StatusSuspended {
		t.Fatalf("status = %q, want suspended (acked carrier must not terminalize)", state.Status)
	}
	if state.EndTime != "" {
		t.Fatalf("end_time = %q, want empty", state.EndTime)
	}
}

// TestResumeCarrierRejectedMetricAndLog (#68 case B): the ack is observable —
// counter with the low-cardinality resume_type label, and "acked" log
// wording instead of "message left pending".
func TestResumeCarrierRejectedMetricAndLog(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := testConfig()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	var buf bytes.Buffer
	w := New(cfg, rdb, slog.New(slog.NewTextHandler(&buf, nil)))
	rec := newRecordingMetrics()
	w.SetMetrics(rec)
	ctx := context.Background()

	executionID := suspendedEventFlowExecution(t, w, rdb, cfg, ctx, "stale_metric_flow")
	enqueueTask(t, rdb, cfg, staleEventResumeTask("stale_metric_flow", executionID), "")
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("stale carrier ProcessOne: %v", err)
	}

	if got := rec.total("resume_carrier_rejected_total"); got != 1 {
		t.Fatalf("resume_carrier_rejected_total = %d, want 1", got)
	}
	if label := rec.labelOf("resume_carrier_rejected_total", "resume_type"); label != "event" {
		t.Fatalf("resume_type label = %q, want event", label)
	}
	logs := buf.String()
	if !strings.Contains(logs, "acked") {
		t.Fatalf("logs must carry the acked disposition, got:\n%s", logs)
	}
	if strings.Contains(logs, "message left pending") {
		t.Fatalf("logs must no longer claim 'message left pending' for the acked carrier:\n%s", logs)
	}
}

// TestDispositionResumeErrorStillPending (#68 case E, negative control):
// seam-level pin of the exact fix point. ResumeError (node resume handler
// failure — possibly transient) must NOT be acked: the message stays pending,
// nothing is dead-lettered. The ResumeGuardError arm of the same seam shows
// the split behavior.
func TestDispositionResumeErrorStillPending(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	q := w.queue
	if err := q.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}

	executionID := newExecutionID()
	rdb.Set(ctx, stateKeyOf(cfg, executionID),
		fmt.Sprintf(`{"execution_id":%q,"status":"suspended"}`, executionID), 0)
	enqueueTask(t, rdb, cfg, staleEventResumeTask("any_flow", executionID), "")
	delivery, err := q.Read(ctx)
	if err != nil || delivery == nil {
		t.Fatalf("queue read = (%v, %v), want a delivery", delivery, err)
	}

	// ResumeError → leave pending (transient handler failure).
	resumeErr := fmt.Errorf("resume carrier rejected: %w", &workflow.ResumeError{Message: "resume node wait: upstream down"})
	handled, herr := w.dispositionHandlerError(ctx, delivery, resumeErr)
	if !handled || herr == nil {
		t.Fatalf("ResumeError disposition = (%v, %v), want (true, err)", handled, herr)
	}
	if n := pendingCount(t, rdb, cfg); n != 1 {
		t.Fatalf("PEL = %d, want 1 (transient ResumeError stays pending)", n)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 1 {
		t.Fatalf("queue XLEN = %d, want 1", n)
	}
	if n := streamLen(t, rdb, cfg.DLQKey); n != 0 {
		t.Fatalf("DLQ XLEN = %d, want 0", n)
	}

	// ResumeGuardError → acked in place (#68).
	guardErr := fmt.Errorf("resume carrier rejected: %w", &workflow.ResumeGuardError{
		Message: fmt.Sprintf(`stale event resume for node wait: subscription binding mismatch (expected "real-sub", got "stale-sub"); execution stays suspended`),
	})
	rec := newRecordingMetrics()
	w.SetMetrics(rec)
	handled, herr = w.dispositionHandlerError(ctx, delivery, guardErr)
	if !handled || herr != nil {
		t.Fatalf("ResumeGuardError disposition = (%v, %v), want (true, nil)", handled, herr)
	}
	if n := pendingCount(t, rdb, cfg); n != 0 {
		t.Fatalf("PEL = %d, want 0 (permanent rejection is acked)", n)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0", n)
	}
	if got := rec.total("resume_carrier_rejected_total"); got != 1 {
		t.Fatalf("resume_carrier_rejected_total = %d, want 1", got)
	}
}
