package worker

// #52 negative controls: a start carrier with an execution_id but NO flow_id
// that lands on a running row used to self-feed — retryCarrierBody degraded a
// flow-id mismatch into a start copy, the #29 start gate only blocks when BOTH
// flow ids are non-empty, so the copy re-entered routeStartRedeliveryOnRunning
// forever: no terminal, no DLQ, half the consumer throughput burned on a hot
// loop while the log claimed "converted to a continue carrier". Both tests
// were red on the pre-fix tree (row still running, carrier still bouncing).

import (
	"context"
	"testing"
)

// TestStartCarrierWithoutFlowIDOnRunningRowTerminates: the protocol-foreign
// carrier (no flow_id) must reach the same deterministic outcome as the #47
// crash-recovery carrier — one conversion, the resume path resolves the flow
// from the stored row, the execution finishes, the queue drains.
func TestStartCarrierWithoutFlowIDOnRunningRowTerminates(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	presetHelloWorld(t, w)

	executionID := newExecutionID()
	seedRunningMidFlow(t, w, cfg, executionID)
	payload := baseStartPayload(executionID)
	delete(payload, "flow_id") // 有 execution_id、无 flow_id：queue.go 不校验
	enqueueTask(t, rdb, cfg, payload, "")

	for i := 0; i < 20; i++ {
		if _, err := w.ProcessOne(ctx); err != nil {
			t.Fatalf("ProcessOne round %d: %v", i+1, err)
		}
	}

	final := loadStateRow(t, rdb, cfg, executionID)
	if final.Status != StatusCompleted || final.EndTime == "" {
		t.Fatalf("status = %q end = %q, want completed within 20 rounds (self-feed loop?)",
			final.Status, final.EndTime)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0 (carrier must not re-enqueue)", n)
	}
	if n := streamLen(t, rdb, cfg.DLQKey); n != 0 {
		t.Fatalf("DLQ XLEN = %d, want 0 (no flood)", n)
	}
	if n := pendingCount(t, rdb, cfg); n != 0 {
		t.Fatalf("PEL = %d, want 0", n)
	}
}

// TestStartCarrierWithForeignFlowIDPoisonAckedAtGate (守恒): a start carrier
// naming ANOTHER flow never reaches the converter — the #29 gate poison-acks
// it (flow_conflict, worker.go) before the running route, so the #52 loop
// could only ever be fed by the EMPTY-flow_id variant. Nothing is enqueued,
// the row stays untouched, and the disposition happens exactly once. This
// held on the pre-fix tree too; it guards the boundary the fix must not move.
func TestStartCarrierWithForeignFlowIDPoisonAckedAtGate(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	presetHelloWorld(t, w)

	executionID := newExecutionID()
	seedRunningMidFlow(t, w, cfg, executionID)
	payload := baseStartPayload(executionID)
	payload["flow_id"] = "some_other_flow"
	enqueueTask(t, rdb, cfg, payload, "")

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne (redelivered start): %v", err)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0 (gate poison ack, nothing enqueued)", n)
	}
	if n := pendingCount(t, rdb, cfg); n != 0 {
		t.Fatalf("PEL = %d, want 0", n)
	}
	if n := streamLen(t, rdb, cfg.DLQKey); n != 0 {
		t.Fatalf("DLQ XLEN = %d, want 0", n)
	}
	row := loadStateRow(t, rdb, cfg, executionID)
	if row.Status != StatusRunning || row.EndTime != "" {
		t.Fatalf("row disturbed by the poison ack: status=%s end=%q", row.Status, row.EndTime)
	}
}
