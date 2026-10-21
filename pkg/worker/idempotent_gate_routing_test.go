package worker

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// #47 negative controls: the #29 start idempotency gate used to ack a
// redelivered start carrier whenever a state row existed — including running
// rows, which is exactly the crash-recovery carrier (worker crash/OOM, deploy
// restart, D2 force-stop, lease loss all converge to "running row + PEL
// message"). The gate now routes a running hit through the §9.1 conversion
// (retryCarrierBody) so the redelivered copy becomes a continue carrier, and
// compensates a pending suspend dispatch on the suspended branch. These tests
// were red on the pre-fix tree (silently acked, no carrier produced).

// seedRunningMidFlow seeds the converged crash state: a running row mid-flow
// (start completed, $NODE.start recorded) + a per-step checkpoint pointing at
// the next node, with the flow fingerprint matching the preset definition so
// the §7 admission gate passes on the converted carrier.
func seedRunningMidFlow(t *testing.T, w *Worker, cfg *Config, executionID string) {
	t.Helper()
	ctx := context.Background()
	def, err := w.flows.GetFlowDefinition(ctx, "", "hello_world_go", "latest")
	if err != nil {
		t.Fatalf("flow definition: %v", err)
	}
	seeded := &ExecutionState{
		ExecutionID:    executionID,
		FlowID:         "hello_world_go",
		TenantID:       "",
		Status:         StatusRunning,
		StartTime:      nowISO(),
		LastUpdateTime: nowISO(),
		FlowVersion:    def.Version,
		FlowHash:       def.Hash,
		FlowHashAlgo:   def.HashAlgo,
		Context: map[string]any{
			"$INPUT":     map[string]any{"who": "worker-test"},
			"$NODE":      map[string]any{"start": map[string]any{"status": "completed"}},
			"$LAST_NODE": "start",
			"$FLOW_ID":   "hello_world_go",
		},
	}
	if err := w.store.SaveExecutionState(ctx, "", seeded, 0); err != nil {
		t.Fatal(err)
	}
	cp := &workflow.Checkpoint{
		SchemaVersion: workflow.CheckpointSchemaVersion,
		Kind:          workflow.CheckpointKindPerStep,
		ExecutionID:   executionID,
		FlowID:        "hello_world_go",
		SuspendedNode: "start",
		NextNode:      "end",
		ResumeType:    workflow.ResumeContinue,
		Context: map[string]any{
			"$INPUT":     map[string]any{"who": "worker-test"},
			"$NODE":      map[string]any{"start": map[string]any{"status": "completed"}},
			"$LAST_NODE": "start",
			"$FLOW_ID":   "hello_world_go",
		},
		CreatedAt: nowISO(),
	}
	if err := w.checkpoints.SaveCheckpoint(ctx, cp); err != nil {
		t.Fatal(err)
	}
}

// TestRedeliveredStartOnRunningConvertsToContinue (#47, crash-redelivery
// path; supersedes the pre-#47 TestDuplicateStartRunningRowNoOverwrite whose
// "running row → idempotent ack" premise WAS the stranding bug): a start
// carrier redelivered onto a running row + per-step checkpoint must be
// converted into a continue carrier (enqueue→ack), not acked into a zombie.
// The follow-up round takes the continue carrier and finishes the execution
// from the breakpoint.
func TestRedeliveredStartOnRunningConvertsToContinue(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	presetHelloWorld(t, w)

	executionID := newExecutionID()
	seedRunningMidFlow(t, w, cfg, executionID)
	enqueueTask(t, rdb, cfg, baseStartPayload(executionID), "")

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne (redelivered start): %v", err)
	}

	// The original is acked and exactly one converted carrier went out.
	if n := pendingCount(t, rdb, cfg); n != 0 {
		t.Fatalf("PEL = %d, want 0 (original acked)", n)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 1 {
		t.Fatalf("queue XLEN = %d, want 1 (converted continue carrier)", n)
	}
	if n := streamLen(t, rdb, cfg.DLQKey); n != 0 {
		t.Fatalf("DLQ XLEN = %d, want 0", n)
	}
	// The conversion must not touch the running row or the checkpoint.
	row := loadStateRow(t, rdb, cfg, executionID)
	if row.Status != StatusRunning || row.EndTime != "" {
		t.Fatalf("row disturbed by the conversion: status=%s end=%q", row.Status, row.EndTime)
	}
	if raw := rdb.Get(ctx, cfg.KeyPrefix+":execution:checkpoint:"+executionID).Val(); raw == "" {
		t.Fatal("checkpoint must survive the conversion")
	}
	// Converted body: resume/continue with attribution + params, no version.
	msgs := rdb.XRange(ctx, cfg.QueueName, "-", "+").Val()
	if len(msgs) != 1 {
		t.Fatalf("queue entries = %d, want 1", len(msgs))
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(msgs[0].Values["payload"].(string)), &body); err != nil {
		t.Fatalf("carrier decode: %v", err)
	}
	if body["type"] != "resume" || body["resume_type"] != "continue" {
		t.Fatalf("carrier = %v, want resume/continue", body)
	}
	if body["execution_id"] != executionID || body["flow_id"] != "hello_world_go" {
		t.Fatalf("carrier attribution lost: %v", body)
	}
	if params, _ := body["params"].(map[string]any); params["who"] != "worker-test" {
		t.Fatalf("carrier params lost (#122): %v", body)
	}
	if _, hasVersion := body["version"]; hasVersion {
		t.Fatalf("converted carrier must not pin a version: %v", body)
	}

	// Second round: the continue carrier enters mid-flow and completes.
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne (converted continue): %v", err)
	}
	final := loadStateRow(t, rdb, cfg, executionID)
	if final.Status != StatusCompleted || final.EndTime == "" {
		t.Fatalf("status = %q end = %q, want completed (execution resumed from the breakpoint)", final.Status, final.EndTime)
	}
	if raw := rdb.Get(ctx, cfg.KeyPrefix+":execution:checkpoint:"+executionID).Val(); raw != "" {
		t.Fatal("checkpoint must be deleted on the terminal write")
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0", n)
	}
	if n := pendingCount(t, rdb, cfg); n != 0 {
		t.Fatalf("PEL = %d, want 0", n)
	}
	// Mid-flow entry (not fresh-run parity): no fallback marker on the record.
	events, _ := final.Timeline["events"].([]any)
	for _, raw := range events {
		ev, _ := raw.(map[string]any)
		if ev["type"] == "continue_fallback_start" {
			t.Fatalf("mid-flow checkpoint existed; fresh-run fallback must not fire: %v", ev)
		}
	}
}

// TestRedeliveredStartOnRunningWithoutCheckpointFreshParity (#47 验收 3): a
// running row with NO checkpoint (crash before the first step boundary) still
// recovers: the converted continue carrier degrades to fresh-run parity
// (§9.1) and the execution reaches its terminal.
func TestRedeliveredStartOnRunningWithoutCheckpointFreshParity(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	presetHelloWorld(t, w)

	executionID := newExecutionID()
	def, err := w.flows.GetFlowDefinition(ctx, "", "hello_world_go", "latest")
	if err != nil {
		t.Fatalf("flow definition: %v", err)
	}
	seeded := &ExecutionState{
		ExecutionID:    executionID,
		FlowID:         "hello_world_go",
		Status:         StatusRunning,
		StartTime:      nowISO(),
		LastUpdateTime: nowISO(),
		FlowVersion:    def.Version,
		FlowHash:       def.Hash,
		FlowHashAlgo:   def.HashAlgo,
		Context:        map[string]any{"$INPUT": map[string]any{"who": "worker-test"}, "$FLOW_ID": "hello_world_go"},
	}
	if err := w.store.SaveExecutionState(ctx, "", seeded, 0); err != nil {
		t.Fatal(err)
	}
	enqueueTask(t, rdb, cfg, baseStartPayload(executionID), "")

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne (redelivered start): %v", err)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 1 {
		t.Fatalf("queue XLEN = %d, want 1 (converted continue carrier)", n)
	}
	if n := pendingCount(t, rdb, cfg); n != 0 {
		t.Fatalf("PEL = %d, want 0 (original acked)", n)
	}

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne (fresh-run parity): %v", err)
	}
	final := loadStateRow(t, rdb, cfg, executionID)
	if final.Status != StatusCompleted || final.EndTime == "" {
		t.Fatalf("status = %q end = %q, want completed", final.Status, final.EndTime)
	}
	// Fresh-run parity is on the record (§9.1), and the original params rode
	// the converted carrier back into the engine.
	if v, ok := final.Context["$INPUT"].(map[string]any); !ok || v["who"] != "worker-test" {
		t.Fatalf("$INPUT lost on the parity re-entry: %v", final.Context["$INPUT"])
	}
	fallback := false
	events, _ := final.Timeline["events"].([]any)
	for _, raw := range events {
		ev, _ := raw.(map[string]any)
		if ev["type"] == "continue_fallback_start" {
			fallback = true
		}
	}
	if !fallback {
		t.Fatal("expected a continue_fallback_start event on the fresh-run parity path")
	}
}

// TestRedeliveredStartAfterLeaseLossResumes (#47, lease-loss path): the
// watchdog judged the lease lost and aborted the step — lease key gone, dead
// fence generation left behind, row still running, message back in the
// stream. The redelivered start must recover the execution across the fence
// generation (new lease, fenced terminal write under the NEW token).
func TestRedeliveredStartAfterLeaseLossResumes(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	presetHelloWorld(t, w)

	executionID := newExecutionID()
	seedRunningMidFlow(t, w, cfg, executionID)
	// Dead generation markers: the lost lease's fence trail (§3.1 — the fence
	// key outlives the lease by design).
	rdb.Set(ctx, cfg.KeyPrefix+":execution:fence:"+executionID, "1", 7*24*time.Hour)
	rdb.Set(ctx, cfg.KeyPrefix+":execution:fencegen:"+executionID, "1", 7*24*time.Hour)
	enqueueTask(t, rdb, cfg, baseStartPayload(executionID), "")

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne (redelivered start): %v", err)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 1 {
		t.Fatalf("queue XLEN = %d, want 1 (converted continue carrier)", n)
	}
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne (continue): %v", err)
	}
	final := loadStateRow(t, rdb, cfg, executionID)
	if final.Status != StatusCompleted || final.EndTime == "" {
		t.Fatalf("status = %q end = %q, want completed after lease loss", final.Status, final.EndTime)
	}
	if gen := rdb.Get(ctx, cfg.KeyPrefix+":execution:fencegen:"+executionID).Val(); gen == "" || gen == "1" {
		t.Fatalf("fencegen = %q, want a generation past the dead one", gen)
	}
	if v := rdb.Get(ctx, cfg.KeyPrefix+":execution:lease:"+executionID).Val(); v != "" {
		t.Fatalf("lease still held: %q", v)
	}
}

// TestRedeliveredStartSuspendedCompensatesPendingDispatch (#47 evidence 3,
// plaita#233 alignment): a start carrier hitting a suspended execution acks
// idempotently — but only AFTER compensating a pending suspend service
// dispatch, the same short-circuit handleResume applies. Pre-fix the ack
// stranded the owed dispatch.
func TestRedeliveredStartSuspendedCompensatesPendingDispatch(t *testing.T) {
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
		DispatchPending: map[string]any{
			"pending":      true,
			"node_id":      "confirm",
			"service_type": "hitl",
			"service_config": map[string]any{
				"type":    "hitl",
				"message": "请确认",
			},
		},
	}
	if err := w.store.SaveExecutionState(ctx, "", seeded, 0); err != nil {
		t.Fatal(err)
	}
	enqueueTask(t, rdb, cfg, baseStartPayload(executionID), "")

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	// The owed service dispatch went out on its own queue.
	if n := rdb.LLen(ctx, cfg.KeyPrefix+":hitl:queue").Val(); n != 1 {
		t.Fatalf("hitl queue LLEN = %d, want 1 (dispatch_pending compensated)", n)
	}
	raw := rdb.LIndex(ctx, cfg.KeyPrefix+":hitl:queue", 0).Val()
	var dispatched map[string]any
	if err := json.Unmarshal([]byte(raw), &dispatched); err != nil {
		t.Fatalf("dispatched body decode: %v", err)
	}
	if dispatched["execution_id"] != executionID {
		t.Fatalf("dispatched body missing execution_id: %v", dispatched)
	}
	// The credential is cleared; the row stays suspended for its real wake-up.
	row := loadStateRow(t, rdb, cfg, executionID)
	if row.Status != StatusSuspended {
		t.Fatalf("status = %q, want suspended", row.Status)
	}
	if row.DispatchPending != nil {
		t.Fatalf("dispatch_pending must be cleared, got %v", row.DispatchPending)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0 (idempotent ack after compensation)", n)
	}
	if n := pendingCount(t, rdb, cfg); n != 0 {
		t.Fatalf("PEL = %d, want 0", n)
	}
}

// TestRedeliveredStartOnDeletedExecutionNoResurrection (#47 guard, A51-01): a
// redelivered start for a DELETED execution (tombstone) acks without
// producing a carrier — the conversion's tombstone branch, not a resurrection.
func TestRedeliveredStartOnDeletedExecutionNoResurrection(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	presetHelloWorld(t, w)

	executionID := newExecutionID()
	seedRunningMidFlow(t, w, cfg, executionID)
	rdb.Set(ctx, cfg.KeyPrefix+":exec_tombstone:"+executionID, `{"reason":"user delete"}`, time.Minute)
	enqueueTask(t, rdb, cfg, baseStartPayload(executionID), "")

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0 (acked, no resurrection carrier)", n)
	}
	if n := pendingCount(t, rdb, cfg); n != 0 {
		t.Fatalf("PEL = %d, want 0", n)
	}
	if n := streamLen(t, rdb, cfg.DLQKey); n != 0 {
		t.Fatalf("DLQ XLEN = %d, want 0", n)
	}
}

// TestCancelAtEntryReleasesLease (#47 side fix, P3): a start cancelled at the
// entry checkpoint terminalizes WITHOUT going through executeAndFinalize —
// the lease must be released on that path, not pinned until TTL expiry.
func TestCancelAtEntryReleasesLease(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	presetHelloWorld(t, w)

	executionID := newExecutionID()
	rdb.Set(ctx, cfg.KeyPrefix+":execution:cancel:"+executionID, "operator", time.Minute)
	enqueueTask(t, rdb, cfg, baseStartPayload(executionID), "")

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	row := loadStateRow(t, rdb, cfg, executionID)
	if row.Status != StatusCancelled || row.EndTime == "" {
		t.Fatalf("status = %q end = %q, want cancelled at entry", row.Status, row.EndTime)
	}
	if v := rdb.Get(ctx, cfg.KeyPrefix+":execution:lease:"+executionID).Val(); v != "" {
		t.Fatalf("lease leaked by the cancel-at-entry branch: %q", v)
	}
}
