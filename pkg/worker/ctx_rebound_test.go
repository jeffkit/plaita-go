package worker

// #48 ctx-rebound negative control (worker level): the execution budget only
// cancels COOPERATIVELY, so a node whose body never looks at ctx kept
// Execute from ever returning — the named execution_deadline_exceeded
// terminal (which is classified AFTER the engine call returns) never fired,
// the watchdog exited silently with runCtx and the single consumer stayed
// pinned (with #47 conversion this becomes two engines running one
// execution). This file pins the in-process js backend; the sleep node's
// control is engine-level (pkg/workflow/ctx_rebound_test.go) because the IR
// rejects "sleep" as not part of the plaita dialect — a worker-deployed flow
// cannot carry it.

import (
	"context"
	"testing"
	"time"
)

// ctxReboundSpinFlow: an admitted-language (javascript) code node whose body
// never returns. Pre-#48 the goja vm ignored ctx: the deadline fired,
// cancelRun() cancelled a context nobody observed, and the spin below held
// the consumer forever.
const ctxReboundSpinFlow = `{
  "flow_id": "ctx_rebound_js_spin",
  "inputType": {"dataType": "object"},
  "nodes": [
    {"type": "start", "id": "start", "next": "spin"},
    {"type": "code", "id": "spin", "language": "javascript",
     "code": "function run(input) { while (true) {} }",
     "next": "end"},
    {"type": "end", "id": "end", "resultType": "success", "output": "done"}
  ]
}`

// TestWorkerExecutionDeadlineTerminalizesJsSpin: same acceptance shape as the
// self-loop control — the terminal lands inside the budget window with the
// named signature, and the consumer moves on to the queued-behind task.
func TestWorkerExecutionDeadlineTerminalizesJsSpin(t *testing.T) {
	w, rdb, cfg := newDeadlineTestWorker(t)
	ctx := context.Background()
	if err := w.flows.PresetFlowDefinition(ctx, "", "ctx_rebound_js_spin", "latest", []byte(ctxReboundSpinFlow)); err != nil {
		t.Fatal(err)
	}
	if err := w.flows.PresetFlowDefinition(ctx, "", "hello_world_go", "latest", mustHelloWorld(t)); err != nil {
		t.Fatal(err)
	}

	spin := newExecutionID()
	follower := newExecutionID()
	startSpin := baseStartPayload(spin)
	startSpin["flow_id"] = "ctx_rebound_js_spin"
	enqueueTask(t, rdb, cfg, startSpin, "")
	enqueueTask(t, rdb, cfg, baseStartPayload(follower), "")

	// The watchdog bounds a pre-#48 baseline to a red verdict instead of a
	// hang (there the vm never observes the cancellation).
	began := time.Now()
	done := make(chan error, 1)
	go func() {
		_, err := w.ProcessOne(ctx)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ProcessOne (spin): %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("js spin still occupying the single consumer after 30s: ctx-blind vm (pre-#48 negative control)")
	}
	occupied := time.Since(began)

	const budget = 2 * time.Second
	if occupied < budget {
		t.Fatalf("spin ended after %s — before the %s budget; the gate did not come from the execution deadline", occupied, budget)
	}
	if occupied > budget+6*time.Second {
		t.Fatalf("spin ended after %s — overshoot past the %s budget is too large (interrupt not prompt)", occupied, budget)
	}

	state := loadStateRow(t, rdb, cfg, spin)
	if state.Status != StatusError {
		t.Fatalf("spin status = %q, want error", state.Status)
	}
	if state.Error["error_type"] != "execution_deadline_exceeded" {
		t.Fatalf("error_type = %v, want execution_deadline_exceeded (named signature)", state.Error["error_type"])
	}

	// Recovery: the follower is processed right after the terminal — the
	// single consumer is free again, no #47 double-engine window is left
	// behind (the run ended with its lease released, not wedged in Execute).
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne (follower): %v", err)
	}
	fstate := loadStateRow(t, rdb, cfg, follower)
	if fstate.Status != StatusCompleted {
		t.Fatalf("follower status = %q, want completed (consumer recovered)", fstate.Status)
	}
	if n := streamLen(t, rdb, cfg.DLQKey); n != 0 {
		t.Fatalf("DLQ XLEN = %d, want 0", n)
	}
	if n := pendingCount(t, rdb, cfg); n != 0 {
		t.Fatalf("PEL = %d, want 0", n)
	}
}
