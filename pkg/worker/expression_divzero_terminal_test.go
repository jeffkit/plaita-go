package worker

import (
	"context"
	"strings"
	"testing"
)

// exprDivZeroFlow is the #56 repro flow: an end node whose output expression
// triggers an integer divide by zero (goval re-panics runtime.Error; ir.Load
// accepts the flow, so it is a legal deployment).
const exprDivZeroFlow = `{
  "flow_id": "expr_divzero_flow",
  "inputType": { "dataType": "object" },
  "nodes": [
    { "type": "start", "id": "start", "next": "end" },
    { "type": "end", "id": "end", "resultType": "success",
      "output": {"x": "$F.add(1 / 0, 1)"} }
  ]
}`

// TestExpressionDivZeroLandsNamedErrorTerminal (#56): a flow whose expression
// divides by zero must land a named error terminal — the recovered runtime
// error surfaces in the terminal message, the classification names the
// RuntimeError type, the worker survives and consumes the next task, and the
// DLQ stays clean (no crash loop, no poisoned queue).
func TestExpressionDivZeroLandsNamedErrorTerminal(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	presetFlowJSON(t, w, "expr_divzero_flow", "1", exprDivZeroFlow)

	executionID := newExecutionID()
	enqueueTask(t, rdb, cfg, startPayloadFor("expr_divzero_flow", "1", executionID), "")
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}

	state := loadStateRow(t, rdb, cfg, executionID)
	if state.Status != StatusError {
		t.Fatalf("status = %s error = %v, want a named error terminal (#56: the panic must "+
			"never escape the engine and kill the worker)", state.Status, state.Error)
	}
	if msg, _ := state.Error["message"].(string); !strings.Contains(msg, "integer divide by zero") {
		t.Fatalf("terminal message %q, want the original runtime text preserved", msg)
	}
	if state.Error["error_type"] != "RuntimeError" {
		t.Fatalf("error_type = %v, want RuntimeError (deterministic bridge classification)",
			state.Error["error_type"])
	}
	if n := pendingCount(t, rdb, cfg); n != 0 {
		t.Fatalf("PEL = %d, want 0 (carrier acked)", n)
	}
	if n := streamLen(t, rdb, cfg.DLQKey); n != 0 {
		t.Fatalf("DLQ entries = %d, want 0 (deterministic errors never dead-letter)", n)
	}

	// The worker process survived: the next task runs to completion.
	presetHelloWorld(t, w)
	nextID := newExecutionID()
	enqueueTask(t, rdb, cfg, startPayloadFor("hello_world_go", "latest", nextID), "")
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("next-task ProcessOne: %v", err)
	}
	if next := loadStateRow(t, rdb, cfg, nextID); next.Status != StatusCompleted {
		t.Fatalf("next task status = %s, want completed (worker alive after the error terminal)",
			next.Status)
	}
}
