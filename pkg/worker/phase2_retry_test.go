package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// failOnceHTTPFlow presets start → http → end where the http endpoint aborts
// the first connection (transport error 1002, retryable) and answers
// successfully afterwards.
func failOnceHTTPFlow(t *testing.T, w *Worker, flowID string) *httptest.Server {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if calls.Add(1) == 1 {
			panic(http.ErrAbortHandler) // closes the connection: client sees EOF
		}
		rw.Header().Set("Content-Type", "application/json")
		_, _ = rw.Write([]byte(`{"ok":true}`))
	}))
	flow := fmt.Sprintf(`{
	  "flow_id": %q,
	  "nodes": [
	    {"type": "start", "id": "start", "next": "call"},
	    {"type": "http", "id": "call", "url": %q, "method": "GET", "next": "end"},
	    {"type": "end", "id": "end", "resultType": "success", "output": "done"}
	  ]
	}`, flowID, srv.URL+"/flaky")
	if err := w.flows.PresetFlowDefinition(context.Background(), "", flowID, "latest", []byte(flow)); err != nil {
		srv.Close()
		t.Fatal(err)
	}
	return srv
}

// TestNodeRetryRequeuesThenSucceeds (§9.1 + #91 载体转换): a retryable node
// failure below budget snapshots a failure-boundary checkpoint, re-enqueues a
// CONVERTED resume/continue carrier (the state row exists) and acks the
// original; the redelivered carrier continues the same execution through the
// resume path and completes, clearing the counters and the checkpoint.
func TestNodeRetryRequeuesThenSucceeds(t *testing.T) {
	oldBase := nodeRetryBackoffBase
	nodeRetryBackoffBase = 0
	defer func() { nodeRetryBackoffBase = oldBase }()

	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	cfg.MaxDeliveries = 5
	srv := failOnceHTTPFlow(t, w, "retry_flow")
	defer srv.Close()

	executionID := newExecutionID()
	payload := baseStartPayload(executionID)
	payload["flow_id"] = "retry_flow"
	enqueueTask(t, rdb, cfg, payload, "")

	// First attempt: retryable failure → carrier converted + re-enqueued,
	// original acked.
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("first ProcessOne: %v", err)
	}
	if n := pendingCount(t, rdb, cfg); n != 0 {
		t.Fatalf("PEL = %d, want 0 (original acked)", n)
	}
	msgs := rdb.XRange(ctx, cfg.QueueName, "-", "+").Val()
	if len(msgs) != 1 {
		t.Fatalf("queue entries = %d, want 1 retry carrier", len(msgs))
	}
	var carrier map[string]any
	if err := json.Unmarshal([]byte(msgs[0].Values["payload"].(string)), &carrier); err != nil {
		t.Fatal(err)
	}
	if carrier["type"] != "resume" || carrier["resume_type"] != "continue" {
		t.Fatalf("retry carrier = %v, want resume/continue (§9.1 conversion)", carrier)
	}
	if carrier["execution_id"] != executionID || carrier["flow_id"] != "retry_flow" {
		t.Fatalf("retry carrier attribution lost: %v", carrier)
	}
	if _, hasVersion := carrier["version"]; hasVersion {
		t.Fatalf("converted carrier must not pin a version (resume reads the row's): %v", carrier)
	}
	if got := rdb.Get(ctx, cfg.KeyPrefix+":execution:noderetry:"+executionID+":call").Val(); got != "1" {
		t.Fatalf("per-node counter = %q, want 1", got)
	}
	if got := rdb.Get(ctx, cfg.KeyPrefix+":execution:noderefetch:"+executionID).Val(); got != "1" {
		t.Fatalf("total counter = %q, want 1", got)
	}
	if state := loadStateRow(t, rdb, cfg, executionID); state.Status != StatusRunning {
		t.Fatalf("status after retryable failure = %q, want running", state.Status)
	}
	// The failure-boundary checkpoint is the resume-position evidence.
	cp := loadCheckpointRow(t, rdb, cfg, executionID)
	if cp.ResumeType != workflow.ResumeContinue || cp.SuspendedNode == "" {
		t.Fatalf("failure checkpoint = %+v, want a continue position", cp)
	}

	// Second attempt: the converted carrier continues the same execution, the
	// endpoint answers, the flow completes and every retry counter and the
	// checkpoint are cleaned up.
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("second ProcessOne: %v", err)
	}
	state := loadStateRow(t, rdb, cfg, executionID)
	if state.Status != StatusCompleted {
		t.Fatalf("status = %q, want completed", state.Status)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0", n)
	}
	for _, key := range []string{
		cfg.KeyPrefix + ":execution:noderetry:" + executionID + ":call",
		cfg.KeyPrefix + ":execution:noderefetch:" + executionID,
		cfg.KeyPrefix + ":execution:nofail:" + executionID,
	} {
		if v := rdb.Get(ctx, key).Val(); v != "" {
			t.Fatalf("counter %s not reset: %q", key, v)
		}
	}
	if raw := rdb.Get(ctx, cfg.KeyPrefix+":execution:checkpoint:"+executionID).Val(); raw != "" {
		t.Fatal("checkpoint must be deleted on the terminal write")
	}
}

// TestNodeRetryTombstoneDropsReenqueue (§9.1 / A51-01): a retryable failure
// on a deleted execution acks the original WITHOUT re-enqueueing — a fresh
// carrier would resurrect the execution start_flow-side.
func TestNodeRetryTombstoneDropsReenqueue(t *testing.T) {
	oldBase := nodeRetryBackoffBase
	nodeRetryBackoffBase = 0
	defer func() { nodeRetryBackoffBase = oldBase }()

	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	cfg.MaxDeliveries = 5
	srv := failOnceHTTPFlow(t, w, "tombstone_retry_flow")
	defer srv.Close()

	executionID := newExecutionID()
	payload := baseStartPayload(executionID)
	payload["flow_id"] = "tombstone_retry_flow"
	enqueueTask(t, rdb, cfg, payload, "")
	// Platform-root deletion tombstone (§8.3), written before the run.
	rdb.Set(ctx, cfg.KeyPrefix+":exec_tombstone:"+executionID,
		`{"reason":"user","tenant":""}`, time.Minute)

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0 (tombstone: acked, no re-enqueue)", n)
	}
	if n := pendingCount(t, rdb, cfg); n != 0 {
		t.Fatalf("PEL = %d, want 0 (original acked)", n)
	}
}

// TestNodeRetryContinueFailsOnFlowHashMismatch (§7): the converted continue
// carrier goes through the same fingerprint hard gate as any resume — a
// definition changed between the failure and the redelivery terminalizes
// flow_hash_mismatch instead of corrupting the run.
func TestNodeRetryContinueFailsOnFlowHashMismatch(t *testing.T) {
	oldBase := nodeRetryBackoffBase
	nodeRetryBackoffBase = 0
	defer func() { nodeRetryBackoffBase = oldBase }()

	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	cfg.MaxDeliveries = 5
	srv := failOnceHTTPFlow(t, w, "hash_retry_flow")
	defer srv.Close()

	executionID := newExecutionID()
	payload := baseStartPayload(executionID)
	payload["flow_id"] = "hash_retry_flow"
	enqueueTask(t, rdb, cfg, payload, "")

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("first ProcessOne: %v", err)
	}
	// Rewrite the definition under the same flow/version and drop the cache
	// (fresh-process semantics), then process the converted carrier.
	changed := `{
	  "flow_id": "hash_retry_flow",
	  "nodes": [
	    {"type": "start", "id": "start", "next": "call"},
	    {"type": "http", "id": "call", "url": "http://changed.invalid/x", "method": "GET", "next": "end"},
	    {"type": "end", "id": "end", "resultType": "success", "output": "done"}
	  ]
	}`
	if err := w.flows.PresetFlowDefinition(ctx, "", "hash_retry_flow", "latest", []byte(changed)); err != nil {
		t.Fatal(err)
	}
	w.flows.cache = map[string]FlowDefinition{}

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("second ProcessOne: %v", err)
	}
	state := loadStateRow(t, rdb, cfg, executionID)
	if state.Status != StatusError || state.Error["error_type"] != "flow_hash_mismatch" {
		t.Fatalf("state = %+v, want error/flow_hash_mismatch", state)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0", n)
	}
}

// TestNodeRetryBudgetExhausted: the per-node budget is max_deliveries; at the
// ceiling the execution terminalizes with the fixed §5.4 error_type.
func TestNodeRetryBudgetExhausted(t *testing.T) {
	oldBase := nodeRetryBackoffBase
	nodeRetryBackoffBase = 0
	defer func() { nodeRetryBackoffBase = oldBase }()

	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	cfg.MaxDeliveries = 2
	flow := `{
	  "flow_id": "boom_flow",
	  "nodes": [
	    {"type": "start", "id": "start", "next": "boom"},
	    {"type": "code", "id": "boom", "language": "javascript", "code": "throw new Error('kaputt');", "next": "end"},
	    {"type": "end", "id": "end", "resultType": "success", "output": "never"}
	  ]
	}`
	if err := w.flows.PresetFlowDefinition(ctx, "", "boom_flow", "latest", []byte(flow)); err != nil {
		t.Fatal(err)
	}
	executionID := newExecutionID()
	payload := baseStartPayload(executionID)
	payload["flow_id"] = "boom_flow"
	enqueueTask(t, rdb, cfg, payload, "")

	// Attempt 1 → re-enqueued.
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("first ProcessOne: %v", err)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 1 {
		t.Fatalf("queue XLEN after first failure = %d, want 1", n)
	}
	// Attempt 2 → 2 >= budget → terminal.
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("second ProcessOne: %v", err)
	}
	state := loadStateRow(t, rdb, cfg, executionID)
	if state.Status != StatusError {
		t.Fatalf("status = %q, want error", state.Status)
	}
	if state.Error["error_type"] != "node_retry_budget_exhausted" {
		t.Fatalf("error_type = %v, want node_retry_budget_exhausted", state.Error["error_type"])
	}
	if state.Error["failed_node"] != "boom" {
		t.Fatalf("failed_node = %v, want boom", state.Error["failed_node"])
	}
	if state.Error["node_retries"] != float64(2) {
		t.Fatalf("node_retries = %v, want 2", state.Error["node_retries"])
	}
	if got := rdb.Get(ctx, cfg.KeyPrefix+":execution:noderetry:"+executionID+":boom").Val(); got != "2" {
		t.Fatalf("per-node counter = %q, want 2", got)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0 (terminal, not re-enqueued)", n)
	}
}

// TestNodeRetryDeterministicFailureNotRetried (§9.1): a 1001-class error (bad
// request generation) is deterministic — terminal at once, no re-enqueue, and
// it is counted in the nofail counter.
func TestNodeRetryDeterministicFailureNotRetried(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	cfg.MaxDeliveries = 5
	flow := `{
	  "flow_id": "bad_url_flow",
	  "nodes": [
	    {"type": "start", "id": "start", "next": "call"},
	    {"type": "http", "id": "call", "url": "://not-a-url", "method": "GET", "next": "end"},
	    {"type": "end", "id": "end", "resultType": "success", "output": "never"}
	  ]
	}`
	if err := w.flows.PresetFlowDefinition(ctx, "", "bad_url_flow", "latest", []byte(flow)); err != nil {
		t.Fatal(err)
	}
	executionID := newExecutionID()
	payload := baseStartPayload(executionID)
	payload["flow_id"] = "bad_url_flow"
	enqueueTask(t, rdb, cfg, payload, "")

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	state := loadStateRow(t, rdb, cfg, executionID)
	if state.Status != StatusError {
		t.Fatalf("status = %q, want error", state.Status)
	}
	if state.Error["failed_node"] != "call" {
		t.Fatalf("failed_node = %v, want call", state.Error["failed_node"])
	}
	if state.Error["error_type"] == "node_retry_budget_exhausted" {
		t.Fatalf("deterministic failure must not go through the retry budget: %v", state.Error)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0 (no retry carrier)", n)
	}
	if got := rdb.Get(ctx, cfg.KeyPrefix+":execution:nofail:"+executionID).Val(); got != "1" {
		t.Fatalf("deterministic failure counter = %q, want 1", got)
	}
	if got := rdb.Get(ctx, cfg.KeyPrefix+":execution:noderetry:"+executionID+":call").Val(); got != "" {
		t.Fatalf("retry budget must not be consumed by a deterministic failure, got %q", got)
	}
}

// TestNodeRetryBackoffSchedule: min(30s, base * 2^(attempt-1)).
func TestNodeRetryBackoffSchedule(t *testing.T) {
	oldBase := nodeRetryBackoffBase
	nodeRetryBackoffBase = time.Second
	defer func() { nodeRetryBackoffBase = oldBase }()

	cases := map[int]time.Duration{0: 0, 1: time.Second, 2: 2 * time.Second, 5: 16 * time.Second, 6: 30 * time.Second, 10: 30 * time.Second}
	for attempt, want := range cases {
		if got := nodeRetryBackoff(attempt); got != want {
			t.Fatalf("nodeRetryBackoff(%d) = %v, want %v", attempt, got, want)
		}
	}
}

// TestRetryCarrierKeepsAttribution: with no state row on disk the retry
// carrier stays a start copy (fresh-start semantics, §9.1 conversion branch)
// and preserves flow/tenant/params/version so a replayed run is the same
// execution.
func TestRetryCarrierKeepsAttribution(t *testing.T) {
	w, _, _, _ := newTestWorker(t)
	task := &Task{
		Type: "start", FlowID: "f1", ExecutionID: "e1", TenantID: "acme",
		Version: "v3", Params: map[string]any{"x": 1},
	}
	body, err := w.retryCarrierBody(context.Background(), task)
	if err != nil {
		t.Fatalf("retryCarrierBody: %v", err)
	}
	if body["type"] != "start" || body["flow_id"] != "f1" || body["execution_id"] != "e1" ||
		body["tenant_id"] != "acme" || body["version"] != "v3" {
		t.Fatalf("carrier attribution lost: %v", body)
	}
	params, _ := body["params"].(map[string]any)
	if params["x"] != 1 {
		t.Fatalf("carrier params lost: %v", body["params"])
	}
}

// TestRetryCarrierBodyConversion (§9.1, flow_worker.py:313-389): the
// requeue-time branch table of the start→resume conversion.
func TestRetryCarrierBodyConversion(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()

	t.Run("state row exists → resume/continue, no version", func(t *testing.T) {
		row, _ := json.Marshal(map[string]any{
			"execution_id": "e2", "status": "running", "flow_id": "f1",
		})
		rdb.Set(ctx, cfg.Namespace("acme")+":execution:e2", string(row), 0)
		task := &Task{
			Type: "start", FlowID: "f1", ExecutionID: "e2", TenantID: "acme",
			Version: "v3", Params: map[string]any{"x": 1},
		}
		body, err := w.retryCarrierBody(ctx, task)
		if err != nil {
			t.Fatalf("retryCarrierBody: %v", err)
		}
		if body["type"] != "resume" || body["resume_type"] != "continue" {
			t.Fatalf("carrier = %v, want resume/continue", body)
		}
		if body["flow_id"] != "f1" || body["execution_id"] != "e2" || body["tenant_id"] != "acme" {
			t.Fatalf("attribution lost: %v", body)
		}
		if params, _ := body["params"].(map[string]any); params["x"] != 1 {
			t.Fatalf("params not passed through (#122): %v", body)
		}
		if _, hasVersion := body["version"]; hasVersion {
			t.Fatalf("converted carrier must not pin a version: %v", body)
		}
	})

	t.Run("flow_id mismatch → continue, downstream gate poison-acks (#52)", func(t *testing.T) {
		// #52: the old "start copy on mismatch" fallback fed a self-feed loop
		// whenever the carrier's flow_id was EMPTY (the #29 gate needs both
		// ids to block). The conversion now always produces a continue
		// carrier; a mismatched flow id is poison-acked downstream by
		// loadContinuationDefinition, an empty one resolves from the row.
		row, _ := json.Marshal(map[string]any{
			"execution_id": "e3", "status": "running", "flow_id": "other",
		})
		rdb.Set(ctx, stateKeyOf(cfg, "e3"), string(row), 0)
		task := &Task{Type: "start", FlowID: "f1", ExecutionID: "e3"}
		body, err := w.retryCarrierBody(ctx, task)
		if err != nil {
			t.Fatalf("retryCarrierBody: %v", err)
		}
		if body["type"] != "resume" || body["resume_type"] != "continue" {
			t.Fatalf("carrier = %v, want resume/continue (mismatch handled downstream)", body)
		}
		if body["flow_id"] != "f1" {
			t.Fatalf("carrier flow_id = %v, want the carrier's own f1 (gate input)", body["flow_id"])
		}
	})

	t.Run("empty carrier flow_id on existing row → continue (#52)", func(t *testing.T) {
		row, _ := json.Marshal(map[string]any{
			"execution_id": "e6", "status": "running", "flow_id": "f1",
		})
		rdb.Set(ctx, stateKeyOf(cfg, "e6"), string(row), 0)
		task := &Task{Type: "start", FlowID: "", ExecutionID: "e6"}
		body, err := w.retryCarrierBody(ctx, task)
		if err != nil {
			t.Fatalf("retryCarrierBody: %v", err)
		}
		if body["type"] != "resume" || body["resume_type"] != "continue" {
			t.Fatalf("carrier = %v, want resume/continue (never a start copy)", body)
		}
	})

	t.Run("state read failure → error (fail closed, #29)", func(t *testing.T) {
		// Corrupt row: LoadExecutionState errors instead of returning nil.
		// The old "degrade to a start copy" fallback would be idempotently
		// acked by the #29 start gate against the existing row, stranding the
		// retry — so the conversion now fails closed and the original stays
		// pending for reclaim.
		rdb.Set(ctx, stateKeyOf(cfg, "e4"), "{not-json", 0)
		task := &Task{Type: "start", FlowID: "f1", ExecutionID: "e4"}
		if _, err := w.retryCarrierBody(ctx, task); err == nil {
			t.Fatal("expected the state read failure to surface (fail closed)")
		}
	})

	t.Run("non-start task passes through unchanged", func(t *testing.T) {
		task := &Task{
			Type: "resume", ResumeType: "event", FlowID: "f1", ExecutionID: "e5",
			Data: map[string]any{"subscription_id": "s1"},
		}
		body, err := w.retryCarrierBody(ctx, task)
		if err != nil {
			t.Fatalf("retryCarrierBody: %v", err)
		}
		if body["type"] != "resume" || body["resume_type"] != "event" {
			t.Fatalf("carrier = %v, want the original resume shape", body)
		}
	})

	t.Run("tombstone → nil (ack, no re-enqueue)", func(t *testing.T) {
		rdb.Set(ctx, cfg.KeyPrefix+":exec_tombstone:"+"e6", `{"reason":"user"}`, time.Minute)
		task := &Task{Type: "start", FlowID: "f1", ExecutionID: "e6"}
		body, err := w.retryCarrierBody(ctx, task)
		if err != nil {
			t.Fatalf("retryCarrierBody: %v", err)
		}
		if body != nil {
			t.Fatalf("carrier = %v, want nil for a tombstoned execution", body)
		}
	})

	t.Run("tombstone probe failure → error (leave pending)", func(t *testing.T) {
		rdb.Close()
		task := &Task{Type: "start", FlowID: "f1", ExecutionID: "e7"}
		if _, err := w.retryCarrierBody(ctx, task); err == nil {
			t.Fatal("expected the probe failure to surface (fail closed)")
		}
	})
}
