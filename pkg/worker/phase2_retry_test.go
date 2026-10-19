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

// TestNodeRetryRequeuesThenSucceeds (§9.1): a retryable node failure below
// budget re-enqueues a fresh carrier (backoff skipped in tests), acking the
// original; the retry runs and completes, clearing the counters.
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

	// First attempt: retryable failure → carrier re-enqueued, original acked.
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
	if carrier["type"] != "start" || carrier["execution_id"] != executionID {
		t.Fatalf("retry carrier wrong: %v", carrier)
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

	// Second attempt: the endpoint answers, the flow completes and every retry
	// counter is cleared.
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

// TestRetryCarrierKeepsAttribution: the retry carrier preserves flow/tenant
// and params so a replayed run is the same execution.
func TestRetryCarrierKeepsAttribution(t *testing.T) {
	w, _, _, _ := newTestWorker(t)
	task := &Task{
		Type: "start", FlowID: "f1", ExecutionID: "e1", TenantID: "acme",
		Version: "v3", Params: map[string]any{"x": 1},
	}
	body := w.retryCarrierBody(context.Background(), task)
	if body["type"] != "start" || body["flow_id"] != "f1" || body["execution_id"] != "e1" ||
		body["tenant_id"] != "acme" || body["version"] != "v3" {
		t.Fatalf("carrier attribution lost: %v", body)
	}
	params, _ := body["params"].(map[string]any)
	if params["x"] != 1 {
		t.Fatalf("carrier params lost: %v", body["params"])
	}
}
