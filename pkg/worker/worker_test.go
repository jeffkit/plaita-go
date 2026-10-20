package worker

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// presetHelloWorld seeds the hello-world IR into the flow storage shape.
func presetHelloWorld(t *testing.T, w *Worker) {
	t.Helper()
	raw, err := osReadHelloWorld()
	if err != nil {
		t.Fatal(err)
	}
	if err := w.flows.PresetFlowDefinition(context.Background(), "", "hello_world_go", "latest", raw); err != nil {
		t.Fatal(err)
	}
}

// TestWorkerEndToEndHelloWorld is the MVP acceptance run: enqueue a
// hello-world flow task → one worker round → running→completed state row,
// events persisted + published, ack + XDEL, lease released.
func TestWorkerEndToEndHelloWorld(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	presetHelloWorld(t, w)

	executionID := newExecutionID()
	channel := cfg.KeyPrefix + ":execution:events:" + executionID
	pubsub := rdb.Subscribe(ctx, channel)
	defer pubsub.Close()
	if _, err := pubsub.Receive(ctx); err != nil { // wait for subscription
		t.Fatalf("subscribe: %v", err)
	}

	enqueueTask(t, rdb, cfg, baseStartPayload(executionID), "")

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}

	// State row: terminal success with the §5.1 field set.
	state := loadStateRow(t, rdb, cfg, executionID)
	if state.Status != StatusCompleted {
		t.Fatalf("status = %q, want completed", state.Status)
	}
	if state.Invoker != "worker" {
		t.Fatalf("invoker = %q, want worker", state.Invoker)
	}
	if state.FlowHash == "" || state.FlowHashAlgo != FlowHashAlgoV2 {
		t.Fatalf("flow fingerprint missing: hash=%q algo=%q", state.FlowHash, state.FlowHashAlgo)
	}
	if state.StartTime == "" || state.EndTime == "" || state.LastUpdateTime == "" {
		t.Fatal("start/end/last_update times must be set on a terminal row")
	}
	if state.EngineVersion == "" {
		t.Fatal("engine_version missing")
	}
	if v, ok := state.Context["$INPUT"].(map[string]any); !ok || v["who"] != "worker-test" {
		t.Fatalf("checkpoint $INPUT wrong: %v", state.Context["$INPUT"])
	}
	if state.Context["$FLOW_ID"] != "hello_world_go" {
		t.Fatalf("checkpoint $FLOW_ID = %v", state.Context["$FLOW_ID"])
	}

	// node_timings: start + end nodes, both ended (no empty ended_at).
	for _, node := range []string{"start", "end"} {
		timing, ok := state.NodeTimings[node].(map[string]any)
		if !ok {
			t.Fatalf("node_timings[%s] missing: %v", node, state.NodeTimings)
		}
		if timing["started_at"] == "" || timing["ended_at"] == "" {
			t.Fatalf("node_timings[%s] not closed: %v", node, timing)
		}
	}

	// timeline: run_start → node_start/node_end ×2 → run_end(final).
	timeline, ok := state.Timeline["events"].([]any)
	if !ok || len(timeline) < 6 {
		t.Fatalf("timeline events missing: %v", state.Timeline)
	}
	wantTypes := []string{"run_start", "node_start", "node_end", "node_start", "node_end", "run_end"}
	for i, want := range wantTypes {
		ev, _ := timeline[i].(map[string]any)
		if ev["type"] != want {
			t.Fatalf("timeline[%d] = %v, want %s", i, ev["type"], want)
		}
	}
	last := timeline[len(timeline)-1].(map[string]any)
	if last["final"] != true {
		t.Fatalf("run_end final = %v, want true", last["final"])
	}

	// Queue hygiene: acked + XDELed, nothing pending, nothing dead-lettered.
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0 (acked + XDEL)", n)
	}
	if n := pendingCount(t, rdb, cfg); n != 0 {
		t.Fatalf("PEL = %d, want 0", n)
	}
	if n := streamLen(t, rdb, cfg.DLQKey); n != 0 {
		t.Fatalf("DLQ XLEN = %d, want 0", n)
	}

	// Lease released; fence/fencegen keep the generation trail (§3.1).
	if v := rdb.Get(ctx, cfg.KeyPrefix+":execution:lease:"+executionID).Val(); v != "" {
		t.Fatalf("lease still held: %q", v)
	}
	if v := rdb.Get(ctx, cfg.KeyPrefix+":execution:fence:"+executionID).Val(); v != "1" {
		t.Fatalf("fence gen = %q, want 1", v)
	}
	if v := rdb.Get(ctx, cfg.KeyPrefix+":execution:fencegen:"+executionID).Val(); v != "1" {
		t.Fatalf("fencegen = %q, want 1", v)
	}

	// Index ZSET got the execution (§5.2 write shape).
	if n := rdb.ZCard(ctx, cfg.KeyPrefix+":execution:index").Val(); n != 1 {
		t.Fatalf("index ZCARD = %d, want 1", n)
	}

	// Live event channel received at least one execution event (§6.2).
	msg, err := pubsub.ReceiveTimeout(ctx, 2*time.Second)
	if err != nil {
		t.Fatalf("expected execution event on %s: %v", channel, err)
	}
	redisMsg, ok := msg.(*redis.Message)
	if !ok {
		t.Fatalf("unexpected pubsub message type %T", msg)
	}
	if !strings.Contains(redisMsg.Payload, `"execution_id"`) {
		t.Fatalf("execution event payload missing execution_id: %s", redisMsg.Payload)
	}
}

// TestWorkerLeaseConflict another worker holds the lease → no execution, §3.5
// disposition (ack on live holder, pending otherwise).
func TestWorkerLeaseConflict(t *testing.T) {
	t.Run("live holder acks", func(t *testing.T) {
		w, rdb, _, cfg := newTestWorker(t)
		ctx := context.Background()
		presetHelloWorld(t, w)
		executionID := newExecutionID()
		enqueueTask(t, rdb, cfg, baseStartPayload(executionID), "")

		// Another (alive, running) worker holds the lease.
		leaseValue := "start:other-worker:0123456789abcdef:7"
		rdb.Set(ctx, cfg.KeyPrefix+":execution:lease:"+executionID, leaseValue, time.Minute)
		rdb.Set(ctx, cfg.KeyPrefix+":registry:flow_worker:other-worker",
			`{"instance_id":"other-worker","status":"running"}`, time.Minute)

		got, err := w.ProcessOne(ctx)
		if err != nil {
			t.Fatalf("ProcessOne: %v", err)
		}
		if !got {
			t.Fatal("expected the message to be handled")
		}
		// Acked (live holder, §3.5 #4) and never executed.
		if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
			t.Fatalf("queue XLEN = %d, want 0 (acked)", n)
		}
		if _, err := rdb.Get(ctx, stateKeyOf(cfg, executionID)).Result(); err == nil {
			t.Fatal("state row must not exist (no execution happened)")
		}
	})

	t.Run("dead holder stays pending", func(t *testing.T) {
		w, rdb, _, cfg := newTestWorker(t)
		ctx := context.Background()
		presetHelloWorld(t, w)
		executionID := newExecutionID()
		enqueueTask(t, rdb, cfg, baseStartPayload(executionID), "")

		// Lease held but the holder is NOT in the registry (dead, TTL tail).
		rdb.Set(ctx, cfg.KeyPrefix+":execution:lease:"+executionID,
			"start:ghost-worker:0123456789abcdef:7", time.Minute)

		_, err := w.ProcessOne(ctx)
		if err == nil {
			t.Fatal("expected a leave-pending error")
		}
		if n := streamLen(t, rdb, cfg.QueueName); n != 1 {
			t.Fatalf("queue XLEN = %d, want 1 (kept in PEL)", n)
		}
		if n := pendingCount(t, rdb, cfg); n != 1 {
			t.Fatalf("PEL = %d, want 1", n)
		}
	})

	t.Run("decision resume carrier never acks", func(t *testing.T) {
		w, rdb, _, cfg := newTestWorker(t)
		ctx := context.Background()
		executionID := newExecutionID()
		payload := baseStartPayload(executionID)
		payload["type"] = "resume"
		payload["resume_type"] = "event"
		enqueueTask(t, rdb, cfg, payload, "")

		rdb.Set(ctx, cfg.KeyPrefix+":execution:lease:"+executionID,
			"start:other-worker:0123456789abcdef:7", time.Minute)
		rdb.Set(ctx, cfg.KeyPrefix+":registry:flow_worker:other-worker",
			`{"instance_id":"other-worker","status":"running"}`, time.Minute)

		if _, err := w.ProcessOne(ctx); err == nil {
			t.Fatal("decision carriers must not be acked even with a live holder")
		}
		if n := pendingCount(t, rdb, cfg); n != 1 {
			t.Fatalf("PEL = %d, want 1", n)
		}
	})
}

// TestWorkerCancelMidExecution: flag lands while a node runs → status
// cancelled written back, never error (§4).
func TestWorkerCancelMidExecution(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()

	release := make(chan struct{})
	arrived := make(chan struct{}, 1)
	var once sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		arrived <- struct{}{}
		select {
		case <-req.Context().Done():
		case <-release:
		}
	}))
	defer func() {
		once.Do(func() { close(release) })
		srv.Close()
	}()

	flow := fmt.Sprintf(`{
	  "flow_id": "cancel_flow",
	  "nodes": [
	    {"type": "start", "id": "start", "next": "call"},
	    {"type": "http", "id": "call", "url": %q, "method": "GET", "next": "end"},
	    {"type": "end", "id": "end", "resultType": "success", "output": "done"}
	  ]
	}`, srv.URL+"/block")
	if err := w.flows.PresetFlowDefinition(ctx, "", "cancel_flow", "latest", []byte(flow)); err != nil {
		t.Fatal(err)
	}

	executionID := newExecutionID()
	payload := baseStartPayload(executionID)
	payload["flow_id"] = "cancel_flow"
	enqueueTask(t, rdb, cfg, payload, "")

	done := make(chan error, 1)
	go func() {
		_, err := w.ProcessOne(ctx)
		done <- err
	}()

	// Wait until the http node is executing (its request hit the test server).
	select {
	case <-arrived:
	case <-time.After(10 * time.Second):
		t.Fatal("http node never started")
	}
	// The running row + open node timing must be visible by now (the running
	// row is written before execution, §3.4).
	if !waitFor(t, 5*time.Second, func() bool {
		raw := rdb.Get(ctx, stateKeyOf(cfg, executionID)).Val()
		return strings.Contains(raw, StatusRunning)
	}) {
		t.Fatal("running state row missing")
	}

	// Console-side cancel intent: only the flag key (§4.1).
	rdb.Set(ctx, cfg.KeyPrefix+":execution:cancel:"+executionID, nowISO(), 7*86400*time.Second)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ProcessOne: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("processing did not finish after cancel")
	}
	once.Do(func() { close(release) })

	state := loadStateRow(t, rdb, cfg, executionID)
	if state.Status != StatusCancelled {
		t.Fatalf("status = %q, want cancelled", state.Status)
	}
	if state.Error != nil {
		t.Fatalf("cancelled execution must carry no error, got %v", state.Error)
	}
	if state.EndTime == "" {
		t.Fatal("end_time missing on cancelled row")
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0 (acked after cancel)", n)
	}
	if v := rdb.Get(ctx, cfg.KeyPrefix+":execution:lease:"+executionID).Val(); v != "" {
		t.Fatalf("lease still held after cancel: %q", v)
	}
}

// TestWorkerResumeMVP: the phase-2 A47-F2 carrier semantics. The engine has no
// checkpoint resume yet, so the wake-up path still fails deterministically —
// but the ack/short-circuit dispositions are aligned with the protocol.
func TestWorkerResumeMVP(t *testing.T) {
	t.Run("suspended + continue → idempotent short-circuit, no lease", func(t *testing.T) {
		w, rdb, _, cfg := newTestWorker(t)
		ctx := context.Background()
		executionID := newExecutionID()
		// Existing suspended execution row with no pending dispatch credential.
		rdb.Set(ctx, stateKeyOf(cfg, executionID),
			fmt.Sprintf(`{"execution_id":%q,"status":"suspended"}`, executionID), 0)

		payload := baseStartPayload(executionID)
		payload["type"] = "resume"
		payload["resume_type"] = "continue"
		enqueueTask(t, rdb, cfg, payload, "")

		if _, err := w.ProcessOne(ctx); err != nil {
			t.Fatalf("ProcessOne: %v", err)
		}
		state := loadStateRow(t, rdb, cfg, executionID)
		if state.Status != StatusSuspended {
			t.Fatalf("status = %q, want suspended (no progress, no terminal)", state.Status)
		}
		if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
			t.Fatalf("queue XLEN = %d, want 0 (idempotent ack)", n)
		}
		// #33 contract: the short-circuit does not take the execution lease.
		if v := rdb.Get(ctx, cfg.KeyPrefix+":execution:lease:"+executionID).Val(); v != "" {
			t.Fatalf("suspended+continue must not take a lease, got %q", v)
		}
	})

	t.Run("running + continue advances: missing flow → flow_not_found terminal", func(t *testing.T) {
		// §9.1/#91: a continue carrier on a live execution continues it. With
		// the definition gone the advance fails deterministically (the same
		// terminal the start path writes), not resume_not_supported.
		w, rdb, _, cfg := newTestWorker(t)
		ctx := context.Background()
		executionID := newExecutionID()
		rdb.Set(ctx, stateKeyOf(cfg, executionID),
			fmt.Sprintf(`{"execution_id":%q,"status":"running","flow_id":"never_synced"}`, executionID), 0)

		payload := baseStartPayload(executionID)
		payload["type"] = "resume"
		payload["resume_type"] = "continue"
		payload["flow_id"] = "never_synced"
		enqueueTask(t, rdb, cfg, payload, "")

		if _, err := w.ProcessOne(ctx); err != nil {
			t.Fatalf("ProcessOne: %v", err)
		}
		state := loadStateRow(t, rdb, cfg, executionID)
		if state.Status != StatusError {
			t.Fatalf("status = %q, want error (deterministic failure)", state.Status)
		}
		if state.Error["error_type"] != "flow_not_found" {
			t.Fatalf("error_type = %v, want flow_not_found", state.Error["error_type"])
		}
		if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
			t.Fatalf("queue XLEN = %d, want 0 (poison ack)", n)
		}
	})

	t.Run("running + continue with a loadable flow continues and completes", func(t *testing.T) {
		w, rdb, _, cfg := newTestWorker(t)
		ctx := context.Background()
		presetHelloWorld(t, w)
		executionID := newExecutionID()
		rdb.Set(ctx, stateKeyOf(cfg, executionID),
			fmt.Sprintf(`{"execution_id":%q,"status":"running","flow_id":"hello_world_go"}`, executionID), 0)

		payload := baseStartPayload(executionID)
		payload["type"] = "resume"
		payload["resume_type"] = "continue"
		enqueueTask(t, rdb, cfg, payload, "")

		if _, err := w.ProcessOne(ctx); err != nil {
			t.Fatalf("ProcessOne: %v", err)
		}
		state := loadStateRow(t, rdb, cfg, executionID)
		if state.Status != StatusCompleted {
			t.Fatalf("status = %q, want completed (the carrier must truly advance)", state.Status)
		}
		if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
			t.Fatalf("queue XLEN = %d, want 0 (acked after the advance)", n)
		}
		if v := rdb.Get(ctx, cfg.KeyPrefix+":execution:lease:"+executionID).Val(); v != "" {
			t.Fatalf("lease must be released after the continuation, got %q", v)
		}
	})

	t.Run("running + retry shares the continue path (must really dispatch)", func(t *testing.T) {
		// Python: "running 执行没有失败节点可重跑……retry 在此的语义 = 从
		// checkpoint 派发后继节点（与 continue 同路），必须真实重派".
		w, rdb, _, cfg := newTestWorker(t)
		ctx := context.Background()
		presetHelloWorld(t, w)
		executionID := newExecutionID()
		rdb.Set(ctx, stateKeyOf(cfg, executionID),
			fmt.Sprintf(`{"execution_id":%q,"status":"running","flow_id":"hello_world_go"}`, executionID), 0)

		payload := baseStartPayload(executionID)
		payload["type"] = "resume"
		payload["resume_type"] = "retry"
		enqueueTask(t, rdb, cfg, payload, "")

		if _, err := w.ProcessOne(ctx); err != nil {
			t.Fatalf("ProcessOne: %v", err)
		}
		if state := loadStateRow(t, rdb, cfg, executionID); state.Status != StatusCompleted {
			t.Fatalf("status = %q, want completed", state.Status)
		}
	})

	t.Run("terminal + continue → idempotent ack, row untouched", func(t *testing.T) {
		w, rdb, _, cfg := newTestWorker(t)
		ctx := context.Background()
		executionID := newExecutionID()
		rdb.Set(ctx, stateKeyOf(cfg, executionID),
			fmt.Sprintf(`{"execution_id":%q,"status":"completed","end_time":"2026-01-01T00:00:00"}`, executionID), 0)

		payload := baseStartPayload(executionID)
		payload["type"] = "resume"
		payload["resume_type"] = "continue"
		enqueueTask(t, rdb, cfg, payload, "")

		if _, err := w.ProcessOne(ctx); err != nil {
			t.Fatalf("ProcessOne: %v", err)
		}
		state := loadStateRow(t, rdb, cfg, executionID)
		if state.Status != StatusCompleted || state.Error != nil {
			t.Fatalf("terminal row overwritten: %+v", state)
		}
		if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
			t.Fatalf("queue XLEN = %d, want 0 (already_terminal ack)", n)
		}
	})

	t.Run("error + retry → already_terminal idempotent ack", func(t *testing.T) {
		// A terminalized error execution has no checkpoint (the error terminal
		// deletes it); Python refuses a checkpoint-less retry too (plaita#240)
		// and the terminal short-circuit acks the carrier without touching the
		// row (§5.3 already_terminal).
		w, rdb, _, cfg := newTestWorker(t)
		ctx := context.Background()
		executionID := newExecutionID()
		rdb.Set(ctx, stateKeyOf(cfg, executionID),
			fmt.Sprintf(`{"execution_id":%q,"status":"error","end_time":"2026-01-01T00:00:00"}`, executionID), 0)

		payload := baseStartPayload(executionID)
		payload["type"] = "resume"
		payload["resume_type"] = "retry"
		enqueueTask(t, rdb, cfg, payload, "")

		if _, err := w.ProcessOne(ctx); err != nil {
			t.Fatalf("ProcessOne: %v", err)
		}
		state := loadStateRow(t, rdb, cfg, executionID)
		if state.Status != StatusError || state.Error != nil {
			t.Fatalf("terminal row must stay untouched, got %+v", state)
		}
	})

	t.Run("resume with cancel flag → cancelled", func(t *testing.T) {
		w, rdb, _, cfg := newTestWorker(t)
		ctx := context.Background()
		executionID := newExecutionID()
		rdb.Set(ctx, stateKeyOf(cfg, executionID),
			fmt.Sprintf(`{"execution_id":%q,"status":"suspended"}`, executionID), 0)
		rdb.Set(ctx, cfg.KeyPrefix+":execution:cancel:"+executionID, nowISO(), time.Minute)

		payload := baseStartPayload(executionID)
		payload["type"] = "resume"
		payload["resume_type"] = "cancel"
		enqueueTask(t, rdb, cfg, payload, "")

		if _, err := w.ProcessOne(ctx); err != nil {
			t.Fatalf("ProcessOne: %v", err)
		}
		state := loadStateRow(t, rdb, cfg, executionID)
		if state.Status != StatusCancelled {
			t.Fatalf("status = %q, want cancelled (cancel wins, §4.2)", state.Status)
		}
	})

	t.Run("cancel carrier without flag still cancels", func(t *testing.T) {
		w, rdb, _, cfg := newTestWorker(t)
		ctx := context.Background()
		executionID := newExecutionID()
		rdb.Set(ctx, stateKeyOf(cfg, executionID),
			fmt.Sprintf(`{"execution_id":%q,"status":"suspended"}`, executionID), 0)

		payload := baseStartPayload(executionID)
		payload["type"] = "resume"
		payload["resume_type"] = "cancel"
		enqueueTask(t, rdb, cfg, payload, "")

		if _, err := w.ProcessOne(ctx); err != nil {
			t.Fatalf("ProcessOne: %v", err)
		}
		state := loadStateRow(t, rdb, cfg, executionID)
		if state.Status != StatusCancelled {
			t.Fatalf("status = %q, want cancelled (carrier carries the intent)", state.Status)
		}
	})

	t.Run("resume for missing execution → poison ack", func(t *testing.T) {
		w, rdb, _, cfg := newTestWorker(t)
		ctx := context.Background()
		executionID := newExecutionID()
		payload := baseStartPayload(executionID)
		payload["type"] = "resume"
		payload["resume_type"] = "continue"
		enqueueTask(t, rdb, cfg, payload, "")

		if _, err := w.ProcessOne(ctx); err != nil {
			t.Fatalf("ProcessOne: %v", err)
		}
		if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
			t.Fatalf("queue XLEN = %d, want 0 (acked)", n)
		}
		if _, err := rdb.Get(ctx, stateKeyOf(cfg, executionID)).Result(); err == nil {
			t.Fatal("no state row should be created for a missing execution")
		}
	})
}

// TestWorkerMissingFlowDeterministicFailure: start for a flow that is not in
// engine storage → error terminal with the worker-level error_type, poison
// ack (§7 FlowDefinition path).
func TestWorkerMissingFlowDeterministicFailure(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	executionID := newExecutionID()
	payload := baseStartPayload(executionID)
	payload["flow_id"] = "never_synced"
	enqueueTask(t, rdb, cfg, payload, "")

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	state := loadStateRow(t, rdb, cfg, executionID)
	if state.Status != StatusError {
		t.Fatalf("status = %q, want error", state.Status)
	}
	if state.Error["error_type"] != "flow_not_found" {
		t.Fatalf("error_type = %v, want flow_not_found", state.Error["error_type"])
	}
	if state.Error["code"] != float64(flowErrorCode) {
		t.Fatalf("code = %v, want %d", state.Error["code"], flowErrorCode)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0 (poison ack)", n)
	}
	if v := rdb.Get(ctx, cfg.KeyPrefix+":execution:lease:"+executionID).Val(); v != "" {
		t.Fatal("lease must be released after the deterministic failure")
	}
}

// TestWorkerEngineErrorNormalized: a retryable node failure with no budget
// left → error terminal with the §5.4 normalized structure + the fixed
// node_retry_budget_exhausted error_type (§9.1).
func TestWorkerEngineErrorNormalized(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	cfg.MaxDeliveries = 1 // first failure exhausts the per-node budget
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

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	state := loadStateRow(t, rdb, cfg, executionID)
	if state.Status != StatusError {
		t.Fatalf("status = %q, want error", state.Status)
	}
	if state.Error["failed_node"] != "boom" {
		t.Fatalf("failed_node = %v, want boom", state.Error["failed_node"])
	}
	if msg, _ := state.Error["message"].(string); !strings.Contains(msg, "kaputt") {
		t.Fatalf("message = %v, want the node error", state.Error["message"])
	}
	if state.Error["error_type"] != "node_retry_budget_exhausted" {
		t.Fatalf("error_type = %v, want node_retry_budget_exhausted", state.Error["error_type"])
	}
	if state.Error["code"] != float64(DefaultNodeAbortCode) {
		t.Fatalf("code = %v, want %d", state.Error["code"], DefaultNodeAbortCode)
	}
	if state.Error["node_retries"] != float64(1) {
		t.Fatalf("node_retries = %v, want 1", state.Error["node_retries"])
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0", n)
	}
	// timeline ends with run_end(final=false) then final=true.
	timeline, _ := state.Timeline["events"].([]any)
	if len(timeline) < 2 {
		t.Fatalf("timeline too short: %v", state.Timeline)
	}
	lastTypes := []string{}
	for _, e := range timeline[len(timeline)-2:] {
		ev := e.(map[string]any)
		lastTypes = append(lastTypes, ev["type"].(string)+"."+fmt.Sprint(ev["final"]))
	}
	if lastTypes[0] != "run_end.false" || lastTypes[1] != "run_end.true" {
		t.Fatalf("terminal run_end pair = %v, want [run_end.false run_end.true]", lastTypes)
	}
}

// TestWorkerUnknownTypePoisonAck: unknown type → ValueError class → ack
// without execution (§2.3).
func TestWorkerUnknownTypePoisonAck(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	executionID := newExecutionID()
	payload := baseStartPayload(executionID)
	payload["type"] = "teleport"
	enqueueTask(t, rdb, cfg, payload, "")

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0", n)
	}
	if n := streamLen(t, rdb, cfg.DLQKey); n != 0 {
		t.Fatalf("DLQ XLEN = %d, want 0 (poison ack is not a dead letter)", n)
	}
}

// TestWorkerRunLoop: the Run loop drains tasks until stopped.
func TestWorkerRunLoop(t *testing.T) {
	w, rdb, mr, cfg := newTestWorker(t)
	ctx := context.Background()
	presetHelloWorld(t, w)
	for i := 0; i < 3; i++ {
		enqueueTask(t, rdb, cfg, baseStartPayload(newExecutionID()), "")
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		_ = w.Run(runCtx)
		close(done)
	}()
	if !waitFor(t, 5*time.Second, func() bool {
		return streamLen(t, rdb, cfg.QueueName) == 0 && pendingCount(t, rdb, cfg) == 0
	}) {
		t.Fatal("run loop did not drain the queue")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("run loop did not stop")
	}
	_ = mr
}
