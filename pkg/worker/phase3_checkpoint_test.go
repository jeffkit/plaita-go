package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// --- M6 checkpoint v1.5 worker chain: suspend → wake-up → resume -----------

func presetFlowJSON(t *testing.T, w *Worker, flowID, version, raw string) {
	t.Helper()
	if err := w.flows.PresetFlowDefinition(context.Background(), "", flowID, version, []byte(raw)); err != nil {
		t.Fatalf("preset flow: %v", err)
	}
}

func startPayloadFor(flowID, version, executionID string) map[string]any {
	return map[string]any{
		"type":         "start",
		"flow_id":      flowID,
		"version":      version,
		"execution_id": executionID,
		"params":       map[string]any{},
		"timestamp":    nowISO(),
	}
}

func loadCheckpointRow(t *testing.T, rdb *redis.Client, cfg *Config, executionID string) *workflow.Checkpoint {
	t.Helper()
	raw, err := rdb.Get(context.Background(), cfg.KeyPrefix+":execution:checkpoint:"+executionID).Result()
	if err != nil {
		t.Fatalf("checkpoint read: %v", err)
	}
	var cp workflow.Checkpoint
	if err := json.Unmarshal([]byte(raw), &cp); err != nil {
		t.Fatalf("checkpoint decode: %v", err)
	}
	return &cp
}

func pendingNodeState(t *testing.T, state *ExecutionState, nodeID string) map[string]any {
	t.Helper()
	nodes, ok := state.Context["$NODE"].(map[string]any)
	if !ok {
		t.Fatalf("state context has no $NODE: %v", state.Context)
	}
	node, ok := nodes[nodeID].(map[string]any)
	if !ok {
		t.Fatalf("$NODE.%s missing: %v", nodeID, nodes)
	}
	return node
}

// testEventFlow is a minimal suspend flow: start → event → end.
func testEventFlow(flowID, subscriptionTimeoutField string) string {
	return fmt.Sprintf(`{
	  "flow_id": %q,
	  "version": "1",
	  "nodes": [
	    {"id": "start", "type": "start", "next": "wait"},
	    {"id": "wait", "type": "event", "event_type": "approval"%s, "next": "end"},
	    {"id": "end", "type": "end", "resultType": "success", "output": "$NODE.wait.status"}
	  ]
	}`, flowID, subscriptionTimeoutField)
}

func TestWorkerEventSuspendWakeResume(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	presetFlowJSON(t, w, "event_flow", "1", testEventFlow("event_flow", ""))

	executionID := newExecutionID()
	enqueueTask(t, rdb, cfg, startPayloadFor("event_flow", "1", executionID), "")
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("start ProcessOne: %v", err)
	}

	// Suspended row: no end time, pending node state, checkpoint + subscription.
	state := loadStateRow(t, rdb, cfg, executionID)
	if state.Status != StatusSuspended {
		t.Fatalf("status = %q, want suspended", state.Status)
	}
	if state.EndTime != "" {
		t.Fatalf("suspended row must not have end_time, got %q", state.EndTime)
	}
	waitState := pendingNodeState(t, state, "wait")
	if waitState["status"] != "pending" {
		t.Fatalf("$NODE.wait.status = %v, want pending", waitState["status"])
	}
	subscriptionID, _ := waitState["subscription_id"].(string)
	if subscriptionID == "" {
		t.Fatalf("subscription_id missing from pending state: %v", waitState)
	}
	if raw := rdb.Get(ctx, cfg.KeyPrefix+":execution:checkpoint:"+executionID).Val(); raw == "" {
		t.Fatal("checkpoint key missing after suspension")
	}
	cp := loadCheckpointRow(t, rdb, cfg, executionID)
	if cp.SchemaVersion != workflow.CheckpointSchemaVersion || cp.SuspendedNode != "wait" {
		t.Fatalf("checkpoint header wrong: %+v", cp)
	}
	subKey := cfg.Namespace("") + ":event:subscriptions:" + subscriptionID
	if raw := rdb.Get(ctx, subKey).Val(); raw == "" {
		t.Fatal("event subscription key missing after suspension")
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("start carrier not acked: XLEN=%d", n)
	}
	if v := rdb.Get(ctx, cfg.KeyPrefix+":execution:lease:"+executionID).Val(); v != "" {
		t.Fatalf("lease must be released while suspended, got %q", v)
	}

	// Fake event injection (miniredis, no Pub/Sub needed): the dispatcher must
	// enqueue exactly one event resume carrier.
	ev := &Event{
		EventID:       "evt-1",
		EventType:     "approval",
		CorrelationID: executionID,
		Data:          map[string]any{"by": "tester"},
	}
	if err := w.HandleEvent(ctx, ev); err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 1 {
		t.Fatalf("resume carrier count = %d, want 1", n)
	}
	// A duplicate event must be deduped.
	if err := w.HandleEvent(ctx, ev); err != nil {
		t.Fatalf("duplicate HandleEvent: %v", err)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 1 {
		t.Fatalf("duplicate event produced another carrier: XLEN=%d", n)
	}

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("resume ProcessOne: %v", err)
	}

	final := loadStateRow(t, rdb, cfg, executionID)
	if final.Status != StatusCompleted {
		t.Fatalf("resumed status = %q, want completed", final.Status)
	}
	wait := pendingNodeState(t, final, "wait")
	if wait["status"] != "completed" {
		t.Fatalf("$NODE.wait.status = %v, want completed", wait["status"])
	}
	if raw := rdb.Get(ctx, cfg.KeyPrefix+":execution:checkpoint:"+executionID).Val(); raw != "" {
		t.Fatal("checkpoint must be deleted on a terminal write")
	}
	if raw := rdb.Get(ctx, subKey).Val(); raw != "" {
		t.Fatal("subscription must be unregistered by the resume")
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("resume carrier not acked: XLEN=%d", n)
	}
	// The resumed run must carry the pre-suspend history forward.
	if timing, ok := final.NodeTimings["wait"].(map[string]any); !ok || timing["ended_at"] == "" {
		t.Fatalf("node_timings[wait] not preserved/closed after resume: %v", final.NodeTimings)
	}
	if events, ok := final.Timeline["events"].([]any); !ok || len(events) < 6 {
		t.Fatalf("timeline lost the pre-suspend events: %v", final.Timeline)
	}
}

func TestWorkerEventSubscriptionTimeoutResume(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	presetFlowJSON(t, w, "event_timeout_flow", "1",
		testEventFlow("event_timeout_flow", `, "subscription_timeout": 0.05`))

	executionID := newExecutionID()
	enqueueTask(t, rdb, cfg, startPayloadFor("event_timeout_flow", "1", executionID), "")
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("start ProcessOne: %v", err)
	}
	if state := loadStateRow(t, rdb, cfg, executionID); state.Status != StatusSuspended {
		t.Fatalf("status = %q, want suspended", state.Status)
	}

	time.Sleep(120 * time.Millisecond)
	if err := w.SweepEventTimeouts(ctx); err != nil {
		t.Fatalf("SweepEventTimeouts: %v", err)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 1 {
		t.Fatalf("timeout carrier count = %d, want 1", n)
	}
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("timeout resume ProcessOne: %v", err)
	}
	final := loadStateRow(t, rdb, cfg, executionID)
	if final.Status != StatusCompleted {
		t.Fatalf("status = %q, want completed", final.Status)
	}
	if got := pendingNodeState(t, final, "wait")["status"]; got != "timeout" {
		t.Fatalf("$NODE.wait.status = %v, want timeout", got)
	}
}

func TestWorkerStaleEventResumeLeavesExecutionSuspended(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	presetFlowJSON(t, w, "event_stale_flow", "1", testEventFlow("event_stale_flow", ""))

	executionID := newExecutionID()
	enqueueTask(t, rdb, cfg, startPayloadFor("event_stale_flow", "1", executionID), "")
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("start ProcessOne: %v", err)
	}

	// A carrier bound to a different subscription is stale: it must neither
	// wake the node nor terminalize the execution.
	enqueueTask(t, rdb, cfg, map[string]any{
		"type":         "resume",
		"flow_id":      "event_stale_flow",
		"execution_id": executionID,
		"resume_type":  "event",
		"timestamp":    nowISO(),
		"data": map[string]any{
			"subscription_id": "stale-sub",
			"event_type":      "approval",
		},
	}, "")
	if _, err := w.ProcessOne(ctx); err == nil {
		t.Fatal("stale carrier must surface an error (message left pending)")
	}
	state := loadStateRow(t, rdb, cfg, executionID)
	if state.Status != StatusSuspended {
		t.Fatalf("status = %q, want suspended (stale carrier must not advance)", state.Status)
	}
	if got := pendingCount(t, rdb, cfg); got != 1 {
		t.Fatalf("pending messages = %d, want 1 (stale carrier left pending)", got)
	}
}

func TestWorkerCancelResumeOnSuspended(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	presetFlowJSON(t, w, "event_cancel_flow", "1", testEventFlow("event_cancel_flow", ""))

	executionID := newExecutionID()
	enqueueTask(t, rdb, cfg, startPayloadFor("event_cancel_flow", "1", executionID), "")
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("start ProcessOne: %v", err)
	}

	enqueueTask(t, rdb, cfg, map[string]any{
		"type":         "resume",
		"flow_id":      "event_cancel_flow",
		"execution_id": executionID,
		"resume_type":  "cancel",
		"timestamp":    nowISO(),
	}, "")
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("cancel resume ProcessOne: %v", err)
	}
	state := loadStateRow(t, rdb, cfg, executionID)
	if state.Status != StatusCancelled {
		t.Fatalf("status = %q, want cancelled", state.Status)
	}
	if raw := rdb.Get(ctx, cfg.KeyPrefix+":execution:checkpoint:"+executionID).Val(); raw != "" {
		t.Fatal("checkpoint must be deleted when the suspended execution is cancelled")
	}
}

func TestResumeFlowHashMismatchFailsClosed(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	presetFlowJSON(t, w, "hash_flow", "1", testEventFlow("hash_flow", ""))

	executionID := newExecutionID()
	enqueueTask(t, rdb, cfg, startPayloadFor("hash_flow", "1", executionID), "")
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("start ProcessOne: %v", err)
	}
	state := loadStateRow(t, rdb, cfg, executionID)
	subscriptionID, _ := pendingNodeState(t, state, "wait")["subscription_id"].(string)
	if subscriptionID == "" {
		t.Fatal("subscription id missing")
	}

	// Publish a changed definition under the SAME flow/version, then drop the
	// flow-source cache (fresh process semantics).
	changed := strings.Replace(testEventFlow("hash_flow", ""), "$NODE.wait.status", "$NODE.wait.event_type", 1)
	presetFlowJSON(t, w, "hash_flow", "1", changed)
	w.flows.cache = map[string]FlowDefinition{}

	enqueueTask(t, rdb, cfg, map[string]any{
		"type":         "resume",
		"flow_id":      "hash_flow",
		"execution_id": executionID,
		"resume_type":  "event",
		"timestamp":    nowISO(),
		"data": map[string]any{
			"subscription_id": subscriptionID,
			"event_type":      "approval",
		},
	}, "")
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("resume ProcessOne: %v", err)
	}
	final := loadStateRow(t, rdb, cfg, executionID)
	if final.Status != StatusError || final.Error["error_type"] != "flow_hash_mismatch" {
		t.Fatalf("state = %+v, want error/flow_hash_mismatch", final)
	}
	if raw := rdb.Get(ctx, cfg.KeyPrefix+":execution:checkpoint:"+executionID).Val(); raw != "" {
		t.Fatal("checkpoint must be deleted after the hash-mismatch terminal")
	}
}

func TestEventBridgePubSubDeliversResume(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	presetFlowJSON(t, w, "event_pubsub_flow", "1", testEventFlow("event_pubsub_flow", ""))

	executionID := newExecutionID()
	enqueueTask(t, rdb, cfg, startPayloadFor("event_pubsub_flow", "1", executionID), "")
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("start ProcessOne: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		w.eventBridgeLoop(ctx)
	}()
	if !waitFor(t, 2*time.Second, func() bool {
		return rdb.PubSubNumPat(ctx).Val() >= 1
	}) {
		t.Fatal("event bridge did not subscribe")
	}

	bus := NewEventPublisher(rdb, cfg)
	if err := bus.Publish(ctx, &Event{
		EventID:       "evt-pubsub",
		EventType:     "approval",
		CorrelationID: executionID,
		Timestamp:     float64(time.Now().UnixNano()) / 1e9,
		Data:          map[string]any{"via": "pubsub"},
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if !waitFor(t, 2*time.Second, func() bool { return streamLen(t, rdb, cfg.QueueName) == 1 }) {
		t.Fatalf("event bridge did not enqueue a resume carrier (XLEN=%d)", streamLen(t, rdb, cfg.QueueName))
	}
	cancel()
	<-done
}

func TestWorkerHitlAwaitFullChain(t *testing.T) {
	var sendBody map[string]any
	hitlServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/send" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewDecoder(r.Body).Decode(&sendBody)
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "session_id": "sess-9"})
	}))
	defer hitlServer.Close()

	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	flow := fmt.Sprintf(`{
	  "flow_id": "hitl_await_flow",
	  "version": "1",
	  "nodes": [
	    {"id": "start", "type": "start", "next": "confirm"},
	    {"id": "confirm", "type": "hitl_await", "message": "请确认", "base_url": %q,
	     "timeout_secs": 30, "next": "end"},
	    {"id": "end", "type": "end", "resultType": "success", "output": "$NODE.confirm.replies"}
	  ]
	}`, hitlServer.URL)
	presetFlowJSON(t, w, "hitl_await_flow", "1", flow)

	executionID := newExecutionID()
	enqueueTask(t, rdb, cfg, startPayloadFor("hitl_await_flow", "1", executionID), "")
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("start ProcessOne: %v", err)
	}
	state := loadStateRow(t, rdb, cfg, executionID)
	if state.Status != StatusSuspended {
		t.Fatalf("status = %q, want suspended", state.Status)
	}
	if sendBody["message"] != "请确认" {
		t.Fatalf("hitl message = %v", sendBody["message"])
	}
	confirmState := pendingNodeState(t, state, "confirm")
	if confirmState["session_id"] != "sess-9" || confirmState["status"] != "pending" {
		t.Fatalf("pending hitl state wrong: %v", confirmState)
	}
	subscriptionID, _ := confirmState["subscription_id"].(string)
	if subscriptionID == "" {
		t.Fatal("hitl_await subscription id missing")
	}

	// The external poller publishes hitl_reply; fake-inject it.
	if err := w.HandleEvent(ctx, &Event{
		EventID:       "evt-hitl",
		EventType:     "hitl_reply",
		CorrelationID: executionID,
		Data: map[string]any{
			"status":     "replied",
			"replies":    []any{"同意"},
			"session_id": "sess-9",
		},
	}); err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("resume ProcessOne: %v", err)
	}

	final := loadStateRow(t, rdb, cfg, executionID)
	if final.Status != StatusCompleted {
		t.Fatalf("status = %q, want completed", final.Status)
	}
	replies, ok := pendingNodeState(t, final, "confirm")["replies"].([]any)
	if !ok || len(replies) != 1 || replies[0] != "同意" {
		t.Fatalf("$NODE.confirm.replies = %v", pendingNodeState(t, final, "confirm")["replies"])
	}
}
