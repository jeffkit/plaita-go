package ir

import (
	"context"
	"sync"
	"testing"

	"github.com/jeffkit/plaita-go/pkg/workflow"
)

type irFakeRegistry struct {
	mu       sync.Mutex
	requests []workflow.SubscriptionRequest
}

func (r *irFakeRegistry) Register(_ context.Context, req *workflow.SubscriptionRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if req.SubscriptionID == "" {
		req.SubscriptionID = "sub-ir"
	}
	r.requests = append(r.requests, *req)
	return nil
}

func (r *irFakeRegistry) Unregister(context.Context, string, string) error { return nil }

// TestLoadEventFlowSuspends proves the M6 contract end to end at the IR
// layer: a camelCase deployment flow with an event node loads, suspends with
// a checkpoint, and resumes through its subscription.
func TestLoadEventFlowSuspends(t *testing.T) {
	raw := []byte(`{
	  "flow_id": "event_flow_ir",
	  "nodes": [
	    {"id": "start", "type": "start", "next": "wait"},
	    {"id": "wait", "type": "event", "eventType": "deploy_done",
	     "eventFilter": {"status": "ok"}, "subscriptionTimeout": 30, "next": "end"},
	    {"id": "end", "type": "end", "resultType": "success", "output": "$NODE.wait.status"}
	  ]
	}`)
	wf, err := Load(raw)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	store := workflow.NewMemoryCheckpointStore()
	registry := &irFakeRegistry{}
	opts := &workflow.SuspendOptions{
		ExecutionID:   "exec-ir",
		FlowID:        "event_flow_ir",
		Store:         store,
		Subscriptions: registry,
	}
	ctx := workflow.WithSuspendOptions(context.Background(), opts)
	_, err = wf.Execute(ctx, nil)
	susp := workflow.AsSuspend(err)
	if susp == nil {
		t.Fatalf("expected suspension, got err=%v", err)
	}
	if len(registry.requests) != 1 {
		t.Fatalf("subscriptions = %d, want 1", len(registry.requests))
	}
	req := registry.requests[0]
	if req.EventType != "deploy_done" || req.TimeoutSeconds != 30 {
		t.Fatalf("subscription wrong: %+v", req)
	}
	if _, ok := req.Filter["status"]; !ok {
		t.Fatalf("eventFilter not folded: %+v", req.Filter)
	}

	ctx = workflow.WithSuspendOptions(context.Background(), opts)
	result, err := wf.Resume(ctx, susp.Checkpoint, workflow.ResumeEvent, map[string]any{
		"subscription_id": req.SubscriptionID,
		"event_type":      "deploy_done",
		"event_data":      map[string]any{"status": "ok"},
	})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if result != "completed" {
		t.Fatalf("flow result = %v, want completed", result)
	}
}

func TestLoadEventRejectsEmptyType(t *testing.T) {
	raw := []byte(`{
	  "flow_id": "bad_event",
	  "nodes": [
	    {"id": "start", "type": "start", "next": "wait"},
	    {"id": "wait", "type": "event", "eventType": "  ", "next": "end"},
	    {"id": "end", "type": "end"}
	  ]
	}`)
	if _, err := Load(raw); err == nil {
		t.Fatal("empty event_type must be rejected at load")
	}
}

func TestLoadHitlAwaitFlow(t *testing.T) {
	raw := []byte(`{
	  "flow_id": "hitl_await_ir",
	  "nodes": [
	    {"id": "start", "type": "start", "next": "confirm"},
	    {"id": "confirm", "type": "hitl_await", "message": "ok?",
	     "timeoutSecs": 60, "dryRun": true, "next": "end"},
	    {"id": "end", "type": "end", "resultType": "success", "output": "$NODE.confirm.status"}
	  ]
	}`)
	wf, err := Load(raw)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	registry := &irFakeRegistry{}
	opts := &workflow.SuspendOptions{
		ExecutionID:   "exec-hitl-ir",
		FlowID:        "hitl_await_ir",
		Store:         workflow.NewMemoryCheckpointStore(),
		Subscriptions: registry,
	}
	ctx := workflow.WithSuspendOptions(context.Background(), opts)
	_, err = wf.Execute(ctx, nil)
	susp := workflow.AsSuspend(err)
	if susp == nil {
		t.Fatalf("expected suspension, got err=%v", err)
	}
	nodeState := susp.Checkpoint.Context["$NODE"].(map[string]any)["confirm"].(map[string]any)
	if nodeState["session_id"] != "dryrun" {
		t.Fatalf("dry-run session not captured: %v", nodeState)
	}
	if len(registry.requests) != 1 || registry.requests[0].EventType != "hitl_reply" {
		t.Fatalf("hitl_reply subscription missing: %+v", registry.requests)
	}
	if registry.requests[0].TimeoutSeconds != 60 {
		t.Fatalf("hitl subscription timeout = %v, want 60", registry.requests[0].TimeoutSeconds)
	}
}
