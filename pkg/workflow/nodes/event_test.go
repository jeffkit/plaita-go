package nodes

import (
	"context"
	"testing"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func eventWithBase(id string) *Event {
	return &Event{
		BaseNode:    BaseNode{ID: "wait", Type: nodeTypeEvent, Next: "end"},
		EventType:   "approval",
		EventFilter: map[string]any{"data.status": "ok"},
	}
}

func TestEventExecuteReturnsPending(t *testing.T) {
	e := eventWithBase("wait")
	result, err := e.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	state := result.Result.(map[string]any)
	if state["status"] != EventStatusPending {
		t.Fatalf("status = %v, want pending", state["status"])
	}
	if state["event_type"] != "approval" {
		t.Fatalf("event_type = %v", state["event_type"])
	}
	if state["event_id"] == "" {
		t.Fatal("event_id missing")
	}
	if result.NextNode != "end" {
		t.Fatalf("next = %q, want end", result.NextNode)
	}
	if !e.IsSuspending() || e.GetType() != nodeTypeEvent {
		t.Fatal("event node must be a suspending node of type event")
	}
}

func TestEventTypeExpressionResolves(t *testing.T) {
	rc := runtime.NewContext()
	rc.WorkflowInput = map[string]any{"kind": "deploy"}
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), rc)
	e := &Event{BaseNode: BaseNode{ID: "wait", Type: nodeTypeEvent}, EventType: "$INPUT.kind"}
	result, err := e.Execute(ctx)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got := result.Result.(map[string]any)["event_type"]; got != "deploy" {
		t.Fatalf("resolved event_type = %v, want deploy", got)
	}
}

func TestEventEmptyTypeRejected(t *testing.T) {
	e := &Event{BaseNode: BaseNode{ID: "wait", Type: nodeTypeEvent}, EventType: "  "}
	if _, err := e.Execute(context.Background()); err == nil {
		t.Fatal("empty event_type must be rejected")
	}
}

func TestEventSuspendSpec(t *testing.T) {
	timeout := 12.5
	e := &Event{
		BaseNode:            BaseNode{ID: "wait", Type: nodeTypeEvent},
		EventType:           "approval",
		EventFilter:         map[string]any{"a": 1.0},
		SubscriptionTimeout: &timeout,
	}
	result, _ := e.Execute(context.Background())
	spec, err := e.SuspendSpec(context.Background(), result)
	if err != nil {
		t.Fatalf("SuspendSpec: %v", err)
	}
	if spec == nil || spec.Subscription == nil {
		t.Fatal("expected a subscription spec")
	}
	sub := spec.Subscription
	if sub.EventType != "approval" || sub.NodeID != "wait" || sub.TimeoutSeconds != 12.5 {
		t.Fatalf("subscription spec wrong: %+v", sub)
	}
}

func TestEventResumeThreeWay(t *testing.T) {
	e := eventWithBase("wait")
	cases := []struct {
		resumeType string
		data       map[string]any
		wantStatus string
	}{
		{workflow.ResumeEvent, map[string]any{"event_data": map[string]any{"ok": true}}, EventStatusCompleted},
		{workflow.ResumeTimeout, map[string]any{}, EventStatusTimeout},
		{workflow.ResumeCancel, nil, EventStatusCancelled},
	}
	for _, tc := range cases {
		t.Run(tc.resumeType, func(t *testing.T) {
			result, err := e.Resume(context.Background(), tc.resumeType, tc.data)
			if err != nil {
				t.Fatalf("Resume: %v", err)
			}
			state := result.Result.(map[string]any)
			if state["status"] != tc.wantStatus {
				t.Fatalf("status = %v, want %s", state["status"], tc.wantStatus)
			}
			if result.NextNode != "end" {
				t.Fatalf("next = %q, want end", result.NextNode)
			}
			if tc.resumeType == workflow.ResumeEvent {
				if _, ok := state["event_data"].(map[string]any); !ok {
					t.Fatalf("event_data missing: %v", state)
				}
			}
		})
	}
	if _, err := e.Resume(context.Background(), workflow.ResumeContinue, nil); err == nil {
		t.Fatal("continue must be rejected for a suspending node")
	}
}

func TestEventCanHandleEvent(t *testing.T) {
	e := eventWithBase("wait")
	if !e.CanHandleEvent("approval", map[string]any{"data": map[string]any{"status": "ok"}}) {
		t.Fatal("matching filter must accept the event")
	}
	if e.CanHandleEvent("other", map[string]any{"data": map[string]any{"status": "ok"}}) {
		t.Fatal("event type mismatch must reject")
	}
	if e.CanHandleEvent("approval", map[string]any{"data": map[string]any{"status": "no"}}) {
		t.Fatal("filter mismatch must reject")
	}
	if e.CanHandleEvent("approval", map[string]any{"missing": true}) {
		t.Fatal("missing filter path must reject")
	}
}
