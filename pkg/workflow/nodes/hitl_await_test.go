package nodes

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// hitlAwaitServer is a minimal fake hitl-server: it records the send body and
// answers with a session id.
func hitlAwaitServer(t *testing.T, sessionID string) (*httptest.Server, *map[string]any) {
	t.Helper()
	var sendBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/send":
			_ = json.NewDecoder(r.Body).Decode(&sendBody)
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "session_id": sessionID})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	return server, &sendBody
}

func TestHitlAwaitExecuteSendsAndPends(t *testing.T) {
	server, sendBody := hitlAwaitServer(t, "sess-7")
	node := &HitlAwait{
		BaseNode:    BaseNode{ID: "confirm", Type: nodeTypeHitlAwait, Next: "end"},
		Message:     "请确认发布",
		BaseURL:     server.URL,
		TimeoutSecs: 30,
	}
	result, err := node.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	state := result.Result.(map[string]any)
	if state["status"] != "pending" || state["session_id"] != "sess-7" {
		t.Fatalf("pending state wrong: %v", state)
	}
	if got := (*sendBody)["message"]; got != "请确认发布" {
		t.Fatalf("sent message = %v", got)
	}
	if got := (*sendBody)["wait_reply"]; got != true {
		t.Fatalf("wait_reply = %v, want true", got)
	}

	spec, err := node.SuspendSpec(context.Background(), result)
	if err != nil {
		t.Fatalf("SuspendSpec: %v", err)
	}
	sub := spec.Subscription
	if sub == nil || sub.EventType != hitlReplyEventType || sub.TimeoutSeconds != 30 || sub.NodeID != "confirm" {
		t.Fatalf("subscription spec wrong: %+v", sub)
	}
}

func TestHitlAwaitResume(t *testing.T) {
	node := &HitlAwait{BaseNode: BaseNode{ID: "confirm", Type: nodeTypeHitlAwait, Next: "end"}}

	result, err := node.Resume(context.Background(), workflow.ResumeEvent, map[string]any{
		"status":     "replied",
		"replies":    []any{"同意", "OK"},
		"session_id": "sess-7",
	})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	state := result.Result.(map[string]any)
	if state["status"] != "replied" || state["session_id"] != "sess-7" {
		t.Fatalf("resume state wrong: %v", state)
	}
	replies := state["replies"].([]any)
	if len(replies) != 2 || replies[0] != "同意" || replies[1] != "OK" {
		t.Fatalf("replies = %v", replies)
	}

	// Event-filter carrier shape: values nested under event_data.
	nested, err := node.Resume(context.Background(), workflow.ResumeEvent, map[string]any{
		"subscription_id": "sub-1",
		"event_data":      map[string]any{"status": "replied", "replies": []any{"是"}, "session_id": "sess-8"},
	})
	if err != nil {
		t.Fatalf("nested Resume: %v", err)
	}
	nestedState := nested.Result.(map[string]any)
	if nestedState["session_id"] != "sess-8" || nestedState["replies"].([]any)[0] != "是" {
		t.Fatalf("nested event_data not unwrapped: %v", nestedState)
	}

	for _, resumeType := range []string{workflow.ResumeTimeout, workflow.ResumeCancel} {
		out, err := node.Resume(context.Background(), resumeType, nil)
		if err != nil {
			t.Fatalf("%s Resume: %v", resumeType, err)
		}
		want := "timeout"
		if resumeType == workflow.ResumeCancel {
			want = "cancel"
		}
		if got := out.Result.(map[string]any)["status"]; got != want {
			t.Fatalf("%s status = %v, want %s", resumeType, got, want)
		}
	}
	if _, err := node.Resume(context.Background(), workflow.ResumeRetry, nil); err == nil {
		t.Fatal("retry must be rejected for a suspending node")
	}
}

func TestHitlAwaitDryRun(t *testing.T) {
	node := &HitlAwait{
		BaseNode: BaseNode{ID: "confirm", Type: nodeTypeHitlAwait, Next: "end"},
		Message:  "dry",
		DryRun:   true,
	}
	result, err := node.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	state := result.Result.(map[string]any)
	if state["session_id"] != "dryrun" || state["status"] != "pending" {
		t.Fatalf("dry run state wrong: %v", state)
	}
}
