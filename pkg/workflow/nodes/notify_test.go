package nodes

// Tests for the notify/outbound family. No real external service is ever
// contacted: webhooks run against httptest servers, email delivery goes
// through an injected sender or a local fake SMTP server.
// Semantic authority: plaita-nodes tests/test_notify_backends.py,
// test_a2r7_06_webhook_business_codes.py, test_a2r8_webhook_redaction.py,
// test_issue16_global_dry_run.py.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/jeffkit/plaita-go/internal/runtime"
)

// notifyTestContext mirrors agentRunContext: a runtime context with fixed
// input and global variables.
func notifyTestContext(t *testing.T, input, globals map[string]any) context.Context {
	t.Helper()
	return runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput:      input,
		ExecuteNodeResults: map[string]any{},
		EnvVariables:       map[string]any{},
		GlobalVariables:    globals,
	})
}

// notifyTestCredentialStore is a map-backed NotifyCredentialStore fake.
type notifyTestCredentialStore map[string]map[string]any

// GetCredential resolves a credential name to its data blob.
func (s notifyTestCredentialStore) GetCredential(_ context.Context, name string) (map[string]any, error) {
	if data, ok := s[name]; ok {
		return data, nil
	}
	return nil, fmt.Errorf("credential %q not found", name)
}

// notifyTestCapture records what the webhook server received.
type notifyTestCapture struct {
	Method string
	Path   string
	Query  url.Values
	Header http.Header
	Body   []byte
}

// notifyTestServer spins an httptest server that records the request and
// replies with the given status/body (string bodies verbatim, anything else
// JSON-encoded).
func notifyTestServer(t *testing.T, status int, response any, captured *notifyTestCapture) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if captured != nil {
			captured.Method = r.Method
			captured.Path = r.URL.Path
			captured.Query = r.URL.Query()
			captured.Header = r.Header.Clone()
			captured.Body = body
		}
		switch v := response.(type) {
		case string:
			w.WriteHeader(status)
			_, _ = w.Write([]byte(v))
		case func(w http.ResponseWriter, r *http.Request):
			v(w, r)
		default:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(response)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// notifyTestCaptureServer records requests and always answers 200 {"ok": true}.
func notifyTestCaptureServer(t *testing.T, captured *notifyTestCapture) string {
	t.Helper()
	return notifyTestServer(t, http.StatusOK, map[string]any{"ok": true}, captured).URL
}

// TestNotifyTerminalPrintsAndDryRunSilent mirrors
// test_terminal_prints_and_dry_run_silent.
func TestNotifyTerminalPrintsAndDryRunSilent(t *testing.T) {
	stdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	defer func() { os.Stdout = stdout }()

	node := &Notify{BaseNode: BaseNode{ID: "n"}, Message: "构建完成"}
	res, err := node.Execute(notifyTestContext(t, nil, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	_ = w.Close()
	printed, _ := io.ReadAll(r)
	if want := "[notify] 构建完成\n"; !strings.Contains(string(printed), want) {
		t.Errorf("stdout = %q, want containing %q", string(printed), want)
	}
	if out, ok := res.Result.(map[string]any); !ok || out["ok"] != true || out["channel"] != "terminal" {
		t.Errorf("result = %#v", res.Result)
	}

	dry := &Notify{BaseNode: BaseNode{ID: "n"}, Message: "机密", DryRun: true}
	res, err = dry.Execute(notifyTestContext(t, nil, nil))
	if err != nil {
		t.Fatalf("dry Execute: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["dry_run"] != true || out["channel"] != "terminal" || out["ok"] != true {
		t.Errorf("dry result = %#v", out)
	}
}

// TestNotifyGlobalDryRunGatesBeforeEvaluation mirrors pn#16: the dry gate
// must fire before message evaluation and any backend call.
func TestNotifyGlobalDryRunGatesBeforeEvaluation(t *testing.T) {
	node := &Notify{
		BaseNode: BaseNode{ID: "n"},
		// A broken expression would error if evaluation happened before the gate.
		Message: "$INPUT.",
		Channel: "feishu_webhook",
		Credentials: notifyTestCredentialStore{"c": {
			"url": notifyTestCaptureServer(t, &notifyTestCapture{}),
		}},
	}
	res, err := node.Execute(notifyTestContext(t, map[string]any{"msg": "hi"}, map[string]any{"dry_run": true}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["ok"] != true || out["channel"] != "feishu_webhook" || out["dry_run"] != true {
		t.Errorf("dry result = %#v", out)
	}
}

// TestNotifyUnknownChannelErrorListsRegistered mirrors
// test_unknown_channel_error_lists_registered.
func TestNotifyUnknownChannelErrorListsRegistered(t *testing.T) {
	node := &Notify{BaseNode: BaseNode{ID: "n"}, Message: "x", Channel: "carrier_pigeon"}
	_, err := node.Execute(notifyTestContext(t, nil, nil))
	if err == nil {
		t.Fatal("unknown channel must fail")
	}
	msg := err.Error()
	if !strings.Contains(msg, "carrier_pigeon") || !strings.Contains(msg, "terminal") {
		t.Errorf("error text = %q, want channel name and available list", msg)
	}
}

// TestNotifyChannelDispatchToFeishuBackend: notify(channel=feishu_webhook)
// delegates to the same backend as the dedicated node (registry semantics).
func TestNotifyChannelDispatchToFeishuBackend(t *testing.T) {
	var captured notifyTestCapture
	url := notifyTestCaptureServer(t, &captured)
	node := &Notify{
		BaseNode:    BaseNode{ID: "n"},
		Message:     "$INPUT.msg",
		Channel:     "feishu_webhook",
		Credential:  "bot",
		Credentials: notifyTestCredentialStore{"bot": {"url": url}},
	}
	res, err := node.Execute(notifyTestContext(t, map[string]any{"msg": "发布通知"}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["status"] != 200 {
		t.Errorf("status = %v", out["status"])
	}
	var payload map[string]any
	if err := json.Unmarshal(captured.Body, &payload); err != nil {
		t.Fatalf("payload: %v", err)
	}
	content := payload["content"].(map[string]any)
	if payload["msg_type"] != "text" || content["text"] != "发布通知" {
		t.Errorf("payload = %s", captured.Body)
	}
}

// TestNotifyJSONRoundTrip checks the deploy-JSON unmarshal shape.
func TestNotifyJSONRoundTrip(t *testing.T) {
	doc := `{"type":"notify","id":"n1","message":"$INPUT.msg","channel":"slack_webhook",
		"credential":"c","dry_run":true}`
	node := new(Notify)
	if err := json.Unmarshal([]byte(doc), node); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if node.GetType() != nodeTypeNotify {
		t.Errorf("GetType = %v", node.GetType())
	}
	if node.Message != "$INPUT.msg" || node.Channel != "slack_webhook" ||
		node.Credential != "c" || !node.DryRun {
		t.Errorf("fields = %#v", node)
	}
}
