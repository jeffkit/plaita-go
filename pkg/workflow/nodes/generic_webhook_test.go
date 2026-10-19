package nodes

// Tests for generic_webhook. Semantic authority:
// plaita-nodes api.py GenericWebhookNode — arbitrary JSON payload to the
// credential URL, HTTP >= 400 fails (4xx deterministic except
// 408/425/429; 5xx transient), 3xx counts as success, timeout_ms is
// fail-closed positive.

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestGenericWebhookPostsPayload(t *testing.T) {
	var captured notifyTestCapture
	srv := notifyTestServer(t, 200, map[string]any{"received": true}, &captured)
	node := &GenericWebhook{
		BaseNode: BaseNode{ID: "n"}, Credential: "c",
		Payload:     map[string]any{"event": "$INPUT.event", "count": 3},
		Credentials: notifyTestCredentialStore{"c": {"url": srv.URL + "/hook/SECRET.gw"}},
	}
	res, err := node.Execute(notifyTestContext(t, map[string]any{"event": "deploy"}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if captured.Method != "POST" || captured.Path != "/hook/SECRET.gw" {
		t.Errorf("request = %s %s", captured.Method, captured.Path)
	}
	var payload map[string]any
	if err := json.Unmarshal(captured.Body, &payload); err != nil {
		t.Fatalf("payload: %v", err)
	}
	if payload["event"] != "deploy" || payload["count"] != float64(3) {
		t.Errorf("payload = %s", captured.Body)
	}
	out := res.Result.(map[string]any)
	if out["status"] != 200 {
		t.Errorf("status = %v", out["status"])
	}
	if resp, ok := out["response"].(map[string]any); !ok || resp["received"] != true {
		t.Errorf("response = %v", out["response"])
	}
}

func TestGenericWebhookNilPayloadSendsNoBody(t *testing.T) {
	var captured notifyTestCapture
	srv := notifyTestServer(t, 200, map[string]any{"ok": true}, &captured)
	node := &GenericWebhook{BaseNode: BaseNode{ID: "n"}, Credential: "c",
		Credentials: notifyTestCredentialStore{"c": {"url": srv.URL}}}
	if _, err := node.Execute(notifyTestContext(t, nil, nil)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(captured.Body) != 0 {
		t.Errorf("body = %q, want none (requests json=None parity)", captured.Body)
	}
	if captured.Header.Get("Content-Type") != "" {
		t.Errorf("content-type = %q, want none", captured.Header.Get("Content-Type"))
	}
}

func TestGenericWebhookStatusClassification(t *testing.T) {
	cases := []struct {
		name      string
		status    int
		wantErr   bool
		transient bool
	}{
		{"302_is_success", http.StatusFound, false, false},
		{"399_is_success", 399, false, false},
		{"400_deterministic", http.StatusBadRequest, true, false},
		{"401_deterministic", http.StatusUnauthorized, true, false},
		{"408_transient", http.StatusRequestTimeout, true, true},
		{"425_transient", http.StatusTooEarly, true, true},
		{"429_transient", http.StatusTooManyRequests, true, true},
		{"500_transient", http.StatusInternalServerError, true, true},
		{"503_transient", http.StatusServiceUnavailable, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := notifyTestServer(t, tc.status, "boom detail", nil)
			node := &GenericWebhook{BaseNode: BaseNode{ID: "n"}, Credential: "c", Payload: map[string]any{"x": 1},
				Credentials: notifyTestCredentialStore{"c": {"url": srv.URL}}}
			res, err := node.Execute(notifyTestContext(t, nil, nil))
			if !tc.wantErr {
				if err != nil {
					t.Fatalf("Execute: %v", err)
				}
				if res.Result.(map[string]any)["status"] != tc.status {
					t.Errorf("status = %v", res.Result.(map[string]any)["status"])
				}
				return
			}
			if err == nil {
				t.Fatalf("HTTP %d must fail", tc.status)
			}
			if !strings.Contains(err.Error(), "webhook 调用失败 HTTP") {
				t.Errorf("error = %q", err.Error())
			}
			var det *NotifyDeterministicError
			var tran *NotifyTransientError
			isDet := errors.As(err, &det)
			isTran := errors.As(err, &tran)
			if tc.transient && !isTran {
				t.Errorf("HTTP %d: error %T not transient", tc.status, err)
			}
			if !tc.transient && !isDet {
				t.Errorf("HTTP %d: error %T not deterministic", tc.status, err)
			}
		})
	}
}

func TestGenericWebhookNonJSONResponseBecomesTruncatedText(t *testing.T) {
	long := strings.Repeat("x", 500)
	srv := notifyTestServer(t, 200, long, nil)
	node := &GenericWebhook{BaseNode: BaseNode{ID: "n"}, Credential: "c",
		Credentials: notifyTestCredentialStore{"c": {"url": srv.URL}}}
	res, err := node.Execute(notifyTestContext(t, nil, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	resp := res.Result.(map[string]any)["response"].(string)
	if len([]rune(resp)) != 200 {
		t.Errorf("response length = %d, want 200-rune truncation", len([]rune(resp)))
	}
}

func TestGenericWebhookTimeoutMsValidation(t *testing.T) {
	zero := 0
	negative := -5
	if err := (&GenericWebhook{BaseNode: BaseNode{ID: "n"}, TimeoutMs: &zero}).Validate(); err == nil ||
		!strings.Contains(err.Error(), "timeout_ms must be a positive number of milliseconds") {
		t.Errorf("zero timeout err = %v", err)
	}
	if err := (&GenericWebhook{BaseNode: BaseNode{ID: "n"}, TimeoutMs: &negative}).Validate(); err == nil {
		t.Error("negative timeout must fail")
	}
	if err := (&GenericWebhook{BaseNode: BaseNode{ID: "n"}}).Validate(); err != nil {
		t.Errorf("default validate: %v", err)
	}

	// JSON unmarshal shape.
	node := new(GenericWebhook)
	doc := `{"type":"generic_webhook","id":"g1","credential":"c","payload":{"k":"$INPUT.k"},"timeout_ms":3000}`
	if err := json.Unmarshal([]byte(doc), node); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if node.GetType() != nodeTypeGenericWebhook || node.TimeoutMs == nil || *node.TimeoutMs != 3000 {
		t.Errorf("node = %#v", node)
	}
}

func TestGenericWebhookTimeoutIsApplied(t *testing.T) {
	srv := notifyTestServer(t, 200, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
	}, nil)
	ms := 50
	node := &GenericWebhook{BaseNode: BaseNode{ID: "n"}, Credential: "c", TimeoutMs: &ms,
		Credentials: notifyTestCredentialStore{"c": {"url": srv.URL}}}
	_, err := node.Execute(notifyTestContext(t, nil, nil))
	if err == nil {
		t.Fatal("50ms budget must trip on a 300ms handler")
	}
}

func TestGenericWebhookCredentialChecks(t *testing.T) {
	node := &GenericWebhook{BaseNode: BaseNode{ID: "n"}}
	if _, err := node.Execute(notifyTestContext(t, nil, nil)); err == nil ||
		!strings.Contains(err.Error(), "缺少 credential 字段") {
		t.Errorf("missing credential err = %v", err)
	}
	noURL := &GenericWebhook{BaseNode: BaseNode{ID: "n"}, Credential: "c",
		Credentials: notifyTestCredentialStore{"c": {}}}
	if _, err := noURL.Execute(notifyTestContext(t, nil, nil)); err == nil ||
		!strings.Contains(err.Error(), "缺少 url 字段") {
		t.Errorf("missing url err = %v", err)
	}
}
