package nodes

// Tests for the four IM webhook thin-shell nodes. Semantic authority:
// plaita-nodes webhooks.py + notify_backends.py; the regression cases mirror
// tests/test_a2r7_06_webhook_business_codes.py and
// tests/test_a2r8_webhook_redaction.py.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// webhookShellLike is the common shape of the four thin-shell nodes.
type webhookShellLike interface {
	Execute(ctx context.Context) (*workflow.ExecuteResult, error)
}

func TestFeishuWebhookPostsTextPayload(t *testing.T) {
	var captured notifyTestCapture
	srv := notifyTestServer(t, 200, map[string]any{"code": 0, "msg": "success"}, &captured)
	store := notifyTestCredentialStore{"bot": {"url": srv.URL + "/hook/SECRET.feishu"}}
	node := &FeishuWebhook{
		BaseNode: BaseNode{ID: "n"}, Credential: "bot",
		Text: "$INPUT.text", Credentials: store,
	}
	res, err := node.Execute(notifyTestContext(t, map[string]any{"text": "hello"}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if captured.Method != "POST" || captured.Path != "/hook/SECRET.feishu" {
		t.Errorf("request = %s %s", captured.Method, captured.Path)
	}
	if got := captured.Header.Get("Content-Type"); got != "application/json" {
		t.Errorf("content-type = %q", got)
	}
	var payload map[string]any
	if err := json.Unmarshal(captured.Body, &payload); err != nil {
		t.Fatalf("payload: %v", err)
	}
	content, _ := payload["content"].(map[string]any)
	if payload["msg_type"] != "text" || content["text"] != "hello" {
		t.Errorf("payload = %s", captured.Body)
	}
	out := res.Result.(map[string]any)
	if out["status"] != 200 {
		t.Errorf("status = %v", out["status"])
	}
	resp, _ := out["response"].(map[string]any)
	if resp["code"] != float64(0) {
		t.Errorf("response = %v", out["response"])
	}
}

// TestWebhookBusinessErrors mirrors test_a2r7_06: HTTP 200 + business error
// code must fail named; rate-limit codes stay retryable (transient);
// compatible/success bodies keep the output shape.
func TestWebhookBusinessErrors(t *testing.T) {
	cases := []struct {
		name      string
		nodeMaker func(url string) webhookShellLike
		body      any
		wantErr   string // substring of the error text, "" for success
		transient bool   // expected retryability class when wantErr != ""
	}{
		{
			name: "feishu_invalid_token_deterministic",
			nodeMaker: func(url string) webhookShellLike {
				return &FeishuWebhook{BaseNode: BaseNode{ID: "n"}, Credential: "c", Text: "hello",
					Credentials: notifyTestCredentialStore{"c": {"url": url}}}
			},
			body:    map[string]any{"code": 19003, "msg": "invalid webhook token"},
			wantErr: "19003",
		},
		{
			name: "feishu_rate_limited_transient",
			nodeMaker: func(url string) webhookShellLike {
				return &FeishuWebhook{BaseNode: BaseNode{ID: "n"}, Credential: "c", Text: "hello",
					Credentials: notifyTestCredentialStore{"c": {"url": url}}}
			},
			body:      map[string]any{"code": 9499, "msg": "too many request"},
			wantErr:   "9499",
			transient: true,
		},
		{
			name: "feishu_code_zero_success",
			nodeMaker: func(url string) webhookShellLike {
				return &FeishuWebhook{BaseNode: BaseNode{ID: "n"}, Credential: "c", Text: "hello",
					Credentials: notifyTestCredentialStore{"c": {"url": url}}}
			},
			body: map[string]any{"code": 0, "msg": "success"},
		},
		{
			name: "feishu_missing_code_field_compat_success",
			nodeMaker: func(url string) webhookShellLike {
				return &FeishuWebhook{BaseNode: BaseNode{ID: "n"}, Credential: "c", Text: "hello",
					Credentials: notifyTestCredentialStore{"c": {"url": url}}}
			},
			body: map[string]any{"ok": true},
		},
		{
			name: "wecom_invalid_url_deterministic",
			nodeMaker: func(url string) webhookShellLike {
				return &WecomWebhook{BaseNode: BaseNode{ID: "n"}, Credential: "c", Text: "hello",
					Credentials: notifyTestCredentialStore{"c": {"url": url}}}
			},
			body:    map[string]any{"errcode": 93000, "errmsg": "invalid webhook url"},
			wantErr: "93000",
		},
		{
			name: "wecom_rate_limited_transient",
			nodeMaker: func(url string) webhookShellLike {
				return &WecomWebhook{BaseNode: BaseNode{ID: "n"}, Credential: "c", Text: "hello",
					Credentials: notifyTestCredentialStore{"c": {"url": url}}}
			},
			body:      map[string]any{"errcode": 45009, "errmsg": "api freq out of limit"},
			wantErr:   "45009",
			transient: true,
		},
		{
			name: "wecom_errcode_zero_success",
			nodeMaker: func(url string) webhookShellLike {
				return &WecomWebhook{BaseNode: BaseNode{ID: "n"}, Credential: "c", Text: "hello",
					Credentials: notifyTestCredentialStore{"c": {"url": url}}}
			},
			body: map[string]any{"errcode": 0, "errmsg": "ok"},
		},
		{
			name: "slack_ok_false_deterministic",
			nodeMaker: func(url string) webhookShellLike {
				return &SlackWebhook{BaseNode: BaseNode{ID: "n"}, Credential: "c", Text: "hello",
					Credentials: notifyTestCredentialStore{"c": {"url": url}}}
			},
			body:    map[string]any{"ok": false, "error": "invalid_token"},
			wantErr: "invalid_token",
		},
		{
			name: "slack_rate_limited_transient",
			nodeMaker: func(url string) webhookShellLike {
				return &SlackWebhook{BaseNode: BaseNode{ID: "n"}, Credential: "c", Text: "hello",
					Credentials: notifyTestCredentialStore{"c": {"url": url}}}
			},
			body:      map[string]any{"ok": false, "error": "rate_limited"},
			wantErr:   "rate_limited",
			transient: true,
		},
		{
			name: "slack_ratelimited_alias_transient",
			nodeMaker: func(url string) webhookShellLike {
				return &SlackWebhook{BaseNode: BaseNode{ID: "n"}, Credential: "c", Text: "hello",
					Credentials: notifyTestCredentialStore{"c": {"url": url}}}
			},
			body:      map[string]any{"ok": false, "error": "ratelimited"},
			wantErr:   "ratelimited",
			transient: true,
		},
		{
			name: "slack_ok_true_success",
			nodeMaker: func(url string) webhookShellLike {
				return &SlackWebhook{BaseNode: BaseNode{ID: "n"}, Credential: "c", Text: "hello",
					Credentials: notifyTestCredentialStore{"c": {"url": url}}}
			},
			body: map[string]any{"ok": true},
		},
		{
			name: "dingtalk_sign_not_match_deterministic",
			nodeMaker: func(url string) webhookShellLike {
				return &DingtalkWebhook{BaseNode: BaseNode{ID: "n"}, Credential: "c", Text: "hello",
					Credentials: notifyTestCredentialStore{"c": {"url": url}}}
			},
			body:    map[string]any{"errcode": 310000, "errmsg": "sign not match"},
			wantErr: "310000",
		},
		{
			name: "dingtalk_rate_limited_transient",
			nodeMaker: func(url string) webhookShellLike {
				return &DingtalkWebhook{BaseNode: BaseNode{ID: "n"}, Credential: "c", Text: "hello",
					Credentials: notifyTestCredentialStore{"c": {"url": url}}}
			},
			body:      map[string]any{"errcode": 130101, "errmsg": "exceed rpm limit"},
			wantErr:   "130101",
			transient: true,
		},
		{
			name: "dingtalk_errcode_zero_success",
			nodeMaker: func(url string) webhookShellLike {
				return &DingtalkWebhook{BaseNode: BaseNode{ID: "n"}, Credential: "c", Text: "hello",
					Credentials: notifyTestCredentialStore{"c": {"url": url}}}
			},
			body: map[string]any{"errcode": 0, "errmsg": "ok"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var captured notifyTestCapture
			srv := notifyTestServer(t, 200, tc.body, &captured)
			node := tc.nodeMaker(srv.URL)
			res, err := node.Execute(notifyTestContext(t, nil, nil))
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("Execute: %v", err)
				}
				out := res.Result.(map[string]any)
				if out["status"] != 200 {
					t.Errorf("status = %v", out["status"])
				}
				return
			}
			if err == nil {
				t.Fatalf("business error body must fail, got %#v", res.Result)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error = %q, want substring %q", err.Error(), tc.wantErr)
			}
			var det *NotifyDeterministicError
			var tran *NotifyTransientError
			switch {
			case tc.transient && !errors.As(err, &tran):
				t.Errorf("error %T not transient: %v", err, err)
			case !tc.transient && !errors.As(err, &det):
				t.Errorf("error %T not deterministic: %v", err, err)
			}
		})
	}
}

// TestWebhookBusinessErrorScrubsEchoedURL mirrors
// test_business_error_message_scrubs_echoed_url: a 200 body echoing the
// webhook URL must not leak the token into the error text (A2R8-01).
func TestWebhookBusinessErrorScrubsEchoedURL(t *testing.T) {
	body := map[string]any{"code": 19003, "msg": "invalid token https://stub.invalid/hook?token=SECRET123"}
	srv := notifyTestServer(t, 200, body, nil)
	node := &FeishuWebhook{BaseNode: BaseNode{ID: "n"}, Credential: "c", Text: "hello",
		Credentials: notifyTestCredentialStore{"c": {"url": srv.URL}}}
	_, err := node.Execute(notifyTestContext(t, nil, nil))
	if err == nil {
		t.Fatal("must fail")
	}
	if strings.Contains(err.Error(), "SECRET123") {
		t.Errorf("error leaks token: %q", err.Error())
	}
}

// TestWebhookTransportErrorScrubsBotToken mirrors test_a2r8: transport
// failures carry the full URL in their text; the surfaced error keeps the
// host but masks path/query.
func TestWebhookTransportErrorScrubsBotToken(t *testing.T) {
	token := "FEISHU_HOOK_SECRET_9f3a"
	srv := notifyTestServer(t, 200, "ok", nil)
	url := srv.URL + "/open-apis/bot/v2/hook/" + token
	srv.Close() // connection refused from here on

	node := &FeishuWebhook{BaseNode: BaseNode{ID: "n"}, Credential: "c", Text: "hello",
		Credentials: notifyTestCredentialStore{"c": {"url": url}}}
	_, err := node.Execute(notifyTestContext(t, nil, nil))
	if err == nil {
		t.Fatal("must fail")
	}
	if strings.Contains(err.Error(), token) {
		t.Errorf("error leaks bot token: %q", err.Error())
	}
	if !strings.Contains(err.Error(), "127.0.0.1") {
		t.Errorf("error lost the host diagnostic: %q", err.Error())
	}
}

// TestWebhookShellDryRunMakesNoRequest mirrors the dry-run contract: no
// credential resolution, no request, output {"status": None, ...}.
func TestWebhookShellDryRunMakesNoRequest(t *testing.T) {
	hits := 0
	srv := notifyTestServer(t, 200, func(w http.ResponseWriter, r *http.Request) {
		hits++
	}, nil)
	store := notifyTestCredentialStore{"bot": {"url": srv.URL}}
	_ = store

	feishu := &FeishuWebhook{BaseNode: BaseNode{ID: "n"}, Credential: "bot", Text: "hello", DryRun: true}
	feishu.Credentials = store
	res, err := feishu.Execute(notifyTestContext(t, nil, nil))
	if err != nil {
		t.Fatalf("feishu dry: %v", err)
	}
	want := map[string]any{"status": nil, "response": "[dry-run] would post", "dry_run": true}
	if got := res.Result.(map[string]any); got["dry_run"] != true || got["status"] != nil {
		t.Errorf("feishu dry result = %#v", got)
	}
	_ = want

	wecom := &WecomWebhook{BaseNode: BaseNode{ID: "n"}, Credential: "bot", Text: "hello", Credentials: store}
	res, err = wecom.Execute(notifyTestContext(t, nil, map[string]any{"dry_run": true}))
	if err != nil {
		t.Fatalf("wecom dry: %v", err)
	}
	if got := res.Result.(map[string]any); got["response"] != "[dry-run] would post" || got["dry_run"] != true {
		t.Errorf("wecom global-dry result = %#v", got)
	}

	slack := &SlackWebhook{BaseNode: BaseNode{ID: "n"}, Credential: "bot", Text: "hello", DryRun: true}
	slack.Credentials = store
	if _, err = slack.Execute(notifyTestContext(t, nil, nil)); err != nil {
		t.Fatalf("slack dry: %v", err)
	}
	ding := &DingtalkWebhook{BaseNode: BaseNode{ID: "n"}, Credential: "bot", Text: "hello", Credentials: store}
	if _, err = ding.Execute(notifyTestContext(t, nil, map[string]any{"dry_run": true})); err != nil {
		t.Fatalf("dingtalk dry: %v", err)
	}
	if hits != 0 {
		t.Errorf("server hit %d times during dry-run", hits)
	}
}

// TestWebhookCredentialFailures: the deterministic config errors (missing
// credential field / unresolvable store / credential without url).
func TestWebhookCredentialFailures(t *testing.T) {
	feishuNoCred := &FeishuWebhook{BaseNode: BaseNode{ID: "n"}, Text: "x"}
	if _, err := feishuNoCred.Execute(notifyTestContext(t, nil, nil)); err == nil ||
		!strings.Contains(err.Error(), "缺少 credential 字段") {
		t.Errorf("missing credential err = %v", err)
	}

	feishuNoStore := &FeishuWebhook{BaseNode: BaseNode{ID: "n"}, Credential: "c", Text: "x"}
	if _, err := feishuNoStore.Execute(notifyTestContext(t, nil, nil)); err == nil ||
		!strings.Contains(err.Error(), "凭据存储") {
		t.Errorf("nil store err = %v", err)
	}

	feishuNoURL := &FeishuWebhook{BaseNode: BaseNode{ID: "n"}, Credential: "c", Text: "x",
		Credentials: notifyTestCredentialStore{"c": {"secret": "s"}}}
	if _, err := feishuNoURL.Execute(notifyTestContext(t, nil, nil)); err == nil ||
		!strings.Contains(err.Error(), "缺少 url 字段") {
		t.Errorf("missing url err = %v", err)
	}

	unknown := &WecomWebhook{BaseNode: BaseNode{ID: "n"}, Credential: "nope", Text: "x",
		Credentials: notifyTestCredentialStore{}}
	if _, err := unknown.Execute(notifyTestContext(t, nil, nil)); err == nil {
		t.Error("unknown credential must fail")
	}
}

// TestWecomSlackPayloadShapes pins the two remaining payload shapes directly.
func TestWecomSlackPayloadShapes(t *testing.T) {
	var wecomCap notifyTestCapture
	wecomSrv := notifyTestServer(t, 200, map[string]any{"errcode": 0}, &wecomCap)
	wecom := &WecomWebhook{BaseNode: BaseNode{ID: "n"}, Credential: "c", Text: "你好",
		Credentials: notifyTestCredentialStore{"c": {"url": wecomSrv.URL}}}
	if _, err := wecom.Execute(notifyTestContext(t, nil, nil)); err != nil {
		t.Fatalf("wecom: %v", err)
	}
	var wecomPayload map[string]any
	_ = json.Unmarshal(wecomCap.Body, &wecomPayload)
	wecomText, _ := wecomPayload["text"].(map[string]any)
	if wecomPayload["msgtype"] != "text" || wecomText["content"] != "你好" {
		t.Errorf("wecom payload = %s", wecomCap.Body)
	}

	var slackCap notifyTestCapture
	slackSrv := notifyTestServer(t, 200, "ok", &slackCap)
	slack := &SlackWebhook{BaseNode: BaseNode{ID: "n"}, Credential: "c", Text: "hello",
		Credentials: notifyTestCredentialStore{"c": {"url": slackSrv.URL}}}
	res, err := slack.Execute(notifyTestContext(t, nil, nil))
	if err != nil {
		t.Fatalf("slack: %v", err)
	}
	var slackPayload map[string]any
	_ = json.Unmarshal(slackCap.Body, &slackPayload)
	if len(slackPayload) != 1 || slackPayload["text"] != "hello" {
		t.Errorf("slack payload = %s", slackCap.Body)
	}
	// Slack 成功响应是纯文本 "ok"（非 JSON）——response 原样承载。
	if got := res.Result.(map[string]any)["response"]; got != "ok" {
		t.Errorf("slack response = %v", got)
	}
}
