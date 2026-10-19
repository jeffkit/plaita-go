package nodes

// Tests for the DingTalk webhook signing path. Semantic authority:
// plaita-nodes notify_backends._backend_dingtalk_webhook —
// ts = round(time*1000); sign = quote_plus(base64(hmac_sha256(secret,
// f"{ts}\n{secret}"))); appended as timestamp=&sign= (? or & separator).

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// notifyComputeDingtalkSign recomputes the expected signature.
func notifyComputeDingtalkSign(ts, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(ts + "\n" + secret))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func TestDingtalkWebhookWithoutSecret(t *testing.T) {
	var captured notifyTestCapture
	srv := notifyTestServer(t, 200, map[string]any{"errcode": 0, "errmsg": "ok"}, &captured)
	store := notifyTestCredentialStore{"c": {"url": srv.URL + "/robot/send?access_token=TOKEN123"}}
	node := &DingtalkWebhook{BaseNode: BaseNode{ID: "n"}, Credential: "c", Text: "hello", Credentials: store}
	if _, err := node.Execute(notifyTestContext(t, nil, nil)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if captured.Query.Get("timestamp") != "" || captured.Query.Get("sign") != "" {
		t.Errorf("no secret configured, got signed query %v", captured.Query)
	}
	var payload map[string]any
	_ = json.Unmarshal(captured.Body, &payload)
	text, _ := payload["text"].(map[string]any)
	if payload["msgtype"] != "text" || text["content"] != "hello" {
		t.Errorf("payload = %s", captured.Body)
	}
}

func TestDingtalkWebhookSignsWhenSecretConfigured(t *testing.T) {
	secret := "SEC9f3a_sign_secret"
	var captured notifyTestCapture
	srv := notifyTestServer(t, 200, map[string]any{"errcode": 0}, &captured)
	store := notifyTestCredentialStore{"c": {"url": srv.URL + "/robot/send", "secret": secret}}
	node := &DingtalkWebhook{BaseNode: BaseNode{ID: "n"}, Credential: "c", Text: "hello", Credentials: store}
	before := time.Now().UnixMilli()
	if _, err := node.Execute(notifyTestContext(t, nil, nil)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	after := time.Now().UnixMilli()

	ts := captured.Query.Get("timestamp")
	if ts == "" {
		t.Fatal("timestamp query param missing")
	}
	millis, err := strconv.ParseInt(ts, 10, 64)
	if err != nil || millis < before || millis > after {
		t.Errorf("timestamp = %q not a current millis value", ts)
	}
	// captured.Query values are already URL-decoded: sign must equal the raw
	// base64 exactly (quote_plus round-trip through query parsing).
	sign := captured.Query.Get("sign")
	if sign == "" {
		t.Fatal("sign query param missing")
	}
	if want := notifyComputeDingtalkSign(ts, secret); sign != want {
		t.Errorf("sign = %q, want %q", sign, want)
	}
}

func TestDingtalkWebhookSignSeparatorFollowsExistingQuery(t *testing.T) {
	secret := "SECabc"
	var captured notifyTestCapture
	srv := notifyTestServer(t, 200, map[string]any{"errcode": 0}, &captured)
	// URL already carries a query → separator must be "&", keeping the
	// original params (access_token) intact.
	store := notifyTestCredentialStore{"c": {"url": srv.URL + "/robot/send?access_token=TOKEN123", "secret": secret}}
	node := &DingtalkWebhook{BaseNode: BaseNode{ID: "n"}, Credential: "c", Text: "hello", Credentials: store}
	if _, err := node.Execute(notifyTestContext(t, nil, nil)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if captured.Query.Get("access_token") != "TOKEN123" {
		t.Errorf("original query lost: %v", captured.Query)
	}
	if captured.Query.Get("timestamp") == "" || captured.Query.Get("sign") == "" {
		t.Errorf("signature params missing: %v", captured.Query)
	}
}

func TestDingtalkWebhookMissingURLIsDeterministic(t *testing.T) {
	node := &DingtalkWebhook{BaseNode: BaseNode{ID: "n"}, Credential: "c", Text: "hello",
		Credentials: notifyTestCredentialStore{"c": {"secret": "SECabc"}}}
	_, err := node.Execute(notifyTestContext(t, nil, nil))
	if err == nil || !strings.Contains(err.Error(), "缺少 url 字段") {
		t.Fatalf("err = %v", err)
	}
	var det *NotifyDeterministicError
	if !errors.As(err, &det) {
		t.Errorf("error %T not deterministic", err)
	}
}

func TestDingtalkSignHelperMatchesPythonFormula(t *testing.T) {
	got := notifyDingtalkSign("https://oapi.dingtalk.com/robot/send", "SECk")
	u, err := url.Parse(got)
	if err != nil {
		t.Fatalf("signed url: %v", err)
	}
	q := u.Query()
	ts := q.Get("timestamp")
	if len(ts) != 13 {
		t.Errorf("timestamp = %q, want 13-digit millis", ts)
	}
	if sign := q.Get("sign"); sign != notifyComputeDingtalkSign(ts, "SECk") {
		t.Errorf("sign mismatch: %q", sign)
	}
	if !strings.HasPrefix(got, "https://oapi.dingtalk.com/robot/send?timestamp=") {
		t.Errorf("signed url = %q", got)
	}
}
