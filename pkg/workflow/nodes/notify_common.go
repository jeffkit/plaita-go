package nodes

// Shared machinery for the notify/outbound family: notify, feishu_webhook,
// wecom_webhook, slack_webhook, dingtalk_webhook, generic_webhook, email_send.
//
// Semantic authority is plaita-nodes (src/plaita_nodes/notify_backends.py,
// webhooks.py, api.py, email.py, _redact.py, _retry.py); every helper in this
// file cites the Python function it mirrors. All helpers carry the "notify"
// prefix — the whole family is owned by one change to avoid merge conflicts.
//
// Error classification (A2R7-04/A2R7-06 parity): the Python engine splits
// node failures into deterministic (ValueError family — never replayed by
// retryTimes, so a bad credential cannot spam the group chat) and transient
// (RuntimeError family — retried with backoff). The Go engine currently
// retries every error uniformly (workflow.Execute wraps all errors in
// RetryableError), so the distinction is carried in the error VALUES below;
// a future engine-side classifier can branch on them without touching nodes.

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// NotifyDeterministicError mirrors notify_backends.DeterministicRuntimeError /
// WebhookBusinessError: replaying the same request fails the same way
// (invalid credential, bad signature, deterministic 4xx), so it must NOT be
// amplified by retryTimes into duplicate group messages.
type NotifyDeterministicError struct{ Msg string }

// Error implements the error interface.
func (e *NotifyDeterministicError) Error() string { return e.Msg }

// NotifyTransientError mirrors WebhookTransientBusinessError and the plain
// RuntimeError paths (transport errors, 5xx, rate-limit business codes): a
// backoff retry may succeed.
type NotifyTransientError struct{ Msg string }

// Error implements the error interface.
func (e *NotifyTransientError) Error() string { return e.Msg }

// notifyErrorf builds a classified error: transient=false yields
// NotifyDeterministicError, transient=true yields NotifyTransientError.
func notifyErrorf(transient bool, format string, args ...any) error {
	msg := fmt.Sprintf(format, args...)
	if transient {
		return &NotifyTransientError{Msg: msg}
	}
	return &NotifyDeterministicError{Msg: msg}
}

// notifyIsRetryableHTTPStatus mirrors _retry.is_retryable_http_status:
// 5xx and 408/425/429 are transient; the remaining 4xx are deterministic.
func notifyIsRetryableHTTPStatus(status int) bool {
	return status >= 500 || status == 408 || status == 425 || status == 429
}

// NotifyCredentialStore resolves credential names into their data, mirroring
// plaita.credentials.get_credential (编排台「凭据」页创建的凭据). The host
// runtime wires an implementation into the node's Credentials field; without
// one, credential-dependent nodes fail closed at Execute time.
type NotifyCredentialStore interface {
	GetCredential(ctx context.Context, name string) (map[string]any, error)
}

// notifyCredentialData mirrors notify_backends._credential_data: the
// credential field is required and resolved by name.
func notifyCredentialData(ctx context.Context, store NotifyCredentialStore, credName string) (map[string]any, error) {
	if credName == "" {
		return nil, errors.New("缺少 credential 字段：请填凭据名（编排台「凭据」页创建）")
	}
	if store == nil {
		return nil, fmt.Errorf("凭据 %q 无法解析：引擎未配置凭据存储（NotifyCredentialStore）", credName)
	}
	return store.GetCredential(ctx, credName)
}

// notifyCredentialURL mirrors notify_backends._credential_url: the credential
// data must carry a non-empty "url".
func notifyCredentialURL(ctx context.Context, store NotifyCredentialStore, credName string) (string, error) {
	data, err := notifyCredentialData(ctx, store, credName)
	if err != nil {
		return "", err
	}
	u, _ := data["url"].(string)
	if u == "" {
		return "", fmt.Errorf("凭据 %q 缺少 url 字段", credName)
	}
	return u, nil
}

// notifyCredentialString reads a string field from credential data
// (getattr-with-default semantics for the {url, secret, host, ...} blobs).
func notifyCredentialString(data map[string]any, key string) string {
	s, _ := data[key].(string)
	return s
}

// notifyGlobalDryRun mirrors execution.get_global_variable("dry_run", False):
// the process-wide dry-run gate (pn#16). Checked before any evaluation or
// side effect.
func notifyGlobalDryRun(ctx context.Context) bool {
	return notifyIsTruthy(runtime.WorkflowRuntimeContext(ctx).GetGlobalVariable("dry_run"))
}

// ── Python scalar semantics (str() / truthiness) ────────────────────────

// notifyIsTruthy implements Python bool() semantics for evaluated values.
func notifyIsTruthy(v any) bool {
	switch t := v.(type) {
	case nil:
		return false
	case bool:
		return t
	case string:
		return t != ""
	case int:
		return t != 0
	case int32:
		return t != 0
	case int64:
		return t != 0
	case float64:
		return t != 0
	case json.Number:
		f, err := t.Float64()
		return err == nil && f != 0
	case []any:
		return len(t) > 0
	case map[string]any:
		return len(t) > 0
	default:
		return true
	}
}

// notifyTextOf mirrors `str(execution.evaluate(field) or "")`: falsy values
// (None, 0, False, "", empty containers) become "", truthy ones are
// stringified. Container values render as JSON — a deliberate divergence
// from Python's repr (single quotes) with the same information content.
func notifyTextOf(v any) string {
	if !notifyIsTruthy(v) {
		return ""
	}
	return notifyStringOf(v)
}

// notifyStringOf renders a value the way Python str() would for scalars;
// containers fall back to JSON.
func notifyStringOf(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case bool:
		if t {
			return "True"
		}
		return "False"
	case int:
		return strconv.Itoa(t)
	case int32:
		return strconv.FormatInt(int64(t), 10)
	case int64:
		return strconv.FormatInt(t, 10)
	case float64:
		return notifyFormatFloat(t)
	case json.Number:
		return t.String()
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(b)
	}
}

// notifyFormatFloat mirrors Python str(float): integral floats keep the
// ".0" suffix (str(2.0) == "2.0"); otherwise the shortest round-trip form.
func notifyFormatFloat(f float64) string {
	if !math.IsInf(f, 0) && !math.IsNaN(f) && f == math.Trunc(f) && math.Abs(f) < 1e16 {
		return strconv.FormatFloat(f, 'f', -1, 64) + ".0"
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// notifyTruncateRunes mirrors Python's s[:n] (rune-based prefix).
func notifyTruncateRunes(s string, n int) string {
	runes := []rune(s)
	if len(runes) <= n {
		return s
	}
	return string(runes[:n])
}

// ── URL / credential redaction (A2R8-01) ────────────────────────────────

const notifyUnprintable = "<unprintable>"

var (
	// notifyAbsoluteURLRe mirrors _redact._ABSOLUTE_URL_RE: token boundary is
	// ASCII whitespace only (Unicode spaces would truncate before credentials).
	notifyAbsoluteURLRe = regexp.MustCompile(`[A-Za-z][A-Za-z0-9+.\-]{0,31}://[^ \t\r\n\f\v]+`)
	// notifyRelativeURLRunRe mirrors _redact._RELATIVE_URL_RUN_RE: requests/
	// urllib3 report relative URLs after "with url:" / "with url=".
	notifyRelativeURLRunRe = regexp.MustCompile(`(?i)(\bwith url\s*[:=]\s*)(/[^\s"')\]]+)`)
	// notifyCredentialRunRe mirrors _redact._CREDENTIAL_RUN_RE: bare
	// password=…/token: … runs, keyed on credential names only.
	notifyCredentialRunRe = regexp.MustCompile(`(?i)\b(password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key|client[_-]?secret|private[_-]?key)\b(\s*[=:]\s*)("[^"]*"|'[^']*'|[^\s&,;]+)`)
)

// notifyRenderURL mirrors _redact._render_url: keep scheme://host[:port],
// drop userinfo, mask every non-empty path segment and the whole
// query/fragment. Non-http(s) schemes and unparseable tokens degrade to the
// safe constant.
func notifyRenderURL(token string) string {
	parts, err := url.Parse(token)
	if err != nil {
		return notifyUnprintable
	}
	if (parts.Scheme != "http" && parts.Scheme != "https") || parts.Host == "" {
		return notifyUnprintable
	}
	var b strings.Builder
	b.WriteString(parts.Scheme)
	b.WriteString("://")
	b.WriteString(parts.Host)
	for _, seg := range strings.Split(parts.Path, "/") {
		b.WriteString("/")
		if seg != "" {
			b.WriteString("***")
		}
	}
	if parts.RawQuery != "" {
		b.WriteString("?***")
	}
	if parts.Fragment != "" {
		b.WriteString("#***")
	}
	return b.String()
}

// notifyRenderRelativeURL mirrors _redact._render_relative_url: mask each
// path segment plus the query/fragment of a "with url: /a/b?token=…" run.
func notifyRenderRelativeURL(prefix, token string) string {
	path, sep, _ := strings.Cut(token, "?")
	if sep == "" {
		path, sep, _ = strings.Cut(path, "#")
	}
	var b strings.Builder
	for _, seg := range strings.Split(path, "/") {
		b.WriteString("/")
		if seg != "" {
			b.WriteString("***")
		}
	}
	if sep != "" {
		b.WriteString(sep)
		b.WriteString("***")
	}
	return prefix + b.String()
}

// notifyScrubText mirrors _redact._scrub_url_text — the single entry point
// for error text that may echo URLs/credentials (webhook tokens live in the
// path for Feishu/Slack, in the query for DingTalk). Total: never panics,
// never returns the input on failure paths.
func notifyScrubText(text string) string {
	out := notifyAbsoluteURLRe.ReplaceAllStringFunc(text, notifyRenderURL)
	out = notifyRelativeURLRunRe.ReplaceAllStringFunc(out, func(m string) string {
		groups := notifyRelativeURLRunRe.FindStringSubmatch(m)
		if len(groups) != 3 {
			return m
		}
		return notifyRenderRelativeURL(groups[1], groups[2])
	})
	out = notifyCredentialRunRe.ReplaceAllStringFunc(out, func(m string) string {
		groups := notifyCredentialRunRe.FindStringSubmatch(m)
		if len(groups) != 4 {
			return m
		}
		return groups[1] + groups[2] + "***"
	})
	if out == "" && text != "" {
		return notifyUnprintable
	}
	return out
}

// ── business-code classification (A2R7-06) ──────────────────────────────

// notifyBusinessCodeFields mirrors _BUSINESS_CODE_FIELDS: the per-channel
// "business success code" field. A missing field counts as compatible
// (test stubs / gateways reply {"ok": true} only).
func notifyBusinessCodeFields(channel string) string {
	switch channel {
	case "feishu_webhook":
		return "code"
	case "wecom_webhook", "dingtalk_webhook":
		return "errcode"
	default:
		return ""
	}
}

// notifyRetryableBusinessCodes mirrors _RETRYABLE_BUSINESS_CODES: the
// official rate-limit families (Feishu 9499/11232, WeCom 45009/45033,
// DingTalk 130101). Slack's rate_limited lives in the error field instead.
var notifyRetryableBusinessCodes = map[string]map[int64]bool{
	"feishu_webhook":   {9499: true, 11232: true},
	"wecom_webhook":    {45009: true, 45033: true},
	"dingtalk_webhook": {130101: true},
}

// notifyAsPythonInt mirrors Python int(raw): integral floats, bools and
// numeric strings convert; everything else is reported as inexact.
func notifyAsPythonInt(v any) (code int64, exact bool) {
	switch t := v.(type) {
	case bool:
		if t {
			return 1, true
		}
		return 0, true
	case int:
		return int64(t), true
	case int32:
		return int64(t), true
	case int64:
		return t, true
	case float64:
		if t == math.Trunc(t) && !math.IsInf(t, 0) && !math.IsNaN(t) {
			return int64(t), true
		}
		return 0, false
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i, true
		}
		return 0, false
	case string:
		if i, err := strconv.ParseInt(strings.TrimSpace(t), 10, 64); err == nil {
			return i, true
		}
		return 0, false
	default:
		return 0, false
	}
}

// notifyBusinessError mirrors _business_error: a 200 + business-error body
// becomes a named failure; rate-limit codes stay retryable, everything else
// is deterministic (never replay group messages). The message may echo the
// URL/credential, so it goes through notifyScrubText.
func notifyBusinessError(channel string, code, message any, retryable bool) error {
	detail := notifyScrubText(fmt.Sprintf("%s webhook 业务错误 code=%s: %s",
		channel, notifyStringOf(code), notifyTruncateRunes(notifyStringOf(message), 200)))
	return notifyErrorf(retryable, "%s", detail)
}

// notifyCheckBusinessCode mirrors _check_business_code: parse the 200
// response body's business code per channel; a missing field is treated as
// compatible (success).
func notifyCheckBusinessCode(channel string, body any) error {
	obj, ok := body.(map[string]any)
	if !ok {
		return nil
	}
	if channel == "slack_webhook" {
		if b, isBool := obj["ok"].(bool); isBool && !b {
			errName, named := obj["error"].(string)
			if !named || errName == "" {
				errName = "ok=false"
			}
			var message any = obj
			if e, exists := obj["error"]; exists && notifyIsTruthy(e) {
				message = e
			}
			return notifyBusinessError(channel, errName, message,
				errName == "rate_limited" || errName == "ratelimited")
		}
		return nil
	}
	field := notifyBusinessCodeFields(channel)
	if field == "" {
		return nil
	}
	raw, exists := obj[field]
	if !exists {
		return nil
	}
	code, exact := notifyAsPythonInt(raw)
	if exact && code == 0 {
		return nil
	}
	var message any = obj
	if m, exists := obj["msg"]; exists && notifyIsTruthy(m) {
		message = m
	} else if m, exists := obj["errmsg"]; exists && notifyIsTruthy(m) {
		message = m
	}
	return notifyBusinessError(channel, raw, message,
		exact && notifyRetryableBusinessCodes[channel][code])
}

// ── shared POST JSON ─────────────────────────────────────────────────────

// notifyPostJSON mirrors notify_backends._post_json: POST the payload as
// JSON with a 15s budget, parse the response, classify failures, and return
// {"status", "response"}.
func notifyPostJSON(ctx context.Context, rawURL string, payload map[string]any, channel string) (map[string]any, error) {
	client := &http.Client{Timeout: 15 * time.Second}
	var reqBody io.Reader
	if payload != nil {
		buf, err := json.Marshal(payload)
		if err != nil {
			return nil, notifyErrorf(false, "webhook payload 序列化失败: %v", err)
		}
		reqBody = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rawURL, reqBody)
	if err != nil {
		return nil, &NotifyTransientError{Msg: notifyScrubText(err.Error())}
	}
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		// Transport error text carries the full URL (bot token in path/query);
		// scrub to scheme://host before it can reach state.error (A2R8-01).
		return nil, &NotifyTransientError{Msg: notifyScrubText(err.Error())}
	}
	defer resp.Body.Close()
	raw, err := readCappedBody(resp.Body, maxResponseBytes())
	if err != nil {
		if errors.As(err, &errResponseTooLarge{}) {
			// Over-limit is deterministic: a retry re-downloads the same
			// oversized body. Same class as non-retryable HTTP 4xx.
			return nil, &NotifyDeterministicError{Msg: notifyScrubText(err.Error())}
		}
		return nil, &NotifyTransientError{Msg: notifyScrubText(err.Error())}
	}
	var body any
	if err := json.Unmarshal(raw, &body); err != nil {
		body = notifyTruncateRunes(string(raw), 200)
	}
	if resp.StatusCode != http.StatusOK {
		detail := notifyScrubText(fmt.Sprintf("webhook 调用失败 HTTP %d: %s",
			resp.StatusCode, notifyTruncateRunes(notifyStringOf(body), 200)))
		if notifyIsRetryableHTTPStatus(resp.StatusCode) {
			return nil, &NotifyTransientError{Msg: detail}
		}
		return nil, &NotifyDeterministicError{Msg: detail}
	}
	if err := notifyCheckBusinessCode(channel, body); err != nil {
		return nil, err
	}
	return map[string]any{"status": resp.StatusCode, "response": body}, nil
}

// notifyDryRunResult mirrors the _WebhookNode dry-run output:
// {"status": None, "response": "[dry-run] would post", "dry_run": True}.
func notifyDryRunResult() map[string]any {
	return map[string]any{"status": nil, "response": "[dry-run] would post", "dry_run": true}
}

// notifyDingtalkSign mirrors _backend_dingtalk_webhook's signing block:
// timestamp + HMAC-SHA256 over "ts\nsecret", base64-encoded, then
// quote_plus-ed and appended as timestamp=/sign= query params.
func notifyDingtalkSign(rawURL, secret string) string {
	ts := strconv.FormatInt(time.Now().UnixMilli(), 10)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(ts + "\n" + secret))
	sign := url.QueryEscape(base64.StdEncoding.EncodeToString(mac.Sum(nil)))
	sep := "?"
	if strings.Contains(rawURL, "?") {
		sep = "&"
	}
	return rawURL + sep + "timestamp=" + ts + "&sign=" + sign
}

// notifyBoundary mints a MIME boundary for multipart/alternative.
func notifyBoundary() string {
	buf := make([]byte, 12)
	if _, err := rand.Read(buf); err != nil {
		return "plaita-alt-boundary"
	}
	return fmt.Sprintf("============_plaita_alt_%s==", base64.RawURLEncoding.EncodeToString(buf))
}

// ── thin-shell webhook node helpers (webhooks.py _WebhookNode) ───────────

// webhookText mirrors `str(execution.evaluate(self.text) or "")` in
// _WebhookNode.execute.
func webhookText(ctx context.Context, raw any) (string, error) {
	if raw == nil {
		return "", nil
	}
	v, err := expression.Eval(ctx, raw)
	if err != nil {
		return "", err
	}
	return notifyTextOf(v), nil
}

// webhookResult wraps a backend output dict as an ExecuteResult.
func webhookResult(n interface{ NextNode() workflow.NodeID }, out map[string]any) *workflow.ExecuteResult {
	return &workflow.ExecuteResult{Result: out, NextNode: n.NextNode()}
}

// webhookWrapErr tags a backend failure with the node type/id, mirroring the
// Python engine's NodeExecutionError framing.
func webhookWrapErr(nodeType types.NodeType, id types.NodeID, err error) error {
	return fmt.Errorf("%s %q: %w", nodeType, id, err)
}
