package nodes

// GenericWebhookNode — 通用 Webhook：把任意 JSON payload POST 到凭据指定的
// URL（plaita-nodes api.py GenericWebhookNode）。凭据数据 {"url": "..."}；
// payload 字段为表达式求值后的 JSON 请求体。只判 HTTP 状态码（>=400 失败），
// 不做 IM 业务码检查；3xx 视为成功（Python requests 默认不跟随重定向，
// 3xx 原样返回）。

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// nodeTypeGenericWebhook is the generic_webhook node type.
const nodeTypeGenericWebhook types.NodeType = "generic_webhook"

func init() {
	workflow.RegisterNodeType(
		nodeTypeGenericWebhook,
		func(ctx context.Context) workflow.Node { return new(GenericWebhook) },
	)
}

// defaultTimeoutMs mirrors GenericWebhookNode.timeout_ms (15000).
const defaultTimeoutMs = 15000

// GenericWebhook posts an arbitrary JSON payload to the credential's URL.
type GenericWebhook struct {
	BaseNode
	Credential string `json:"credential,omitempty"`
	// Payload 表达式求值后的 JSON 请求体（None → 不发请求体，对齐
	// requests json=None 的行为）。
	Payload any `json:"payload,omitempty"`
	// TimeoutMs 毫秒预算；Python 侧 positive_millis 在构造期拒绝 0/负值，
	// 这里在 Validate 具名拒绝（默认 15000）。
	TimeoutMs *int `json:"timeout_ms,omitempty"`
	DryRun    bool `json:"dry_run,omitempty"`
	// Credentials resolves the credential name; wired by the host runtime.
	Credentials NotifyCredentialStore `json:"-"`
}

// GetType returns the generic_webhook node type.
func (g *GenericWebhook) GetType() workflow.NodeType { return nodeTypeGenericWebhook }

// Validate mirrors the pydantic _validate_timeout_ms fail-closed check
// (A2R7-05): 0/负值不表达任何合理的预算，具名拒绝。
func (g *GenericWebhook) Validate() error {
	if g.TimeoutMs != nil && *g.TimeoutMs <= 0 {
		return fmt.Errorf(
			"generic_webhook %q: timeout_ms must be a positive number of milliseconds (got %d); 0 and negative values are rejected (fail-closed)",
			g.ID, *g.TimeoutMs)
	}
	return nil
}

// Execute mirrors GenericWebhookNode.execute: dry-run gate first (no
// credential resolution, no request), then payload evaluation, then POST.
func (g *GenericWebhook) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	// dry-run 最先判：不解析凭据、不发请求。
	if g.DryRun || notifyGlobalDryRun(ctx) {
		return webhookResult(g, notifyDryRunResult()), nil
	}
	if g.Credential == "" {
		return nil, fmt.Errorf("generic_webhook %q: 缺少 credential 字段：请填凭据名（编排台「凭据」页创建）", g.ID)
	}
	u, err := notifyCredentialURL(ctx, g.Credentials, g.Credential)
	if err != nil {
		return nil, webhookWrapErr(nodeTypeGenericWebhook, g.ID, err)
	}

	var body any
	if g.Payload != nil {
		body, err = expression.Eval(ctx, g.Payload)
		if err != nil {
			return nil, fmt.Errorf("generic_webhook %q: %w", g.ID, err)
		}
	}

	timeoutMs := defaultTimeoutMs
	if g.TimeoutMs != nil {
		timeoutMs = *g.TimeoutMs
	}
	out, err := notifyGenericPost(ctx, u, body, time.Duration(timeoutMs)*time.Millisecond)
	if err != nil {
		return nil, webhookWrapErr(nodeTypeGenericWebhook, g.ID, err)
	}
	return webhookResult(g, out), nil
}

// notifyGenericPost mirrors the request half of GenericWebhookNode.execute:
// POST JSON (nil body → no body at all, like requests json=None), fail on
// HTTP >= 400 (4xx deterministic except 408/425/429, 5xx transient), parse
// the response as JSON with a truncated-text fallback.
func notifyGenericPost(ctx context.Context, rawURL string, body any, timeout time.Duration) (map[string]any, error) {
	client := &http.Client{Timeout: timeout}
	var reqBody io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return nil, &NotifyDeterministicError{Msg: fmt.Sprintf("webhook payload 序列化失败: %v", err)}
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
		// 传输异常文本带完整 URL（凭据 query 里可能有 token）；清洗后抛出。
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
	if resp.StatusCode >= 400 {
		detail := notifyScrubText(fmt.Sprintf("webhook 调用失败 HTTP %d: %s",
			resp.StatusCode, notifyTruncateRunes(string(raw), 200)))
		if notifyIsRetryableHTTPStatus(resp.StatusCode) {
			return nil, &NotifyTransientError{Msg: detail}
		}
		return nil, &NotifyDeterministicError{Msg: detail}
	}
	var data any
	if err := json.Unmarshal(raw, &data); err != nil {
		data = notifyTruncateRunes(string(raw), 200)
	}
	return map[string]any{"status": resp.StatusCode, "response": data}, nil
}
