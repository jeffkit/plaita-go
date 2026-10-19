package nodes

// WecomWebhookNode — 企业微信群机器人（plaita-nodes webhooks.py 薄壳节点；
// 协议适配在 notify_backends._backend_wecom_webhook）。
// 凭据数据 {"url": "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=..."}。

import (
	"context"

	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// nodeTypeWecomWebhook is the wecom_webhook node type.
const nodeTypeWecomWebhook types.NodeType = "wecom_webhook"

func init() {
	workflow.RegisterNodeType(
		nodeTypeWecomWebhook,
		func(ctx context.Context) workflow.Node { return new(WecomWebhook) },
	)
}

// WecomWebhook posts a text message to a WeCom group bot.
type WecomWebhook struct {
	BaseNode
	Credential string `json:"credential,omitempty"`
	Text       any    `json:"text,omitempty"`
	DryRun     bool   `json:"dry_run,omitempty"`
	// Credentials resolves the credential name; wired by the host runtime.
	Credentials NotifyCredentialStore `json:"-"`
}

// GetType returns the wecom_webhook node type.
func (w *WecomWebhook) GetType() workflow.NodeType { return nodeTypeWecomWebhook }

// Execute mirrors _WebhookNode.execute: dry-run gate first, then text
// evaluation, then backend dispatch.
func (w *WecomWebhook) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	if w.DryRun || notifyGlobalDryRun(ctx) {
		return webhookResult(w, notifyDryRunResult()), nil
	}
	text, err := webhookText(ctx, w.Text)
	if err != nil {
		return nil, err
	}
	out, err := notifyWecomBackend(ctx, w.Credentials, w.Credential, text)
	if err != nil {
		return nil, webhookWrapErr(nodeTypeWecomWebhook, w.ID, err)
	}
	return webhookResult(w, out), nil
}

// notifyWecomBackend mirrors _backend_wecom_webhook: {"msgtype": "text",
// "text": {"content": ...}} with business-code checking (field "errcode").
func notifyWecomBackend(ctx context.Context, store NotifyCredentialStore, credential, text string) (map[string]any, error) {
	u, err := notifyCredentialURL(ctx, store, credential)
	if err != nil {
		return nil, err
	}
	return notifyPostJSON(ctx, u, map[string]any{
		"msgtype": "text",
		"text":    map[string]any{"content": text},
	}, "wecom_webhook")
}
