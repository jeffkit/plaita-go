package nodes

// FeishuWebhookNode — 飞书群机器人（plaita-nodes webhooks.py 薄壳节点；
// 协议适配在 notify_backends._backend_feishu_webhook）。
// 凭据数据 {"url": "https://open.feishu.cn/open-apis/bot/v2/hook/..."}。

import (
	"context"

	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// nodeTypeFeishuWebhook is the feishu_webhook node type.
const nodeTypeFeishuWebhook types.NodeType = "feishu_webhook"

func init() {
	workflow.RegisterNodeType(
		nodeTypeFeishuWebhook,
		func(ctx context.Context) workflow.Node { return new(FeishuWebhook) },
	)
}

// FeishuWebhook posts a text message to a Feishu group bot.
type FeishuWebhook struct {
	BaseNode
	Credential string `json:"credential,omitempty"`
	Text       any    `json:"text,omitempty"`
	DryRun     bool   `json:"dry_run,omitempty"`
	// Credentials resolves the credential name; wired by the host runtime.
	Credentials NotifyCredentialStore `json:"-"`
}

// GetType returns the feishu_webhook node type.
func (f *FeishuWebhook) GetType() workflow.NodeType { return nodeTypeFeishuWebhook }

// Execute mirrors _WebhookNode.execute: dry-run gate first (no credential
// resolution, no request), then text evaluation, then backend dispatch.
func (f *FeishuWebhook) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	if f.DryRun || notifyGlobalDryRun(ctx) {
		return webhookResult(f, notifyDryRunResult()), nil
	}
	text, err := webhookText(ctx, f.Text)
	if err != nil {
		return nil, err
	}
	out, err := notifyFeishuBackend(ctx, f.Credentials, f.Credential, text)
	if err != nil {
		return nil, webhookWrapErr(nodeTypeFeishuWebhook, f.ID, err)
	}
	return webhookResult(f, out), nil
}

// notifyFeishuBackend mirrors _backend_feishu_webhook: {"msg_type": "text",
// "content": {"text": ...}} with business-code checking (field "code").
func notifyFeishuBackend(ctx context.Context, store NotifyCredentialStore, credential, text string) (map[string]any, error) {
	u, err := notifyCredentialURL(ctx, store, credential)
	if err != nil {
		return nil, err
	}
	return notifyPostJSON(ctx, u, map[string]any{
		"msg_type": "text",
		"content":  map[string]any{"text": text},
	}, "feishu_webhook")
}
