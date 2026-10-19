package nodes

// SlackWebhookNode — Slack Incoming Webhook（plaita-nodes webhooks.py 薄壳
// 节点；协议适配在 notify_backends._backend_slack_webhook）。
// 凭据数据 {"url": "https://hooks.slack.com/services/..."}。
// 成功响应是纯文本 "ok"（非 JSON）；业务失败是 200 + {"ok": false, ...}。

import (
	"context"

	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// nodeTypeSlackWebhook is the slack_webhook node type.
const nodeTypeSlackWebhook types.NodeType = "slack_webhook"

func init() {
	workflow.RegisterNodeType(
		nodeTypeSlackWebhook,
		func(ctx context.Context) workflow.Node { return new(SlackWebhook) },
	)
}

// SlackWebhook posts a text message to a Slack Incoming Webhook.
type SlackWebhook struct {
	BaseNode
	Credential string `json:"credential,omitempty"`
	Text       any    `json:"text,omitempty"`
	DryRun     bool   `json:"dry_run,omitempty"`
	// Credentials resolves the credential name; wired by the host runtime.
	Credentials NotifyCredentialStore `json:"-"`
}

// GetType returns the slack_webhook node type.
func (s *SlackWebhook) GetType() workflow.NodeType { return nodeTypeSlackWebhook }

// Execute mirrors _WebhookNode.execute: dry-run gate first, then text
// evaluation, then backend dispatch.
func (s *SlackWebhook) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	if s.DryRun || notifyGlobalDryRun(ctx) {
		return webhookResult(s, notifyDryRunResult()), nil
	}
	text, err := webhookText(ctx, s.Text)
	if err != nil {
		return nil, err
	}
	out, err := notifySlackBackend(ctx, s.Credentials, s.Credential, text)
	if err != nil {
		return nil, webhookWrapErr(nodeTypeSlackWebhook, s.ID, err)
	}
	return webhookResult(s, out), nil
}

// notifySlackBackend mirrors _backend_slack_webhook: {"text": ...} with
// ok=false business-code checking.
func notifySlackBackend(ctx context.Context, store NotifyCredentialStore, credential, text string) (map[string]any, error) {
	u, err := notifyCredentialURL(ctx, store, credential)
	if err != nil {
		return nil, err
	}
	return notifyPostJSON(ctx, u, map[string]any{"text": text}, "slack_webhook")
}
