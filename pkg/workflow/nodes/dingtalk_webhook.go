package nodes

// DingtalkWebhookNode — 钉钉群机器人（plaita-nodes webhooks.py 薄壳节点；
// 协议适配在 notify_backends._backend_dingtalk_webhook）。
// 凭据数据 {"url": "...", "secret": "<加签密钥，可选>"}。
// 配置 secret 时自动加签（timestamp+HMAC-SHA256，notifyDingtalkSign）。

import (
	"context"
	"fmt"

	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// nodeTypeDingtalkWebhook is the dingtalk_webhook node type.
const nodeTypeDingtalkWebhook types.NodeType = "dingtalk_webhook"

func init() {
	workflow.RegisterNodeType(
		nodeTypeDingtalkWebhook,
		func(ctx context.Context) workflow.Node { return new(DingtalkWebhook) },
	)
}

// DingtalkWebhook posts a text message to a DingTalk group bot.
type DingtalkWebhook struct {
	BaseNode
	Credential string `json:"credential,omitempty"`
	Text       any    `json:"text,omitempty"`
	DryRun     bool   `json:"dry_run,omitempty"`
	// Credentials resolves the credential name; wired by the host runtime.
	Credentials NotifyCredentialStore `json:"-"`
}

// GetType returns the dingtalk_webhook node type.
func (d *DingtalkWebhook) GetType() workflow.NodeType { return nodeTypeDingtalkWebhook }

// Execute mirrors _WebhookNode.execute: dry-run gate first, then text
// evaluation, then backend dispatch.
func (d *DingtalkWebhook) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	if d.DryRun || notifyGlobalDryRun(ctx) {
		return webhookResult(d, notifyDryRunResult()), nil
	}
	text, err := webhookText(ctx, d.Text)
	if err != nil {
		return nil, err
	}
	out, err := notifyDingtalkBackend(ctx, d.Credentials, d.Credential, text)
	if err != nil {
		return nil, webhookWrapErr(nodeTypeDingtalkWebhook, d.ID, err)
	}
	return webhookResult(d, out), nil
}

// notifyDingtalkBackend mirrors _backend_dingtalk_webhook: credential data
// is {"url", "secret"?}; a configured secret signs the URL before posting
// {"msgtype": "text", "text": {"content": ...}} (business code "errcode").
func notifyDingtalkBackend(ctx context.Context, store NotifyCredentialStore, credential, text string) (map[string]any, error) {
	cred, err := notifyCredentialData(ctx, store, credential)
	if err != nil {
		return nil, err
	}
	u := notifyCredentialString(cred, "url")
	if u == "" {
		return nil, webhookMissingField(credential, "url")
	}
	if secret := notifyCredentialString(cred, "secret"); secret != "" {
		u = notifyDingtalkSign(u, secret)
	}
	return notifyPostJSON(ctx, u, map[string]any{
		"msgtype": "text",
		"text":    map[string]any{"content": text},
	}, "dingtalk_webhook")
}

// webhookMissingField mirrors the `凭据 %r 缺少 url 字段` ValueError shape
// (确定性配置错误).
func webhookMissingField(credName, field string) error {
	return &NotifyDeterministicError{Msg: fmt.Sprintf("凭据 %q 缺少 %s 字段", credName, field)}
}
