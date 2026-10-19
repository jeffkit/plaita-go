package nodes

// NotifyNode — 通知外发（channel 分发到内置 backend，对标 plaita-nodes
// notify.py + notify_backends 注册表）。channel 可为内置 "terminal" 或
// 本包支持的 backend（feishu_webhook 等）；Python 侧的
// register_notify_backend 扩展点是宿主注册表能力，引擎内置集合之外
// 的 channel 具名报错（文案对齐 get_notify_backend 的 ValueError）。

import (
	"context"
	"fmt"

	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// nodeTypeNotify is the notify node type ("notify" in plaita-nodes).
const nodeTypeNotify types.NodeType = "notify"

func init() {
	workflow.RegisterNodeType(
		nodeTypeNotify,
		func(ctx context.Context) workflow.Node { return new(Notify) },
	)
}

// Notify sends one notification through the configured channel.
// Python contract (notify.py): dry-run gate first (no evaluation, no
// backend call), then message evaluation, then backend dispatch.
type Notify struct {
	BaseNode
	Message any `json:"message,omitempty"` // 文本（支持表达式）
	// Channel: "terminal"（默认，打印到 stdout）或已注册通知 backend。
	Channel string `json:"channel,omitempty"`
	// Credential 凭据名（channel 需要时）。
	Credential string `json:"credential,omitempty"`
	DryRun     bool   `json:"dry_run,omitempty"`
	// Credentials resolves the credential name; wired by the host runtime.
	Credentials NotifyCredentialStore `json:"-"`
}

// GetType returns the notify node type.
func (n *Notify) GetType() workflow.NodeType { return nodeTypeNotify }

// Execute evaluates message and dispatches to the channel backend.
func (n *Notify) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	// dry-run 最先判：不解析凭据、不求值、不外发（pn#16）。
	if n.DryRun || notifyGlobalDryRun(ctx) {
		channel := n.Channel
		if channel == "" {
			channel = "terminal"
		}
		return notifyResult(n, map[string]any{
			"ok": true, "channel": channel, "dry_run": true,
		}), nil
	}

	text := ""
	if n.Message != nil {
		v, err := expression.Eval(ctx, n.Message)
		if err != nil {
			return nil, fmt.Errorf("notify %q: %w", n.ID, err)
		}
		text = notifyTextOf(v)
	}

	out, err := notifyDispatchChannel(ctx, n.Channel, n.Credential, n.Credentials, text, nil)
	if err != nil {
		return nil, fmt.Errorf("notify %q: %w", n.ID, err)
	}
	return notifyResult(n, out), nil
}

// notifyEmailFields carries the email-specific fields for the email_send
// backend. The notify node delegates with nil (Python reads the missing
// to/subject/body_is_html via getattr defaults), the email_send node passes
// its raw fields.
type notifyEmailFields struct {
	To         any // 收件人：字符串、逗号分隔或列表（支持表达式）
	Subject    any // 主题（支持表达式）
	BodyIsHTML bool
	SMTPSender notifySMTPSender // nil → real net/smtp delivery
}

// notifyDispatchChannel mirrors notify_backends.get_notify_backend dispatch
// for the built-in backends. Unknown channels raise a ValueError-shaped
// error listing the available set.
func notifyDispatchChannel(ctx context.Context, channel, credential string, store NotifyCredentialStore, text string, email *notifyEmailFields) (map[string]any, error) {
	switch channel {
	case "", "terminal":
		return notifyTerminalBackend(text), nil
	case "feishu_webhook":
		return notifyFeishuBackend(ctx, store, credential, text)
	case "wecom_webhook":
		return notifyWecomBackend(ctx, store, credential, text)
	case "slack_webhook":
		return notifySlackBackend(ctx, store, credential, text)
	case "dingtalk_webhook":
		return notifyDingtalkBackend(ctx, store, credential, text)
	case "email_send":
		if email == nil {
			email = &notifyEmailFields{}
		}
		return notifyEmailBackend(ctx, store, credential, email.To, email.Subject, email.BodyIsHTML, text, email.SMTPSender)
	default:
		return nil, fmt.Errorf(
			"未知通知 channel: %s（可用：dingtalk_webhook, email_send, feishu_webhook, slack_webhook, terminal, wecom_webhook；自定义渠道为 Python 注册表能力，引擎未内置）",
			channel)
	}
}

// notifyTerminalBackend mirrors notify_backends._backend_terminal.
func notifyTerminalBackend(text string) map[string]any {
	fmt.Printf("[notify] %s\n", text)
	return map[string]any{"ok": true, "channel": "terminal"}
}

func notifyResult(n *Notify, v any) *workflow.ExecuteResult {
	return &workflow.ExecuteResult{Result: v, NextNode: n.NextNode()}
}
