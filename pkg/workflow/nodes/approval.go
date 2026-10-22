package nodes

import (
	"context"
	"fmt"

	"time"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// nodeTypeApproval mirrors plaita's ApprovalNode.node_type ("approval",
// plaita/server/nodes/approval_node.py).
const (
	nodeTypeApproval workflow.NodeType = "approval"

	// approvalDefaultSubscriptionTimeoutSeconds：无人审批的兜底终态——审批
	// 等待方是人，缺省 24h 落 timeout 状态而非永久 suspended（#56 兜底，
	// APPROVAL_DEFAULT_SUBSCRIPTION_TIMEOUT_SECONDS）。
	approvalDefaultSubscriptionTimeoutSeconds = 24 * 3600
)

func init() {
	workflow.RegisterNodeType(
		nodeTypeApproval,
		func(ctx context.Context) workflow.Node { return new(Approval) },
	)
}

// Approval 是挂起型人工审批节点：Execute 产出审批 service_config（标题/内容/
// 审批人列表的 $ 表达式解析），固定订阅 approval_decision 事件；审批记录的
// 创建与 any/all/majority 策略判定在消费 `{prefix}:approval:queue` 的外部
// approval service（console/控制面交互面），不在节点进程内。
type Approval struct {
	BaseNode
	extendedEventCore
	ApprovalTitle          string           `json:"approval_title"`
	ApprovalContent        string           `json:"approval_content"`
	ApprovalType           string           `json:"approval_type,omitempty"`
	Approvers              []any            `json:"approvers"`
	ApprovalStrategy       string           `json:"approval_strategy,omitempty"`
	AutoEscalation         bool             `json:"auto_escalation,omitempty"`
	EscalationTimeoutHours *int             `json:"escalation_timeout_hours,omitempty"`
	EscalationApprovers    []any            `json:"escalation_approvers,omitempty"`
	FormFields             []map[string]any `json:"form_fields,omitempty"`
	AllowComments          *bool            `json:"allow_comments,omitempty"`
	RequireComments        bool             `json:"require_comments,omitempty"`
	NotificationConfig     map[string]any   `json:"notification_config,omitempty"`
}

// GetType returns the approval node type.
func (a *Approval) GetType() workflow.NodeType { return nodeTypeApproval }

// IsSuspending declares the engine suspend capability.
func (a *Approval) IsSuspending() bool { return true }

// approvalContractEventType 是运行时契约：审批节点固定订阅 approval_decision
// （Python _normalize_enums 无条件覆盖，用户值不参与）。
const approvalContractEventType = "approval_decision"

// Execute resolves the approval config and returns the pending state with the
// approval service_config.
func (a *Approval) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	now := time.Now().UnixMilli()
	rc := runtime.WorkflowRuntimeContext(ctx)
	filter := nonNilFilter(a.EventFilter)

	resolvedApprovers := resolveListValues(ctx, a.Approvers)
	resolvedEscalation := resolveListValues(ctx, a.EscalationApprovers)
	resolvedFields := make([]any, 0, len(a.FormFields))
	for _, field := range a.FormFields {
		resolved := make(map[string]any, len(field))
		for key, value := range field {
			resolved[key] = resolveExtendedValue(ctx, value)
		}
		resolvedFields = append(resolvedFields, resolved)
	}
	resolvedNotification := make(map[string]any, len(a.NotificationConfig))
	for key, value := range a.NotificationConfig {
		resolvedNotification[key] = resolveExtendedValue(ctx, value)
	}

	allowComments := true
	if a.AllowComments != nil {
		allowComments = *a.AllowComments
	}

	serviceConfig := map[string]any{
		"type":         string(nodeTypeApproval),
		"node_id":      string(a.ID),
		"approval_id":  fmt.Sprintf("approval_%s_%d", a.ID, now),
		"execution_id": executionIDOf(rc),
		"flow_id":      flowIDOf(rc, a.ID),
		"event_type":   approvalContractEventType,
		"event_filter": filter,
		"approval_config": map[string]any{
			"title":        resolveExtendedValue(ctx, a.ApprovalTitle),
			"content":      resolveExtendedValue(ctx, a.ApprovalContent),
			"type":         orDefaultStr(a.ApprovalType, "manual"),
			"created_time": now,
		},
		"approver_config": map[string]any{
			"approvers":                resolvedApprovers,
			"strategy":                 orDefaultStr(a.ApprovalStrategy, "any"),
			"auto_escalation":          a.AutoEscalation,
			"escalation_timeout_hours": orDefault(a.EscalationTimeoutHours, 24),
			"escalation_approvers":     resolvedEscalation,
		},
		"form_config": map[string]any{
			"fields":           resolvedFields,
			"allow_comments":   allowComments,
			"require_comments": a.RequireComments,
		},
		"notification_config": resolvedNotification,
		"retry_config":        extendedRetryConfig(),
	}
	state := extendedPendingState(
		fmt.Sprintf("event_%s_%d", a.ID, now),
		approvalContractEventType, filter, serviceConfig, nodeTypeApproval)
	return &workflow.ExecuteResult{Result: state, NextNode: a.NextNode()}, nil
}

// SuspendSpec declares the wake-up subscription. 缺省 24h 订阅超时是节点语义
// 的一部分（ApprovalNode 覆盖父类 None 缺省；显式 null = 无限等待——部署
// JSON 的显式 null 在 ir translate 层折叠为 -1 哨兵，这里转回 0=不设超时）。
func (a *Approval) SuspendSpec(_ context.Context, _ *workflow.ExecuteResult) (*workflow.SuspendSpec, error) {
	timeout := coreTimeout(a.SubscriptionTimeout)
	switch {
	case a.SubscriptionTimeout == nil:
		timeout = approvalDefaultSubscriptionTimeoutSeconds
	case timeout < 0:
		timeout = 0
	}
	return &workflow.SuspendSpec{Subscription: &workflow.SubscriptionRequest{
		EventType:      approvalContractEventType,
		Filter:         a.EventFilter,
		NodeID:         string(a.ID),
		TimeoutSeconds: timeout,
	}}, nil
}

// Resume applies the wake-up carrier（EventNode.resume 多态分发）。
func (a *Approval) Resume(ctx context.Context, resumeType string, data map[string]any) (*workflow.ExecuteResult, error) {
	return extendedResume(ctx, nodeTypeApproval, approvalContractEventType, a.EventFilter, a.ID, a.NextNode(), resumeType, data)
}

// CanHandleEvent reports whether this node's filter accepts the event.
func (a *Approval) CanHandleEvent(eventType string, eventData map[string]any) bool {
	if eventType != approvalContractEventType {
		return false
	}
	return eventFilterMatches(a.EventFilter, eventData)
}

// resolveListValues 逐元素 $ 解析；解析结果是 list 时摊平（Python
// _resolve_approval_config 的 extend 语义）。
func resolveListValues(ctx context.Context, values []any) []any {
	resolved := make([]any, 0, len(values))
	for _, value := range values {
		r := resolveExtendedValue(ctx, value)
		if list, ok := r.([]any); ok {
			resolved = append(resolved, list...)
			continue
		}
		resolved = append(resolved, r)
	}
	return resolved
}
