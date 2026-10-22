package nodes

import (
	"context"
	"fmt"
	"strings"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// #80 phase 3：server 扩展节点共享件。
//
// 权威语义 = plaita/server/nodes/base_extended_node.py（BaseExtendedNode）+
// plaita/node/event_node.py（EventNode 基类）。五节点（delay/approval/
// redis_queue/kafka_queue/http_callback）全部是挂起型：Execute 产出
// service_config + pending 状态，由外部 service 消费
// `{prefix}:{subtype}:queue` 后发布恢复事件（worker eventbus 唤醒）。
//
// 挂起/恢复状态形状（Python 实测钉死，2026-10-22 probe）：
//   - Execute 结果 = {event_type, event_filter, is_async:true, event_id,
//     status:"pending", service_config, node_subtype}（+挂起边界的
//     subscription_id/subscription_tenant，引擎写入）；
//   - 恢复后状态 = 挂起态全量保留（service_config/node_subtype/event_id/
//     subscription_id）+ status 切换 + event_data（EventNode.on_event 的
//     _get_node_state → 更新 → _create_result 语义）。
//
// extendedEventCore 刻意不带任何方法：作为匿名字段嵌入时不会把 Event 的
// UnmarshalJSON 提升到外层节点（那会吞掉节点自身字段的解码）。
type extendedEventCore struct {
	EventType           string         `json:"event_type"`
	EventFilter         map[string]any `json:"event_filter,omitempty"`
	SubscriptionTimeout *float64       `json:"subscription_timeout,omitempty"`
}

// extendedRetryConfig 是 BaseExtendedNode.get_default_retry_config 的固定形状。
func extendedRetryConfig() map[string]any {
	return map[string]any{
		"max_retries":         3,
		"retry_delay_ms":      1000,
		"exponential_backoff": true,
	}
}

// coreTimeout dereferences the optional subscription timeout (0 = 无限等待).
func coreTimeout(t *float64) float64 {
	if t == nil {
		return 0
	}
	return *t
}

// orDefaultStr 是 Literal 枚举字段的节点层防御缺省：加载期 translate 已按
// pydantic 缺省填充，直构流（绕过 IR）的空串回落同款缺省——post-load 形状
// 一致（空串本身是 Literal 拒绝值，不会与合法配置冲突）。
func orDefaultStr(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// nonNilFilter 是 pydantic default_factory=dict：字段缺省时是空对象而非
// null（节点状态/服务配置里的 event_filter 形状对齐）。
func nonNilFilter(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	return m
}

// executionIDOf reads the host-injected execution id（service_config 的
// execution_id 字段；裸引擎跑是引擎铸造的 uuid，与 Python 的 execution_id
// 同为易变值，golden 对拍在归一层抹平）。
func executionIDOf(rc *runtime.Context) string {
	if rc == nil {
		return ""
	}
	return rc.GetExecutionID()
}

// flowIDOf reads the effective flow id（plaita 的 execution.state.flow_id；
// 回落链与引擎 prepareExecutionRuntimeContext 一致：上下文继承 → 流程 id）。
func flowIDOf(rc *runtime.Context, fallback workflow.NodeID) string {
	if rc != nil {
		if id := rc.GetFlowID(); id != "" {
			return id
		}
	}
	return string(fallback)
}

// resolveEventExpression mirrors EventNode.execute 的 event_type 解析：`$`
// 表达式求值，非字符串/空结果/求值失败保留原值。
func resolveEventExpression(ctx context.Context, raw string) string {
	if !strings.HasPrefix(raw, "$") {
		return raw
	}
	resolved, err := expression.Eval(ctx, raw)
	if err != nil {
		return raw
	}
	if s, ok := resolved.(string); ok && s != "" {
		return s
	}
	return raw
}

// pyHasValue 是 Python 真值在 Optional[str] 字段上的口径：nil 与空串都算
// falsy（`if self.callback_path:` / `if self.auth_token:`）——显式空串触发
// 自动生成，不是透传。
func pyHasValue(v any) bool {
	if v == nil {
		return false
	}
	if s, ok := v.(string); ok && s == "" {
		return false
	}
	return true
}

// extendedPendingState builds the EventNode.execute pending result plus the
// BaseExtendedNode merge (service_config + node_subtype).
func extendedPendingState(eventID, eventType string, filter map[string]any, serviceConfig map[string]any, subtype workflow.NodeType) map[string]any {
	return map[string]any{
		"event_id":       eventID,
		"status":         EventStatusPending,
		"event_type":     eventType,
		"event_filter":   filter,
		"is_async":       true,
		"service_config": serviceConfig,
		"node_subtype":   string(subtype),
	}
}

// resolveExtendedValue 是 approval/redis_queue/kafka_queue/http_callback 共享
// 的 $ 表达式解析（各 *_node.py _resolve_value）：`$` 开头走求值——结果
// None（nil）或求值失败时返回**原值**。delay 的时长解析是另一套语义
// （解析失败回退 60 秒），不共用。
func resolveExtendedValue(ctx context.Context, value any) any {
	s, ok := value.(string)
	if !ok || !strings.HasPrefix(s, "$") {
		return value
	}
	resolved, err := expression.Eval(ctx, s)
	if err != nil || resolved == nil {
		return value
	}
	return resolved
}

// extendedSuspendSpec builds the shared wake-up subscription from the node's
// event fields（EventNode 契约：event_type + event_filter +
// subscription_timeout；correlation/flow/tenant 由引擎边界补齐）。
func extendedSuspendSpec(core *extendedEventCore, nodeID workflow.NodeID) *workflow.SuspendSpec {
	var timeout float64
	if core.SubscriptionTimeout != nil {
		timeout = *core.SubscriptionTimeout
	}
	return &workflow.SuspendSpec{Subscription: &workflow.SubscriptionRequest{
		EventType:      core.EventType,
		Filter:         core.EventFilter,
		NodeID:         string(nodeID),
		TimeoutSeconds: timeout,
	}}
}

// extendedResume implements the EventNode.resume polymorphic dispatch for the
// extended family. The suspend-time state is preserved whole (plaita on_event
// rebuilds from the persisted node state): service_config, node_subtype,
// event_id and subscription_id all survive the wake-up; only status flips and
// event_data lands.
func extendedResume(ctx context.Context, subtype workflow.NodeType, eventType string, filter map[string]any, nodeID workflow.NodeID, next workflow.NodeID, resumeType string, data map[string]any) (*workflow.ExecuteResult, error) {
	state := map[string]any{
		"event_type":   eventType,
		"event_filter": filter,
		"is_async":     true,
	}
	// 挂起态字段从 $NODE 结果表回填（Workflow.Resume 在调用节点 Resume 前
	// 已恢复快照；引擎已校验 pending 状态与订阅绑定）。
	if rc := runtime.WorkflowRuntimeContext(ctx); rc != nil && rc.ExecuteNodeResults != nil {
		if prev, ok := rc.ExecuteNodeResults[string(nodeID)].(map[string]any); ok {
			for _, key := range []string{"event_id", "service_config", "node_subtype", "subscription_id", "subscription_tenant"} {
				if v, ok := prev[key]; ok {
					state[key] = v
				}
			}
		}
	}
	switch resumeType {
	case workflow.ResumeCancel:
		state["status"] = EventStatusCancelled
	case workflow.ResumeTimeout:
		state["status"] = EventStatusTimeout
	case workflow.ResumeEvent, "":
		state["status"] = EventStatusCompleted
		state["event_data"] = data
	default:
		return nil, &workflow.ResumeGuardError{Message: fmt.Sprintf("unsupported resume type %q for %s node", resumeType, subtype)}
	}
	return &workflow.ExecuteResult{Result: state, NextNode: next}, nil
}
