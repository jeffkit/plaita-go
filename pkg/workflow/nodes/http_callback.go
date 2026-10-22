package nodes

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// nodeTypeHTTPCallback mirrors plaita's HttpCallbackNode.node_type
// ("http_callback", plaita/server/nodes/http_callback_node.py)。
const nodeTypeHTTPCallback workflow.NodeType = "http_callback"

// httpCallbackContractEventType：固定订阅 http_callback（Python
// _normalize_enums 无条件覆盖，用户值不参与）。
const httpCallbackContractEventType = "http_callback"

// httpCallbackBaseURL 是 Python _generate_full_callback_url 的硬编码 base
// （「实际使用中应该从环境变量或配置文件获取」——仓内即此字面值）。
const httpCallbackBaseURL = "http://localhost:8080"

func init() {
	workflow.RegisterNodeType(
		nodeTypeHTTPCallback,
		func(ctx context.Context) workflow.Node { return new(HTTPCallback) },
	)
}

// HTTPCallback 是挂起型 HTTP 回调节点：Execute 生成/解析回调路径 + 认证配置，
// 产出注册用 service_config；由外部 ingress 把回调请求交给
// handle_callback_request 同族入口后发布 http_callback 事件唤醒流程。节点
// 自身不监听端口。
type HTTPCallback struct {
	BaseNode
	extendedEventCore
	CallbackPath           any            `json:"callback_path,omitempty"`
	CallbackMethod         string         `json:"callback_method,omitempty"`
	CallbackTimeoutMinutes *int           `json:"callback_timeout_minutes,omitempty"`
	RequireAuth            *bool          `json:"require_auth,omitempty"`
	AuthType               string         `json:"auth_type,omitempty"`
	AuthToken              any            `json:"auth_token,omitempty"`
	ValidateHeaders        map[string]any `json:"validate_headers,omitempty"`
	ValidateParams         map[string]any `json:"validate_params,omitempty"`
	SuccessResponse        map[string]any `json:"success_response,omitempty"`
	ErrorResponse          map[string]any `json:"error_response,omitempty"`
}

// GetType returns the http_callback node type.
func (h *HTTPCallback) GetType() workflow.NodeType { return nodeTypeHTTPCallback }

// IsSuspending declares the engine suspend capability.
func (h *HTTPCallback) IsSuspending() bool { return true }

// Execute builds the registration service_config and returns the pending state.
func (h *HTTPCallback) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	now := time.Now().UnixMilli()
	rc := runtime.WorkflowRuntimeContext(ctx)
	filter := nonNilFilter(h.EventFilter)

	path := h.callbackPath(ctx, rc)
	pathStr, _ := path.(string)
	method := strings.ToUpper(h.CallbackMethod)
	if method == "" {
		method = "POST"
	}

	serviceConfig := map[string]any{
		"type":         string(nodeTypeHTTPCallback),
		"node_id":      string(h.ID),
		"execution_id": executionIDOf(rc),
		"flow_id":      flowIDOf(rc, h.ID),
		"event_type":   httpCallbackContractEventType,
		"event_filter": filter,
		"callback_config": map[string]any{
			"path":            path,
			"method":          method,
			"timeout_minutes": orDefault(h.CallbackTimeoutMinutes, 60),
			// full_url 与 path 同源（Python f"{base}{path}"；path 理论上可被
			// $ 解析成非串，那时两处一致地落原值）。
			"full_url": httpCallbackBaseURL + pathStr,
		},
		"auth_config": h.authConfig(ctx),
		"validation_config": map[string]any{
			"headers": resolveMapValues(ctx, h.ValidateHeaders),
			"params":  resolveMapValues(ctx, h.ValidateParams),
		},
		"response_config": map[string]any{
			"success_response": orDefaultMap(h.SuccessResponse, map[string]any{"status": "success"}),
			"error_response":   orDefaultMap(h.ErrorResponse, map[string]any{"status": "error"}),
		},
		"retry_config": extendedRetryConfig(),
	}
	state := extendedPendingState(
		fmt.Sprintf("event_%s_%d", h.ID, now),
		httpCallbackContractEventType, filter, serviceConfig, nodeTypeHTTPCallback)
	return &workflow.ExecuteResult{Result: state, NextNode: h.NextNode()}, nil
}

// callbackPath 是 Python _generate_callback_path：真值路径（`if
// self.callback_path:`——空串 falsy）走 $ 解析；缺省/空串自动生成
// `/callback/{flow_id}/{node_id}/{execution_id}`（execution_id 缺省时现场铸
// uuid hex，flow_id 缺省字面 "unknown"）。
func (h *HTTPCallback) callbackPath(ctx context.Context, rc *runtime.Context) any {
	if pyHasValue(h.CallbackPath) {
		return resolveExtendedValue(ctx, h.CallbackPath)
	}
	executionID := executionIDOf(rc)
	if executionID == "" {
		executionID = randomHex32()
	}
	flowID := flowIDOf(rc, "")
	if flowID == "" {
		flowID = "unknown"
	}
	return fmt.Sprintf("/callback/%s/%s/%s", flowID, h.ID, executionID)
}

// authConfig 是 Python _generate_auth_config：require_auth=false →
// {"enabled":false}；token/basic/signature 三型各按字段生成，凭据缺省现场铸
// uuid hex。
func (h *HTTPCallback) authConfig(ctx context.Context) map[string]any {
	requireAuth := true
	if h.RequireAuth != nil {
		requireAuth = *h.RequireAuth
	}
	if !requireAuth {
		return map[string]any{"enabled": false}
	}
	config := map[string]any{
		"enabled": true,
		"type":    orDefaultStr(h.AuthType, "token"),
	}
	switch h.AuthType {
	case "basic":
		// username 用显式 auth_token（真值判伪：nil/空串都走生成缺省——Python
		// `if self.auth_token` 分支），password 现场生成。
		if pyHasValue(h.AuthToken) {
			config["username"] = resolveExtendedValue(ctx, h.AuthToken)
		}
		config["password"] = randomHex32()
	case "signature":
		secret := any(randomHex32())
		if pyHasValue(h.AuthToken) {
			secret = resolveExtendedValue(ctx, h.AuthToken)
		}
		config["secret"] = secret
		config["algorithm"] = "HMAC-SHA256"
	default: // "token"（加载期 Literal 已拦未知类型，直构流兜底同 token）
		token := any(randomHex32())
		if pyHasValue(h.AuthToken) {
			token = resolveExtendedValue(ctx, h.AuthToken)
		}
		config["token"] = token
	}
	return config
}

// SuspendSpec declares the wake-up subscription.
func (h *HTTPCallback) SuspendSpec(_ context.Context, _ *workflow.ExecuteResult) (*workflow.SuspendSpec, error) {
	return &workflow.SuspendSpec{Subscription: &workflow.SubscriptionRequest{
		EventType:      httpCallbackContractEventType,
		Filter:         h.EventFilter,
		NodeID:         string(h.ID),
		TimeoutSeconds: coreTimeout(h.SubscriptionTimeout),
	}}, nil
}

// Resume applies the wake-up carrier（EventNode.resume 多态分发）。
func (h *HTTPCallback) Resume(ctx context.Context, resumeType string, data map[string]any) (*workflow.ExecuteResult, error) {
	return extendedResume(ctx, nodeTypeHTTPCallback, httpCallbackContractEventType, h.EventFilter, h.ID, h.NextNode(), resumeType, data)
}

// CanHandleEvent reports whether this node's filter accepts the event.
func (h *HTTPCallback) CanHandleEvent(eventType string, eventData map[string]any) bool {
	if eventType != httpCallbackContractEventType {
		return false
	}
	return eventFilterMatches(h.EventFilter, eventData)
}

// resolveMapValues 逐值 $ 解析（Python _resolve_validation_config）。
func resolveMapValues(ctx context.Context, m map[string]any) map[string]any {
	resolved := make(map[string]any, len(m))
	for key, value := range m {
		resolved[key] = resolveExtendedValue(ctx, value)
	}
	return resolved
}

// orDefaultMap 是 pydantic default_factory：缺省时给默认 map（空 map 是显式
// 值，原样保留）。
func orDefaultMap(m map[string]any, def map[string]any) map[string]any {
	if m == nil {
		return def
	}
	return m
}

// randomHex32 现场铸 uuid4().hex 同形凭据（32 位十六进制）。
func randomHex32() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		// crypto/rand 失败在可行的平台上是系统级故障；退化为时间戳熵源。
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)
}
