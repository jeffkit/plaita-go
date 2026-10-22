package nodes

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// nodeTypeDelay mirrors plaita's DelayNode.node_type ("delay",
// plaita/server/nodes/delay_node.py).
const nodeTypeDelay workflow.NodeType = "delay"

func init() {
	workflow.RegisterNodeType(
		nodeTypeDelay,
		func(ctx context.Context) workflow.Node { return new(Delay) },
	)
}

// Delay 是挂起型延迟节点：Execute 解析时长（数值+单位枚举，`$` 表达式可引用，
// 解析失败静默回退 60 秒——Python _resolve_delay_time 语义）、产出 service_config
// （delay_ms/trigger_timestamp），引擎按订阅挂起；真正的等待发生在消费
// `{prefix}:delay:queue` 的外部 delay service（Redis ZSET 排程），不在节点进程内。
type Delay struct {
	BaseNode
	extendedEventCore
	DelaySeconds any    `json:"delay_seconds"`
	DelayUnit    string `json:"delay_unit,omitempty"`
}

// GetType returns the delay node type.
func (d *Delay) GetType() workflow.NodeType { return nodeTypeDelay }

// IsSuspending declares the engine suspend capability.
func (d *Delay) IsSuspending() bool { return true }

// Execute resolves the delay duration and returns the pending state with the
// delay service_config（BaseExtendedNode.execute 的合并顺序：EventNode 结果 +
// service_config + node_subtype）。
func (d *Delay) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	seconds, err := d.resolveDelaySeconds(ctx)
	if err != nil {
		return nil, err
	}
	filter := nonNilFilter(d.EventFilter)
	delayMs := delayToMilliseconds(seconds, d.DelayUnit)
	rc := runtime.WorkflowRuntimeContext(ctx)
	serviceConfig := map[string]any{
		"type":              string(nodeTypeDelay),
		"delay_ms":          delayMs,
		"trigger_timestamp": time.Now().UnixMilli() + delayMs,
		"node_id":           string(d.ID),
		"execution_id":      executionIDOf(rc),
		"flow_id":           flowIDOf(rc, d.ID),
		// delay 的 service_config 携带**原始** event_type 字段（Python
		// generate_service_config 用 self.event_type，不做 $ 解析）。
		"event_type":   d.EventType,
		"event_filter": filter,
		"retry_config": extendedRetryConfig(),
	}
	state := extendedPendingState(
		fmt.Sprintf("event_%s_%d", d.ID, time.Now().UnixMilli()),
		resolveEventExpression(ctx, d.EventType), filter, serviceConfig, nodeTypeDelay)
	return &workflow.ExecuteResult{Result: state, NextNode: d.NextNode()}, nil
}

// SuspendSpec declares the wake-up subscription（event_type 支持 $ 表达式，
// 解析语义与 Event 节点一致）。
func (d *Delay) SuspendSpec(ctx context.Context, result *workflow.ExecuteResult) (*workflow.SuspendSpec, error) {
	if m, ok := result.Result.(map[string]any); ok {
		if et, _ := m["event_type"].(string); et != "" {
			return &workflow.SuspendSpec{Subscription: &workflow.SubscriptionRequest{
				EventType:      et,
				Filter:         d.EventFilter,
				NodeID:         string(d.ID),
				TimeoutSeconds: coreTimeout(d.SubscriptionTimeout),
			}}, nil
		}
	}
	return extendedSuspendSpec(&d.extendedEventCore, d.ID), nil
}

// Resume applies the wake-up carrier（EventNode.resume 多态分发）。
func (d *Delay) Resume(ctx context.Context, resumeType string, data map[string]any) (*workflow.ExecuteResult, error) {
	return extendedResume(ctx, nodeTypeDelay, d.EventType, d.EventFilter, d.ID, d.NextNode(), resumeType, data)
}

// CanHandleEvent reports whether this node's filter accepts the event
// （plaita EventNode.can_handle_event；event_type 为表达式时按原样串比较，
// 同 Event 节点的既有口径）。
func (d *Delay) CanHandleEvent(eventType string, eventData map[string]any) bool {
	if eventType != d.EventType {
		return false
	}
	return eventFilterMatches(d.EventFilter, eventData)
}

// resolveDelaySeconds 是 Python _resolve_delay_time：`$` 表达式解析出非数值/
// None/求值异常一律回退 60（秒）；非表达式串按 float 解析（Python
// float(self.delay_seconds) 的 ValueError 口径，逐字文案对拍）。
func (d *Delay) resolveDelaySeconds(ctx context.Context) (float64, error) {
	if s, ok := d.DelaySeconds.(string); ok && strings.HasPrefix(s, "$") {
		resolved, err := expression.Eval(ctx, s)
		if err == nil {
			switch v := resolved.(type) {
			case int:
				return float64(v), nil
			case int64:
				return float64(v), nil
			case float64:
				return v, nil
			case bool:
				// Python isinstance(True, int) == True：布尔按 1/0 计。
				if v {
					return 1, nil
				}
				return 0, nil
			case json.Number:
				if f, err := v.Float64(); err == nil {
					return f, nil
				}
			}
		}
		return 60, nil
	}
	switch v := d.DelaySeconds.(type) {
	case nil:
		// pydantic 必填字段在加载期已拒；直构引擎流到这里按 float(None) 的
		// Python 口径报错。
		return 0, fmt.Errorf("delay node %q: delay_seconds is required", d.ID)
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case float64:
		return v, nil
	case json.Number:
		return v.Float64()
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return 0, fmt.Errorf("delay node %q: could not convert string to float: '%s'", d.ID, v)
		}
		return f, nil
	default:
		return 0, fmt.Errorf("delay node %q: delay_seconds must be a number, got %T", d.ID, d.DelaySeconds)
	}
}

// delayToMilliseconds 是 Python _convert_to_milliseconds：单位乘法表 + 未知
// 单位兜底按秒（Literal 在加载期已拦，这里是直构流的防御），int() 向零截断。
func delayToMilliseconds(value float64, unit string) int64 {
	switch strings.ToLower(unit) {
	case "minutes":
		return int64(value * 60 * 1000)
	case "hours":
		return int64(value * 60 * 60 * 1000)
	case "days":
		return int64(value * 24 * 60 * 60 * 1000)
	default: // "seconds" 与未知单位
		return int64(value * 1000)
	}
}
