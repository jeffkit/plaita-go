package nodes

import (
	"context"
	"fmt"
	"time"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// nodeTypeRedisQueue mirrors plaita's RedisQueueNode.node_type ("redis_queue",
// plaita/server/nodes/redis_queue_node.py)。注意这是**监听型**节点，与动作型
// redis 节点是两回事。
const nodeTypeRedisQueue workflow.NodeType = "redis_queue"

// redisQueueContractEventType：固定订阅 redis_message（Python _set_event_type
// 无条件覆盖，用户值不参与）。
const redisQueueContractEventType = "redis_message"

func init() {
	workflow.RegisterNodeType(
		nodeTypeRedisQueue,
		func(ctx context.Context) workflow.Node { return new(RedisQueue) },
	)
}

// RedisQueue 是挂起型 Redis 队列监听节点：Execute 产出监听 service_config
// （连接/队列/监听三组配置），由外部 service 按 queue_type（list/stream/
// pubsub）消费消息后发布 redis_message 事件唤醒流程。节点自身不连接 Redis。
// 指针字段区分「缺省」（pydantic default 生效）与「显式值」（含空串/0）。
type RedisQueue struct {
	BaseNode
	extendedEventCore
	RedisHost      *string `json:"redis_host,omitempty"`
	RedisPort      *int    `json:"redis_port,omitempty"`
	RedisDB        *int    `json:"redis_db,omitempty"`
	RedisPassword  any     `json:"redis_password,omitempty"`
	QueueName      any     `json:"queue_name"`
	QueueType      string  `json:"queue_type,omitempty"`
	TimeoutSeconds *int    `json:"timeout_seconds,omitempty"`
	BatchSize      *int    `json:"batch_size,omitempty"`
	MessageFormat  string  `json:"message_format,omitempty"`
}

// GetType returns the redis_queue node type.
func (r *RedisQueue) GetType() workflow.NodeType { return nodeTypeRedisQueue }

// IsSuspending declares the engine suspend capability.
func (r *RedisQueue) IsSuspending() bool { return true }

// Execute builds the listen service_config and returns the pending state.
func (r *RedisQueue) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	now := time.Now().UnixMilli()
	rc := runtime.WorkflowRuntimeContext(ctx)
	filter := nonNilFilter(r.EventFilter)
	serviceConfig := map[string]any{
		"type":         string(nodeTypeRedisQueue),
		"node_id":      string(r.ID),
		"execution_id": executionIDOf(rc),
		"flow_id":      flowIDOf(rc, r.ID),
		"event_type":   redisQueueContractEventType,
		"event_filter": filter,
		"redis_config": map[string]any{
			"host":     resolveExtendedString(ctx, r.RedisHost, "localhost"),
			"port":     orDefault(r.RedisPort, 6379),
			"db":       orDefault(r.RedisDB, 0),
			"password": resolveExtendedValue(ctx, r.RedisPassword),
		},
		"queue_config": map[string]any{
			"name":           resolveExtendedValue(ctx, r.QueueName),
			"type":           orDefaultStr(r.QueueType, "list"),
			"message_format": orDefaultStr(r.MessageFormat, "json"),
		},
		"listen_config": map[string]any{
			"timeout_seconds": orDefault(r.TimeoutSeconds, 0),
			"batch_size":      orDefault(r.BatchSize, 1),
		},
		"retry_config": extendedRetryConfig(),
	}
	state := extendedPendingState(
		fmt.Sprintf("event_%s_%d", r.ID, now),
		redisQueueContractEventType, filter, serviceConfig, nodeTypeRedisQueue)
	return &workflow.ExecuteResult{Result: state, NextNode: r.NextNode()}, nil
}

// SuspendSpec declares the wake-up subscription.
func (r *RedisQueue) SuspendSpec(_ context.Context, _ *workflow.ExecuteResult) (*workflow.SuspendSpec, error) {
	return &workflow.SuspendSpec{Subscription: &workflow.SubscriptionRequest{
		EventType:      redisQueueContractEventType,
		Filter:         r.EventFilter,
		NodeID:         string(r.ID),
		TimeoutSeconds: coreTimeout(r.SubscriptionTimeout),
	}}, nil
}

// Resume applies the wake-up carrier（EventNode.resume 多态分发）。
func (r *RedisQueue) Resume(ctx context.Context, resumeType string, data map[string]any) (*workflow.ExecuteResult, error) {
	return extendedResume(ctx, nodeTypeRedisQueue, redisQueueContractEventType, r.EventFilter, r.ID, r.NextNode(), resumeType, data)
}

// CanHandleEvent reports whether this node's filter accepts the event.
func (r *RedisQueue) CanHandleEvent(eventType string, eventData map[string]any) bool {
	if eventType != redisQueueContractEventType {
		return false
	}
	return eventFilterMatches(r.EventFilter, eventData)
}

// orDefault 是 pydantic 字段缺省：仅缺省时生效（nil），显式零值原样保留。
func orDefault[T any](p *T, def T) T {
	if p == nil {
		return def
	}
	return *p
}

// resolveExtendedString 对 *string 字段做缺省 + $ 解析（resolveExtendedValue
// 的字符串形态；空串不是 `$` 开头，原样返回，同 Python）。
func resolveExtendedString(ctx context.Context, p *string, def string) any {
	if p == nil {
		return def
	}
	return resolveExtendedValue(ctx, *p)
}
