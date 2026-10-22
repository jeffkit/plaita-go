package nodes

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// nodeTypeKafkaQueue mirrors plaita's KafkaQueueNode.node_type ("kafka_queue",
// plaita/server/nodes/kafka_queue_node.py)。
const nodeTypeKafkaQueue workflow.NodeType = "kafka_queue"

// kafkaQueueContractEventType：固定订阅 kafka_message（Python _set_event_type
// 无条件覆盖，用户值不参与）。
const kafkaQueueContractEventType = "kafka_message"

func init() {
	workflow.RegisterNodeType(
		nodeTypeKafkaQueue,
		func(ctx context.Context) workflow.Node { return new(KafkaQueue) },
	)
}

// KafkaQueue 是挂起型 Kafka 主题监听节点：Execute 产出消费 service_config
// （连接/SASL/主题/消费者组配置），由外部 service 消费主题消息后发布
// kafka_message 事件唤醒流程。节点自身不连接 Kafka（broker 是部署依赖）。
type KafkaQueue struct {
	BaseNode
	extendedEventCore
	BootstrapServers any    `json:"bootstrap_servers"`
	SecurityProtocol string `json:"security_protocol,omitempty"`
	SaslMechanism    any    `json:"sasl_mechanism,omitempty"`
	SaslUsername     any    `json:"sasl_username,omitempty"`
	SaslPassword     any    `json:"sasl_password,omitempty"`
	Topic            any    `json:"topic"`
	Partition        *int   `json:"partition,omitempty"`
	GroupID          any    `json:"group_id"`
	AutoOffsetReset  string `json:"auto_offset_reset,omitempty"`
	EnableAutoCommit *bool  `json:"enable_auto_commit,omitempty"`
	MaxPollRecords   *int   `json:"max_poll_records,omitempty"`
	MessageFormat    string `json:"message_format,omitempty"`
	TimeoutMs        *int64 `json:"timeout_ms,omitempty"`
}

// GetType returns the kafka_queue node type.
func (k *KafkaQueue) GetType() workflow.NodeType { return nodeTypeKafkaQueue }

// IsSuspending declares the engine suspend capability.
func (k *KafkaQueue) IsSuspending() bool { return true }

// Execute builds the consumer service_config and returns the pending state.
// bootstrap_servers：字符串按逗号切分为列表（Python _resolve_kafka_config）。
func (k *KafkaQueue) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	now := time.Now().UnixMilli()
	rc := runtime.WorkflowRuntimeContext(ctx)
	filter := nonNilFilter(k.EventFilter)

	bootstrap := resolveExtendedValue(ctx, k.BootstrapServers)
	var servers any
	switch v := bootstrap.(type) {
	case string:
		list := make([]any, 0)
		for _, s := range strings.Split(v, ",") {
			list = append(list, strings.TrimSpace(s))
		}
		servers = list
	default:
		// 非 str（list/None/其他）原样透传（Python isinstance(str) 分支语义）。
		servers = bootstrap
	}

	enableAutoCommit := true
	if k.EnableAutoCommit != nil {
		enableAutoCommit = *k.EnableAutoCommit
	}

	serviceConfig := map[string]any{
		"type":         string(nodeTypeKafkaQueue),
		"node_id":      string(k.ID),
		"execution_id": executionIDOf(rc),
		"flow_id":      flowIDOf(rc, k.ID),
		"event_type":   kafkaQueueContractEventType,
		"event_filter": filter,
		"kafka_config": map[string]any{
			"bootstrap_servers": servers,
			"security_protocol": orDefaultStr(k.SecurityProtocol, "PLAINTEXT"),
			"sasl_mechanism":    resolveExtendedValue(ctx, k.SaslMechanism),
			"sasl_username":     resolveExtendedValue(ctx, k.SaslUsername),
			"sasl_password":     resolveExtendedValue(ctx, k.SaslPassword),
		},
		"topic_config": map[string]any{
			"topic":          resolveExtendedValue(ctx, k.Topic),
			"partition":      k.Partition,
			"message_format": orDefaultStr(k.MessageFormat, "json"),
		},
		"consumer_config": map[string]any{
			"group_id":           resolveExtendedValue(ctx, k.GroupID),
			"auto_offset_reset":  orDefaultStr(k.AutoOffsetReset, "latest"),
			"enable_auto_commit": enableAutoCommit,
			"max_poll_records":   orDefault(k.MaxPollRecords, 1),
			"timeout_ms":         orDefault(k.TimeoutMs, 30000),
		},
		"retry_config": extendedRetryConfig(),
	}
	state := extendedPendingState(
		fmt.Sprintf("event_%s_%d", k.ID, now),
		kafkaQueueContractEventType, filter, serviceConfig, nodeTypeKafkaQueue)
	return &workflow.ExecuteResult{Result: state, NextNode: k.NextNode()}, nil
}

// SuspendSpec declares the wake-up subscription.
func (k *KafkaQueue) SuspendSpec(_ context.Context, _ *workflow.ExecuteResult) (*workflow.SuspendSpec, error) {
	return &workflow.SuspendSpec{Subscription: &workflow.SubscriptionRequest{
		EventType:      kafkaQueueContractEventType,
		Filter:         k.EventFilter,
		NodeID:         string(k.ID),
		TimeoutSeconds: coreTimeout(k.SubscriptionTimeout),
	}}, nil
}

// Resume applies the wake-up carrier（EventNode.resume 多态分发）。
func (k *KafkaQueue) Resume(ctx context.Context, resumeType string, data map[string]any) (*workflow.ExecuteResult, error) {
	return extendedResume(ctx, nodeTypeKafkaQueue, kafkaQueueContractEventType, k.EventFilter, k.ID, k.NextNode(), resumeType, data)
}

// CanHandleEvent reports whether this node's filter accepts the event.
func (k *KafkaQueue) CanHandleEvent(eventType string, eventData map[string]any) bool {
	if eventType != kafkaQueueContractEventType {
		return false
	}
	return eventFilterMatches(k.EventFilter, eventData)
}
