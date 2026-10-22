package ir

// #80 phase 3：server 扩展节点（delay/approval/redis_queue/kafka_queue/
// http_callback）的加载期对齐。
//
// 权威 = plaita/server/nodes/*_node.py 的 pydantic 模型层：必填字段、Literal
// 枚举、大小写归一（mode="before" validator）与缺省值都在模型校验期生效——
// 部署 flow 带非法枚举/缺必填时 plaita 的 Flow.model_validate 直接拒绝，
// Go 的 ir.Load 同样在加载期拒绝（fail-closed 对齐，不给「Go 能跑、plaita
// 拒载」的跨运行时假象）。
//
// 固定事件订阅契约：approval/redis_queue/kafka_queue/http_callback 的
// event_type 由 validator 无条件覆盖（用户值不参与）；delay 不覆盖
// （event_type 可配，默认 delay_trigger）。
//
// approval 的显式 subscription_timeout=null（Python None = 无限等待）在
// 部署 JSON 里与「缺省 24h」可区分——JSON null 折叠为 -1 哨兵值，节点侧
// 转回 0=不设超时。

import (
	"encoding/json"
	"fmt"
	"strings"
)

// lowerEnumValue 大小写归一并守住非字符串输入（pydantic 对非 str 的
// Literal 输入报 validation error，这里同拒）。
func lowerEnumValue(v any) (string, bool) {
	s, ok := v.(string)
	if !ok {
		return "", false
	}
	return strings.ToLower(s), true
}

// upperEnumValue 同 lowerEnumValue，大小写归一方向相反（callback_method）。
func upperEnumValue(v any) (string, bool) {
	s, ok := v.(string)
	if !ok {
		return "", false
	}
	return strings.ToUpper(s), true
}

var (
	delayUnits         = map[string]bool{"seconds": true, "minutes": true, "hours": true, "days": true}
	approvalTypes      = map[string]bool{"manual": true, "auto": true}
	approvalStrategies = map[string]bool{"any": true, "all": true, "majority": true}
	queueTypes         = map[string]bool{"list": true, "stream": true, "pubsub": true}
	redisMsgFormats    = map[string]bool{"json": true, "text": true, "raw": true}
	kafkaProtocols     = map[string]bool{"PLAINTEXT": true, "SSL": true, "SASL_PLAINTEXT": true, "SASL_SSL": true}
	kafkaMechanisms    = map[string]bool{"PLAIN": true, "SCRAM-SHA-256": true, "SCRAM-SHA-512": true}
	kafkaOffsetResets  = map[string]bool{"earliest": true, "latest": true}
	kafkaMsgFormats    = map[string]bool{"json": true, "text": true}
	callbackMethods    = map[string]bool{"GET": true, "POST": true, "PUT": true, "DELETE": true}
	authTypes          = map[string]bool{"token": true, "basic": true, "signature": true}
)

// translateDelay 校验 delay 节点（delay_node.py）：delay_seconds 必填
// （int|float|str，$ 表达式合法），delay_unit 大小写归一后限枚举。
func translateDelay(n map[string]any) error {
	foldKey(n, "eventType", "event_type")
	foldKey(n, "eventFilter", "event_filter")
	foldKey(n, "subscriptionTimeout", "subscription_timeout")
	foldKey(n, "delaySeconds", "delay_seconds")
	foldKey(n, "delayUnit", "delay_unit")

	seconds, ok := n["delay_seconds"]
	if !ok || seconds == nil {
		return fmt.Errorf("delay node requires delay_seconds (int/float/expression)")
	}
	if v, ok := n["event_type"]; ok && v != nil {
		if _, isStr := v.(string); !isStr || strings.TrimSpace(v.(string)) == "" {
			return fmt.Errorf("delay event_type must be a non-empty string")
		}
	} else {
		// pydantic default：delay 是五节点里唯一 event_type 可配的
		//（默认 delay_trigger，其余四类由契约强制覆盖）。
		n["event_type"] = "delay_trigger"
	}
	switch seconds.(type) {
	case string, json.Number:
	default:
		return fmt.Errorf("delay_seconds must be a number or expression string, got %T", seconds)
	}
	if unit, ok := n["delay_unit"]; ok && unit != nil {
		s, isStr := unit.(string)
		if !isStr {
			return fmt.Errorf("delay_unit must be a string, got %T", unit)
		}
		s = strings.ToLower(s)
		if !delayUnits[s] {
			return fmt.Errorf("delay_unit must be one of seconds/minutes/hours/days, got %q", s)
		}
		n["delay_unit"] = s
	} else {
		n["delay_unit"] = "seconds"
	}
	if v, ok := n["event_filter"]; ok && v != nil {
		if _, isMap := v.(map[string]any); !isMap {
			return fmt.Errorf("event_filter must be an object, got %T", v)
		}
	}
	if v, ok := n["subscription_timeout"]; ok && v != nil {
		if err := validateSubscriptionTimeout(v); err != nil {
			return err
		}
	}
	return nil
}

// translateApproval 校验 approval 节点（approval_node.py）：标题/内容/审批人
// 必填，枚举大小写归一，缺省值按模型填充；event_type 强制覆盖为契约值；
// 显式 null 的 subscription_timeout 折叠为 -1（无限等待哨兵）。
func translateApproval(n map[string]any) error {
	foldKey(n, "eventType", "event_type")
	foldKey(n, "eventFilter", "event_filter")
	foldKey(n, "subscriptionTimeout", "subscription_timeout")
	foldKey(n, "approvalTitle", "approval_title")
	foldKey(n, "approvalContent", "approval_content")
	foldKey(n, "approvalType", "approval_type")
	foldKey(n, "approvalStrategy", "approval_strategy")
	foldKey(n, "autoEscalation", "auto_escalation")
	foldKey(n, "escalationTimeoutHours", "escalation_timeout_hours")
	foldKey(n, "escalationApprovers", "escalation_approvers")
	foldKey(n, "formFields", "form_fields")
	foldKey(n, "allowComments", "allow_comments")
	foldKey(n, "requireComments", "require_comments")
	foldKey(n, "notificationConfig", "notification_config")

	for _, field := range []string{"approval_title", "approval_content", "approvers"} {
		if v, ok := n[field]; !ok || v == nil {
			return fmt.Errorf("approval node requires %s", field)
		}
	}
	if _, isList := n["approvers"].([]any); !isList {
		return fmt.Errorf("approval approvers must be a list, got %T", n["approvers"])
	}
	if v, ok := n["approval_type"]; ok && v != nil {
		s, isStr := lowerEnumValue(v)
		if !isStr {
			return fmt.Errorf("approval_type must be a string, got %T", v)
		}
		if !approvalTypes[s] {
			return fmt.Errorf("approval_type must be one of manual/auto, got %q", s)
		}
		n["approval_type"] = s
	} else {
		n["approval_type"] = "manual"
	}
	if v, ok := n["approval_strategy"]; ok && v != nil {
		s, isStr := lowerEnumValue(v)
		if !isStr {
			return fmt.Errorf("approval_strategy must be a string, got %T", v)
		}
		if !approvalStrategies[s] {
			return fmt.Errorf("approval_strategy must be one of any/all/majority, got %q", s)
		}
		n["approval_strategy"] = s
	} else {
		n["approval_strategy"] = "any"
	}
	if _, ok := n["escalation_timeout_hours"]; !ok {
		n["escalation_timeout_hours"] = json.Number("24")
	}
	if v, ok := n["allow_comments"]; !ok || v == nil {
		n["allow_comments"] = true
	}
	// 运行时契约：event_type 无条件覆盖（Python _normalize_enums）。
	n["event_type"] = "approval_decision"
	if v, ok := n["event_filter"]; ok && v != nil {
		if _, isMap := v.(map[string]any); !isMap {
			return fmt.Errorf("event_filter must be an object, got %T", v)
		}
	}
	// gt=0 校验 + 显式 null → -1 哨兵（无限等待）。
	if v, ok := n["subscription_timeout"]; ok {
		if v == nil {
			n["subscription_timeout"] = json.Number("-1")
			return nil
		}
		if err := validateSubscriptionTimeout(v); err != nil {
			return err
		}
	}
	return nil
}

// translateRedisQueue 校验 redis_queue 节点（redis_queue_node.py）：queue_name
// 必填，queue_type/message_format 限枚举，event_type 强制契约值。
func translateRedisQueue(n map[string]any) error {
	foldKey(n, "eventType", "event_type")
	foldKey(n, "eventFilter", "event_filter")
	foldKey(n, "subscriptionTimeout", "subscription_timeout")
	foldKey(n, "redisHost", "redis_host")
	foldKey(n, "redisPort", "redis_port")
	foldKey(n, "redisDb", "redis_db")
	foldKey(n, "redisPassword", "redis_password")
	foldKey(n, "queueName", "queue_name")
	foldKey(n, "queueType", "queue_type")
	foldKey(n, "timeoutSeconds", "timeout_seconds")
	foldKey(n, "batchSize", "batch_size")
	foldKey(n, "messageFormat", "message_format")

	if v, ok := n["queue_name"]; !ok || v == nil {
		return fmt.Errorf("redis_queue node requires queue_name")
	}
	if v, ok := n["queue_type"]; ok && v != nil {
		s, isStr := v.(string)
		if !isStr || !queueTypes[s] {
			return fmt.Errorf("queue_type must be one of list/stream/pubsub, got %v", v)
		}
	} else {
		n["queue_type"] = "list"
	}
	if v, ok := n["message_format"]; ok && v != nil {
		s, isStr := v.(string)
		if !isStr || !redisMsgFormats[s] {
			return fmt.Errorf("message_format must be one of json/text/raw, got %v", v)
		}
	} else {
		n["message_format"] = "json"
	}
	n["event_type"] = "redis_message"
	if v, ok := n["event_filter"]; ok && v != nil {
		if _, isMap := v.(map[string]any); !isMap {
			return fmt.Errorf("event_filter must be an object, got %T", v)
		}
	}
	if v, ok := n["subscription_timeout"]; ok && v != nil {
		if err := validateSubscriptionTimeout(v); err != nil {
			return err
		}
	}
	return nil
}

// translateKafkaQueue 校验 kafka_queue 节点（kafka_queue_node.py）：
// bootstrap_servers（str|list）/topic/group_id 必填，协议/SASL/偏移/格式限
// 枚举，event_type 强制契约值。
func translateKafkaQueue(n map[string]any) error {
	foldKey(n, "eventType", "event_type")
	foldKey(n, "eventFilter", "event_filter")
	foldKey(n, "subscriptionTimeout", "subscription_timeout")
	foldKey(n, "bootstrapServers", "bootstrap_servers")
	foldKey(n, "securityProtocol", "security_protocol")
	foldKey(n, "saslMechanism", "sasl_mechanism")
	foldKey(n, "saslUsername", "sasl_username")
	foldKey(n, "saslPassword", "sasl_password")
	foldKey(n, "autoOffsetReset", "auto_offset_reset")
	foldKey(n, "enableAutoCommit", "enable_auto_commit")
	foldKey(n, "maxPollRecords", "max_poll_records")
	foldKey(n, "messageFormat", "message_format")
	foldKey(n, "timeoutMs", "timeout_ms")

	bootstrap, ok := n["bootstrap_servers"]
	if !ok || bootstrap == nil {
		return fmt.Errorf("kafka_queue node requires bootstrap_servers (string or list)")
	}
	switch v := bootstrap.(type) {
	case string:
	case []any:
		for i, item := range v {
			if _, isStr := item.(string); !isStr {
				return fmt.Errorf("bootstrap_servers[%d] must be a string, got %T", i, item)
			}
		}
	default:
		return fmt.Errorf("bootstrap_servers must be a string or list of strings, got %T", bootstrap)
	}
	for _, field := range []string{"topic", "group_id"} {
		if v, ok := n[field]; !ok || v == nil {
			return fmt.Errorf("kafka_queue node requires %s", field)
		}
	}
	if v, ok := n["security_protocol"]; ok && v != nil {
		s, isStr := v.(string)
		if !isStr || !kafkaProtocols[s] {
			return fmt.Errorf("security_protocol must be one of PLAINTEXT/SSL/SASL_PLAINTEXT/SASL_SSL, got %v", v)
		}
	} else {
		n["security_protocol"] = "PLAINTEXT"
	}
	if v, ok := n["sasl_mechanism"]; ok && v != nil {
		s, isStr := v.(string)
		if !isStr || !kafkaMechanisms[s] {
			return fmt.Errorf("sasl_mechanism must be one of PLAIN/SCRAM-SHA-256/SCRAM-SHA-512, got %v", v)
		}
	}
	if v, ok := n["auto_offset_reset"]; ok && v != nil {
		s, isStr := v.(string)
		if !isStr || !kafkaOffsetResets[s] {
			return fmt.Errorf("auto_offset_reset must be one of earliest/latest, got %v", v)
		}
	} else {
		n["auto_offset_reset"] = "latest"
	}
	if v, ok := n["message_format"]; ok && v != nil {
		s, isStr := v.(string)
		if !isStr || !kafkaMsgFormats[s] {
			return fmt.Errorf("message_format must be one of json/text, got %v", v)
		}
	} else {
		n["message_format"] = "json"
	}
	if _, ok := n["timeout_ms"]; !ok {
		n["timeout_ms"] = json.Number("30000")
	}
	if v, ok := n["enable_auto_commit"]; !ok || v == nil {
		n["enable_auto_commit"] = true
	}
	if v, ok := n["max_poll_records"]; !ok || v == nil {
		n["max_poll_records"] = json.Number("1")
	}
	n["event_type"] = "kafka_message"
	if v, ok := n["event_filter"]; ok && v != nil {
		if _, isMap := v.(map[string]any); !isMap {
			return fmt.Errorf("event_filter must be an object, got %T", v)
		}
	}
	if v, ok := n["subscription_timeout"]; ok && v != nil {
		if err := validateSubscriptionTimeout(v); err != nil {
			return err
		}
	}
	return nil
}

// translateHTTPCallback 校验 http_callback 节点（http_callback_node.py）：
// callback_method 大小写归一限枚举、auth_type 限枚举，其余全有缺省；
// event_type 强制契约值。
func translateHTTPCallback(n map[string]any) error {
	foldKey(n, "eventType", "event_type")
	foldKey(n, "eventFilter", "event_filter")
	foldKey(n, "subscriptionTimeout", "subscription_timeout")
	foldKey(n, "callbackPath", "callback_path")
	foldKey(n, "callbackMethod", "callback_method")
	foldKey(n, "callbackTimeoutMinutes", "callback_timeout_minutes")
	foldKey(n, "requireAuth", "require_auth")
	foldKey(n, "authType", "auth_type")
	foldKey(n, "authToken", "auth_token")
	foldKey(n, "validateHeaders", "validate_headers")
	foldKey(n, "validateParams", "validate_params")
	foldKey(n, "successResponse", "success_response")
	foldKey(n, "errorResponse", "error_response")

	if v, ok := n["callback_method"]; ok && v != nil {
		m, isStr := upperEnumValue(v)
		if !isStr {
			return fmt.Errorf("callback_method must be a string, got %T", v)
		}
		if !callbackMethods[m] {
			return fmt.Errorf("callback_method must be one of GET/POST/PUT/DELETE, got %q", m)
		}
		n["callback_method"] = m
	} else {
		n["callback_method"] = "POST"
	}
	if v, ok := n["auth_type"]; ok && v != nil {
		s, isStr := v.(string)
		if !isStr || !authTypes[s] {
			return fmt.Errorf("auth_type must be one of token/basic/signature, got %v", v)
		}
	} else {
		n["auth_type"] = "token"
	}
	if v, ok := n["require_auth"]; !ok || v == nil {
		n["require_auth"] = true
	}
	if _, ok := n["callback_timeout_minutes"]; !ok {
		n["callback_timeout_minutes"] = json.Number("60")
	}
	n["event_type"] = "http_callback"
	if v, ok := n["event_filter"]; ok && v != nil {
		if _, isMap := v.(map[string]any); !isMap {
			return fmt.Errorf("event_filter must be an object, got %T", v)
		}
	}
	if v, ok := n["subscription_timeout"]; ok && v != nil {
		if err := validateSubscriptionTimeout(v); err != nil {
			return err
		}
	}
	return nil
}

// validateSubscriptionTimeout 是 EventNode 家族共享的 gt=0 约束（0 会让订阅
// 一注册就超时）。
func validateSubscriptionTimeout(v any) error {
	seconds, valid := normalizeSeconds(v)
	if !valid || seconds <= 0 {
		return fmt.Errorf("subscription_timeout must be a positive number of seconds, got %v", v)
	}
	return nil
}
