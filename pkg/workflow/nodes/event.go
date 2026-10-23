package nodes

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// nodeTypeEvent mirrors plaita's EventNode.node_type ("event").
const nodeTypeEvent types.NodeType = "event"

func init() {
	workflow.RegisterNodeType(
		nodeTypeEvent,
		func(ctx context.Context) workflow.Node { return new(Event) },
	)
}

// Event statuses (plaita EventNodeStatus).
const (
	EventStatusPending   = "pending"
	EventStatusCompleted = "completed"
	EventStatusError     = "error"
	EventStatusTimeout   = "timeout"
	EventStatusCancelled = "cancelled"
)

// Event is the suspending event node, ported from
// plaita/node/event_node.py:
//
//   - Execute resolves event_type (a `$` expression is evaluated; a
//     non-string/empty evaluation keeps the original literal, like Python)
//     and returns a pending state — it does NOT wait in-process;
//   - the engine registers the subscription from SuspendSpec and parks the
//     flow (is_suspending);
//   - Resume interprets the carrier: event → completed + event_data,
//     timeout → timeout, cancel → cancelled.
//
// event_filter matches dot paths into the event payload ({"data.status":
// "ok"}); an empty filter matches every event of the type. An empty
// event_type is rejected — an empty type is falsy in plaita's matcher and
// would silently become a wildcard.
type Event struct {
	BaseNode
	EventType           string         `json:"event_type"`
	EventFilter         map[string]any `json:"event_filter,omitempty"`
	SubscriptionTimeout *float64       `json:"subscription_timeout,omitempty"`
}

// eventFieldAliases folds the camelCase spellings (codeflow compiler / console
// forms) into the canonical snake_case JSON tags.
var eventFieldAliases = [][2]string{
	{"eventType", "event_type"},
	{"eventFilter", "event_filter"},
	{"subscriptionTimeout", "subscription_timeout"},
}

// UnmarshalJSON folds camelCase aliases before the standard decode. snake
// wins when both spellings are present.
func (e *Event) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	for _, pair := range eventFieldAliases {
		if v, ok := raw[pair[0]]; ok {
			if _, exists := raw[pair[1]]; !exists {
				raw[pair[1]] = v
			}
			delete(raw, pair[0])
		}
	}
	folded, err := json.Marshal(raw)
	if err != nil {
		return err
	}
	type eventFields Event
	return json.Unmarshal(folded, (*eventFields)(e))
}

// GetType returns the event node type.
func (e *Event) GetType() workflow.NodeType { return nodeTypeEvent }

// IsSuspending declares the engine suspend capability.
func (e *Event) IsSuspending() bool { return true }

// Execute resolves the event type and returns the pending state. No
// subscription is registered here: the engine does that at the suspend
// boundary and writes subscription_id into the pending state.
func (e *Event) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	resolved, err := e.resolveEventType(ctx)
	if err != nil {
		return nil, err
	}
	state := map[string]any{
		"event_id":     fmt.Sprintf("event_%s_%d", e.ID, time.Now().UnixMilli()),
		"status":       EventStatusPending,
		"event_type":   resolved,
		"event_filter": e.EventFilter,
		"is_async":     true,
	}
	return &workflow.ExecuteResult{Result: state, NextNode: e.NextNode()}, nil
}

// SuspendSpec declares the event subscription the host must register.
func (e *Event) SuspendSpec(ctx context.Context, result *workflow.ExecuteResult) (*workflow.SuspendSpec, error) {
	eventType := ""
	if m, ok := result.Result.(map[string]any); ok {
		eventType, _ = m["event_type"].(string)
	}
	if eventType == "" {
		resolved, err := e.resolveEventType(ctx)
		if err != nil {
			return nil, err
		}
		eventType = resolved
	}
	var timeout float64
	if e.SubscriptionTimeout != nil {
		timeout = *e.SubscriptionTimeout
	}
	return &workflow.SuspendSpec{Subscription: &workflow.SubscriptionRequest{
		EventType:      eventType,
		Filter:         e.EventFilter,
		NodeID:         string(e.ID),
		TimeoutSeconds: timeout,
	}}, nil
}

// Resume applies a wake-up carrier to the suspended node (plaita
// EventNode.resume polymorphic dispatch).
//
// 嵌套挂起 v1 对齐（rs resume_node_body 的「挂起态全量保留」契约——extended
// 家族的 extendedResume 已有、裸 Event 节点补齐）：恢复后状态保留挂起期的
// event_id/subscription_id/subscription_tenant，status 翻转 + event_data 落地。
func (e *Event) Resume(ctx context.Context, resumeType string, data map[string]any) (*workflow.ExecuteResult, error) {
	state := e.baseResult()
	if rc := runtime.WorkflowRuntimeContext(ctx); rc != nil && rc.ExecuteNodeResults != nil {
		if prev, ok := rc.ExecuteNodeResults[string(e.ID)].(map[string]any); ok {
			for _, key := range []string{"event_id", "subscription_id", "subscription_tenant"} {
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
		return nil, &workflow.ResumeGuardError{Message: fmt.Sprintf("unsupported resume type %q for event node", resumeType)}
	}
	return &workflow.ExecuteResult{Result: state, NextNode: e.NextNode()}, nil
}

// CanHandleEvent reports whether this node's filter accepts an event of
// eventType with eventData (plaita EventNode.can_handle_event).
func (e *Event) CanHandleEvent(eventType string, eventData map[string]any) bool {
	if eventType != e.EventType {
		return false
	}
	return eventFilterMatches(e.EventFilter, eventData)
}

func (e *Event) resolveEventType(ctx context.Context) (string, error) {
	raw := e.EventType
	if strings.TrimSpace(raw) == "" {
		return "", fmt.Errorf("event %q: event_type is required (an empty type would match every event)", e.ID)
	}
	if !strings.HasPrefix(raw, "$") {
		return raw, nil
	}
	resolved, err := expression.Eval(ctx, raw)
	if err != nil {
		// Python logs and keeps the original reference on evaluation failure.
		return raw, nil
	}
	if s, ok := resolved.(string); ok && s != "" {
		return s, nil
	}
	return raw, nil
}

func (e *Event) baseResult() map[string]any {
	return map[string]any{
		"event_type":   e.EventType,
		"event_filter": e.EventFilter,
		"is_async":     true,
	}
}

// eventFilterMatches applies plaita's dot-path filter: every key must resolve
// inside the payload and equal the configured value. An empty filter matches.
func eventFilterMatches(filter map[string]any, eventData map[string]any) bool {
	if len(filter) == 0 {
		return true
	}
	for key, want := range filter {
		var current any = eventData
		for _, segment := range strings.Split(key, ".") {
			m, ok := current.(map[string]any)
			if !ok {
				return false
			}
			value, exists := m[segment]
			if !exists {
				return false
			}
			current = value
		}
		if !eventValuesEqual(current, want) {
			return false
		}
	}
	return true
}

// eventValuesEqual compares JSON-decoded scalars the way Python's == does for
// the numeric family (1 == 1.0). Containers fall back to structural equality.
func eventValuesEqual(a, b any) bool {
	af, aok := gateToFloat(a)
	bf, bok := gateToFloat(b)
	if aok && bok {
		return af == bf
	}
	return fmt.Sprint(a) == fmt.Sprint(b)
}
