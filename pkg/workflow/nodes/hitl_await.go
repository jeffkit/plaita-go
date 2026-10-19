package nodes

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// nodeTypeHitlAwait mirrors plaita-nodes HitlAwaitNode.node_type.
const nodeTypeHitlAwait types.NodeType = "hitl_await"

// hitlReplyEventType is the event type the external hitl poller publishes
// (plaita_nodes.hitl_poller.EVENT_TYPE).
const hitlReplyEventType = "hitl_reply"

func init() {
	workflow.RegisterNodeType(
		nodeTypeHitlAwait,
		func(ctx context.Context) workflow.Node { return new(HitlAwait) },
	)
}

// HitlAwait is the SUSPENDING human-confirmation atom (plaita-nodes
// HitlAwaitNode, Distributed mode): Execute sends the message through
// hitl-server, returns {status: pending, session_id, ...} and the engine parks
// the flow. The reply arrives later as a hitl_reply event (external poller) or
// as a timeout carrier; Resume turns it into the structured output.
//
// Field set matches the blocking Hitl node (message/images/base_url/
// timeout_secs) plus event_type/event_filter. Divergences from Python, both
// deliberate:
//   - the subscription carries subscription_timeout = timeout_secs, so the
//     host's timeout checker enqueues a timeout carrier even if no external
//     poller publishes one (Python relies on the poller's status=timeout
//     event);
//   - Resume unwraps a nested `event_data` payload (the event-filter carrier
//     shape) before reading status/replies/session_id.
type HitlAwait struct {
	BaseNode
	Message     any            `json:"message,omitempty"`
	Images      any            `json:"images,omitempty"`
	BaseURL     any            `json:"base_url,omitempty"`
	TimeoutSecs any            `json:"timeout_secs,omitempty"`
	EventType   string         `json:"event_type,omitempty"`
	EventFilter map[string]any `json:"event_filter,omitempty"`
	DryRun      bool           `json:"dry_run,omitempty"`
}

var hitlAwaitFieldAliases = [][2]string{
	{"baseUrl", "base_url"},
	{"timeoutSecs", "timeout_secs"},
	{"eventType", "event_type"},
	{"eventFilter", "event_filter"},
	{"dryRun", "dry_run"},
}

// UnmarshalJSON folds camelCase aliases before the standard decode.
func (h *HitlAwait) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	for _, pair := range hitlAwaitFieldAliases {
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
	type hitlAwaitFields HitlAwait
	return json.Unmarshal(folded, (*hitlAwaitFields)(h))
}

// GetType returns the hitl_await node type.
func (h *HitlAwait) GetType() workflow.NodeType { return nodeTypeHitlAwait }

// IsSuspending declares the engine suspend capability.
func (h *HitlAwait) IsSuspending() bool { return true }

// Execute evaluates the message fields, sends the confirmation request and
// returns the pending state (waiting for the external reply event).
func (h *HitlAwait) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	message, err := hitlEvalText(ctx, h.Message)
	if err != nil {
		return nil, fmt.Errorf("hitl_await %q: %w", h.ID, err)
	}
	images, err := hitlEvalImages(ctx, string(h.ID), h.Images)
	if err != nil {
		return nil, fmt.Errorf("hitl_await %q: %w", h.ID, err)
	}
	base, err := hitlResolveBaseURL(ctx, string(h.ID), h.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("hitl_await %q: %w", h.ID, err)
	}
	timeoutSecs, err := hitlResolveTimeoutSecs(ctx, string(h.ID), h.TimeoutSecs)
	if err != nil {
		return nil, fmt.Errorf("hitl_await %q: %w", h.ID, err)
	}

	dry := h.DryRun
	if rc := runtime.WorkflowRuntimeContext(ctx); rc != nil {
		if isTruthy(rc.GetGlobalVariable("dry_run")) {
			dry = true
		}
	}
	if dry {
		return &workflow.ExecuteResult{
			Result: map[string]any{
				"status":       "pending",
				"session_id":   "dryrun",
				"replies":      []any{},
				"dry_run":      true,
				"timeout_secs": timeoutSecs,
			},
			NextNode: h.NextNode(),
		}, nil
	}

	sessionID, err := hitlSendMessage(ctx, base, message, images, timeoutSecs, true)
	if err != nil {
		return nil, fmt.Errorf("hitl_await %q: %w", h.ID, err)
	}
	return &workflow.ExecuteResult{
		Result: map[string]any{
			"status":       "pending",
			"session_id":   sessionID,
			"replies":      []any{},
			"timeout_secs": timeoutSecs,
		},
		NextNode: h.NextNode(),
	}, nil
}

// SuspendSpec registers the hitl_reply subscription (correlation_id binds it
// to this execution) with the reply deadline as the subscription timeout.
func (h *HitlAwait) SuspendSpec(ctx context.Context, result *workflow.ExecuteResult) (*workflow.SuspendSpec, error) {
	eventType := h.eventType()
	if strings.HasPrefix(eventType, "$") {
		if resolved, err := expression.Eval(ctx, eventType); err == nil {
			if s, ok := resolved.(string); ok && s != "" {
				eventType = s
			}
		}
	}
	var timeout float64
	if m, ok := result.Result.(map[string]any); ok {
		if f, ok := gateToFloat(m["timeout_secs"]); ok {
			timeout = f
		}
	}
	return &workflow.SuspendSpec{Subscription: &workflow.SubscriptionRequest{
		EventType:      eventType,
		Filter:         h.EventFilter,
		NodeID:         string(h.ID),
		TimeoutSeconds: timeout,
	}}, nil
}

// Resume turns the wake-up carrier into the structured output. Python's
// resume reads flat status/replies/session_id; an event-filter carrier nests
// them under event_data, which is also accepted here.
func (h *HitlAwait) Resume(_ context.Context, resumeType string, data map[string]any) (*workflow.ExecuteResult, error) {
	if nested, ok := data["event_data"].(map[string]any); ok {
		merged := make(map[string]any, len(data)+len(nested))
		for k, v := range data {
			merged[k] = v
		}
		for k, v := range nested {
			if _, exists := merged[k]; !exists {
				merged[k] = v
			}
		}
		data = merged
	}
	sessionID, _ := data["session_id"].(string)
	switch resumeType {
	case workflow.ResumeCancel:
		return h.result("cancel", []any{}, sessionID), nil
	case workflow.ResumeTimeout:
		return h.result("timeout", []any{}, sessionID), nil
	case workflow.ResumeEvent, "":
		status, _ := data["status"].(string)
		if status == "" {
			status = "replied"
		}
		return h.result(status, stringAnyList(data["replies"]), sessionID), nil
	default:
		return nil, &workflow.ResumeGuardError{Message: fmt.Sprintf("unsupported resume type %q for hitl_await node", resumeType)}
	}
}

func (h *HitlAwait) result(status string, replies []any, sessionID string) *workflow.ExecuteResult {
	return &workflow.ExecuteResult{
		Result: map[string]any{
			"status":     status,
			"replies":    replies,
			"session_id": sessionID,
		},
		NextNode: h.NextNode(),
	}
}

func (h *HitlAwait) eventType() string {
	if strings.TrimSpace(h.EventType) == "" {
		return hitlReplyEventType
	}
	return h.EventType
}

// stringAnyList mirrors Python's [str(r) for r in (data.get("replies") or [])].
func stringAnyList(v any) []any {
	switch list := v.(type) {
	case []any:
		out := make([]any, 0, len(list))
		for _, item := range list {
			out = append(out, gateToStr(item))
		}
		return out
	case []string:
		out := make([]any, 0, len(list))
		for _, item := range list {
			out = append(out, item)
		}
		return out
	}
	return []any{}
}
