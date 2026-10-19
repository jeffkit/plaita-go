package nodes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// nodeTypeHitl mirrors plaita-nodes HitlNode.node_type ("hitl").
const nodeTypeHitl types.NodeType = "hitl"

func init() {
	workflow.RegisterNodeType(
		nodeTypeHitl,
		func(ctx context.Context) workflow.Node { return new(Hitl) },
	)
}

// Hitl is the blocking human-confirmation atom: send a message through the
// hitl-server (WeChat via the iLink upstream) and poll for the reply inside
// Execute. Mirrors plaita-nodes HitlNode (the blocking "Normal" variant; the
// suspended hitl_await branch depends on engine suspend/resume and is not
// ported):
//
//   - message: text (expression, stringified like Python's str(x or ""));
//   - images: optional image path list (expression);
//   - base_url: hitl-server address (field > env HITL_URL > 127.0.0.1:8081);
//   - timeout_secs: reply deadline in seconds (default 3600, invalid → default);
//   - poll_interval: seconds between polls (default 5, invalid → default);
//   - wait_reply: false sends without waiting (weekly-report push style,
//     hitl.js waitReply:false parity) — default true;
//   - dry_run (or globalContext.dry_run) simulates an "同意" reply without
//     touching any server.
//
// Output: {"status": "replied"|"timeout"|"cancel"|"sent", "replies": [str],
// "session_id", "dry_run"}. Reaching the deadline yields status="timeout"
// instead of an error (hitl.js parity); only a server-rejected send raises
// (*HitlError). Cancellation rides the Go context where Python uses
// cancel_event/cancel_requested: the poll loop observes ctx.Done at the loop
// top and inside the inter-poll sleep (A2R8-03 — no full-interval lag), and
// returns status="cancel" instead of an error.
//
// Divergences from the Python node, all deliberate and fail-closed:
//   - images evaluating to a non-list is an error (Python would iterate the
//     value pathologically, e.g. a string char-by-char);
//   - a non-dict entry in poll replies is an error (Python would crash on
//     .get of a non-dict).
type Hitl struct {
	BaseNode
	Message      any   `json:"message,omitempty"`
	Images       any   `json:"images,omitempty"`
	BaseURL      any   `json:"base_url,omitempty"`
	TimeoutSecs  any   `json:"timeout_secs,omitempty"`
	PollInterval any   `json:"poll_interval,omitempty"`
	WaitReply    *bool `json:"wait_reply,omitempty"` // Python default true
	DryRun       bool  `json:"dry_run,omitempty"`
}

// hitlFieldAliases folds the camelCase spellings of the plaita-nodes hitl
// fields into the canonical snake_case JSON tags (same fold as pkg/ir's
// nodeFieldAliases, applied in-node because hitl is not yet in the ir
// allowlist). snake wins when both spellings are present.
var hitlFieldAliases = [][2]string{
	{"baseUrl", "base_url"},
	{"timeoutSecs", "timeout_secs"},
	{"pollInterval", "poll_interval"},
	{"waitReply", "wait_reply"},
	{"dryRun", "dry_run"},
}

// UnmarshalJSON folds camelCase field aliases before the standard decode so
// plaita-compiled camelCase flows bind correctly on the direct-engine path.
func (h *Hitl) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	for _, pair := range hitlFieldAliases {
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
	type hitlFields Hitl // shed the UnmarshalJSON method
	return json.Unmarshal(folded, (*hitlFields)(h))
}

// GetType returns the hitl node type.
func (h *Hitl) GetType() workflow.NodeType { return nodeTypeHitl }

// Execute evaluates the expression fields, sends the message and polls for
// the reply until the deadline.
func (h *Hitl) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	message, err := hitlEvalText(ctx, h.Message)
	if err != nil {
		return nil, fmt.Errorf("hitl %q: %w", h.ID, err)
	}
	images, err := hitlEvalImages(ctx, string(h.ID), h.Images)
	if err != nil {
		return nil, fmt.Errorf("hitl %q: %w", h.ID, err)
	}
	base, err := hitlResolveBaseURL(ctx, string(h.ID), h.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("hitl %q: %w", h.ID, err)
	}
	timeoutSecs, err := hitlResolveTimeoutSecs(ctx, string(h.ID), h.TimeoutSecs)
	if err != nil {
		return nil, fmt.Errorf("hitl %q: %w", h.ID, err)
	}
	pollInterval := hitlDefaultPollIntervalSecs
	if h.PollInterval != nil {
		ev, err := expression.Eval(ctx, h.PollInterval)
		if err != nil {
			return nil, fmt.Errorf("hitl %q: eval poll_interval: %w", h.ID, err)
		}
		if f, ok := gateToFloat(ev); ok {
			pollInterval = f
		}
	}

	waitReply := h.WaitReply == nil || *h.WaitReply
	dry := h.DryRun
	if rc := runtime.WorkflowRuntimeContext(ctx); rc != nil {
		if isTruthy(rc.GetGlobalVariable("dry_run")) {
			dry = true
		}
	}

	if dry {
		if !waitReply {
			return hitlResult(h, "sent", []any{}, "dryrun", true), nil
		}
		return hitlResult(h, "replied", []any{"同意"}, "dryrun", true), nil
	}

	sessionID, err := hitlSendMessage(ctx, base, message, images, timeoutSecs, waitReply)
	if err != nil {
		var hitlErr *HitlError
		if errors.As(err, &hitlErr) {
			return nil, fmt.Errorf("hitl %q: %w", h.ID, err)
		}
		if ctx.Err() != nil {
			// cancelled mid-send: no session to report (Python's send is not
			// cancel-aware, so this branch has no Python counterpart)
			return hitlResult(h, "cancel", []any{}, "", false), nil
		}
		return nil, fmt.Errorf("hitl %q: %w", h.ID, err)
	}

	if !waitReply {
		// send-and-forget (e.g. weekly report push), hitl.js waitReply:false
		return hitlResult(h, "sent", []any{}, sessionID, false), nil
	}

	deadline := time.Now().Add(time.Duration(timeoutSecs) * time.Second)
	status := "timeout"
	replies := []any{}
pollLoop:
	for time.Now().Before(deadline) {
		// observe cancellation at the loop top instead of sleeping through a
		// full poll interval (A2R8-03)
		select {
		case <-ctx.Done():
			return hitlResult(h, "cancel", []any{}, sessionID, false), nil
		default:
		}

		poll, err := hitlPollReply(ctx, base, sessionID, hitlPollTimeout(deadline))
		if err != nil {
			if ctx.Err() != nil {
				return hitlResult(h, "cancel", []any{}, sessionID, false), nil
			}
			return nil, fmt.Errorf("hitl %q: %w", h.ID, err)
		}
		if isTruthy(poll["has_reply"]) {
			extracted, err := hitlExtractReplies(poll["replies"])
			if err != nil {
				return nil, fmt.Errorf("hitl %q: %w", h.ID, err)
			}
			status = "replied"
			replies = extracted
			break
		}
		switch hitlPollStatus(poll) {
		case "error":
			return nil, fmt.Errorf("hitl %q: %w", h.ID,
				&HitlError{msg: fmt.Sprintf("hitl-server 轮询错误: %v", poll)})
		case "timeout":
			break pollLoop
		}

		// cancellable wait: ctx.Done during the interval returns immediately
		select {
		case <-ctx.Done():
			return hitlResult(h, "cancel", []any{}, sessionID, false), nil
		case <-time.After(time.Duration(pollInterval * float64(time.Second))):
		}
	}
	return hitlResult(h, status, replies, sessionID, false), nil
}

// hitlPollStatus extracts the poll body's status string (missing/non-string → ""
// like Python's .get returning None).
func hitlPollStatus(poll map[string]any) string {
	s, _ := poll["status"].(string)
	return s
}

// hitlResult wraps a hitl output dict into an ExecuteResult. Keys mirror
// Python exactly: status / replies / session_id / dry_run.
func hitlResult(h *Hitl, status string, replies []any, sessionID string, dryRun bool) *workflow.ExecuteResult {
	return &workflow.ExecuteResult{
		Result: map[string]any{
			"status":     status,
			"replies":    replies,
			"session_id": sessionID,
			"dry_run":    dryRun,
		},
		NextNode: h.NextNode(),
	}
}

// hitlEvalText evaluates a text field like Python's str(evaluate(x) or ""):
// a falsy evaluated value becomes "", everything else is stringified.
func hitlEvalText(ctx context.Context, v any) (string, error) {
	if v == nil {
		return "", nil
	}
	ev, err := expression.Eval(ctx, v)
	if err != nil {
		return "", err
	}
	if !isTruthy(ev) {
		return "", nil
	}
	return gateToStr(ev), nil
}

// hitlEvalImages evaluates the images field to a list of stringified paths.
// A non-list value is rejected fail-closed (Python would iterate it
// pathologically instead).
func hitlEvalImages(ctx context.Context, id string, v any) ([]string, error) {
	if v == nil {
		return nil, nil
	}
	ev, err := expression.Eval(ctx, v)
	if err != nil {
		return nil, fmt.Errorf("eval images: %w", err)
	}
	if ev == nil {
		return nil, nil
	}
	list, ok := ev.([]any)
	if !ok {
		return nil, fmt.Errorf("images must evaluate to a list, got %T", ev)
	}
	out := make([]string, 0, len(list))
	for _, item := range list {
		out = append(out, gateToStr(item))
	}
	return out, nil
}

// hitlExtractReplies ports hitl.py's reply extraction: for each reply dict,
// str(content) if truthy, else str(text) if truthy, else "".
func hitlExtractReplies(v any) ([]any, error) {
	if v == nil {
		return []any{}, nil
	}
	list, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("hitl-server replies 必须是列表，得到 %T", v)
	}
	out := make([]any, 0, len(list))
	for _, item := range list {
		m, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("hitl-server 回复项必须是对象，得到 %T", item)
		}
		switch {
		case isTruthy(m["content"]):
			out = append(out, gateToStr(m["content"]))
		case isTruthy(m["text"]):
			out = append(out, gateToStr(m["text"]))
		default:
			out = append(out, "")
		}
	}
	return out, nil
}
