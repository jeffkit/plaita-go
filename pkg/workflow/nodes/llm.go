package nodes

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/llm"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// nodeTypeLlm mirrors plaita-nodes LlmNode.node_type ("llm").
const nodeTypeLlm types.NodeType = "llm"

func init() {
	workflow.RegisterNodeType(
		nodeTypeLlm,
		func(ctx context.Context) workflow.Node { return new(Llm) },
	)
}

// Defaults from plaita-nodes llm.py.
const (
	// defaultLlmTemperature mirrors LlmNode.temperature (llm.py:100).
	defaultLlmTemperature = 0.2
	// defaultLlmTimeoutSecs mirrors LlmNode.timeout_secs (llm.py:106).
	defaultLlmTimeoutSecs = 120
)

// Llm is the single-completion atom: input messages → output text, one
// request, no agentic loop (the boundary agentrun/llm draws in
// plaita-nodes llm.py:1-15). Mirrors plaita-nodes LlmNode:
//
//   - messages ([{"role","content"},…]， content supports expressions) or the
//     prompt/system shorthand assembled into one user / system message;
//   - endpoint three-level fallback: explicit api_base/api_key/model >
//     providers.json bundle (provider field) > LLM_API_BASE/LLM_API_KEY/
//     LLM_MODEL environment variables (llm.py:51-73);
//   - temperature (default 0.2), max_tokens, json_mode, extra_body merged
//     into the request body verbatim, timeout_secs (default 120);
//   - dry_run (or globalContext.dry_run) short-circuits to fake text.
//
// Output: {"text", "model", "usage", "dry_run"}.
//
// Error classification (llm.py:145-158): missing messages / incomplete
// endpoint / provider resolution → deterministic (no retry); transport
// failures and 429/5xx/408/425 → retryable; other 4xx → deterministic;
// a response missing choices[].message.content → retryable (LlmError).
//
// Wire protocol: OpenAI-compatible /chat/completions (the only protocol
// plaita-nodes speaks); a providers.json bundle with "type": "anthropic"
// switches the client to the Anthropic /v1/messages shape.
type Llm struct {
	BaseNode
	Messages    any      `json:"messages,omitempty"` // expression → [{"role","content"},…]
	Prompt      any      `json:"prompt,omitempty"`
	System      any      `json:"system,omitempty"`
	Provider    any      `json:"provider,omitempty"`
	APIBase     any      `json:"api_base,omitempty"`
	APIKey      any      `json:"api_key,omitempty"`
	Model       any      `json:"model,omitempty"`
	Temperature *float64 `json:"temperature,omitempty"` // default 0.2
	MaxTokens   any      `json:"max_tokens,omitempty"`
	JSONMode    bool     `json:"json_mode,omitempty"`
	ExtraBody   any      `json:"extra_body,omitempty"`
	TimeoutSecs any      `json:"timeout_secs,omitempty"` // default 120
	DryRun      bool     `json:"dry_run,omitempty"`
}

// GetType returns the llm node type.
func (n *Llm) GetType() workflow.NodeType { return nodeTypeLlm }

// Execute evaluates the expression fields and performs one completion.
func (n *Llm) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	dry := n.DryRun
	if rc := runtime.WorkflowRuntimeContext(ctx); rc != nil {
		if gv := rc.GetGlobalVariable("dry_run"); isTruthy(gv) {
			dry = true
		}
	}
	if dry {
		model := ""
		if n.Model != nil {
			v, err := expression.Eval(ctx, n.Model)
			if err != nil {
				return nil, fmt.Errorf("llm %q: eval model: %w", n.ID, err)
			}
			if isTruthy(v) {
				model = llmStringify(v)
			}
		}
		return &workflow.ExecuteResult{Result: map[string]any{
			"text":    "[dry-run] llm completion",
			"model":   model,
			"usage":   nil,
			"dry_run": true,
		}, NextNode: n.NextNode()}, nil
	}

	messages, err := n.buildMessages(ctx)
	if err != nil {
		return nil, err
	}

	apiBase, err := llmEvalString(ctx, n, "api_base", n.APIBase)
	if err != nil {
		return nil, err
	}
	apiKey, err := llmEvalString(ctx, n, "api_key", n.APIKey)
	if err != nil {
		return nil, err
	}
	model, err := llmEvalString(ctx, n, "model", n.Model)
	if err != nil {
		return nil, err
	}
	provider, err := llmEvalString(ctx, n, "provider", n.Provider)
	if err != nil {
		return nil, err
	}
	endpoint, err := llm.ResolveEndpoint(apiBase, apiKey, model, provider)
	if err != nil {
		return nil, fmt.Errorf("llm %q: %w", n.ID, err)
	}

	req := &llm.ChatRequest{
		Protocol: endpoint.Protocol,
		APIBase:  endpoint.APIBase,
		APIKey:   endpoint.APIKey,
		Model:    endpoint.Model,
		Messages: messages,
		JSONMode: n.JSONMode,
	}
	if n.Temperature != nil {
		t := *n.Temperature
		req.Temperature = &t
	} else {
		t := defaultLlmTemperature
		req.Temperature = &t
	}
	if n.MaxTokens != nil {
		v, err := expression.Eval(ctx, n.MaxTokens)
		if err != nil {
			return nil, fmt.Errorf("llm %q: eval max_tokens: %w", n.ID, err)
		}
		mt, err := toInt(v)
		if err != nil {
			return nil, fmt.Errorf("llm %q: max_tokens must be an integer, got %v", n.ID, v)
		}
		req.MaxTokens = &mt
	}
	if n.ExtraBody != nil {
		v, err := expression.Eval(ctx, n.ExtraBody)
		if err != nil {
			return nil, fmt.Errorf("llm %q: eval extra_body: %w", n.ID, err)
		}
		// Python: only a dict is merged, anything else is silently ignored.
		if m, ok := v.(map[string]any); ok {
			req.ExtraBody = m
		}
	}
	if n.TimeoutSecs != nil {
		v, err := expression.Eval(ctx, n.TimeoutSecs)
		if err != nil {
			return nil, fmt.Errorf("llm %q: eval timeout_secs: %w", n.ID, err)
		}
		if v != nil {
			secs, err := toInt(v)
			if err != nil {
				return nil, fmt.Errorf("llm %q: timeout_secs must be an integer, got %v", n.ID, v)
			}
			req.Timeout = llmSecondsDuration(secs)
		}
	}

	resp, err := llm.NewClient().Chat(ctx, req)
	if err != nil {
		if llm.Retryable(err) {
			// 429/5xx/408/425 and transport failures keep the retry budget
			// (A2R7-04); deterministic 4xx come back unmarked.
			return nil, fmt.Errorf("llm %q: %w", n.ID, workflow.RetryableError(err))
		}
		return nil, fmt.Errorf("llm %q: %w", n.ID, err)
	}
	return &workflow.ExecuteResult{Result: map[string]any{
		"text":    resp.Text,
		"model":   endpoint.Model,
		"usage":   usageOrNil(resp.Usage),
		"dry_run": false,
	}, NextNode: n.NextNode()}, nil
}

// buildMessages evaluates the messages field, or assembles the prompt/system
// shorthand into [{"role":"system"?},{"role":"user"}] (llm.py:122-132).
func (n *Llm) buildMessages(ctx context.Context) ([]llm.Message, error) {
	if n.Messages != nil {
		v, err := expression.Eval(ctx, n.Messages)
		if err != nil {
			return nil, fmt.Errorf("llm %q: eval messages: %w", n.ID, err)
		}
		items, ok := v.([]any)
		if !ok {
			if v == nil {
				return nil, fmt.Errorf("llm %q: 节点缺少消息（prompt/messages）", n.ID)
			}
			return nil, fmt.Errorf("llm %q: messages 需为消息数组，得到 %T", n.ID, v)
		}
		if len(items) == 0 {
			return nil, fmt.Errorf("llm %q: 节点缺少消息（prompt/messages）", n.ID)
		}
		msgs := make([]llm.Message, 0, len(items))
		for i, el := range items {
			m, ok := el.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("llm %q: messages 第 %d 条需为 {role, content} 对象，得到 %T", n.ID, i, el)
			}
			role, _ := m["role"].(string)
			msgs = append(msgs, llm.Message{Role: role, Content: m["content"]})
		}
		return msgs, nil
	}

	msgs := make([]llm.Message, 0, 2)
	if n.System != nil {
		v, err := expression.Eval(ctx, n.System)
		if err != nil {
			return nil, fmt.Errorf("llm %q: eval system: %w", n.ID, err)
		}
		if isTruthy(v) {
			msgs = append(msgs, llm.Message{Role: "system", Content: llmStringify(v)})
		}
	}
	prompt := ""
	if n.Prompt != nil {
		v, err := expression.Eval(ctx, n.Prompt)
		if err != nil {
			return nil, fmt.Errorf("llm %q: eval prompt: %w", n.ID, err)
		}
		if isTruthy(v) {
			prompt = llmStringify(v)
		}
	}
	msgs = append(msgs, llm.Message{Role: "user", Content: prompt})
	return msgs, nil
}

// llmEvalString evaluates an endpoint string field; nil evaluates to "".
// Credential red line: a string carrying ${VAR} tokens interpolates from the
// process environment first (plaita-nodes config.interpolate semantics,
// fail-fast on a missing variable) — "${KEY}" must not be mistaken for a
// plaita expression just because it starts with "$".
func llmEvalString(ctx context.Context, n *Llm, field string, v any) (string, error) {
	if v == nil {
		return "", nil
	}
	if s, isStr := v.(string); isStr && strings.Contains(s, "${") {
		ev, err := llm.Interpolate(s)
		if err != nil {
			return "", fmt.Errorf("llm %q: %s: %w", n.ID, field, err)
		}
		v = ev
	}
	ev, err := expression.Eval(ctx, v)
	if err != nil {
		return "", fmt.Errorf("llm %q: eval %s: %w", n.ID, field, err)
	}
	if ev == nil {
		return "", nil
	}
	s, ok := ev.(string)
	if !ok {
		return "", fmt.Errorf("llm %q: %s 需为字符串，得到 %T", n.ID, field, ev)
	}
	return s, nil
}

// llmSecondsDuration converts a timeout_secs value; non-positive means no
// per-request deadline (Python passes the int straight to requests, whose
// timeout<=0 also disables the socket budget).
func llmSecondsDuration(secs int) time.Duration {
	if secs <= 0 {
		return 0
	}
	return time.Duration(secs) * time.Second
}

// llmStringify approximates Python str() over the JSON scalar domain for the
// prompt/system shorthand content.
func llmStringify(v any) string { return apiRequestStringify(v) }
