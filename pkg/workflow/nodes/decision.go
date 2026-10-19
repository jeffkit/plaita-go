package nodes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/llm"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// nodeTypeDecision mirrors plaita-nodes DecisionNode.node_type ("decision").
const nodeTypeDecision types.NodeType = "decision"

func init() {
	workflow.RegisterNodeType(
		nodeTypeDecision,
		func(ctx context.Context) workflow.Node { return new(Decision) },
	)
}

// defaultDecisionTimeoutSecs mirrors DecisionNode.timeout_secs (decision.py:432).
const defaultDecisionTimeoutSecs = 30

// Decision is the closed-decision-space atom: one judgement step over an
// explicit choice space, outputting a typed choice + confidence (the
// "System One" model of plaita-nodes decision.py). Mirrors DecisionNode:
//
//   - choices: string list / [{value, description}] list / {value: description}
//     map — normalized once and enforced: the provider's choice must fall
//     inside the space (no hallucinated third option);
//   - single mode (input) vs batch mode (items — provider judges every item
//     in one call, results aligned by the model-returned index, never by
//     array position, decision.py #8);
//   - provider registry: "llm" (default, OpenAI-compatible /chat/completions
//     with constrained JSON prompt, temperature fixed 0) / "jev" (Jev line
//     protocol POST {base}/v1/systemone) / "jevlike" (Python/torch-only,
//     registered as a fail-closed stub) / custom via RegisterDecisionProvider;
//   - confidence gate: min_confidence marks low_confidence, on_low_confidence
//     = passthrough (default) / default (take default_choice) / error;
//   - dry_run (or globalContext.dry_run) returns the first choice.
//
// Output, single: {"choice", "confidence", "provider", "model",
// "low_confidence", "raw", "dry_run"}; batch swaps "choice"/"confidence" for
// "results": [{"index","choice","confidence","low_confidence"},…] and
// "low_confidence" = any(item).
//
// Error classification (plaita engine parity, _retry.py): DecisionError is a
// RuntimeError → the engine's default is retryable — choice-out-of-space,
// confidence violations, batch index violations, unregistered provider, the
// low-confidence "error" policy all keep the retry budget. Only the endpoint
// chain's *llm.DeterministicError (incomplete endpoint, provider bundle
// failures) is deterministic.
//
// Note (llm.py:188): the decision "llm" provider resolves its endpoint via
// resolve_llm_endpoint WITHOUT a provider bundle name — decision's api_base/
// api_key/model fall back to LLM_* environment variables only.
type Decision struct {
	BaseNode
	Question        any      `json:"question,omitempty"`
	Choices         any      `json:"choices,omitempty"`
	Input           any      `json:"input,omitempty"`
	Items           any      `json:"items,omitempty"`
	Provider        any      `json:"provider,omitempty"` // literal name; expressions fail Validate like Python's str check
	APIBase         any      `json:"api_base,omitempty"`
	APIKey          any      `json:"api_key,omitempty"`
	Model           any      `json:"model,omitempty"`
	ExtraBody       any      `json:"extra_body,omitempty"` // llm provider only
	TimeoutSecs     any      `json:"timeout_secs,omitempty"`
	MinConfidence   *float64 `json:"min_confidence,omitempty"`
	OnLowConfidence string   `json:"on_low_confidence,omitempty"` // passthrough|default|error
	DefaultChoice   any      `json:"default_choice,omitempty"`
	DryRun          bool     `json:"dry_run,omitempty"`
}

// GetType returns the decision node type.
func (d *Decision) GetType() workflow.NodeType { return nodeTypeDecision }

// Validate checks the build-time contract (decision.py:438-455): a literal
// choice space must normalize, a literal provider must be registered,
// on_low_confidence=default needs default_choice, min_confidence must be a
// finite 0..1 number.
func (d *Decision) Validate() error {
	if err := d.BaseNode.Validate(); err != nil {
		return err
	}
	if d.Choices != nil {
		switch d.Choices.(type) {
		case []any, map[string]any:
			if _, err := decisionNormalizeChoices(d.Choices); err != nil {
				return fmt.Errorf("decision %q: 节点配置非法: %w", d.ID, err)
			}
		}
	}
	if p, ok := d.Provider.(string); ok && !decisionProviderRegistered(p) {
		return fmt.Errorf("decision %q: 节点 provider %q 未注册（已注册：%s）",
			d.ID, p, strings.Join(decisionProviderNames(), ", "))
	}
	switch d.OnLowConfidence {
	case "", "passthrough", "default", "error":
	default:
		return fmt.Errorf("decision %q: on_low_confidence 需为 passthrough/default/error，得到 %q",
			d.ID, d.OnLowConfidence)
	}
	if d.OnLowConfidence == "default" && d.DefaultChoice == nil {
		return fmt.Errorf("decision %q: on_low_confidence=default 需要同时配置 default_choice", d.ID)
	}
	if d.MinConfidence != nil {
		if c := *d.MinConfidence; math.IsNaN(c) || math.IsInf(c, 0) || c < 0 || c > 1 {
			return fmt.Errorf("decision %q: 节点配置非法: min_confidence 越界（需 0..1）: %v", d.ID, c)
		}
	}
	return nil
}

// Execute evaluates the expression fields and runs one provider call.
func (d *Decision) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	rawChoices, err := expression.Eval(ctx, d.Choices)
	if err != nil {
		return nil, fmt.Errorf("decision %q: eval choices: %w", d.ID, err)
	}
	choices, err := decisionNormalizeChoices(rawChoices)
	if err != nil {
		return nil, d.fail("%s", err.Error())
	}
	providerName := "llm"
	if d.Provider != nil {
		v, err := expression.Eval(ctx, d.Provider)
		if err != nil {
			return nil, fmt.Errorf("decision %q: eval provider: %w", d.ID, err)
		}
		if isTruthy(v) {
			providerName = decisionStringify(v)
		}
	}
	dry := d.DryRun || decisionGlobalDryRun(ctx)

	if d.Items != nil {
		return d.executeBatch(ctx, choices, providerName, dry)
	}

	if dry {
		return &workflow.ExecuteResult{Result: map[string]any{
			"choice":         choices[0].Value,
			"confidence":     1.0,
			"provider":       providerName,
			"model":          "",
			"low_confidence": false,
			"raw":            "[dry-run]",
			"dry_run":        true,
		}, NextNode: d.NextNode()}, nil
	}

	p, ok := decisionLookupProvider(providerName)
	if !ok {
		return nil, d.fail("decision provider %q 未注册（已注册：%s）",
			providerName, strings.Join(decisionProviderNames(), ", "))
	}
	req, err := d.buildRequest(ctx, choices, providerName, nil)
	if err != nil {
		return nil, err
	}
	out, err := p.Decide(ctx, req)
	if err != nil {
		return nil, d.mapProviderError(err)
	}
	choice, confidence, err := d.checked(out.Choice, out.Confidence, choices, nil)
	if err != nil {
		return nil, err
	}
	low := d.MinConfidence != nil && confidence < *d.MinConfidence
	switch {
	case low && d.lowPolicy() == "error":
		return nil, d.fail("置信度 %.2f 低于阈值 %v（choice=%q）", confidence, *d.MinConfidence, choice)
	case low && d.lowPolicy() == "default":
		if choice, err = d.evalDefaultChoice(ctx); err != nil {
			return nil, err
		}
	}
	return &workflow.ExecuteResult{Result: map[string]any{
		"choice":         choice,
		"confidence":     confidence,
		"provider":       providerName,
		"model":          out.Model,
		"low_confidence": low,
		"raw":            decisionTruncate(out.Raw, 2000),
		"dry_run":        false,
	}, NextNode: d.NextNode()}, nil
}

// executeBatch is the items mode: one provider call judges every item
// (decision.py:499-545).
func (d *Decision) executeBatch(ctx context.Context, choices []DecisionChoice, providerName string, dry bool) (*workflow.ExecuteResult, error) {
	itemsVal, err := expression.Eval(ctx, d.Items)
	if err != nil {
		return nil, fmt.Errorf("decision %q: eval items: %w", d.ID, err)
	}
	items, ok := itemsVal.([]any)
	if !ok || len(items) == 0 {
		return nil, d.fail("items 需为非空列表（批量模式）")
	}

	if dry {
		results := make([]map[string]any, 0, len(items))
		for i := range items {
			results = append(results, map[string]any{
				"index": i, "choice": choices[0].Value,
				"confidence": 1.0, "low_confidence": false,
			})
		}
		return &workflow.ExecuteResult{Result: map[string]any{
			"results":        results,
			"provider":       providerName,
			"model":          "",
			"low_confidence": false,
			"raw":            "[dry-run]",
			"dry_run":        true,
		}, NextNode: d.NextNode()}, nil
	}

	p, ok := decisionLookupProvider(providerName)
	if !ok {
		return nil, d.fail("decision provider %q 未注册（已注册：%s）",
			providerName, strings.Join(decisionProviderNames(), ", "))
	}
	req, err := d.buildRequest(ctx, choices, providerName, items)
	if err != nil {
		return nil, err
	}
	out, err := p.Decide(ctx, req)
	if err != nil {
		return nil, d.mapProviderError(err)
	}
	ordered, err := decisionIndexedBatch(out.Results, len(items))
	if err != nil {
		return nil, d.fail("%s", err.Error())
	}
	results := make([]map[string]any, 0, len(ordered))
	anyLow := false
	for _, entry := range ordered {
		index := entry.Index
		choice, confidence, err := d.checked(entry.Choice, entry.Confidence, choices, &index)
		if err != nil {
			return nil, err
		}
		low := d.MinConfidence != nil && confidence < *d.MinConfidence
		switch {
		case low && d.lowPolicy() == "error":
			return nil, d.fail("第 %d 项置信度 %.2f 低于阈值 %v（choice=%q）",
				index, confidence, *d.MinConfidence, choice)
		case low && d.lowPolicy() == "default":
			if choice, err = d.evalDefaultChoice(ctx); err != nil {
				return nil, err
			}
		}
		anyLow = anyLow || low
		results = append(results, map[string]any{
			"index": index, "choice": choice,
			"confidence": confidence, "low_confidence": low,
		})
	}
	return &workflow.ExecuteResult{Result: map[string]any{
		"results":        results,
		"provider":       providerName,
		"model":          out.Model,
		"low_confidence": anyLow,
		"raw":            decisionTruncate(out.Raw, 2000),
		"dry_run":        false,
	}, NextNode: d.NextNode()}, nil
}

// buildRequest assembles the provider call (decision.py:475-485); items non-
// nil selects batch mode. extra_body rides along only for the llm provider
// (Python kwargs gate, decision.py:483-484).
func (d *Decision) buildRequest(ctx context.Context, choices []DecisionChoice, providerName string, items []any) (*DecisionRequest, error) {
	req := &DecisionRequest{Choices: choices, Items: items}
	var err error
	if req.Question, err = decisionEvalAny(ctx, "question", d.Question); err != nil {
		return nil, err
	}
	if items == nil {
		if req.Input, err = decisionEvalAny(ctx, "input", d.Input); err != nil {
			return nil, err
		}
	}
	if req.APIBase, err = decisionEvalString(ctx, d, "api_base", d.APIBase); err != nil {
		return nil, err
	}
	if req.APIKey, err = decisionEvalString(ctx, d, "api_key", d.APIKey); err != nil {
		return nil, err
	}
	if req.Model, err = decisionEvalString(ctx, d, "model", d.Model); err != nil {
		return nil, err
	}
	req.TimeoutSecs = defaultDecisionTimeoutSecs
	if d.TimeoutSecs != nil {
		v, err := expression.Eval(ctx, d.TimeoutSecs)
		if err != nil {
			return nil, fmt.Errorf("decision %q: eval timeout_secs: %w", d.ID, err)
		}
		if v != nil {
			secs, err := toInt(v)
			if err != nil {
				return nil, fmt.Errorf("decision %q: timeout_secs must be an integer, got %v", d.ID, v)
			}
			req.TimeoutSecs = secs
		}
	}
	if providerName == "llm" && d.ExtraBody != nil {
		v, err := expression.Eval(ctx, d.ExtraBody)
		if err != nil {
			return nil, fmt.Errorf("decision %q: eval extra_body: %w", d.ID, err)
		}
		if m, ok := v.(map[string]any); ok {
			req.ExtraBody = m
		}
	}
	return req, nil
}

// checked is decision.py:547-553 (_checked): the choice must sit inside the
// closed space, the confidence must be a finite 0..1 number.
func (d *Decision) checked(choice string, confidence float64, choices []DecisionChoice, index *int) (string, float64, error) {
	at := ""
	if index != nil {
		at = fmt.Sprintf("第 %d 项 ", *index)
	}
	found := false
	for _, c := range choices {
		if c.Value == choice {
			found = true
			break
		}
	}
	if !found {
		return "", 0, d.fail("provider 返回的 %schoice %q 不在决策空间内", at, choice)
	}
	label := "confidence"
	if index != nil {
		label = fmt.Sprintf("第 %d 项 confidence", *index)
	}
	if math.IsNaN(confidence) || math.IsInf(confidence, 0) {
		return "", 0, d.fail("%s 非有限值: %v", label, confidence)
	}
	if confidence < 0 || confidence > 1 {
		return "", 0, d.fail("%s 越界（需 0..1）: %v", label, confidence)
	}
	return choice, confidence, nil
}

func (d *Decision) lowPolicy() string {
	if d.OnLowConfidence == "" {
		return "passthrough"
	}
	return d.OnLowConfidence
}

// evalDefaultChoice is decision.py:555-559 (_default): the evaluated
// default_choice must not be None.
func (d *Decision) evalDefaultChoice(ctx context.Context) (string, error) {
	v, err := expression.Eval(ctx, d.DefaultChoice)
	if err != nil {
		return "", fmt.Errorf("decision %q: eval default_choice: %w", d.ID, err)
	}
	if v == nil {
		return "", d.fail("on_low_confidence=default 需要配置 default_choice")
	}
	return decisionStringify(v), nil
}

// fail renders a decision-domain error. plaita-nodes raises DecisionError
// (a RuntimeError) for all of these, and the engine's default classification
// retries RuntimeErrors — so the marker stays on (A2R7-04 table).
func (d *Decision) fail(format string, args ...any) error {
	return workflow.RetryableError(fmt.Errorf("decision %q: %s", d.ID, fmt.Sprintf(format, args...)))
}

// mapProviderError keeps only the endpoint chain's deterministic errors
// unmarked; everything else (HTTP failures — decision.py:218 raises
// DecisionError for every status ≥ 400 — transport failures, malformed
// payloads) is retryable, mirroring the Python classification.
func (d *Decision) mapProviderError(err error) error {
	if llm.Retryable(err) {
		return d.fail("%v", err)
	}
	return fmt.Errorf("decision %q: %w", d.ID, err)
}

// ---- provider contract ----

// DecisionChoice is one normalized decision-space entry
// ({"value", "description"}, description "" when absent).
type DecisionChoice struct {
	Value       string
	Description string
}

// DecisionRequest is the provider call contract — the keyword set of
// decision.py:168-169 (question, choices, input, items, api_base, api_key,
// model, timeout_secs; items non-nil = batch mode with input unused).
type DecisionRequest struct {
	Question    any
	Choices     []DecisionChoice
	Input       any
	Items       []any
	APIBase     string
	APIKey      string
	Model       string
	TimeoutSecs int
	ExtraBody   map[string]any
}

// DecisionItemResult is one judged item in batch mode.
type DecisionItemResult struct {
	Index      int
	Choice     string
	Confidence float64
}

// DecisionOutcome mirrors the provider return dict: single mode fills
// Choice/Confidence, batch fills Results; Model/Raw always.
type DecisionOutcome struct {
	Choice     string
	Confidence float64
	Results    []DecisionItemResult
	Model      string
	Raw        string
}

// DecisionProvider computes a structured decision — the Go shape of
// decision.py's DecisionProvider callable and its DECISION_PROVIDERS
// registry.
type DecisionProvider interface {
	Decide(ctx context.Context, req *DecisionRequest) (*DecisionOutcome, error)
}

var (
	decisionRegistryMu sync.RWMutex
	decisionRegistry   = map[string]DecisionProvider{
		"llm":     decisionLLMProvider{},
		"jev":     decisionJevProvider{},
		"jevlike": decisionJevlikeProvider{},
	}
)

// RegisterDecisionProvider installs (or overrides) a provider by name —
// register_decision_provider (decision.py:383-385).
func RegisterDecisionProvider(name string, p DecisionProvider) {
	decisionRegistryMu.Lock()
	defer decisionRegistryMu.Unlock()
	decisionRegistry[name] = p
}

func decisionLookupProvider(name string) (DecisionProvider, bool) {
	decisionRegistryMu.RLock()
	defer decisionRegistryMu.RUnlock()
	p, ok := decisionRegistry[name]
	return p, ok
}

func decisionProviderRegistered(name string) bool {
	_, ok := decisionLookupProvider(name)
	return ok
}

func decisionProviderNames() []string {
	decisionRegistryMu.RLock()
	defer decisionRegistryMu.RUnlock()
	names := make([]string, 0, len(decisionRegistry))
	for name := range decisionRegistry {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// ---- "llm" provider: OpenAI-compatible /chat/completions ----

// Constrained prompts, verbatim from decision.py:171-181.
const (
	decisionLLMSystemSingle = `你是结构化决策引擎。只输出一个 JSON 对象，格式 {"choice": "<决策，必须是候选之一>", "confidence": <0到1的小数>}，不要输出任何其他文字。`
	decisionLLMSystemBatch  = `你是结构化决策引擎。输入是待判定条目列表（[i] 前缀为序号）。只输出一个 JSON 对象，格式 {"results": [{"index": <序号>, "choice": "<决策，必须是候选之一>", "confidence": <0到1的小数>}, ...]}，每个条目一条、不得遗漏，不要输出任何其他文字。`
	decisionNoQuestion      = "（未提供，按内容直接判定）"
)

type decisionLLMProvider struct{}

// Decide speaks /chat/completions with temperature fixed 0 and json_mode
// (decision.py:184-241).
func (decisionLLMProvider) Decide(ctx context.Context, req *DecisionRequest) (*DecisionOutcome, error) {
	// Endpoint chain without a bundle name — decision.py:188 passes no
	// provider_name, so only explicit fields and LLM_* env apply.
	endpoint, err := llm.ResolveEndpoint(req.APIBase, req.APIKey, req.Model, "")
	if err != nil {
		return nil, err
	}

	lines := make([]string, 0, len(req.Choices))
	for _, c := range req.Choices {
		line := "- " + c.Value
		if c.Description != "" {
			line += "：" + c.Description
		}
		lines = append(lines, line)
	}
	question := decisionNoQuestion
	if isTruthy(req.Question) {
		question = decisionStringify(req.Question)
	}
	var messages []llm.Message
	if req.Items != nil {
		listing := make([]string, 0, len(req.Items))
		for i, it := range req.Items {
			listing = append(listing, fmt.Sprintf("[%d] %s", i, decisionRenderPayload(it)))
		}
		messages = []llm.Message{
			{Role: "system", Content: decisionLLMSystemBatch},
			{Role: "user", Content: fmt.Sprintf("## 判定问题\n%s\n\n## 候选决策\n%s\n\n## 待判定列表\n%s",
				question, strings.Join(lines, "\n"), strings.Join(listing, "\n"))},
		}
	} else {
		messages = []llm.Message{
			{Role: "system", Content: decisionLLMSystemSingle},
			{Role: "user", Content: fmt.Sprintf("## 判定问题\n%s\n\n## 候选决策\n%s\n\n## 待判定内容\n%s",
				question, strings.Join(lines, "\n"), decisionRenderPayload(req.Input))},
		}
	}

	zero := 0.0
	chatReq := &llm.ChatRequest{
		APIBase:     endpoint.APIBase,
		APIKey:      endpoint.APIKey,
		Model:       endpoint.Model,
		Messages:    messages,
		Temperature: &zero,
		JSONMode:    true,
		ExtraBody:   req.ExtraBody,
	}
	if req.TimeoutSecs > 0 {
		chatReq.Timeout = time.Duration(req.TimeoutSecs) * time.Second
	}
	resp, err := llm.NewClient().Chat(ctx, chatReq)
	if err != nil {
		// decision.py:218 raises DecisionError for every HTTP ≥ 400 — the
		// message prefix shifts to "LLM 决策请求失败".
		var apiErr *llm.APIError
		if errors.As(err, &apiErr) {
			return nil, fmt.Errorf("LLM 决策请求失败 %d: %s", apiErr.Status, apiErr.Body)
		}
		return nil, err
	}

	space := make([]string, 0, len(req.Choices))
	for _, c := range req.Choices {
		space = append(space, c.Value)
	}
	sort.Strings(space)

	data, err := decisionExtractJSON(resp.Text)
	if err != nil {
		return nil, err
	}
	one := func(entry map[string]any) (string, float64, error) {
		choice := strings.TrimSpace(decisionStringify(entry["choice"]))
		if !decisionSpaceContains(space, choice) {
			return "", 0, fmt.Errorf("LLM 返回的 choice %q 不在决策空间内: %q", choice, space)
		}
		confidence, ok := entry["confidence"]
		if !ok {
			confidence = 0.0
		}
		conf, err := decisionConfidence(confidence, "confidence")
		return choice, conf, err
	}

	if req.Items != nil {
		entries, err := decisionParseBatchEntries(data, len(req.Items))
		if err != nil {
			return nil, err
		}
		results := make([]DecisionItemResult, 0, len(entries))
		for _, entry := range entries {
			choice, conf, err := one(entry)
			if err != nil {
				return nil, err
			}
			results = append(results, DecisionItemResult{Index: entry["_idx"].(int), Choice: choice, Confidence: conf})
		}
		return &DecisionOutcome{Results: results, Model: endpoint.Model, Raw: decisionTruncate(resp.Text, 2000)}, nil
	}

	choice, conf, err := one(data)
	if err != nil {
		return nil, err
	}
	return &DecisionOutcome{Choice: choice, Confidence: conf, Model: endpoint.Model,
		Raw: decisionTruncate(resp.Text, 2000)}, nil
}

// decisionParseBatchEntries validates the model's results array shape the
// way decision.py:116-146 does — including that "index" is an integer (a
// JSON 2.0 is a Python float and is rejected).
func decisionParseBatchEntries(data map[string]any, count int) ([]map[string]any, error) {
	raw, ok := data["results"]
	if !ok {
		raw = nil
	}
	entries, ok := raw.([]any)
	if !ok || len(entries) != count {
		return nil, fmt.Errorf("批量结果数不符：期望 %d 条，实际 %s",
			count, decisionTruncate(decisionStringify(raw), 120))
	}
	out := make([]map[string]any, 0, count)
	for pos, el := range entries {
		entry, ok := el.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("批量结果第 %d 条不是对象: %s", pos, decisionTruncate(decisionStringify(el), 80))
		}
		idxVal, present := entry["index"]
		if !present {
			return nil, fmt.Errorf("批量结果第 %d 条缺少 index: %s", pos, decisionTruncate(decisionStringify(el), 80))
		}
		idx, err := decisionStrictInt(idxVal)
		if err != nil {
			return nil, fmt.Errorf("批量结果第 %d 条的 index 非整数: %s", pos, decisionStringify(idxVal))
		}
		if idx < 0 || idx >= count {
			return nil, fmt.Errorf("批量结果第 %d 条的 index 越界: %d（期望 0..%d）", pos, idx, count-1)
		}
		wrapped := map[string]any{"_idx": idx}
		for k, v := range entry {
			wrapped[k] = v
		}
		out = append(out, wrapped)
	}
	return out, nil
}

// decisionStrictInt accepts only integral JSON numbers (json.Number without
// a fraction/exponent), mirroring Python's isinstance(index, int) over
// json.loads output.
func decisionStrictInt(v any) (int, error) {
	switch t := v.(type) {
	case int:
		return t, nil
	case int64:
		return int(t), nil
	case json.Number:
		return strconv.Atoi(t.String())
	}
	return 0, fmt.Errorf("not an integer: %T", v)
}

func decisionSpaceContains(space []string, choice string) bool {
	for _, s := range space {
		if s == choice {
			return true
		}
	}
	return false
}

// ---- "jev" provider: Jev line protocol POST {base}/v1/systemone ----

type decisionJevProvider struct{}

// Decide mirrors decision.py:244-303: state + choice-type question with the
// decision space as criteria; batch mode submits one question per item.
func (decisionJevProvider) Decide(ctx context.Context, req *DecisionRequest) (*DecisionOutcome, error) {
	base := strings.TrimRight(req.APIBase, "/")
	if base == "" {
		base = strings.TrimRight(os.Getenv("JEV_API_BASE"), "/")
	}
	if base == "" {
		return nil, fmt.Errorf("jev provider 需要 api_base 字段或 JEV_API_BASE 环境变量")
	}
	key := req.APIKey
	if key == "" {
		key = os.Getenv("JEV_API_KEY")
	}
	criteria := make(map[string]string, len(req.Choices))
	for _, c := range req.Choices {
		if c.Description != "" {
			criteria[c.Value] = c.Description
		} else {
			criteria[c.Value] = c.Value
		}
	}
	instructions := "Choose the best option."
	if isTruthy(req.Question) {
		instructions = decisionStringify(req.Question)
	}
	question := func() map[string]any {
		return map[string]any{"type": "choice", "instructions": instructions, "criteria": criteria}
	}
	body := map[string]any{"questions": map[string]any{}}
	if req.Items != nil {
		states := make([]string, 0, len(req.Items))
		questions := body["questions"].(map[string]any)
		for i, it := range req.Items {
			questions[fmt.Sprintf("item_%d", i)] = question()
			states = append(states, decisionRenderPayload(it))
		}
		body["state"] = strings.Join(states, "\n")
	} else {
		body["state"] = decisionRenderPayload(req.Input)
		body["questions"].(map[string]any)["decision"] = question()
	}
	if isTruthy(req.Model) {
		body["model"] = decisionStringify(req.Model)
	}

	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("jev: marshal request: %v", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v1/systemone", bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("jev: new request: %v", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if key != "" {
		httpReq.Header.Set("Authorization", "Bearer "+key)
	}
	client := &http.Client{}
	if req.TimeoutSecs > 0 {
		client.Timeout = time.Duration(req.TimeoutSecs) * time.Second
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("systemone 传输异常: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取 systemone 响应失败: %w", err)
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("systemone 请求失败 %d: %s", resp.StatusCode,
			decisionScrubText(decisionTruncate(string(raw), 300)))
	}
	var data map[string]any
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, fmt.Errorf("解析 systemone 响应 JSON 失败: %w", err)
	}
	model := ""
	if v, ok := data["model"]; ok && v != nil {
		model = decisionStringify(v)
	}
	full := decisionTruncate(decisionMustJSON(data), 2000)

	answers, ok := data["answers"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("systemone 响应缺少 answers 对象: %s",
			decisionTruncate(decisionMustJSON(data), 200))
	}
	answer := func(name string) (string, float64, error) {
		a, ok := answers[name].(map[string]any)
		if !ok {
			return "", 0, fmt.Errorf("systemone answers.%s 缺少 choice: %s",
				name, decisionTruncate(decisionMustJSON(data), 200))
		}
		if _, has := a["choice"]; !has {
			return "", 0, fmt.Errorf("systemone answers.%s 缺少 choice: %s",
				name, decisionTruncate(decisionMustJSON(data), 200))
		}
		confVal, has := a["confidence"]
		if !has {
			confVal = 0.0
		}
		conf, err := decisionConfidence(confVal, "confidence")
		if err != nil {
			return "", 0, fmt.Errorf("systemone answers.%s %v", name, err)
		}
		return decisionStringify(a["choice"]), conf, nil
	}

	if req.Items != nil {
		results := make([]DecisionItemResult, 0, len(req.Items))
		for i := range req.Items {
			choice, conf, err := answer(fmt.Sprintf("item_%d", i))
			if err != nil {
				return nil, err
			}
			results = append(results, DecisionItemResult{Index: i, Choice: choice, Confidence: conf})
		}
		return &DecisionOutcome{Results: results, Model: model, Raw: full}, nil
	}
	choice, conf, err := answer("decision")
	if err != nil {
		return nil, err
	}
	return &DecisionOutcome{Choice: choice, Confidence: conf, Model: model, Raw: full}, nil
}

// ---- "jevlike" provider: fail-closed stub ----

// decisionJevlikeProvider stands in for the local torch scorer
// (decision.py:312-373): the name stays registered so the registry listing
// matches Python, but the scorer itself is Python/torch-only.
type decisionJevlikeProvider struct{}

func (decisionJevlikeProvider) Decide(ctx context.Context, req *DecisionRequest) (*DecisionOutcome, error) {
	if req.Model == "" {
		return nil, fmt.Errorf("jevlike provider 需要 model 字段指向 checkpoint 路径")
	}
	return nil, fmt.Errorf("jevlike provider 需要 jevlike + torch" +
		"（git clone https://github.com/vinnylarouge/jevlike && pip install -e jevlike）；" +
		"Go 引擎未移植本地打分器，请改用 llm/jev provider")
}

// ---- shared helpers ----

// decisionNormalizeChoices ports _normalize_choices (decision.py:55-81).
// Map-shaped spaces are sorted by key for determinism (Python preserves
// insertion order; Go map iteration order is unspecified).
func decisionNormalizeChoices(raw any) ([]DecisionChoice, error) {
	if raw == nil {
		return nil, fmt.Errorf("decision 节点缺少 choices（决策空间不能为空）")
	}
	var items []DecisionChoice
	switch t := raw.(type) {
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			desc := ""
			if t[k] != nil {
				desc = decisionStringify(t[k])
			}
			items = append(items, DecisionChoice{Value: k, Description: desc})
		}
	case []any:
		for _, el := range t {
			switch e := el.(type) {
			case string:
				items = append(items, DecisionChoice{Value: e})
			case map[string]any:
				if e["value"] == nil {
					return nil, fmt.Errorf("choices 元素缺少 value: %s", decisionMustJSON(e))
				}
				desc := ""
				if e["description"] != nil {
					desc = decisionStringify(e["description"])
				}
				items = append(items, DecisionChoice{Value: decisionStringify(e["value"]), Description: desc})
			default:
				return nil, fmt.Errorf("choices 元素类型不支持: %s", decisionStringify(el))
			}
		}
	default:
		return nil, fmt.Errorf("choices 需为字符串列表、{value,description} 列表或 {值: 说明} 映射")
	}
	if len(items) == 0 {
		return nil, fmt.Errorf("决策空间为空（choices 不能是空列表）")
	}
	return items, nil
}

// decisionConfidence ports _confidence (decision.py:95-113): a finite 0..1
// number; NaN/inf/bool/out-of-range are rejected by name, never clamped.
func decisionConfidence(value any, label string) (float64, error) {
	if _, isBool := value.(bool); isBool {
		return 0, fmt.Errorf("%s 非数值: %s", label, decisionStringify(value))
	}
	f, ok := decisionToFloat(value)
	if !ok {
		return 0, fmt.Errorf("%s 非数值: %s", label, decisionStringify(value))
	}
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, fmt.Errorf("%s 非有限值: %s", label, decisionStringify(value))
	}
	if f < 0 || f > 1 {
		return 0, fmt.Errorf("%s 越界（需 0..1）: %s", label, decisionStringify(value))
	}
	return f, nil
}

// decisionToFloat converts numeric scalars and numeric strings (Python
// float() semantics).
func decisionToFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case float32:
		return float64(t), true
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	case json.Number:
		f, err := t.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(t), 64)
		return f, err == nil
	default:
		return 0, false
	}
}

// decisionExtractJSON ports _extract_json (decision.py:149-165): whole-text
// JSON first, then the greedy outermost-brace span (tolerates ```json fences
// and surrounding prose). Decoded with UseNumber so integer-ness survives
// for batch index validation.
func decisionExtractJSON(text string) (map[string]any, error) {
	text = strings.TrimSpace(text)
	if data, ok := decisionUnmarshalObject(text); ok {
		return data, nil
	}
	if start := strings.Index(text, "{"); start >= 0 {
		if end := strings.LastIndex(text, "}"); end > start {
			if data, ok := decisionUnmarshalObject(text[start : end+1]); ok {
				return data, nil
			}
		}
	}
	return nil, fmt.Errorf("LLM 未返回 JSON 决策对象: %s", decisionTruncate(text, 200))
}

func decisionUnmarshalObject(text string) (map[string]any, bool) {
	dec := json.NewDecoder(strings.NewReader(text))
	dec.UseNumber()
	var data map[string]any
	if err := dec.Decode(&data); err != nil {
		return nil, false
	}
	// json.Decoder stops at the first value; Python json.loads rejects
	// trailing garbage, so enforce the same.
	if dec.Decode(&struct{}{}) != io.EOF {
		return nil, false
	}
	return data, true
}

// decisionIndexedBatch ports _indexed_batch (decision.py:116-146) onto typed
// provider results: count, range, duplicates, and completeness — results are
// reordered by index, never by array position.
func decisionIndexedBatch(results []DecisionItemResult, count int) ([]DecisionItemResult, error) {
	if len(results) != count {
		return nil, fmt.Errorf("批量结果数不符：期望 %d 条，实际 %s",
			count, decisionTruncate(fmt.Sprintf("%v", results), 120))
	}
	byIndex := make(map[int]int, count)
	for pos, r := range results {
		if r.Index < 0 || r.Index >= count {
			return nil, fmt.Errorf("批量结果第 %d 条的 index 越界: %d（期望 0..%d）", pos, r.Index, count-1)
		}
		if prev, dup := byIndex[r.Index]; dup {
			return nil, fmt.Errorf("批量结果 index 重复: %d（第 %d 与第 %d 条）", r.Index, prev, pos)
		}
		byIndex[r.Index] = pos
	}
	ordered := make([]DecisionItemResult, 0, count)
	for i := 0; i < count; i++ {
		ordered = append(ordered, results[byIndex[i]])
	}
	return ordered, nil
}

// decisionRenderPayload ports _render_payload (decision.py:84-92).
func decisionRenderPayload(payload any) string {
	if payload == nil {
		return "(空)"
	}
	if s, ok := payload.(string); ok {
		return s
	}
	if b, err := json.Marshal(payload); err == nil {
		return string(b)
	}
	return decisionStringify(payload)
}

func decisionMustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return decisionStringify(v)
	}
	return string(b)
}

// decisionEvalAny evaluates an expression field, nil passing through.
func decisionEvalAny(ctx context.Context, field string, v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	ev, err := expression.Eval(ctx, v)
	if err != nil {
		return nil, fmt.Errorf("eval %s: %w", field, err)
	}
	return ev, nil
}

// decisionEvalString evaluates an endpoint string field with the same ${VAR}
// env pre-pass as the llm node (credential red line).
func decisionEvalString(ctx context.Context, d *Decision, field string, v any) (string, error) {
	if v == nil {
		return "", nil
	}
	if s, isStr := v.(string); isStr && strings.Contains(s, "${") {
		ev, err := llm.Interpolate(s)
		if err != nil {
			return "", fmt.Errorf("decision %q: %s: %w", d.ID, field, err)
		}
		v = ev
	}
	ev, err := expression.Eval(ctx, v)
	if err != nil {
		return "", fmt.Errorf("decision %q: eval %s: %w", d.ID, field, err)
	}
	if ev == nil {
		return "", nil
	}
	s, ok := ev.(string)
	if !ok {
		return "", fmt.Errorf("decision %q: %s 需为字符串，得到 %T", d.ID, field, ev)
	}
	return s, nil
}

func decisionGlobalDryRun(ctx context.Context) bool {
	if rc := runtime.WorkflowRuntimeContext(ctx); rc != nil {
		if gv := rc.GetGlobalVariable("dry_run"); isTruthy(gv) {
			return true
		}
	}
	return false
}

// decisionStringify approximates Python str() over the JSON scalar domain.
func decisionStringify(v any) string { return apiRequestStringify(v) }

// decisionTruncate slices to at most n runes (Python str slicing).
func decisionTruncate(s string, n int) string {
	if n <= 0 {
		return ""
	}
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}

// decisionScrubText reuses the pkg/llm redaction port for error text.
func decisionScrubText(s string) string { return llm.ScrubText(s) }
