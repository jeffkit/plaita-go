package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// Message is one chat message. Content keeps the raw JSON shape (string for
// plain completions; block arrays pass through untouched, mirroring Python
// where the node forwards messages as-is).
type Message struct {
	Role    string `json:"role"`
	Content any    `json:"content"`
}

// ChatRequest is one chat-completions request.
type ChatRequest struct {
	Protocol Protocol // "" = ProtocolOpenAI
	APIBase  string   // no trailing slash (ResolveEndpoint guarantees it)
	APIKey   string
	Model    string
	Messages []Message

	Temperature *float64       // nil omits the field (Anthropic API default)
	MaxTokens   *int           // nil: OpenAI omits; Anthropic defaults to 4096 (API requires it)
	JSONMode    bool           // OpenAI: response_format {"type":"json_object"}; Anthropic has no equivalent, ignored
	ExtraBody   map[string]any // merged into the request body last, verbatim

	// Timeout is the per-request budget (requests' timeout= semantics: a
	// transport-level failure, classified retryable). The caller's ctx stays
	// the engine-level cancellation path.
	Timeout time.Duration
}

// ChatResponse is a successful completion. Usage is the endpoint's usage
// object verbatim (nil when absent); Text is the assistant text.
type ChatResponse struct {
	Text  string
	Model string
	Usage map[string]any
}

// Client issues chat-completions requests. The zero value is ready to use.
type Client struct {
	// HTTP overrides the transport client; nil uses http.DefaultClient.
	HTTP *http.Client
}

// NewClient returns a client on the default transport.
func NewClient() *Client { return &Client{HTTP: http.DefaultClient} }

// Chat performs one completion. Errors are classified for the engine's retry
// gate — see Retryable: deterministic endpoint/payload problems come back as
// *DeterministicError, non-2xx responses as *APIError, transport and
// malformed-payload failures as plain errors (retryable, Python RuntimeError
// parity).
func (c *Client) Chat(ctx context.Context, req *ChatRequest) (*ChatResponse, error) {
	if req == nil {
		return nil, detErr("llm: nil chat request")
	}
	if req.Model == "" {
		return nil, detErr("llm: model is required")
	}
	if len(req.Messages) == 0 {
		return nil, detErr("llm: 节点缺少消息（prompt/messages）")
	}
	if req.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, req.Timeout)
		defer cancel()
	}
	protocol := req.Protocol
	if protocol == "" {
		protocol = ProtocolOpenAI
	}
	switch protocol {
	case ProtocolAnthropic:
		return c.chatAnthropic(ctx, req)
	case ProtocolOpenAI:
		return c.chatOpenAI(ctx, req)
	default:
		return nil, detErr("llm: unknown protocol %q", string(protocol))
	}
}

// chatOpenAI speaks the OpenAI-compatible /chat/completions protocol — the
// wire shape of plaita-nodes llm.py:146-148.
func (c *Client) chatOpenAI(ctx context.Context, req *ChatRequest) (*ChatResponse, error) {
	body := map[string]any{
		"model":    req.Model,
		"messages": req.Messages,
	}
	if req.Temperature != nil {
		body["temperature"] = *req.Temperature
	}
	if req.MaxTokens != nil {
		body["max_tokens"] = *req.MaxTokens
	}
	if req.JSONMode {
		body["response_format"] = map[string]any{"type": "json_object"}
	}
	for k, v := range req.ExtraBody {
		body[k] = v
	}
	header := http.Header{}
	header.Set("Authorization", "Bearer "+req.APIKey)
	raw, err := c.post(ctx, req, "/chat/completions", body, header)
	if err != nil {
		return nil, err
	}
	var doc struct {
		Choices []struct {
			Message struct {
				Content any `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		Usage map[string]any `json:"usage"`
		Model string         `json:"model"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("解析 LLM 响应 JSON 失败: %w", err)
	}
	if len(doc.Choices) == 0 {
		return nil, fmt.Errorf("LLM 响应缺少 choices[].message.content: %s",
			truncateRunes(string(raw), 200))
	}
	return &ChatResponse{
		Text:  contentText(doc.Choices[0].Message.Content),
		Model: doc.Model,
		Usage: doc.Usage,
	}, nil
}

// chatAnthropic speaks the Anthropic /v1/messages protocol: system messages
// move to the top-level "system" field, max_tokens is mandatory (default
// 4096 when unset), auth rides the x-api-key header.
func (c *Client) chatAnthropic(ctx context.Context, req *ChatRequest) (*ChatResponse, error) {
	var system []string
	messages := make([]Message, 0, len(req.Messages))
	for _, m := range req.Messages {
		if m.Role == "system" {
			if s, ok := m.Content.(string); ok && s != "" {
				system = append(system, s)
			}
			continue
		}
		messages = append(messages, m)
	}
	maxTokens := 4096
	if req.MaxTokens != nil {
		maxTokens = *req.MaxTokens
	}
	body := map[string]any{
		"model":      req.Model,
		"messages":   messages,
		"max_tokens": maxTokens,
	}
	if len(system) > 0 {
		body["system"] = strings.Join(system, "\n")
	}
	if req.Temperature != nil {
		body["temperature"] = *req.Temperature
	}
	for k, v := range req.ExtraBody {
		body[k] = v
	}
	header := http.Header{}
	header.Set("x-api-key", req.APIKey)
	header.Set("anthropic-version", "2023-06-01")
	raw, err := c.post(ctx, req, "/v1/messages", body, header)
	if err != nil {
		return nil, err
	}
	var doc struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		Usage map[string]any `json:"usage"`
		Model string         `json:"model"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("解析 LLM 响应 JSON 失败: %w", err)
	}
	if len(doc.Content) == 0 {
		return nil, fmt.Errorf("anthropic 响应缺少 content[].text: %s",
			truncateRunes(string(raw), 200))
	}
	var text strings.Builder
	for _, block := range doc.Content {
		text.WriteString(block.Text)
	}
	return &ChatResponse{
		Text:  text.String(),
		Model: doc.Model,
		Usage: doc.Usage,
	}, nil
}

// llmMaxResponseBytesEnv is shared with the http node's ceiling: one
// operator knob governs every outbound response-body read (plaita-go#72).
const llmMaxResponseBytesEnv = "PLAITA_HTTP_MAX_RESPONSE_BYTES"

// defaultLLMMaxResponseBytes mirrors the http node's 10MiB default.
const defaultLLMMaxResponseBytes = 10 * 1024 * 1024

// maxResponseBytes parses llmMaxResponseBytesEnv; unset/invalid/non-positive
// falls back to the 10MiB default (same tolerance as the http node).
func maxResponseBytes() int {
	raw := strings.TrimSpace(os.Getenv(llmMaxResponseBytesEnv))
	if raw == "" {
		return defaultLLMMaxResponseBytes
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return defaultLLMMaxResponseBytes
	}
	return v
}

// ResponseTooLargeError marks an over-limit completions response
// (plaita-go#72): the read stops at max+1 bytes so a degraded or hostile
// provider cannot balloon worker memory. Deterministic — retrying
// re-downloads the same body and exceeds again.
type ResponseTooLargeError struct{ Max int }

// Error renders the ceiling and the escape hatch.
func (e *ResponseTooLargeError) Error() string {
	return fmt.Sprintf("LLM 响应体超过上限 maxResponseBytes=%d；可用 %s 放宽", e.Max, llmMaxResponseBytesEnv)
}

// Retryable pins the deterministic class for the engine's retryClassifier
// bridge and llm.Retryable.
func (e *ResponseTooLargeError) Retryable() bool { return false }

// readCapped reads at most max bytes; one byte more → *ResponseTooLargeError.
// The +1 read is deliberate: a single Read(max) can mistake a big body for
// EOF and silently truncate.
func readCapped(r io.Reader, max int) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, int64(max)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > max {
		return nil, &ResponseTooLargeError{Max: max}
	}
	return data, nil
}

// post marshals the body, issues the request and classifies the response.
func (c *Client) post(ctx context.Context, req *ChatRequest, path string, body map[string]any, header http.Header) ([]byte, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, detErr("llm: marshal request body: %v", err)
	}
	endpoint := strings.TrimRight(req.APIBase, "/") + path
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, detErr("llm: new request: %v", err)
	}
	httpReq.Header = header.Clone()
	if httpReq.Header.Get("Content-Type") == "" {
		httpReq.Header.Set("Content-Type", "application/json")
	}
	client := c.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		// Transport failure (incl. per-request timeout): retryable, Python
		// requests.RequestException → RuntimeError parity; the URL is
		// scrubbed before it lands in the message (redaction discipline).
		return nil, fmt.Errorf("LLM 传输异常 (%s): %w", scrubText(endpoint), err)
	}
	defer resp.Body.Close()
	raw, err := readCapped(resp.Body, maxResponseBytes())
	if err != nil {
		return nil, fmt.Errorf("读取 LLM 响应失败: %w", err)
	}
	if resp.StatusCode >= 400 {
		return nil, &APIError{
			Status: resp.StatusCode,
			Body:   scrubText(truncateRunes(string(raw), 300)),
		}
	}
	return raw, nil
}

// contentText normalizes choices[].message.content: null → "" (Python
// `content or ""`), string as-is; any other JSON shape is serialized so the
// field stays a string (Python would carry the raw list/map through —
// documented, pathological divergence).
func contentText(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case json.Number:
		return t.String()
	case bool:
		if t {
			return "True"
		}
		return "False"
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return fmt.Sprintf("%v", t)
		}
		return string(b)
	}
}
