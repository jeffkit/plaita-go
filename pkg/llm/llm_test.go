package llm

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func openAIBody(t *testing.T, r *http.Request) map[string]any {
	t.Helper()
	b, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatalf("request body: %v", err)
	}
	return doc
}

func sampleMessages() []Message {
	return []Message{
		{Role: "system", Content: "be terse"},
		{Role: "user", Content: "hi"},
	}
}

func temp(v float64) *float64 { return &v }

func TestClientOpenAIShapeAndParse(t *testing.T) {
	var gotPath, gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		body := openAIBody(t, r)
		if body["model"] != "m-1" {
			t.Errorf("model = %v", body["model"])
		}
		if body["temperature"] != 0.2 {
			t.Errorf("temperature = %v", body["temperature"])
		}
		if body["max_tokens"] != float64(64) {
			t.Errorf("max_tokens = %v", body["max_tokens"])
		}
		rf, ok := body["response_format"].(map[string]any)
		if !ok || rf["type"] != "json_object" {
			t.Errorf("response_format = %v", body["response_format"])
		}
		if body["reasoning_effort"] != "low" {
			t.Errorf("extra_body merge missing: %v", body)
		}
		msgs, _ := body["messages"].([]any)
		if len(msgs) != 2 {
			t.Fatalf("messages = %v", body["messages"])
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"pong"}}],
			"usage":{"prompt_tokens":3,"completion_tokens":1},"model":"m-1-resp"}`))
	}))
	defer srv.Close()

	c := NewClient()
	resp, err := c.Chat(context.Background(), &ChatRequest{
		APIBase: srv.URL + "/", APIKey: "k-1", Model: "m-1",
		Messages: sampleMessages(), Temperature: temp(0.2),
		MaxTokens: func() *int { n := 64; return &n }(), JSONMode: true,
		ExtraBody: map[string]any{"reasoning_effort": "low"},
	})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if gotPath != "/chat/completions" {
		t.Errorf("path = %q", gotPath)
	}
	if gotAuth != "Bearer k-1" {
		t.Errorf("auth = %q", gotAuth)
	}
	if resp.Text != "pong" {
		t.Errorf("text = %q", resp.Text)
	}
	if resp.Model != "m-1-resp" {
		t.Errorf("model = %q", resp.Model)
	}
	if resp.Usage["prompt_tokens"] != float64(3) {
		t.Errorf("usage = %v", resp.Usage)
	}
}

func TestClientOpenAINullContent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":null}}]}`))
	}))
	defer srv.Close()
	resp, err := NewClient().Chat(context.Background(), &ChatRequest{
		APIBase: srv.URL, APIKey: "k", Model: "m", Messages: sampleMessages()})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if resp.Text != "" {
		t.Errorf("text = %q, want empty for null content", resp.Text)
	}
}

func TestClientAnthropicShapeAndParse(t *testing.T) {
	var gotPath, gotKey, gotVersion string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotKey = r.Header.Get("x-api-key")
		gotVersion = r.Header.Get("anthropic-version")
		body := openAIBody(t, r)
		if _, has := body["response_format"]; has {
			t.Errorf("anthropic must not carry response_format")
		}
		if body["system"] != "be terse" {
			t.Errorf("system = %v", body["system"])
		}
		if body["max_tokens"] != float64(4096) {
			t.Errorf("default max_tokens = %v", body["max_tokens"])
		}
		msgs, _ := body["messages"].([]any)
		for _, m := range msgs {
			mm := m.(map[string]any)
			if mm["role"] == "system" {
				t.Errorf("system message leaked into messages: %v", msgs)
			}
		}
		_, _ = w.Write([]byte(`{"content":[{"type":"text","text":"hello "},{"type":"text","text":"world"}],
			"usage":{"input_tokens":2,"output_tokens":3},"model":"claude-x"}`))
	}))
	defer srv.Close()

	resp, err := NewClient().Chat(context.Background(), &ChatRequest{
		Protocol: ProtocolAnthropic,
		APIBase:  srv.URL, APIKey: "ak", Model: "claude-x", Messages: sampleMessages(),
	})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if gotPath != "/v1/messages" {
		t.Errorf("path = %q", gotPath)
	}
	if gotKey != "ak" || gotVersion != "2023-06-01" {
		t.Errorf("headers = %q %q", gotKey, gotVersion)
	}
	if resp.Text != "hello world" {
		t.Errorf("text = %q", resp.Text)
	}
	if resp.Usage["output_tokens"] != float64(3) {
		t.Errorf("usage = %v", resp.Usage)
	}
}

func TestClientErrorClassification(t *testing.T) {
	cases := []struct {
		status    int
		retryable bool
	}{
		{400, false}, {401, false}, {403, false}, {404, false}, {409, false},
		{408, true}, {425, true}, {429, true}, {500, true}, {503, true},
	}
	for _, tc := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(tc.status)
			_, _ = w.Write([]byte(`{"error":"boom"}`))
		}))
		_, err := NewClient().Chat(context.Background(), &ChatRequest{
			APIBase: srv.URL, APIKey: "k", Model: "m", Messages: sampleMessages()})
		srv.Close()
		var apiErr *APIError
		if !errors.As(err, &apiErr) {
			t.Fatalf("status %d: got %T (%v)", tc.status, err, err)
		}
		if apiErr.Status != tc.status {
			t.Errorf("status = %d", apiErr.Status)
		}
		if Retryable(err) != tc.retryable {
			t.Errorf("status %d: Retryable = %v, want %v (err=%v)", tc.status, Retryable(err), tc.retryable, err)
		}
		if !strings.Contains(err.Error(), "LLM 请求失败") {
			t.Errorf("message shape: %v", err)
		}
	}
}

func TestClientErrorDetailScrubbedAndTruncated(t *testing.T) {
	long := strings.Repeat("x", 500)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(401)
		_, _ = w.Write([]byte(`{"upstream":"https://secret-host.example.com/v1/keys?token=abc123","pad":"` + long + `"}`))
	}))
	defer srv.Close()
	_, err := NewClient().Chat(context.Background(), &ChatRequest{
		APIBase: srv.URL, APIKey: "k", Model: "m", Messages: sampleMessages()})
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("got %v", err)
	}
	if strings.Contains(apiErr.Body, "token=abc123") {
		t.Errorf("query leaked: %s", apiErr.Body)
	}
	if strings.Contains(apiErr.Body, "secret-host") && strings.Contains(apiErr.Body, "/v1/keys") {
		t.Errorf("path leaked: %s", apiErr.Body)
	}
	if len([]rune(apiErr.Body)) > 300 {
		t.Errorf("body not truncated: %d runes", len([]rune(apiErr.Body)))
	}
}

func TestClientTransportErrorRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close() // nothing listens → connection refused
	_, err := NewClient().Chat(context.Background(), &ChatRequest{
		APIBase: url, APIKey: "k", Model: "m", Messages: sampleMessages()})
	if err == nil {
		t.Fatal("want transport error")
	}
	if !Retryable(err) {
		t.Errorf("transport error must be retryable: %v", err)
	}
	if strings.Contains(err.Error(), "token=") {
		t.Errorf("raw URL leaked: %v", err)
	}
}

func TestClientTimeoutRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
	}))
	defer srv.Close()
	_, err := NewClient().Chat(context.Background(), &ChatRequest{
		APIBase: srv.URL, APIKey: "k", Model: "m", Messages: sampleMessages(),
		Timeout: 30 * time.Millisecond,
	})
	if err == nil {
		t.Fatal("want timeout error")
	}
	if !Retryable(err) {
		t.Errorf("timeout must be retryable: %v", err)
	}
}

func TestClientValidationErrors(t *testing.T) {
	c := NewClient()
	if _, err := c.Chat(context.Background(), &ChatRequest{Messages: sampleMessages()}); !IsDeterministicErr(err) {
		t.Errorf("missing model: %v", err)
	}
	if _, err := c.Chat(context.Background(), &ChatRequest{Model: "m"}); !IsDeterministicErr(err) {
		t.Errorf("missing messages: %v", err)
	}
	if _, err := c.Chat(context.Background(), &ChatRequest{Model: "m", Messages: sampleMessages(), Protocol: "grpc"}); !IsDeterministicErr(err) {
		t.Errorf("unknown protocol: %v", err)
	}
}

// IsDeterministicErr reports whether err is (or wraps) a DeterministicError.
func IsDeterministicErr(err error) bool {
	var det *DeterministicError
	return errors.As(err, &det)
}

func writeProviders(t *testing.T, doc string) {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("PLAITA_HOME", dir)
	if err := os.WriteFile(filepath.Join(dir, "providers.json"), []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestResolveProviderBundle(t *testing.T) {
	writeProviders(t, `{"providers": {
		"a": {"type": "openai", "apiBase": "https://a.example.com/v1", "apiKey": "ka", "model": "ma"},
		"b": {"base": "https://b.example.com", "keyEnv": "B_KEY", "model": "mb"},
		"c": {"apiBase": "https://c/${NETLOC}", "apiKey": "${C_KEY}"}
	}}`)
	t.Setenv("B_KEY", "kb")
	t.Setenv("NETLOC", "c.example.com")
	t.Setenv("C_KEY", "kc")

	b, err := ResolveProvider("a")
	if err != nil {
		t.Fatalf("a: %v", err)
	}
	if b.Type != "openai" || b.APIBase != "https://a.example.com/v1" || b.APIKey != "ka" || b.Model != "ma" {
		t.Errorf("a = %+v", b)
	}
	b, err = ResolveProvider("b")
	if err != nil {
		t.Fatalf("b: %v", err)
	}
	if b.APIBase != "https://b.example.com" || b.APIKey != "kb" {
		t.Errorf("b = %+v", b)
	}
	b, err = ResolveProvider("c")
	if err != nil {
		t.Fatalf("c: %v", err)
	}
	if b.APIBase != "https://c/c.example.com" || b.APIKey != "kc" {
		t.Errorf("c interpolation: %+v", b)
	}
}

func TestResolveProviderErrors(t *testing.T) {
	writeProviders(t, `{"providers": {
		"envkey": {"apiBase": "https://x", "keyEnv": "MISSING_KEY_ENV"},
		"emptykey": {"apiBase": "https://x", "keyEnv": "EMPTY_KEY_ENV"},
		"weird": {"apiBase": "https://x", "apiKey": "${NOPE_VAR}"}
	}}`)
	t.Setenv("EMPTY_KEY_ENV", "")
	if _, err := ResolveProvider("nope"); err == nil || !IsDeterministicErr(err) {
		t.Errorf("unknown provider: %v", err)
	}
	_, err := ResolveProvider("envkey")
	if err == nil || !IsDeterministicErr(err) || !strings.Contains(err.Error(), "MISSING_KEY_ENV") {
		t.Errorf("missing keyEnv: %v", err)
	}
	_, err = ResolveProvider("emptykey")
	if err == nil || !strings.Contains(err.Error(), "为空") {
		t.Errorf("empty keyEnv: %v", err)
	}
	_, err = ResolveProvider("weird")
	if err == nil || !IsDeterministicErr(err) || !strings.Contains(err.Error(), "NOPE_VAR") {
		t.Errorf("interpolation fail-fast: %v", err)
	}
}

func TestResolveEndpointPriority(t *testing.T) {
	writeProviders(t, `{"providers": {"p": {"apiBase": "https://bundle/", "apiKey": "kb", "model": "mb", "type": "anthropic"}}}`)
	t.Setenv("LLM_API_BASE", "https://envbase")
	t.Setenv("LLM_API_KEY", "kenv")
	t.Setenv("LLM_MODEL", "menv")

	// explicit values win over bundle and env; the bundle's type still
	// declares the wire protocol of that provider
	ep, err := ResolveEndpoint("https://x/", "kx", "mx", "p")
	if err != nil {
		t.Fatal(err)
	}
	if ep.APIBase != "https://x" || ep.APIKey != "kx" || ep.Model != "mx" {
		t.Errorf("explicit priority: %+v", ep)
	}
	if ep.Protocol != ProtocolAnthropic {
		t.Errorf("bundle type must pick protocol: %v", ep.Protocol)
	}

	// bundle fills the gaps, protocol from bundle type, trailing slash gone
	ep, err = ResolveEndpoint("", "", "", "p")
	if err != nil {
		t.Fatal(err)
	}
	if ep.APIBase != "https://bundle" || ep.APIKey != "kb" || ep.Model != "mb" {
		t.Errorf("bundle fill: %+v", ep)
	}
	if ep.Protocol != ProtocolAnthropic {
		t.Errorf("bundle type must pick protocol: %v", ep.Protocol)
	}

	// env fallbacks
	ep, err = ResolveEndpoint("", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if ep.APIBase != "https://envbase" || ep.APIKey != "kenv" || ep.Model != "menv" {
		t.Errorf("env fallback: %+v", ep)
	}
	if ep.Protocol != ProtocolOpenAI {
		t.Errorf("no-bundle endpoint must stay openai: %v", ep.Protocol)
	}

	// incomplete → deterministic
	t.Setenv("LLM_API_KEY", "")
	_, err = ResolveEndpoint("https://x", "", "m", "")
	if err == nil || !IsDeterministicErr(err) || !strings.Contains(err.Error(), "LLM 端点不完整") {
		t.Errorf("incomplete endpoint: %v", err)
	}

	// unknown provider → deterministic
	_, err = ResolveEndpoint("", "", "", "ghost")
	if err == nil || !IsDeterministicErr(err) || !strings.Contains(err.Error(), "解析失败") {
		t.Errorf("unknown provider resolve: %v", err)
	}
}

func TestResolveEndpointEnvVarInterpolation(t *testing.T) {
	t.Setenv("SECRET_KEY_MAT", "sk-live-1")
	ep, err := ResolveEndpoint("https://x", "${SECRET_KEY_MAT}", "m", "")
	if err != nil {
		t.Fatal(err)
	}
	if ep.APIKey != "sk-live-1" {
		t.Errorf("interpolated key = %q", ep.APIKey)
	}
	_, err = ResolveEndpoint("https://x", "${SECRET_KEY_MISSING_XYZ}", "m", "")
	if err == nil || !IsDeterministicErr(err) {
		t.Errorf("missing interpolated var: %v", err)
	}
}

// TestClientResponseSizeCap pins the outbound read ceiling (plaita-go#72):
// a 64KiB completion body behind a 4096-byte cap fails named with the
// ceiling in the message and classifies deterministic — pre-fix, Chat
// swallowed the whole body and returned it as the text.
func TestClientResponseSizeCap(t *testing.T) {
	t.Setenv("PLAITA_HTTP_MAX_RESPONSE_BYTES", "4096")
	bigBody := `{"choices":[{"message":{"role":"assistant","content":"` +
		strings.Repeat("x", 64<<10) + `"}}],"usage":{"total_tokens":7}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(bigBody))
	}))
	defer srv.Close()

	_, err := NewClient().Chat(context.Background(), &ChatRequest{
		APIBase: srv.URL, APIKey: "k-1", Model: "m-1", Messages: sampleMessages()})
	if err == nil {
		t.Fatalf("64KiB body behind a 4096 cap: Chat succeeded (pre-fix swallow)")
	}
	if !strings.Contains(err.Error(), "maxResponseBytes=4096") {
		t.Errorf("message = %q, want the ceiling value", err)
	}
	if Retryable(err) {
		t.Errorf("over-limit is deterministic (retry re-downloads the same body), got retryable")
	}

	small := `{"choices":[{"message":{"role":"assistant","content":"ok"}}],"usage":{"total_tokens":1}}`
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(small))
	}))
	defer srv2.Close()
	resp, err := NewClient().Chat(context.Background(), &ChatRequest{
		APIBase: srv2.URL, APIKey: "k-1", Model: "m-1", Messages: sampleMessages()})
	if err != nil || resp.Text != "ok" {
		t.Errorf("small body: resp=%v err=%v, want untouched success", resp, err)
	}
}
