package nodes

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

	"github.com/jeffkit/plaita-go/internal/runtime"
)

func llmContext(t *testing.T, input, globals map[string]any) context.Context {
	t.Helper()
	return runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput:      input,
		ExecuteNodeResults: map[string]any{},
		EnvVariables:       map[string]any{},
		GlobalVariables:    globals,
	})
}

// llmIsRetryable reports whether err carries the workflow.RetryableError
// marker ("retryable: " prefix), the engine's transient classification.
func llmIsRetryable(err error) bool {
	for err != nil {
		if strings.HasPrefix(err.Error(), "retryable: ") {
			return true
		}
		err = errors.Unwrap(err)
	}
	return false
}

func llmFakeServer(t *testing.T, status int, body string, hook func(r *http.Request, req map[string]any)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var doc map[string]any
		_ = json.Unmarshal(raw, &doc)
		if hook != nil {
			hook(r, doc)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
}

func llmOKBody(content string) string {
	return `{"choices":[{"message":{"role":"assistant","content":"` + content + `"}}],"usage":{"total_tokens":7}}`
}

func TestLlmExecutePromptShorthand(t *testing.T) {
	var gotAuth, gotPath string
	srv := llmFakeServer(t, 200, llmOKBody("摘要结果"), func(r *http.Request, req map[string]any) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		msgs := req["messages"].([]any)
		if len(msgs) != 2 {
			t.Errorf("messages = %v", msgs)
			return
		}
		sys := msgs[0].(map[string]any)
		usr := msgs[1].(map[string]any)
		if sys["role"] != "system" || sys["content"] != "你是摘要助手" {
			t.Errorf("system = %v", sys)
		}
		if usr["role"] != "user" || usr["content"] != "总结一下" {
			t.Errorf("user = %v", usr)
		}
		if req["temperature"] != 0.2 {
			t.Errorf("default temperature = %v", req["temperature"])
		}
		if _, has := req["max_tokens"]; has {
			t.Errorf("max_tokens must be absent when unset")
		}
		if _, has := req["response_format"]; has {
			t.Errorf("response_format must be absent without json_mode")
		}
	})
	defer srv.Close()

	node := &Llm{
		BaseNode: BaseNode{ID: "llm1"},
		System:   "你是摘要助手",
		Prompt:   "总结一下",
		APIBase:  srv.URL,
		APIKey:   "sk-test",
		Model:    "test-model",
	}
	res, err := node.Execute(llmContext(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out := res.Result.(map[string]any)
	if gotPath != "/chat/completions" || gotAuth != "Bearer sk-test" {
		t.Errorf("wire = %q %q", gotPath, gotAuth)
	}
	if out["text"] != "摘要结果" || out["model"] != "test-model" {
		t.Errorf("out = %v", out)
	}
	usage, _ := out["usage"].(map[string]any)
	if usage["total_tokens"] != float64(7) {
		t.Errorf("usage = %v", out["usage"])
	}
	if dry, _ := out["dry_run"].(bool); dry {
		t.Errorf("dry_run = %v", out["dry_run"])
	}
}

func TestLlmExecuteMessagesFieldWithExpression(t *testing.T) {
	var gotMsgs []any
	srv := llmFakeServer(t, 200, llmOKBody("ok"), func(r *http.Request, req map[string]any) {
		gotMsgs = req["messages"].([]any)
	})
	defer srv.Close()

	node := &Llm{
		BaseNode: BaseNode{ID: "llm1"},
		Messages: []any{
			map[string]any{"role": "user", "content": "分类：{% $INPUT.text %}"},
		},
		APIBase: srv.URL, APIKey: "k", Model: "m",
	}
	if _, err := node.Execute(llmContext(t, map[string]any{"text": "hello"}, nil)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	m0 := gotMsgs[0].(map[string]any)
	if m0["content"] != "分类：hello" {
		t.Errorf("interpolated content = %v", m0["content"])
	}
}

func TestLlmSamplingFields(t *testing.T) {
	temp := 0.7
	srv := llmFakeServer(t, 200, llmOKBody("ok"), func(r *http.Request, req map[string]any) {
		if req["temperature"] != 0.7 {
			t.Errorf("temperature = %v", req["temperature"])
		}
		if req["max_tokens"] != float64(128) {
			t.Errorf("max_tokens = %v", req["max_tokens"])
		}
		rf := req["response_format"].(map[string]any)
		if rf["type"] != "json_object" {
			t.Errorf("response_format = %v", rf)
		}
		if req["reasoning_effort"] != "low" {
			t.Errorf("extra_body not merged: %v", req)
		}
	})
	defer srv.Close()

	node := &Llm{
		BaseNode: BaseNode{ID: "llm1"},
		Prompt:   "go",
		APIBase:  srv.URL, APIKey: "k", Model: "m",
		Temperature: &temp,
		MaxTokens:   128,
		JSONMode:    true,
		ExtraBody:   map[string]any{"reasoning_effort": "low"},
	}
	if _, err := node.Execute(llmContext(t, map[string]any{}, nil)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
}

func TestLlmDryRunFieldAndGlobal(t *testing.T) {
	node := &Llm{
		BaseNode: BaseNode{ID: "llm1"},
		Prompt:   "go",
		Model:    "m-9",
		APIBase:  "http://127.0.0.1:1", APIKey: "k", // would fail if actually called
		DryRun: true,
	}
	res, err := node.Execute(llmContext(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["text"] != "[dry-run] llm completion" || out["model"] != "m-9" {
		t.Errorf("dry output = %v", out)
	}
	if out["usage"] != nil {
		t.Errorf("usage = %v", out["usage"])
	}
	if dry, _ := out["dry_run"].(bool); !dry {
		t.Errorf("dry_run = %v", out)
	}

	// globalContext.dry_run
	node2 := &Llm{BaseNode: BaseNode{ID: "llm2"}, Prompt: "go", APIBase: "http://127.0.0.1:1", APIKey: "k"}
	res2, err := node2.Execute(llmContext(t, map[string]any{}, map[string]any{"dry_run": true}))
	if err != nil {
		t.Fatalf("Execute global dry: %v", err)
	}
	if res2.Result.(map[string]any)["text"] != "[dry-run] llm completion" {
		t.Errorf("global dry output = %v", res2.Result)
	}
}

func TestLlmMissingMessagesDeterministic(t *testing.T) {
	node := &Llm{
		BaseNode: BaseNode{ID: "llm1"},
		Messages: []any{},
		APIBase:  "http://127.0.0.1:1", APIKey: "k", Model: "m",
	}
	_, err := node.Execute(llmContext(t, map[string]any{}, nil))
	if err == nil {
		t.Fatal("empty messages must fail")
	}
	if llmIsRetryable(err) {
		t.Errorf("missing messages is deterministic, got retryable: %v", err)
	}
	if !strings.Contains(err.Error(), "缺少消息") {
		t.Errorf("message: %v", err)
	}

	// messages field nil and no prompt → one user message with empty
	// content still fires (Python assembles the user message unconditionally)
	var gotMsgs []any
	srv := llmFakeServer(t, 200, llmOKBody("ok"), func(r *http.Request, req map[string]any) {
		gotMsgs = req["messages"].([]any)
	})
	defer srv.Close()
	node2 := &Llm{BaseNode: BaseNode{ID: "llm2"}, APIBase: srv.URL, APIKey: "k", Model: "m"}
	if _, err := node2.Execute(llmContext(t, map[string]any{}, nil)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(gotMsgs) != 1 || gotMsgs[0].(map[string]any)["content"] != "" {
		t.Errorf("fallback messages = %v", gotMsgs)
	}
}

func TestLlmErrorClassification(t *testing.T) {
	cases := []struct {
		name      string
		status    int
		body      string
		retryable bool
	}{
		{"401 deterministic", 401, `{"error":"bad key"}`, false},
		{"404 deterministic", 404, "no such model", false},
		{"429 transient", 429, `{"error":"rate limited"}`, true},
		{"500 transient", 500, "boom", true},
		{"malformed response retryable", 200, `{"choices":[]}`, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := llmFakeServer(t, tc.status, tc.body, nil)
			defer srv.Close()
			node := &Llm{
				BaseNode: BaseNode{ID: "llm1"},
				Prompt:   "go",
				APIBase:  srv.URL, APIKey: "k", Model: "m",
			}
			_, err := node.Execute(llmContext(t, map[string]any{}, nil))
			if err == nil {
				t.Fatal("want error")
			}
			if llmIsRetryable(err) != tc.retryable {
				t.Errorf("retryable = %v, want %v (err=%v)", llmIsRetryable(err), tc.retryable, err)
			}
		})
	}
}

func TestLlmTransportErrorRetryable(t *testing.T) {
	srv := llmFakeServer(t, 200, "x", nil)
	url := srv.URL
	srv.Close()
	node := &Llm{
		BaseNode: BaseNode{ID: "llm1"},
		Prompt:   "go",
		APIBase:  url, APIKey: "k", Model: "m",
	}
	_, err := node.Execute(llmContext(t, map[string]any{}, nil))
	if err == nil || !llmIsRetryable(err) {
		t.Errorf("transport error must be retryable: %v", err)
	}
}

func TestLlmTimeoutSecs(t *testing.T) {
	released := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Hang until the client timeout gives up and the test releases us.
		select {
		case <-r.Context().Done():
		case <-released:
		}
	}))
	t.Cleanup(func() { close(released); srv.Close() })
	node := &Llm{
		BaseNode: BaseNode{ID: "llm1"},
		Prompt:   "go",
		APIBase:  srv.URL, APIKey: "k", Model: "m",
		TimeoutSecs: 1,
	}
	start := time.Now()
	_, err := node.Execute(llmContext(t, map[string]any{}, nil))
	if err == nil {
		t.Fatal("want timeout error")
	}
	if !llmIsRetryable(err) {
		t.Errorf("timeout must be retryable: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("timeout did not bound the request: %v", elapsed)
	}
}

func TestLlmProviderBundleResolution(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PLAITA_HOME", dir)

	srv := llmFakeServer(t, 200, llmOKBody("via-bundle"), func(r *http.Request, req map[string]any) {
		if req["model"] != "mb" {
			t.Errorf("model = %v", req["model"])
		}
	})
	defer srv.Close()
	providers := `{"providers": {"budget": {"apiBase": "` + srv.URL + `", "apiKey": "kb", "model": "mb"}}}`
	if err := os.WriteFile(filepath.Join(dir, "providers.json"), []byte(providers), 0o600); err != nil {
		t.Fatal(err)
	}

	node := &Llm{BaseNode: BaseNode{ID: "llm1"}, Prompt: "go", Provider: "budget"}
	res, err := node.Execute(llmContext(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["model"] != "mb" || out["text"] != "via-bundle" {
		t.Errorf("out = %v", out)
	}

	// explicit values override the bundle
	srv2 := llmFakeServer(t, 200, llmOKBody("explicit"), func(r *http.Request, req map[string]any) {
		if req["model"] != "mx" {
			t.Errorf("model = %v", req["model"])
		}
	})
	defer srv2.Close()
	node2 := &Llm{BaseNode: BaseNode{ID: "llm2"}, Prompt: "go",
		Provider: "budget", APIBase: srv2.URL, APIKey: "kx", Model: "mx"}
	res2, err := node2.Execute(llmContext(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res2.Result.(map[string]any)["model"] != "mx" {
		t.Errorf("explicit must win: %v", res2.Result)
	}
}

func TestLlmUnknownProviderDeterministic(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PLAITA_HOME", dir)
	node := &Llm{BaseNode: BaseNode{ID: "llm1"}, Prompt: "go", Provider: "ghost"}
	_, err := node.Execute(llmContext(t, map[string]any{}, nil))
	if err == nil || llmIsRetryable(err) {
		t.Errorf("provider resolution failure is deterministic: %v", err)
	}
}

func TestLlmAPIKeyEnvInterpolation(t *testing.T) {
	t.Setenv("LLM_TEST_KEY_XYZ", "sk-from-env")
	var gotAuth string
	srv := llmFakeServer(t, 200, llmOKBody("ok"), func(r *http.Request, req map[string]any) {
		gotAuth = r.Header.Get("Authorization")
	})
	defer srv.Close()
	node := &Llm{
		BaseNode: BaseNode{ID: "llm1"},
		Prompt:   "go",
		APIBase:  srv.URL, APIKey: "${LLM_TEST_KEY_XYZ}", Model: "m",
	}
	if _, err := node.Execute(llmContext(t, map[string]any{}, nil)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if gotAuth != "Bearer sk-from-env" {
		t.Errorf("auth = %q", gotAuth)
	}

	// missing env → deterministic failure, no request possible
	node2 := &Llm{
		BaseNode: BaseNode{ID: "llm2"},
		Prompt:   "go",
		APIBase:  "http://127.0.0.1:1", APIKey: "${LLM_TEST_KEY_MISSING}", Model: "m",
	}
	_, err := node2.Execute(llmContext(t, map[string]any{}, nil))
	if err == nil || llmIsRetryable(err) {
		t.Errorf("missing env var is deterministic: %v", err)
	}
}

func TestLlmJSONRoundTrip(t *testing.T) {
	doc := `{"type":"llm","id":"l1","prompt":"$INPUT.q","model":"m",
		"temperature":0.5,"json_mode":true,"dry_run":true}`
	node := new(Llm)
	if err := json.Unmarshal([]byte(doc), node); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if node.Prompt != "$INPUT.q" || node.Model != "m" || !node.JSONMode || !node.DryRun {
		t.Errorf("fields = %+v", node)
	}
	if node.Temperature == nil || *node.Temperature != 0.5 {
		t.Errorf("temperature = %v", node.Temperature)
	}
	if node.GetType() != "llm" {
		t.Errorf("type = %v", node.GetType())
	}
}
