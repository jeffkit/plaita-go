package nodes

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// decisionFakeLLM returns an OpenAI-compatible endpoint whose assistant
// content is the given JSON text.
func decisionFakeLLM(t *testing.T, content string, hook func(r *http.Request, req map[string]any)) *httptest.Server {
	t.Helper()
	return llmFakeServer(t, 200, `{"choices":[{"message":{"role":"assistant","content":`+
		string(decisionMustJSON(content))+`}}],"usage":{"total_tokens":3}}`, hook)
}

func decisionNode(baseURL string, mutate func(*Decision)) *Decision {
	n := &Decision{
		BaseNode: BaseNode{ID: "d1"},
		Question: "这条短信是否垃圾短信",
		Choices:  []any{"spam", "ham"},
		Input:    "中奖啦",
		APIBase:  baseURL, APIKey: "k", Model: "m",
	}
	if mutate != nil {
		mutate(n)
	}
	return n
}

func TestDecisionExecuteSingle(t *testing.T) {
	var gotReq map[string]any
	var gotAuth string
	srv := decisionFakeLLM(t, `{"choice":"spam","confidence":0.87}`, func(r *http.Request, req map[string]any) {
		gotReq = req
		gotAuth = r.Header.Get("Authorization")
	})
	defer srv.Close()

	res, err := decisionNode(srv.URL, nil).Execute(llmContext(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	// constrained request shape (decision.py:184-216)
	if gotAuth != "Bearer k" {
		t.Errorf("auth = %q", gotAuth)
	}
	if gotReq["temperature"] != 0.0 {
		t.Errorf("temperature must be fixed 0, got %v", gotReq["temperature"])
	}
	if rf := gotReq["response_format"].(map[string]any); rf["type"] != "json_object" {
		t.Errorf("response_format = %v", rf)
	}
	msgs := gotReq["messages"].([]any)
	sys := msgs[0].(map[string]any)
	if sys["role"] != "system" || sys["content"] != decisionLLMSystemSingle {
		t.Errorf("system prompt = %v", sys)
	}
	user := msgs[1].(map[string]any)
	content := user["content"].(string)
	for _, want := range []string{
		"## 判定问题\n这条短信是否垃圾短信",
		"## 候选决策\n- spam\n- ham",
		"## 待判定内容\n中奖啦",
	} {
		if !strings.Contains(content, want) {
			t.Errorf("user content missing %q:\n%s", want, content)
		}
	}

	// output keys (decision.py:494-497)
	out := res.Result.(map[string]any)
	if out["choice"] != "spam" || out["confidence"] != 0.87 || out["provider"] != "llm" {
		t.Errorf("out = %v", out)
	}
	if out["model"] != "m" {
		t.Errorf("model = %v", out["model"])
	}
	if low, _ := out["low_confidence"].(bool); low {
		t.Errorf("low_confidence = %v", out)
	}
	if out["dry_run"] != false {
		t.Errorf("dry_run = %v", out)
	}
	if raw, _ := out["raw"].(string); !strings.Contains(raw, "\"choice\"") {
		t.Errorf("raw = %v", out["raw"])
	}
}

func TestDecisionChoicesShapes(t *testing.T) {
	cases := []struct {
		name    string
		choices any
		want    string // expected choices[0]
		wantErr string
	}{
		{"string list", []any{"a", "b"}, "a", ""},
		{"dict list", []any{map[string]any{"value": "a", "description": "A"}, map[string]any{"value": "b"}}, "a", ""},
		{"map", map[string]any{"a": "A", "b": nil}, "a", ""},
		{"missing", nil, "", "缺少 choices"},
		{"empty list", []any{}, "", "决策空间为空"},
		{"wrong type", 42, "", "choices 需为字符串列表"},
		{"element missing value", []any{map[string]any{"description": "x"}}, "", "缺少 value"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv("PLAITA_HOME", dir)
			srv := decisionFakeLLM(t, `{"choice":"a","confidence":0.5}`, nil)
			defer srv.Close()
			n := decisionNode(srv.URL, func(d *Decision) { d.Choices = tc.choices })
			// map/dict-list literals are checkable at build time; nil and
			// scalar choices are skipped by Validate (Python parity) and
			// surface at execute.
			if err := n.Validate(); err != nil {
				if tc.wantErr == "" {
					t.Fatalf("Validate: %v", err)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("Validate error = %v, want %q", err, tc.wantErr)
				}
				return
			}
			// dry-run returns choices[0] — the cheapest normalization probe
			n.DryRun = true
			res, err := n.Execute(llmContext(t, map[string]any{}, nil))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			if got := res.Result.(map[string]any)["choice"]; got != tc.want {
				t.Errorf("choices[0] = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDecisionChoiceOutOfRange(t *testing.T) {
	srv := decisionFakeLLM(t, `{"choice":"other","confidence":0.9}`, nil)
	defer srv.Close()
	_, err := decisionNode(srv.URL, nil).Execute(llmContext(t, map[string]any{}, nil))
	if err == nil {
		t.Fatal("out-of-space choice must fail")
	}
	if !strings.Contains(err.Error(), "不在决策空间内") {
		t.Errorf("message: %v", err)
	}
	// DecisionError(RuntimeError) parity: retried by the engine
	if !llmIsRetryable(err) {
		t.Errorf("choice-out-of-space must stay retryable (DecisionError parity): %v", err)
	}
}

func TestDecisionConfidenceParsing(t *testing.T) {
	cases := []struct {
		name      string
		content   string
		wantConf  float64
		wantErr   string
		retryable bool
	}{
		{"numeric", `{"choice":"spam","confidence":0.4}`, 0.4, "", false},
		{"numeric string", `{"choice":"spam","confidence":"0.5"}`, 0.5, "", false},
		{"missing → 0.0", `{"choice":"spam"}`, 0.0, "", false},
		{"bool rejected", `{"choice":"spam","confidence":true}`, 0, "非数值", true},
		{"string word rejected", `{"choice":"spam","confidence":"high"}`, 0, "非数值", true},
		{"out of range", `{"choice":"spam","confidence":1.5}`, 0, "越界", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := decisionFakeLLM(t, tc.content, nil)
			defer srv.Close()
			res, err := decisionNode(srv.URL, nil).Execute(llmContext(t, map[string]any{}, nil))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				if llmIsRetryable(err) != tc.retryable {
					t.Errorf("retryable = %v, want %v", llmIsRetryable(err), tc.retryable)
				}
				return
			}
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			if got := res.Result.(map[string]any)["confidence"]; got != tc.wantConf {
				t.Errorf("confidence = %v, want %v", got, tc.wantConf)
			}
		})
	}
}

func TestDecisionLowConfidenceGate(t *testing.T) {
	minConf := 0.8
	mk := func(baseURL, policy string) *Decision {
		return decisionNode(baseURL, func(d *Decision) {
			d.MinConfidence = &minConf
			d.OnLowConfidence = policy
			d.DefaultChoice = "ham"
		})
	}
	// passthrough (default): flag only
	srv := decisionFakeLLM(t, `{"choice":"spam","confidence":0.5}`, nil)
	defer srv.Close()
	res, err := mk(srv.URL, "").Execute(llmContext(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("passthrough: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["choice"] != "spam" || out["low_confidence"] != true {
		t.Errorf("passthrough out = %v", out)
	}

	// default: replaced, flag stays true
	srv2 := decisionFakeLLM(t, `{"choice":"spam","confidence":0.5}`, nil)
	defer srv2.Close()
	res2, err := mk(srv2.URL, "default").Execute(llmContext(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("default: %v", err)
	}
	out2 := res2.Result.(map[string]any)
	if out2["choice"] != "ham" || out2["low_confidence"] != true {
		t.Errorf("default out = %v", out2)
	}

	// error: named failure
	srv3 := decisionFakeLLM(t, `{"choice":"spam","confidence":0.5}`, nil)
	defer srv3.Close()
	_, err = mk(srv3.URL, "error").Execute(llmContext(t, map[string]any{}, nil))
	if err == nil || !strings.Contains(err.Error(), "置信度 0.50 低于阈值 0.8") {
		t.Fatalf("error policy: %v", err)
	}

	// high confidence passes the gate untouched
	srv4 := decisionFakeLLM(t, `{"choice":"spam","confidence":0.95}`, nil)
	defer srv4.Close()
	res4, err := mk(srv4.URL, "error").Execute(llmContext(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("above threshold: %v", err)
	}
	if res4.Result.(map[string]any)["low_confidence"] != false {
		t.Errorf("above threshold out = %v", res4.Result)
	}
}

func TestDecisionBatch(t *testing.T) {
	// results deliberately out of order — alignment is by index (#8)
	content := `{"results":[
		{"index":2,"choice":"ham","confidence":0.9},
		{"index":0,"choice":"spam","confidence":0.3},
		{"index":1,"choice":"spam","confidence":0.95}]}`
	srv := decisionFakeLLM(t, content, nil)
	defer srv.Close()
	minConf := 0.8
	n := decisionNode(srv.URL, func(d *Decision) {
		d.MinConfidence = &minConf
		d.Items = []any{"msg-0", "msg-1", "msg-2"}
	})
	n.Input = nil
	res, err := n.Execute(llmContext(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out := res.Result.(map[string]any)
	results := out["results"].([]map[string]any)
	if len(results) != 3 {
		t.Fatalf("results = %v", out["results"])
	}
	if results[0]["index"] != 0 || results[0]["choice"] != "spam" || results[0]["low_confidence"] != true {
		t.Errorf("r0 = %v", results[0])
	}
	if results[1]["choice"] != "spam" || results[1]["low_confidence"] != false {
		t.Errorf("r1 = %v", results[1])
	}
	if results[2]["choice"] != "ham" || results[2]["low_confidence"] != false {
		t.Errorf("r2 = %v", results[2])
	}
	if out["low_confidence"] != true {
		t.Errorf("any low = %v", out["low_confidence"])
	}

	// malformed batches fail by name
	bad := []struct {
		name    string
		content string
		want    string
	}{
		{"duplicate index", `{"results":[{"index":0,"choice":"spam"},{"index":0,"choice":"ham"}]}`, "index 重复"},
		{"count mismatch", `{"results":[{"index":0,"choice":"spam"}]}`, "数不符"},
		{"float index", `{"results":[{"index":0,"choice":"spam"},{"index":1.0,"choice":"ham"}]}`, "非整数"},
		{"out of range", `{"results":[{"index":0,"choice":"spam"},{"index":9,"choice":"ham"}]}`, "越界"},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			srv := decisionFakeLLM(t, tc.content, nil)
			defer srv.Close()
			n := decisionNode(srv.URL, func(d *Decision) { d.Items = []any{"a", "b"} })
			_, err := n.Execute(llmContext(t, map[string]any{}, nil))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
			if !llmIsRetryable(err) {
				t.Errorf("batch violation must be retryable: %v", err)
			}
		})
	}

	// items must be a non-empty list
	n2 := decisionNode("http://127.0.0.1:1", func(d *Decision) { d.Items = "nope" })
	if _, err := n2.Execute(llmContext(t, map[string]any{}, nil)); err == nil || !strings.Contains(err.Error(), "items 需为非空列表") {
		t.Errorf("items type: %v", err)
	}
}

func TestDecisionDryRun(t *testing.T) {
	srv := decisionFakeLLM(t, `{"choice":"spam","confidence":0.9}`, nil)
	defer srv.Close()

	n := decisionNode("http://127.0.0.1:1", func(d *Decision) { d.DryRun = true })
	res, err := n.Execute(llmContext(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("single dry: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["choice"] != "spam" || out["confidence"] != 1.0 || out["raw"] != "[dry-run]" || out["dry_run"] != true {
		t.Errorf("single dry out = %v", out)
	}

	n2 := decisionNode("http://127.0.0.1:1", func(d *Decision) {
		d.DryRun = true
		d.Items = []any{"a", "b", "c"}
	})
	res2, err := n2.Execute(llmContext(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("batch dry: %v", err)
	}
	out2 := res2.Result.(map[string]any)
	results := out2["results"].([]map[string]any)
	if len(results) != 3 || results[2]["choice"] != "spam" || results[2]["confidence"] != 1.0 {
		t.Errorf("batch dry out = %v", out2)
	}

	// global dry_run
	n3 := decisionNode("http://127.0.0.1:1", nil)
	res3, err := n3.Execute(llmContext(t, map[string]any{}, map[string]any{"dry_run": true}))
	if err != nil {
		t.Fatalf("global dry: %v", err)
	}
	if res3.Result.(map[string]any)["dry_run"] != true {
		t.Errorf("global dry out = %v", res3.Result)
	}
}

func TestDecisionValidate(t *testing.T) {
	cases := []struct {
		name    string
		node    *Decision
		wantErr string
	}{
		{"valid", decisionNode("", nil), ""},
		{"bad literal choices", decisionNode("", func(d *Decision) { d.Choices = []any{} }), "决策空间为空"},
		{"unregistered provider", decisionNode("", func(d *Decision) { d.Provider = "ghost" }), "未注册"},
		{"default without choice", decisionNode("", func(d *Decision) {
			d.OnLowConfidence = "default"
		}), "on_low_confidence=default 需要同时配置 default_choice"},
		{"bad policy", decisionNode("", func(d *Decision) { d.OnLowConfidence = "nope" }), "passthrough/default/error"},
		{"min_confidence out of range", decisionNode("", func(d *Decision) {
			c := 1.5
			d.MinConfidence = &c
		}), "min_confidence 越界"},
		{"expression choices deferred", decisionNode("", func(d *Decision) { d.Choices = "$INPUT.c" }), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.node.Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestDecisionUnregisteredProviderAtExecute(t *testing.T) {
	n := decisionNode("http://127.0.0.1:1", func(d *Decision) { d.Provider = "ghost" })
	_, err := n.Execute(llmContext(t, map[string]any{}, nil))
	if err == nil || !strings.Contains(err.Error(), "未注册") {
		t.Fatalf("err = %v", err)
	}
	if !llmIsRetryable(err) {
		t.Errorf("unregistered provider is DecisionError → retryable: %v", err)
	}
}

func TestDecisionCustomProvider(t *testing.T) {
	RegisterDecisionProvider("test-fixed", decisionFuncProvider(func(ctx context.Context, req *DecisionRequest) (*DecisionOutcome, error) {
		if len(req.ExtraBody) != 0 {
			t.Errorf("extra_body must ride only with the llm provider, got %v", req.ExtraBody)
		}
		return &DecisionOutcome{Choice: "spam", Confidence: 0.6, Model: "stub", Raw: "stub-raw"}, nil
	}))
	n := decisionNode("http://127.0.0.1:1", func(d *Decision) {
		d.Provider = "test-fixed"
		d.ExtraBody = map[string]any{"reasoning_effort": "low"}
	})
	res, err := n.Execute(llmContext(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["choice"] != "spam" || out["provider"] != "test-fixed" || out["model"] != "stub" || out["raw"] != "stub-raw" {
		t.Errorf("out = %v", out)
	}

	// custom provider returning an out-of-space choice still fails closed
	RegisterDecisionProvider("test-wild", decisionFuncProvider(func(ctx context.Context, req *DecisionRequest) (*DecisionOutcome, error) {
		return &DecisionOutcome{Choice: "wild", Confidence: 0.9}, nil
	}))
	n2 := decisionNode("http://127.0.0.1:1", func(d *Decision) { d.Provider = "test-wild" })
	if _, err := n2.Execute(llmContext(t, map[string]any{}, nil)); err == nil || !strings.Contains(err.Error(), "不在决策空间内") {
		t.Errorf("wild choice: %v", err)
	}
}

type decisionFuncProvider func(ctx context.Context, req *DecisionRequest) (*DecisionOutcome, error)

func (f decisionFuncProvider) Decide(ctx context.Context, req *DecisionRequest) (*DecisionOutcome, error) {
	return f(ctx, req)
}

func TestDecisionTransientVsDeterministic(t *testing.T) {
	// 401 from the endpoint: decision.py:218 raises DecisionError for every
	// status ≥ 400 → retryable (deliberate divergence from the llm node).
	srv := llmFakeServer(t, 401, `{"error":"bad key"}`, nil)
	defer srv.Close()
	_, err := decisionNode(srv.URL, nil).Execute(llmContext(t, map[string]any{}, nil))
	if err == nil || !strings.Contains(err.Error(), "LLM 决策请求失败 401") {
		t.Fatalf("401: %v", err)
	}
	if !llmIsRetryable(err) {
		t.Errorf("decision HTTP 401 must stay retryable: %v", err)
	}

	// incomplete endpoint chain (no fields, no env): deterministic
	t.Setenv("LLM_API_BASE", "")
	t.Setenv("LLM_API_KEY", "")
	t.Setenv("LLM_MODEL", "")
	n := decisionNode("", func(d *Decision) { d.APIBase = ""; d.APIKey = ""; d.Model = "" })
	_, err = n.Execute(llmContext(t, map[string]any{}, nil))
	if err == nil || llmIsRetryable(err) {
		t.Errorf("endpoint chain failure is deterministic: %v", err)
	}

	// transport failure: retryable
	srv2 := decisionFakeLLM(t, "x", nil)
	url := srv2.URL
	srv2.Close()
	_, err = decisionNode(url, nil).Execute(llmContext(t, map[string]any{}, nil))
	if err == nil || !llmIsRetryable(err) {
		t.Errorf("transport must be retryable: %v", err)
	}
}

func TestDecisionTimeout(t *testing.T) {
	released := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-released:
		}
	}))
	t.Cleanup(func() { close(released); srv.Close() })
	n := decisionNode(srv.URL, func(d *Decision) { d.TimeoutSecs = 1 })
	if _, err := n.Execute(llmContext(t, map[string]any{}, nil)); err == nil || !llmIsRetryable(err) {
		t.Errorf("timeout must be retryable: %v", err)
	}
}

func TestDecisionAPIKeyEnvInterpolation(t *testing.T) {
	t.Setenv("DECISION_TEST_KEY_XYZ", "sk-decision")
	var gotAuth string
	srv := decisionFakeLLM(t, `{"choice":"spam","confidence":0.9}`, func(r *http.Request, req map[string]any) {
		gotAuth = r.Header.Get("Authorization")
	})
	defer srv.Close()
	n := decisionNode(srv.URL, func(d *Decision) { d.APIKey = "${DECISION_TEST_KEY_XYZ}" })
	if _, err := n.Execute(llmContext(t, map[string]any{}, nil)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if gotAuth != "Bearer sk-decision" {
		t.Errorf("auth = %q", gotAuth)
	}
}

func TestDecisionExtraBodyMerged(t *testing.T) {
	var gotReq map[string]any
	srv := decisionFakeLLM(t, `{"choice":"spam","confidence":0.9}`, func(r *http.Request, req map[string]any) {
		gotReq = req
	})
	defer srv.Close()
	n := decisionNode(srv.URL, func(d *Decision) {
		d.ExtraBody = map[string]any{"reasoning_effort": "low"}
	})
	if _, err := n.Execute(llmContext(t, map[string]any{}, nil)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if gotReq["reasoning_effort"] != "low" {
		t.Errorf("extra_body not merged: %v", gotReq)
	}
}

func TestDecisionJevProvider(t *testing.T) {
	var gotPath string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &gotBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"jev-1","answers":{"decision":{"choice":"spam","confidence":0.7}}}`))
	}))
	defer srv.Close()

	n := decisionNode(srv.URL, func(d *Decision) { d.Provider = "jev"; d.Model = "jev-x" })
	res, err := n.Execute(llmContext(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if gotPath != "/v1/systemone" {
		t.Errorf("path = %q", gotPath)
	}
	if gotBody["state"] != "中奖啦" {
		t.Errorf("state = %v", gotBody["state"])
	}
	if gotBody["model"] != "jev-x" {
		t.Errorf("model = %v", gotBody["model"])
	}
	questions := gotBody["questions"].(map[string]any)
	q := questions["decision"].(map[string]any)
	if q["type"] != "choice" || q["instructions"] != "这条短信是否垃圾短信" {
		t.Errorf("question = %v", q)
	}
	criteria := q["criteria"].(map[string]any)
	if len(criteria) != 2 {
		t.Errorf("criteria = %v", criteria)
	}

	out := res.Result.(map[string]any)
	if out["choice"] != "spam" || out["confidence"] != 0.7 || out["model"] != "jev-1" || out["provider"] != "jev" {
		t.Errorf("out = %v", out)
	}

	// batch mode: one question per item
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"answers":{"item_0":{"choice":"spam","confidence":0.6},"item_1":{"choice":"ham","confidence":0.4}}}`))
	}))
	defer srv2.Close()
	minConf := 0.5
	n2 := decisionNode(srv2.URL, func(d *Decision) {
		d.Provider = "jev"
		d.Items = []any{"x", "y"}
		d.MinConfidence = &minConf
	})
	n2.Input = nil
	res2, err := n2.Execute(llmContext(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("batch jev: %v", err)
	}
	out2 := res2.Result.(map[string]any)
	results := out2["results"].([]map[string]any)
	if results[0]["low_confidence"] != false || results[1]["low_confidence"] != true {
		t.Errorf("batch jev results = %v", results)
	}
	if out2["low_confidence"] != true || out2["model"] != "" {
		t.Errorf("batch jev out = %v", out2)
	}

	// missing answers → named failure
	srv3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"unexpected":1}`))
	}))
	defer srv3.Close()
	n3 := decisionNode(srv3.URL, func(d *Decision) { d.Provider = "jev" })
	_, err = n3.Execute(llmContext(t, map[string]any{}, nil))
	if err == nil || !strings.Contains(err.Error(), "缺少 answers 对象") {
		t.Errorf("missing answers: %v", err)
	}

	// answer missing choice
	srv4 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"answers":{"decision":{"confidence":0.5}}}`))
	}))
	defer srv4.Close()
	n4 := decisionNode(srv4.URL, func(d *Decision) { d.Provider = "jev" })
	_, err = n4.Execute(llmContext(t, map[string]any{}, nil))
	if err == nil || !strings.Contains(err.Error(), "answers.decision 缺少 choice") {
		t.Errorf("missing choice: %v", err)
	}
}

func TestDecisionJevEnvFallback(t *testing.T) {
	t.Setenv("JEV_API_BASE", "")
	n := decisionNode("", func(d *Decision) { d.Provider = "jev"; d.APIBase = "" })
	if _, err := n.Execute(llmContext(t, map[string]any{}, nil)); err == nil || !strings.Contains(err.Error(), "JEV_API_BASE") {
		t.Fatalf("missing base: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"answers":{"decision":{"choice":"spam","confidence":0.9}}}`))
	}))
	defer srv.Close()
	t.Setenv("JEV_API_BASE", srv.URL)
	n2 := decisionNode("", func(d *Decision) { d.Provider = "jev"; d.APIBase = "" })
	if _, err := n2.Execute(llmContext(t, map[string]any{}, nil)); err != nil {
		t.Fatalf("env fallback: %v", err)
	}
}

func TestDecisionJevlikeStub(t *testing.T) {
	names := strings.Join(decisionProviderNames(), ",")
	for _, want := range []string{"llm", "jev", "jevlike"} {
		if !strings.Contains(names, want) {
			t.Errorf("registry missing %q: %s", want, names)
		}
	}
	n := decisionNode("", func(d *Decision) { d.Provider = "jevlike" })
	_, err := n.Execute(llmContext(t, map[string]any{}, nil))
	if err == nil || !strings.Contains(err.Error(), "jevlike + torch") {
		t.Errorf("stub: %v", err)
	}
}

func TestDecisionProviderBundleNotConsulted(t *testing.T) {
	// decision's llm provider resolves the endpoint WITHOUT a bundle name
	// (llm.py:188) — a providers.json entry must not leak in.
	dir := t.TempDir()
	t.Setenv("PLAITA_HOME", dir)
	providers := `{"providers": {"p": {"apiBase": "http://127.0.0.1:1", "apiKey": "kb", "model": "mb"}}}`
	if err := os.WriteFile(filepath.Join(dir, "providers.json"), []byte(providers), 0o600); err != nil {
		t.Fatal(err)
	}
	srv := decisionFakeLLM(t, `{"choice":"spam","confidence":0.9}`, func(r *http.Request, req map[string]any) {
		if req["model"] == "mb" {
			t.Errorf("bundle leaked into decision endpoint chain")
		}
	})
	defer srv.Close()
	n := decisionNode(srv.URL, nil)
	if _, err := n.Execute(llmContext(t, map[string]any{}, nil)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
}

func TestDecisionJSONRoundTrip(t *testing.T) {
	doc := `{"type":"decision","id":"d1","question":"$INPUT.q","provider":"jev",
		"min_confidence":0.6,"on_low_confidence":"default","default_choice":"ham","dry_run":true}`
	node := new(Decision)
	if err := json.Unmarshal([]byte(doc), node); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if node.Provider != "jev" || node.OnLowConfidence != "default" || node.DefaultChoice != "ham" {
		t.Errorf("fields = %+v", node)
	}
	if node.MinConfidence == nil || *node.MinConfidence != 0.6 {
		t.Errorf("min_confidence = %v", node.MinConfidence)
	}
	if node.GetType() != "decision" {
		t.Errorf("type = %v", node.GetType())
	}
	// a valid default_choice config passes Validate
	if err := node.Validate(); err != nil {
		t.Errorf("Validate: %v", err)
	}
}

func TestDecisionExtractJSONUnit(t *testing.T) {
	cases := []struct {
		text string
		want string
		err  string
	}{
		{`{"choice":"a"}`, "a", ""},
		{"前置说明\n```json\n{\"choice\":\"b\"}\n```\n后缀", "b", ""},
		{"no json here", "", "LLM 未返回 JSON 决策对象"},
		{"[1,2]", "", "LLM 未返回 JSON 决策对象"},
	}
	for i, tc := range cases {
		data, err := decisionExtractJSON(tc.text)
		if tc.err != "" {
			if err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Errorf("case %d: err = %v, want %q", i, err, tc.err)
			}
			continue
		}
		if err != nil {
			t.Errorf("case %d: %v", i, err)
			continue
		}
		if data["choice"] != tc.want {
			t.Errorf("case %d: choice = %v, want %v", i, data["choice"], tc.want)
		}
	}
	// trailing garbage after the object is rejected like json.loads
	if _, ok := decisionUnmarshalObject(`{"choice":"a"} junk`); ok {
		t.Errorf("trailing garbage must not parse")
	}
}
