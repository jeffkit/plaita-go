package nodes

// Negative + positive controls for plaita-go#72: every outbound
// response-body read is capped under PLAITA_HTTP_MAX_RESPONSE_BYTES (the
// http node's knob, default 10MiB) and rejects over-limit bodies with a
// named error instead of swallowing them into worker memory. Pre-fix, each
// driver below succeeds while the server-written body lands in memory whole
// (red); post-fix each returns the named too-large rejection (green).

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// cappedTestReader counts served bytes so the test can pin the read-path
// bound: at most max+1 bytes ever leave the reader.
type cappedTestReader struct {
	r io.Reader
	n int
}

func (c *cappedTestReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n
	return n, err
}

// TestCappedBodyBoundedRead pins the read bound directly on the primitive
// (issue acceptance #3): a 1MiB body behind a 64-byte ceiling is rejected
// after reading at most max+1 bytes, and a small body passes untouched.
func TestCappedBodyBoundedRead(t *testing.T) {
	const max = 64
	cr := &cappedTestReader{r: bytes.NewReader(bytes.Repeat([]byte("x"), 1<<20))}
	_, err := readCappedBody(cr, max)
	if !errors.As(err, &errResponseTooLarge{}) {
		t.Fatalf("err = %v, want errResponseTooLarge", err)
	}
	if !strings.Contains(err.Error(), "maxResponseBytes=64") {
		t.Errorf("message = %q, want the ceiling value", err)
	}
	if cr.n > max+1 {
		t.Errorf("read %d bytes, want <= max+1", cr.n)
	}
	if data, err := readCappedBody(bytes.NewReader([]byte("ok")), max); err != nil || string(data) != "ok" {
		t.Errorf("small body: data=%q err=%v, want untouched success", data, err)
	}
}

// bigBodyBehind is the shared negative-control shape: an httptest server
// that writes bigLen bytes regardless of the request.
func bigBodyBehind(t *testing.T, bigLen int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(bytes.Repeat([]byte("x"), bigLen))
	}))
}

// TestCappedReads_OverLimitNamedRejection drives every fixed read point
// against an oversized 200 response: the read must fail named with the
// ceiling in the message, and the retry classes follow each node's error
// family (api_request deterministic, notify/webhook NotifyDeterministicError,
// github_comment NonRetryable, llm non-retryable; decision keeps its
// provider-error surface with the named cause riding the chain).
func TestCappedReads_OverLimitNamedRejection(t *testing.T) {
	t.Setenv(httpMaxResponseBytesEnv, "4096")
	const bigLen = 64 << 10 // 64KiB server body behind a 4KiB cap

	t.Run("api_request", func(t *testing.T) {
		srv := bigBodyBehind(t, bigLen)
		defer srv.Close()
		node := &APIRequest{
			BaseNode: BaseNode{ID: "api1"}, Credential: "c", Path: "/x",
			CredentialResolver: apiRequestTestResolver(srv.URL, nil),
		}
		_, err := node.Execute(apiRequestTestContext(t, map[string]any{}, nil))
		if err == nil {
			t.Fatalf("server wrote %d bytes behind a 4096 cap: Execute succeeded (pre-fix swallow)", bigLen)
		}
		var tooLarge errResponseTooLarge
		if !errors.As(err, &tooLarge) {
			t.Fatalf("err = %v, want errResponseTooLarge", err)
		}
		if workflow.IsRetryableError(err) {
			t.Errorf("over-limit is deterministic (same body re-read exceeds again), got retryable")
		}
	})

	t.Run("feishu_webhook", func(t *testing.T) {
		srv := bigBodyBehind(t, bigLen)
		defer srv.Close()
		node := &FeishuWebhook{
			BaseNode: BaseNode{ID: "n"}, Credential: "bot", Text: "hello",
			Credentials: notifyTestCredentialStore{"bot": {"url": srv.URL + "/hook"}},
		}
		_, err := node.Execute(notifyTestContext(t, map[string]any{"text": "hello"}, nil))
		if err == nil {
			t.Fatalf("server wrote %d bytes behind a 4096 cap: Execute succeeded (pre-fix swallow)", bigLen)
		}
		var det *NotifyDeterministicError
		if !errors.As(err, &det) {
			t.Fatalf("err = %v, want NotifyDeterministicError", err)
		}
		if !strings.Contains(err.Error(), "maxResponseBytes=4096") {
			t.Errorf("message = %q, want the ceiling value", err)
		}
		if workflow.IsRetryableError(err) {
			t.Errorf("over-limit is deterministic, got retryable")
		}
	})

	t.Run("generic_webhook", func(t *testing.T) {
		srv := bigBodyBehind(t, bigLen)
		defer srv.Close()
		node := &GenericWebhook{
			BaseNode: BaseNode{ID: "n"}, Credential: "c", Payload: map[string]any{"k": "v"},
			Credentials: notifyTestCredentialStore{"c": {"url": srv.URL}},
		}
		_, err := node.Execute(notifyTestContext(t, map[string]any{}, nil))
		if err == nil {
			t.Fatalf("server wrote %d bytes behind a 4096 cap: Execute succeeded (pre-fix swallow)", bigLen)
		}
		var det *NotifyDeterministicError
		if !errors.As(err, &det) {
			t.Fatalf("err = %v, want NotifyDeterministicError", err)
		}
		if workflow.IsRetryableError(err) {
			t.Errorf("over-limit is deterministic, got retryable")
		}
	})

	t.Run("github_comment", func(t *testing.T) {
		f := githubCommentStartFake(t)
		// >4KiB of valid, marker-free comments: pre-fix the dedup query
		// swallows it and the node posts normally.
		f.mu.Lock()
		f.getBody = `[` + strings.Repeat(`{"body":"x"},`, 400) + `{"body":"y"}]`
		f.mu.Unlock()
		githubCommentFakeEnv(t, f, "test-token")
		node := githubCommentTestNode(t, f, func(n *GithubComment) { n.DedupMarker = "plaita-marker-72" })
		_, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
		if err == nil {
			t.Fatalf("server wrote %d bytes behind a 4096 cap: Execute succeeded (pre-fix swallow)", len(f.getBody))
		}
		var det *workflow.DeterministicError
		if !errors.As(err, &det) {
			t.Fatalf("err = %v, want NonRetryable (DeterministicError)", err)
		}
		if !strings.Contains(err.Error(), "maxResponseBytes=4096") {
			t.Errorf("message = %q, want the ceiling value", err)
		}
		if workflow.IsRetryableError(err) {
			t.Errorf("over-limit must not burn the retryTimes budget, got retryable")
		}
	})

	t.Run("decision_jev", func(t *testing.T) {
		bigAnswers := `{"model":"jev-1","pad":"` + strings.Repeat("x", bigLen) +
			`","answers":{"decision":{"choice":"spam","confidence":0.7}}}`
		srv := llmFakeServer(t, 200, bigAnswers, nil)
		defer srv.Close()
		node := decisionNode(srv.URL, func(d *Decision) { d.Provider = "jev" })
		_, err := node.Execute(llmContext(t, map[string]any{}, nil))
		if err == nil {
			t.Fatalf("server wrote %d bytes behind a 4096 cap: Execute succeeded (pre-fix swallow)", bigLen)
		}
		if !strings.Contains(err.Error(), "maxResponseBytes=4096") {
			t.Errorf("message = %v, want the ceiling value", err)
		}
	})

	t.Run("llm", func(t *testing.T) {
		srv := llmFakeServer(t, 200, llmOKBody(strings.Repeat("x", bigLen)), nil)
		defer srv.Close()
		node := &Llm{
			BaseNode: BaseNode{ID: "llm1"}, Prompt: "hi",
			APIBase: srv.URL, APIKey: "sk-test", Model: "test-model",
		}
		_, err := node.Execute(llmContext(t, map[string]any{}, nil))
		if err == nil {
			t.Fatalf("server wrote %d bytes behind a 4096 cap: Execute succeeded (pre-fix swallow)", bigLen)
		}
		if !strings.Contains(err.Error(), "maxResponseBytes=4096") {
			t.Errorf("message = %q, want the ceiling value", err)
		}
		if workflow.IsRetryableError(err) {
			t.Errorf("over-limit must not burn the retry budget, got retryable")
		}
	})
}

// TestCappedReads_SmallBodyUnaffected pins the positive control: with the
// same tightened cap, bodies under the ceiling keep every driver's success
// path and output shape unchanged.
func TestCappedReads_SmallBodyUnaffected(t *testing.T) {
	t.Setenv(httpMaxResponseBytesEnv, "4096")

	t.Run("api_request", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"ok":true}`))
		}))
		defer srv.Close()
		node := &APIRequest{
			BaseNode: BaseNode{ID: "api1"}, Credential: "c", Path: "/x",
			CredentialResolver: apiRequestTestResolver(srv.URL, nil),
		}
		res, err := node.Execute(apiRequestTestContext(t, map[string]any{}, nil))
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		out := res.Result.(map[string]any)
		if out["status"] != 200 {
			t.Errorf("status = %v", out["status"])
		}
	})

	t.Run("feishu_webhook", func(t *testing.T) {
		var captured notifyTestCapture
		srv := notifyTestServer(t, 200, map[string]any{"code": 0, "msg": "success"}, &captured)
		node := &FeishuWebhook{
			BaseNode: BaseNode{ID: "n"}, Credential: "bot", Text: "hello",
			Credentials: notifyTestCredentialStore{"bot": {"url": srv.URL + "/hook"}},
		}
		res, err := node.Execute(notifyTestContext(t, map[string]any{"text": "hello"}, nil))
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if out := res.Result.(map[string]any); out["status"] != 200 {
			t.Errorf("status = %v", out["status"])
		}
	})

	t.Run("llm", func(t *testing.T) {
		srv := llmFakeServer(t, 200, llmOKBody("摘要结果"), nil)
		defer srv.Close()
		node := &Llm{
			BaseNode: BaseNode{ID: "llm1"}, Prompt: "总结一下",
			APIBase: srv.URL, APIKey: "sk-test", Model: "test-model",
		}
		res, err := node.Execute(llmContext(t, map[string]any{}, nil))
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if out := res.Result.(map[string]any); out["text"] != "摘要结果" {
			t.Errorf("text = %v", out["text"])
		}
	})
}
