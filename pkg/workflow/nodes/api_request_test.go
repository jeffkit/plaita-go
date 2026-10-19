package nodes

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/internal/types"
)

// apiRequestTestContext builds a workflow runtime context for direct Execute calls.
func apiRequestTestContext(t *testing.T, input map[string]any, globals map[string]any) context.Context {
	t.Helper()
	return runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput:      input,
		ExecuteNodeResults: map[string]any{},
		EnvVariables:       map[string]any{},
		GlobalVariables:    globals,
	})
}

func apiRequestTestResolver(baseURL string, headers map[string]any) APIRequestCredentialResolver {
	return func(context.Context, string) (map[string]any, error) {
		return map[string]any{"base_url": baseURL, "headers": headers}, nil
	}
}

// apiRequestHit is one recorded request on the test server.
type apiRequestHit struct {
	method      string
	path        string
	rawQuery    string
	auth        string
	apiKey      string
	contentType string
	body        string
}

type apiRequestServer struct {
	srv  *httptest.Server
	mu   sync.Mutex
	hits []apiRequestHit
}

func newAPIRequestServer(t *testing.T, status int, contentType, payload string, redirect func(h *apiRequestServer, r *http.Request) (string, int)) *apiRequestServer {
	t.Helper()
	s := &apiRequestServer{}
	// Plain HandlerFunc (not ServeMux): ServeMux auto-cleans unclean paths
	// (//x//y → /x/y via 301) before dispatch, which would mask the node's
	// own path handling.
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.hits = append(s.hits, apiRequestHit{
			method:      r.Method,
			path:        r.URL.Path,
			rawQuery:    r.URL.RawQuery,
			auth:        r.Header.Get("Authorization"),
			apiKey:      r.Header.Get("X-Api-Key"),
			contentType: r.Header.Get("Content-Type"),
			body:        string(body),
		})
		s.mu.Unlock()
		if redirect != nil {
			if loc, code := redirect(s, r); loc != "" {
				w.Header().Set("Location", loc)
				w.WriteHeader(code)
				return
			}
		}
		if contentType != "" {
			w.Header().Set("Content-Type", contentType)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(payload))
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *apiRequestServer) snapshot() []apiRequestHit {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]apiRequestHit, len(s.hits))
	copy(out, s.hits)
	return out
}

func apiRequestNewNode(t *testing.T, srv *apiRequestServer, mutate func(*APIRequest)) *APIRequest {
	t.Helper()
	node := &APIRequest{
		BaseNode:           BaseNode{ID: "api1"},
		Credential:         "cred1",
		CredentialResolver: apiRequestTestResolver(srv.srv.URL, map[string]any{"Authorization": "Bearer tok", "X-Api-Key": "key-1"}),
	}
	if mutate != nil {
		mutate(node)
	}
	return node
}

func TestAPIRequestExecute(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*APIRequest)
		input  map[string]any
		verify func(t *testing.T, s *apiRequestServer, out map[string]any)
	}{
		{
			name: "get_with_headers_query_and_path_interpolation",
			mutate: func(n *APIRequest) {
				n.Method = "GET"
				n.Path = "repos/$INPUT.org/issues"
				n.Query = map[string]any{"state": "open", "labels": []any{"a", "b"}, "empty": nil}
			},
			input: map[string]any{"org": "acme"},
			verify: func(t *testing.T, s *apiRequestServer, out map[string]any) {
				hits := s.snapshot()
				if len(hits) != 1 {
					t.Fatalf("hits = %d", len(hits))
				}
				h := hits[0]
				if h.method != http.MethodGet || h.path != "/repos/acme/issues" {
					t.Errorf("method/path = %s %s", h.method, h.path)
				}
				q := h.rawQuery
				for _, want := range []string{"state=open", "labels=a", "labels=b"} {
					if !strings.Contains(q, want) {
						t.Errorf("query %q missing %q", q, want)
					}
				}
				if strings.Contains(q, "empty") {
					t.Errorf("nil param leaked: %q", q)
				}
				if h.auth != "Bearer tok" || h.apiKey != "key-1" {
					t.Errorf("credential headers missing: %q %q", h.auth, h.apiKey)
				}
				data, ok := out["data"].(map[string]any)
				if !ok || data["ok"] != true {
					t.Errorf("data = %v", out["data"])
				}
				if st, _ := out["status"].(int); st != 200 {
					t.Errorf("status = %v", out["status"])
				}
			},
		},
		{
			name: "post_json_body_with_inner_expression",
			mutate: func(n *APIRequest) {
				n.Method = "post" // lower-case input must be uppered
				n.Body = map[string]any{"title": "$INPUT.title"}
			},
			input: map[string]any{"title": "hello"},
			verify: func(t *testing.T, s *apiRequestServer, out map[string]any) {
				h := s.snapshot()[0]
				if h.method != http.MethodPost {
					t.Errorf("method = %s", h.method)
				}
				if h.contentType != "application/json" {
					t.Errorf("content-type = %q", h.contentType)
				}
				var got map[string]any
				if err := json.Unmarshal([]byte(h.body), &got); err != nil {
					t.Fatalf("body %q: %v", h.body, err)
				}
				if got["title"] != "hello" {
					t.Errorf("body = %v", got)
				}
			},
		},
		{
			name: "whole_body_expression_native_type",
			mutate: func(n *APIRequest) {
				n.Method = "PUT"
				n.Body = "$INPUT.payload"
			},
			input: map[string]any{"payload": map[string]any{"k": "v"}},
			verify: func(t *testing.T, s *apiRequestServer, out map[string]any) {
				h := s.snapshot()[0]
				var got map[string]any
				if err := json.Unmarshal([]byte(h.body), &got); err != nil || got["k"] != "v" {
					t.Errorf("body = %q err = %v", h.body, err)
				}
			},
		},
		{
			name: "get_drops_body",
			mutate: func(n *APIRequest) {
				n.Method = "GET"
				n.Body = map[string]any{"ignored": true}
			},
			input: map[string]any{},
			verify: func(t *testing.T, s *apiRequestServer, out map[string]any) {
				h := s.snapshot()[0]
				if h.body != "" || h.contentType != "" {
					t.Errorf("GET carried body %q / content-type %q", h.body, h.contentType)
				}
			},
		},
		{
			name:   "non_json_response_becomes_truncated_text",
			mutate: func(n *APIRequest) { n.Method = "GET" },
			input:  map[string]any{},
			verify: func(t *testing.T, s *apiRequestServer, out map[string]any) {
				if text, _ := out["data"].(string); text != "plain not-json" {
					t.Errorf("data = %q", out["data"])
				}
			},
		},
		{
			name: "path_leading_slashes_trimmed",
			mutate: func(n *APIRequest) {
				n.Method = "GET"
				n.Path = "//x//y"
			},
			input: map[string]any{},
			verify: func(t *testing.T, s *apiRequestServer, out map[string]any) {
				if p := s.snapshot()[0].path; p != "/x//y" {
					t.Errorf("path = %s", p)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newAPIRequestServer(t, 200, "application/json", `{"ok": true}`, nil)
			if tc.name == "non_json_response_becomes_truncated_text" {
				s = newAPIRequestServer(t, 200, "text/plain", "plain not-json", nil)
			}
			node := apiRequestNewNode(t, s, tc.mutate)
			res, err := node.Execute(apiRequestTestContext(t, tc.input, nil))
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			out, ok := res.Result.(map[string]any)
			if !ok {
				t.Fatalf("result type %T", res.Result)
			}
			tc.verify(t, s, out)
		})
	}
}

func TestAPIRequestRedirects(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*APIRequest)
		cross   bool // redirect to a second server (cross-origin)
		locCode int
		loc     string
		verify  func(t *testing.T, first, final apiRequestHit, err error, out map[string]any, total int)
	}{
		{
			name:    "default_not_followed",
			locCode: 302,
			loc:     "/final",
			verify: func(t *testing.T, first, final apiRequestHit, err error, out map[string]any, total int) {
				if err != nil {
					t.Fatalf("Execute: %v", err)
				}
				if total != 1 {
					t.Errorf("requests = %d, want 1", total)
				}
				if st, _ := out["status"].(int); st != 302 {
					t.Errorf("status = %v", out["status"])
				}
				if first.auth != "Bearer tok" {
					t.Errorf("auth = %q", first.auth)
				}
			},
		},
		{
			name:    "same_origin_follow_rewrites_302_post_to_get_and_drops_body",
			mutate:  func(n *APIRequest) { n.Method = "POST"; n.Body = map[string]any{"x": 1}; n.AllowRedirects = true },
			locCode: 302,
			loc:     "/final",
			verify: func(t *testing.T, first, final apiRequestHit, err error, out map[string]any, total int) {
				if err != nil {
					t.Fatalf("Execute: %v", err)
				}
				if total != 2 {
					t.Fatalf("requests = %d", total)
				}
				if first.method != http.MethodPost || first.body == "" {
					t.Errorf("first = %+v", first)
				}
				if final.method != http.MethodGet || final.body != "" || final.contentType != "" {
					t.Errorf("final = %+v", final)
				}
				if final.auth != "Bearer tok" {
					t.Errorf("same-origin must keep credentials, auth = %q", final.auth)
				}
			},
		},
		{
			name:    "301_rewrites_post_only",
			mutate:  func(n *APIRequest) { n.Method = "POST"; n.Body = map[string]any{"x": 1}; n.AllowRedirects = true },
			locCode: 301,
			loc:     "/final",
			verify: func(t *testing.T, first, final apiRequestHit, err error, out map[string]any, total int) {
				if err != nil {
					t.Fatalf("Execute: %v", err)
				}
				if final.method != http.MethodGet || final.body != "" {
					t.Errorf("final = %+v", final)
				}
			},
		},
		{
			name:    "303_rewrites_non_head_to_get",
			mutate:  func(n *APIRequest) { n.Method = "POST"; n.Body = map[string]any{"x": 1}; n.AllowRedirects = true },
			locCode: 303,
			loc:     "/final",
			verify: func(t *testing.T, first, final apiRequestHit, err error, out map[string]any, total int) {
				if err != nil {
					t.Fatalf("Execute: %v", err)
				}
				if final.method != http.MethodGet || final.body != "" {
					t.Errorf("final = %+v", final)
				}
			},
		},
		{
			name:    "307_preserves_method_body_and_content_type",
			mutate:  func(n *APIRequest) { n.Method = "POST"; n.Body = map[string]any{"x": "keep"}; n.AllowRedirects = true },
			locCode: 307,
			loc:     "/final",
			verify: func(t *testing.T, first, final apiRequestHit, err error, out map[string]any, total int) {
				if err != nil {
					t.Fatalf("Execute: %v", err)
				}
				if final.method != http.MethodPost || final.body == "" || final.contentType != "application/json" {
					t.Errorf("final = %+v", final)
				}
			},
		},
		{
			name:    "cross_origin_strips_all_headers",
			mutate:  func(n *APIRequest) { n.Method = "GET"; n.AllowRedirects = true },
			cross:   true,
			locCode: 302,
			loc:     "/final",
			verify: func(t *testing.T, first, final apiRequestHit, err error, out map[string]any, total int) {
				if err != nil {
					t.Fatalf("Execute: %v", err)
				}
				if first.auth != "Bearer tok" {
					t.Errorf("first auth = %q", first.auth)
				}
				if final.auth != "" || final.apiKey != "" {
					t.Errorf("cross-origin must strip credentials, auth=%q key=%q", final.auth, final.apiKey)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var s *apiRequestServer
			var target *apiRequestServer
			if tc.cross {
				target = newAPIRequestServer(t, 200, "application/json", `{"ok": true}`, nil)
				s = newAPIRequestServer(t, 200, "", "",
					func(hs *apiRequestServer, r *http.Request) (string, int) {
						return target.srv.URL + tc.loc, tc.locCode
					})
			} else {
				s = newAPIRequestServer(t, 200, "application/json", `{"ok": true}`,
					func(hs *apiRequestServer, r *http.Request) (string, int) {
						if r.URL.Path == tc.loc {
							return "", 0 // final endpoint answers normally
						}
						return tc.loc, tc.locCode
					})
			}
			node := apiRequestNewNode(t, s, tc.mutate)
			res, err := node.Execute(apiRequestTestContext(t, map[string]any{}, nil))
			var out map[string]any
			if res != nil {
				out, _ = res.Result.(map[string]any)
			}
			hits := s.snapshot()
			if len(hits) == 0 {
				t.Fatal("no requests recorded")
			}
			// For a cross-origin hop the final request lands on the target server.
			final := hits[len(hits)-1]
			if tc.cross {
				targetHits := target.snapshot()
				if len(targetHits) != 1 {
					t.Fatalf("cross-origin target hits = %d", len(targetHits))
				}
				final = targetHits[0]
			}
			tc.verify(t, hits[0], final, err, out, len(hits))
		})
	}
}

func TestAPIRequestRedirectLoopAborts(t *testing.T) {
	s := newAPIRequestServer(t, 308, "", "", func(hs *apiRequestServer, r *http.Request) (string, int) {
		return "/loop", 308
	})
	node := apiRequestNewNode(t, s, func(n *APIRequest) { n.Method = "GET"; n.AllowRedirects = true })
	_, err := node.Execute(apiRequestTestContext(t, map[string]any{}, nil))
	if err == nil {
		t.Fatal("redirect loop must abort")
	}
	if !apiRequestIsRetryable(err) {
		t.Errorf("redirect-loop abort should be retryable (RuntimeError parity): %v", err)
	}
	if got := len(s.snapshot()); got != apiRequestMaxRedirects+1 {
		t.Errorf("requests = %d, want %d", got, apiRequestMaxRedirects+1)
	}
}

func TestAPIRequestErrorClassification(t *testing.T) {
	cases := []struct {
		name          string
		status        int
		raiseOnError  bool
		wantErr       bool
		wantRetryable bool
	}{
		{"404_deterministic", 404, true, true, false},
		{"403_deterministic", 403, true, true, false},
		{"429_retryable", 429, true, true, true},
		{"408_retryable", 408, true, true, true},
		{"500_retryable", 500, true, true, true},
		{"503_retryable", 503, true, true, true},
		{"404_tolerated_when_raise_off", 404, false, false, false},
		{"500_tolerated_when_raise_off", 500, false, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newAPIRequestServer(t, tc.status, "application/json", `{"error":"nope"}`, nil)
			node := apiRequestNewNode(t, s, func(n *APIRequest) {
				n.Method = "GET"
				n.Path = "secret/path"
				n.RaiseOnError = &tc.raiseOnError
			})
			res, err := node.Execute(apiRequestTestContext(t, map[string]any{}, nil))
			if tc.wantErr {
				if err == nil {
					t.Fatal("want error")
				}
				if apiRequestIsRetryable(err) != tc.wantRetryable {
					t.Errorf("retryable = %v, want %v (err %v)", apiRequestIsRetryable(err), tc.wantRetryable, err)
				}
				// the URL path and credential payload must not leak into the message
				if strings.Contains(err.Error(), "secret/path") || strings.Contains(err.Error(), "hush") {
					t.Errorf("error message leaks: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			out := res.Result.(map[string]any)
			if st, _ := out["status"].(int); st != tc.status {
				t.Errorf("status = %v", out["status"])
			}
			data, _ := out["data"].(map[string]any)
			if data["error"] != "nope" {
				t.Errorf("data = %v", data)
			}
		})
	}
}

func TestAPIRequestTransportErrorIsRetryable(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	baseURL := s.URL
	s.Close() // dial will be refused
	node := &APIRequest{
		BaseNode:           BaseNode{ID: "api1"},
		Credential:         "cred1",
		CredentialResolver: apiRequestTestResolver(baseURL, nil),
	}
	_, err := node.Execute(apiRequestTestContext(t, map[string]any{}, nil))
	if err == nil {
		t.Fatal("want transport error")
	}
	if !apiRequestIsRetryable(err) {
		t.Errorf("transport error must be retryable: %v", err)
	}
}

func TestAPIRequestConfigErrors(t *testing.T) {
	t.Run("no_credential_store", func(t *testing.T) {
		node := &APIRequest{BaseNode: BaseNode{ID: "api1"}, Credential: "cred1"}
		_, err := node.Execute(apiRequestTestContext(t, map[string]any{}, nil))
		if err == nil || !strings.Contains(err.Error(), "no credential store") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("missing_credential_field", func(t *testing.T) {
		s := newAPIRequestServer(t, 200, "", "", nil)
		node := apiRequestNewNode(t, s, nil)
		node.Credential = ""
		_, err := node.Execute(apiRequestTestContext(t, map[string]any{}, nil))
		if err == nil || !strings.Contains(err.Error(), "missing credential") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("credential_without_base_url", func(t *testing.T) {
		s := newAPIRequestServer(t, 200, "", "", nil)
		node := apiRequestNewNode(t, s, nil)
		node.CredentialResolver = func(context.Context, string) (map[string]any, error) {
			return map[string]any{"headers": map[string]any{}}, nil
		}
		_, err := node.Execute(apiRequestTestContext(t, map[string]any{}, nil))
		if err == nil || !strings.Contains(err.Error(), "base_url") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("non_positive_timeout", func(t *testing.T) {
		s := newAPIRequestServer(t, 200, "", "", nil)
		for _, bad := range []any{0, -5, "abc", true} {
			node := apiRequestNewNode(t, s, func(n *APIRequest) { n.TimeoutMs = bad })
			if _, err := node.Execute(apiRequestTestContext(t, map[string]any{}, nil)); err == nil {
				t.Fatalf("timeout_ms %v must be rejected", bad)
			}
		}
	})
	t.Run("non_dict_query", func(t *testing.T) {
		s := newAPIRequestServer(t, 200, "", "", nil)
		node := apiRequestNewNode(t, s, func(n *APIRequest) { n.Query = "notadict" })
		if _, err := node.Execute(apiRequestTestContext(t, map[string]any{}, nil)); err == nil {
			t.Fatal("string query must be rejected")
		}
	})
	t.Run("validate_rejects_bad_timeout", func(t *testing.T) {
		node := &APIRequest{BaseNode: BaseNode{ID: "api1"}, TimeoutMs: 0}
		if err := node.Validate(); err == nil {
			t.Fatal("Validate must reject timeout_ms=0")
		}
		ok := &APIRequest{BaseNode: BaseNode{ID: "api1"}, TimeoutMs: "250"}
		if err := ok.Validate(); err != nil {
			t.Fatalf("Validate: %v", err)
		}
	})
}

func TestAPIRequestDryRun(t *testing.T) {
	cases := []struct {
		name    string
		dryRun  bool
		globals map[string]any
	}{
		{"node_dry_run", true, nil},
		{"global_dry_run", false, map[string]any{"dry_run": true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			node := &APIRequest{ // no resolver on purpose: dry-run must not touch credentials
				BaseNode:   BaseNode{ID: "api1"},
				Credential: "cred1",
				DryRun:     tc.dryRun,
			}
			res, err := node.Execute(apiRequestTestContext(t, map[string]any{}, tc.globals))
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			out := res.Result.(map[string]any)
			if dry, _ := out["dry_run"].(bool); !dry {
				t.Errorf("dry_run = %v", out["dry_run"])
			}
			if out["status"] != nil || out["data"] != "[dry-run] would request" {
				t.Errorf("out = %v", out)
			}
		})
	}
}

func TestAPIRequestScrubText(t *testing.T) {
	cases := []struct {
		name string
		in   string
		not  []string // substrings that must not survive
		want []string // substrings that must survive
	}{
		{
			name: "absolute_url_masked",
			in:   `Get "https://user:pw@api.example.com/a/b?token=xyz#frag": dial tcp`,
			not:  []string{"user:pw", "token=xyz", "/a/b", "frag"},
			want: []string{"https://api.example.com", "/***/***?***#***"},
		},
		{
			name: "unsafe_scheme_degrades",
			in:   "postgresql://u:p@db.host/x",
			not:  []string{"u:p", "db.host"},
			want: []string{"<unprintable>"},
		},
		{
			name: "credential_run_masked",
			in:   `auth failed: password=hunter2, API_KEY: "abc123" kept`,
			not:  []string{"hunter2", "abc123"},
			want: []string{"password=", "API_KEY:", "kept"},
		},
		{
			name: "plain_text_untouched",
			in:   "code=1002 port=8080",
			want: []string{"code=1002 port=8080"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := apiRequestScrubText(tc.in)
			for _, s := range tc.not {
				if strings.Contains(out, s) {
					t.Errorf("scrubbed text leaks %q: %s", s, out)
				}
			}
			for _, s := range tc.want {
				if !strings.Contains(out, s) {
					t.Errorf("scrubbed text lost %q: %s", s, out)
				}
			}
		})
	}
}

func TestAPIRequestJSONRoundTrip(t *testing.T) {
	doc := `{"type":"api_request","id":"a1","credential":"cred1","method":"post","path":"x/$INPUT.p",
		"query":{"a":1},"body":{"b":2},"timeout_ms":250,"raise_on_error":false,"allow_redirects":true,"dry_run":true}`
	node := new(APIRequest)
	if err := json.Unmarshal([]byte(doc), node); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if node.Credential != "cred1" || node.Method != "post" || node.Path != "x/$INPUT.p" {
		t.Errorf("fields = %v %v %v", node.Credential, node.Method, node.Path)
	}
	if node.RaiseOnError == nil || *node.RaiseOnError {
		t.Errorf("raise_on_error = %v", node.RaiseOnError)
	}
	if !node.AllowRedirects || !node.DryRun {
		t.Errorf("flags = %v %v", node.AllowRedirects, node.DryRun)
	}
	if node.GetType() != types.NodeType("api_request") {
		t.Errorf("type = %v", node.GetType())
	}
}
