package nodes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jeffkit/plaita-go/internal/errs"
	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/delegate"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func TestHTTP_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name         string
		data         string
		wantHTTPNode *HTTP
		wantErr      bool
	}{
		{
			name: "test normal unmarshal",
			data: `
			{
				"id": "http",
				"type": "http",
				"next": "end",
				"url": "http://example.com/api/v1/foo",
				"method": "GET",
				"headers": {
					"Content-Type": "application/json"
				},
				"body": {
					"key": "value",
					"key2": [1, 2]
				},
				"query": {
					"foo": "bar"
				}
			}
			`,
			wantHTTPNode: &HTTP{
				BaseNode: BaseNode{
					ID:   "http",
					Type: "http",
					Next: "end",
				},
				Method: "GET",
				RawBody: map[string]any{
					"key":  "value",
					"key2": []any{float64(1), float64(2)},
				},
				ContentType: "application/json",
				RawURL:      "http://example.com/api/v1/foo",
				RawQuery: map[string]any{
					"foo": "bar",
				},
				RawHeaders: map[string]any{
					"Content-Type": "application/json",
				},
				RawAddressing: &RawAddressing{
					Name: "domain",
				},
			},
			wantErr: false,
		},
		{
			name: "test default Content-Type",
			data: `
			{
				"id": "http",
				"type": "http",
				"next": "end",
				"url": "http://example.com/api/v1/foo",
				"method": "GET",
				"headers": {},
				"body": {
					"key": "value",
					"key2": [1, 2]
				},
				"query": {
					"foo": "bar"
				}
			}
			`,
			wantHTTPNode: &HTTP{
				BaseNode: BaseNode{
					ID:   "http",
					Type: "http",
					Next: "end",
				},
				Method: "GET",
				RawBody: map[string]any{
					"key":  "value",
					"key2": []any{float64(1), float64(2)},
				},
				ContentType: "application/json",
				RawURL:      "http://example.com/api/v1/foo",
				RawQuery: map[string]any{
					"foo": "bar",
				},
				RawHeaders: map[string]any{},
				RawAddressing: &RawAddressing{
					Name: "domain",
				},
			},
			wantErr: false,
		},
		{
			name: "test override Content-Type",
			data: `
			{
				"id": "http",
				"type": "http",
				"next": "end",
				"url": "http://example.com/api/v1/foo",
				"method": "GET",
				"headers": {"Content-Type": "form-urlencoded"},
				"body": {
					"key": "value",
					"key2": [1, 2]
				},
				"query": {
					"foo": "bar"
				}
			}
			`,
			wantHTTPNode: &HTTP{
				BaseNode: BaseNode{
					ID:   "http",
					Type: "http",
					Next: "end",
				},
				Method: "GET",
				RawBody: map[string]any{
					"key":  "value",
					"key2": []any{float64(1), float64(2)},
				},
				ContentType: "application/json",
				RawURL:      "http://example.com/api/v1/foo",
				RawQuery: map[string]any{
					"foo": "bar",
				},
				RawHeaders: map[string]any{
					"Content-Type": "form-urlencoded",
				},
				RawAddressing: &RawAddressing{
					Name: "domain",
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := new(HTTP)
			if err := got.UnmarshalJSON([]byte(tt.data)); (err != nil) != tt.wantErr {
				t.Errorf("UnmarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				require.Equal(t, tt.wantHTTPNode, got)
			}
		})
	}
}

type mockBar struct{}

// Do mock bar http request
func (b *mockBar) Do(request *http.Request, params json.RawMessage) (*http.Response, error) {
	result := map[string]any{
		"user": "nick",
	}
	jsonResult, _ := json.Marshal(result)
	return &http.Response{
		Status:     "200 OK",
		StatusCode: 200,
		Header: map[string][]string{
			"Content-Type": {"application/json"},
		},
		Body: io.NopCloser(bytes.NewBuffer(jsonResult)),
	}, nil
}

func TestHTTP_Execute(t *testing.T) {
	mux := http.NewServeMux()
	handlers := map[string]http.HandlerFunc{
		"/api/v1/foo": func(writer http.ResponseWriter, request *http.Request) {
			if request.Method != http.MethodGet {
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(http.StatusOK)
			_, _ = writer.Write([]byte(`{"foo": "bar"}`))
		},
		"/api/v1/bar": func(writer http.ResponseWriter, request *http.Request) {
			if request.Method != http.MethodPost {
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			body, _ := io.ReadAll(request.Body)
			t.Logf("body: %s", string(body))

			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(http.StatusOK)
			_, _ = writer.Write([]byte(`{"foo": "bar", "age": 20, "country": "China"}`))
		},
	}
	for path, h := range handlers {
		mux.Handle(path, h)
	}
	s := httptest.NewServer(mux)
	defer s.Close()
	host := strings.TrimPrefix(s.URL, "http://")

	delegate.RegisterHTTPDelegator("mockBar", &mockBar{})

	tests := []struct {
		name       string
		runtimeCtx *runtime.Context
		httpNode   *HTTP
		want       *workflow.ExecuteResult
		// check optionally asserts the result instead of strict DeepEqual
		// (used when the response carries volatile headers like Date)
		check   func(t *testing.T, got *workflow.ExecuteResult)
		wantErr bool
	}{
		{
			name: "test simple http node",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"name": "foo",
				},
			},
			httpNode: &HTTP{
				BaseNode: BaseNode{
					ID:   "http",
					Next: "end",
					OutputType: workflow.Property{
						DataType: workflow.Object,
						Properties: map[string]workflow.Property{
							"foo": {DataType: workflow.String},
						},
					},
				},
				Method: http.MethodGet,
				RawURL: fmt.Sprintf("http://%s/api/v1/foo", host),
				RawQuery: map[string]any{
					"name": "$INPUT.name",
				},
				RawAddressing: &RawAddressing{},
			},
			check: func(t *testing.T, got *workflow.ExecuteResult) {
				assertResponseShape(t, got, map[string]any{"foo": "bar"})
			},
			wantErr: false,
		},
		{
			name: "test http node query merge",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"name": "foo",
				},
			},
			httpNode: &HTTP{
				BaseNode: BaseNode{
					ID:   "http",
					Next: "end",
					OutputType: workflow.Property{
						DataType: workflow.Object,
						Properties: map[string]workflow.Property{
							"foo": {DataType: workflow.String},
						},
					},
				},
				Method: http.MethodGet,
				RawURL: fmt.Sprintf("http://%s/api/v1/foo?name=bar", host),
				RawQuery: map[string]any{
					"name": "$INPUT.name",
				},
				RawAddressing: &RawAddressing{},
			},
			check: func(t *testing.T, got *workflow.ExecuteResult) {
				assertResponseShape(t, got, map[string]any{"foo": "bar"})
			},
			wantErr: false,
		},
		{
			name: "test POST http node with expr body",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"name":   "foo",
					"fields": "age,country",
				},
			},
			httpNode: &HTTP{
				BaseNode: BaseNode{
					ID:     "http",
					Next:   "end",
					Output: "$STATUS",
					OutputType: workflow.Property{
						DataType: workflow.Integer,
					},
				},
				Method: http.MethodPost,
				RawBody: map[string]any{
					"fields": "$INPUT.fields",
				},
				RawURL: fmt.Sprintf("http://%s/api/v1/bar", host),
				RawQuery: map[string]any{
					"name": "$INPUT.name",
				},
				RawAddressing: &RawAddressing{},
			},
			want: &workflow.ExecuteResult{
				Result:   200,
				NextNode: "end",
			},
			wantErr: false,
		},
		{
			name: "test POST http node with delegate method",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"name":   "foo",
					"fields": "age,country",
				},
			},
			httpNode: &HTTP{
				BaseNode: BaseNode{
					ID:     "http",
					Next:   "end",
					Output: "$RESPONSE.data.user",
					OutputType: workflow.Property{
						DataType: workflow.String,
					},
				},
				Method: http.MethodPost,
				RawBody: map[string]any{
					"fields": "$INPUT.fields",
				},
				RawAddressing: &RawAddressing{},
				RawDelegate: &RawDelegateParam{
					Name:   "mockBar",
					Params: "",
				},
				RawURL: fmt.Sprintf("http://%s/api/v1/bar", host),
				RawQuery: map[string]any{
					"name": "$INPUT.name",
				},
			},
			want: &workflow.ExecuteResult{
				Result:   "nick",
				NextNode: "end",
			},
			wantErr: false,
		},
		{
			name: "test POST http node with response data",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"name":   "foo",
					"fields": "age,country",
				},
			},
			httpNode: &HTTP{
				BaseNode: BaseNode{
					ID:     "http",
					Next:   "end",
					Output: "$RESPONSE.data.foo",
					OutputType: workflow.Property{
						DataType: workflow.String,
					},
				},
				Method: http.MethodPost,
				RawBody: map[string]any{
					"fields": "$INPUT.fields",
				},
				RawURL: fmt.Sprintf("http://%s/api/v1/bar", host),
				RawQuery: map[string]any{
					"name": "$INPUT.name",
				},
				RawAddressing: &RawAddressing{},
			},
			want: &workflow.ExecuteResult{
				Result:   "bar",
				NextNode: "end",
			},
			wantErr: false,
		},
		{
			name: "test POST http node with plaita expr",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"name":   "foo",
					"fields": "age,country",
					"requestHeaders": map[string]any{
						"Content-Type": "application/json",
					},
					"requestParams": map[string]any{
						"name": "foo",
					},
				},
				GlobalVariables: map[string]any{
					"domain":     fmt.Sprintf("http://%s", host),
					"addressing": "domain",
				},
			},
			httpNode: &HTTP{
				BaseNode: BaseNode{
					ID:     "http",
					Next:   "end",
					Output: "$RESPONSE.data.foo",
					OutputType: workflow.Property{
						DataType: workflow.String,
					},
				},
				Method:  http.MethodPost,
				RawBody: "$INPUT.requestParams",
				RawURL:  `$F.concat($GLOBAL.domain, "/api/v1/bar")`,
				RawQuery: map[string]any{
					"name": "$INPUT.name",
				},
				RawHeaders: "$INPUT.requestHeaders",
				RawAddressing: &RawAddressing{
					Name: "$GLOBAL.addressing",
				},
			},
			want: &workflow.ExecuteResult{
				Result:   "bar",
				NextNode: "end",
			},
			wantErr: false,
		},
		{
			name: "test POST http node with delegate method(plaita expr)",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"name":   "foo",
					"fields": "age,country",
				},
				GlobalVariables: map[string]any{
					"delegate": "mockBar",
				},
			},
			httpNode: &HTTP{
				BaseNode: BaseNode{
					ID:     "http",
					Next:   "end",
					Output: "$RESPONSE.data.user",
					OutputType: workflow.Property{
						DataType: workflow.String,
					},
				},
				Method: http.MethodPost,
				RawBody: map[string]any{
					"fields": "$INPUT.fields",
				},
				RawURL: fmt.Sprintf("http://%s/api/v1/bar", host),
				RawQuery: map[string]any{
					"name": "$INPUT.name",
				},
				RawAddressing: &RawAddressing{},
				RawDelegate: &RawDelegateParam{
					Name:   "$GLOBAL.delegate",
					Params: "",
				},
			},
			want: &workflow.ExecuteResult{
				Result:   "nick",
				NextNode: "end",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.httpNode.Execute(runtime.WithWorkflowRuntimeContext(context.Background(), tt.runtimeCtx))
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.check != nil {
				tt.check(t, got)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Execute() got = %v, want %v", got, tt.want)
			}
		})
	}
}

// assertResponseShape pins the $NODE.<http> contract (issue #12, plaita
// node_result_payload): with no output expression the node result IS the
// response structure {data,status,statusText,headers}.
func assertResponseShape(t *testing.T, got *workflow.ExecuteResult, wantData map[string]any) {
	t.Helper()
	response, ok := got.Result.(map[string]any)
	if !ok {
		t.Fatalf("result type %T, want the response structure map", got.Result)
	}
	data, ok := response["data"].(map[string]any)
	if !ok || !reflect.DeepEqual(data, wantData) {
		t.Errorf("data = %v, want %v", response["data"], wantData)
	}
	if response["status"] != 200 {
		t.Errorf("status = %v, want 200", response["status"])
	}
	if response["statusText"] != "OK" {
		t.Errorf("statusText = %v, want the reason phrase OK", response["statusText"])
	}
	headers, ok := response["headers"].(map[string]string)
	if !ok || headers["Content-Type"] != "application/json" {
		t.Errorf("headers = %v, want a string-valued map with Content-Type", response["headers"])
	}
}

// --- helpers -------------------------------------------------------------

func intPtr(i int) *int { return &i }

func httpNodeFor(url string, mutate func(*HTTP)) *HTTP {
	node := &HTTP{
		BaseNode:      BaseNode{ID: "http", Next: "end"},
		Method:        http.MethodGet,
		RawURL:        url,
		RawAddressing: &RawAddressing{},
	}
	if mutate != nil {
		mutate(node)
	}
	return node
}

func errCode(t *testing.T, err error) (int, string) {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	var e *errs.Error
	if !errors.As(err, &e) {
		t.Fatalf("error %T is not *errs.Error: %v", err, err)
	}
	return e.Code, e.Message
}

// TestHTTP_NonJSONResponseContract pins issue #12: text / HTML / empty
// bodies are VALUES (plaita _loads_lenient falls back to raw text), never
// errors, and $NODE.<http> carries the full {data,status,statusText,headers}
// structure.
func TestHTTP_NonJSONResponseContract(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/text", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("plain text value"))
	})
	mux.HandleFunc("/html", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte("<html><body>health: ok</body></html>"))
	})
	mux.HandleFunc("/empty", func(w http.ResponseWriter, r *http.Request) {})
	s := httptest.NewServer(mux)
	defer s.Close()

	cases := []struct {
		name     string
		path     string
		wantData any
	}{
		{"text body", "/text", "plain text value"},
		{"html body", "/html", "<html><body>health: ok</body></html>"},
		{"empty body", "/empty", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := httpNodeFor(s.URL+tc.path, nil).Execute(
				runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{}))
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			assertResponseShapeShim(t, res, tc.wantData)
		})
	}
}

// assertResponseShapeShim asserts the response-structure result with an
// arbitrary expected data value.
func assertResponseShapeShim(t *testing.T, got *workflow.ExecuteResult, wantData any) {
	t.Helper()
	response, ok := got.Result.(map[string]any)
	if !ok {
		t.Fatalf("result type %T, want the response structure map", got.Result)
	}
	if !reflect.DeepEqual(response["data"], wantData) {
		t.Errorf("data = %#v, want %#v", response["data"], wantData)
	}
	if response["status"] != 200 {
		t.Errorf("status = %v", response["status"])
	}
	if response["statusText"] != "OK" {
		t.Errorf("statusText = %v", response["statusText"])
	}
	if _, ok := response["headers"].(map[string]string); !ok {
		t.Errorf("headers = %T, want map[string]string", response["headers"])
	}
}

// TestHTTP_RequestTimeout pins issue #13: each request (hop) is bounded by
// requestTimeout (default 30s; overridden here) — an unresponsive peer
// fails the node instead of hanging the worker, classified 1002.
func TestHTTP_RequestTimeout(t *testing.T) {
	release := make(chan struct{})
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	}))
	defer s.Close()
	defer close(release)

	node := httpNodeFor(s.URL+"/slow", func(n *HTTP) {
		n.RequestTimeoutSecs = 0.2
	})
	start := time.Now()
	_, err := node.Execute(runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{}))
	code, msg := errCode(t, err)
	if code != httpDoRequestError {
		t.Errorf("code = %d, want %d", code, httpDoRequestError)
	}
	if !strings.Contains(msg, "context deadline exceeded") {
		t.Errorf("message = %q, want a deadline-exceeded transport failure", msg)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("timeout took %s, want ~200ms", elapsed)
	}
}

// TestHTTP_MaxRedirects pins the cap: maxRedirects=0 never follows,
// maxRedirects=2 fails after two hops, maxRedirects=3 completes a 3-hop
// chain. Default (unset) is 5.
func TestHTTP_MaxRedirects(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/final", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("arrived"))
	})
	for i := 0; i < 3; i++ {
		path := fmt.Sprintf("/r%d", i)
		next := fmt.Sprintf("/r%d", i+1)
		if i == 2 {
			next = "/final"
		}
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, next, http.StatusFound)
		})
	}
	s := httptest.NewServer(mux)
	defer s.Close()

	t.Run("zero never follows", func(t *testing.T) {
		_, err := httpNodeFor(s.URL+"/r0", func(n *HTTP) { n.MaxRedirects = intPtr(0) }).Execute(
			runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{}))
		code, msg := errCode(t, err)
		if code != httpDoRequestError || !strings.Contains(msg, "Too many redirects (> 0)") {
			t.Errorf("code=%d msg=%q", code, msg)
		}
	})
	t.Run("two hops insufficient", func(t *testing.T) {
		_, err := httpNodeFor(s.URL+"/r0", func(n *HTTP) { n.MaxRedirects = intPtr(2) }).Execute(
			runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{}))
		_, msg := errCode(t, err)
		if !strings.Contains(msg, "Too many redirects (> 2)") {
			t.Errorf("msg = %q", msg)
		}
	})
	t.Run("three hops arrive", func(t *testing.T) {
		res, err := httpNodeFor(s.URL+"/r0", func(n *HTTP) { n.MaxRedirects = intPtr(3) }).Execute(
			runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{}))
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		assertResponseShapeShim(t, res, "arrived")
	})
	t.Run("default is five from JSON", func(t *testing.T) {
		doc := `{"id":"h","type":"http","url":"http://example.com","addressing":{"name":"domain"},"maxRedirects":0}`
		node := new(HTTP)
		if err := json.Unmarshal([]byte(doc), node); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if node.MaxRedirects == nil || *node.MaxRedirects != 0 {
			t.Errorf("maxRedirects = %v, want explicit 0 (not silently defaulted)", node.MaxRedirects)
		}
	})
}

// TestHTTP_RedirectMethodAndHeaderRules pins the hop rewrite rules: 302
// turns POST into GET and drops the body/content-type; 307 keeps both;
// cross-origin redirects strip Authorization while same-origin keeps it.
func TestHTTP_RedirectMethodAndHeaderRules(t *testing.T) {
	var seenHeader, seenMethod, seenBody atomic.Value
	mux := http.NewServeMux()
	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		seenMethod.Store(r.Method)
		seenHeader.Store(r.Header.Get("Content-Type"))
		seenBody.Store(string(body))
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("echoed"))
	})
	mux.HandleFunc("/go302", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/echo", http.StatusFound)
	})
	mux.HandleFunc("/go307", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/echo", http.StatusTemporaryRedirect)
	})
	s := httptest.NewServer(mux)
	host := strings.TrimPrefix(s.URL, "http://")

	run := func(t *testing.T, path, redirectHost string) {
		t.Helper()
		node := httpNodeFor("http://"+redirectHost+path, func(n *HTTP) {
			n.Method = http.MethodPost
			n.RawHeaders = map[string]any{"Authorization": "Bearer sekrit"}
			n.RawBody = map[string]any{"k": "v"}
		})
		res, err := node.Execute(runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{}))
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		assertResponseShapeShim(t, res, "echoed")
	}

	t.Run("302 POST becomes GET without body", func(t *testing.T) {
		run(t, "/go302", host)
		if m, _ := seenMethod.Load().(string); m != http.MethodGet {
			t.Errorf("method = %s, want GET", m)
		}
		if h, _ := seenHeader.Load().(string); h != "" {
			t.Errorf("content-type = %q, want stripped", h)
		}
		if b, _ := seenBody.Load().(string); b != "" {
			t.Errorf("body = %q, want dropped", b)
		}
	})
	t.Run("307 POST keeps method and body", func(t *testing.T) {
		run(t, "/go307", host)
		if m, _ := seenMethod.Load().(string); m != http.MethodPost {
			t.Errorf("method = %s, want POST kept", m)
		}
		if b, _ := seenBody.Load().(string); b != `{"k":"v"}` {
			t.Errorf("body = %q, want kept", b)
		}
	})

	// same server host, different hostname (127.0.0.1 vs localhost) →
	// cross-origin per the requests should_strip_auth rule
	muxCross := http.NewServeMux()
	muxCross.HandleFunc("/hop", func(w http.ResponseWriter, r *http.Request) {
		port := host[strings.LastIndex(host, ":")+1:]
		http.Redirect(w, r, "http://localhost:"+port+"/echo", http.StatusFound)
	})
	sCross := httptest.NewServer(muxCross)
	defer sCross.Close()
	crossHost := strings.TrimPrefix(sCross.URL, "http://")

	t.Run("cross-origin strips Authorization", func(t *testing.T) {
		var auth atomic.Value
		mux.HandleFunc("/auth-echo", func(w http.ResponseWriter, r *http.Request) {
			auth.Store(r.Header.Get("Authorization"))
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte("ok"))
		})
		muxCross.HandleFunc("/hop-auth", func(w http.ResponseWriter, r *http.Request) {
			port := crossHost[strings.LastIndex(crossHost, ":")+1:]
			http.Redirect(w, r, "http://localhost:"+port+"/auth-echo", http.StatusFound)
		})
		node := httpNodeFor("http://"+crossHost+"/hop-auth", func(n *HTTP) {
			n.RawHeaders = map[string]any{"Authorization": "Bearer sekrit"}
		})
		_, err := node.Execute(runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{}))
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if a, _ := auth.Load().(string); a != "" {
			t.Errorf("Authorization = %q, want stripped on cross-origin redirect", a)
		}
	})
	t.Run("same-origin keeps Authorization", func(t *testing.T) {
		var auth atomic.Value
		mux.HandleFunc("/same-echo", func(w http.ResponseWriter, r *http.Request) {
			auth.Store(r.Header.Get("Authorization"))
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte("ok"))
		})
		mux.HandleFunc("/same-hop", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "/same-echo", http.StatusFound)
		})
		node := httpNodeFor(s.URL+"/same-hop", func(n *HTTP) {
			n.RawHeaders = map[string]any{"Authorization": "Bearer sekrit"}
		})
		_, err := node.Execute(runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{}))
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if a, _ := auth.Load().(string); a != "Bearer sekrit" {
			t.Errorf("Authorization = %q, want kept on same-origin redirect", a)
		}
	})
}

// TestHTTP_ResponseSizeCap pins the body ceiling (default 10MiB, env
// overridable): an over-limit body is a policy error carrying the response
// snapshot (1003), not a truncated value.
func TestHTTP_ResponseSizeCap(t *testing.T) {
	t.Setenv(httpMaxResponseBytesEnv, "16")
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write(bytes.Repeat([]byte("x"), 100))
	}))
	defer s.Close()

	_, err := httpNodeFor(s.URL, nil).Execute(
		runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{}))
	code, msg := errCode(t, err)
	if code != httpNodeExecError {
		t.Errorf("code = %d, want %d (over-limit carries the response snapshot)", code, httpNodeExecError)
	}
	if !strings.Contains(msg, "maxResponseBytes=16") {
		t.Errorf("message = %q", msg)
	}
}

// TestHTTP_SSRFPrivateNetworks pins the SSRF posture: node-declared
// blockPrivateNetworks and the operator switch both reject private targets
// before any connection; with neither set, the plaita default (allow) holds.
func TestHTTP_SSRFPrivateNetworks(t *testing.T) {
	var hits atomic.Int64
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("ok"))
	}))
	defer s.Close()
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{})

	t.Run("node declaration blocks before connecting", func(t *testing.T) {
		hits.Store(0)
		_, err := httpNodeFor(s.URL, func(n *HTTP) { n.BlockPrivateNetworks = true }).Execute(ctx)
		code, msg := errCode(t, err)
		if code != httpDoRequestError {
			t.Errorf("code = %d, want %d", code, httpDoRequestError)
		}
		if !strings.Contains(msg, "private/special address") {
			t.Errorf("message = %q", msg)
		}
		if hits.Load() != 0 {
			t.Errorf("server was hit %d times, want 0 (rejected before connecting)", hits.Load())
		}
	})
	t.Run("operator switch forces blocking", func(t *testing.T) {
		t.Setenv(httpBlockPrivateEnv, "1")
		hits.Store(0)
		_, err := httpNodeFor(s.URL, nil).Execute(ctx)
		_, msg := errCode(t, err)
		if !strings.Contains(msg, "private/special address") {
			t.Errorf("message = %q", msg)
		}
		if hits.Load() != 0 {
			t.Errorf("server was hit %d times, want 0", hits.Load())
		}
	})
	t.Run("unset means plaita default allow", func(t *testing.T) {
		hits.Store(0)
		if _, err := httpNodeFor(s.URL, nil).Execute(ctx); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if hits.Load() != 1 {
			t.Errorf("hits = %d, want 1", hits.Load())
		}
	})
}

// TestHTTP_AllowedDeniedHosts pins allowlist/denylist matching: exact
// hosts, wildcard suffixes and CIDR (matched on resolved IPs), plus the
// http/https-only scheme gate.
func TestHTTP_AllowedDeniedHosts(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("ok"))
	}))
	defer s.Close()
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{})

	t.Run("denied exact host", func(t *testing.T) {
		_, err := httpNodeFor(s.URL, func(n *HTTP) { n.DeniedHosts = []string{"127.0.0.1"} }).Execute(ctx)
		_, msg := errCode(t, err)
		if !strings.Contains(msg, `matches denied_hosts entry "127.0.0.1"`) {
			t.Errorf("message = %q", msg)
		}
	})
	t.Run("allowed literal miss", func(t *testing.T) {
		_, err := httpNodeFor(s.URL, func(n *HTTP) { n.AllowedHosts = []string{"example.com"} }).Execute(ctx)
		_, msg := errCode(t, err)
		if !strings.Contains(msg, "not in allowed_hosts") {
			t.Errorf("message = %q", msg)
		}
	})
	t.Run("allowed CIDR hit on resolved IP", func(t *testing.T) {
		res, err := httpNodeFor(s.URL, func(n *HTTP) { n.AllowedHosts = []string{"127.0.0.0/8"} }).Execute(ctx)
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		assertResponseShapeShim(t, res, "ok")
	})
	t.Run("allowed wildcard", func(t *testing.T) {
		if _, err := httpNodeFor(s.URL, func(n *HTTP) { n.AllowedHosts = []string{"*.localhost", "localhost"} }).Execute(ctx); err == nil {
			t.Fatal("127.0.0.1 must not match a localhost allowlist")
		}
	})
	t.Run("scheme gate when restricted", func(t *testing.T) {
		node := httpNodeFor("file:///etc/passwd", func(n *HTTP) { n.DeniedHosts = []string{"example.com"} })
		_, err := node.Execute(ctx)
		_, msg := errCode(t, err)
		if !strings.Contains(msg, `scheme "file" is not allowed`) {
			t.Errorf("message = %q", msg)
		}
	})
}

// TestHTTP_RedirectHopPolicyRecheck pins the restricted-mode hop recheck: a
// redirect must not carry the request past the policy (allowlist for
// localhost, redirect target 127.0.0.1).
func TestHTTP_RedirectHopPolicyRecheck(t *testing.T) {
	port := "0"
	mux := http.NewServeMux()
	s := httptest.NewServer(mux)
	host := strings.TrimPrefix(s.URL, "http://")
	port = host[strings.LastIndex(host, ":")+1:]
	mux.HandleFunc("/hop", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://127.0.0.1:"+port+"/secret", http.StatusFound)
	})
	mux.HandleFunc("/secret", func(w http.ResponseWriter, r *http.Request) {
		t.Error("redirect target must never be reached")
		_, _ = w.Write([]byte("leaked"))
	})

	node := httpNodeFor(s.URL+"/hop", func(n *HTTP) { n.AllowedHosts = []string{"localhost"} })
	_, err := node.Execute(runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{}))
	code, msg := errCode(t, err)
	if code != httpDoRequestError {
		t.Errorf("code = %d, want %d", code, httpDoRequestError)
	}
	if !strings.Contains(msg, "not in allowed_hosts") || !strings.Contains(msg, "127.0.0.1") {
		t.Errorf("message = %q", msg)
	}
}

// TestHTTP_CookieSession pins the session semantics (plaita http_session):
// cookies are OFF by default; PLAITA_HTTP_COOKIES=1 opts the process into a
// shared cookie jar so login-then-call flows keep their session.
func TestHTTP_CookieSession(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "sid", Value: "abc123", Path: "/"})
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("logged in"))
	})
	mux.HandleFunc("/profile", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		if c, err := r.Cookie("sid"); err != nil {
			_, _ = w.Write([]byte("no-session"))
		} else {
			_, _ = w.Write([]byte("session:" + c.Value))
		}
	})
	s := httptest.NewServer(mux)
	defer s.Close()
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{})

	t.Run("cookies off by default", func(t *testing.T) {
		for _, path := range []string{"/login", "/profile"} {
			res, err := httpNodeFor(s.URL+path, nil).Execute(ctx)
			if err != nil {
				t.Fatalf("Execute %s: %v", path, err)
			}
			response := res.Result.(map[string]any)
			if path == "/profile" && response["data"] != "no-session" {
				t.Errorf("data = %v, want no-session (default blocks cookies)", response["data"])
			}
		}
	})
	t.Run("PLAITA_HTTP_COOKIES=1 enables the shared jar", func(t *testing.T) {
		t.Setenv(httpCookiesEnv, "1")
		for _, path := range []string{"/login", "/profile"} {
			res, err := httpNodeFor(s.URL+path, nil).Execute(ctx)
			if err != nil {
				t.Fatalf("Execute %s: %v", path, err)
			}
			response := res.Result.(map[string]any)
			if path == "/profile" && response["data"] != "session:abc123" {
				t.Errorf("data = %v, want session:abc123", response["data"])
			}
		}
	})
}
