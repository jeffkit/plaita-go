package nodes

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// hitlStubServer is a minimal fake hitl-server (POST /api/send + GET
// /api/poll/{id}) covering the behaviors the table-driven cases need:
// reply (after pollAfter polls), waiting, send_fail, poll_error,
// poll_timeout, probe_fail / probe_html (image probe degradation).
// Everything runs against httptest — no real hitl-server is ever contacted.
type hitlStubServer struct {
	srv          *httptest.Server
	mu           sync.Mutex
	behavior     string
	pollBehavior string // GET /api/poll behavior; empty → behavior
	pollAfter    int
	replies      []map[string]any // override reply entries; nil → [{"content":"同意"}]
	sends        []map[string]any // decoded POST /api/send bodies in order
	sendPaths    []string
	polls        int
	pollPaths    []string
}

func newHitlStub(t *testing.T, behavior string) *hitlStubServer {
	t.Helper()
	s := &hitlStubServer{behavior: behavior, pollAfter: 1}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, "bad json", http.StatusBadRequest)
				return
			}
			s.mu.Lock()
			s.sends = append(s.sends, body)
			s.sendPaths = append(s.sendPaths, r.URL.Path)
			behavior := s.behavior
			s.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			switch {
			case behavior == "send_fail":
				_ = json.NewEncoder(w).Encode(map[string]any{"success": false, "error": "prepare failed"})
			case behavior == "probe_fail" && bodyHasImages(body):
				_ = json.NewEncoder(w).Encode(map[string]any{"success": false})
			case behavior == "probe_html" && bodyHasImages(body):
				w.Header().Set("Content-Type", "text/html")
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte("<html>boom</html>"))
			default:
				_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "session_id": "sess-1"})
			}
		case http.MethodGet:
			s.mu.Lock()
			s.polls++
			poll := s.polls
			behavior := s.behavior
			if s.pollBehavior != "" {
				behavior = s.pollBehavior
			}
			pollAfter := s.pollAfter
			replies := s.replies
			s.pollPaths = append(s.pollPaths, r.URL.Path)
			s.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			switch {
			case behavior == "poll_error":
				_ = json.NewEncoder(w).Encode(map[string]any{"status": "error"})
			case behavior == "poll_timeout":
				_ = json.NewEncoder(w).Encode(map[string]any{"has_reply": false, "replies": []any{}, "status": "timeout"})
			case behavior == "reply" && poll >= pollAfter:
				if replies == nil {
					replies = []map[string]any{{"content": "同意"}}
				}
				entries := make([]any, 0, len(replies))
				for _, r := range replies {
					entries = append(entries, r)
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"has_reply": true, "replies": entries, "status": "ok"})
			default:
				_ = json.NewEncoder(w).Encode(map[string]any{"has_reply": false, "replies": []any{}, "status": "waiting"})
			}
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func bodyHasImages(body map[string]any) bool {
	_, ok := body["images"]
	return ok
}

func (s *hitlStubServer) recorded() (sends int, polls int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sends), s.polls
}

func hitlBoolPtr(b bool) *bool { return &b }

// hitlCase is one table row of the full-flow cases.
type hitlCase struct {
	name     string
	behavior string
	// pollBehavior overrides the GET /api/poll behavior (the image-probe
	// degradations need a failing POST probe but a replying poll).
	pollBehavior string
	node         func(url string) *Hitl
	globals      map[string]any
	want         map[string]any // expected output dict (compared key-by-key)
	wantErr      string         // non-empty → Execute must fail with a *HitlError containing it
}

func TestHitlExecuteTable(t *testing.T) {
	cases := []hitlCase{
		{
			name:     "replied after polling",
			behavior: "reply",
			node: func(url string) *Hitl {
				return &Hitl{BaseNode: BaseNode{ID: "t"}, Message: "确认发布？",
					BaseURL: url, TimeoutSecs: 5, PollInterval: 0.01}
			},
			want: map[string]any{"status": "replied", "replies": []any{"同意"},
				"session_id": "sess-1", "dry_run": false},
		},
		{
			name:     "deadline returns timeout not error",
			behavior: "waiting",
			node: func(url string) *Hitl {
				return &Hitl{BaseNode: BaseNode{ID: "t"}, Message: "确认？",
					BaseURL: url, TimeoutSecs: 1, PollInterval: 0.05}
			},
			want: map[string]any{"status": "timeout", "replies": []any{},
				"session_id": "sess-1", "dry_run": false},
		},
		{
			name:     "poll status=timeout breaks immediately",
			behavior: "poll_timeout",
			node: func(url string) *Hitl {
				return &Hitl{BaseNode: BaseNode{ID: "t"}, Message: "确认？",
					BaseURL: url, TimeoutSecs: 30, PollInterval: 0.01}
			},
			want: map[string]any{"status": "timeout", "replies": []any{},
				"session_id": "sess-1", "dry_run": false},
		},
		{
			name:     "send success=false raises HitlError",
			behavior: "send_fail",
			node: func(url string) *Hitl {
				return &Hitl{BaseNode: BaseNode{ID: "t"}, Message: "确认？", BaseURL: url}
			},
			wantErr: "发送失败",
		},
		{
			name:     "poll status=error raises HitlError",
			behavior: "poll_error",
			node: func(url string) *Hitl {
				return &Hitl{BaseNode: BaseNode{ID: "t"}, Message: "确认？",
					BaseURL: url, TimeoutSecs: 5, PollInterval: 0.01}
			},
			wantErr: "轮询错误",
		},
		{
			name:     "dry run simulates agree",
			behavior: "reply",
			node: func(url string) *Hitl {
				return &Hitl{BaseNode: BaseNode{ID: "t"}, Message: "确认？",
					BaseURL: "http://127.0.0.1:1", DryRun: true}
			},
			want: map[string]any{"status": "replied", "replies": []any{"同意"},
				"session_id": "dryrun", "dry_run": true},
		},
		{
			name:     "dry run send-only",
			behavior: "reply",
			node: func(url string) *Hitl {
				return &Hitl{BaseNode: BaseNode{ID: "t"}, Message: "确认？",
					BaseURL: "http://127.0.0.1:1", DryRun: true, WaitReply: hitlBoolPtr(false)}
			},
			want: map[string]any{"status": "sent", "replies": []any{},
				"session_id": "dryrun", "dry_run": true},
		},
		{
			name:     "global dry_run wins without server contact",
			behavior: "reply",
			globals:  map[string]any{"dry_run": true},
			node: func(url string) *Hitl {
				return &Hitl{BaseNode: BaseNode{ID: "t"}, Message: "确认？",
					BaseURL: "http://127.0.0.1:1"}
			},
			want: map[string]any{"status": "replied", "replies": []any{"同意"},
				"session_id": "dryrun", "dry_run": true},
		},
		{
			name:     "wait_reply false sends without polling",
			behavior: "reply",
			node: func(url string) *Hitl {
				return &Hitl{BaseNode: BaseNode{ID: "t"}, Message: "周报好了",
					BaseURL: url, TimeoutSecs: 5, WaitReply: hitlBoolPtr(false)}
			},
			want: map[string]any{"status": "sent", "replies": []any{},
				"session_id": "sess-1", "dry_run": false},
		},
		{
			name:         "image probe failure degrades message body",
			behavior:     "probe_fail",
			pollBehavior: "reply",
			node: func(url string) *Hitl {
				return &Hitl{BaseNode: BaseNode{ID: "t"}, Message: "看图确认",
					BaseURL: url, TimeoutSecs: 5, PollInterval: 0.01,
					Images: []any{"/tmp/cover.png"}}
			},
			want: map[string]any{"status": "replied", "replies": []any{"同意"},
				"session_id": "sess-1", "dry_run": false},
		},
		{
			name:         "image probe non-json response degrades too",
			behavior:     "probe_html",
			pollBehavior: "reply",
			node: func(url string) *Hitl {
				return &Hitl{BaseNode: BaseNode{ID: "t"}, Message: "看图确认",
					BaseURL: url, TimeoutSecs: 5, PollInterval: 0.01,
					Images: []any{"/tmp/cover.png"}}
			},
			want: map[string]any{"status": "replied", "replies": []any{"同意"},
				"session_id": "sess-1", "dry_run": false},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			stub := newHitlStub(t, tc.behavior)
			stub.pollBehavior = tc.pollBehavior
			node := tc.node(stub.srv.URL)
			res, err := node.Execute(gateTestCtx(t, map[string]any{}, tc.globals))
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("Execute: want error containing %q, got result %v", tc.wantErr, res.Result)
				}
				var hitlErr *HitlError
				if !errors.As(err, &hitlErr) {
					t.Fatalf("Execute: want *HitlError, got %T: %v", err, err)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("Execute: error %q missing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			out, ok := res.Result.(map[string]any)
			if !ok {
				t.Fatalf("result type %T", res.Result)
			}
			for k, want := range tc.want {
				if !reflect.DeepEqual(out[k], want) {
					t.Errorf("out[%q] = %v, want %v", k, out[k], want)
				}
			}
			if len(out) != len(tc.want) {
				t.Errorf("output keys = %v, want exactly %v", out, tc.want)
			}
		})
	}
}

func TestHitlRepliedWireContract(t *testing.T) {
	stub := newHitlStub(t, "reply")
	stub.pollAfter = 2
	node := &Hitl{BaseNode: BaseNode{ID: "t"}, Message: "确认发布？",
		BaseURL: stub.srv.URL, TimeoutSecs: 5, PollInterval: 0.01}
	res, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["status"] != "replied" {
		t.Errorf("status = %v", out["status"])
	}
	// wire contract: iLink upstream fixed, wait_reply/timeout echoed
	main := stub.sends[len(stub.sends)-1]
	if main["upstream"] != "ilink" {
		t.Errorf("upstream = %v, want ilink", main["upstream"])
	}
	if main["wait_reply"] != true {
		t.Errorf("wait_reply = %v, want true", main["wait_reply"])
	}
	if main["timeout"] != float64(5) {
		t.Errorf("timeout = %v, want 5", main["timeout"])
	}
	if main["message"] != "确认发布？" {
		t.Errorf("message = %v", main["message"])
	}
	// poll_after=2: the first poll waits, the second replies
	if stub.polls != 2 {
		t.Errorf("polls = %d, want 2", stub.polls)
	}
	if got := stub.pollPaths[len(stub.pollPaths)-1]; got != "/api/poll/sess-1" {
		t.Errorf("poll path = %q", got)
	}
	if got := stub.sendPaths[0]; got != "/api/send" {
		t.Errorf("send path = %q", got)
	}
}

func TestHitlImageProbeBodies(t *testing.T) {
	stub := newHitlStub(t, "probe_fail")
	stub.pollBehavior = "reply"
	node := &Hitl{BaseNode: BaseNode{ID: "t"}, Message: "看图确认",
		BaseURL: stub.srv.URL, TimeoutSecs: 5, PollInterval: 0.01,
		Images: []any{"/tmp/cover.png"}}
	if _, err := node.Execute(gateTestCtx(t, map[string]any{}, nil)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(stub.sends) != 2 {
		t.Fatalf("sends = %d, want probe + main", len(stub.sends))
	}
	probe, main := stub.sends[0], stub.sends[1]
	if !reflect.DeepEqual(probe["images"], []any{"/tmp/cover.png"}) {
		t.Errorf("probe images = %v", probe["images"])
	}
	if probe["message"] != "（图片见下）" {
		t.Errorf("probe message = %q", probe["message"])
	}
	if probe["wait_reply"] != false {
		t.Errorf("probe wait_reply = %v, want false", probe["wait_reply"])
	}
	msg, _ := main["message"].(string)
	if !strings.Contains(msg, "封面图发送失败") {
		t.Errorf("main message %q missing degradation notice", msg)
	}
	if !strings.Contains(msg, "看图确认") {
		t.Errorf("main message %q lost the original body", msg)
	}
	if main["wait_reply"] != true {
		t.Errorf("main wait_reply = %v, want true", main["wait_reply"])
	}
}

func TestHitlSendTransportErrorIsNotHitlError(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	srv.Close() // port is now refused
	node := &Hitl{BaseNode: BaseNode{ID: "t"}, Message: "确认？", BaseURL: srv.URL}
	_, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err == nil {
		t.Fatal("Execute: want transport error")
	}
	var hitlErr *HitlError
	if errors.As(err, &hitlErr) {
		t.Fatalf("transport failure must not be *HitlError, got %v", err)
	}
}

func TestHitlCtxCancelInterruptsPoll(t *testing.T) {
	stub := newHitlStub(t, "waiting")
	node := &Hitl{BaseNode: BaseNode{ID: "t"}, Message: "确认？",
		BaseURL: stub.srv.URL, TimeoutSecs: 3600, PollInterval: 0.05}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(150 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	res, err := node.Execute(ctx)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["status"] != "cancel" {
		t.Errorf("status = %v, want cancel", out["status"])
	}
	if out["session_id"] != "sess-1" {
		t.Errorf("session_id = %v, want sess-1", out["session_id"])
	}
	if out["dry_run"] != false {
		t.Errorf("dry_run = %v", out["dry_run"])
	}
	if elapsed > 5*time.Second {
		t.Errorf("cancel took %v, poll loop did not observe ctx.Done promptly", elapsed)
	}
}

func TestHitlBaseURLEnvFallbackAndFieldWins(t *testing.T) {
	envStub := newHitlStub(t, "reply")
	t.Setenv("HITL_URL", envStub.srv.URL)

	// no base_url field → env HITL_URL
	node := &Hitl{BaseNode: BaseNode{ID: "t"}, Message: "m", TimeoutSecs: 2, PollInterval: 0.01}
	if _, err := node.Execute(gateTestCtx(t, map[string]any{}, nil)); err != nil {
		t.Fatalf("Execute via env: %v", err)
	}
	if sends, _ := envStub.recorded(); sends != 1 {
		t.Errorf("env stub sends = %d, want 1", sends)
	}

	// explicit base_url field wins over env
	fieldStub := newHitlStub(t, "reply")
	node2 := &Hitl{BaseNode: BaseNode{ID: "t"}, Message: "m",
		BaseURL: fieldStub.srv.URL, TimeoutSecs: 2, PollInterval: 0.01}
	if _, err := node2.Execute(gateTestCtx(t, map[string]any{}, nil)); err != nil {
		t.Fatalf("Execute via field: %v", err)
	}
	if sends, _ := fieldStub.recorded(); sends != 1 {
		t.Errorf("field stub sends = %d, want 1", sends)
	}
	if sends, _ := envStub.recorded(); sends != 1 {
		t.Errorf("env stub got extra sends = %d, want still 1", sends)
	}
}

func TestHitlBaseURLTrailingSlashStripped(t *testing.T) {
	stub := newHitlStub(t, "reply")
	node := &Hitl{BaseNode: BaseNode{ID: "t"}, Message: "m",
		BaseURL: stub.srv.URL + "///", TimeoutSecs: 2, PollInterval: 0.01}
	if _, err := node.Execute(gateTestCtx(t, map[string]any{}, nil)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	for i, p := range stub.sendPaths {
		if p != "/api/send" {
			t.Errorf("send[%d] path = %q, want /api/send", i, p)
		}
	}
}

func TestHitlExpressionFieldsAndDefaults(t *testing.T) {
	stub := newHitlStub(t, "probe_fail")
	stub.pollBehavior = "reply"
	input := map[string]any{
		"msg":  "你好",
		"imgs": []any{"/tmp/a.png"},
		"url":  stub.srv.URL,
		"t":    3,
	}
	node := &Hitl{BaseNode: BaseNode{ID: "t"}, Message: "$INPUT.msg",
		Images: "$INPUT.imgs", BaseURL: "$INPUT.url", TimeoutSecs: "$INPUT.t",
		PollInterval: "not-a-number"} // float() fails → default 5.0
	if _, err := node.Execute(gateTestCtx(t, input, nil)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(stub.sends) != 2 {
		t.Fatalf("sends = %d, want probe + main", len(stub.sends))
	}
	probe, main := stub.sends[0], stub.sends[1]
	if !reflect.DeepEqual(probe["images"], []any{"/tmp/a.png"}) {
		t.Errorf("probe images = %v", probe["images"])
	}
	msg, _ := main["message"].(string)
	if !strings.Contains(msg, "你好") || !strings.Contains(msg, "封面图发送失败") {
		t.Errorf("main message = %q, want original body + degradation notice", msg)
	}
	if main["timeout"] != float64(3) {
		t.Errorf("timeout = %v, want 3", main["timeout"])
	}
}

func TestHitlInvalidTimeoutFallsBackToDefault(t *testing.T) {
	stub := newHitlStub(t, "reply")
	node := &Hitl{BaseNode: BaseNode{ID: "t"}, Message: "m",
		BaseURL: stub.srv.URL, TimeoutSecs: "garbage", PollInterval: 0.01}
	if _, err := node.Execute(gateTestCtx(t, map[string]any{}, nil)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	main := stub.sends[len(stub.sends)-1]
	if main["timeout"] != float64(3600) {
		t.Errorf("timeout = %v, want default 3600", main["timeout"])
	}
}

func TestHitlReplyContentTextFallback(t *testing.T) {
	stub := newHitlStub(t, "reply")
	stub.replies = []map[string]any{
		{"content": "同意"},
		{"text": "hi"},
		{"content": "", "text": "txt"}, // empty content falls through to text
		{},                             // neither → ""
	}
	node := &Hitl{BaseNode: BaseNode{ID: "t"}, Message: "m",
		BaseURL: stub.srv.URL, TimeoutSecs: 5, PollInterval: 0.01}
	res, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	replies := res.Result.(map[string]any)["replies"].([]any)
	want := []any{"同意", "hi", "txt", ""}
	if !reflect.DeepEqual(replies, want) {
		t.Errorf("replies = %v, want %v", replies, want)
	}
}

func TestHitlCamelCaseAliasFolding(t *testing.T) {
	var node Hitl
	doc := `{"id":"h","type":"hitl","message":"m","baseUrl":"http://x/",
		"timeoutSecs":7,"pollInterval":0.5,"waitReply":false,"dryRun":true}`
	if err := json.Unmarshal([]byte(doc), &node); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if node.BaseURL != "http://x/" {
		t.Errorf("BaseURL = %v", node.BaseURL)
	}
	if node.TimeoutSecs != float64(7) {
		t.Errorf("TimeoutSecs = %v", node.TimeoutSecs)
	}
	if node.PollInterval != 0.5 {
		t.Errorf("PollInterval = %v", node.PollInterval)
	}
	if node.WaitReply == nil || *node.WaitReply != false {
		t.Errorf("WaitReply = %v, want false", node.WaitReply)
	}
	if !node.DryRun {
		t.Error("DryRun = false, want true")
	}

	// snake_case wins when both spellings are present (foldKey parity)
	var both Hitl
	doc2 := `{"id":"h","type":"hitl","base_url":"snake","baseUrl":"camel","dry_run":false,"dryRun":true}`
	if err := json.Unmarshal([]byte(doc2), &both); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if both.BaseURL != "snake" {
		t.Errorf("BaseURL = %v, want snake_case winner", both.BaseURL)
	}
	if both.DryRun {
		t.Error("DryRun = true, want snake_case winner false")
	}
}

func TestHitlDefaultWaitReplyIsTrue(t *testing.T) {
	stub := newHitlStub(t, "reply")
	node := &Hitl{BaseNode: BaseNode{ID: "t"}, Message: "m",
		BaseURL: stub.srv.URL, TimeoutSecs: 5, PollInterval: 0.01}
	if _, err := node.Execute(gateTestCtx(t, map[string]any{}, nil)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	main := stub.sends[len(stub.sends)-1]
	if main["wait_reply"] != true {
		t.Errorf("wait_reply = %v, want Python default true", main["wait_reply"])
	}
}
