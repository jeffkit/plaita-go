package nodes

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// githubCommentFake is a fake GitHub issues-comments API (httptest; the token
// is a fake value injected via env — no external service is ever contacted).
type githubCommentFake struct {
	t      *testing.T
	server *httptest.Server

	mu         sync.Mutex
	gets       int
	posts      int
	getBody    string
	getStatus  int
	postStatus int
	postBody   string
	lastAuth   string
	lastPath   string
	lastPosts  []string
}

func githubCommentStartFake(t *testing.T) *githubCommentFake {
	t.Helper()
	f := &githubCommentFake{t: t, getStatus: 200, postStatus: 201,
		getBody: `[]`, postBody: `{"id": 1}`}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.lastAuth = r.Header.Get("Authorization")
		f.lastPath = r.URL.Path
		switch r.Method {
		case http.MethodGet:
			f.gets++
			w.WriteHeader(f.getStatus)
			_, _ = io.WriteString(w, f.getBody)
		case http.MethodPost:
			data, _ := io.ReadAll(r.Body)
			f.posts++
			f.lastPosts = append(f.lastPosts, string(data))
			w.WriteHeader(f.postStatus)
			_, _ = io.WriteString(w, f.postBody)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(f.server.Close)
	return f
}

// githubCommentFakeEnv injects the fake credentials + API base. The token is
// always a fake value; the fake server never leaves localhost.
func githubCommentFakeEnv(t *testing.T, f *githubCommentFake, token string) {
	t.Helper()
	t.Setenv("GH_TOKEN", token)
	t.Setenv("GITHUB_API_URL", f.server.URL)
}

// ── redaction rules, pinned one by one ────────────────────────────────────

func TestGithubCommentRedactRules(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"macOS user path", "see /Users/alice/proj/secrets/x", "see [REDACTED-PATH]"},
		{"linux home path", "check /home/bob/deploy/y", "check [REDACTED-PATH]"},
		{"api_key assignment", "api_key=abc123", "[REDACTED-SECRET]"},
		{"TOKEN colon case-insensitive", "TOKEN: xyz", "[REDACTED-SECRET]"},
		{"password spaces", "password = p@ss w0rd", "[REDACTED-SECRET] w0rd"},
		{"hyphenated api-key", "my api-key: v123", "my [REDACTED-SECRET]"},
		{"unexecuted command substitution", "run `$(rm -rf /tmp/x)` now",
			// `\$\(` and `\)` are consumed by the pattern; the backreference
			// carries the inner command text (exact Python behavior).
			"run （命令 `rm -rf /tmp/x` 未在发布时执行，以仓库实际状态为准） now"},
		{"rules apply in order", "/Users/x has token=abc", "[REDACTED-PATH] has [REDACTED-SECRET]"},
		{"no match unchanged", "hello world, all clean", "hello world, all clean"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := githubCommentRedactText(tc.in, nil); got != tc.want {
				t.Errorf("redact(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestGithubCommentRedactExtraSecrets(t *testing.T) {
	got := githubCommentRedactText("artifact at /tmp/art-123/body.md ready", []string{"/tmp/art-123"})
	if got != "artifact at [ARTIFACT-DIR]/body.md ready" {
		t.Errorf("got %q", got)
	}
	// Empty extras are skipped (Python `if secret:`), literal replace only.
	if got := githubCommentRedactText("a b", []string{"", "b"}); got != "a [ARTIFACT-DIR]" {
		t.Errorf("got %q", got)
	}
}

// ── post path ─────────────────────────────────────────────────────────────

func githubCommentTestNode(t *testing.T, f *githubCommentFake, mutate func(*GithubComment)) *GithubComment {
	t.Helper()
	node := &GithubComment{
		BaseNode:    BaseNode{ID: "gh1"},
		Repo:        "org/repo",
		IssueNumber: 12,
		Text:        "build ok",
	}
	if mutate != nil {
		mutate(node)
	}
	_ = f
	return node
}

func TestGithubCommentPost(t *testing.T) {
	f := githubCommentStartFake(t)
	githubCommentFakeEnv(t, f, "test-token")
	node := githubCommentTestNode(t, f, nil)

	res, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["posted"] != true || out["note"] != "" {
		t.Errorf("output = %v", out)
	}
	if _, hasDry := out["dry_run"]; hasDry {
		t.Errorf("non-dry output must not carry dry_run key: %v", out)
	}
	if f.lastAuth != "Bearer test-token" {
		t.Errorf("Authorization = %q", f.lastAuth)
	}
	if f.lastPath != "/repos/org/repo/issues/12/comments" {
		t.Errorf("path = %q", f.lastPath)
	}
	if f.posts != 1 {
		t.Fatalf("posts = %d", f.posts)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(f.lastPosts[0]), &payload); err != nil {
		t.Fatalf("post body: %v", err)
	}
	if payload["body"] != "build ok" {
		t.Errorf("body = %v", payload["body"])
	}
}

func TestGithubCommentRedactionAppliedBeforePost(t *testing.T) {
	f := githubCommentStartFake(t)
	githubCommentFakeEnv(t, f, "test-token")
	artDir := t.TempDir()
	node := githubCommentTestNode(t, f, func(n *GithubComment) {
		n.Text = "log dir /Users/me/runs/1 has token=xyz; artifact at " + artDir
		n.ArtifactDir = artDir
	})
	res, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Result.(map[string]any)["posted"] != true {
		t.Fatalf("posted: %v", res.Result)
	}
	var payload map[string]any
	_ = json.Unmarshal([]byte(f.lastPosts[0]), &payload)
	body := payload["body"].(string)
	if strings.Contains(body, "/Users/me") || strings.Contains(body, "token=xyz") {
		t.Errorf("unredacted content posted: %q", body)
	}
	if !strings.Contains(body, "[REDACTED-PATH]") || !strings.Contains(body, "[REDACTED-SECRET]") {
		t.Errorf("missing masks: %q", body)
	}
	if !strings.Contains(body, "[ARTIFACT-DIR]") {
		t.Errorf("artifact dir not replaced: %q", body)
	}
	// The durable draft in the artifact dir carries the same redacted body.
	draft, err := os.ReadFile(filepath.Join(artDir, "reply.md"))
	if err != nil {
		t.Fatalf("draft: %v", err)
	}
	if string(draft) != body {
		t.Errorf("draft %q != posted body %q", draft, body)
	}
}

func TestGithubCommentFooterAfterRedaction(t *testing.T) {
	f := githubCommentStartFake(t)
	githubCommentFakeEnv(t, f, "test-token")
	node := githubCommentTestNode(t, f, func(n *GithubComment) {
		n.Text = "token=leaked-value in body"
		n.Footer = "branch feature token=footer-secret"
	})
	res, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Result.(map[string]any)["posted"] != true {
		t.Fatalf("posted: %v", res.Result)
	}
	var payload map[string]any
	_ = json.Unmarshal([]byte(f.lastPosts[0]), &payload)
	body := payload["body"].(string)
	if !strings.HasSuffix(body, "\n\n---\n*branch feature token=footer-secret*") {
		t.Errorf("footer not appended verbatim: %q", body)
	}
	if !strings.Contains(strings.SplitN(body, "\n\n", 2)[0], "[REDACTED-SECRET]") {
		t.Errorf("body not redacted: %q", body)
	}
	if !strings.Contains(body, "token=footer-secret") {
		t.Errorf("footer (trusted flow content) must not be redacted: %q", body)
	}
}

func TestGithubCommentPostFailureIsData(t *testing.T) {
	f := githubCommentStartFake(t)
	githubCommentFakeEnv(t, f, "test-token")
	f.postStatus = 400
	f.postBody = `{"message":"Validation Failed"}` + strings.Repeat("x", 250)
	node := githubCommentTestNode(t, f, nil)

	res, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("post failure must be data, not a node error: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["posted"] != false {
		t.Errorf("posted = %v", out["posted"])
	}
	note := out["note"].(string)
	// note = the LAST 200 runes of the response — the JSON prefix fell off.
	if len([]rune(note)) != 200 || strings.Contains(note, "Validation") {
		t.Errorf("note = %q (%d runes), want last 200 runes of the body", note, len([]rune(note)))
	}
}

// ── dedup path ────────────────────────────────────────────────────────────

func TestGithubCommentDedupSkipsPost(t *testing.T) {
	f := githubCommentStartFake(t)
	githubCommentFakeEnv(t, f, "test-token")
	// REST shape: top-level comment array.
	f.getBody = `[{"body":"earlier note"},{"body":"done <!-- issue-pipeline -->"}]`
	node := githubCommentTestNode(t, f, func(n *GithubComment) {
		n.DedupMarker = "<!-- issue-pipeline -->"
	})
	res, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["posted"] != false {
		t.Errorf("posted = %v", out["posted"])
	}
	if out["note"] != "已有含去重标记的评论（断点续跑），跳过" {
		t.Errorf("note = %q", out["note"])
	}
	if f.posts != 0 {
		t.Errorf("dedup hit must not post (%d posts)", f.posts)
	}
	if f.gets != 1 {
		t.Errorf("dedup query count = %d, want 1", f.gets)
	}
}

func TestGithubCommentDedupNoHitPosts(t *testing.T) {
	f := githubCommentStartFake(t)
	githubCommentFakeEnv(t, f, "test-token")
	// gh CLI envelope shape ({"comments": [...]}) is accepted too.
	f.getBody = `{"comments":[{"body":"unrelated"}]}`
	node := githubCommentTestNode(t, f, func(n *GithubComment) {
		n.DedupMarker = "<!-- issue-pipeline -->"
	})
	res, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Result.(map[string]any)["posted"] != true {
		t.Errorf("no dedup hit must post: %v", res.Result)
	}
	if f.posts != 1 {
		t.Errorf("posts = %d", f.posts)
	}
}

// TestGithubCommentDedupMarkerIsDataOnly pins pn#13: the marker is compared
// as a data substring, never interpolated into a query — quotes/backslashes
// in the marker cannot change the lookup semantics.
func TestGithubCommentDedupMarkerIsDataOnly(t *testing.T) {
	f := githubCommentStartFake(t)
	githubCommentFakeEnv(t, f, "test-token")
	marker := `weird "marker" \x'; DROP comments; --`
	// Serve a VALID JSON body containing the hostile marker verbatim.
	raw, err := json.Marshal([]map[string]string{{"body": "prefix `" + marker + "` suffix"}})
	if err != nil {
		t.Fatal(err)
	}
	f.getBody = string(raw)
	node := githubCommentTestNode(t, f, func(n *GithubComment) {
		n.DedupMarker = marker
	})
	res, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Result.(map[string]any)["posted"] != false {
		t.Errorf("marker substring match failed: %v / raw get body %q", res.Result, f.getBody)
	}
}

func TestGithubCommentDedupFailuresAreNamedErrors(t *testing.T) {
	cases := []struct {
		name      string
		status    int
		getBody   string
		wantErr   string
		wantRetry bool
	}{
		{"http failure", 502, "upstream error", "去重查询失败（HTTP 502）", true},
		{"bad json", 200, "not json at all", "去重查询输出不是 JSON", true},
		{"missing comments array", 200, `{"nope": 1}`, "去重查询结果缺少 comments 数组", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := githubCommentStartFake(t)
			githubCommentFakeEnv(t, f, "test-token")
			f.getStatus = tc.status
			f.getBody = tc.getBody
			node := githubCommentTestNode(t, f, func(n *GithubComment) {
				n.DedupMarker = "<!-- x -->"
			})
			_, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want containing %q", err, tc.wantErr)
			}
			if isRetryable := apiRequestIsRetryable(err); isRetryable != tc.wantRetry {
				t.Errorf("retryable = %v", isRetryable)
			}
			if f.posts != 0 {
				t.Errorf("failed dedup must never post (%d posts)", f.posts)
			}
		})
	}
}

// ── dry-run path ──────────────────────────────────────────────────────────

func TestGithubCommentDryRun(t *testing.T) {
	f := githubCommentStartFake(t)
	githubCommentFakeEnv(t, f, "test-token")
	artDir := t.TempDir()
	node := githubCommentTestNode(t, f, func(n *GithubComment) {
		n.Text = "token=secretvalue and /Users/me/x"
		n.ArtifactDir = artDir
		n.Footer = "trusted footer"
		n.DryRun = true
	})
	res, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["posted"] != true || out["dry_run"] != true {
		t.Errorf("dry-run output = %v", out)
	}
	if out["note"] != "dry-run：写草稿但跳过 GitHub 调用" {
		t.Errorf("note = %q", out["note"])
	}
	if f.gets != 0 || f.posts != 0 {
		t.Errorf("dry-run must not contact GitHub (gets=%d posts=%d)", f.gets, f.posts)
	}
	// The draft is still written and redacted (writefile convention)…
	draft, err := os.ReadFile(filepath.Join(artDir, "reply.md"))
	if err != nil {
		t.Fatalf("draft: %v", err)
	}
	if strings.Contains(string(draft), "token=secretvalue") || strings.Contains(string(draft), "/Users/me") {
		t.Errorf("draft not redacted: %q", draft)
	}
	// …but the footer is NOT appended on the dry-run path (Python parity).
	if strings.Contains(string(draft), "trusted footer") {
		t.Errorf("dry-run draft must not carry the footer: %q", draft)
	}
}

func TestGithubCommentDryRunFromGlobal(t *testing.T) {
	f := githubCommentStartFake(t)
	githubCommentFakeEnv(t, f, "test-token")
	node := githubCommentTestNode(t, f, nil)
	res, err := node.Execute(gateTestCtx(t, map[string]any{}, map[string]any{"dry_run": true}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if out := res.Result.(map[string]any); out["dry_run"] != true {
		t.Errorf("global dry_run not honored: %v", out)
	}
	if f.posts != 0 {
		t.Errorf("global dry-run must not post")
	}
}

// ── credentials fail-closed ───────────────────────────────────────────────

func TestGithubCommentMissingTokenFailsClosed(t *testing.T) {
	f := githubCommentStartFake(t)
	t.Setenv("GH_TOKEN", "")
	t.Setenv("GITHUB_TOKEN", "")
	t.Setenv("GITHUB_API_URL", f.server.URL)
	node := githubCommentTestNode(t, f, nil)
	_, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err == nil || !strings.Contains(err.Error(), "GH_TOKEN") {
		t.Fatalf("err = %v, want fail-closed credential error", err)
	}
	if f.posts != 0 {
		t.Errorf("no token must mean no post (%d)", f.posts)
	}

	// Missing token also blocks the dedup query (宁可报错，不静默发布).
	node2 := githubCommentTestNode(t, f, func(n *GithubComment) { n.DedupMarker = "m" })
	if _, err := node2.Execute(gateTestCtx(t, map[string]any{}, nil)); err == nil ||
		!strings.Contains(err.Error(), "去重查询需要凭据") {
		t.Fatalf("err = %v, want named dedup credential error", err)
	}
}

func TestGithubCommentTokenPrecedence(t *testing.T) {
	f := githubCommentStartFake(t)
	githubCommentFakeEnv(t, f, "gh-token-wins")
	t.Setenv("GITHUB_TOKEN", "fallback-token")
	node := githubCommentTestNode(t, f, nil)
	if _, err := node.Execute(gateTestCtx(t, map[string]any{}, nil)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if f.lastAuth != "Bearer gh-token-wins" {
		t.Errorf("Authorization = %q, want GH_TOKEN to win", f.lastAuth)
	}
}

// ── fields / validation / timeout ─────────────────────────────────────────

func TestGithubCommentMissingFields(t *testing.T) {
	f := githubCommentStartFake(t)
	githubCommentFakeEnv(t, f, "test-token")
	for _, node := range []*GithubComment{
		{BaseNode: BaseNode{ID: "g"}},
		{BaseNode: BaseNode{ID: "g"}, Repo: "org/repo"},
		{BaseNode: BaseNode{ID: "g"}, Repo: "org/repo", IssueNumber: 1},
	} {
		if _, err := node.Execute(gateTestCtx(t, map[string]any{}, nil)); err == nil ||
			!strings.Contains(err.Error(), "缺少 repo/issue_number/text") {
			t.Fatalf("node %+v: err = %v, want missing-field error", node, err)
		}
	}
}

func TestGithubCommentIssueNumberAsString(t *testing.T) {
	f := githubCommentStartFake(t)
	githubCommentFakeEnv(t, f, "test-token")
	node := githubCommentTestNode(t, f, func(n *GithubComment) { n.IssueNumber = "34" })
	if _, err := node.Execute(gateTestCtx(t, map[string]any{}, nil)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if f.lastPath != "/repos/org/repo/issues/34/comments" {
		t.Errorf("path = %q", f.lastPath)
	}
}

func TestGithubCommentExpressionFields(t *testing.T) {
	f := githubCommentStartFake(t)
	githubCommentFakeEnv(t, f, "test-token")
	node := &GithubComment{
		BaseNode:    BaseNode{ID: "gh1"},
		Repo:        "$INPUT.repo",
		IssueNumber: "$INPUT.n",
		Text:        "status: {% $INPUT.state %}",
	}
	res, err := node.Execute(gateTestCtx(t, map[string]any{"repo": "acme/widgets", "n": 7, "state": "green"}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Result.(map[string]any)["posted"] != true {
		t.Fatalf("posted: %v", res.Result)
	}
	if f.lastPath != "/repos/acme/widgets/issues/7/comments" {
		t.Errorf("path = %q", f.lastPath)
	}
	var payload map[string]any
	_ = json.Unmarshal([]byte(f.lastPosts[0]), &payload)
	if payload["body"] != "status: green" {
		t.Errorf("body = %v", payload["body"])
	}
}

func TestGithubCommentTimeout(t *testing.T) {
	block := make(chan struct{})
	f := githubCommentStartFake(t)
	f.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-block // hang until the test finishes
	})
	t.Cleanup(func() { close(block) })
	githubCommentFakeEnv(t, f, "test-token")
	node := githubCommentTestNode(t, f, func(n *GithubComment) { n.TimeoutSecs = 1 })
	if _, err := node.Execute(gateTestCtx(t, map[string]any{}, nil)); err == nil ||
		!strings.Contains(err.Error(), "GitHub 请求失败") {
		t.Fatalf("err = %v, want transport timeout error", err)
	}
}

func TestGithubCommentJSONRoundTripAndRegistration(t *testing.T) {
	doc := `{"type":"github_comment","id":"gh1","repo":"org/repo","issue_number":9,
		"text":"hi","redact":false,"artifact_dir":"/tmp/a","dedup_marker":"<!-- x -->",
		"footer":"done","dry_run":true,"timeout_secs":30}`
	node := new(GithubComment)
	if err := json.Unmarshal([]byte(doc), node); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if node.ID != "gh1" || node.Repo != "org/repo" || node.ArtifactDir != "/tmp/a" ||
		node.DedupMarker != "<!-- x -->" || node.Footer != "done" || !node.DryRun {
		t.Errorf("fields = %+v", node)
	}
	if node.Redact == nil || *node.Redact != false {
		t.Errorf("redact = %v, want explicit false", node.Redact)
	}
	if n, ok := node.IssueNumber.(float64); !ok || n != 9 {
		t.Errorf("issue_number = %#v", node.IssueNumber)
	}
	// Default redact is TRUE (pydantic default parity).
	absent := new(GithubComment)
	if absent.Redact != nil {
		t.Errorf("absent redact = %v, want nil (defaults to true at Execute)", absent.Redact)
	}
	if g := (&GithubComment{BaseNode: BaseNode{ID: "g"}}); g.finalize("token=x", "") != "[REDACTED-SECRET]" {
		t.Error("redact must default to enabled")
	}
	if workflow.GetNodeFactory(nodeTypeGithubComment) == nil {
		t.Error("github_comment node type not registered")
	}
}
