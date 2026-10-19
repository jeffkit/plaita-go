package nodes

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// nodeTypeGithubComment mirrors plaita-nodes GithubCommentNode.node_type
// ("github_comment").
const nodeTypeGithubComment types.NodeType = "github_comment"

func init() {
	workflow.RegisterNodeType(
		nodeTypeGithubComment,
		func(ctx context.Context) workflow.Node { return new(GithubComment) },
	)
}

// githubCommentDefaultTimeoutSecs mirrors GithubCommentNode.timeout_secs; it
// bounds each GitHub API call.
const githubCommentDefaultTimeoutSecs = 60

// Credential contract (deliberate Go-side divergence, see below):
//   - token: env GH_TOKEN, falling back to GITHUB_TOKEN — the same variables
//     the gh CLI reads implicitly. Missing/empty at call time is a fail-closed
//     named error: the node would rather fail than post unauthenticated;
//   - API base: env GITHUB_API_URL (default https://api.github.com), the same
//     override switch GitHub Actions and gh enterprise setups use.
//
// The Python node shells out to the gh CLI (which resolves the token itself
// and whose subprocess env intentionally strips GH_TOKEN — pn#17). This
// engine talks to the GitHub REST API over net/http instead (stdlib, no gh
// binary dependency, and testable against a fake API server); the token is
// therefore supplied explicitly as a Bearer header and never logged.
type GithubComment struct {
	BaseNode
	Repo        any    `json:"repo,omitempty"`         // owner/name (expression)
	IssueNumber any    `json:"issue_number,omitempty"` // issue/PR number (expression)
	Text        any    `json:"text,omitempty"`         // comment body (expression)
	Redact      *bool  `json:"redact,omitempty"`       // default true (pydantic default parity)
	ArtifactDir any    `json:"artifact_dir,omitempty"` // expression; body draft dir
	DedupMarker string `json:"dedup_marker,omitempty"` // plain string, not an expression
	Footer      any    `json:"footer,omitempty"`       // expression
	DryRun      bool   `json:"dry_run,omitempty"`
	TimeoutSecs any    `json:"timeout_secs,omitempty"`
}

// GetType returns the github_comment node type.
func (g *GithubComment) GetType() workflow.NodeType { return nodeTypeGithubComment }

// Execute posts one GitHub issue/PR comment, converging the three outbound
// controls (github_comment.py docstring):
//
//   - redact: local paths / secret assignments / unexecuted `$(...)` command
//     substitutions / artifact dir names are masked before anything leaves
//     the machine — the comment body is untrusted input;
//   - dedup_marker: when set, existing comments are searched for the marker
//     first; a hit skips posting (resume runs never double-post);
//   - footer: an optional "\n\n---\n*<footer>*" tail appended AFTER
//     redaction (footer is flow-controlled trusted content).
//
// dry_run (or globalContext.dry_run) still writes the artifact draft (the
// writefile convention, for human inspection) and redacts the body, but skips
// both the dedup query and the real post — mirroring the Python node, the
// footer is NOT appended on the dry-run path.
//
// Output: {"posted", "note"} (+ "dry_run": true on the dry-run path).
func (g *GithubComment) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	if g.Repo == nil || g.IssueNumber == nil || g.Text == nil {
		return nil, fmt.Errorf("github_comment %q: 节点缺少 repo/issue_number/text 字段", g.ID)
	}
	repoV, err := expression.Eval(ctx, g.Repo)
	if err != nil {
		return nil, fmt.Errorf("github_comment %q: eval repo: %w", g.ID, err)
	}
	repo := gateToStr(repoV)
	numV, err := expression.Eval(ctx, g.IssueNumber)
	if err != nil {
		return nil, fmt.Errorf("github_comment %q: eval issue_number: %w", g.ID, err)
	}
	number, err := toInt(numV)
	if err != nil {
		return nil, fmt.Errorf("github_comment %q: issue_number 不是整数: %v", g.ID, numV)
	}
	bodyV, err := expression.Eval(ctx, g.Text)
	if err != nil {
		return nil, fmt.Errorf("github_comment %q: eval text: %w", g.ID, err)
	}
	body := gitPublishOrEmpty(bodyV) // Python `str(evaluate(text) or "")`

	artifactDir := ""
	if g.ArtifactDir != nil {
		v, evalErr := expression.Eval(ctx, g.ArtifactDir)
		if evalErr != nil {
			return nil, fmt.Errorf("github_comment %q: eval artifact_dir: %w", g.ID, evalErr)
		}
		artifactDir = gitPublishOrEmpty(v) // Python `str(evaluate(...) or "") or None`
	}

	timeout := githubCommentDefaultTimeoutSecs
	if g.TimeoutSecs != nil {
		if n, toErr := toInt(g.TimeoutSecs); toErr == nil {
			timeout = n
		} else {
			return nil, fmt.Errorf("github_comment %q: timeout_secs 必须是数字: %v", g.ID, g.TimeoutSecs)
		}
	}

	dry := g.DryRun
	if rc := runtime.WorkflowRuntimeContext(ctx); rc != nil {
		if gv := rc.GetGlobalVariable("dry_run"); isTruthy(gv) {
			dry = true
		}
	}
	if dry {
		body = g.finalize(body, artifactDir)
		if _, err := githubCommentWriteArtifact(body, artifactDir); err != nil {
			return nil, fmt.Errorf("github_comment %q: %w", g.ID, err)
		}
		return &workflow.ExecuteResult{Result: map[string]any{
			"posted":  true,
			"dry_run": true,
			"note":    "dry-run：写草稿但跳过 GitHub 调用",
		}, NextNode: g.NextNode()}, nil
	}

	if g.DedupMarker != "" {
		commented, err := g.alreadyCommented(ctx, repo, number, time.Duration(timeout)*time.Second)
		if err != nil {
			return nil, err
		}
		if commented {
			return &workflow.ExecuteResult{Result: map[string]any{
				"posted": false,
				"note":   "已有含去重标记的评论（断点续跑），跳过",
			}, NextNode: g.NextNode()}, nil
		}
	}

	body = g.finalize(body, artifactDir)
	if g.Footer != nil {
		v, evalErr := expression.Eval(ctx, g.Footer)
		if evalErr != nil {
			return nil, fmt.Errorf("github_comment %q: eval footer: %w", g.ID, evalErr)
		}
		if tail := gitPublishOrEmpty(v); tail != "" {
			body = body + fmt.Sprintf("\n\n---\n*%s*", tail)
		}
	}
	// The artifact write happens before the network call (parity: a failing
	// draft write fails the node before anything is posted). The path itself
	// is not needed by the HTTP client; it exists for the audit trail.
	if _, err := githubCommentWriteArtifact(body, artifactDir); err != nil {
		return nil, fmt.Errorf("github_comment %q: %w", g.ID, err)
	}

	token, err := githubCommentToken()
	if err != nil {
		return nil, fmt.Errorf("github_comment %q: %w", g.ID, err)
	}
	posted, note, err := githubCommentPost(ctx, repo, number, body, token, time.Duration(timeout)*time.Second)
	if err != nil {
		return nil, fmt.Errorf("github_comment %q: %w", g.ID, err)
	}
	if !posted {
		return &workflow.ExecuteResult{Result: map[string]any{
			"posted": false,
			"note":   note,
		}, NextNode: g.NextNode()}, nil
	}
	return &workflow.ExecuteResult{Result: map[string]any{
		"posted": true,
		"note":   "",
	}, NextNode: g.NextNode()}, nil
}

// finalize redacts the body when the (default-on) redact switch is enabled,
// passing the artifact dir as the extra literal secret.
func (g *GithubComment) finalize(body, artifactDir string) string {
	if g.Redact != nil && !*g.Redact {
		return body
	}
	var extra []string
	if artifactDir != "" {
		extra = []string{artifactDir}
	}
	return githubCommentRedactText(body, extra)
}

// ── redaction (issue-pipeline production rule list; order matters) ────────

var githubCommentRedactPatterns = []struct {
	re   *regexp.Regexp
	repl string
}{
	{regexp.MustCompile(`/Users/\S+`), "[REDACTED-PATH]"},
	{regexp.MustCompile(`/home/\S+`), "[REDACTED-PATH]"},
	{regexp.MustCompile(`(?i)(api[_-]?key|token|secret|password)\s*[=:]\s*\S+`), "[REDACTED-SECRET]"},
	// Backreference keeps the command text: the reader learns WHAT was not run.
	{regexp.MustCompile("`\\$\\(([^`]+)\\)`"), "（命令 `$1` 未在发布时执行，以仓库实际状态为准）"},
}

// githubCommentRedactText applies the fixed rule list in order, then replaces
// each extra secret literally (e.g. the artifact dir path).
func githubCommentRedactText(text string, extraSecrets []string) string {
	for _, p := range githubCommentRedactPatterns {
		text = p.re.ReplaceAllString(text, p.repl)
	}
	for _, secret := range extraSecrets {
		if secret != "" {
			text = strings.ReplaceAll(text, secret, "[ARTIFACT-DIR]")
		}
	}
	return text
}

// ── GitHub REST client ────────────────────────────────────────────────────

// githubCommentToken resolves the bearer token from the environment
// (GH_TOKEN → GITHUB_TOKEN); empty/missing is a fail-closed error.
func githubCommentToken() (string, error) {
	for _, key := range []string{"GH_TOKEN", "GITHUB_TOKEN"} {
		if v := os.Getenv(key); v != "" {
			return v, nil
		}
	}
	return "", fmt.Errorf("缺少 GitHub 凭据（设置 GH_TOKEN 或 GITHUB_TOKEN）；宁可报错，不静默发布")
}

// githubCommentAPIBase returns the API root (env GITHUB_API_URL override).
func githubCommentAPIBase() string {
	if v := os.Getenv("GITHUB_API_URL"); v != "" {
		return strings.TrimRight(v, "/")
	}
	return "https://api.github.com"
}

// githubCommentIssuePath builds /repos/{owner}/{repo}/issues/{n}/comments
// with each repo path segment escaped (repo is "owner/name").
func githubCommentIssuePath(repo string, number int) string {
	segments := strings.Split(repo, "/")
	for i, seg := range segments {
		segments[i] = url.PathEscape(seg)
	}
	return fmt.Sprintf("/repos/%s/issues/%d/comments", strings.Join(segments, "/"), number)
}

// githubCommentDo runs one authenticated API request with the per-call
// timeout; returns the status code and the (possibly truncated) body.
func githubCommentDo(ctx context.Context, method, path, token string, payload []byte, timeout time.Duration) (int, []byte, error) {
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var reader io.Reader
	if payload != nil {
		reader = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(callCtx, method, githubCommentAPIBase()+path, reader)
	if err != nil {
		return 0, nil, workflow.RetryableError(fmt.Errorf("构造请求失败: %w", err))
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("User-Agent", "plaita-go") // GitHub API rejects agentless clients
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, workflow.RetryableError(fmt.Errorf("GitHub 请求失败: %w", err))
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, workflow.RetryableError(fmt.Errorf("读取响应失败: %w", err))
	}
	return resp.StatusCode, data, nil
}

// alreadyCommented mirrors _already_commented: fetch the issue's comments
// and check whether any body contains the marker. The marker is only ever
// compared as data (strings.Contains), never interpolated into a query.
// Every failure mode is a named retryable error — treating a failed lookup
// as "no duplicate" would double-post whenever GitHub is degraded.
func (g *GithubComment) alreadyCommented(ctx context.Context, repo string, number int, timeout time.Duration) (bool, error) {
	token, err := githubCommentToken()
	if err != nil {
		return false, fmt.Errorf("github_comment %q: 去重查询需要凭据（%v）；请确认 GH_TOKEN/GITHUB_TOKEN 已注入", g.ID, err)
	}
	status, data, err := githubCommentDo(ctx, http.MethodGet, githubCommentIssuePath(repo, number), token, nil, timeout)
	if err != nil {
		return false, fmt.Errorf("github_comment %q: 去重查询失败: %w", g.ID, err)
	}
	if status < 200 || status >= 300 {
		return false, workflow.RetryableError(fmt.Errorf(
			"github_comment %q：去重查询失败（HTTP %d）：%s", g.ID, status, gitPublishTail(string(data), 200)))
	}
	comments, err := githubCommentExtractComments(data)
	if err != nil {
		return false, fmt.Errorf("github_comment %q: %w", g.ID, err)
	}
	for _, c := range comments {
		if m, ok := c.(map[string]any); ok && strings.Contains(gitPublishOrEmpty(m["body"]), g.DedupMarker) {
			return true, nil
		}
	}
	return false, nil
}

// githubCommentExtractComments accepts both comment-array shapes: the REST
// API's top-level array and the gh CLI's {"comments": [...]} envelope (the
// shape the Python node parses). Anything else is the named shape error.
func githubCommentExtractComments(data []byte) ([]any, error) {
	var payload any
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, workflow.RetryableError(fmt.Errorf(
			"github_comment：去重查询输出不是 JSON：%s", gitPublishHead(string(data), 200)))
	}
	switch p := payload.(type) {
	case []any:
		return p, nil
	case map[string]any:
		if comments, ok := p["comments"].([]any); ok {
			return comments, nil
		}
	}
	return nil, workflow.RetryableError(fmt.Errorf(
		"github_comment：去重查询结果缺少 comments 数组：%s", gitPublishHead(string(data), 200)))
}

// githubCommentPost mirrors the `gh issue comment` call: a non-2xx response
// is DATA (posted=false with the response tail as note), not a node error —
// the flow decides what a failed post means. Transport-level errors are
// retryable node errors.
func githubCommentPost(ctx context.Context, repo string, number int, body, token string, timeout time.Duration) (bool, string, error) {
	payload, err := json.Marshal(map[string]string{"body": body})
	if err != nil {
		return false, "", fmt.Errorf("序列化评论正文失败: %w", err)
	}
	status, data, err := githubCommentDo(ctx, http.MethodPost, githubCommentIssuePath(repo, number), token, payload, timeout)
	if err != nil {
		return false, "", err
	}
	if status < 200 || status >= 300 {
		return false, gitPublishTail(string(data), 200), nil
	}
	return true, "", nil
}

// githubCommentWriteArtifact mirrors _write_artifact: the artifact dir (when
// given) gets reply.md as the durable draft; otherwise a temp .md file is
// written (the Python node needed it for gh --body-file; here it keeps the
// side-effect contract identical for auditors).
func githubCommentWriteArtifact(body, artifactDir string) (string, error) {
	if artifactDir != "" {
		path := filepath.Join(artifactDir, "reply.md")
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return "", fmt.Errorf("创建 artifact 目录失败: %w", err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			return "", fmt.Errorf("写 artifact 草稿失败: %w", err)
		}
		return path, nil
	}
	f, err := os.CreateTemp("", "plaita-comment-*.md")
	if err != nil {
		return "", fmt.Errorf("创建临时文件失败: %w", err)
	}
	if _, err := f.WriteString(body); err != nil {
		f.Close()
		return "", fmt.Errorf("写临时文件失败: %w", err)
	}
	return f.Name(), f.Close()
}
