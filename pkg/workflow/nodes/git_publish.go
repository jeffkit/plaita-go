package nodes

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// nodeTypeGitPublish mirrors plaita-nodes GitPublishNode.node_type ("git_publish").
const nodeTypeGitPublish types.NodeType = "git_publish"

func init() {
	workflow.RegisterNodeType(
		nodeTypeGitPublish,
		func(ctx context.Context) workflow.Node { return new(GitPublish) },
	)
}

// gitPublishDefaultTimeoutSecs mirrors GitPublishNode.timeout_secs; it is the
// per-git-command timeout (subprocess.run(timeout=…) parity — an expiry is a
// node error, unlike capture's exit_code=124 convention).
const gitPublishDefaultTimeoutSecs = 300

// gitPublishLsRemoteTimeoutSecs / gitPublishGHTimeoutSecs mirror the explicit
// t= overrides in git_publish.py (ls-remote, gh pr create).
const (
	gitPublishLsRemoteTimeoutSecs = 60
	gitPublishGHTimeoutSecs       = 120
)

// GitPublish commits + pushes idempotently, optionally ff-merging the branch
// into the base branch. Mirrors plaita-nodes GitPublishNode (git_publish.py)
// and its README #6 semantics fix:
//
//  1. uncommitted changes (status --porcelain non-empty) are ALWAYS committed
//     first (message priority: commit_message > COMMIT_MESSAGE: line in
//     plan_file > "fix: issue #<issue_number>");
//  2. after commit, remote branch head == local HEAD → skip push (true
//     duplicate delivery, changes preserved — the old deliver node silently
//     dropped freshly produced changes here);
//  3. no remote branch → push -u origin <branch>; branch exists and local is
//     ahead → plain push.
//
// merge_mode: "branch" (default, push branch only) | "main" (ff-merge
// origin/<branch> into the base branch in main_clone and push it — the BRANCH
// is merged, not origin/<base>) | "none" (local commit only, no push) | "pr"
// (push branch, then gh pr create --base <base>; an existing PR is a note,
// not a failure).
//
// Output: {"pushed", "merged", "note", "push_note"[, "pr_url", "dry_run"]}.
//
// Security boundary (git_publish.py _git_env port, pn#17 + pn#21): every git
// invocation runs with a whitelist-rebuilt env (host credentials out) and a
// hardened argv — core.hooksPath=/dev/null, core.fsmonitor=false,
// protocol.ext.allow=never, and every configured filter.*.{clean,smudge,
// process} overridden to empty. Repo-reconfigurable executable surface gets
// zero execution opportunities.
type GitPublish struct {
	BaseNode
	WorktreeDir   any  `json:"worktree_dir,omitempty"`
	BranchName    any  `json:"branch_name,omitempty"`
	CommitMessage any  `json:"commit_message,omitempty"`
	PlanFile      any  `json:"plan_file,omitempty"`
	IssueNumber   any  `json:"issue_number,omitempty"`
	MergeMode     any  `json:"merge_mode,omitempty"` // "branch"|"main"|"none"|"pr"; DSL may pass an expression
	MainClone     any  `json:"main_clone,omitempty"`
	BaseBranch    any  `json:"base_branch,omitempty"` // default "main"
	DryRun        bool `json:"dry_run,omitempty"`
	TimeoutSecs   any  `json:"timeout_secs,omitempty"` // per-command seconds, default 300
}

// GetType returns the git_publish node type.
func (g *GitPublish) GetType() workflow.NodeType { return nodeTypeGitPublish }

// Execute evaluates the expression fields and performs the idempotent
// commit/push sequence.
func (g *GitPublish) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	if g.WorktreeDir == nil || g.BranchName == nil {
		return nil, fmt.Errorf("git_publish %q: 节点缺少 worktree_dir/branch_name 字段", g.ID)
	}
	wt, err := g.evalString(ctx, g.WorktreeDir, "worktree_dir")
	if err != nil {
		return nil, err
	}
	branch, err := g.evalString(ctx, g.BranchName, "branch_name")
	if err != nil {
		return nil, err
	}
	// merge_mode may arrive as a DSL expression string (e.g. "$INPUT.push_mode").
	mergeMode := "branch"
	if g.MergeMode != nil {
		v, evalErr := expression.Eval(ctx, g.MergeMode)
		if evalErr != nil {
			return nil, fmt.Errorf("git_publish %q: eval merge_mode: %w", g.ID, evalErr)
		}
		if isTruthy(v) {
			mergeMode = gateToStr(v)
		}
	}
	if !slices.Contains([]string{"branch", "main", "none", "pr"}, mergeMode) {
		return nil, fmt.Errorf("git_publish %q: merge_mode 非法: %q", g.ID, mergeMode)
	}
	baseBranch := "main"
	if g.BaseBranch != nil {
		v, evalErr := expression.Eval(ctx, g.BaseBranch)
		if evalErr != nil {
			return nil, fmt.Errorf("git_publish %q: eval base_branch: %w", g.ID, evalErr)
		}
		if isTruthy(v) {
			baseBranch = gateToStr(v)
		}
	}
	timeout, err := g.evalTimeout()
	if err != nil {
		return nil, err
	}

	dry := g.DryRun
	if rc := runtime.WorkflowRuntimeContext(ctx); rc != nil {
		if gv := rc.GetGlobalVariable("dry_run"); isTruthy(gv) {
			dry = true
		}
	}
	if dry {
		return &workflow.ExecuteResult{Result: map[string]any{
			"pushed":    true,
			"merged":    nil,
			"push_note": "",
			"note":      "dry-run：跳过 git 操作",
			"dry_run":   true,
		}, NextNode: g.NextNode()}, nil
	}

	// Tenant gate (#36), after the dry-run short-circuit (which performs no
	// git operation) and before the first git invocation: worktree_dir /
	// branch are flow-controlled host write/push faces.
	if err := refuseNonDefaultTenantWriteFace(ctx, "git_publish",
		"the node would commit, push and merge git branches"); err != nil {
		return nil, err
	}

	if mergeMode == "main" && g.MainClone == nil {
		return nil, fmt.Errorf("git_publish %q: merge_mode=main 需要 main_clone 字段", g.ID)
	}

	env, err := gitPublishBuildEnv()
	if err != nil {
		return nil, fmt.Errorf("git_publish %q: %w", g.ID, err)
	}
	run := &gitPublishGit{
		ctx: ctx, env: env, cwd: wt, baseTimeout: timeout,
		argv: gitPublishHardenedArgv(ctx, wt, env, timeout),
	}

	dirtyOut, _, _, err := run.sh([]string{"status", "--porcelain"}, "", 0)
	if err != nil {
		return nil, fmt.Errorf("git_publish %q: %w", g.ID, err)
	}
	dirty := strings.TrimSpace(dirtyOut) != ""

	committedNow := false
	var msg string
	if dirty || mergeMode == "pr" {
		msg, err = g.resolveMessage(ctx)
		if err != nil {
			return nil, err
		}
	}
	if dirty {
		if _, _, _, err := run.sh([]string{"add", "-A"}, "", 0); err != nil {
			return nil, fmt.Errorf("git_publish %q: %w", g.ID, err)
		}
		_, commitStderr, rc, err := run.sh([]string{"commit", "-m", msg}, "", 0)
		if err != nil {
			return nil, fmt.Errorf("git_publish %q: %w", g.ID, err)
		}
		if rc != 0 {
			// Python's dict literal carries push_note twice; the later
			// (stderr tail) value wins.
			return &workflow.ExecuteResult{Result: map[string]any{
				"pushed":    false,
				"merged":    nil,
				"push_note": gitPublishTail(commitStderr, 300),
				"note":      fmt.Sprintf("commit 失败: %s", gitPublishTail(commitStderr, 300)),
			}, NextNode: g.NextNode()}, nil
		}
		committedNow = true
	}

	if mergeMode == "none" {
		return &workflow.ExecuteResult{Result: map[string]any{
			"pushed":    false,
			"merged":    nil,
			"push_note": "",
			"note":      fmt.Sprintf("none 模式：改动已 commit 到本地分支 %s，未推送", branch),
		}, NextNode: g.NextNode()}, nil
	}

	remoteHead, err := gitPublishRemoteHead(run, branch)
	if err != nil {
		return nil, fmt.Errorf("git_publish %q: %w", g.ID, err)
	}
	headOut, _, _, err := run.sh([]string{"rev-parse", "HEAD"}, "", 0)
	if err != nil {
		return nil, fmt.Errorf("git_publish %q: %w", g.ID, err)
	}
	head := strings.TrimSpace(headOut)

	var pushed bool
	var pushNote string
	if remoteHead != "" && remoteHead == head && !committedNow {
		pushed, pushNote = true, "远端已同步，跳过重复 push"
	} else {
		args := []string{"push", "origin", branch}
		if remoteHead == "" {
			args = []string{"push", "-u", "origin", branch}
		}
		_, pushStderr, rc, err := run.sh(args, "", 0)
		if err != nil {
			return nil, fmt.Errorf("git_publish %q: %w", g.ID, err)
		}
		pushed = rc == 0
		if !pushed {
			pushNote = gitPublishTail(pushStderr, 300)
		}
	}

	if mergeMode == "pr" {
		if !pushed {
			return &workflow.ExecuteResult{Result: map[string]any{
				"pushed":    false,
				"merged":    false,
				"push_note": pushNote,
				"note":      fmt.Sprintf("分支推送失败，未开 PR：%s", pushNote),
			}, NextNode: g.NextNode()}, nil
		}
		prMerged, prURL, prNote, err := gitPublishCreatePR(ctx, wt, env, timeout, branch, baseBranch, msg)
		if err != nil {
			return nil, fmt.Errorf("git_publish %q: %w", g.ID, err)
		}
		return &workflow.ExecuteResult{Result: map[string]any{
			"pushed":    true,
			"merged":    prMerged,
			"pr_url":    prURL,
			"note":      prNote,
			"push_note": pushNote,
		}, NextNode: g.NextNode()}, nil
	}

	if mergeMode != "main" {
		return &workflow.ExecuteResult{Result: map[string]any{
			"pushed":    pushed,
			"merged":    nil,
			"push_note": pushNote,
			"note":      "branch 模式：仅推分支，不合并 main",
		}, NextNode: g.NextNode()}, nil
	}

	if !pushed {
		return &workflow.ExecuteResult{Result: map[string]any{
			"pushed":    false,
			"merged":    false,
			"push_note": pushNote,
			"note":      fmt.Sprintf("分支推送失败，未合并 %s：%s", baseBranch, pushNote),
		}, NextNode: g.NextNode()}, nil
	}
	mainClone, err := g.evalString(ctx, g.MainClone, "main_clone")
	if err != nil {
		return nil, err
	}
	merged, mergeNote, err := gitPublishMergeToMain(ctx, mainClone, env, timeout, branch, baseBranch)
	if err != nil {
		return nil, fmt.Errorf("git_publish %q: %w", g.ID, err)
	}
	return &workflow.ExecuteResult{Result: map[string]any{
		"pushed":    true,
		"merged":    merged,
		"note":      mergeNote,
		"push_note": pushNote,
	}, NextNode: g.NextNode()}, nil
}

// evalString evaluates an expression field into a string (Python str() parity).
func (g *GitPublish) evalString(ctx context.Context, field any, name string) (string, error) {
	v, err := expression.Eval(ctx, field)
	if err != nil {
		return "", fmt.Errorf("git_publish %q: eval %s: %w", g.ID, name, err)
	}
	return gateToStr(v), nil
}

// resolveMessage mirrors _resolve_message: commit_message > COMMIT_MESSAGE:
// line in plan_file > "fix: issue #N"; nothing resolvable is a named error.
func (g *GitPublish) resolveMessage(ctx context.Context) (string, error) {
	if g.CommitMessage != nil {
		v, err := expression.Eval(ctx, g.CommitMessage)
		if err != nil {
			return "", fmt.Errorf("git_publish %q: eval commit_message: %w", g.ID, err)
		}
		if msg := strings.TrimSpace(gitPublishOrEmpty(v)); msg != "" {
			return msg, nil
		}
	}
	if g.PlanFile != nil {
		v, err := expression.Eval(ctx, g.PlanFile)
		if err != nil {
			return "", fmt.Errorf("git_publish %q: eval plan_file: %w", g.ID, err)
		}
		planPath := gitPublishOrEmpty(v)
		if plan, readErr := os.ReadFile(planPath); readErr == nil {
			if m := gitPublishCommitMsgRe.FindSubmatch(plan); m != nil {
				return strings.TrimSpace(string(m[1])), nil
			}
		}
		// readErr (OSError parity): fall through to the next source
	}
	if g.IssueNumber != nil {
		v, err := expression.Eval(ctx, g.IssueNumber)
		if err != nil {
			return "", fmt.Errorf("git_publish %q: eval issue_number: %w", g.ID, err)
		}
		n, err := toInt(v)
		if err != nil {
			return "", fmt.Errorf("git_publish %q: issue_number 不是整数: %v", g.ID, v)
		}
		return fmt.Sprintf("fix: issue #%d", n), nil
	}
	return "", fmt.Errorf("git_publish %q：无 commit_message/plan_file/issue_number，无法确定提交消息", g.ID)
}

// gitPublishOrEmpty mirrors Python `evaluate(x) or ""`: a falsy evaluation is
// the empty string, anything else is str()-ed.
func gitPublishOrEmpty(v any) string {
	if !isTruthy(v) {
		return ""
	}
	return gateToStr(v)
}

// evalTimeout decodes timeout_secs (JSON scalar parity with the Python int
// field; not expression-evaluated there either). Non-numeric values fail.
// A zero/negative value makes every git command time out immediately —
// subprocess.run(timeout=0) raising TimeoutExpired in Python.
func (g *GitPublish) evalTimeout() (time.Duration, error) {
	secs := gitPublishDefaultTimeoutSecs
	if g.TimeoutSecs != nil {
		n, err := toInt(g.TimeoutSecs)
		if err != nil {
			return 0, fmt.Errorf("git_publish %q: timeout_secs 必须是数字: %v", g.ID, g.TimeoutSecs)
		}
		secs = n
	}
	return time.Duration(secs) * time.Second, nil
}

// gitPublishGit is the sh() closure of git_publish.py: a hardened git argv
// prefix plus cwd/env/timeout, returning capture_output+returncode data.
// Non-zero exit codes are DATA here (callers branch on them); only own
// timeouts (subprocess.TimeoutExpired parity), parent cancellation and spawn
// failures are errors.
type gitPublishGit struct {
	ctx         context.Context
	argv        []string
	env         []string
	cwd         string
	baseTimeout time.Duration
}

// sh runs one hardened git command. A zero timeout means the base timeout
// (Python `t or self.timeout_secs`); cwd overrides the runner default.
func (r *gitPublishGit) sh(args []string, cwd string, timeout time.Duration) (string, string, int, error) {
	if timeout == 0 {
		timeout = r.baseTimeout
	}
	if cwd == "" {
		cwd = r.cwd
	}
	return gitPublishRun(r.ctx, append(slices.Clone(r.argv), args...), cwd, r.env, timeout)
}

// gitPublishRun is the subprocess.run equivalent shared by the git and gh
// invocations of this node: the child leads its own process group (killed on
// timeout/cancel); an own deadline is an error (subprocess.TimeoutExpired
// propagates and fails the node in Python), parent cancellation is an error,
// non-zero exit is returned as data.
func gitPublishRun(ctx context.Context, argv []string, cwd string, env []string, timeout time.Duration) (string, string, int, error) {
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(runCtx, argv[0], argv[1:]...)
	cmd.Dir = cwd
	cmd.Env = env
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil {
			_ = cmd.Process.Kill()
		}
		return nil
	}
	cmd.WaitDelay = gateDrainTimeout

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	runErr := cmd.Run()

	if ctx.Err() != nil {
		return "", "", 0, fmt.Errorf("cancelled via context; process tree killed")
	}
	if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
		return "", "", 0, fmt.Errorf("command timed out after %s: %s", timeout, strings.Join(argv, " "))
	}
	out, errOut := stdout.String(), stderr.String()
	if runErr != nil {
		var ee *exec.ExitError
		if errors.As(runErr, &ee) {
			return out, errOut, ee.ExitCode(), nil
		}
		if errors.Is(runErr, exec.ErrWaitDelay) {
			return out, errOut, 0, nil
		}
		return "", "", 0, fmt.Errorf("run %s: %w", strings.Join(argv, " "), runErr)
	}
	return out, errOut, 0, nil
}

// gitPublishRemoteHead mirrors _remote_head: ls-remote --heads origin
// <branch> (60s) → the sha before the tab, or "" when the branch is absent.
func gitPublishRemoteHead(run *gitPublishGit, branch string) (string, error) {
	out, _, _, err := run.sh([]string{"ls-remote", "--heads", "origin", branch}, "", gitPublishLsRemoteTimeoutSecs*time.Second)
	if err != nil {
		return "", err
	}
	out = strings.TrimSpace(out)
	if out == "" {
		return "", nil
	}
	return strings.SplitN(out, "\t", 2)[0], nil
}

// gitPublishMergeToMain mirrors _merge_to_main: in the main clone (itself
// hardened against its own hooks/filters), fetch origin, merge --ff-only
// origin/<branch>; on failure merge --abort and report honestly; on success
// push HEAD:<base_branch>.
func gitPublishMergeToMain(ctx context.Context, mainClone string, env []string, timeout time.Duration, branch, baseBranch string) (bool, string, error) {
	run := &gitPublishGit{
		ctx: ctx, env: env, cwd: mainClone, baseTimeout: timeout,
		argv: gitPublishHardenedArgv(ctx, mainClone, env, timeout),
	}
	if _, _, _, err := run.sh([]string{"fetch", "origin"}, "", 0); err != nil {
		return false, "", err // fetch rc is ignored in Python; only hard errors propagate
	}
	_, _, rc, err := run.sh([]string{"merge", "--ff-only", "origin/" + branch}, "", 0)
	if err != nil {
		return false, "", err
	}
	if rc != 0 {
		if _, _, _, abortErr := run.sh([]string{"merge", "--abort"}, "", 0); abortErr != nil {
			// Python: the abort's return code is ignored, but a subprocess
			// error (timeout/spawn) would propagate and fail the node.
			return false, "", abortErr
		}
		return false, fmt.Sprintf("ff 合并失败（%s 已前进或分支未推送），分支在远端，请人工合并", baseBranch), nil
	}
	_, _, rc, err = run.sh([]string{"push", "origin", "HEAD:" + baseBranch}, "", 0)
	if err != nil {
		return false, "", err
	}
	if rc == 0 {
		return true, fmt.Sprintf("已 ff 合并推送 %s", baseBranch), nil
	}
	return false, fmt.Sprintf("合并成功但推送 %s 失败", baseBranch), nil
}

// gitPublishCreatePR mirrors _create_pr: gh pr create in the worktree; an
// existing PR is recorded as a note, not a failure. Unlike Python (which
// relies on gh's stored auth), gh inherits the same minimal git env —
// see github_comment.go for the Go-side credential contract.
func gitPublishCreatePR(ctx context.Context, wt string, env []string, timeout time.Duration, branch, baseBranch, title string) (bool, string, string, error) {
	if title == "" {
		title = "merge " + branch
	}
	title = gitPublishHead(gitPublishFirstLine(title), 120)
	body := fmt.Sprintf("Automated by issue-pipeline (branch %s).", branch)
	args := []string{"gh", "pr", "create", "--head", branch, "--base", baseBranch,
		"--title", title, "--body", body}
	stdout, stderr, rc, err := gitPublishRun(ctx, args, wt, env, gitPublishGHTimeoutSecs*time.Second)
	if err != nil {
		return false, "", "", err
	}
	out := stdout + stderr
	if rc == 0 {
		url := ""
		if trimmed := strings.TrimSpace(stdout); trimmed != "" {
			lines := strings.Split(trimmed, "\n")
			url = lines[len(lines)-1]
		}
		return false, url, fmt.Sprintf("已推送分支并创建 PR（base=%s）%s", baseBranch, url), nil
	}
	if strings.Contains(strings.ToLower(out), "already exists") {
		return false, "", fmt.Sprintf("分支已推送；PR 已存在（base=%s），详见仓库 PR 列表", baseBranch), nil
	}
	return false, "", fmt.Sprintf("分支已推送，但 gh pr create 失败：%s", gitPublishTail(out, 200)), nil
}

// ── git env hardening (_git_env.py port: pn#17 whitelist + pn#21 value scan
//    and argv hardening) ──────────────────────────────────────────────────

// gitPublishBaseEnvAllowlist is the base subprocess allowlist
// (plaita.subprocess_env _HOST_ALLOWLIST), shared entry point of
// build_git_env.
var gitPublishBaseEnvAllowlist = []string{
	"HOME", "LANG", "LC_ALL", "LC_CTYPE", "PATH", "PYTHONIOENCODING", "TMPDIR",
}

// gitPublishGHAllowlist is gh's non-credential config-location set
// (_GH_ALLOWLIST). GH_TOKEN / GH_ENTERPRISE_TOKEN are deliberately absent.
var gitPublishGHAllowlist = []string{
	"GH_CONFIG_DIR", "GH_HOST", "GH_REPO", "GH_PROMPT_DISABLED",
	"GH_NO_UPDATE_NOTIFIER", "GH_FORCE_TTY",
}

// gitPublishExtraAllow mirrors _EXTRA_ALLOW: gh's default config dir and the
// local timezone for commit timestamps.
var gitPublishExtraAllow = []string{"XDG_CONFIG_HOME", "TZ"}

var (
	gitPublishDenyExact = map[string]bool{
		"GIT_ASKPASS": true, "GIT_CONFIG_COUNT": true, "GIT_CONFIG_PARAMETERS": true,
		"GIT_DIR": true, "GIT_WORK_TREE": true, "GIT_INDEX_FILE": true,
		"GIT_OBJECT_DIRECTORY": true, "GIT_ALTERNATE_OBJECT_DIRECTORIES": true,
		"GIT_COMMON_DIR": true, "GIT_NAMESPACE": true, "GIT_PREFIX": true,
		"GIT_CURL_VERBOSE": true, "SSH_AUTH_SOCK": true, // pn#21: agent identity stays out
	}
	gitPublishDenyPrefixes = []string{"GIT_CONFIG_KEY_", "GIT_CONFIG_VALUE_", "GIT_TRACE"}

	// gitPublishSecretNameRe: name hit → denied (case-insensitive). A plain
	// path-only GIT_SSH_COMMAND does not match.
	gitPublishSecretNameRe = regexp.MustCompile(`(?i)(TOKEN|SECRET|PASSWORD|PASSWD|CREDENTIAL|PRIVATE_KEY|API_KEY)`)

	// Value-level scan (pn#21 secondary net): only these keys' values may
	// carry credentials; ordinary env values are not scanned.
	gitPublishScanKeyRe  = regexp.MustCompile(`(?i)^(?:GIT_SSH_COMMAND|GIT_PROXY_COMMAND|PROXY|.*_PROXY)$`)
	gitPublishProxyKeyRe = regexp.MustCompile(`(?i)^(?:PROXY|.*_PROXY)$`)
)

// gitPublishValuePatterns are the embedded-credential shapes scanned in
// GIT_SSH_COMMAND / GIT_PROXY_COMMAND / *_PROXY values (_VALUE_PATTERNS).
var gitPublishValuePatterns = []struct {
	label string
	re    *regexp.Regexp
}{
	{"URL 内嵌口令", regexp.MustCompile(`[A-Za-z][A-Za-z0-9+.-]*://[^/\s:@]+:[^/\s@]+@`)},
	{"已知 token 形态", regexp.MustCompile(`(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}|glpat-[A-Za-z0-9_-]{20,}|xox[abposr]-[A-Za-z0-9-]{10,}|sk-[A-Za-z0-9]{20,}|AKIA[0-9A-Z]{16}|npm_[A-Za-z0-9]{20,}|pypi-[A-Za-z0-9_-]{20,})`)},
	{"Authorization 头", regexp.MustCompile(`(?i)\b(?:authorization\s*:\s*)?(?:bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}`)},
	{"内嵌 secret 赋值", regexp.MustCompile(`(?i)\b(?:token|secret|password|passwd|api[_-]?key|access[_-]?key)\s*[=:]\s*\S`)},
	{"代理/认证命令行参数", regexp.MustCompile(`(?i)(?:--proxy-user|--proxy-password|sshpass\b[^|;]*\s-p)\s+\S+`)},
}

// gitPublishProxyUserinfoRe: any userinfo in a proxy URL is a credential
// surface (tokens are commonly used as the userinfo).
var gitPublishProxyUserinfoRe = regexp.MustCompile(`[A-Za-z][A-Za-z0-9+.-]*://[^/\s:@]+@`)

// gitPublishEnvDenied mirrors _denied: exact deny list, deny prefixes and the
// credential-name regex (all case-insensitive on the upper-cased key).
func gitPublishEnvDenied(key string) bool {
	upper := strings.ToUpper(key)
	if gitPublishDenyExact[upper] {
		return true
	}
	for _, p := range gitPublishDenyPrefixes {
		if strings.HasPrefix(upper, p) {
			return true
		}
	}
	return gitPublishSecretNameRe.MatchString(upper)
}

// gitPublishCredentialHits mirrors _credential_hits.
func gitPublishCredentialHits(key, value string) []string {
	var hits []string
	for _, p := range gitPublishValuePatterns {
		if p.re.MatchString(value) {
			hits = append(hits, p.label)
		}
	}
	if gitPublishProxyKeyRe.MatchString(key) && gitPublishProxyUserinfoRe.MatchString(value) {
		hits = append(hits, "代理 URL userinfo")
	}
	return hits
}

// gitPublishBuildEnv mirrors build_git_env: base host whitelist → host
// non-denied GIT_* → gh config allowlist → extra allowlist; a final deny
// pass; then a value-level credential scan over GIT_SSH_COMMAND /
// GIT_PROXY_COMMAND / *_PROXY that FAILS CLOSED (named error, value never
// echoed) instead of silently dropping.
func gitPublishBuildEnv() ([]string, error) {
	env := map[string]string{}
	for _, key := range gitPublishBaseEnvAllowlist {
		if v, ok := os.LookupEnv(key); ok {
			env[key] = v
		}
	}
	for _, kv := range os.Environ() {
		key, value, found := strings.Cut(kv, "=")
		if !found {
			continue
		}
		if strings.HasPrefix(key, "GIT_") && !gitPublishEnvDenied(key) {
			env[key] = value
		}
	}
	for _, keys := range [][]string{gitPublishGHAllowlist, gitPublishExtraAllow} {
		for _, key := range keys {
			if v, ok := os.LookupEnv(key); ok {
				env[key] = v
			}
		}
	}
	// Final deny pass: anything that slipped in via a later stage is dropped
	// (deployers cannot sneak credentials through the allowlists).
	for key := range env {
		if gitPublishEnvDenied(key) {
			delete(env, key)
		}
	}
	keys := slices.Sorted(maps.Keys(env))
	for _, key := range keys {
		if !gitPublishScanKeyRe.MatchString(key) {
			continue
		}
		if hits := gitPublishCredentialHits(key, env[key]); len(hits) > 0 {
			// Only the variable name and hit categories are reported — the
			// value would leak into logs/issues.
			return nil, fmt.Errorf("拒绝把 %s 传入 git 子进程：值疑似内嵌凭据（%s）；请改用 credential helper / 预置远程 URL / 无凭据代理（pn#21）",
				key, strings.Join(hits, "、"))
		}
	}
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, key+"="+env[key])
	}
	return out, nil
}

// ── argv hardening (pn#21 boundary move) ─────────────────────────────────

var (
	// gitPublishFilterKeyQuery is passed to `git config --get-regexp`.
	gitPublishFilterKeyQuery = `^filter\..*\.(clean|smudge|process)$`
	// gitPublishFilterKeyRe extracts the driver name from a config key
	// (re.S parity via (?s)).
	gitPublishFilterKeyRe = regexp.MustCompile(`(?s)^filter\.(.+)\.(clean|smudge|process)$`)
	gitPublishFilterOps   = []string{"clean", "smudge", "process"}
)

// gitPublishBaseArgv is _base_argv: the hardened -c overrides every git
// command carries.
func gitPublishBaseArgv() []string {
	return []string{
		"git",
		"-c", "core.hooksPath=/dev/null", // hooks never execute
		"-c", "core.fsmonitor=false", // fsmonitor is hook-equivalent
		"-c", "protocol.ext.allow=never", // ext:: transport can run arbitrary commands
	}
}

// gitPublishFilterDrivers enumerates the filter drivers configured in ANY
// scope (local/global/system/worktree) of repo. Any failure (git missing,
// broken repo) yields an empty set — later git commands will fail on their
// own; the enumeration never changes semantics. Keys are NUL-separated
// (--null --name-only: driver names may contain spaces/dots).
func gitPublishFilterDrivers(ctx context.Context, repo string, env []string, timeout time.Duration) []string {
	argv := append(gitPublishBaseArgv(), "config", "--get-regexp", "--null", "--name-only", gitPublishFilterKeyQuery)
	stdout, _, _, err := gitPublishRun(ctx, argv, repo, env, timeout)
	if err != nil {
		return nil
	}
	seen := map[string]bool{}
	for _, key := range strings.Split(stdout, "\x00") {
		if m := gitPublishFilterKeyRe.FindStringSubmatch(key); m != nil {
			seen[m[1]] = true
		}
	}
	return slices.Sorted(maps.Keys(seen))
}

// gitPublishHardenedArgv mirrors git_hardened_argv: the hardened prefix plus
// one empty override per configured filter driver/op (required=true drivers
// fail loudly rather than silently passing raw content). Drivers are
// re-enumerated per call — the local config is an agent-writable surface and
// must not be cached across operations.
func gitPublishHardenedArgv(ctx context.Context, repo string, env []string, timeout time.Duration) []string {
	argv := gitPublishBaseArgv()
	for _, driver := range gitPublishFilterDrivers(ctx, repo, env, timeout) {
		for _, op := range gitPublishFilterOps {
			argv = append(argv, "-c", fmt.Sprintf("filter.%s.%s=", driver, op))
		}
	}
	return argv
}

// ── small string helpers ─────────────────────────────────────────────────

var gitPublishCommitMsgRe = regexp.MustCompile(`(?m)^COMMIT_MESSAGE: (.+)$`)

// gitPublishTail keeps the last n runes of s (Python s[-n:] parity).
func gitPublishTail(s string, n int) string {
	if n <= 0 {
		return ""
	}
	rs := []rune(s)
	if len(rs) <= n {
		return s
	}
	return string(rs[len(rs)-n:])
}

// gitPublishHead keeps the first n runes of s (Python s[:n] parity).
func gitPublishHead(s string, n int) string {
	if n <= 0 {
		return ""
	}
	rs := []rune(s)
	if len(rs) <= n {
		return s
	}
	return string(rs[:n])
}

// gitPublishFirstLine is splitlines()[0] for the PR title.
func gitPublishFirstLine(s string) string {
	if i := strings.IndexAny(s, "\r\n"); i >= 0 {
		return s[:i]
	}
	return s
}
