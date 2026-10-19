package nodes

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// ── test scaffolding: real git binary + local bare remote (zero network,
//    zero credentials) ─────────────────────────────────────────────────────

func gitPublishRequireGit(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not on PATH")
	}
}

// gitPublishGit runs a plain (un-hardened) git command for test setup and
// fails the test on error, returning stdout.
func gitPublishTestGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	var out, errBuf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errBuf
	if err := cmd.Run(); err != nil {
		t.Fatalf("git %v: %v: %s", args, err, strings.TrimSpace(errBuf.String()))
	}
	return out.String()
}

// gitPublishSetupRepo creates a worktree repo (branch main) with one commit,
// pushed to a fresh local bare remote named origin.
func gitPublishSetupRepo(t *testing.T) (wt, remote string) {
	t.Helper()
	gitPublishRequireGit(t)
	base := t.TempDir()
	remote = filepath.Join(base, "remote.git")
	wt = filepath.Join(base, "wt")
	gitPublishTestGit(t, "", "init", "--bare", remote)
	gitPublishTestGit(t, "", "init", wt)
	// Deterministic branch name regardless of init.defaultBranch.
	gitPublishTestGit(t, wt, "symbolic-ref", "HEAD", "refs/heads/main")
	gitPublishTestGit(t, wt, "config", "user.email", "plaita-test@example.com")
	gitPublishTestGit(t, wt, "config", "user.name", "plaita-test")
	if err := os.WriteFile(filepath.Join(wt, "README.md"), []byte("init\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitPublishTestGit(t, wt, "add", "-A")
	gitPublishTestGit(t, wt, "commit", "-m", "init")
	gitPublishTestGit(t, wt, "remote", "add", "origin", remote)
	gitPublishTestGit(t, wt, "push", "-u", "origin", "main")
	return wt, remote
}

// gitPublishClone clones the bare remote into a sibling directory.
func gitPublishClone(t *testing.T, remote string) string {
	t.Helper()
	dst := filepath.Join(filepath.Dir(remote), "clone")
	gitPublishTestGit(t, "", "clone", remote, dst)
	gitPublishTestGit(t, dst, "config", "user.email", "plaita-test@example.com")
	gitPublishTestGit(t, dst, "config", "user.name", "plaita-test")
	return dst
}

func gitPublishTestRemoteHead(t *testing.T, remote, branch string) string {
	t.Helper()
	out := strings.TrimSpace(gitPublishTestGit(t, "", "ls-remote", "--heads", remote, branch))
	if out == "" {
		return ""
	}
	return strings.SplitN(out, "\t", 2)[0]
}

func gitPublishLocalHead(t *testing.T, wt string) string {
	t.Helper()
	return strings.TrimSpace(gitPublishTestGit(t, wt, "rev-parse", "HEAD"))
}

func gitPublishWriteDirtyFile(t *testing.T, wt, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(wt, "feature.txt"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// gitPublishFakeGH installs a fake gh CLI on PATH. Behavior is keyed on the
// --head branch value so the (non-allowlisted) environment cannot reach the
// subprocess: branches named exists-branch/fail-branch fail accordingly, any
// other branch gets a URL. All invocations are appended to a log file.
func gitPublishFakeGH(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	script := `#!/bin/sh
echo "$@" >> "$(dirname "$0")/invocations.log"
for a in "$@"; do
  case "$a" in
    exists-branch) echo "pull request for branch exists-branch already exists" >&2; exit 1 ;;
    fail-branch) echo "gh: network exploded" >&2; exit 1 ;;
  esac
done
echo "Creating pull request..."
echo "https://github.example/org/repo/pull/9"
`
	if err := os.WriteFile(filepath.Join(dir, "gh"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return filepath.Join(dir, "invocations.log")
}

// ── execute-level scenarios ───────────────────────────────────────────────

func TestGitPublishBranchModeFreshPush(t *testing.T) {
	wt, remote := gitPublishSetupRepo(t)
	gitPublishTestGit(t, wt, "checkout", "-b", "feature")
	gitPublishWriteDirtyFile(t, wt, "v1\n")

	node := &GitPublish{
		BaseNode:      BaseNode{ID: "gp1"},
		WorktreeDir:   wt,
		BranchName:    "feature",
		CommitMessage: "feat: add feature",
	}
	res, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["pushed"] != true {
		t.Errorf("pushed = %v (%v)", out["pushed"], out["push_note"])
	}
	if out["merged"] != nil {
		t.Errorf("merged = %v, want nil in branch mode", out["merged"])
	}
	if out["note"] != "branch 模式：仅推分支，不合并 main" {
		t.Errorf("note = %q", out["note"])
	}
	if out["push_note"] != "" {
		t.Errorf("push_note = %q", out["push_note"])
	}
	if _, hasDry := out["dry_run"]; hasDry {
		t.Errorf("non-dry output must not carry dry_run key: %v", out)
	}
	if head := gitPublishTestRemoteHead(t, remote, "feature"); head != gitPublishLocalHead(t, wt) {
		t.Errorf("remote head %q != local head %q", head, gitPublishLocalHead(t, wt))
	}
	if msg := strings.TrimSpace(gitPublishTestGit(t, wt, "log", "-1", "--format=%s")); msg != "feat: add feature" {
		t.Errorf("commit message = %q", msg)
	}
}

// TestGitPublishIdempotentRedelivery pins the README #6 fix: a duplicate
// delivery must first commit any fresh worktree changes (never silently drop
// them just because the branch already exists on the remote), and a true
// duplicate (nothing new) skips the push.
func TestGitPublishIdempotentRedelivery(t *testing.T) {
	wt, remote := gitPublishSetupRepo(t)
	gitPublishTestGit(t, wt, "checkout", "-b", "feature")
	node := &GitPublish{
		BaseNode:      BaseNode{ID: "gp1"},
		WorktreeDir:   wt,
		BranchName:    "feature",
		CommitMessage: "feat: one",
	}

	// 1st delivery: clean tree, branch not on remote yet → push -u.
	res, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	if out := res.Result.(map[string]any); out["pushed"] != true {
		t.Fatalf("first delivery: pushed = %v (%v)", out["pushed"], out["push_note"])
	}
	firstHead := gitPublishLocalHead(t, wt)

	// 2nd delivery: true duplicate (clean, remote == local) → skip push.
	res, err = node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("duplicate delivery: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["pushed"] != true || out["push_note"] != "远端已同步，跳过重复 push" {
		t.Errorf("duplicate delivery = %v", out)
	}
	if gitPublishLocalHead(t, wt) != firstHead {
		t.Error("duplicate delivery must not create commits")
	}

	// 3rd delivery: fresh uncommitted changes appear before the retry — they
	// must be committed and pushed, not dropped.
	gitPublishWriteDirtyFile(t, wt, "v2\n")
	res, err = node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("redelivery with changes: %v", err)
	}
	out = res.Result.(map[string]any)
	if out["pushed"] != true || out["push_note"] != "" {
		t.Errorf("redelivery = %v", out)
	}
	if head := gitPublishTestRemoteHead(t, remote, "feature"); head != gitPublishLocalHead(t, wt) || head == firstHead {
		t.Errorf("fresh changes lost: remote head %q, local %q, first %q", head, gitPublishLocalHead(t, wt), firstHead)
	}
}

func TestGitPublishNoneMode(t *testing.T) {
	wt, remote := gitPublishSetupRepo(t)
	gitPublishTestGit(t, wt, "checkout", "-b", "feature")
	gitPublishWriteDirtyFile(t, wt, "local only\n")
	before := gitPublishTestRemoteHead(t, remote, "feature")

	node := &GitPublish{
		BaseNode:      BaseNode{ID: "gp1"},
		WorktreeDir:   wt,
		BranchName:    "feature",
		CommitMessage: "feat: local",
		MergeMode:     "none",
	}
	res, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["pushed"] != false || out["merged"] != nil {
		t.Errorf("none mode pushed/merged = %v/%v", out["pushed"], out["merged"])
	}
	if note, _ := out["note"].(string); !strings.Contains(note, "none 模式") || !strings.Contains(note, "feature") {
		t.Errorf("note = %q", out["note"])
	}
	if got := gitPublishTestRemoteHead(t, remote, "feature"); got != before {
		t.Errorf("none mode must not push: remote head %q → %q", before, got)
	}
	// The change IS committed locally.
	if dirty := gitPublishTestGit(t, wt, "status", "--porcelain"); dirty != "" {
		t.Errorf("none mode must still commit: status = %q", dirty)
	}
}

func TestGitPublishDryRun(t *testing.T) {
	wt, remote := gitPublishSetupRepo(t)
	gitPublishTestGit(t, wt, "checkout", "-b", "feature")
	gitPublishWriteDirtyFile(t, wt, "v1\n")
	before := gitPublishLocalHead(t, wt)

	node := &GitPublish{
		BaseNode:      BaseNode{ID: "gp1"},
		WorktreeDir:   wt,
		BranchName:    "feature",
		CommitMessage: "feat: dry",
		DryRun:        true,
	}
	res, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["pushed"] != true || out["merged"] != nil || out["dry_run"] != true {
		t.Errorf("dry-run output = %v", out)
	}
	if out["note"] != "dry-run：跳过 git 操作" {
		t.Errorf("note = %q", out["note"])
	}
	if gitPublishLocalHead(t, wt) != before {
		t.Error("dry-run must not commit")
	}
	if gitPublishTestRemoteHead(t, remote, "feature") != "" {
		t.Error("dry-run must not push")
	}
}

func TestGitPublishMainModeFFMerge(t *testing.T) {
	wt, remote := gitPublishSetupRepo(t)
	cloneB := gitPublishClone(t, remote) // sits on main at the initial commit
	gitPublishTestGit(t, wt, "checkout", "-b", "feature")
	gitPublishWriteDirtyFile(t, wt, "the fix\n")

	node := &GitPublish{
		BaseNode:      BaseNode{ID: "gp1"},
		WorktreeDir:   wt,
		BranchName:    "feature",
		CommitMessage: "fix: issue #6",
		MergeMode:     "main",
		MainClone:     cloneB,
	}
	res, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["pushed"] != true || out["merged"] != true {
		t.Fatalf("pushed/merged = %v/%v (push_note=%v)", out["pushed"], out["merged"], out["push_note"])
	}
	if out["note"] != "已 ff 合并推送 main" {
		t.Errorf("note = %q", out["note"])
	}
	// The BRANCH was merged into main — remote main now carries the fix.
	featureHead := gitPublishTestRemoteHead(t, remote, "feature")
	if mainHead := gitPublishTestRemoteHead(t, remote, "main"); mainHead != featureHead {
		t.Errorf("remote main %q != merged feature %q", mainHead, featureHead)
	}
}

func TestGitPublishMainModeFFFailsWhenBaseDiverged(t *testing.T) {
	wt, remote := gitPublishSetupRepo(t)
	cloneB := gitPublishClone(t, remote)
	gitPublishTestGit(t, wt, "checkout", "-b", "feature")
	gitPublishWriteDirtyFile(t, wt, "fix 1\n")

	node := &GitPublish{
		BaseNode:      BaseNode{ID: "gp1"},
		WorktreeDir:   wt,
		BranchName:    "feature",
		CommitMessage: "fix: one",
		MergeMode:     "main",
		MainClone:     cloneB,
	}
	if _, err := node.Execute(gateTestCtx(t, map[string]any{}, nil)); err != nil {
		t.Fatalf("first run: %v", err)
	}

	// base branch advances independently on the remote.
	gitPublishWriteDirtyFile(t, cloneB, "base advances\n")
	gitPublishTestGit(t, cloneB, "add", "-A")
	gitPublishTestGit(t, cloneB, "commit", "-m", "base advances")
	mainAdvanced := gitPublishLocalHead(t, cloneB)
	gitPublishTestGit(t, cloneB, "push", "origin", "main")

	// new work on the branch → push succeeds, ff merge into main must fail honestly.
	gitPublishWriteDirtyFile(t, wt, "fix 2\n")
	res, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["pushed"] != true || out["merged"] != false {
		t.Fatalf("pushed/merged = %v/%v", out["pushed"], out["merged"])
	}
	if note, _ := out["note"].(string); !strings.Contains(note, "ff 合并失败") || !strings.Contains(note, "请人工合并") {
		t.Errorf("note = %q", out["note"])
	}
	// abort left the base untouched.
	if mainHead := gitPublishTestRemoteHead(t, remote, "main"); mainHead != mainAdvanced {
		t.Errorf("remote main moved after failed merge: %q → %q", mainAdvanced, mainHead)
	}
}

func TestGitPublishPRMode(t *testing.T) {
	cases := []struct {
		name     string
		branch   string
		wantPush any
		wantURL  string
		wantNote string
	}{
		{"success", "pr-branch", true, "https://github.example/org/repo/pull/9", "已推送分支并创建 PR（base=develop）"},
		{"already exists", "exists-branch", true, "", "PR 已存在"},
		{"gh failure", "fail-branch", true, "", "gh pr create 失败"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			log := gitPublishFakeGH(t)
			wt, _ := gitPublishSetupRepo(t)
			gitPublishTestGit(t, wt, "checkout", "-b", tc.branch)

			node := &GitPublish{
				BaseNode:      BaseNode{ID: "gp1"},
				WorktreeDir:   wt,
				BranchName:    tc.branch,
				CommitMessage: "feat: pr body title",
				MergeMode:     "pr",
				BaseBranch:    "develop",
			}
			res, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			out := res.Result.(map[string]any)
			if out["pushed"] != tc.wantPush || out["merged"] != false {
				t.Errorf("pushed/merged = %v/%v", out["pushed"], out["merged"])
			}
			if out["pr_url"] != tc.wantURL {
				t.Errorf("pr_url = %q, want %q", out["pr_url"], tc.wantURL)
			}
			if note, _ := out["note"].(string); !strings.Contains(note, tc.wantNote) {
				t.Errorf("note = %q, want containing %q", note, tc.wantNote)
			}
			if tc.name == "success" {
				invocation, err := os.ReadFile(log)
				if err != nil {
					t.Fatalf("fake gh not invoked: %v", err)
				}
				for _, want := range []string{"--head", tc.branch, "--base", "develop"} {
					if !strings.Contains(string(invocation), want) {
						t.Errorf("gh invocation %q missing %q", strings.TrimSpace(string(invocation)), want)
					}
				}
			}
		})
	}
}

func TestGitPublishPushFailureIsDataNotError(t *testing.T) {
	// Not a git repository at all: every git command fails with rc != 0,
	// which the node reports as data (pushed=false + stderr tail), mirroring
	// the Python subprocess.run(returncode) handling.
	node := &GitPublish{
		BaseNode:      BaseNode{ID: "gp1"},
		WorktreeDir:   t.TempDir(),
		BranchName:    "feature",
		CommitMessage: "whatever",
	}
	res, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("push failure must be data, not a node error: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["pushed"] != false {
		t.Errorf("pushed = %v", out["pushed"])
	}
	if out["merged"] != nil {
		t.Errorf("merged = %v, want nil in branch mode", out["merged"])
	}
	if pushNote, _ := out["push_note"].(string); !strings.Contains(pushNote, "not a git repository") {
		t.Errorf("push_note = %q, want git's failure stderr", pushNote)
	}
}

func TestGitPublishValidationErrors(t *testing.T) {
	wt, _ := gitPublishSetupRepo(t)
	cases := []struct {
		name string
		node *GitPublish
		want string
	}{
		{"missing fields", &GitPublish{BaseNode: BaseNode{ID: "g"}}, "缺少 worktree_dir/branch_name"},
		{"bad merge_mode", &GitPublish{
			BaseNode: BaseNode{ID: "g"}, WorktreeDir: wt, BranchName: "feature", MergeMode: "rebase",
		}, "merge_mode 非法"},
		{"main without main_clone", &GitPublish{
			BaseNode: BaseNode{ID: "g"}, WorktreeDir: wt, BranchName: "feature", MergeMode: "main",
		}, "需要 main_clone"},
		{"no message source", func() *GitPublish {
			// The message is only resolved when the tree is dirty.
			wt2, _ := gitPublishSetupRepo(t)
			gitPublishWriteDirtyFile(t, wt2, "dirty\n")
			return &GitPublish{
				BaseNode: BaseNode{ID: "g"}, WorktreeDir: wt2, BranchName: "feature",
			}
		}(), "无法确定提交消息"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.node.Execute(gateTestCtx(t, map[string]any{}, nil)); err == nil {
				t.Fatal("expected a named error")
			} else if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestGitPublishResolveMessage(t *testing.T) {
	dir := t.TempDir()
	plan := filepath.Join(dir, "plan.md")
	if err := os.WriteFile(plan, []byte("step 1\nCOMMIT_MESSAGE: from plan file\nstep 2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		node *GitPublish
		want string
	}{
		{"explicit message wins", &GitPublish{
			CommitMessage: "  explicit msg  ", PlanFile: plan, IssueNumber: 7,
		}, "explicit msg"},
		{"plan file line", &GitPublish{PlanFile: plan}, "from plan file"},
		{"empty explicit falls to plan", &GitPublish{CommitMessage: "   ", PlanFile: plan}, "from plan file"},
		{"issue fallback", &GitPublish{IssueNumber: 7}, "fix: issue #7"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.node.BaseNode = BaseNode{ID: "g"}
			msg, err := tc.node.resolveMessage(context.Background())
			if err != nil {
				t.Fatalf("resolveMessage: %v", err)
			}
			if msg != tc.want {
				t.Errorf("msg = %q, want %q", msg, tc.want)
			}
		})
	}
	// Missing plan file (OSError parity) falls through to the issue fallback.
	node := &GitPublish{BaseNode: BaseNode{ID: "g"}, PlanFile: filepath.Join(dir, "missing.md"), IssueNumber: 3}
	if msg, err := node.resolveMessage(context.Background()); err != nil || msg != "fix: issue #3" {
		t.Errorf("missing plan file: msg=%q err=%v", msg, err)
	}
}

func TestGitPublishTimeoutIsAnError(t *testing.T) {
	gitPublishRequireGit(t)
	_, _, _, err := gitPublishRun(context.Background(), []string{"sleep", "2"}, "", nil, 100_000_000) // 100ms
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("err = %v, want own-timeout error (subprocess.TimeoutExpired parity)", err)
	}
}

// ── env / argv hardening (pn#17 + pn#21) ──────────────────────────────────

func TestGitPublishBuildEnv(t *testing.T) {
	cases := []struct {
		name    string
		env     map[string]string
		wantIn  []string
		wantOut []string
		wantErr string
	}{
		{
			name: "credentials stripped by name",
			env: map[string]string{
				"GH_TOKEN": "ghp_secret", "GITHUB_TOKEN": "x", "PLAITA_CREDENTIALS_KEY": "k",
				"GIT_DIR": "/somewhere", "GIT_WORK_TREE": "/elsewhere", "SSH_AUTH_SOCK": "/agent",
				"GIT_TRACE_CURL": "1", "GIT_CONFIG_KEY_0": "http.extraheader", "GIT_CONFIG_VALUE_0": "x",
				"GIT_ASKPASS": "/helper", "GIT_CONFIG_COUNT": "1",
			},
			wantIn:  []string{"PATH"},
			wantOut: []string{"GH_TOKEN", "GITHUB_TOKEN", "PLAITA_CREDENTIALS_KEY", "GIT_DIR", "GIT_WORK_TREE", "SSH_AUTH_SOCK", "GIT_TRACE_CURL", "GIT_CONFIG_KEY_0", "GIT_ASKPASS"},
		},
		{
			name:    "benign GIT_*/gh/tz kept",
			env:     map[string]string{"GIT_SSL_CAINFO": "/ca.pem", "GH_HOST": "github.example.com", "TZ": "Asia/Shanghai"},
			wantIn:  []string{"GIT_SSL_CAINFO", "GH_HOST", "TZ"},
			wantOut: nil,
		},
		{
			name:    "plain GIT_SSH_COMMAND kept",
			env:     map[string]string{"GIT_SSH_COMMAND": "ssh -i /keys/id_ed25519"},
			wantIn:  []string{"GIT_SSH_COMMAND"},
			wantOut: nil,
		},
		{
			name:    "sshpass in GIT_SSH_COMMAND fails closed",
			env:     map[string]string{"GIT_SSH_COMMAND": "sshpass -p hunter2 ssh -o UserKnownHostsFile=/dev/null -i /k"},
			wantErr: "拒绝把 GIT_SSH_COMMAND",
		},
		{
			name:    "embedded token in GIT_PROXY_COMMAND fails closed",
			env:     map[string]string{"GIT_PROXY_COMMAND": "connect -X ghp_1234567890abcdefghijklmn"},
			wantErr: "拒绝把 GIT_PROXY_COMMAND",
		},
		{
			name:    "proxy userinfo URL fails closed",
			env:     map[string]string{"GIT_PROXY_COMMAND": "http://user:pass@proxy.internal:8080"},
			wantErr: "URL 内嵌口令",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			env, err := gitPublishBuildEnv()
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want containing %q", err, tc.wantErr)
				}
				// The offending value must never be echoed.
				if strings.Contains(err.Error(), "hunter2") || strings.Contains(err.Error(), "user:pass") ||
					strings.Contains(err.Error(), "ghp_") {
					t.Errorf("error echoes the credential value: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("gitPublishBuildEnv: %v", err)
			}
			joined := strings.Join(env, "\n")
			for _, key := range tc.wantIn {
				found := false
				for _, kv := range env {
					if strings.HasPrefix(kv, key+"=") {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("env missing %s:\n%s", key, joined)
				}
			}
			for _, key := range tc.wantOut {
				for _, kv := range env {
					if strings.HasPrefix(kv, key+"=") {
						t.Errorf("env must not carry %s (got %q)", key, kv)
					}
				}
			}
		})
	}
}

func TestGitPublishHardenedArgv(t *testing.T) {
	gitPublishRequireGit(t)
	wt, _ := gitPublishSetupRepo(t)
	// Configure a filter driver in the repo's local config (agent-writable
	// surface) — the argv must neutralize all three ops for it.
	gitPublishTestGit(t, wt, "config", "filter.broken.clean", "false")

	env, err := gitPublishBuildEnv()
	if err != nil {
		t.Fatal(err)
	}
	argv := gitPublishHardenedArgv(context.Background(), wt, env, 10_000_000_000)
	joined := strings.Join(argv, "\x1f")
	for _, want := range []string{
		"git", "core.hooksPath=/dev/null", "core.fsmonitor=false", "protocol.ext.allow=never",
		"filter.broken.clean=", "filter.broken.smudge=", "filter.broken.process=",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("hardened argv missing %q: %v", want, argv)
		}
	}
}

// TestGitPublishRepoExecutableSurfaceNeverRuns pins pn#21 end-to-end: a
// hostile pre-commit hook and a hostile clean filter must both get zero
// execution while the node still commits the content.
func TestGitPublishRepoExecutableSurfaceNeverRuns(t *testing.T) {
	gitPublishRequireGit(t)
	wt, remote := gitPublishSetupRepo(t)
	gitPublishTestGit(t, wt, "checkout", "-b", "feature")

	// pre-commit hook that would fail the commit if it ever ran.
	hooks := filepath.Join(wt, ".git", "hooks")
	if err := os.WriteFile(filepath.Join(hooks, "pre-commit"), []byte("#!/bin/sh\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	// clean filter that would break `git add` if it ever ran.
	gitPublishTestGit(t, wt, "config", "filter.hostile.clean", "false")
	if err := os.WriteFile(filepath.Join(wt, ".gitattributes"), []byte("*.txt filter=hostile\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitPublishWriteDirtyFile(t, wt, "content\n")

	node := &GitPublish{
		BaseNode:      BaseNode{ID: "gp1"},
		WorktreeDir:   wt,
		BranchName:    "feature",
		CommitMessage: "feat: hardened",
	}
	res, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if out := res.Result.(map[string]any); out["pushed"] != true {
		t.Fatalf("pushed = %v (%v)", out["pushed"], out["push_note"])
	}
	if head := gitPublishTestRemoteHead(t, remote, "feature"); head != gitPublishLocalHead(t, wt) {
		t.Errorf("commit/push failed under hostile hook+filter: remote %q local %q",
			head, gitPublishLocalHead(t, wt))
	}
}

// TestGitPublishProxyUserinfoHit pins the *_PROXY userinfo rule: any userinfo
// in a PROXY-family value counts as a credential (tokens are commonly used as
// the userinfo). Not reachable through gitPublishBuildEnv in the node path
// (no *_PROXY key is allowlisted), so it is pinned at the hit-classifier level.
func TestGitPublishProxyUserinfoHit(t *testing.T) {
	if hits := gitPublishCredentialHits("HTTPS_PROXY", "http://token123@proxy.internal:8080"); len(hits) == 0 || !strings.Contains(strings.Join(hits, "、"), "代理 URL userinfo") {
		t.Errorf("hits = %v, want 代理 URL userinfo", hits)
	}
	if hits := gitPublishCredentialHits("GIT_PROXY_COMMAND", "http://token123@proxy.internal:8080"); strings.Contains(strings.Join(hits, "、"), "代理 URL userinfo") {
		// parity: the userinfo rule is keyed on PROXY-family names only, and
		// GIT_PROXY_COMMAND does not match `^(?:PROXY|.*_PROXY)$`.
		t.Errorf("GIT_PROXY_COMMAND must not take the proxy-userinfo branch: %v", hits)
	}
}

func TestGitPublishJSONRoundTripAndRegistration(t *testing.T) {
	doc := `{"type":"git_publish","id":"gp1","worktree_dir":"$INPUT.wt","branch_name":"feature",
		"commit_message":"msg","plan_file":"plan.md","issue_number":7,"merge_mode":"main",
		"main_clone":"/srv/main","base_branch":"develop","dry_run":true,"timeout_secs":60}`
	node := new(GitPublish)
	if err := json.Unmarshal([]byte(doc), node); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if node.ID != "gp1" || !node.DryRun || node.MergeMode != "main" || node.MainClone != "/srv/main" ||
		node.BaseBranch != "develop" || node.CommitMessage != "msg" || node.PlanFile != "plan.md" {
		t.Errorf("fields = %+v", node)
	}
	if n, ok := node.IssueNumber.(float64); !ok || n != 7 {
		t.Errorf("issue_number = %#v", node.IssueNumber)
	}
	if n, ok := node.TimeoutSecs.(float64); !ok || n != 60 {
		t.Errorf("timeout_secs = %#v", node.TimeoutSecs)
	}
	if workflow.GetNodeFactory(nodeTypeGitPublish) == nil {
		t.Error("git_publish node type not registered")
	}
}
