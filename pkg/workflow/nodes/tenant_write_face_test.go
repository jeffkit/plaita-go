package nodes

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jeffkit/plaita-go/internal/runtime"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redismock/v8"
)

// Negative/positive controls for the flow-controlled host WRITE face (#36:
// writefile / git_publish / report / rate_limit / github_comment) and the
// redis platform-state face (#37). Each negative control proves the refusal
// (family sentinel error + marker + named tenant + NonRetryable) AND zero
// side effect; each positive control pins the default / bare-engine tenant on
// the historical semantics.

func tenantWriteCtx(t *testing.T, tenant string) context.Context {
	t.Helper()
	return runtime.WithTenantID(gateTestCtx(t, map[string]any{}, nil), tenant)
}

// ── writefile ─────────────────────────────────────────────────────────────

func TestWriteFileRefusesNonDefaultTenant(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "out", "result.json")
	node := &WriteFile{
		BaseNode:      BaseNode{ID: "w1"},
		Path:          target,
		Content:       "tenant payload",
		WorkspaceRoot: root,
	}
	_, err := node.Execute(tenantWriteCtx(t, "acme"))
	assertTenantRefusal(t, err, target, "writefile")
}

func TestWriteFileDefaultTenantStillWrites(t *testing.T) {
	for _, tenant := range []string{"", runtime.DefaultTenantID} {
		root := t.TempDir()
		target := filepath.Join(root, "out", "result.json")
		node := &WriteFile{
			BaseNode:      BaseNode{ID: "w1"},
			Path:          target,
			Content:       "ok",
			WorkspaceRoot: root,
		}
		if _, err := node.Execute(tenantWriteCtx(t, tenant)); err != nil {
			t.Fatalf("tenant %q: Execute: %v", tenant, err)
		}
		if _, err := os.Stat(target); err != nil {
			t.Fatalf("tenant %q: writefile must write for the default tenant: %v", tenant, err)
		}
	}
}

// ── git_publish ───────────────────────────────────────────────────────────

func TestGitPublishRefusesNonDefaultTenant(t *testing.T) {
	wt, remote := gitPublishSetupRepo(t)
	gitPublishWriteDirtyFile(t, wt, "tenant work\n")
	baseHead := gitPublishLocalHead(t, wt)
	node := &GitPublish{
		BaseNode:    BaseNode{ID: "gp1"},
		WorktreeDir: wt,
		BranchName:  "feature",
		IssueNumber: 42,
	}
	_, err := node.Execute(tenantWriteCtx(t, "acme"))
	requireTenantRefusalError(t, err, "git_publish")
	// Zero-execution proof: no commit (HEAD unchanged, worktree still dirty),
	// no push (branch never reached the remote).
	if got := gitPublishLocalHead(t, wt); got != baseHead {
		t.Errorf("HEAD moved %s -> %s for a refused tenant", baseHead, got)
	}
	if status := gitPublishTestGit(t, wt, "status", "--porcelain"); !strings.Contains(status, "feature.txt") {
		t.Errorf("worktree state changed for a refused tenant: %q", status)
	}
	if head := gitPublishTestRemoteHead(t, remote, "feature"); head != "" {
		t.Errorf("branch %q reached the remote for a refused tenant", head)
	}
}

func TestGitPublishDefaultTenantStillPushes(t *testing.T) {
	wt, remote := gitPublishSetupRepo(t)
	gitPublishTestGit(t, wt, "checkout", "-b", "feature")
	gitPublishWriteDirtyFile(t, wt, "work\n")
	for _, tenant := range []string{"", runtime.DefaultTenantID} {
		node := &GitPublish{
			BaseNode:    BaseNode{ID: "gp1"},
			WorktreeDir: wt,
			BranchName:  "feature",
			IssueNumber: 42,
		}
		res, err := node.Execute(tenantWriteCtx(t, tenant))
		if err != nil {
			t.Fatalf("tenant %q: Execute: %v", tenant, err)
		}
		if out := res.Result.(map[string]any); out["pushed"] != true {
			t.Errorf("tenant %q: pushed = %v (%v)", tenant, out["pushed"], out["note"])
		}
		if head := gitPublishTestRemoteHead(t, remote, "feature"); head == "" {
			t.Fatalf("tenant %q: branch must reach the remote for the default tenant", tenant)
		}
		// Second tenant round-trips on the now-pushed branch: still pushed=true
		// (idempotent skip), the gate is the only thing under test here.
	}
}

// ── report ────────────────────────────────────────────────────────────────

func TestReportRefusesNonDefaultTenant(t *testing.T) {
	base := t.TempDir()
	sentinel := filepath.Join(base, ".flowcast", "plaita-reports", "run1.jsonl")
	writeNode := &Report{
		BaseNode: BaseNode{ID: "r1"},
		Action:   "write",
		Repo:     base,
		Token:    "run1",
		Entry:    map[string]any{"status": "ok"},
	}
	_, err := writeNode.Execute(tenantWriteCtx(t, "acme"))
	assertTenantRefusal(t, err, sentinel, "report")

	readNode := &Report{
		BaseNode: BaseNode{ID: "r2"},
		Action:   "read",
		Repo:     base,
		Token:    "run1",
	}
	_, err = readNode.Execute(tenantWriteCtx(t, "acme"))
	requireTenantRefusalError(t, err, "report")
}

func TestReportDefaultTenantStillWrites(t *testing.T) {
	for _, tenant := range []string{"", runtime.DefaultTenantID} {
		base := t.TempDir()
		node := &Report{
			BaseNode: BaseNode{ID: "r1"},
			Action:   "write",
			Repo:     base,
			Token:    "run1",
			Entry:    map[string]any{"status": "ok"},
		}
		if _, err := node.Execute(tenantWriteCtx(t, tenant)); err != nil {
			t.Fatalf("tenant %q: Execute: %v", tenant, err)
		}
		if _, err := os.Stat(filepath.Join(base, ".flowcast", "plaita-reports", "run1.jsonl")); err != nil {
			t.Fatalf("tenant %q: report channel must be written for the default tenant: %v", tenant, err)
		}
	}
}

// ── rate_limit ────────────────────────────────────────────────────────────

func TestRateLimitRefusesNonDefaultTenant(t *testing.T) {
	store := t.TempDir()
	sentinel := filepath.Join(store, "tenant-key.json")
	for _, action := range []string{"record", "acquire", "clear"} {
		node := &RateLimit{
			BaseNode: BaseNode{ID: "rl1"},
			Action:   action,
			Key:      "tenant-key",
			StoreDir: store,
		}
		_, err := node.Execute(tenantWriteCtx(t, "acme"))
		assertTenantRefusal(t, err, sentinel, "rate_limit")
	}
}

func TestRateLimitDefaultTenantStillRecords(t *testing.T) {
	for _, tenant := range []string{"", runtime.DefaultTenantID} {
		store := t.TempDir()
		node := &RateLimit{
			BaseNode: BaseNode{ID: "rl1"},
			Action:   "record",
			Key:      "tenant-key",
			StoreDir: store,
		}
		if _, err := node.Execute(tenantWriteCtx(t, tenant)); err != nil {
			t.Fatalf("tenant %q: Execute: %v", tenant, err)
		}
		if _, err := os.Stat(filepath.Join(store, "tenant-key.json")); err != nil {
			t.Fatalf("tenant %q: counter must be written for the default tenant: %v", tenant, err)
		}
	}
}

// ── github_comment ────────────────────────────────────────────────────────

func TestGithubCommentRefusesNonDefaultTenantBeforeDryRunArtifact(t *testing.T) {
	// The dry-run path still writes the artifact draft to the host — a host
	// write face — so the gate must fire before the dry-run short-circuit.
	artifact := t.TempDir()
	sentinel := filepath.Join(artifact, "reply.md")
	node := &GithubComment{
		BaseNode:    BaseNode{ID: "gc1"},
		Repo:        "acme-org/repo",
		IssueNumber: 1,
		Text:        "hello",
		ArtifactDir: artifact,
		DryRun:      true,
	}
	_, err := node.Execute(tenantWriteCtx(t, "acme"))
	assertTenantRefusal(t, err, sentinel, "github_comment")
}

func TestGithubCommentRefusesNonDefaultTenantBeforeTokenRead(t *testing.T) {
	// The host GH_TOKEN/GITHUB_TOKEN read face sits behind the same gate: with
	// no token in the environment a pre-gate node would fail with the missing
	// credential error; the refusal must win instead.
	t.Setenv("GH_TOKEN", "")
	t.Setenv("GITHUB_TOKEN", "")
	node := &GithubComment{
		BaseNode:    BaseNode{ID: "gc2"},
		Repo:        "acme-org/repo",
		IssueNumber: 1,
		Text:        "hello",
	}
	_, err := node.Execute(tenantWriteCtx(t, "acme"))
	requireTenantRefusalError(t, err, "github_comment")
}

func TestGithubCommentDefaultTenantReachesTokenFace(t *testing.T) {
	t.Setenv("GH_TOKEN", "")
	t.Setenv("GITHUB_TOKEN", "")
	node := &GithubComment{
		BaseNode:    BaseNode{ID: "gc3"},
		Repo:        "acme-org/repo",
		IssueNumber: 1,
		Text:        "hello",
	}
	for _, tenant := range []string{"", runtime.DefaultTenantID} {
		_, err := node.Execute(tenantWriteCtx(t, tenant))
		if err == nil || !strings.Contains(err.Error(), "GH_TOKEN") {
			t.Fatalf("tenant %q: err = %v, want the missing-credential error (gate passed, token face reached)", tenant, err)
		}
		if errors.Is(err, ErrTenantIsolationUnavailable) {
			t.Fatalf("tenant %q: default tenant must not be tenant-refused", tenant)
		}
	}
}

// ── redis (#37) ───────────────────────────────────────────────────────────

func TestRedisNodeRefusesNonDefaultTenantWithoutAllowlist(t *testing.T) {
	mr := miniredis.RunT(t)
	platformKey := "plaita:execution:checkpoint:other-tenant"
	mr.Set(platformKey, "state")
	target := "redis://" + mr.Addr() + "/0"
	cmdsBefore := mr.CommandCount()

	for _, tc := range []struct {
		name       string
		command    string
		arguments  string
		wantbidden string // value that must NOT change
	}{
		{"get platform key", "get", platformKey, "state"},
		{"keys platform space", "keys", "*", "state"},
		{"set platform key", "set", platformKey + " hacked", "state"},
		{"del platform key", "del", platformKey, "state"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := &RedisNode{
				BaseNode:  BaseNode{ID: "redis"},
				Target:    target,
				Command:   tc.command,
				Arguments: tc.arguments,
			}
			_, err := node.Execute(tenantWriteCtx(t, "acme"))
			requireTenantRefusalError(t, err, "redis")
			// Sentinel: the platform key is untouched, nothing new appeared,
			// and the server saw ZERO commands from the refused tenant.
			if got, gerr := mr.Get(platformKey); gerr != nil || got != tc.wantbidden {
				t.Errorf("platform key changed for a refused tenant: %q (err=%v)", got, gerr)
			}
			if mr.Exists(platformKey + "-new") {
				t.Errorf("refused tenant created keys in the platform space")
			}
			if got := mr.CommandCount(); got != cmdsBefore {
				t.Errorf("redis server served %d commands for a refused tenant (want 0)", got-cmdsBefore)
			}
		})
	}

	// The gate precedes transport entirely: even an injected client (DI hook
	// that bypasses target resolution) must not serve a non-default tenant.
	client, _ := redismock.NewClientMock()
	injected := &RedisNode{
		BaseNode:  BaseNode{ID: "redis"},
		Target:    target,
		Command:   "get",
		Arguments: platformKey,
		client:    client,
	}
	_, err := injected.Execute(tenantWriteCtx(t, "acme"))
	requireTenantRefusalError(t, err, "redis")
}

func TestRedisNodeDefaultTenantUnaffectedByGate(t *testing.T) {
	// No allowlist configured: the default / bare-engine tenant keeps the
	// historical semantics (the redismock suite below is the execution proof).
	for _, tenant := range []string{"", runtime.DefaultTenantID} {
		client, mock := redismock.NewClientMock()
		mock.ExpectGet("k").SetVal("v")
		node := &RedisNode{
			BaseNode:  BaseNode{ID: "redis"},
			Target:    "redis://root:123456@localhost:6379/1",
			Command:   "get",
			Arguments: "k",
			client:    client,
		}
		got, err := node.Execute(tenantWriteCtx(t, tenant))
		if err != nil {
			t.Fatalf("tenant %q: Execute: %v", tenant, err)
		}
		if got.Result != "v" {
			t.Errorf("tenant %q: Result = %v, want v", tenant, got.Result)
		}
	}
}

// The allowlist is the positive path of the #37 gate: a deployer explicitly
// declares the Redis instances a tenant-scoped flow may reach; matching is on
// host:port/db identity (credentials excluded, case-insensitive, bare
// host:port entries accepted).
func TestRedisNodeNonDefaultTenantAllowlistedTargetRuns(t *testing.T) {
	mr := miniredis.RunT(t)
	mr.Set("k", "v")
	SetRedisTargetAllowlist([]string{
		"redis://JOE:secret@" + strings.ToUpper(mr.Addr()) + "/0", // creds + case noise on the entry side
		"redis://elsewhere:6379/0",
	})
	t.Cleanup(func() { SetRedisTargetAllowlist(nil) })

	node := &RedisNode{
		BaseNode:  BaseNode{ID: "redis"},
		Target:    "redis://" + mr.Addr() + "/0",
		Command:   "get",
		Arguments: "k",
	}
	got, err := node.Execute(tenantWriteCtx(t, "acme"))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got.Result != "v" {
		t.Errorf("Result = %v, want v", got.Result)
	}
}

func TestRedisNodeNonDefaultTenantTargetNotOnAllowlistRefused(t *testing.T) {
	mr := miniredis.RunT(t)
	mr.Set("k", "v")
	SetRedisTargetAllowlist([]string{"redis://elsewhere:6379/0"})
	t.Cleanup(func() { SetRedisTargetAllowlist(nil) })

	node := &RedisNode{
		BaseNode:  BaseNode{ID: "redis"},
		Target:    "redis://" + mr.Addr() + "/0",
		Command:   "get",
		Arguments: "k",
	}
	_, err := node.Execute(tenantWriteCtx(t, "acme"))
	requireTenantRefusalError(t, err, "redis")
	if v, _ := mr.Get("k"); v != "v" {
		t.Errorf("platform key changed for a refused tenant: %q", v)
	}
}

// PLAITA_REDIS_TARGET_ALLOWLIST is the env surface of the same declaration;
// the process-level override wins over it.
func TestRedisNodeAllowlistEnvAndOverridePrecedence(t *testing.T) {
	mr := miniredis.RunT(t)
	mr.Set("k", "v")
	t.Setenv(redisTargetAllowlistEnv, "redis://elsewhere:6379/0, "+mr.Addr()+"/0  ")

	node := &RedisNode{
		BaseNode:  BaseNode{ID: "redis"},
		Target:    "redis://" + mr.Addr() + "/0",
		Command:   "get",
		Arguments: "k",
	}
	if _, err := node.Execute(tenantWriteCtx(t, "acme")); err != nil {
		t.Fatalf("env-configured allowlist must admit the target: %v", err)
	}

	SetRedisTargetAllowlist([]string{"redis://elsewhere:6379/0"})
	t.Cleanup(func() { SetRedisTargetAllowlist(nil) })
	_, err := node.Execute(tenantWriteCtx(t, "acme"))
	requireTenantRefusalError(t, err, "redis")
}
