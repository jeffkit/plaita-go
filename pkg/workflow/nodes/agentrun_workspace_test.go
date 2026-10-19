package nodes

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/jeffkit/plaita-go/pkg/agentproc"
	"github.com/jeffkit/plaita-go/pkg/sandbox"
)

type fakeEnsureCall struct {
	ExecutionID string
	WSKey       string
	Spec        *sandbox.WorkspaceSpec
}

type fakeInject struct {
	Path string
	Data []byte
	Mode int64
}

type fakeSandboxDriver struct {
	mu sync.Mutex

	ensureCalls  []fakeEnsureCall
	execCalls    []sandbox.ExecOptions
	injects      []fakeInject
	removed      []string
	releaseCalls []sandbox.ReleaseOptions
	enforces     int

	ensureErr   error
	execResult  *sandbox.ExecResult
	execErr     error
	releaseErr  error
	injectError error
}

func (f *fakeSandboxDriver) Name() string { return "fake" }

func (f *fakeSandboxDriver) Ensure(_ context.Context, spec *sandbox.WorkspaceSpec, executionID, wsKey string) (*sandbox.WorkspaceHandle, error) {
	f.mu.Lock()
	f.ensureCalls = append(f.ensureCalls, fakeEnsureCall{ExecutionID: executionID, WSKey: wsKey, Spec: spec})
	f.mu.Unlock()
	if f.ensureErr != nil {
		return nil, f.ensureErr
	}
	return &sandbox.WorkspaceHandle{
		Driver:      "fake",
		ID:          sandbox.HandleID(executionID, wsKey),
		Path:        sandbox.SandboxPath,
		Key:         wsKey,
		ExecutionID: executionID,
		Data:        map[string]string{"datadir": "/tmp/fake", "container": "fake-ctr"},
	}, nil
}

func (f *fakeSandboxDriver) Exec(_ context.Context, _ *sandbox.WorkspaceHandle, opts sandbox.ExecOptions) (*sandbox.ExecResult, error) {
	f.mu.Lock()
	f.execCalls = append(f.execCalls, opts)
	f.mu.Unlock()
	if f.execErr != nil {
		return nil, f.execErr
	}
	if f.execResult != nil {
		return f.execResult, nil
	}
	return &sandbox.ExecResult{ExitCode: 0, Stdout: `{"result":"ok"}`}, nil
}

func (f *fakeSandboxDriver) InjectFile(_ context.Context, _ *sandbox.WorkspaceHandle, containerPath string, data []byte, mode int64) error {
	f.mu.Lock()
	f.injects = append(f.injects, fakeInject{Path: containerPath, Data: append([]byte(nil), data...), Mode: mode})
	f.mu.Unlock()
	return f.injectError
}

func (f *fakeSandboxDriver) RemoveFile(_ context.Context, _ *sandbox.WorkspaceHandle, containerPath string) error {
	f.mu.Lock()
	f.removed = append(f.removed, containerPath)
	f.mu.Unlock()
	return nil
}

func (f *fakeSandboxDriver) Enforce(context.Context, *sandbox.WorkspaceHandle) error {
	f.mu.Lock()
	f.enforces++
	f.mu.Unlock()
	return nil
}

func (f *fakeSandboxDriver) Release(_ context.Context, _ *sandbox.WorkspaceHandle, opts sandbox.ReleaseOptions) error {
	f.mu.Lock()
	f.releaseCalls = append(f.releaseCalls, opts)
	f.mu.Unlock()
	return f.releaseErr
}

func sandboxTestHome(t *testing.T, sandboxes string) string {
	t.Helper()
	home := t.TempDir()
	if err := os.WriteFile(filepath.Join(home, "sandboxes.json"), []byte(sandboxes), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PLAITA_HOME", home)
	return home
}

func sandboxAgentRegistry(t *testing.T, agents string) *agentproc.Registry {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "agents.json"), []byte(agents), 0o600); err != nil {
		t.Fatal(err)
	}
	return &agentproc.Registry{Dir: dir}
}

const fakeSandboxRegistry = `{"sandboxes": {
	"sbx": {"driver": "fake", "image": "img", "allow_unpinned": true,
	         "env": {"SANDBOX_FLAG": "on"}}
}}`

const recursiveSandboxAgent = `{"agents": {
	"runner": {"executor": "recursive-direct", "model": "m1",
	           "env": {"RECURSIVE_API_KEY": "secret-token-value", "RECURSIVE_MODEL": "m1"}}
}}`

func TestAgentRunSandboxRecursiveDirect(t *testing.T) {
	home := sandboxTestHome(t, fakeSandboxRegistry)
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)

	driver := &fakeSandboxDriver{execResult: &sandbox.ExecResult{
		ExitCode: 0,
		Stdout:   `{"result":"sandbox-reply","session_id":"s-1","usage":{"input_tokens":2}}`,
	}}
	node := &AgentRun{
		BaseNode:      BaseNode{ID: "ag"},
		Agent:         "runner",
		Prompt:        "do the thing",
		Workspace:     "sbx",
		Registry:      sandboxAgentRegistry(t, recursiveSandboxAgent),
		SandboxDriver: func(string) (sandbox.Driver, error) { return driver, nil },
	}
	res, err := node.Execute(agentRunContext(t, map[string]any{}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["text"] != "sandbox-reply" {
		t.Errorf("text = %v", out["text"])
	}
	if out["session_id"] != "s-1" {
		t.Errorf("session_id = %v", out["session_id"])
	}
	if out["cli"] != "runner" {
		t.Errorf("cli = %v", out["cli"])
	}
	snapshot, ok := out["workspace"].(map[string]any)
	if !ok {
		t.Fatalf("workspace snapshot = %T %v", out["workspace"], out["workspace"])
	}
	if snapshot["ws_key"] != "sbx" || snapshot["driver"] != "fake" || snapshot["path"] != "/work" {
		t.Errorf("snapshot = %v", snapshot)
	}
	names, _ := snapshot["env_names"].([]string)
	if !containsString(names, "RECURSIVE_API_KEY") || !containsString(names, "SANDBOX_FLAG") {
		t.Errorf("env_names = %v", names)
	}

	if len(driver.ensureCalls) != 1 {
		t.Fatalf("ensure calls = %d", len(driver.ensureCalls))
	}
	if driver.ensureCalls[0].WSKey != "sbx" || driver.ensureCalls[0].ExecutionID == "" {
		t.Errorf("ensure call = %+v", driver.ensureCalls[0])
	}
	if len(driver.execCalls) != 1 {
		t.Fatalf("exec calls = %d", len(driver.execCalls))
	}
	exec := driver.execCalls[0]
	if exec.WorkDir != "/work" {
		t.Errorf("workdir = %q", exec.WorkDir)
	}
	if exec.EnvFile == "" {
		t.Errorf("no envfile option")
	}
	if !containsArgPair(exec.Cmd, "--workspace", "/work") {
		t.Errorf("argv = %v", exec.Cmd)
	}
	if !containsArgPair(exec.Cmd, "--output-format", "json") {
		t.Errorf("argv = %v", exec.Cmd)
	}
	if !containsString(exec.Cmd, "run") || !containsString(exec.Cmd, "do the thing") {
		t.Errorf("argv = %v", exec.Cmd)
	}

	// envfile injected 0600 with the secret, then burned locally.
	var envInject *fakeInject
	for i := range driver.injects {
		if strings.Contains(string(driver.injects[i].Data), "RECURSIVE_API_KEY=") {
			envInject = &driver.injects[i]
		}
	}
	if envInject == nil {
		t.Fatalf("envfile was not injected: %+v", driver.injects)
	}
	if envInject.Mode != 0o600 {
		t.Errorf("envfile mode = %o", envInject.Mode)
	}
	content := string(envInject.Data)
	for _, want := range []string{
		"RECURSIVE_API_KEY=secret-token-value",
		"RECURSIVE_WORKSPACE=/work",
		"SANDBOX_FLAG=on",
	} {
		if !strings.Contains(content, want) {
			t.Errorf("envfile missing %q:\n%s", want, content)
		}
	}
	if !containsString(driver.removed, envInject.Path) {
		t.Errorf("container envfile not removed: %v", driver.removed)
	}
	leftovers, _ := filepath.Glob(filepath.Join(tmp, "plaita-sandbox-env-*"))
	if len(leftovers) != 0 {
		t.Errorf("host envfile not burned: %v", leftovers)
	}

	if len(driver.releaseCalls) != 1 || driver.releaseCalls[0].Failed || !driver.releaseCalls[0].KeepData {
		t.Errorf("release = %+v", driver.releaseCalls)
	}
	if driver.enforces != 0 {
		t.Errorf("enforce called on a successful turn")
	}

	// The workspace lease is released after the turn.
	leaseFiles, _ := filepath.Glob(filepath.Join(home, "sandbox-leases", "*.lease"))
	if len(leaseFiles) != 0 {
		t.Errorf("lease not released: %v", leaseFiles)
	}
}

func TestAgentRunSandboxRedaction(t *testing.T) {
	sandboxTestHome(t, fakeSandboxRegistry)
	t.Setenv("TMPDIR", t.TempDir())

	driver := &fakeSandboxDriver{execResult: &sandbox.ExecResult{
		ExitCode: 0,
		Stdout:   `{"result":"leaked secret-token-value","session_id":"s"}`,
	}}
	node := &AgentRun{
		BaseNode:      BaseNode{ID: "ag"},
		Agent:         "runner",
		Prompt:        "p",
		Workspace:     "sbx",
		Registry:      sandboxAgentRegistry(t, recursiveSandboxAgent),
		SandboxDriver: func(string) (sandbox.Driver, error) { return driver, nil },
	}
	res, err := node.Execute(agentRunContext(t, map[string]any{}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	text := res.Result.(map[string]any)["text"].(string)
	if strings.Contains(text, "secret-token-value") {
		t.Fatalf("secret leaked into text: %q", text)
	}
	if !strings.Contains(text, "[REDACTED:RECURSIVE_API_KEY]") {
		t.Errorf("text not redacted: %q", text)
	}
}

func TestAgentRunSandboxFailureReleasesWithMarker(t *testing.T) {
	sandboxTestHome(t, fakeSandboxRegistry)
	t.Setenv("TMPDIR", t.TempDir())

	driver := &fakeSandboxDriver{execErr: errors.New("exec failed: secret-token-value")}
	node := &AgentRun{
		BaseNode:      BaseNode{ID: "ag"},
		Agent:         "runner",
		Prompt:        "p",
		Workspace:     "sbx",
		Registry:      sandboxAgentRegistry(t, recursiveSandboxAgent),
		SandboxDriver: func(string) (sandbox.Driver, error) { return driver, nil },
	}
	_, err := node.Execute(agentRunContext(t, map[string]any{}))
	if err == nil {
		t.Fatal("expected failure")
	}
	if strings.Contains(err.Error(), "secret-token-value") {
		t.Fatalf("secret leaked into error: %v", err)
	}
	if driver.enforces != 1 {
		t.Errorf("enforce calls = %d", driver.enforces)
	}
	if len(driver.releaseCalls) != 1 {
		t.Fatalf("release calls = %d", len(driver.releaseCalls))
	}
	release := driver.releaseCalls[0]
	if !release.Failed || !release.KeepData {
		t.Errorf("release = %+v", release)
	}
	if strings.Contains(release.FailureReason, "secret-token-value") {
		t.Errorf("secret leaked into retention marker: %q", release.FailureReason)
	}
}

func TestAgentRunSandboxNonZeroExit(t *testing.T) {
	sandboxTestHome(t, fakeSandboxRegistry)
	t.Setenv("TMPDIR", t.TempDir())

	driver := &fakeSandboxDriver{execResult: &sandbox.ExecResult{
		ExitCode: 2, Stderr: "boom secret-token-value",
	}}
	node := &AgentRun{
		BaseNode:      BaseNode{ID: "ag"},
		Agent:         "runner",
		Prompt:        "p",
		Workspace:     "sbx",
		Registry:      sandboxAgentRegistry(t, recursiveSandboxAgent),
		SandboxDriver: func(string) (sandbox.Driver, error) { return driver, nil },
	}
	_, err := node.Execute(agentRunContext(t, map[string]any{}))
	if err == nil {
		t.Fatal("expected failure")
	}
	if strings.Contains(err.Error(), "secret-token-value") || !strings.Contains(err.Error(), "[REDACTED:") {
		t.Errorf("stderr not redacted: %v", err)
	}
	if driver.enforces != 1 || len(driver.releaseCalls) != 1 || !driver.releaseCalls[0].Failed {
		t.Errorf("enforce/release = %d/%+v", driver.enforces, driver.releaseCalls)
	}
}

func TestAgentRunEmptyWorkspaceUsesHostPath(t *testing.T) {
	sandboxTestHome(t, fakeSandboxRegistry)
	driver := &fakeSandboxDriver{}
	node := &AgentRun{
		BaseNode:      BaseNode{ID: "ag"},
		Agent:         "echo",
		Prompt:        "host path",
		Workspace:     "",
		Registry:      agentRegistryForTest(t),
		SandboxDriver: func(string) (sandbox.Driver, error) { return driver, nil },
	}
	res, err := node.Execute(agentRunContext(t, map[string]any{}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got := res.Result.(map[string]any)["text"]; got != "reply-from-agent" {
		t.Errorf("text = %v", got)
	}
	if len(driver.ensureCalls)+len(driver.execCalls) != 0 {
		t.Errorf("empty workspace touched the sandbox driver")
	}
}

func TestAgentRunSandboxUnregisteredFailsClosed(t *testing.T) {
	sandboxTestHome(t, fakeSandboxRegistry)
	driver := &fakeSandboxDriver{}
	node := &AgentRun{
		BaseNode:      BaseNode{ID: "ag"},
		Agent:         "runner",
		Prompt:        "p",
		Workspace:     "nope",
		Registry:      sandboxAgentRegistry(t, recursiveSandboxAgent),
		SandboxDriver: func(string) (sandbox.Driver, error) { return driver, nil },
	}
	_, err := node.Execute(agentRunContext(t, map[string]any{}))
	if err == nil || !errors.Is(err, sandbox.ErrNotRegistered) {
		t.Fatalf("err = %v, want ErrNotRegistered", err)
	}
	if len(driver.ensureCalls) != 0 {
		t.Errorf("ensure called for an unregistered workspace")
	}
}

func TestAgentRunSandboxEmptyWorkspaceRejected(t *testing.T) {
	sandboxTestHome(t, fakeSandboxRegistry)
	driver := &fakeSandboxDriver{}
	node := &AgentRun{
		BaseNode:      BaseNode{ID: "ag"},
		Agent:         "runner",
		Prompt:        "p",
		Workspace:     "$INPUT.ws",
		Registry:      sandboxAgentRegistry(t, recursiveSandboxAgent),
		SandboxDriver: func(string) (sandbox.Driver, error) { return driver, nil },
	}
	_, err := node.Execute(agentRunContext(t, map[string]any{"ws": ""}))
	if err == nil || !strings.Contains(err.Error(), "empty name") {
		t.Fatalf("err = %v", err)
	}
	if len(driver.ensureCalls) != 0 {
		t.Errorf("ensure called for an empty workspace")
	}
}

func TestAgentRunSandboxDryRunZeroCalls(t *testing.T) {
	sandboxTestHome(t, fakeSandboxRegistry)
	driver := &fakeSandboxDriver{}
	node := &AgentRun{
		BaseNode:      BaseNode{ID: "ag"},
		Agent:         "runner",
		Prompt:        "p",
		Workspace:     "sbx",
		DryRun:        true,
		Registry:      sandboxAgentRegistry(t, recursiveSandboxAgent),
		SandboxDriver: func(string) (sandbox.Driver, error) { return driver, nil },
	}
	res, err := node.Execute(agentRunContext(t, map[string]any{}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !strings.HasPrefix(res.Result.(map[string]any)["text"].(string), "[dry-run]") {
		t.Errorf("result = %v", res.Result)
	}
	if len(driver.ensureCalls)+len(driver.execCalls)+len(driver.injects)+len(driver.releaseCalls) != 0 {
		t.Errorf("dry run touched the driver: %+v", driver)
	}
}

func TestAgentRunSandboxBridgeProtocol(t *testing.T) {
	sandboxTestHome(t, fakeSandboxRegistry)
	t.Setenv("TMPDIR", t.TempDir())

	driver := &fakeSandboxDriver{execResult: &sandbox.ExecResult{
		ExitCode: 0,
		Stdout: strings.Join([]string{
			`{"type":"partial","text":"hello "}`,
			`{"type":"result","text":"hello world","session_id":"proto-1","usage":{"output_tokens":1}}`,
		}, "\n"),
	}}
	agents := `{"agents": {
		"bridge": {"executor": "claude-code", "command": "/usr/local/bin/bridge", "args": ["--flag"],
		           "env": {"ANTHROPIC_AUTH_TOKEN": "secret-token-value"}}
	}}`
	node := &AgentRun{
		BaseNode:      BaseNode{ID: "ag"},
		Agent:         "bridge",
		Prompt:        "turn message",
		Workspace:     "sbx",
		Registry:      sandboxAgentRegistry(t, agents),
		SandboxDriver: func(string) (sandbox.Driver, error) { return driver, nil },
	}
	res, err := node.Execute(agentRunContext(t, map[string]any{}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["text"] != "hello " {
		t.Errorf("text = %q (streaming partials win)", out["text"])
	}
	if out["session_id"] != "proto-1" {
		t.Errorf("session_id = %v", out["session_id"])
	}
	exec := driver.execCalls[0]
	if exec.Cmd[0] != "/usr/local/bin/bridge" || exec.Cmd[1] != "--flag" {
		t.Errorf("argv = %v", exec.Cmd)
	}
	if exec.StdinFile == "" {
		t.Fatal("bridge path must inject the turn on stdin")
	}
	var stdinData string
	for _, inject := range driver.injects {
		if inject.Path == exec.StdinFile {
			stdinData = string(inject.Data)
		}
	}
	if !strings.Contains(stdinData, `"type":"turn"`) || !strings.Contains(stdinData, "turn message") {
		t.Errorf("turn payload = %q", stdinData)
	}
}

func TestAgentRunSandboxProvisionBranchExpression(t *testing.T) {
	sandboxTestHome(t, `{"sandboxes": {
		"sbx": {"driver": "fake", "image": "img", "allow_unpinned": true,
		        "provision": {"git": {"repo": "https://example.com/r.git", "branch": "$INPUT.branch"}}}
	}}`)
	t.Setenv("TMPDIR", t.TempDir())

	driver := &fakeSandboxDriver{}
	node := &AgentRun{
		BaseNode:      BaseNode{ID: "ag"},
		Agent:         "runner",
		Prompt:        "p",
		Workspace:     "sbx",
		Registry:      sandboxAgentRegistry(t, recursiveSandboxAgent),
		SandboxDriver: func(string) (sandbox.Driver, error) { return driver, nil },
	}
	if _, err := node.Execute(agentRunContext(t, map[string]any{"branch": "release"})); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(driver.ensureCalls) != 1 {
		t.Fatalf("ensure calls = %d", len(driver.ensureCalls))
	}
	if got := driver.ensureCalls[0].Spec.GitBranch(); got != "release" {
		t.Errorf("branch = %q", got)
	}
}

func TestAgentRunSandboxExecutionScopeIsStable(t *testing.T) {
	sandboxTestHome(t, fakeSandboxRegistry)
	t.Setenv("TMPDIR", t.TempDir())

	driver := &fakeSandboxDriver{execResult: &sandbox.ExecResult{ExitCode: 0, Stdout: `{"result":"ok"}`}}
	node := &AgentRun{
		BaseNode:      BaseNode{ID: "ag"},
		Agent:         "runner",
		Prompt:        "p",
		Workspace:     "sbx",
		Registry:      sandboxAgentRegistry(t, recursiveSandboxAgent),
		SandboxDriver: func(string) (sandbox.Driver, error) { return driver, nil },
	}
	input := map[string]any{"same": "input"}
	if _, err := node.Execute(agentRunContext(t, input)); err != nil {
		t.Fatalf("first: %v", err)
	}
	if _, err := node.Execute(agentRunContext(t, input)); err != nil {
		t.Fatalf("second: %v", err)
	}
	if len(driver.ensureCalls) != 2 {
		t.Fatalf("ensure calls = %d", len(driver.ensureCalls))
	}
	if driver.ensureCalls[0].ExecutionID == "" ||
		driver.ensureCalls[0].ExecutionID != driver.ensureCalls[1].ExecutionID {
		t.Errorf("execution scope not stable: %q vs %q",
			driver.ensureCalls[0].ExecutionID, driver.ensureCalls[1].ExecutionID)
	}
}

func TestAgentRunSandboxCredentialResolver(t *testing.T) {
	sandboxTestHome(t, `{"sandboxes": {
		"sbx": {"driver": "fake", "image": "img", "allow_unpinned": true,
		        "env": {"GIT_TOKEN": {"credential": "repo", "field": "token"}}}
	}}`)
	t.Setenv("TMPDIR", t.TempDir())

	driver := &fakeSandboxDriver{}
	node := &AgentRun{
		BaseNode:      BaseNode{ID: "ag"},
		Agent:         "runner",
		Prompt:        "p",
		Workspace:     "sbx",
		Registry:      sandboxAgentRegistry(t, recursiveSandboxAgent),
		SandboxDriver: func(string) (sandbox.Driver, error) { return driver, nil },
		SandboxCredentials: func(credential, field string) (string, error) {
			if credential != "repo" || field != "token" {
				return "", errors.New("unexpected credential reference")
			}
			return "credential-value", nil
		},
	}
	if _, err := node.Execute(agentRunContext(t, map[string]any{})); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	found := false
	for _, inject := range driver.injects {
		if strings.Contains(string(inject.Data), "GIT_TOKEN=credential-value") {
			found = true
		}
	}
	if !found {
		t.Errorf("resolved credential missing from envfile: %+v", driver.injects)
	}
}

func TestAgentRunSandboxCredentialFailClosed(t *testing.T) {
	sandboxTestHome(t, `{"sandboxes": {
		"sbx": {"driver": "fake", "image": "img", "allow_unpinned": true,
		        "env": {"GIT_TOKEN": {"credential": "repo"}}}
	}}`)
	driver := &fakeSandboxDriver{}
	node := &AgentRun{
		BaseNode:      BaseNode{ID: "ag"},
		Agent:         "runner",
		Prompt:        "p",
		Workspace:     "sbx",
		Registry:      sandboxAgentRegistry(t, recursiveSandboxAgent),
		SandboxDriver: func(string) (sandbox.Driver, error) { return driver, nil },
	}
	_, err := node.Execute(agentRunContext(t, map[string]any{}))
	if err == nil || !errors.Is(err, sandbox.ErrConfig) {
		t.Fatalf("err = %v, want ErrConfig", err)
	}
	if len(driver.ensureCalls) != 0 {
		t.Errorf("ensure called before credential resolution failed")
	}
}

// TestAgentRunSandboxLive exercises the full node path against a real Docker
// daemon: registry → lease → envfile tar injection → bridge exec in the
// container → reply parse → release. Gated because CI has no Docker.
//
//	PLAITA_SANDBOX_LIVE=1 \
//	PLAITA_DOCKER_HOST=unix:///path/to/docker.sock \
//	PLAITA_SANDBOX_LIVE_IMAGE=debian:bookworm-slim \
//	go test ./pkg/workflow/nodes -run TestAgentRunSandboxLive
func TestAgentRunSandboxLive(t *testing.T) {
	if os.Getenv("PLAITA_SANDBOX_LIVE") != "1" {
		t.Skip("set PLAITA_SANDBOX_LIVE=1 to run against a real Docker daemon")
	}
	image := os.Getenv("PLAITA_SANDBOX_LIVE_IMAGE")
	socket := strings.TrimPrefix(os.Getenv("PLAITA_DOCKER_HOST"), "unix://")
	if image == "" || socket == "" {
		t.Skip("set PLAITA_SANDBOX_LIVE_IMAGE and PLAITA_DOCKER_HOST")
	}
	if _, err := os.Stat(socket); err != nil {
		t.Skipf("docker socket unavailable: %v", err)
	}

	dataRoot := filepath.Join("/tmp", fmt.Sprintf("plaita-agentrun-live-%d", os.Getpid()))
	_ = os.RemoveAll(dataRoot)
	t.Cleanup(func() { _ = os.RemoveAll(dataRoot) })
	t.Setenv("PLAITA_DOCKER_DATA_ROOT", dataRoot)

	home := sandboxTestHome(t, fmt.Sprintf(`{"sandboxes": {
		"live-sbx": {"driver": "docker", "image": %q, "allow_unpinned": true, "egress": "none"}
	}}`, image))
	t.Setenv("TMPDIR", t.TempDir())
	// Keep the lease store in the test home.
	t.Setenv("PLAITA_HOME", home)

	agents := `{"agents": {
		"live": {"executor": "claude-code", "model": "live-model",
		         "command": "/bin/sh",
		         "args": ["-c", "cat >/dev/null; printf '%s\\n' '{\"type\":\"result\",\"text\":\"live-ok\",\"session_id\":\"live-s\"}'"],
		         "env": {"ANTHROPIC_AUTH_TOKEN": "live-secret-token"}}
	}}`
	node := &AgentRun{
		BaseNode:  BaseNode{ID: "ag-live"},
		Agent:     "live",
		Prompt:    "hello live sandbox",
		Workspace: "live-sbx",
		Registry:  sandboxAgentRegistry(t, agents),
	}
	res, err := node.Execute(agentRunContext(t, map[string]any{}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["text"] != "live-ok" {
		t.Errorf("text = %v", out["text"])
	}
	snapshot, _ := out["workspace"].(map[string]any)
	if snapshot["driver"] != "docker" || snapshot["ws_key"] != "live-sbx" {
		t.Errorf("snapshot = %v", snapshot)
	}
	if releaseErr, ok := snapshot["release_error"]; ok {
		t.Errorf("release error = %v", releaseErr)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func containsArgPair(argv []string, first, second string) bool {
	for i := 0; i+1 < len(argv); i++ {
		if argv[i] == first && argv[i+1] == second {
			return true
		}
	}
	return false
}
