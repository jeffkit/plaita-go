package nodes

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/agentproc"
	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/sandbox"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// AgentRun runs one agentproc turn: spawn the configured bridge, deliver
// the evaluated prompt as the turn message, assemble the reply from the
// NDJSON event stream. Mirrors plaita-nodes' AgentRunNode contract
// (node type "agentrun"); output keys are identical so downstream
// $NODE.<id>.text / .session_id / .usage resolve the same way.
//
// Two execution paths:
//   - repo / bare: the agentproc runner spawns the bridge on the host
//     (explicit command/args in the agents.json entry, same hub bridges as
//     Python);
//   - workspace: the workspace name resolves through .plaita/sandboxes.json,
//     a Docker driver ensures the sandbox, and the same executable argv runs
//     inside the container. Credentials travel only through a 0600 envfile
//     that is burned after the turn.
type AgentRun struct {
	BaseNode
	Agent       any                 `json:"agent,omitempty"`
	Prompt      any                 `json:"prompt,omitempty"`
	Repo        any                 `json:"repo,omitempty"`
	Workspace   any                 `json:"workspace,omitempty"`
	Session     any                 `json:"session,omitempty"`
	TimeoutSecs any                 `json:"timeout_secs,omitempty"`
	Details     bool                `json:"details,omitempty"`
	DryRun      bool                `json:"dry_run,omitempty"`
	Registry    *agentproc.Registry `json:"-"`

	// SandboxDriver resolves a driver by name; nil uses the sandbox package
	// registry (docker). Tests inject a fake through this hook.
	SandboxDriver func(name string) (sandbox.Driver, error) `json:"-"`
	// SandboxCredentials resolves sandbox registry {"credential": …} env
	// references. A nil resolver is fail-closed: credential refs cannot be
	// silently dropped.
	SandboxCredentials func(credential, field string) (string, error) `json:"-"`
}

func init() {
	workflow.RegisterNodeType(
		types.NodeTypeAgentRun,
		func(ctx context.Context) workflow.Node { return new(AgentRun) },
	)
}

// defaultTimeoutSecs mirrors plaita-nodes AgentRunNode.timeout_secs.
const defaultTimeoutSecs = 1800

// GetType returns the node type.
func (a *AgentRun) GetType() workflow.NodeType { return types.NodeTypeAgentRun }

// Validate checks the build-time contract: workspace (sandbox) and repo
// (host run) are mutually exclusive.
func (a *AgentRun) Validate() error {
	if presentLiteral(a.Workspace) && presentLiteral(a.Repo) {
		return fmt.Errorf("agentrun %q: workspace and repo are mutually exclusive", a.ID)
	}
	return nil
}

// presentLiteral treats nil and an empty/blank literal string as "field not
// set"; expressions are always present (their value is runtime data).
func presentLiteral(value any) bool {
	if value == nil {
		return false
	}
	if text, ok := value.(string); ok {
		return strings.TrimSpace(text) != ""
	}
	return true
}

// Execute evaluates the expression fields and runs one agentproc turn.
func (a *AgentRun) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	agentName := "glm-52"
	if a.Agent != nil {
		v, err := a.evalString(ctx, a.Agent)
		if err != nil {
			return nil, err
		}
		if v != "" {
			agentName = v
		}
	}
	prompt, err := a.evalString(ctx, a.Prompt)
	if err != nil {
		return nil, err
	}
	repo, err := a.evalString(ctx, a.Repo)
	if err != nil {
		return nil, err
	}
	sessionID, err := a.evalString(ctx, a.Session)
	if err != nil {
		return nil, err
	}

	timeout := defaultTimeoutSecs
	if a.TimeoutSecs != nil {
		v, err := expression.Eval(ctx, a.TimeoutSecs)
		if err != nil {
			return nil, err
		}
		if n, err := toInt(v); err == nil && n > 0 {
			timeout = n
		}
	}

	// dry_run is decided before workspace resolution, credential/env
	// assembly and any driver call (design §7.7: dry runs prove zero side
	// effects).
	dry := a.DryRun
	if rc := runtime.WorkflowRuntimeContext(ctx); rc != nil {
		if g := rc.GetGlobalVariable("dry_run"); g != nil {
			if b, ok := g.(bool); ok && b {
				dry = true
			}
		}
	}
	if dry {
		preview := prompt
		if len(preview) > 80 {
			preview = preview[:80] + "…"
		}
		return result(a, map[string]any{
			"text": "[dry-run] " + agentName + " would run: " + preview,
			"cli":  agentName, "model": nil, "session_id": "", "usage": nil, "dry_run": true,
		}), nil
	}

	registry := a.Registry
	if registry == nil {
		registry = agentproc.NewRegistry()
	}
	spec, err := registry.ResolveAgent(agentName)
	if err != nil {
		return nil, fmt.Errorf("agentrun %q: %w", a.ID, err)
	}

	if presentLiteral(a.Workspace) {
		if repo != "" {
			return nil, fmt.Errorf("agentrun %q: workspace and repo are mutually exclusive", a.ID)
		}
		return a.executeSandboxed(ctx, agentName, prompt, sessionID, spec, timeout)
	}

	// Python-node parity: the recursive bridge takes its workspace from
	// RECURSIVE_WORKSPACE (setdefault semantics — an explicit entry env wins).
	if repo != "" {
		if _, exists := spec.Env["RECURSIVE_WORKSPACE"]; !exists {
			spec.Env["RECURSIVE_WORKSPACE"] = repo
		}
	}
	// details=true on the recursive executor: switch the CLI to stream-json so
	// the event stream (tool calls / text turns) is parseable into
	// observations — the plain single-object output carries no tool events.
	// plaita-nodes parity: want_details && executor == "recursive" ⇒
	// RECURSIVE_OUTPUT_FORMAT = "stream-json" (explicit override).
	if a.Details && spec.Mode == agentproc.ModeRecursiveDirect {
		spec.Env["RECURSIVE_OUTPUT_FORMAT"] = "stream-json"
	}

	runResult, err := agentproc.Run(ctx, spec, agentproc.RunOptions{
		Message:        prompt,
		SessionID:      sessionID,
		Dir:            repo,
		Timeout:        time.Duration(timeout) * time.Second,
		Streaming:      true,
		CollectDetails: a.Details,
	})
	if err != nil {
		return nil, fmt.Errorf("agentrun %q: %w", a.ID, err)
	}

	out := map[string]any{
		"text":       runResult.Text,
		"cli":        agentName,
		"model":      spec.Model,
		"session_id": runResult.SessionID,
		"usage":      usageOrNil(runResult.Usage),
	}
	if a.Details {
		// Python output contract (agent_run.py:644): details=true exposes
		// `observations` — the parsed {type:"span"/"generation",...} list —
		// never raw protocol lines. The recursive-direct path always parses
		// (its reply IS the stream-json transcript); the protocol path only
		// when the bridge actually emitted lines.
		if spec.Mode == agentproc.ModeRecursiveDirect || len(runResult.Details) > 0 {
			out["observations"] = parseStreamDetails(runResult.Details)
		}
	}
	return result(a, out), nil
}

// evalString evaluates one expression-typed node field; nil stays "".
func (a *AgentRun) evalString(ctx context.Context, v any) (string, error) {
	if v == nil {
		return "", nil
	}
	ev, err := expression.Eval(ctx, v)
	if err != nil {
		return "", err
	}
	if ev == nil {
		return "", nil
	}
	s, ok := ev.(string)
	if !ok {
		return "", fmt.Errorf("agentrun %q: expected a string value, got %T", a.ID, ev)
	}
	return s, nil
}

// executeSandboxed is the workspace branch. Order is discipline (sandbox.py
// _execute_sandboxed): registry fail-closed → lease fast-fail → credential /
// env whitelist → envfile(0600) → ensure (idempotent provision) → exec →
// enforce on failure → burn envfile + release.
func (a *AgentRun) executeSandboxed(ctx context.Context, agentName, prompt, sessionID string,
	agentSpec *agentproc.AgentSpec, timeout int) (*workflow.ExecuteResult, error) {
	wsValue, err := expression.Eval(ctx, a.Workspace)
	if err != nil {
		return nil, err
	}
	wsKey, ok := wsValue.(string)
	wsKey = strings.TrimSpace(wsKey)
	if !ok || wsKey == "" {
		return nil, fmt.Errorf("agentrun %q: workspace evaluated to %v; an empty name is rejected "+
			"(every iteration would share one sandbox)", a.ID, wsValue)
	}

	specs, err := sandbox.LoadSandboxes("")
	if err != nil {
		return nil, fmt.Errorf("agentrun %q: %w", a.ID, err)
	}
	wsSpec, registered := specs[wsKey]
	if !registered {
		return nil, fmt.Errorf("%w: workspace %q (fail-closed: define it in .plaita/sandboxes.json "+
			"and reference it by name)", sandbox.ErrNotRegistered, wsKey)
	}
	if err := a.resolveProvisionBranch(ctx, wsSpec); err != nil {
		return nil, err
	}

	driver, err := a.resolveSandboxDriver(wsSpec.Driver)
	if err != nil {
		return nil, fmt.Errorf("agentrun %q: %w", a.ID, err)
	}
	env, err := a.sandboxEnv(agentSpec, wsSpec)
	if err != nil {
		return nil, fmt.Errorf("agentrun %q: %w", a.ID, err)
	}
	// The host workspace path is meaningless inside the container; the
	// sandbox path is stable across rebuilds (design §4).
	env["RECURSIVE_WORKSPACE"] = sandbox.SandboxPath
	if a.Details && agentSpec.Mode == agentproc.ModeRecursiveDirect {
		env["RECURSIVE_OUTPUT_FORMAT"] = "stream-json"
	}
	redactor := sandbox.NewRedactor(env)

	executionID := sandbox.ExecutionIDFromContext(ctx)
	if executionID == "" {
		// The worker does not thread execution_id into the engine runtime yet;
		// hash the node + input so a retried carrier re-attaches (documented
		// deviation from Python, which requires execution.execution_id).
		executionID = sandbox.DeriveExecutionID(ctx, string(a.ID))
	}

	leaseTTL := time.Duration(timeout+60) * time.Second
	lease := sandbox.NewWorkspaceLease(sandbox.DefaultLeaseStore(),
		sandbox.HandleID(executionID, wsKey), leaseTTL)
	if err := lease.Acquire(); err != nil {
		return nil, fmt.Errorf("agentrun %q: %w", a.ID, err)
	}
	lease.StartHeartbeat()
	defer lease.Release()

	envPath, err := sandbox.WriteEnvFile(env)
	if err != nil {
		return nil, fmt.Errorf("agentrun %q: %w", a.ID, err)
	}
	defer sandbox.BurnEnvFile(envPath)

	handle, err := driver.Ensure(ctx, wsSpec, executionID, wsKey)
	if err != nil {
		return nil, fmt.Errorf("agentrun %q: sandbox ensure: %s", a.ID, redactor.Redact(err.Error()))
	}

	out, runErr := a.runSandboxTurn(ctx, driver, handle, executionID, envPath, env,
		agentName, prompt, sessionID, agentSpec, timeout, redactor)

	releaseCtx, cancelRelease := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	releaseOpts := sandbox.ReleaseOptions{KeepData: true}
	if runErr != nil {
		releaseOpts.Failed = true
		releaseOpts.FailureReason = redactor.Redact(runErr.Error())
	}
	releaseErr := driver.Release(releaseCtx, handle, releaseOpts)
	cancelRelease()
	if runErr != nil {
		if releaseErr != nil {
			return nil, fmt.Errorf("%w (sandbox release also failed: %v)", runErr, releaseErr)
		}
		return nil, runErr
	}
	if snapshot, ok := out["workspace"].(map[string]any); ok {
		if releaseErr != nil {
			snapshot["release_error"] = redactor.Redact(releaseErr.Error())
		}
		// Surface a lost lease instead of swallowing it: the turn completed,
		// but another worker may have believed the workspace was free.
		if lease.Lost() {
			snapshot["lease_lost"] = true
		}
	}
	return result(a, out), nil
}

func (a *AgentRun) resolveSandboxDriver(name string) (sandbox.Driver, error) {
	if name == "" {
		name = "docker"
	}
	if a.SandboxDriver != nil {
		driver, err := a.SandboxDriver(name)
		if err != nil {
			return nil, err
		}
		if driver == nil {
			return nil, fmt.Errorf("%w: %q (factory returned nil)", sandbox.ErrDriverMissing, name)
		}
		return driver, nil
	}
	driver, err := sandbox.GetDriver(name)
	if err != nil {
		return nil, fmt.Errorf("%w (available: %s)", err, strings.Join(sandbox.DriverNames(), ", "))
	}
	return driver, nil
}

// sandboxEnv builds the whitelist: the agent profile's provider/env
// translation plus the registry entry's env. Host process env is NOT
// inherited — the container sees exactly this map.
func (a *AgentRun) sandboxEnv(agentSpec *agentproc.AgentSpec, wsSpec *sandbox.WorkspaceSpec) (map[string]string, error) {
	env := make(map[string]string, len(agentSpec.Env)+len(wsSpec.Env))
	for key, value := range agentSpec.Env {
		env[key] = value
	}
	for key, raw := range wsSpec.Env {
		value, err := a.resolveSandboxEnvValue(key, raw)
		if err != nil {
			return nil, err
		}
		env[key] = value
	}
	if err := sandbox.ValidateEnv(env); err != nil {
		return nil, err
	}
	return env, nil
}

func (a *AgentRun) resolveSandboxEnvValue(key string, raw any) (string, error) {
	switch value := raw.(type) {
	case nil:
		return "", nil
	case string:
		return value, nil
	case bool, float64, int, int64:
		return fmt.Sprint(value), nil
	case map[string]any:
		if credential, _ := value["credential"].(string); credential != "" {
			field, _ := value["field"].(string)
			if field == "" {
				field = "token"
			}
			if a.SandboxCredentials == nil {
				return "", fmt.Errorf("%w: env %q references credential %q but no credential "+
					"resolver is wired (fail-closed)", sandbox.ErrConfig, key, credential)
			}
			resolved, err := a.SandboxCredentials(credential, field)
			if err != nil {
				return "", fmt.Errorf("sandbox credential %q field %q: %w", credential, field, err)
			}
			return resolved, nil
		}
		if envName, _ := value["env"].(string); envName != "" {
			resolved, present := os.LookupEnv(envName)
			if !present {
				return "", fmt.Errorf("%w: env %q references missing host environment variable %q",
					sandbox.ErrConfig, key, envName)
			}
			return resolved, nil
		}
		return "", fmt.Errorf("%w: env %q has an unsupported object value", sandbox.ErrConfig, key)
	default:
		return "", fmt.Errorf("%w: env %q has unsupported type %T", sandbox.ErrConfig, key, raw)
	}
}

// resolveProvisionBranch evaluates provision.git.branch in the referencing
// node's context (design §4). The expression must be deterministic by
// authoring contract; ensure idempotence makes the first evaluation stick.
func (a *AgentRun) resolveProvisionBranch(ctx context.Context, spec *sandbox.WorkspaceSpec) error {
	git, _ := spec.Provision["git"].(map[string]any)
	if git == nil {
		return nil
	}
	raw, present := git["branch"]
	if !present || raw == nil {
		return nil
	}
	branch, ok := raw.(string)
	if !ok {
		return fmt.Errorf("agentrun %q: provision.git.branch must be a string, got %T", a.ID, raw)
	}
	value, err := expression.Eval(ctx, branch)
	if err != nil {
		return fmt.Errorf("agentrun %q: provision.git.branch: %w", a.ID, err)
	}
	if value == nil {
		delete(git, "branch")
		return nil
	}
	text, ok := value.(string)
	if !ok {
		return fmt.Errorf("agentrun %q: provision.git.branch must evaluate to a string, got %T",
			a.ID, value)
	}
	if strings.TrimSpace(text) == "" {
		delete(git, "branch")
		return nil
	}
	git["branch"] = strings.TrimSpace(text)
	return nil
}

// runSandboxTurn injects the envfile, executes the agent argv, parses the
// reply and cleans the injected files. The caller owns ensure/release.
func (a *AgentRun) runSandboxTurn(ctx context.Context, driver sandbox.Driver, handle *sandbox.WorkspaceHandle,
	executionID, envPath string, env map[string]string, agentName, prompt, sessionID string,
	agentSpec *agentproc.AgentSpec, timeout int, redactor *sandbox.Redactor) (map[string]any, error) {
	envContent, err := os.ReadFile(envPath)
	if err != nil {
		return nil, fmt.Errorf("agentrun %q: read sandbox envfile: %w", a.ID, err)
	}
	envContainer := containerTempPath("env")
	if err := driver.InjectFile(ctx, handle, envContainer, envContent, 0o600); err != nil {
		return nil, fmt.Errorf("agentrun %q: inject envfile: %s", a.ID, redactor.Redact(err.Error()))
	}
	stdinContainer := ""
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		_ = driver.RemoveFile(cleanupCtx, handle, envContainer)
		if stdinContainer != "" {
			_ = driver.RemoveFile(cleanupCtx, handle, stdinContainer)
		}
		cancel()
	}()

	var argv []string
	if agentSpec.Mode == agentproc.ModeRecursiveDirect {
		argv = sandbox.RecursiveDirectArgv(env, prompt, sessionID)
	} else {
		if agentSpec.Command == "" {
			return nil, fmt.Errorf("agentrun %q: executor %q has no command for the sandbox path",
				a.ID, agentSpec.Executor)
		}
		argv = append([]string{agentSpec.Command}, agentSpec.Args...)
		turn, marshalErr := json.Marshal(map[string]any{
			"type":             "turn",
			"message":          prompt,
			"session_id":       sessionID,
			"protocol_version": "0.4",
		})
		if marshalErr != nil {
			return nil, marshalErr
		}
		stdinContainer = containerTempPath("stdin")
		if err := driver.InjectFile(ctx, handle, stdinContainer, append(turn, '\n'), 0o600); err != nil {
			return nil, fmt.Errorf("agentrun %q: inject turn: %s", a.ID, redactor.Redact(err.Error()))
		}
	}

	result, err := driver.Exec(ctx, handle, sandbox.ExecOptions{
		Cmd: argv,
		Env: map[string]string{
			"PLAITA_EXECUTION_ID": executionID,
			"PLAITA_WORKSPACE":    handle.Key,
			"PLAITA_NODE_ID":      string(a.ID),
		},
		WorkDir:   handle.Path,
		EnvFile:   envContainer,
		StdinFile: stdinContainer,
		Timeout:   time.Duration(timeout)*time.Second + 60*time.Second,
	})
	if err != nil {
		a.enforceSandbox(driver, handle)
		return nil, fmt.Errorf("agentrun %q: sandbox exec: %s", a.ID, redactor.Redact(err.Error()))
	}

	stdout := redactor.Redact(result.Stdout)
	stderr := redactor.Redact(result.Stderr)
	if result.ExitCode != 0 {
		a.enforceSandbox(driver, handle)
		detail := strings.TrimSpace(stderr)
		if detail == "" {
			detail = strings.TrimSpace(stdout)
		}
		return nil, fmt.Errorf("agentrun %q: sandbox command exited %d: %s",
			a.ID, result.ExitCode, truncate(detail, 400))
	}

	hasOutput := strings.TrimSpace(stdout) != ""
	var text string
	var usage map[string]any
	var turnSession string
	var observations []map[string]any
	if agentSpec.Mode == agentproc.ModeRecursiveDirect {
		text, usage, turnSession, observations, err = interpretRecursiveOutput(stdout, a.Details)
	} else {
		text, usage, turnSession, observations, err = interpretProtocolOutput(stdout, a.Details)
	}
	if err != nil {
		a.enforceSandbox(driver, handle)
		return nil, fmt.Errorf("agentrun %q: %s", a.ID, redactor.Redact(err.Error()))
	}
	if turnSession == "" {
		turnSession = sessionID
	}
	if a.Details {
		redactObservations(observations, redactor)
	}

	model := agentSpec.Model
	if model == "" {
		model = env["RECURSIVE_MODEL"]
	}
	if model == "" {
		model = env["CLAUDE_MODEL"]
	}
	if model == "" {
		model = agentName
	}
	out := map[string]any{
		"text":       text,
		"cli":        agentName,
		"model":      model,
		"session_id": turnSession,
		"usage":      usageOrNil(usage),
		"workspace":  handle.Snapshot(envNames(env)),
	}
	if a.Details && (agentSpec.Mode == agentproc.ModeRecursiveDirect || hasOutput) {
		out["observations"] = observations
	}
	return out, nil
}

func (a *AgentRun) enforceSandbox(driver sandbox.Driver, handle *sandbox.WorkspaceHandle) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = driver.Enforce(ctx, handle)
}

func containerTempPath(kind string) string {
	var random [8]byte
	_, _ = rand.Read(random[:])
	return "/tmp/plaita-sandbox-" + kind + "-" + hex.EncodeToString(random[:]) + ".tmp"
}

func envNames(env map[string]string) []string {
	names := make([]string, 0, len(env))
	for key := range env {
		names = append(names, key)
	}
	return names
}

func truncate(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	return text[:limit] + "…"
}

func redactObservations(observations []map[string]any, redactor *sandbox.Redactor) {
	for _, observation := range observations {
		for key, value := range observation {
			observation[key] = redactValue(value, redactor)
		}
	}
}

func redactValue(value any, redactor *sandbox.Redactor) any {
	switch v := value.(type) {
	case string:
		return redactor.Redact(v)
	case map[string]any:
		for key := range v {
			v[key] = redactValue(v[key], redactor)
		}
		return v
	case []any:
		for i := range v {
			v[i] = redactValue(v[i], redactor)
		}
		return v
	case []map[string]any:
		for _, item := range v {
			for key := range item {
				item[key] = redactValue(item[key], redactor)
			}
		}
		return v
	default:
		return value
	}
}

// interpretRecursiveOutput applies the Python _interpret_result recursive
// branch: the trailing JSON object is the reply; is_error fails the turn;
// usage falls back to the object's own usage; details parses the transcript.
func interpretRecursiveOutput(stdout string, details bool) (string, map[string]any, string, []map[string]any, error) {
	obj := extractRecursiveResult(stdout)
	if isErr, _ := obj["is_error"].(bool); isErr {
		return "", nil, "", nil, fmt.Errorf("recursive is_error: %s",
			truncate(fmt.Sprint(obj["result"]), 300))
	}
	text := ""
	if value, present := obj["result"]; present && value != nil {
		if str, ok := value.(string); ok {
			text = str
		} else {
			text = fmt.Sprint(value)
		}
	}
	if raw, _ := obj["raw"].(bool); raw {
		text = strings.TrimSpace(text)
	}
	sessionID, _ := obj["session_id"].(string)
	usage, _ := obj["usage"].(map[string]any)
	var observations []map[string]any
	if details {
		observations = parseStreamDetails(strings.Split(stdout, "\n"))
	}
	return text, usage, sessionID, observations, nil
}

// interpretProtocolOutput mirrors agentproc's event state machine for the
// sandbox path (the runner cannot be reused because the process lives in the
// container): partials concatenate, result.text fills when no partial was
// seen, an error event is terminal, first valid session id wins.
func interpretProtocolOutput(stdout string, details bool) (string, map[string]any, string, []map[string]any, error) {
	lines := strings.Split(stdout, "\n")
	var body strings.Builder
	sawPartial := false
	var finalText *string
	terminalErr := ""
	sessionID := ""
	var usage map[string]any
	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		var event map[string]any
		if json.Unmarshal([]byte(line), &event) != nil {
			continue // malformed lines are ignored per the wire spec
		}
		if sessionID == "" {
			if id, _ := event["session_id"].(string); validWireSessionID(id) {
				sessionID = id
			}
		}
		switch event["type"] {
		case "partial":
			if terminalErr != "" {
				continue
			}
			if text, _ := event["text"].(string); text != "" {
				sawPartial = true
				body.WriteString(text)
			}
		case "result":
			if finalText != nil {
				continue
			}
			text, _ := event["text"].(string)
			finalText = &text
			if u, ok := event["usage"].(map[string]any); ok && usage == nil {
				usage = u
			}
		case "error":
			if terminalErr != "" {
				continue
			}
			terminalErr, _ = event["message"].(string)
			if u, ok := event["usage"].(map[string]any); ok && usage == nil {
				usage = u
			}
		}
	}
	if terminalErr != "" {
		return "", usage, sessionID, nil, fmt.Errorf("agent error: %s", terminalErr)
	}
	text := ""
	if sawPartial {
		text = body.String()
	} else if finalText != nil {
		text = *finalText
	}
	var observations []map[string]any
	if details {
		observations = parseStreamDetails(lines)
	}
	return text, usage, sessionID, observations, nil
}

func validWireSessionID(id string) bool {
	if id == "" {
		return false
	}
	for _, r := range id {
		if r == '/' || r == '\\' || r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

// extractRecursiveResult mirrors agentproc's parser (recursive
// --output-format json: line-start objects scanned last-first, then the first
// inline {"…", then raw text). It lives here because the sandbox path reads
// container stdout directly.
func extractRecursiveResult(stdout string) map[string]any {
	var lineStarts []int
	offset := 0
	for _, line := range strings.Split(stdout, "\n") {
		if strings.HasPrefix(strings.TrimLeft(line, " \t"), "{") {
			lineStarts = append(lineStarts, offset)
		}
		offset += len(line) + 1
	}
	scanOrder := make([]int, 0, len(lineStarts)+1)
	for i := len(lineStarts) - 1; i >= 0; i-- {
		scanOrder = append(scanOrder, lineStarts[i])
	}
	if idx := strings.Index(stdout, `{"`); idx >= 0 {
		scanOrder = append(scanOrder, idx)
	}
	for _, start := range scanOrder {
		if obj, ok := balancedJSONAt(stdout, start); ok {
			return obj
		}
	}
	return map[string]any{"result": stdout, "is_error": false, "raw": true}
}

func balancedJSONAt(s string, start int) (map[string]any, bool) {
	if start >= len(s) || s[start] != '{' {
		return nil, false
	}
	depth, inString, escaped := 0, false, false
	for i := start; i < len(s); i++ {
		ch := s[i]
		if inString {
			switch {
			case escaped:
				escaped = false
			case ch == '\\':
				escaped = true
			case ch == '"':
				inString = false
			}
			continue
		}
		switch ch {
		case '"':
			inString = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				var obj map[string]any
				if json.Unmarshal([]byte(s[start:i+1]), &obj) != nil {
					return nil, false
				}
				return obj, true
			}
		}
	}
	return nil, false
}

// detailsCap mirrors plaita-nodes _DETAILS_CAP: an observation ceiling per
// agent run so a runaway session cannot flood the trace.
const detailsCap = 50

// parseStreamDetails ports plaita-nodes parse_stream_details: recursive /
// claude-compatible stream-json NDJSON lines → observations list.
//
//   - assistant tool_use blocks pair with the following user tool_result (by
//     tool_use_id) into {"type":"span","name":"tool:<name>","input","output"};
//   - assistant text blocks become {"type":"generation","name":"turn:<n>",
//     "model","output"} (per-turn usage is not reported by this runtime);
//   - system / stream_event / result lines are skipped; capped at detailsCap.
func parseStreamDetails(lines []string) []map[string]any {
	observations := []map[string]any{}
	pendingTools := map[string]map[string]any{} // tool_use_id → tool_use block
	turnNo := 0
	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			continue
		}
		eventType, _ := event["type"].(string)
		switch eventType {
		case "assistant":
			message, _ := event["message"].(map[string]any)
			content, _ := message["content"].([]any)
			for _, item := range content {
				block, ok := item.(map[string]any)
				if !ok {
					continue
				}
				blockType, _ := block["type"].(string)
				switch blockType {
				case "tool_use":
					id, _ := block["id"].(string)
					pendingTools[id] = block
				case "text":
					text, _ := block["text"].(string)
					if strings.TrimSpace(text) == "" {
						continue
					}
					turnNo++
					observations = append(observations, map[string]any{
						"type": "generation", "name": fmt.Sprintf("turn:%d", turnNo),
						"model": message["model"], "output": text,
					})
				}
			}
		case "user":
			message, _ := event["message"].(map[string]any)
			content, _ := message["content"].([]any)
			for _, item := range content {
				block, ok := item.(map[string]any)
				if !ok {
					continue
				}
				blockType, _ := block["type"].(string)
				if blockType != "tool_result" {
					continue
				}
				toolUseID, _ := block["tool_use_id"].(string)
				toolUse, pending := pendingTools[toolUseID]
				if !pending {
					continue
				}
				delete(pendingTools, toolUseID)
				name, _ := toolUse["name"].(string)
				observations = append(observations, map[string]any{
					"type": "span", "name": "tool:" + name,
					"input":  toolUse["input"],
					"output": toolResultText(block["content"]),
				})
			}
		}
		if len(observations) >= detailsCap {
			break
		}
	}
	if len(observations) > detailsCap {
		observations = observations[:detailsCap]
	}
	return observations
}

// toolResultText renders a tool_result content: a content-block array joins
// its text blocks with "\n" (plaita parity); anything else passes through.
func toolResultText(content any) any {
	blocks, ok := content.([]any)
	if !ok {
		return content
	}
	texts := make([]string, 0, len(blocks))
	for _, item := range blocks {
		block, ok := item.(map[string]any)
		if !ok {
			continue
		}
		blockType, _ := block["type"].(string)
		if blockType != "text" {
			continue
		}
		text, _ := block["text"].(string)
		texts = append(texts, text)
	}
	return strings.Join(texts, "\n")
}

func result(a *AgentRun, v any) *workflow.ExecuteResult {
	return &workflow.ExecuteResult{Result: v, NextNode: a.NextNode()}
}

func usageOrNil(u map[string]any) any {
	if len(u) == 0 {
		return nil
	}
	return u
}

func toInt(v any) (int, error) {
	switch n := v.(type) {
	case int:
		return n, nil
	case int64:
		return int(n), nil
	case float64:
		return int(n), nil
	case json.Number:
		return strconv.Atoi(n.String())
	case string:
		return strconv.Atoi(n)
	default:
		return 0, fmt.Errorf("not a number: %T", v)
	}
}
