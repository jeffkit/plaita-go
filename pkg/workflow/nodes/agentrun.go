package nodes

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/agentproc"
	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// AgentRun runs one agentproc turn: spawn the configured bridge, deliver
// the evaluated prompt as the turn message, assemble the reply from the
// NDJSON event stream. Mirrors plaita-nodes' AgentRunNode contract
// (node type "agentrun"); output keys are identical so downstream
// $NODE.<id>.text / .session_id / .usage resolve the same way.
//
// Differences from the Python node, both deliberate:
//   - workspace (sandbox branch) is rejected at load time — the AGS sandbox
//     driver is not ported to this engine;
//   - the bridge runs on the agentproc spawn path (explicit command/args in
//     the agents.json entry), sharing the same hub bridges as Python.
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
	if a.Workspace != nil && a.Repo != nil {
		return fmt.Errorf("agentrun %q: workspace and repo are mutually exclusive", a.ID)
	}
	return nil
}

// Execute evaluates the expression fields and runs one agentproc turn.
func (a *AgentRun) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	evalString := func(v any) (string, error) {
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

	agentName := "glm-52"
	if a.Agent != nil {
		v, err := evalString(a.Agent)
		if err != nil {
			return nil, err
		}
		if v != "" {
			agentName = v
		}
	}
	prompt, err := evalString(a.Prompt)
	if err != nil {
		return nil, err
	}
	repo, err := evalString(a.Repo)
	if err != nil {
		return nil, err
	}
	sessionID, err := evalString(a.Session)
	if err != nil {
		return nil, err
	}

	if a.Workspace != nil {
		return nil, fmt.Errorf("agentrun %q: workspace (sandbox branch) is not supported by this engine", a.ID)
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
		out["details"] = runResult.Details
	}
	return result(a, out), nil
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
