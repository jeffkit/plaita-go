package nodes

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// nodeTypeCapture mirrors plaita-nodes CaptureNode.node_type ("capture").
const nodeTypeCapture types.NodeType = "capture"

func init() {
	workflow.RegisterNodeType(
		nodeTypeCapture,
		func(ctx context.Context) workflow.Node { return new(Capture) },
	)
}

// captureDefaultTimeoutSecs mirrors plaita-nodes CaptureNode.timeout_secs.
const captureDefaultTimeoutSecs = 120.0

// Capture runs a local command and captures its output. Mirrors
// plaita-nodes CaptureNode (capture.py):
//
//   - command failure is NOT a node failure — the output dict
//     {"ok", "exit_code", "stdout", "stderr", "dry_run"} lets the flow branch
//     on the result; only configuration errors (missing/empty command) are
//     node errors (CaptureConfigError parity);
//   - timeout (default 120s, positive_seconds fail-closed like gate) kills
//     the whole process group and keeps the historical exit_code=124;
//   - the child env is rebuilt from the host whitelist (the shared
//     gateBuildSubprocessEnv — plaita.subprocess_env parity), so host
//     credentials never reach the executed command; the “env“ field is the
//     explicitly declared extra;
//   - stdout/stderr are head+tail clipped past 4000/2000 chars (node output
//     lands in checkpoints / event streams — a full dump would leak to
//     low-privilege viewers);
//   - dry_run (or globalContext.dry_run) returns a fake success without
//     spawning anything.
//
// Divergences from the Python node, forced by the runtime:
//   - cancellation rides the Go context instead of cancel_event: a cancelled
//     capture is an error after the process group is killed.
//
// Tenant boundary (pn#18 / A42-06): the command runs on the worker host, as
// the worker user, with the tenant-wide credentials key — so a non-default
// tenant is refused before any spawn (ErrTenantIsolationUnavailable). Dry runs
// stay allowed (nothing executes); the default tenant keeps historical
// semantics.
type Capture struct {
	BaseNode
	Command     any  `json:"command,omitempty"`      // string (shlex split / list-valued expression) or list
	Cwd         any  `json:"cwd,omitempty"`          // expression; default = process cwd
	TimeoutSecs any  `json:"timeout_secs,omitempty"` // seconds; literal validated in Validate, expressions re-checked in Execute
	Env         any  `json:"env,omitempty"`          // expression evaluating to a map of extra env vars
	Stdin       any  `json:"stdin,omitempty"`        // expression; text piped to the child's stdin
	DryRun      bool `json:"dry_run,omitempty"`
}

// GetType returns the capture node type.
func (c *Capture) GetType() workflow.NodeType { return nodeTypeCapture }

// Validate checks the build-time contract: a literal timeout_secs must
// already be a positive number of seconds. Expression strings cannot be
// evaluated here and are re-checked right after evaluation in Execute —
// same split as the Python field_validator (A2R7-05).
func (c *Capture) Validate() error {
	if c.TimeoutSecs == nil {
		return nil
	}
	if s, ok := c.TimeoutSecs.(string); ok && strings.Contains(s, "$") {
		return nil // deferred expression: fail-closed check happens post-eval
	}
	_, err := gatePositiveSeconds("capture timeout_secs", c.TimeoutSecs)
	return err
}

// Execute evaluates the expression fields and runs the command.
func (c *Capture) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	if c.Command == nil {
		return nil, fmt.Errorf("capture %q: 节点缺少 command 字段", c.ID)
	}
	cmd, err := c.buildArgv(ctx)
	if err != nil {
		return nil, err
	}
	if len(cmd) == 0 {
		return nil, fmt.Errorf("capture %q: 命令为空", c.ID)
	}
	cwd, err := c.evalCwd(ctx)
	if err != nil {
		return nil, err
	}
	timeout, err := c.evalTimeout(ctx)
	if err != nil {
		return nil, err
	}
	var stdinText *string
	if c.Stdin != nil {
		v, err := expression.Eval(ctx, c.Stdin)
		if err != nil {
			return nil, fmt.Errorf("capture %q: eval stdin: %w", c.ID, err)
		}
		s := gateToStr(v) // Python str(evaluate(stdin)); nil renders as "None"
		stdinText = &s
	}

	dry := c.DryRun
	if rc := runtime.WorkflowRuntimeContext(ctx); rc != nil {
		if gv := rc.GetGlobalVariable("dry_run"); isTruthy(gv) {
			dry = true
		}
	}
	if dry {
		return &workflow.ExecuteResult{Result: map[string]any{
			"ok":        true,
			"exit_code": 0,
			"stdout":    "[dry-run] would exec: " + strings.Join(cmd, " "),
			"stderr":    "",
			"dry_run":   true,
		}, NextNode: c.NextNode()}, nil
	}

	// Tenant gate (pn#18 / A42-06), after the dry-run short-circuit and before
	// the child env is assembled or any spawn happens: the command would run as
	// the worker user with host file-system access and could read the default
	// tenant's credentials. capture.py refuses at the same point.
	if err := refuseNonDefaultTenant(ctx, "capture"); err != nil {
		return nil, err
	}

	// Whitelist-rebuilt env: host credentials stay out of the child; the
	// ``env`` field is the explicitly declared extra (plaita.subprocess_env).
	extra, err := c.evalExtraEnv(ctx)
	if err != nil {
		return nil, err
	}
	env := gateBuildSubprocessEnv(extra)

	exitCode, stdout, stderr, err := captureRunOnce(ctx, cmd, cwd, env, timeout, stdinText)
	if err != nil {
		return nil, err
	}
	return &workflow.ExecuteResult{Result: map[string]any{
		"ok":        exitCode == 0,
		"exit_code": exitCode,
		"stdout":    gateClipOutput(stdout, gateStdoutCap), // caps = capture.py _STDOUT_CAP/_STDERR_CAP
		"stderr":    gateClipOutput(stderr, gateStderrCap),
		"dry_run":   false,
	}, NextNode: c.NextNode()}, nil
}

// buildArgv evaluates the command. A list field is element-wise evaluated
// (mixed literals/expressions); a non-list field is evaluated once and either
// yields a list (used as argv directly) or a string that is shlex split
// (capture.py execute(), first branch).
func (c *Capture) buildArgv(ctx context.Context) ([]string, error) {
	if els, ok := c.Command.([]any); ok {
		cmd := make([]string, 0, len(els))
		for _, el := range els {
			v, err := expression.Eval(ctx, el)
			if err != nil {
				return nil, fmt.Errorf("capture %q: eval command: %w", c.ID, err)
			}
			cmd = append(cmd, gateToStr(v))
		}
		return cmd, nil
	}
	v, err := expression.Eval(ctx, c.Command)
	if err != nil {
		return nil, fmt.Errorf("capture %q: eval command: %w", c.ID, err)
	}
	if lst, ok := v.([]any); ok {
		// e.g. "$NODE.x.argv" evaluating to a list
		cmd := make([]string, 0, len(lst))
		for _, x := range lst {
			cmd = append(cmd, gateToStr(x))
		}
		return cmd, nil
	}
	argv, err := gateShlexSplit(gateToStr(v))
	if err != nil {
		return nil, fmt.Errorf("capture %q: %w", c.ID, err)
	}
	return argv, nil
}

// evalCwd evaluates cwd; Python checks the FIELD's truthiness before
// evaluation (`evaluate(self.cwd) if self.cwd else os.getcwd()`), so a nil or
// empty cwd field means the process cwd (empty Dir in Go).
func (c *Capture) evalCwd(ctx context.Context) (string, error) {
	if !isTruthy(c.Cwd) {
		return "", nil
	}
	v, err := expression.Eval(ctx, c.Cwd)
	if err != nil {
		return "", fmt.Errorf("capture %q: eval cwd: %w", c.ID, err)
	}
	return gateToStr(v), nil
}

// evalTimeout evaluates timeout_secs; nil evaluates to the 120s default and
// the evaluated value is fail-closed validated (0/negative rejected — A2R7-05).
func (c *Capture) evalTimeout(ctx context.Context) (time.Duration, error) {
	raw := any(captureDefaultTimeoutSecs)
	if c.TimeoutSecs != nil {
		v, err := expression.Eval(ctx, c.TimeoutSecs)
		if err != nil {
			return 0, fmt.Errorf("capture %q: eval timeout_secs: %w", c.ID, err)
		}
		if v != nil {
			raw = v
		}
	}
	secs, err := gatePositiveSeconds("capture timeout_secs", raw)
	if err != nil {
		return 0, err
	}
	return time.Duration(secs * float64(time.Second)), nil
}

// evalExtraEnv evaluates the env field into an explicit extra map. Python:
// `{str(k): str(v) for k, v in (evaluate(self.env) or {}).items()}` — a nil
// evaluation is empty; a non-map evaluation is an attribute error there and a
// named error here.
func (c *Capture) evalExtraEnv(ctx context.Context) (map[string]string, error) {
	if c.Env == nil {
		return nil, nil
	}
	v, err := expression.Eval(ctx, c.Env)
	if err != nil {
		return nil, fmt.Errorf("capture %q: eval env: %w", c.ID, err)
	}
	m, ok := v.(map[string]any)
	if !ok {
		if v == nil {
			return nil, nil
		}
		return nil, fmt.Errorf("capture %q: env must evaluate to a dict, got %T", c.ID, v)
	}
	extra := make(map[string]string, len(m))
	for k, val := range m {
		extra[gateToStr(k)] = gateToStr(val)
	}
	return extra, nil
}

// captureRunOnce spawns the command in its own process group and waits with
// capture, mirroring capture.py Popen(start_new_session=True) +
// communicate_cancellable: an own timeout keeps the historical
// exit_code=124 with whatever output was captured; cancellation of the
// parent context kills the process group first and is an error
// (NodeCancelledError parity); a spawn failure (command not found) is an
// error, matching the Python Popen raising. stdinText is nil when the child
// gets no stdin pipe, otherwise it is written and closed (communicate(input=…)).
func captureRunOnce(ctx context.Context, argv []string, cwd string, env []string, timeout time.Duration, stdinText *string) (int, string, string, error) {
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(runCtx, argv[0], argv[1:]...)
	cmd.Dir = cwd
	cmd.Env = env
	// start_new_session=True equivalent: the child leads its own process
	// group so cancellation/timeout can kill the whole tree.
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
	if stdinText != nil {
		cmd.Stdin = strings.NewReader(*stdinText)
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	runErr := cmd.Run()

	if ctx.Err() != nil {
		return 0, "", "", fmt.Errorf("capture: cancelled via context; process tree killed")
	}
	if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
		return 124, stdout.String(), stderr.String(), nil
	}
	if runErr != nil {
		var ee *exec.ExitError
		if errors.As(runErr, &ee) {
			return ee.ExitCode(), stdout.String(), stderr.String(), nil
		}
		if errors.Is(runErr, exec.ErrWaitDelay) {
			return 0, stdout.String(), stderr.String(), nil
		}
		return 0, "", "", fmt.Errorf("capture: %w", runErr)
	}
	return 0, stdout.String(), stderr.String(), nil
}
