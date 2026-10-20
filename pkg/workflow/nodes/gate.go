package nodes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// nodeTypeGate mirrors plaita-nodes GateNode.node_type ("gate").
const nodeTypeGate types.NodeType = "gate"

func init() {
	workflow.RegisterNodeType(
		nodeTypeGate,
		func(ctx context.Context) workflow.Node { return new(Gate) },
	)
}

// Output caps and timing defaults from plaita-nodes gate.py.
const (
	gateStdoutCap          = 4000
	gateStderrCap          = 2000
	gateDefaultTimeoutSecs = 600.0
	// gateDrainTimeout bounds the pipe drain after a kill, mirroring
	// plaita-nodes _cancel._DRAIN_TIMEOUT_SECONDS.
	gateDrainTimeout = time.Second
)

// gateEnvAllowlist is the subprocess env whitelist (plaita.subprocess_env
// _HOST_ALLOWLIST): host credentials never reach the executed command.
// Keys are kept sorted to produce deterministic env slices.
var gateEnvAllowlist = []string{
	"HOME", "LANG", "LC_ALL", "LC_CTYPE", "PATH", "PYTHONIOENCODING", "TMPDIR",
}

// Gate is the quality-gate atom: run a verification command and classify it
// as pass/fail. Mirrors plaita-nodes GateNode:
//
//   - command (string or arg list, elements support {% %} expressions);
//   - gate_name for reports;
//   - timeout_secs (default 600) with fail-closed rejection of 0/negative —
//     a literal is rejected in Validate, an expression is re-checked right
//     after evaluation in Execute (A2R7-05);
//   - max_retries: each retry re-executes the command (A2R8-02: the retry
//     loop also honors cancellation);
//   - dry_run (or globalContext.dry_run) short-circuits to passed=true.
//
// Output: {"passed", "gate", "exit_code", "stdout", "stderr", "retries"
// [, "dry_run"]}. stdout/stderr are head+tail clipped past the caps with an
// explicit omission marker.
//
// Divergences from the Python node, forced by the runtime:
//   - cancellation rides the Go context instead of cancel_event, and a
//     cancelled gate is an error while an own timeout keeps the historical
//     exit_code=124 / passed=false semantics.
//
// Tenant boundary (pn#18 / A42-06): the command runs on the worker host, as
// the worker user, with the tenant-wide credentials key — so a non-default
// tenant is refused before any spawn (ErrTenantIsolationUnavailable). Dry runs
// stay allowed (nothing executes); the default tenant keeps historical
// semantics.
type Gate struct {
	BaseNode
	Command     any               `json:"command,omitempty"`
	GateName    string            `json:"gate_name,omitempty"`
	Cwd         any               `json:"cwd,omitempty"`
	TimeoutSecs any               `json:"timeout_secs,omitempty"`
	MaxRetries  int               `json:"max_retries,omitempty"`
	DryRun      bool              `json:"dry_run,omitempty"`
	EnvExtra    map[string]string `json:"-"`
}

// GetType returns the gate node type.
func (g *Gate) GetType() workflow.NodeType { return nodeTypeGate }

// Validate checks the build-time contract: a literal timeout_secs must
// already be a positive number of seconds. Expression strings (containing
// "$") cannot be evaluated here and are re-checked right after evaluation
// in Execute — same split as the Python field_validator (A2R7-05).
func (g *Gate) Validate() error {
	if g.TimeoutSecs == nil {
		return nil
	}
	if s, ok := g.TimeoutSecs.(string); ok && strings.Contains(s, "$") {
		return nil // deferred expression: fail-closed check happens post-eval
	}
	_, err := gatePositiveSeconds("gate timeout_secs", g.TimeoutSecs)
	return err
}

// Execute evaluates the expression fields and runs the verification command.
func (g *Gate) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	if g.Command == nil {
		return nil, fmt.Errorf("gate %q: 节点缺少 command", g.ID)
	}
	argv, err := g.buildArgv(ctx)
	if err != nil {
		return nil, err
	}
	if len(argv) == 0 {
		return nil, fmt.Errorf("gate %q: 命令为空", g.ID)
	}
	cwd, err := g.evalCwd(ctx)
	if err != nil {
		return nil, err
	}
	timeout, err := g.evalTimeout(ctx)
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
			"passed":    true,
			"gate":      g.gateName(),
			"exit_code": 0,
			"stdout":    "[dry-run]",
			"stderr":    "",
			"retries":   0,
			"dry_run":   true,
		}, NextNode: g.NextNode()}, nil
	}

	// Tenant gate (pn#18 / A42-06), after the dry-run short-circuit and before
	// any spawn: a non-default tenant's command would run as the worker user
	// and could read the default tenant's credentials. gate.py refuses at the
	// same point (dry runs never execute, so they stay allowed).
	if err := refuseNonDefaultTenant(ctx, "gate"); err != nil {
		return nil, err
	}

	env := gateBuildSubprocessEnv(g.EnvExtra)
	exitCode, stdout, stderr := 1, "", ""
	attempts := 1 + g.MaxRetries
	if attempts < 1 {
		attempts = 1 // Python: attempts = 1 + max(0, self.max_retries)
	}
	retries := 0
	for attempt := 0; attempt < attempts; attempt++ {
		retries = attempt
		// The retry loop consumes cancellation too: no further command runs
		// after cancellation is requested (A2R8-02).
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("gate %q: cancelled via context; no further attempts", g.ID)
		}
		exitCode, stdout, stderr, err = gateRunOnce(ctx, argv, cwd, env, timeout)
		if err != nil {
			return nil, err
		}
		if exitCode == 0 {
			break
		}
	}
	return &workflow.ExecuteResult{Result: map[string]any{
		"passed":    exitCode == 0,
		"gate":      g.gateName(),
		"exit_code": exitCode,
		"stdout":    gateClipOutput(stdout, gateStdoutCap),
		"stderr":    gateClipOutput(stderr, gateStderrCap),
		"retries":   retries,
		"dry_run":   false,
	}, NextNode: g.NextNode()}, nil
}

func (g *Gate) gateName() string {
	if g.GateName == "" {
		return "quality-gate"
	}
	return g.GateName
}

// buildArgv evaluates the command expression(s). A single string is shlex
// split (POSIX mode); a list of elements becomes the argv directly after
// per-element evaluation and stringification.
func (g *Gate) buildArgv(ctx context.Context) ([]string, error) {
	raw, ok := g.Command.([]any)
	if !ok {
		raw = []any{g.Command}
	}
	parts := make([]string, 0, len(raw))
	for _, el := range raw {
		v, err := expression.Eval(ctx, el)
		if err != nil {
			return nil, fmt.Errorf("gate %q: eval command: %w", g.ID, err)
		}
		parts = append(parts, gateToStr(v))
	}
	if len(parts) == 1 {
		argv, err := gateShlexSplit(parts[0])
		if err != nil {
			return nil, fmt.Errorf("gate %q: %w", g.ID, err)
		}
		return argv, nil
	}
	return parts, nil
}

func (g *Gate) evalCwd(ctx context.Context) (string, error) {
	if g.Cwd == nil {
		return "", nil
	}
	v, err := expression.Eval(ctx, g.Cwd)
	if err != nil {
		return "", fmt.Errorf("gate %q: eval cwd: %w", g.ID, err)
	}
	if !isTruthy(v) {
		return "", nil
	}
	return gateToStr(v), nil
}

// evalTimeout evaluates timeout_secs; nil evaluates to the 600s default and
// the evaluated value is fail-closed validated (0/negative/non-finite are
// rejected, never silently swallowed — A2R7-05).
func (g *Gate) evalTimeout(ctx context.Context) (time.Duration, error) {
	raw := any(gateDefaultTimeoutSecs)
	if g.TimeoutSecs != nil {
		v, err := expression.Eval(ctx, g.TimeoutSecs)
		if err != nil {
			return 0, fmt.Errorf("gate %q: eval timeout_secs: %w", g.ID, err)
		}
		if v != nil {
			raw = v
		}
	}
	secs, err := gatePositiveSeconds("gate timeout_secs", raw)
	if err != nil {
		return 0, err
	}
	return time.Duration(secs * float64(time.Second)), nil
}

// gateRunOnce runs one attempt. Returns (exitCode, stdout, stderr, error):
// an own timeout keeps the historical exit_code=124 with whatever output was
// captured; cancellation of the parent context is an error (the process tree
// is killed first); a spawn failure (e.g. command not found) is an error,
// matching the Python Popen raising.
func gateRunOnce(ctx context.Context, argv []string, cwd string, env []string, timeout time.Duration) (int, string, string, error) {
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

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	runErr := cmd.Run()

	if ctx.Err() != nil {
		return 0, "", "", fmt.Errorf("gate: cancelled via context; process tree killed")
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
			// Process exited (successfully — a failing status would surface
			// as ExitError) but grandchildren held the pipes past WaitDelay.
			return 0, stdout.String(), stderr.String(), nil
		}
		return 0, "", "", fmt.Errorf("gate: %w", runErr)
	}
	return 0, stdout.String(), stderr.String(), nil
}

// gateBuildSubprocessEnv rebuilds a whitelisted child environment from the
// host env, then applies extra (call-site declared variables, which win).
func gateBuildSubprocessEnv(extra map[string]string) []string {
	env := make([]string, 0, len(gateEnvAllowlist)+len(extra))
	for _, key := range gateEnvAllowlist {
		if v, ok := os.LookupEnv(key); ok {
			env = append(env, key+"="+v)
		}
	}
	for _, key := range slices.Sorted(maps.Keys(extra)) {
		env = append(env, key+"="+extra[key])
	}
	return env
}

// gateClipOutput keeps head 1/4 + tail 3/4 past the cap and marks the omitted
// amount (diagnostics like "failures:" live at the tail). Lengths are counted
// in runes to mirror Python string indexing.
func gateClipOutput(text string, capChars int) string {
	runes := []rune(text)
	if capChars <= 0 || len(runes) <= capChars {
		return text
	}
	headLen := capChars / 4
	omitted := len(runes) - capChars
	tail := string(runes[len(runes)-(capChars-headLen):])
	return fmt.Sprintf("%s…[省略 %d 字符]…%s", string(runes[:headLen]), omitted, tail)
}

// gatePositiveSeconds ports plaita-nodes _timeouts.positive_seconds: a finite
// positive number of seconds; 0/negative/non-finite/bool/non-numeric are
// rejected with a named error (fail-closed).
func gatePositiveSeconds(name string, value any) (float64, error) {
	reject := func() (float64, error) {
		return 0, fmt.Errorf(
			"%s must be a positive number of seconds (got %v); 0 and negative values are rejected (fail-closed)",
			name, value)
	}
	if value == nil {
		return reject()
	}
	if _, isBool := value.(bool); isBool {
		return reject()
	}
	secs, ok := gateToFloat(value)
	if !ok {
		return reject()
	}
	if math.IsNaN(secs) || math.IsInf(secs, 0) || secs <= 0 {
		return reject()
	}
	return secs, nil
}

// gateToFloat converts numeric scalars and numeric strings (Python float()
// accepts "600"); bool and everything else return false.
func gateToFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case float32:
		return float64(t), true
	case int:
		return float64(t), true
	case int32:
		return float64(t), true
	case int64:
		return float64(t), true
	case json.Number:
		f, err := t.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(t), 64)
		return f, err == nil
	default:
		return 0, false
	}
}

// gateToStr approximates Python str() over the JSON scalar domain for
// command argv elements: nil → "None", bool → True/False, integral floats
// print without a decimal tail. Containers fall back to fmt formatting
// (cosmetic difference only; they are not useful argv anyway).
func gateToStr(v any) string {
	switch t := v.(type) {
	case nil:
		return "None"
	case string:
		return t
	case bool:
		if t {
			return "True"
		}
		return "False"
	case json.Number:
		return t.String()
	case int:
		return strconv.Itoa(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case float64:
		if t == math.Trunc(t) && math.Abs(t) < 1e16 {
			return strconv.FormatInt(int64(t), 10)
		}
		return strconv.FormatFloat(t, 'g', -1, 64)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// gateShlexSplit ports the shlex.split POSIX-mode subset gate.py relies on:
// split on space/tab/CR/LF, single and double quotes (stripped, may appear
// mid-word), backslash escaping outside quotes, and inside double quotes
// only for "\"" and "\\" (any other backslash stays literal). Unterminated
// quotes or a trailing lone backslash are errors, like shlex's ValueError.
func gateShlexSplit(s string) ([]string, error) {
	const (
		stOut = iota
		stSQ
		stDQ
		stEscape
	)
	rs := []rune(s)
	tokens := []string{}
	var cur strings.Builder
	quoted := false // token contains quoted/escaped content ("''" yields "")
	state := stOut
	escTarget := stOut
	isWS := func(r rune) bool { return r == ' ' || r == '\t' || r == '\r' || r == '\n' }
	flush := func() {
		if cur.Len() > 0 || quoted {
			tokens = append(tokens, cur.String())
			cur.Reset()
			quoted = false
		}
	}
	for _, r := range rs {
		switch state {
		case stOut:
			switch {
			case isWS(r):
				flush()
			case r == '\'':
				state = stSQ
				quoted = true
			case r == '"':
				state = stDQ
				quoted = true
			case r == '\\':
				state = stEscape
				escTarget = stOut
			default:
				cur.WriteRune(r)
			}
		case stSQ:
			if r == '\'' {
				state = stOut
			} else {
				cur.WriteRune(r)
			}
		case stDQ:
			switch r {
			case '"':
				state = stOut
			case '\\':
				state = stEscape
				escTarget = stDQ
			default:
				cur.WriteRune(r)
			}
		case stEscape:
			if escTarget == stDQ && r != '"' && r != '\\' {
				cur.WriteRune('\\') // backslash keeps literal meaning here
			}
			cur.WriteRune(r)
			state = escTarget
		}
	}
	switch state {
	case stSQ, stDQ:
		return nil, errors.New("No closing quotation")
	case stEscape:
		return nil, errors.New("No escaped character")
	}
	flush()
	return tokens, nil
}
