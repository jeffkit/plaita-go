// Python code execution for the code node: a python3 subprocess sidecar that
// mirrors the plaita CodeNode "subprocess" backend (plaita/node/code.py,
// run_python_subprocess + _RUNNER_TEMPLATE) contract-for-contract.
//
// Trust model
// -----------
// The sidecar runs **operator-trusted flow code** with the host's file system
// and network access — exactly like plaita's subprocess backend. It is **not**
// an isolation boundary: the child runs as the same OS user, shares the host
// file system (including credential files under $HOME), and has unrestricted
// network access. The environment passed to the child is the whitelisted
// rebuild (gateBuildSubprocessEnv — the same allowlist as plaita
// subprocess_env.SUBPROCESS_ENV_ALLOWLIST), so host env secrets are not
// handed over, but env allowlisting is not a security boundary against the
// code itself. Multi-tenant / semi-trusted authors need the plaita runtime's
// docker backend; this engine fail-closes on sandbox_backend values it does
// not implement (restricted/docker/unsafe) instead of silently downgrading.
//
// Tenant boundary (plaita#256 / A42-06): the sidecar shares the worker's OS
// user and file system, and every tenant's credentials share one key, so a
// non-default tenant's user code could read the default tenant's credentials
// no matter which env the parent passes. runPythonSubprocessBudget therefore
// refuses non-default tenants **before spawning** (zero execution chance,
// ErrTenantIsolationUnavailable); tenant-scoped code belongs in a container
// that mounts no host credentials.
//
// Bounds (mirroring plaita):
//   - wall-clock timeout from PLAITA_SANDBOX_TIMEOUT (seconds, default 10;
//     must be a positive integer — 0/negative/non-integers are rejected
//     fail-closed, r39 A1);
//   - optional Linux address-space limit from PLAITA_SANDBOX_MEMORY_MB
//     (MB, default 0 = unlimited; >0 is embedded into the runner and applied
//     as RLIMIT_AS by the child itself);
//   - cooperative cancellation rides the Go context (plaita uses a
//     cancel_event): timeout and cancellation both SIGKILL the whole process
//     group — the child starts in its own group (Setpgid, the Go equivalent
//     of start_new_session=True), so grandchildren spawned by user code do
//     not survive.
//
// Sandbox linkage: unlike agentrun's workspace branch, the sidecar creates no
// sandbox resources (no container, no workspace, no lease), so
// sandbox.WithExecutionID has nothing to attach to here — execution scoping
// simply does not apply to this node form.
package nodes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// pySandboxPythonBin is the sidecar interpreter. plaita uses sys.executable
// (the running interpreter); the Go engine has no host interpreter to reuse,
// so the deployment must provide python3 on PATH.
const pySandboxPythonBin = "python3"

// pySandboxTimeoutEnv / pySandboxMemoryEnv mirror the plaita subprocess
// backend configuration env vars (same names, same defaults).
const (
	pySandboxTimeoutEnv = "PLAITA_SANDBOX_TIMEOUT"
	pySandboxMemoryEnv  = "PLAITA_SANDBOX_MEMORY_MB"
	pyDefaultTimeout    = 10
)

// pyRunnerTemplate is a verbatim port of plaita/node/code.py _RUNNER_TEMPLATE
// (subprocess and docker backends share it). Placeholders __MEM_BYTES__ and
// __USER_CODE__ are substituted with plain string replacement (never
// fmt/template) so arbitrary user code — dict literals, %-formatting, braces —
// passes through untouched.
const pyRunnerTemplate = `import sys as _sys, json as _json

# ---- resource limits (Linux only) ------------------------------------
try:
    import resource as _resource
    _mem_bytes = __MEM_BYTES__
    if _mem_bytes > 0:
        _resource.setrlimit(_resource.RLIMIT_AS, (_mem_bytes, _mem_bytes))
except Exception:
    pass

# ---- user code -------------------------------------------------------
__USER_CODE__
# ---- run -------------------------------------------------------------
_input = _json.loads(_sys.stdin.read())
try:
    _result = run(_input)
    _sys.stdout.write(_json.dumps({"ok": True, "result": _result}))
except Exception as _e:
    _sys.stdout.write(_json.dumps({"ok": False, "error": str(_e), "type": type(_e).__name__}))
_sys.stdout.flush()
`

// placeholders used by buildPythonRunnerScript (plaita _PLACEHOLDER_*).
const (
	pyPlaceholderMem  = "__MEM_BYTES__"
	pyPlaceholderCode = "__USER_CODE__"
)

// buildPythonRunnerScript embeds userCode at column 0 of the runner template,
// mirroring plaita _build_runner_script: str.replace semantics (not format),
// user code first, then the memory limit literal. Replacement order matters
// only for adversarial code containing the other placeholder — kept identical
// to plaita anyway.
func buildPythonRunnerScript(userCode string, memBytes int64) string {
	script := strings.ReplaceAll(pyRunnerTemplate, pyPlaceholderCode, userCode)
	return strings.ReplaceAll(script, pyPlaceholderMem, fmt.Sprintf("%d", memBytes))
}

// pyRunFuncRe is the pragmatic stand-in for plaita's parse-time AST check
// (a FunctionDef named "run" must exist). A regex cannot fully replace the
// AST walk (a "def run(" inside a string literal would slip through), but the
// subprocess run itself re-fails with the envelope error when run is missing,
// so the build-time check stays a cheap preflight, not the enforcement point.
var pyRunFuncRe = regexp.MustCompile(`(?m)^\s*def\s+run\s*\(`)

// pyDefinesRunFunction reports whether the source plausibly defines
// `def run(input)` at any indentation (module level in practice).
func pyDefinesRunFunction(code string) bool {
	return pyRunFuncRe.MatchString(code)
}

// pySandboxTimeout reads PLAITA_SANDBOX_TIMEOUT, mirroring plaita
// _positive_int_env_seconds: a positive integer number of seconds; 0,
// negatives and non-integers are named failures (fail-closed) instead of
// silently disabling the wall clock. plaita evaluates this at import time and
// refuses to boot; the Go engine fails the node execution instead — a smaller
// blast radius with the same contract.
func pySandboxTimeout() (int, error) {
	return pyPositiveIntEnvSeconds(pySandboxTimeoutEnv, pyDefaultTimeout)
}

func pyPositiveIntEnvSeconds(name string, fallback int) (int, error) {
	raw, ok := os.LookupEnv(name)
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, fmt.Errorf("%s must be a positive integer number of seconds (got %q)", name, raw)
	}
	if value <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer number of seconds (got %q); 0 is a zero budget, not unlimited", name, raw)
	}
	return value, nil
}

// pySandboxMemoryMB reads PLAITA_SANDBOX_MEMORY_MB, mirroring plaita
// SANDBOX_SUBPROCESS_MEMORY_MB: any int is accepted (default 0 = unlimited);
// only >0 activates the RLIMIT_AS block in the runner. plaita's int() would
// raise at import on garbage — mirrored as a named execution error.
func pySandboxMemoryMB() (int64, error) {
	raw, ok := os.LookupEnv(pySandboxMemoryEnv)
	if !ok || strings.TrimSpace(raw) == "" {
		return 0, nil
	}
	value, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer number of megabytes (got %q)", pySandboxMemoryEnv, raw)
	}
	return value, nil
}

// runPythonSubprocess executes code in a fresh python3 process — the Go port
// of plaita run_python_subprocess. Input and output are JSON via
// stdin/stdout; only JSON-serialisable types cross the boundary (the caller
// already holds a decoded-expression value, marshalled here).
//
// Divergences from plaita, both forced by the runtime:
//   - cancellation rides the Go context instead of cancel_event.
//
// The tenant refusal (A42-06) is enforced below, before any spawn.
func runPythonSubprocess(ctx context.Context, code string, input any) (any, error) {
	timeoutSecs, err := pySandboxTimeout()
	if err != nil {
		return nil, err
	}
	memMB, err := pySandboxMemoryMB()
	if err != nil {
		return nil, err
	}
	return runPythonSubprocessBudget(ctx, code, input,
		time.Duration(timeoutSecs)*time.Second, memMB*1024*1024,
		gateBuildSubprocessEnv(nil))
}

// runPythonSubprocessBudget is runPythonSubprocess with the bounds and the
// child environment made explicit — the seam the tests use to pin HOME/TMPDIR
// and shorten the timeout without touching host state. The tenant gate lives
// here, at the single point every spawn goes through (plaita refuses at
// run_python_subprocess entry for the same reason).
func runPythonSubprocessBudget(ctx context.Context, code string, input any,
	timeout time.Duration, memBytes int64, childEnv []string) (any, error) {
	// Tenant gate (plaita#256 / A42-06): refuse before building the runner or
	// spawning — user code must have zero execution chance for a non-default
	// tenant.
	if err := refuseNonDefaultTenant(ctx, "code python"); err != nil {
		return nil, err
	}

	inputJSON, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("python sidecar: input is not JSON-serialisable: %w", err)
	}
	runner := buildPythonRunnerScript(code, memBytes)

	// Validate the budget before spawning so a misconfigured deployment can
	// never leak a live child (plaita kills-then-raises post-spawn; the
	// user-visible contract — no code ran — is the same).
	if timeout <= 0 {
		return nil, fmt.Errorf("%s must be a positive integer number of seconds (got %v); 0 is a zero budget, not unlimited",
			pySandboxTimeoutEnv, timeout.Seconds())
	}

	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(runCtx, pySandboxPythonBin, "-c", runner)
	cmd.Env = childEnv
	// start_new_session=True equivalent: the child leads its own process
	// group so timeout/cancellation can kill the whole tree.
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

	cmd.Stdin = bytes.NewReader(inputJSON)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	runErr := cmd.Run()

	if ctx.Err() != nil {
		return nil, fmt.Errorf("Subprocess sandbox cancelled via context; process tree killed")
	}
	if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
		return nil, fmt.Errorf(
			"Subprocess sandbox timed out after %vs (process tree killed). "+
				"Increase the timeout env var or optimise the code.",
			timeout.Seconds())
	}

	stdoutText := pyLossyUTF8(stdout.Bytes())
	stderrText := pyLossyUTF8(stderr.Bytes())

	if runErr != nil {
		var ee *exec.ExitError
		if !errors.As(runErr, &ee) {
			// spawn failure (python3 missing etc.) — plaita's Popen raises the
			// raw OSError in this case too
			return nil, fmt.Errorf("python sidecar: %w", runErr)
		}
		if ee.ExitCode() != 0 && strings.TrimSpace(stdoutText) == "" {
			return nil, fmt.Errorf("Subprocess exited with code %d. Stderr: %s",
				ee.ExitCode(), pyClipStderr(stderrText))
		}
		// non-zero exit WITH stdout output: fall through to envelope decoding,
		// mirroring run_python_subprocess
	}
	result, err := decodePythonRunnerOutput(stdoutText, stderrText)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// decodePythonRunnerOutput parses the JSON envelope written by the runner —
// a verbatim port of plaita _decode_runner_output. The result is decoded with
// json.Number preservation so integer-ness survives into the engine values
// (Python json.loads keeps ints; the engine's JSON-value convention is
// UseNumber).
func decodePythonRunnerOutput(raw, stderr string) (any, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("Sandbox produced no output. Stderr: %s", pyClipStderr(stderr))
	}
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	var envelope map[string]any
	if err := dec.Decode(&envelope); err != nil {
		return nil, fmt.Errorf("Sandbox output is not valid JSON: %.200q. Stderr: %s",
			raw, pyClipStderr(stderr))
	}
	if ok, _ := envelope["ok"].(bool); !ok {
		excType, _ := envelope["type"].(string)
		if excType == "" {
			excType = "Error"
		}
		excMsg, _ := envelope["error"].(string)
		if excMsg == "" {
			excMsg = "(unknown)"
		}
		return nil, fmt.Errorf("%s: %s", excType, excMsg)
	}
	return envelope["result"], nil
}

// pyClipStderr mirrors Python's stderr[:500] or '(empty)'.
func pyClipStderr(stderr string) string {
	if stderr == "" {
		return "(empty)"
	}
	runes := []rune(stderr)
	if len(runes) > 500 {
		return string(runes[:500])
	}
	return stderr
}

// pyLossyUTF8 mirrors Python's bytes.decode(errors="replace"): invalid
// sequences become U+FFFD instead of poisoning the error text.
func pyLossyUTF8(b []byte) string {
	return strings.ToValidUTF8(string(b), "\uFFFD")
}
