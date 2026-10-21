package nodes

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// The python sidecar contract mirrors plaita/node/code.py run_python_subprocess:
// user code embedded in _RUNNER_TEMPLATE, JSON via stdin/stdout envelope,
// wall-clock timeout (PLAITA_SANDBOX_TIMEOUT) and process-group kill. The
// end-to-end tests need a real python3 (present on dev macOS and CI ubuntu).

func pyRequirePython3(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath(pySandboxPythonBin); err != nil {
		t.Skipf("%s not on PATH: %v", pySandboxPythonBin, err)
	}
}

func pyTestEnv(t *testing.T) []string {
	return pySandboxedEnv(t, "")
}

// pySandboxedEnv builds the child env for sample runs: the whitelisted rebuild
// with HOME/TMPDIR pinned into throwaway dirs. The child PATH is restricted to
// system dirs so production samples' inner `python ...` lookups fail fast and
// deterministically (dev machines may have a pyenv `python` with heavyweight
// tooling installed). home="" pins HOME to a fresh temp dir; the issue-keeper
// directory layout the samples expect is pre-created under it.
func pySandboxedEnv(t *testing.T, home string) []string {
	t.Helper()
	if home == "" {
		home = t.TempDir()
	}
	tmp := t.TempDir()
	keeper := filepath.Join(home, ".issue-keeper", "pipeline", "controller", "handoffs")
	if err := os.MkdirAll(keeper, 0o755); err != nil {
		t.Fatal(err)
	}
	return []string{
		"PATH=/usr/bin:/bin:/usr/sbin:/sbin",
		"HOME=" + home,
		"TMPDIR=" + tmp,
		"LANG=" + os.Getenv("LANG"),
	}
}

func TestBuildPythonRunnerScript(t *testing.T) {
	// placeholder substitution must be plain replacement (plaita uses
	// str.replace, not format) — braces and %-signs in user code survive.
	code := `M = {"a": 1}
FMT = "%s {braces} 100% %d"
def run(input):
    return FMT % (M["a"], input)`
	script := buildPythonRunnerScript(code, 268435456)
	if !strings.Contains(script, "import sys as _sys, json as _json") {
		t.Fatal("runner template header missing")
	}
	if !strings.Contains(script, "_mem_bytes = 268435456") {
		t.Fatal("mem placeholder not substituted")
	}
	for _, frag := range []string{`M = {"a": 1}`, `FMT = "%s {braces} 100% %d"`, `_result = run(_input)`} {
		if !strings.Contains(script, frag) {
			t.Fatalf("user code or tail mangled, missing %q in:\n%s", frag, script)
		}
	}
	// user code sits at module level, before the run tail (plaita template order)
	if strings.Index(script, "M = {") > strings.Index(script, "_input = _json.loads") {
		t.Fatal("user code not embedded before run tail")
	}
	// plaita quirk preserved verbatim: the second replacement runs over the
	// WHOLE script, so a literal __MEM_BYTES__ inside user code is substituted
	// too (str.replace semantics — mirrored, not fixed)
	script2 := buildPythonRunnerScript("MARK = \"mem=__MEM_BYTES__\"\n\n\ndef run(i):\n    return i\n", 1)
	if !strings.Contains(script2, "MARK = \"mem=1\"") {
		t.Fatalf("plaita whole-script replace semantics not mirrored:\n%s", script2)
	}
}

func TestPyDefinesRunFunction(t *testing.T) {
	cases := []struct {
		code string
		want bool
	}{
		{"def run(input):\n    return input\n", true},
		{"import json\n\ndef run(input):\n    return json.dumps(input)\n", true},
		{"@decorator\ndef run(input):\n    return input\n", true},
		{"    def run(input):\n        return input\n", true},
		{"def runner(input):\n    return input\n", false},
		{"async def run(input):\n    return input\n", false}, // plaita AST check: FunctionDef only
		{"x = 1\n", false},
		{"def run ():\n    return 1\n", true},
	}
	for _, tc := range cases {
		if got := pyDefinesRunFunction(tc.code); got != tc.want {
			t.Errorf("pyDefinesRunFunction(%q) = %v, want %v", tc.code, got, tc.want)
		}
	}
}

func TestPySandboxTimeoutEnv(t *testing.T) {
	cases := []struct {
		raw    string
		want   int
		errSub string
	}{
		{"", pyDefaultTimeout, ""}, // unset → default
		{"30", 30, ""},
		{"0", 0, "0 is a zero budget, not unlimited"},
		{"-3", 0, "0 is a zero budget, not unlimited"},
		{"1.5", 0, "must be a positive integer number of seconds"},
		{"abc", 0, "must be a positive integer number of seconds"},
	}
	for _, tc := range cases {
		t.Setenv(pySandboxTimeoutEnv, tc.raw)
		got, err := pySandboxTimeout()
		if tc.errSub == "" {
			if err != nil {
				t.Fatalf("PLAITA_SANDBOX_TIMEOUT=%q: unexpected error %v", tc.raw, err)
			}
			if got != tc.want {
				t.Fatalf("PLAITA_SANDBOX_TIMEOUT=%q = %d, want %d", tc.raw, got, tc.want)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), tc.errSub) {
			t.Fatalf("PLAITA_SANDBOX_TIMEOUT=%q: err = %v, want contains %q", tc.raw, err, tc.errSub)
		}
	}
}

func TestPySandboxMemoryMBEnv(t *testing.T) {
	t.Setenv(pySandboxMemoryEnv, "256")
	mb, err := pySandboxMemoryMB()
	if err != nil || mb != 256 {
		t.Fatalf("got %d, %v", mb, err)
	}
	// negative mirrors plaita int() acceptance; template treats <=0 as unlimited
	t.Setenv(pySandboxMemoryEnv, "-1")
	if mb, err = pySandboxMemoryMB(); err != nil || mb != -1 {
		t.Fatalf("got %d, %v", mb, err)
	}
	t.Setenv(pySandboxMemoryEnv, "abc")
	if _, err = pySandboxMemoryMB(); err == nil || !strings.Contains(err.Error(), pySandboxMemoryEnv) {
		t.Fatalf("want named error, got %v", err)
	}
	os.Unsetenv(pySandboxMemoryEnv)
	if mb, err = pySandboxMemoryMB(); err != nil || mb != 0 {
		t.Fatalf("default: got %d, %v", mb, err)
	}
}

func TestDecodePythonRunnerOutput(t *testing.T) {
	result, err := decodePythonRunnerOutput(`{"ok": true, "result": {"a": 1, "b": [true, null]}}`, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("result type %T", result)
	}
	if m["a"] != json.Number("1") {
		t.Fatalf("integer-ness lost: %v (%T)", m["a"], m["a"])
	}

	// error envelope → "type: error", plaita's defaults for missing fields
	for raw, want := range map[string]string{
		`{"ok": false, "error": "boom", "type": "ValueError"}`: "ValueError: boom",
		`{"ok": false, "error": "boom"}`:                       "Error: boom",
		`{"ok": false}`:                                        "Error: (unknown)",
	} {
		_, err := decodePythonRunnerOutput(raw, "")
		if err == nil || err.Error() != want {
			t.Fatalf("decode(%s) = %v, want %q", raw, err, want)
		}
	}

	// empty stdout (plaita: Sandbox produced no output; '(empty)' fallback)
	_, err = decodePythonRunnerOutput("  \n", "")
	if err == nil || !strings.Contains(err.Error(), "Sandbox produced no output") || !strings.Contains(err.Error(), "(empty)") {
		t.Fatalf("empty stdout: %v", err)
	}
	_, err = decodePythonRunnerOutput("", "Traceback (most recent call last)")
	if err == nil || !strings.Contains(err.Error(), "Traceback") {
		t.Fatalf("empty stdout with stderr: %v", err)
	}

	// non-JSON stdout (user code printed noise) → not-valid-JSON, raw prefix quoted
	_, err = decodePythonRunnerOutput("hello world\n{\"ok\": true}", "")
	if err == nil || !strings.Contains(err.Error(), "Sandbox output is not valid JSON") || !strings.Contains(err.Error(), "hello world") {
		t.Fatalf("non-JSON: %v", err)
	}
}

func TestPythonCodeValidate(t *testing.T) {
	base := BaseNode{ID: "c", Next: "end"}
	missing := Code{BaseNode: base, Language: LanguagePython, Code: "x = 1"}
	if err := missing.Validate(); err == nil ||
		!strings.Contains(err.Error(), "No run function found") {
		t.Fatalf("missing run: %v", err)
	}
	docker := Code{BaseNode: base, Language: LanguagePython,
		Code: "def run(input):\n    return input", SandboxBackend: "docker"}
	if err := docker.Validate(); err == nil ||
		!strings.Contains(err.Error(), "docker") || !strings.Contains(err.Error(), "plaita runtime") {
		t.Fatalf("docker backend: %v", err)
	}
	// absent and explicit subprocess pass validation
	for _, backend := range []string{"", "subprocess"} {
		c := Code{BaseNode: base, Language: LanguagePython,
			Code: "def run(input):\n    return input", SandboxBackend: backend}
		if err := c.Validate(); err != nil {
			t.Fatalf("backend %q: %v", backend, err)
		}
	}
	// javascript is untouched by the python admission
	js := Code{BaseNode: base, Language: JS, Code: "function run(i){return i}"}
	if err := js.Validate(); err != nil {
		t.Fatalf("js node: %v", err)
	}
}

func pyNode(code string, input any) *Code {
	return &Code{
		BaseNode: BaseNode{
			ID:         "py-c",
			Next:       "end",
			OutputType: workflow.Property{DataType: workflow.Object},
		},
		Language: LanguagePython,
		Code:     code,
		Input:    input,
	}
}

func pyRunNode(t *testing.T, node *Code, input map[string]any) (any, error) {
	t.Helper()
	pyRequirePython3(t)
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: input,
	})
	res, err := node.Execute(ctx)
	if err != nil {
		return nil, err
	}
	return res.Result, nil
}

func TestPythonSidecarEndToEnd(t *testing.T) {
	pyRequirePython3(t)

	t.Run("run returns json", func(t *testing.T) {
		got, err := pyRunNode(t, pyNode(
			"import math\n\n\ndef run(input):\n    return {\"msg\": \"hi \" + input[\"name\"], \"scale\": input[\"n\"] * 2, \"ceil\": math.ceil(input[\"n\"])}\n",
			"$INPUT",
		), map[string]any{"name": "py", "n": json.Number("3")})
		if err != nil {
			t.Fatal(err)
		}
		m, ok := got.(map[string]any)
		if !ok || m["msg"] != "hi py" || m["scale"] != json.Number("6") || m["ceil"] != json.Number("3") {
			t.Fatalf("got %#v", got)
		}
	})

	t.Run("run raises", func(t *testing.T) {
		_, err := pyRunNode(t, pyNode(
			"def run(input):\n    raise ValueError(\"boom\")\n",
			"$INPUT",
		), map[string]any{})
		if err == nil || !strings.Contains(err.Error(), "ValueError: boom") {
			t.Fatalf("want ValueError envelope error, got %v", err)
		}
	})

	t.Run("run not defined", func(t *testing.T) {
		// plaita's parse-time check fires in Validate; here the node is
		// constructed directly, so the runtime envelope is the enforcement
		// point — exactly the NameError the plaita runner produces.
		_, err := pyRunNode(t, pyNode(
			"def runner(input):\n    return input\n",
			"$INPUT",
		), map[string]any{})
		if err == nil || !strings.Contains(err.Error(), "NameError") {
			t.Fatalf("want NameError envelope error, got %v", err)
		}
	})

	t.Run("non-json stdout", func(t *testing.T) {
		_, err := pyRunNode(t, pyNode(
			"print(\"noise from user code\")\n\n\ndef run(input):\n    return input\n",
			"$INPUT",
		), map[string]any{})
		if err == nil || !strings.Contains(err.Error(), "Sandbox output is not valid JSON") {
			t.Fatalf("want non-JSON envelope error, got %v", err)
		}
	})

	t.Run("syntax error", func(t *testing.T) {
		_, err := pyRunNode(t, pyNode(
			"def run(input)\n    return input\n",
			"$INPUT",
		), map[string]any{})
		if err == nil || !strings.Contains(err.Error(), "Subprocess exited with code 1") ||
			!strings.Contains(err.Error(), "SyntaxError") {
			t.Fatalf("want exit-code error with stderr, got %v", err)
		}
	})
}

func TestPythonSidecarTimeoutKillsProcessTree(t *testing.T) {
	pyRequirePython3(t)
	// user code spawns a grandchild, then outlives the budget: both the child
	// and the grandchild must be gone after the killpg (plaita 2026-09-30
	// orphan fix, mirrored via Setpgid + group SIGKILL).
	code := `
import subprocess, time
subprocess.Popen(["sleep", "12345"])
time.sleep(60)
`
	_, err := runPythonSubprocessBudget(context.Background(), code, nil,
		400*time.Millisecond, 0, pyTestEnv(t))
	if err == nil || !strings.Contains(err.Error(), "Subprocess sandbox timed out after") {
		t.Fatalf("want timeout error, got %v", err)
	}

	// give the SIGKILL a beat to land, then prove no orphan survives
	time.Sleep(300 * time.Millisecond)
	psOut, psErr := exec.Command("ps", "ax", "-o", "command").CombinedOutput()
	if psErr != nil {
		t.Logf("ps unavailable, skipping orphan assertion: %v", psErr)
		return
	}
	if strings.Contains(string(psOut), "sleep 12345") {
		t.Fatalf("grandchild survived the process-group kill:\n%s", psOut)
	}
}

func TestPythonSidecarContextCancel(t *testing.T) {
	pyRequirePython3(t)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	_, err := runPythonSubprocessBudget(ctx,
		"import time\ntime.sleep(60)\n", nil,
		time.Minute, 0, pyTestEnv(t))
	if err == nil || !strings.Contains(err.Error(), "cancelled via context; process tree killed") {
		t.Fatalf("want cancellation error, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("cancellation took %v, expected prompt kill", elapsed)
	}
}

func TestPythonSidecarExpressionFields(t *testing.T) {
	pyRequirePython3(t)
	rc := &runtime.Context{WorkflowInput: map[string]any{"name": "py"}}
	rc.WithEnvVariables(map[string]any{"PY_SIDECAR_ECHO": "tok-123"})
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), rc)

	// $ENV root as the whole input
	res, err := pyNode("def run(input):\n    return {\"token\": input}\n", "$ENV.PY_SIDECAR_ECHO").Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if m, ok := res.Result.(map[string]any); !ok || m["token"] != "tok-123" {
		t.Fatalf("got %#v", res.Result)
	}

	// {% %} template interpolation of the env var (string output type)
	node := &Code{
		BaseNode: BaseNode{
			ID:         "py-tpl",
			Next:       "end",
			OutputType: workflow.Property{DataType: workflow.String},
		},
		Language: LanguagePython,
		Code:     "def run(input):\n    return input\n",
		Input:    "{% $ENV.PY_SIDECAR_ECHO %}",
	}
	res, err = node.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Result != "tok-123" {
		t.Fatalf("template interpolation: got %#v", res.Result)
	}
}

// Production samples: real code-node sources lifted read-only from the
// production Redis (plaita:flow:*), latest version per flow. Provenance:
//
//	issue_pipeline_kanban.py ← plaita:flow:issue-pipeline:2.1.1 / kanban   (backend=subprocess)
//	ctrl_watch_finish.py     ← plaita:flow:ctrl-watch:1.0.3     / finish  (backend absent)
//	keeper_watch_finish.py   ← plaita:flow:keeper-watch:0.0.6   / finish  (backend absent)
//	land_rebase2.py          ← plaita:flow:self-improve-v2:1.0.8/ land_rebase2 (backend absent)
//	cancel_drill_slow.py     ← plaita:flow:cancel-drill-flow:1.0.0 / slow (backend absent)
//
// All five run side-effect-free in the test: writes go into t.TempDir()s
// (duty_dir / HOME from the injected child env) and the git sample operates
// on a throwaway repo on its failure path (no push). The plaita_nodes-import
// family in production (e.g. self-improve-v2-sbx land_apply) is NOT sampled:
// it needs the plaita-nodes repo on PYTHONPATH — noted in the alignment doc.
func TestPythonSidecarProductionSamples(t *testing.T) {
	pyRequirePython3(t)

	readSample := func(t *testing.T, name string) string {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join("testdata", "code_python_prod", name))
		if err != nil {
			t.Fatalf("read sample: %v", err)
		}
		return string(raw)
	}

	t.Run("issue_pipeline_kanban", func(t *testing.T) {
		// The sample shells out to `python -m issue_keeper ...` with a bogus
		// repo and catches every failure mode. The child PATH is restricted
		// (no `python` on /usr/bin:/bin on macOS/CI ubuntu) so the inner
		// subprocess fails fast with FileNotFoundError on any machine → the
		// sample must return kanban_ok=false.
		got, err := runPythonSubprocessBudget(context.Background(),
			readSample(t, "issue_pipeline_kanban.py"),
			map[string]any{"repo_full": "nonexistent/repo", "issue_number": json.Number("0")},
			30*time.Second, 0, pySandboxedEnv(t, ""))
		if err != nil {
			t.Fatal(err)
		}
		if m, ok := got.(map[string]any); !ok || m["kanban_ok"] != false {
			t.Fatalf("got %#v, want {kanban_ok: false}", got)
		}
	})

	t.Run("ctrl_watch_finish", func(t *testing.T) {
		// fcntl-guarded duty state machine, first round into an empty dir
		got, err := pyRunNode(t, pyNode(readSample(t, "ctrl_watch_finish.py"), "$INPUT"),
			map[string]any{"duty_dir": t.TempDir(), "findings": []any{}, "report": "", "posted": nil})
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]any{"round": json.Number("1"), "status": "ok", "posted": false}
		m, ok := got.(map[string]any)
		if !ok || m["round"] != want["round"] || m["status"] != want["status"] || m["posted"] != want["posted"] {
			t.Fatalf("got %#v, want %v", got, want)
		}
	})

	t.Run("keeper_watch_finish", func(t *testing.T) {
		// writes under Path.home() — the injected child env pins HOME to a
		// throwaway dir with the directory layout production has, and the
		// assertion reads the written file back through the returned path.
		home := t.TempDir()
		got, err := runPythonSubprocessBudget(context.Background(),
			readSample(t, "keeper_watch_finish.py"),
			map[string]any{"text": "line one\nline two", "report": `{"findings": [1, 2, 3]}`},
			30*time.Second, 0, pySandboxedEnv(t, home))
		if err != nil {
			t.Fatal(err)
		}
		m, ok := got.(map[string]any)
		if !ok || m["rounds_line"] == nil || m["handoff"] == nil {
			t.Fatalf("got %#v", got)
		}
		if s, _ := m["rounds_line"].(string); !strings.Contains(s, "findings=3") {
			t.Fatalf("rounds_line: %v", m["rounds_line"])
		}
		handoff, _ := m["handoff"].(string)
		if !strings.HasPrefix(handoff, home) {
			t.Fatalf("handoff escaped the pinned HOME: %v", handoff)
		}
		raw, err := os.ReadFile(handoff)
		if err != nil || !strings.Contains(string(raw), "findings") {
			t.Fatalf("handoff file: %v", err)
		}
	})

	t.Run("land_rebase2", func(t *testing.T) {
		// fresh git repo without commits: rebasing=false, done=false → the
		// sample takes its failure path ({ok: false, why: ...}) and never
		// touches the network.
		wt := t.TempDir()
		for _, args := range [][]string{{"init", "-q"}, {"config", "user.email", "t@t"}, {"config", "user.name", "t"}} {
			out, err := exec.Command("git", append([]string{"-C", wt}, args...)...).CombinedOutput()
			if err != nil {
				t.Fatalf("git %v: %v: %s", args, err, out)
			}
		}
		got, err := pyRunNode(t, pyNode(readSample(t, "land_rebase2.py"), "$INPUT"),
			map[string]any{"wt": wt, "branch": "feature/x"})
		if err != nil {
			t.Fatal(err)
		}
		m, ok := got.(map[string]any)
		if !ok || m["ok"] != false || m["why"] == nil {
			t.Fatalf("got %#v, want {ok: false, why: <set>}", got)
		}
	})

	t.Run("cancel_drill_slow", func(t *testing.T) {
		// production drill node declares run(inputs, context): plaita's runner
		// always calls run(input), so the call itself raises TypeError — the
		// envelope carries it verbatim.
		_, err := pyRunNode(t, pyNode(readSample(t, "cancel_drill_slow.py"), "$INPUT"),
			map[string]any{})
		if err == nil || !strings.Contains(err.Error(), "TypeError:") {
			t.Fatalf("want TypeError envelope error, got %v", err)
		}
	})
}

// TestDecodePythonRunnerOutputDeterministicClass (issue #43): the envelope
// carries the original exception type, and the node-F2 deterministic class
// (ValueError / TypeError / NameError / SyntaxError) must arrive at the
// engine as a NonRetryable error — a retry cannot make user code correct.
// Everything else keeps the historical retryable default (plaita's unknown
// error posture; its subprocess backend wraps in RuntimeError the same way).
// Error TEXT is unchanged either way.
func TestDecodePythonRunnerOutputDeterministicClass(t *testing.T) {
	rows := []struct {
		raw       string
		text      string
		retryable bool
	}{
		{`{"ok": false, "error": "boom", "type": "ValueError"}`, "ValueError: boom", false},
		{`{"ok": false, "error": "bad type", "type": "TypeError"}`, "TypeError: bad type", false},
		{`{"ok": false, "error": "no name", "type": "NameError"}`, "NameError: no name", false},
		{`{"ok": false, "error": "bad syntax", "type": "SyntaxError"}`, "SyntaxError: bad syntax", false},
		{`{"ok": false, "error": "infra", "type": "RuntimeError"}`, "RuntimeError: infra", true},
		{`{"ok": false, "error": "missing", "type": "KeyError"}`, "KeyError: missing", true},
		{`{"ok": false}`, "Error: (unknown)", true},
	}
	for _, r := range rows {
		_, err := decodePythonRunnerOutput(r.raw, "")
		if err == nil {
			t.Fatalf("decode(%s): expected error", r.raw)
		}
		if err.Error() != r.text {
			t.Fatalf("decode(%s) text = %q, want %q", r.raw, err.Error(), r.text)
		}
		if got := workflow.IsRetryableError(err); got != r.retryable {
			t.Errorf("decode(%s): IsRetryableError=%v, want %v", r.raw, got, r.retryable)
		}
	}
}
