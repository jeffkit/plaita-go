package nodes

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func gateTestCtx(t *testing.T, input map[string]any, globals map[string]any) context.Context {
	t.Helper()
	rc := &runtime.Context{
		WorkflowInput:      input,
		ExecuteNodeResults: map[string]any{},
		EnvVariables:       map[string]any{},
		GlobalVariables:    map[string]any{},
	}
	for k, v := range globals {
		rc.WithGlobalVariable(k, v)
	}
	return runtime.WithWorkflowRuntimeContext(context.Background(), rc)
}

func TestGateExecutePass(t *testing.T) {
	t.Setenv("PLAITA_SECRET_TOKEN", "leak-me") // must not reach the command
	node := &Gate{
		BaseNode: BaseNode{ID: "g1"},
		Command:  []any{"echo", "hello"},
	}
	res, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out, ok := res.Result.(map[string]any)
	if !ok {
		t.Fatalf("result type %T", res.Result)
	}
	if out["passed"] != true {
		t.Errorf("passed = %v", out["passed"])
	}
	if out["gate"] != "quality-gate" {
		t.Errorf("gate = %v, want default quality-gate", out["gate"])
	}
	if out["exit_code"] != 0 {
		t.Errorf("exit_code = %v", out["exit_code"])
	}
	if out["stdout"] != "hello\n" {
		t.Errorf("stdout = %q", out["stdout"])
	}
	if out["stderr"] != "" {
		t.Errorf("stderr = %q", out["stderr"])
	}
	if out["retries"] != 0 {
		t.Errorf("retries = %v", out["retries"])
	}
	if out["dry_run"] != false {
		t.Errorf("dry_run = %v", out["dry_run"])
	}
}

func TestGateExecuteFailKeepsStreams(t *testing.T) {
	node := &Gate{
		BaseNode: BaseNode{ID: "g1"},
		Command:  []any{"sh", "-c", "echo out; echo err 1>&2; exit 3"},
		GateName: "unit-tests",
	}
	res, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["passed"] != false {
		t.Errorf("passed = %v", out["passed"])
	}
	if out["exit_code"] != 3 {
		t.Errorf("exit_code = %v, want 3", out["exit_code"])
	}
	if out["gate"] != "unit-tests" {
		t.Errorf("gate = %v", out["gate"])
	}
	if out["stdout"] != "out\n" {
		t.Errorf("stdout = %q", out["stdout"])
	}
	if out["stderr"] != "err\n" {
		t.Errorf("stderr = %q", out["stderr"])
	}
}

func TestGateCommandStringIsShlexSplit(t *testing.T) {
	node := &Gate{
		BaseNode: BaseNode{ID: "g1"},
		Command:  "echo   'a  b' \"c d\"",
	}
	res, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got := res.Result.(map[string]any)["stdout"]; got != "a  b c d\n" {
		t.Errorf("stdout = %q, want quoted args preserved", got)
	}
}

func TestGateCommandExpressions(t *testing.T) {
	node := &Gate{
		BaseNode: BaseNode{ID: "g1"},
		Command:  []any{"$INPUT.cmd", "arg-{% $INPUT.n %}"},
	}
	res, err := node.Execute(gateTestCtx(t, map[string]any{"cmd": "echo", "n": "42"}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got := res.Result.(map[string]any)["stdout"]; got != "arg-42\n" {
		t.Errorf("stdout = %q, want evaluated argv", got)
	}
}

func TestGateSubprocessEnvWhitelist(t *testing.T) {
	node := &Gate{
		BaseNode: BaseNode{ID: "g1"},
		Command:  []any{"sh", "-c", `printf '%s|%s' "$PLAITA_SECRET_TOKEN" "$GATE_TEST_VAR"`},
		EnvExtra: map[string]string{"GATE_TEST_VAR": "declared"},
	}
	res, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got := res.Result.(map[string]any)["stdout"]; got != "|declared" {
		t.Errorf("stdout = %q, want secret stripped and extra declared", got)
	}
}

func TestGateDryRun(t *testing.T) {
	// A command that would fail loudly if actually spawned.
	node := &Gate{
		BaseNode: BaseNode{ID: "g1"},
		Command:  []any{"definitely-not-a-real-binary-xyz"},
		DryRun:   true,
	}
	res, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["passed"] != true || out["exit_code"] != 0 {
		t.Errorf("dry run must pass: %v", out)
	}
	if out["stdout"] != "[dry-run]" {
		t.Errorf("stdout = %q", out["stdout"])
	}
	if out["dry_run"] != true || out["retries"] != 0 {
		t.Errorf("dry run flags = %v", out)
	}
}

func TestGateDryRunFromGlobal(t *testing.T) {
	node := &Gate{
		BaseNode: BaseNode{ID: "g1"},
		Command:  []any{"definitely-not-a-real-binary-xyz"},
	}
	res, err := node.Execute(gateTestCtx(t, map[string]any{}, map[string]any{"dry_run": true}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if out := res.Result.(map[string]any); out["dry_run"] != true {
		t.Errorf("global dry_run not honored: %v", out)
	}
}

func TestGateMissingCommand(t *testing.T) {
	node := &Gate{BaseNode: BaseNode{ID: "g1"}}
	if _, err := node.Execute(gateTestCtx(t, map[string]any{}, nil)); err == nil {
		t.Fatal("missing command must be a named error")
	} else if !strings.Contains(err.Error(), "缺少 command") {
		t.Errorf("err = %v", err)
	}
}

func TestGateEmptyCommand(t *testing.T) {
	node := &Gate{BaseNode: BaseNode{ID: "g1"}, Command: "   "}
	if _, err := node.Execute(gateTestCtx(t, map[string]any{}, nil)); err == nil {
		t.Fatal("empty command must be a named error")
	} else if !strings.Contains(err.Error(), "命令为空") {
		t.Errorf("err = %v", err)
	}
}

func TestGateTimeoutExitCode124(t *testing.T) {
	node := &Gate{
		BaseNode:    BaseNode{ID: "g1"},
		Command:     []any{"sleep", "5"},
		TimeoutSecs: 1,
	}
	res, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["exit_code"] != 124 {
		t.Errorf("exit_code = %v, want 124", out["exit_code"])
	}
	if out["passed"] != false {
		t.Errorf("timed-out gate must not pass: %v", out)
	}
}

func TestGateTimeoutExpressionValidatedAfterEval(t *testing.T) {
	cases := []struct {
		name    string
		timeout any
		wantErr bool
	}{
		{"zero", 0, true},
		{"negative", -3, true},
		{"bool", true, true},
		{"string-number", "600", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			node := &Gate{
				BaseNode:    BaseNode{ID: "g1"},
				Command:     []any{"echo", "ok"},
				TimeoutSecs: "$INPUT.t",
			}
			_, err := node.Execute(gateTestCtx(t, map[string]any{"t": tc.timeout}, nil))
			if (err != nil) != tc.wantErr {
				t.Fatalf("timeout %v: err = %v, wantErr %v", tc.timeout, err, tc.wantErr)
			}
			if err != nil && !strings.Contains(err.Error(), "positive number of seconds") {
				t.Errorf("err = %v, want named fail-closed message", err)
			}
		})
	}
}

func TestGateValidateLiteralTimeout(t *testing.T) {
	cases := []struct {
		name    string
		timeout any
		wantErr bool
	}{
		{"nil default", nil, false},
		{"deferred expression", "$INPUT.gate_timeout_secs", false},
		{"positive int", 600, false},
		{"numeric string", "30", false},
		{"zero", 0, true},
		{"negative", -1, true},
		{"bool", false, true},
		{"garbage string", "soon", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := &Gate{TimeoutSecs: tc.timeout}
			err := g.Validate()
			if (err != nil) != tc.wantErr {
				t.Fatalf("Validate() = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestGateRetries(t *testing.T) {
	script := "test -f marker && exit 0; touch marker; exit 1"

	failing := &Gate{
		BaseNode:   BaseNode{ID: "g1"},
		Command:    []any{"sh", "-c", script},
		Cwd:        t.TempDir(),
		MaxRetries: 0,
	}
	res, err := failing.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["passed"] != false || out["retries"] != 0 {
		t.Errorf("no-retry run = %v", out)
	}

	// Each retry re-executes the command: the marker the first attempt left
	// behind makes the second attempt pass.
	retrying := &Gate{
		BaseNode:   BaseNode{ID: "g2"},
		Command:    []any{"sh", "-c", script},
		Cwd:        t.TempDir(),
		MaxRetries: 2,
	}
	res, err = retrying.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute retry: %v", err)
	}
	out = res.Result.(map[string]any)
	if out["passed"] != true {
		t.Errorf("retry run must pass once command succeeds: %v", out)
	}
	if out["retries"] != 1 {
		t.Errorf("retries = %v, want 1", out["retries"])
	}
}

func TestGateOutputClipping(t *testing.T) {
	long := strings.Repeat("x", 5000)
	node := &Gate{
		BaseNode: BaseNode{ID: "g1"},
		Command:  []any{"echo", long},
	}
	res, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	stdout := res.Result.(map[string]any)["stdout"].(string)
	// stdout = 5001 runes (\n included); cap 4000 → head 1000 + marker + tail 3000.
	if !strings.HasPrefix(stdout, strings.Repeat("x", 1000)) {
		t.Error("clipped stdout lost the head")
	}
	if !strings.HasSuffix(stdout, strings.Repeat("x", 2999)+"\n") {
		t.Error("clipped stdout lost the tail (diagnostics live at the tail)")
	}
	if !strings.Contains(stdout, "…[省略 1001 字符]…") {
		t.Errorf("clipped stdout missing omission marker: %q", stdout[:60])
	}
}

func TestGateCancelledBeforeAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	node := &Gate{
		BaseNode: BaseNode{ID: "g1"},
		Command:  []any{"echo", "nope"},
	}
	if _, err := node.Execute(ctx); err == nil {
		t.Fatal("cancelled context must abort before running the command")
	}
}

func TestGateShlexSplit(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		want    []string
		wantErr string
	}{
		{"plain", "a b c", []string{"a", "b", "c"}, ""},
		{"extra spaces", "  a   b  ", []string{"a", "b"}, ""},
		{"single quotes keep spaces", "a 'b  c' d", []string{"a", "b  c", "d"}, ""},
		{"double quotes", `a "b c" d`, []string{"a", "b c", "d"}, ""},
		{"quotes mid-word", `a"b c"d e`, []string{"ab cd", "e"}, ""},
		{"apostrophe opens quote (Python shlex semantics)", "it's", nil, "No closing quotation"},
		{"escaped space", `back\ slash`, []string{"back slash"}, ""},
		{"sq wrapping dq", `'"quoted"'`, []string{`"quoted"`}, ""},
		{"dq literal backslash-a", `"\\a"`, []string{"\\a"}, ""},
		{"dq escaped quote", `"a\"b"`, []string{`a"b`}, ""},
		{"empty", "", []string{}, ""},
		{"whitespace only", " \t\n ", []string{}, ""},
		{"empty quotes are a token", "a '' b", []string{"a", "", "b"}, ""},
		{"unterminated sq", "'abc", nil, "No closing quotation"},
		{"unterminated dq", `"abc`, nil, "No closing quotation"},
		{"trailing backslash", `abc\`, nil, "No escaped character"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := gateShlexSplit(tc.in)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("shlex split: %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("tokens = %#v, want %#v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("tokens = %#v, want %#v", got, tc.want)
				}
			}
		})
	}
}

func TestGateJSONRoundTripAndRegistration(t *testing.T) {
	doc := `{"type":"gate","id":"g1","command":["echo","hi"],"gate_name":"build",
		"timeout_secs":30,"max_retries":2,"dry_run":true,"cwd":"/tmp"}`
	node := new(Gate)
	if err := json.Unmarshal([]byte(doc), node); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if node.ID != "g1" || node.GateName != "build" || node.MaxRetries != 2 ||
		!node.DryRun || node.Cwd != "/tmp" {
		t.Errorf("fields = %+v", node)
	}
	if cmd, ok := node.Command.([]any); !ok || len(cmd) != 2 {
		t.Errorf("command = %#v", node.Command)
	}
	if n, ok := node.TimeoutSecs.(float64); !ok || n != 30 {
		t.Errorf("timeout_secs = %#v", node.TimeoutSecs)
	}
	if workflow.GetNodeFactory(nodeTypeGate) == nil {
		t.Error("gate node type not registered")
	}
}
