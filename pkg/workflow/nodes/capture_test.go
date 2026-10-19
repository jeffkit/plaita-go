package nodes

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func TestCaptureExecutePass(t *testing.T) {
	t.Setenv("PLAITA_SECRET_TOKEN", "leak-me") // must not reach the command
	node := &Capture{
		BaseNode: BaseNode{ID: "c1"},
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
	if out["ok"] != true || out["exit_code"] != 0 {
		t.Errorf("ok/exit = %v/%v", out["ok"], out["exit_code"])
	}
	if out["stdout"] != "hello\n" {
		t.Errorf("stdout = %q", out["stdout"])
	}
	if out["stderr"] != "" {
		t.Errorf("stderr = %q", out["stderr"])
	}
	if out["dry_run"] != false {
		t.Errorf("dry_run = %v", out["dry_run"])
	}
}

func TestCaptureFailureIsNotNodeFailure(t *testing.T) {
	node := &Capture{
		BaseNode: BaseNode{ID: "c1"},
		Command:  []any{"sh", "-c", "echo boom 1>&2; exit 3"},
	}
	res, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("command failure must not be a node failure: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["ok"] != false || out["exit_code"] != 3 {
		t.Errorf("ok/exit_code = %v/%v, want false/3", out["ok"], out["exit_code"])
	}
	if out["stderr"] != "boom\n" {
		t.Errorf("stderr = %q", out["stderr"])
	}
}

func TestCaptureEnvWhitelistAndDeclaredExtra(t *testing.T) {
	node := &Capture{
		BaseNode: BaseNode{ID: "c1"},
		Command:  []any{"sh", "-c", `printf '%s|%s' "$PLAITA_SECRET_TOKEN" "$CAP_TEST_VAR"`},
		Env:      map[string]any{"CAP_TEST_VAR": "declared"},
	}
	res, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got := res.Result.(map[string]any)["stdout"]; got != "|declared" {
		t.Errorf("stdout = %q, want host secret stripped and extra declared", got)
	}
}

func TestCaptureCommandForms(t *testing.T) {
	cases := []struct {
		name    string
		command any
		input   map[string]any
		want    string
	}{
		{"literal list", []any{"echo", "a", "b"}, nil, "a b\n"},
		{"list with expression element", []any{"echo", "$INPUT.word"}, map[string]any{"word": "hi"}, "hi\n"},
		{"shlex string", "echo   'a  b' \"c d\"", nil, "a  b c d\n"},
		{"expression evaluating to list", "$INPUT.argv", map[string]any{"argv": []any{"echo", "from-list"}}, "from-list\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			node := &Capture{BaseNode: BaseNode{ID: "c1"}, Command: tc.command}
			res, err := node.Execute(gateTestCtx(t, tc.input, nil))
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			if got := res.Result.(map[string]any)["stdout"]; got != tc.want {
				t.Errorf("stdout = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestCaptureCwd(t *testing.T) {
	dir := t.TempDir()
	node := &Capture{
		BaseNode: BaseNode{ID: "c1"},
		Command:  []any{"sh", "-c", "pwd"},
		Cwd:      dir,
	}
	res, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	got := strings.TrimSuffix(res.Result.(map[string]any)["stdout"].(string), "\n")
	// macOS /var → /private/var symlink: compare resolved paths.
	want, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatalf("EvalSymlinks: %v", err)
	}
	if got != want {
		t.Errorf("pwd = %q, want cwd %q", got, want)
	}
}

func TestCaptureStdin(t *testing.T) {
	node := &Capture{
		BaseNode: BaseNode{ID: "c1"},
		Command:  []any{"sh", "-c", "read line; echo got:$line"},
		Stdin:    "ping",
	}
	res, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got := res.Result.(map[string]any)["stdout"]; got != "got:ping\n" {
		t.Errorf("stdout = %q, want stdin delivered", got)
	}
}

func TestCaptureDryRun(t *testing.T) {
	node := &Capture{
		BaseNode: BaseNode{ID: "c1"},
		Command:  []any{"echo", "a b"},
		DryRun:   true,
	}
	res, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["ok"] != true || out["exit_code"] != 0 || out["dry_run"] != true {
		t.Errorf("dry run flags = %v", out)
	}
	if out["stdout"] != "[dry-run] would exec: echo a b" {
		t.Errorf("stdout = %q", out["stdout"])
	}
}

func TestCaptureDryRunFromGlobal(t *testing.T) {
	node := &Capture{
		BaseNode: BaseNode{ID: "c1"},
		Command:  []any{"sh", "-c", "touch should-not-exist"},
	}
	res, err := node.Execute(gateTestCtx(t, map[string]any{}, map[string]any{"dry_run": true}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if out := res.Result.(map[string]any); out["dry_run"] != true {
		t.Errorf("global dry_run not honored: %v", out)
	}
}

func TestCaptureConfigErrors(t *testing.T) {
	cases := []struct {
		name    string
		command any
		wantErr string
	}{
		{"missing command", nil, "缺少 command"},
		{"empty shlex string", "   ", "命令为空"},
		{"empty list", []any{}, "命令为空"},
		{"unterminated quote", "echo 'oops", "No closing quotation"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			node := &Capture{BaseNode: BaseNode{ID: "c1"}, Command: tc.command}
			if _, err := node.Execute(gateTestCtx(t, map[string]any{}, nil)); err == nil {
				t.Fatal("config error must be a named error")
			} else if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("err = %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestCaptureTimeoutExitCode124(t *testing.T) {
	node := &Capture{
		BaseNode:    BaseNode{ID: "c1"},
		Command:     []any{"sh", "-c", "echo partial; sleep 5"},
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
	if out["ok"] != false {
		t.Errorf("timed-out capture must not be ok: %v", out)
	}
	if out["stdout"] != "partial\n" {
		t.Errorf("stdout = %q, want partial output kept", out["stdout"])
	}
}

func TestCaptureTimeoutExpressionValidatedAfterEval(t *testing.T) {
	cases := []struct {
		name    string
		timeout any
		wantErr bool
	}{
		{"zero", 0, true},
		{"negative", -3, true},
		{"bool", true, true},
		{"numeric string", "30", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			node := &Capture{
				BaseNode:    BaseNode{ID: "c1"},
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

func TestCaptureValidateLiteralTimeout(t *testing.T) {
	cases := []struct {
		name    string
		timeout any
		wantErr bool
	}{
		{"nil default", nil, false},
		{"deferred expression", "$INPUT.t", false},
		{"positive int", 120, false},
		{"zero", 0, true},
		{"negative", -1, true},
		{"bool", false, true},
		{"garbage string", "soon", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &Capture{TimeoutSecs: tc.timeout}
			err := c.Validate()
			if (err != nil) != tc.wantErr {
				t.Fatalf("Validate() = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestCaptureOutputClipping(t *testing.T) {
	node := &Capture{
		BaseNode: BaseNode{ID: "c1"},
		Command: []any{"sh", "-c",
			"echo " + strings.Repeat("o", 5000) + "; echo " + strings.Repeat("e", 3000) + " 1>&2"},
	}
	res, err := node.Execute(gateTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out := res.Result.(map[string]any)
	stdout := out["stdout"].(string)
	// stdout = 5001 runes; cap 4000 → head 1000 + marker + tail 3000.
	if !strings.HasPrefix(stdout, strings.Repeat("o", 1000)) || !strings.Contains(stdout, "…[省略 1001 字符]…") {
		t.Errorf("stdout not clipped with marker: %q", stdout[:60])
	}
	stderr := out["stderr"].(string)
	// stderr = 3001 runes; cap 2000 → head 500 + marker + tail 1500.
	if !strings.Contains(stderr, "…[省略 1001 字符]…") {
		t.Errorf("stderr not clipped with marker: %q", stderr[:60])
	}
	if !strings.HasSuffix(stderr, strings.Repeat("e", 1499)+"\n") {
		t.Error("clipped stderr lost the tail")
	}
}

func TestCaptureCancelledBeforeSpawn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	node := &Capture{BaseNode: BaseNode{ID: "c1"}, Command: []any{"echo", "nope"}}
	if _, err := node.Execute(ctx); err == nil {
		t.Fatal("cancelled context must abort before running the command")
	}
}

func TestCaptureJSONRoundTripAndRegistration(t *testing.T) {
	doc := `{"type":"capture","id":"c1","command":["echo","hi"],"cwd":"/tmp",
		"timeout_secs":30,"env":{"K":"v"},"stdin":"x","dry_run":true}`
	node := new(Capture)
	if err := json.Unmarshal([]byte(doc), node); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if node.ID != "c1" || !node.DryRun || node.Cwd != "/tmp" || node.Stdin != "x" {
		t.Errorf("fields = %+v", node)
	}
	if cmd, ok := node.Command.([]any); !ok || len(cmd) != 2 {
		t.Errorf("command = %#v", node.Command)
	}
	env, ok := node.Env.(map[string]any)
	if !ok || env["K"] != "v" {
		t.Errorf("env = %#v", node.Env)
	}
	if n, ok := node.TimeoutSecs.(float64); !ok || n != 30 {
		t.Errorf("timeout_secs = %#v", node.TimeoutSecs)
	}
	if err := node.Validate(); err != nil {
		t.Errorf("Validate: %v", err)
	}
	if workflow.GetNodeFactory(nodeTypeCapture) == nil {
		t.Error("capture node type not registered")
	}
}
