package nodes

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func reportTestCtx(t *testing.T, input map[string]any, globals map[string]any) context.Context {
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

func TestReportWriteRead(t *testing.T) {
	dir := t.TempDir()
	node := &Report{
		BaseNode: BaseNode{ID: "rep1"},
		BaseDir:  dir,
		Token:    "run-tok",
		Entry:    map[string]any{"status": "ok", "score": 9},
	}
	res, err := node.Execute(reportTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	out := res.Result.(map[string]any)
	if out["written"] != true {
		t.Errorf("written = %v", out["written"])
	}
	entry := out["entry"].(map[string]any)
	if entry["status"] != "ok" || entry["score"] != 9 {
		t.Errorf("entry echo = %v", entry)
	}

	res, err = node.Execute(reportTestCtx(t, map[string]any{}, nil)) // append again
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	node.Action = "read"
	node.Entry = nil
	res, err = node.Execute(reportTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	entries := res.Result.(map[string]any)["entries"].([]any)
	if len(entries) != 2 {
		t.Fatalf("entries = %d, want 2 (jsonl append)", len(entries))
	}
	first := entries[0].(map[string]any)
	if first["status"] != "ok" {
		t.Errorf("first entry = %v", first)
	}
}

func TestReportDefaultActionIsWrite(t *testing.T) {
	dir := t.TempDir()
	node := &Report{
		BaseNode: BaseNode{ID: "rep1"},
		BaseDir:  dir,
		Token:    "tok",
		Entry:    "plain-value",
	}
	if _, err := node.Execute(reportTestCtx(t, map[string]any{}, nil)); err != nil {
		t.Fatalf("write: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "tok.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"value":"plain-value"`) {
		t.Errorf("non-dict entry must be wrapped in {value: ...}: %q", data)
	}
}

func TestReportNilEntryIsEmptiedObject(t *testing.T) {
	dir := t.TempDir()
	node := &Report{BaseNode: BaseNode{ID: "rep1"}, BaseDir: dir, Token: "tok"}
	res, err := node.Execute(reportTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	entry, ok := res.Result.(map[string]any)["entry"].(map[string]any)
	if !ok || len(entry) != 0 {
		t.Errorf("nil entry must become {}, got %#v", res.Result.(map[string]any)["entry"])
	}
}

func TestReportEntryExpression(t *testing.T) {
	dir := t.TempDir()
	node := &Report{
		BaseNode: BaseNode{ID: "rep1"},
		BaseDir:  dir,
		Token:    "tok",
		Entry:    "$INPUT.item",
	}
	ctx := reportTestCtx(t, map[string]any{"item": map[string]any{"k": "v1"}}, nil)
	res, err := node.Execute(ctx)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	entry := res.Result.(map[string]any)["entry"].(map[string]any)
	if entry["k"] != "v1" {
		t.Errorf("evaluated entry = %v", entry)
	}
}

func TestReportReadMissingFile(t *testing.T) {
	node := &Report{
		BaseNode: BaseNode{ID: "rep1"},
		BaseDir:  t.TempDir(),
		Token:    "absent",
		Action:   "read",
	}
	res, err := node.Execute(reportTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	entries := res.Result.(map[string]any)["entries"].([]any)
	if len(entries) != 0 {
		t.Errorf("entries = %v, want empty list", entries)
	}
}

func TestReportCorruptLinesSkipped(t *testing.T) {
	dir := t.TempDir()
	content := "{not json\n" + `{"ok":1}` + "\n\n   \n" + "[1,2]\n"
	if err := os.WriteFile(filepath.Join(dir, "tok.jsonl"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	node := &Report{BaseNode: BaseNode{ID: "rep1"}, BaseDir: dir, Token: "tok", Action: "read"}
	res, err := node.Execute(reportTestCtx(t, map[string]any{}, nil))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	entries := res.Result.(map[string]any)["entries"].([]any)
	if len(entries) != 2 {
		t.Fatalf("entries = %#v, want the dict and the list line only", entries)
	}
	if _, ok := entries[1].([]any); !ok {
		t.Errorf("second entry = %#v, want the parsed list", entries[1])
	}
}

func TestReportUnknownAction(t *testing.T) {
	node := &Report{
		BaseNode: BaseNode{ID: "rep1"},
		BaseDir:  t.TempDir(),
		Token:    "tok",
		Action:   "append",
	}
	_, err := node.Execute(reportTestCtx(t, map[string]any{}, nil))
	if err == nil || !strings.Contains(err.Error(), `未知 report action: "append"`) {
		t.Fatalf("err = %v, want named unknown-action error", err)
	}
}

func TestReportTokenWhitelistFailsClosed(t *testing.T) {
	dir := t.TempDir()
	for _, token := range []any{"../escape", "a/b", ".hidden", "/abs", "x%2fy"} {
		node := &Report{
			BaseNode: BaseNode{ID: "rep1"},
			BaseDir:  dir,
			Token:    token,
			Entry:    map[string]any{"sneaky": true},
		}
		if _, err := node.Execute(reportTestCtx(t, map[string]any{}, nil)); err == nil {
			t.Fatalf("token %q must be rejected", token)
		} else if !strings.Contains(err.Error(), "非法") {
			t.Errorf("token %q: err = %v, want named whitelist error", token, err)
		}
	}
	// fail-closed means nothing was ever written
	matches, _ := filepath.Glob(filepath.Join(dir, "*"))
	if len(matches) != 0 {
		t.Errorf("files created despite invalid token: %v", matches)
	}
}

func TestReportTokenFallsBackToGlobal(t *testing.T) {
	dir := t.TempDir()
	node := &Report{
		BaseNode: BaseNode{ID: "rep1"},
		BaseDir:  dir, // no token field
		Entry:    map[string]any{"k": "v"},
	}
	res, err := node.Execute(reportTestCtx(t, map[string]any{}, map[string]any{"run_token": "run-42"}))
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if res.Result.(map[string]any)["written"] != true {
		t.Errorf("written = %v", res.Result.(map[string]any)["written"])
	}
	if _, err := os.Stat(filepath.Join(dir, "run-42.jsonl")); err != nil {
		t.Errorf("global run_token not used: %v", err)
	}
}

func TestReportTokenEmptyStringFallsBackToDefault(t *testing.T) {
	dir := t.TempDir()
	node := &Report{
		BaseNode: BaseNode{ID: "rep1"},
		BaseDir:  dir,
		Token:    "", // falsy → global (missing) → "default"
		Entry:    map[string]any{},
	}
	if _, err := node.Execute(reportTestCtx(t, map[string]any{}, nil)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "default.jsonl")); err != nil {
		t.Errorf("default token file missing: %v", err)
	}
}

func TestReportDefaultDirFromRepo(t *testing.T) {
	repo := t.TempDir()
	node := &Report{
		BaseNode: BaseNode{ID: "rep1"},
		Repo:     repo,
		Token:    "tok",
		Entry:    map[string]any{"k": "v"},
	}
	if _, err := node.Execute(reportTestCtx(t, map[string]any{}, nil)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := os.Stat(filepath.Join(repo, ".flowcast", "plaita-reports", "tok.jsonl")); err != nil {
		t.Errorf("default <repo>/.flowcast/plaita-reports layout broken: %v", err)
	}
}

func TestReportUTF8RawAndNoHTMLEscape(t *testing.T) {
	dir := t.TempDir()
	node := &Report{
		BaseNode: BaseNode{ID: "rep1"},
		BaseDir:  dir,
		Token:    "tok",
		Entry:    map[string]any{"text": "中文<>&"},
	}
	if _, err := node.Execute(reportTestCtx(t, map[string]any{}, nil)); err != nil {
		t.Fatalf("write: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "tok.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	line := string(data)
	if !strings.Contains(line, "中文") {
		t.Errorf("ensure_ascii=False violated (escaped non-ASCII): %q", line)
	}
	if !strings.Contains(line, "<>&") {
		t.Errorf("HTML escaping must stay off (Python json.dumps behavior): %q", line)
	}
}

func TestReportJSONRoundTripAndRegistration(t *testing.T) {
	doc := `{"type":"report","id":"rep1","action":"read","base_dir":"/tmp/reports","token":"tok"}`
	node := new(Report)
	if err := json.Unmarshal([]byte(doc), node); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if node.ID != "rep1" || node.Action != "read" || node.BaseDir != "/tmp/reports" || node.Token != "tok" {
		t.Errorf("fields = %+v", node)
	}
	if workflow.GetNodeFactory(nodeTypeReport) == nil {
		t.Error("report node type not registered")
	}
}
