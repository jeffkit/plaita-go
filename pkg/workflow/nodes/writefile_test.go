package nodes

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/internal/types"
)

func writefileTestContext(t *testing.T, input map[string]any) context.Context {
	t.Helper()
	return runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput:      input,
		ExecuteNodeResults: map[string]any{},
		EnvVariables:       map[string]any{},
		GlobalVariables:    map[string]any{},
	})
}

// writefileClearEnv neutralizes all jail sources except the per-node field;
// t.Setenv also makes the test fail if it tries to run in parallel.
func writefileClearEnv(t *testing.T) {
	t.Helper()
	t.Setenv(writefileRootEnv, "")
	t.Setenv(writefileOptOutEnv, "")
	t.Setenv(writefileProjectRootEn, "")
	prev := writefileWorkspaceRootOverride
	writefileWorkspaceRootOverride = ""
	t.Cleanup(func() { writefileWorkspaceRootOverride = prev })
}

func writefileResolvedTempRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatalf("EvalSymlinks(%s): %v", root, err)
	}
	return resolved
}

func TestWritefileWritesInsideJail(t *testing.T) {
	writefileClearEnv(t)
	root := writefileResolvedTempRoot(t)
	cases := []struct {
		name    string
		path    any
		content any
		want    string
		wantNum int
	}{
		{name: "relative_write_with_parent_mkdir", path: "out/deep/report.txt", content: "你好，plaita",
			want: "你好，plaita", wantNum: len("你好，plaita")},
		{name: "flat_relative", path: "flat.txt", content: "plain", want: "plain", wantNum: 5},
		{name: "absolute_inside_jail_allowed", path: filepath.Join(root, "inside", "abs.txt"), content: "abs",
			want: "abs", wantNum: 3},
		{name: "dotdot_staying_inside_allowed", path: "a/../b.txt", content: "kept", want: "kept", wantNum: 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			node := &WriteFile{
				BaseNode:      BaseNode{ID: "wf1"},
				Path:          tc.path,
				Content:       tc.content,
				WorkspaceRoot: root,
			}
			res, err := node.Execute(writefileTestContext(t, map[string]any{}))
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			out, ok := res.Result.(map[string]any)
			if !ok {
				t.Fatalf("result type %T", res.Result)
			}
			gotPath, _ := out["path"].(string)
			if !strings.HasPrefix(gotPath, root+string(os.PathSeparator)) {
				t.Errorf("path %q outside root %q", gotPath, root)
			}
			data, err := os.ReadFile(gotPath)
			if err != nil {
				t.Fatalf("read written file: %v", err)
			}
			if string(data) != tc.want {
				t.Errorf("content = %q, want %q", data, tc.want)
			}
			if size, _ := out["size"].(int); size != tc.wantNum {
				t.Errorf("size = %v, want %d (utf-8 bytes)", out["size"], tc.wantNum)
			}
		})
	}
}

func TestWritefileEscapeRejected(t *testing.T) {
	writefileClearEnv(t)
	root := writefileResolvedTempRoot(t)
	outsideDir := writefileResolvedTempRoot(t)
	outsideFile := filepath.Join(outsideDir, "existing.txt")
	if err := os.WriteFile(outsideFile, []byte("outside"), 0o644); err != nil {
		t.Fatal(err)
	}
	// symlink file inside the jail → existing file outside
	if err := os.Symlink(outsideFile, filepath.Join(root, "link.txt")); err != nil {
		t.Fatal(err)
	}
	// symlinked directory inside the jail → outside directory
	if err := os.Symlink(outsideDir, filepath.Join(root, "linkdir")); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		path any
	}{
		{name: "dotdot_escape", path: "../escaped.txt"},
		{name: "deep_dotdot_escape", path: "a/b/../../../escaped.txt"},
		{name: "absolute_outside", path: filepath.Join(outsideDir, "abs-escape.txt")},
		{name: "symlink_file_out", path: "link.txt"},
		{name: "symlink_dir_out", path: "linkdir/escape.txt"},
		{name: "absolute_dotdot_out", path: filepath.Join(root, "..", "sneak.txt")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			node := &WriteFile{
				BaseNode:      BaseNode{ID: "wf1"},
				Path:          tc.path,
				Content:       "boom",
				WorkspaceRoot: root,
			}
			_, err := node.Execute(writefileTestContext(t, map[string]any{}))
			if err == nil {
				t.Fatalf("path %v must be rejected", tc.path)
			}
			if !strings.Contains(err.Error(), "escapes workspace_root") &&
				!strings.Contains(err.Error(), "fail-closed") {
				t.Errorf("unexpected error: %v", err)
			}
			// nothing may have landed outside
			if _, statErr := os.Stat(filepath.Join(outsideDir, "escaped.txt")); statErr == nil {
				t.Error("escaped.txt was created outside the jail")
			}
			if _, statErr := os.Stat(filepath.Join(outsideDir, "abs-escape.txt")); statErr == nil {
				t.Error("abs-escape.txt was created outside the jail")
			}
			if _, statErr := os.Stat(filepath.Join(outsideDir, "sneak.txt")); statErr == nil {
				t.Error("sneak.txt was created outside the jail")
			}
			if b, statErr := os.ReadFile(outsideFile); statErr == nil && string(b) != "outside" {
				t.Errorf("existing outside file was modified: %q", b)
			}
			if _, statErr := os.Stat(filepath.Join(outsideDir, "escape.txt")); statErr == nil {
				t.Error("linkdir/escape.txt was created outside the jail")
			}
		})
	}
}

func TestWritefileNoRootFailClosed(t *testing.T) {
	writefileClearEnv(t)
	// force the "nothing derivable" branch of the default jail
	prev := writefileDefaultRootCandidates
	writefileDefaultRootCandidates = func() []string { return nil }
	t.Cleanup(func() { writefileDefaultRootCandidates = prev })

	node := &WriteFile{BaseNode: BaseNode{ID: "wf1"}, Path: "any.txt", Content: "x"}
	_, err := node.Execute(writefileTestContext(t, map[string]any{}))
	if err == nil || !strings.Contains(err.Error(), "fail-closed") {
		t.Fatalf("no-root write must be refused (fail-closed), got %v", err)
	}
}

func TestWritefileOptOutRestoresUnrestricted(t *testing.T) {
	t.Setenv(writefileRootEnv, "")
	t.Setenv(writefileOptOutEnv, "1")
	t.Setenv(writefileProjectRootEn, "")
	outside := filepath.Join(t.TempDir(), "free.txt")
	node := &WriteFile{BaseNode: BaseNode{ID: "wf1"}, Path: outside, Content: "trusted"}
	if _, err := node.Execute(writefileTestContext(t, map[string]any{})); err != nil {
		t.Fatalf("opt-out write: %v", err)
	}
	if b, _ := os.ReadFile(outside); string(b) != "trusted" {
		t.Errorf("content = %q", b)
	}
}

func TestWritefileRootPrecedence(t *testing.T) {
	writefileClearEnv(t)
	rootNode := writefileResolvedTempRoot(t)
	rootOverride := writefileResolvedTempRoot(t)
	rootEnv := writefileResolvedTempRoot(t)

	t.Setenv(writefileRootEnv, rootEnv)
	SetWritefileWorkspaceRootOverride(rootOverride)
	t.Cleanup(func() { SetWritefileWorkspaceRootOverride("") })

	t.Run("node_field_wins", func(t *testing.T) {
		node := &WriteFile{BaseNode: BaseNode{ID: "wf1"}, Path: "where.txt",
			Content: "node", WorkspaceRoot: rootNode}
		res, err := node.Execute(writefileTestContext(t, map[string]any{}))
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if p, _ := res.Result.(map[string]any)["path"].(string); !strings.HasPrefix(p, rootNode) {
			t.Errorf("path = %q, want under %q", p, rootNode)
		}
	})
	t.Run("override_beats_env", func(t *testing.T) {
		node := &WriteFile{BaseNode: BaseNode{ID: "wf1"}, Path: "where.txt", Content: "override"}
		res, err := node.Execute(writefileTestContext(t, map[string]any{}))
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if p, _ := res.Result.(map[string]any)["path"].(string); !strings.HasPrefix(p, rootOverride) {
			t.Errorf("path = %q, want under %q", p, rootOverride)
		}
	})
}

func TestWritefileJSONMode(t *testing.T) {
	writefileClearEnv(t)
	root := writefileResolvedTempRoot(t)

	t.Run("default_indent_two_no_html_escape", func(t *testing.T) {
		node := &WriteFile{BaseNode: BaseNode{ID: "wf1"}, Path: "payload.json",
			JSONMode: true, WorkspaceRoot: root,
			Content: map[string]any{"tag": "<a&b>", "判": "值"}}
		if _, err := node.Execute(writefileTestContext(t, map[string]any{})); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		p := filepath.Join(root, "payload.json")
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		s := string(raw)
		if !strings.Contains(s, "<a&b>") {
			t.Errorf("HTML-escaped content: %s", s)
		}
		if strings.Contains(s, `\u`) {
			t.Errorf("ascii-escaped content: %s", s)
		}
		if !strings.Contains(s, "\n  \"tag\"") {
			t.Errorf("expected 2-space indent, got: %s", s)
		}
		if strings.HasSuffix(s, "\n") {
			t.Errorf("trailing newline (json.dumps has none): %q", s)
		}
		var back map[string]any
		if err := json.Unmarshal(raw, &back); err != nil {
			t.Fatalf("written file is not valid json: %v", err)
		}
	})
	t.Run("explicit_indent_zero", func(t *testing.T) {
		zero := 0
		node := &WriteFile{BaseNode: BaseNode{ID: "wf1"}, Path: "compact.json",
			JSONMode: true, Indent: &zero, WorkspaceRoot: root,
			Content: []any{1, 2}}
		if _, err := node.Execute(writefileTestContext(t, map[string]any{})); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		raw, _ := os.ReadFile(filepath.Join(root, "compact.json"))
		if strings.Contains(string(raw), "\n") {
			t.Errorf("expected compact json, got %q", raw)
		}
	})
	t.Run("missing_content_is_empty_json_string", func(t *testing.T) {
		node := &WriteFile{BaseNode: BaseNode{ID: "wf1"}, Path: "empty.json",
			JSONMode: true, WorkspaceRoot: root}
		if _, err := node.Execute(writefileTestContext(t, map[string]any{})); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		raw, _ := os.ReadFile(filepath.Join(root, "empty.json"))
		if string(raw) != `""` {
			t.Errorf("content = %q, want %q", raw, `""`)
		}
	})
}

func TestWritefileExpressionsAndScalars(t *testing.T) {
	writefileClearEnv(t)
	root := writefileResolvedTempRoot(t)
	cases := []struct {
		name    string
		path    any
		content any
		input   map[string]any
		wantRel string
		want    string
	}{
		{name: "path_and_content_expressions", path: "$INPUT.name", content: "$INPUT.body",
			input: map[string]any{"name": "gen/out.txt", "body": "from-input"}, wantRel: "gen/out.txt", want: "from-input"},
		{name: "numeric_content_stringified", path: "num.txt", content: 42, input: map[string]any{},
			wantRel: "num.txt", want: "42"},
		{name: "missing_content_empty", path: "none.txt", content: nil, input: map[string]any{},
			wantRel: "none.txt", want: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			node := &WriteFile{BaseNode: BaseNode{ID: "wf1"}, Path: tc.path,
				Content: tc.content, WorkspaceRoot: root}
			if _, err := node.Execute(writefileTestContext(t, tc.input)); err != nil {
				t.Fatalf("Execute: %v", err)
			}
			raw, err := os.ReadFile(filepath.Join(root, tc.wantRel))
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if string(raw) != tc.want {
				t.Errorf("content = %q, want %q", raw, tc.want)
			}
		})
	}
}

func TestWritefileCtxCancelled(t *testing.T) {
	writefileClearEnv(t)
	root := writefileResolvedTempRoot(t)
	node := &WriteFile{BaseNode: BaseNode{ID: "wf1"}, Path: "x.txt", WorkspaceRoot: root}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := node.Execute(ctx); err == nil {
		t.Fatal("cancelled ctx must abort the write")
	}
	if _, statErr := os.Stat(filepath.Join(root, "x.txt")); statErr == nil {
		t.Error("file was written despite cancelled ctx")
	}
}

func TestWritefileMissingPath(t *testing.T) {
	writefileClearEnv(t)
	node := &WriteFile{BaseNode: BaseNode{ID: "wf1"}, WorkspaceRoot: t.TempDir()}
	if _, err := node.Execute(writefileTestContext(t, map[string]any{})); err == nil {
		t.Fatal("missing path must raise")
	}
}

func TestWritefileJSONRoundTrip(t *testing.T) {
	doc := `{"type":"writefile","id":"wf1","path":"$INPUT.p","content":"$INPUT.c",
		"json":true,"indent":4,"workspace_root":"/data/sandbox"}`
	node := new(WriteFile)
	if err := json.Unmarshal([]byte(doc), node); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !node.JSONMode || node.Indent == nil || *node.Indent != 4 {
		t.Errorf("json/indent = %v %v", node.JSONMode, node.Indent)
	}
	if node.WorkspaceRoot != "/data/sandbox" {
		t.Errorf("workspace_root = %q", node.WorkspaceRoot)
	}
	if node.GetType() != types.NodeType("writefile") {
		t.Errorf("type = %v", node.GetType())
	}
}
