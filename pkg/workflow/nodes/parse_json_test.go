package nodes

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/internal/types"
)

func parseJSONTestContext(t *testing.T, input map[string]any) context.Context {
	t.Helper()
	return runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput:      input,
		ExecuteNodeResults: map[string]any{},
		EnvVariables:       map[string]any{},
		GlobalVariables:    map[string]any{},
	})
}

func parseJSONOut(t *testing.T, res any) map[string]any {
	t.Helper()
	m, ok := res.(map[string]any)
	if !ok {
		t.Fatalf("result type %T", res)
	}
	return m
}

func TestParseJSONExecute(t *testing.T) {
	cases := []struct {
		name   string
		node   *ParseJSON
		input  map[string]any
		verify func(t *testing.T, out map[string]any, err error)
	}{
		{
			name: "last_json_line_wins",
			node: &ParseJSON{BaseNode: BaseNode{ID: "pj1"},
				Text: `{"verdict": "deny", "why": "earlier"}
				{"verdict": "allow", "why": "final"}`},
			input: map[string]any{},
			verify: func(t *testing.T, out map[string]any, err error) {
				if err != nil {
					t.Fatalf("Execute: %v", err)
				}
				if out["verdict"] != "allow" || out["why"] != "final" {
					t.Errorf("out = %v", out)
				}
				if out["parse_ok"] != true || out["parse_error"] != "" {
					t.Errorf("parse markers = %v %v", out["parse_ok"], out["parse_error"])
				}
			},
		},
		{
			name: "backtick_wrapped_line",
			node: &ParseJSON{BaseNode: BaseNode{ID: "pj1"},
				Text: "some preamble\n```{\"verdict\": \"allow\"}```\n"},
			input: map[string]any{},
			verify: func(t *testing.T, out map[string]any, err error) {
				if err != nil {
					t.Fatalf("Execute: %v", err)
				}
				if out["verdict"] != "allow" {
					t.Errorf("out = %v", out)
				}
			},
		},
		{
			// issue-pipeline #43: prose braces must not break extraction —
			// the LAST { to LAST } slice is the JSON.
			name: "prose_braces_last_slice",
			node: &ParseJSON{BaseNode: BaseNode{ID: "pj1"},
				Text: `检查结论 type={}, model={}。最终 {"verdict": "deny", "reason": "risky"}`},
			input: map[string]any{},
			verify: func(t *testing.T, out map[string]any, err error) {
				if err != nil {
					t.Fatalf("Execute: %v", err)
				}
				if out["verdict"] != "deny" || out["reason"] != "risky" {
					t.Errorf("out = %v", out)
				}
			},
		},
		{
			name:  "whole_text_is_json",
			node:  &ParseJSON{BaseNode: BaseNode{ID: "pj1"}, Text: `{"verdict": "allow", "score": 5}`},
			input: map[string]any{},
			verify: func(t *testing.T, out map[string]any, err error) {
				if err != nil {
					t.Fatalf("Execute: %v", err)
				}
				if out["score"] != float64(5) {
					t.Errorf("out = %v", out)
				}
			},
		},
		{
			name:  "text_expression_evaluated",
			node:  &ParseJSON{BaseNode: BaseNode{ID: "pj1"}, Text: "$INPUT.reply"},
			input: map[string]any{"reply": `{"verdict": "allow"}`},
			verify: func(t *testing.T, out map[string]any, err error) {
				if err != nil {
					t.Fatalf("Execute: %v", err)
				}
				if out["verdict"] != "allow" {
					t.Errorf("out = %v", out)
				}
			},
		},
		{
			name: "no_json_fails_to_default",
			node: &ParseJSON{BaseNode: BaseNode{ID: "pj1"}, Text: "absolutely no braces here",
				Default: map[string]any{"verdict": "skip", "notes": "fallback"}},
			input: map[string]any{},
			verify: func(t *testing.T, out map[string]any, err error) {
				if err != nil {
					t.Fatalf("Execute: %v", err)
				}
				if out["verdict"] != "skip" || out["parse_ok"] != false {
					t.Errorf("out = %v", out)
				}
				if pe, _ := out["parse_error"].(string); !strings.HasPrefix(pe, "解析失败: absolutely") {
					t.Errorf("parse_error = %q", pe)
				}
				if notes, _ := out["notes"].(string); notes != "fallback；解析失败: absolutely no braces here" {
					t.Errorf("notes = %q", notes)
				}
			},
		},
		{
			name: "empty_notes_gets_detail",
			node: &ParseJSON{BaseNode: BaseNode{ID: "pj1"}, Text: "nothing",
				Default: map[string]any{"notes": ""}},
			input: map[string]any{},
			verify: func(t *testing.T, out map[string]any, err error) {
				if err != nil {
					t.Fatalf("Execute: %v", err)
				}
				if notes, _ := out["notes"].(string); notes != "解析失败: nothing" {
					t.Errorf("notes = %q", notes)
				}
			},
		},
		{
			name:  "fail_without_default_raises",
			node:  &ParseJSON{BaseNode: BaseNode{ID: "pj1"}, Text: "nothing"},
			input: map[string]any{},
			verify: func(t *testing.T, out map[string]any, err error) {
				if err == nil {
					t.Fatal("must raise loudly without default")
				}
			},
		},
		{
			name: "verdict_in_choices_ok",
			node: &ParseJSON{BaseNode: BaseNode{ID: "pj1"}, Text: `{"verdict": "deny"}`,
				Choices: []string{"allow", "deny"}},
			input: map[string]any{},
			verify: func(t *testing.T, out map[string]any, err error) {
				if err != nil {
					t.Fatalf("Execute: %v", err)
				}
				if out["verdict"] != "deny" || out["parse_ok"] != true {
					t.Errorf("out = %v", out)
				}
			},
		},
		{
			name: "verdict_not_in_choices_falls_back",
			node: &ParseJSON{BaseNode: BaseNode{ID: "pj1"}, Text: `{"verdict": "maybe"}`,
				Choices: []string{"allow", "deny"},
				Default: map[string]any{"verdict": "skip"}},
			input: map[string]any{},
			verify: func(t *testing.T, out map[string]any, err error) {
				if err != nil {
					t.Fatalf("Execute: %v", err)
				}
				if out["verdict"] != "skip" || out["parse_ok"] != false {
					t.Errorf("out = %v", out)
				}
				if pe, _ := out["parse_error"].(string); pe != `输出非法 verdict: "maybe"` {
					t.Errorf("parse_error = %q", pe)
				}
			},
		},
		{
			name: "missing_verdict_fails_choices",
			node: &ParseJSON{BaseNode: BaseNode{ID: "pj1"}, Text: `{"other": 1}`,
				Choices: []string{"allow"},
				Default: map[string]any{"verdict": "skip"}},
			input: map[string]any{},
			verify: func(t *testing.T, out map[string]any, err error) {
				if err != nil {
					t.Fatalf("Execute: %v", err)
				}
				if pe, _ := out["parse_error"].(string); pe != "输出非法 verdict: None" {
					t.Errorf("parse_error = %q", pe)
				}
			},
		},
		{
			name: "join_fields_flattened",
			node: &ParseJSON{BaseNode: BaseNode{ID: "pj1"},
				Text:       `{"verdict": "allow", "tags": ["a", "b", 3], "n": 1, "single": ["x"]}`,
				JoinFields: []string{"tags", "missing_field", "n", "single"}},
			input: map[string]any{},
			verify: func(t *testing.T, out map[string]any, err error) {
				if err != nil {
					t.Fatalf("Execute: %v", err)
				}
				if out["tags_str"] != "a; b; 3" {
					t.Errorf("tags_str = %v", out["tags_str"])
				}
				if out["single_str"] != "x" {
					t.Errorf("single_str = %v", out["single_str"])
				}
				if _, ok := out["missing_field_str"]; ok {
					t.Errorf("missing field must be ignored: %v", out)
				}
				if _, ok := out["n_str"]; ok {
					t.Errorf("non-list field must be ignored: %v", out)
				}
			},
		},
		{
			name: "falsy_text_becomes_empty_and_fails",
			node: &ParseJSON{BaseNode: BaseNode{ID: "pj1"}, Text: 0,
				Default: map[string]any{"verdict": "skip"}},
			input: map[string]any{},
			verify: func(t *testing.T, out map[string]any, err error) {
				if err != nil {
					t.Fatalf("Execute: %v", err)
				}
				if pe, _ := out["parse_error"].(string); pe != "解析失败: " {
					t.Errorf("parse_error = %q", pe)
				}
			},
		},
		{
			name:  "missing_text_field_raises",
			node:  &ParseJSON{BaseNode: BaseNode{ID: "pj1"}},
			input: map[string]any{},
			verify: func(t *testing.T, out map[string]any, err error) {
				if err == nil {
					t.Fatal("missing text must raise")
				}
			},
		},
		{
			name: "invalid_default_verdict_check_non_string",
			node: &ParseJSON{BaseNode: BaseNode{ID: "pj1"}, Text: `{"verdict": 5}`,
				Choices: []string{"5"},
				Default: map[string]any{"verdict": "skip"}},
			input: map[string]any{},
			verify: func(t *testing.T, out map[string]any, err error) {
				if err != nil {
					t.Fatalf("Execute: %v", err)
				}
				if out["parse_ok"] != false {
					t.Errorf("numeric verdict must not match string choice: %v", out)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := tc.node.Execute(parseJSONTestContext(t, tc.input))
			var out map[string]any
			if res != nil && res.Result != nil {
				out = parseJSONOut(t, res.Result)
			}
			tc.verify(t, out, err)
		})
	}
}

func TestParseJSONGetType(t *testing.T) {
	p := &ParseJSON{}
	if p.GetType() != types.NodeType("parse_json") {
		t.Errorf("type = %v", p.GetType())
	}
}

func TestParseJSONJSONRoundTrip(t *testing.T) {
	doc := `{"type":"parse_json","id":"pj1","text":"$INPUT.t","choices":["allow","deny"],
		"default":{"verdict":"skip"},"join_fields":["tags"]}`
	node := new(ParseJSON)
	if err := json.Unmarshal([]byte(doc), node); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if node.Text != "$INPUT.t" || len(node.Choices) != 2 || node.Default["verdict"] != "skip" {
		t.Errorf("node = %+v", node)
	}
	if len(node.JoinFields) != 1 || node.JoinFields[0] != "tags" {
		t.Errorf("join_fields = %v", node.JoinFields)
	}
}
