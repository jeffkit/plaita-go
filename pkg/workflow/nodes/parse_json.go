package nodes

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"strconv"
	"strings"

	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// nodeTypeParseJSON mirrors plaita-nodes ParseJsonNode.node_type ("parse_json").
const nodeTypeParseJSON types.NodeType = "parse_json"

const (
	// raw[:150] in the parse-failure detail (parse_json.py:77).
	parseJSONFailPreviewLimit = 150
	// list fields are joined with "; " into "<field>_str" (parse_json.py:87).
	parseJSONJoinSep = "; "
	// notes detail separator (parse_json.py:97).
	parseJSONNotesSep = "；"
)

// ParseJSON parses the JSON object inside an LLM reply with the robust
// candidate strategy from plaita-nodes ParseJsonNode (parse_json.py) and
// validates the "verdict" field against a whitelist. Fail-safe semantics are
// built in: when parsing or validation fails the node returns the `default`
// dict plus parse_ok=False / parse_error (or raises loudly when no default
// was provided).
//
// Candidate extraction (parse_json.py:25-35), in robustness order:
//  1. whole lines that ARE a JSON object, scanned back to front (the prompt
//     asks for one-line JSON, the last line is the most trustworthy);
//  2. the slice from the LAST "{" to the LAST "}" — never the first "{":
//     prose containing braces (issue-pipeline #43's "type={}, model={}")
//     makes naive slicing glue prose and JSON together.
type ParseJSON struct {
	BaseNode
	Text       any            `json:"text,omitempty"`        // expression
	Choices    []string       `json:"choices,omitempty"`     // verdict whitelist
	Default    map[string]any `json:"default,omitempty"`     // failure fallback
	JoinFields []string       `json:"join_fields,omitempty"` // list fields flattened to "<f>_str"
}

func init() {
	workflow.RegisterNodeType(nodeTypeParseJSON, func(ctx context.Context) workflow.Node { return new(ParseJSON) })
}

// GetType returns the node type.
func (p *ParseJSON) GetType() workflow.NodeType { return nodeTypeParseJSON }

// Execute evaluates text, extracts and validates the JSON object.
func (p *ParseJSON) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	if p.Text == nil {
		return nil, fmt.Errorf("parse_json %q: missing text field", p.ID)
	}
	ev, err := expression.Eval(ctx, p.Text)
	if err != nil {
		return nil, fmt.Errorf("parse_json %q: eval text: %w", p.ID, err)
	}
	raw := strings.TrimSpace(parseJSONRawString(ev))

	var parsed map[string]any
	for _, cand := range parseJSONCandidates(raw) {
		var v any
		if err := json.Unmarshal([]byte(cand), &v); err != nil {
			continue
		}
		if m, ok := v.(map[string]any); ok {
			parsed = m
			break
		}
	}

	if parsed == nil {
		return p.fail(fmt.Sprintf("解析失败: %s", parseJSONTruncate(raw, parseJSONFailPreviewLimit)))
	}
	if p.Choices != nil {
		verdict, _ := parsed["verdict"].(string)
		if !parseJSONInChoices(verdict, parsed["verdict"], p.Choices) {
			return p.fail(fmt.Sprintf("输出非法 verdict: %s", parseJSONRepr(parsed["verdict"])))
		}
	}

	out := maps.Clone(parsed)
	out["parse_ok"] = true
	out["parse_error"] = ""
	for _, field := range p.JoinFields {
		if items, ok := out[field].([]any); ok {
			parts := make([]string, len(items))
			for i, item := range items {
				parts[i] = parseJSONStringify(item)
			}
			out[field+"_str"] = strings.Join(parts, parseJSONJoinSep)
		}
	}
	return &workflow.ExecuteResult{Result: out, NextNode: p.NextNode()}, nil
}

// fail mirrors ParseJsonNode._fail (parse_json.py:90-100): return the default
// dict enriched with parse_ok/parse_error, appending the detail to a string
// `notes`; without a default the node raises loudly.
func (p *ParseJSON) fail(detail string) (*workflow.ExecuteResult, error) {
	if p.Default == nil {
		return nil, fmt.Errorf("parse_json %q: 解析失败且未提供 default 兜底：%s", p.ID, detail)
	}
	out := maps.Clone(p.Default)
	out["parse_ok"] = false
	out["parse_error"] = detail
	switch notes, ok := out["notes"].(string); {
	case ok && notes != "":
		out["notes"] = notes + parseJSONNotesSep + detail
	case ok:
		out["notes"] = detail
	}
	return &workflow.ExecuteResult{Result: out, NextNode: p.NextNode()}, nil
}

// parseJSONCandidates mirrors extract_json_candidates (parse_json.py:25-35).
func parseJSONCandidates(raw string) []string {
	var cands []string
	lines := strings.Split(raw, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		t := strings.TrimSpace(strings.Trim(strings.TrimSpace(lines[i]), "`"))
		if strings.HasPrefix(t, "{") && strings.HasSuffix(t, "}") {
			cands = append(cands, t)
		}
	}
	lo, lc := strings.LastIndex(raw, "{"), strings.LastIndex(raw, "}")
	if lo != -1 && lc > lo {
		cands = append(cands, raw[lo:lc+1])
	}
	return cands
}

// parseJSONRawString mirrors `str(evaluate(text) or "")` (parse_json.py:63):
// falsy evaluated values (None/0/""/empty containers/False) become "".
func parseJSONRawString(v any) string {
	if !parseJSONTruthy(v) {
		return ""
	}
	return parseJSONStringify(v)
}

// parseJSONTruthy applies Python truthiness (parseJSONInChoices relies on the
// engine-wide convention documented in AGENTS.md).
func parseJSONTruthy(v any) bool {
	switch t := v.(type) {
	case nil:
		return false
	case bool:
		return t
	case string:
		return t != ""
	case float64:
		return t != 0
	case int:
		return t != 0
	case int64:
		return t != 0
	case json.Number:
		f, err := t.Float64()
		return err == nil && f != 0
	case []any:
		return len(t) > 0
	case map[string]any:
		return len(t) > 0
	default:
		return true
	}
}

// parseJSONInChoices reports whether the verdict is in the whitelist; only
// string verdicts can match (Python `in` on List[str]).
func parseJSONInChoices(verdict string, rawVerdict any, choices []string) bool {
	if _, ok := rawVerdict.(string); !ok {
		return false
	}
	for _, c := range choices {
		if c == verdict {
			return true
		}
	}
	return false
}

// parseJSONStringify mirrors Python str() for join_fields elements
// (None renders as "None", Python parity; numbers render as decimals).
func parseJSONStringify(v any) string {
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
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64)
	case int:
		return strconv.Itoa(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case json.Number:
		return t.String()
	default:
		return fmt.Sprint(t)
	}
}

// parseJSONRepr mirrors Python repr() for the illegal-verdict detail.
func parseJSONRepr(v any) string {
	switch t := v.(type) {
	case nil:
		return "None"
	case bool:
		if t {
			return "True"
		}
		return "False"
	case string:
		return strconv.Quote(t)
	default:
		return fmt.Sprint(t)
	}
}

// parseJSONTruncate truncates by runes (Python slices str by characters).
func parseJSONTruncate(s string, n int) string {
	if n <= 0 {
		return ""
	}
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}
