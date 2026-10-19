package expression_test

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/expression"
)

// goldenCase mirrors pkg/expression/testdata/expression_golden.json, exported
// from plaita's tests/unit/test_expression_golden.py by
// scripts/export_golden.py. Regenerate after upgrading the Python side:
//
//	cd /Users/kong/projects/infra4agent/plaita && \
//	python3 /Users/kong/projects/plaita-go/scripts/export_golden.py
type goldenCase struct {
	ID       string          `json:"id"`
	Expr     json.RawMessage `json:"expr"`
	Context  map[string]any  `json:"context"`
	Expected json.RawMessage `json:"expected"`
	Note     string          `json:"note"`
}

// TestPlaitaExpressionGolden runs the plaita golden cases through the Go
// evaluator. Context roots carry plaita's "$"-prefixed spelling in the JSON;
// they map onto the engine runtime context here.
func TestPlaitaExpressionGolden(t *testing.T) {
	data, err := os.ReadFile("testdata/expression_golden.json")
	if err != nil {
		t.Fatalf("read golden cases: %v", err)
	}
	var cases []goldenCase
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatalf("parse golden cases: %v", err)
	}
	if len(cases) == 0 {
		t.Fatal("no golden cases")
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.ID, func(t *testing.T) {
			if strings.Contains(tc.ID, "parse_function") {
				t.Skip("tests plaita's parse_function API, not the evaluate path this facade mirrors")
			}
			ctx := goldenContext(t, tc.Context)

			var exprVal any
			if err := json.Unmarshal(tc.Expr, &exprVal); err != nil {
				t.Fatalf("decode expr: %v", err)
			}
			got, err := expression.Eval(ctx, exprVal)
			if wantErr(tc.Expected) {
				if err == nil {
					t.Fatalf("expected an error, got value %#v", got)
				}
				t.Logf("error (plaita %s): %v", errorName(tc.Expected), err)
				return
			}
			if err != nil {
				t.Fatalf("eval: %v", err)
			}
			want := jsonNormalize(t, tc.Expected)
			if !reflect.DeepEqual(jsonNormalize(t, got), want) {
				t.Fatalf("mismatch\n expr: %s\n want: %#v\n got:  %#v", tc.Expr, want, got)
			}
		})
	}
}

// goldenContext maps the exported "$INPUT"-style roots onto the engine
// runtime context.
func goldenContext(t *testing.T, m map[string]any) context.Context {
	t.Helper()
	rc := &runtime.Context{
		WorkflowInput:      map[string]any{},
		ExecuteNodeResults: map[string]any{},
		EnvVariables:       map[string]any{},
		GlobalVariables:    map[string]any{},
		CurrentNodeContext: map[string]any{},
	}
	for k, v := range m {
		name := strings.TrimPrefix(k, "$")
		switch name {
		case "INPUT":
			rc.WorkflowInput = v
		case "NODE":
			nodes, ok := v.(map[string]any)
			if !ok {
				t.Fatalf("%s: NODE context must be an object", k)
			}
			rc.ExecuteNodeResults = nodes
		case "GLOBAL":
			g, ok := v.(map[string]any)
			if !ok {
				t.Fatalf("%s: GLOBAL context must be an object", k)
			}
			rc.GlobalVariables = g
		case "ENV":
			e, ok := v.(map[string]any)
			if !ok {
				t.Fatalf("%s: ENV context must be an object", k)
			}
			rc.EnvVariables = e
		default:
			// plain variables (plaita context keys beyond the built-in
			// roots, e.g. `user` / `$A`) resolve like node-context entries
			rc.CurrentNodeContext[name] = v
		}
	}
	return runtime.WithWorkflowRuntimeContext(context.Background(), rc)
}

func wantErr(raw json.RawMessage) bool {
	var m map[string]any
	return json.Unmarshal(raw, &m) == nil && m["$error"] != nil
}

func errorName(raw json.RawMessage) string {
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	if s, ok := m["$error"].(string); ok {
		return s
	}
	return "?"
}

// jsonNormalize pushes both sides through the JSON domain so goval's int /
// plaita's int / JSON float64 compare equal while bool stays distinct.
func jsonNormalize(t *testing.T, v any) any {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("normalize %+#v: %v", v, err)
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("normalize %+#v: %v", v, err)
	}
	return out
}
