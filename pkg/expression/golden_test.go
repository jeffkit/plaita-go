package expression_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strconv"
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
	// UseNumber keeps the exported Python types faithful: a Python int 3 in
	// the original test arrives as JSON `3` and must stay an int on the Go
	// side (Python str(3) == "3", str(3.0) == "3.0" — template interpolation
	// renders the difference, so the decode must not lose it).
	var cases []goldenCase
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&cases); err != nil {
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
		v = restoreGoldenNumbers(v)
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

// restoreGoldenNumbers converts UseNumber-decoded json.Number values back to
// the Python types the export started from: integral numbers become int64
// (Python int), everything else float64 (Python float).
func restoreGoldenNumbers(v any) any {
	switch t := v.(type) {
	case json.Number:
		if i, err := strconv.ParseInt(t.String(), 10, 64); err == nil {
			return i
		}
		f, err := strconv.ParseFloat(t.String(), 64)
		if err != nil {
			return t
		}
		return f
	case map[string]any:
		for k, e := range t {
			t[k] = restoreGoldenNumbers(e)
		}
		return t
	case []any:
		for i, e := range t {
			t[i] = restoreGoldenNumbers(e)
		}
		return t
	default:
		return v
	}
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
