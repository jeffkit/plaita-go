package parser

import (
	"reflect"
	"testing"
)

// #62 parser-level controls: literals survive the statement rewrite
// byte-identical (single quotes still normalize to double), and ghost
// variables inside literals are not registered for pre-resolution.
func TestR62ParseFuncLiteralIsolation(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		wantStmt string
		wantVars []VariableExpr
	}{
		{
			name:     "dollar inside literal kept",
			expr:     `$F.concat("$", "5")`,
			wantStmt: `concat("$", "5")`,
			wantVars: nil,
		},
		{
			name:     "single quoted converted, content kept",
			expr:     `$F.concat('cost: $100', "!")`,
			wantStmt: `concat("cost: $100", "!")`,
			wantVars: nil,
		},
		{
			name:     "ghost variable not registered",
			expr:     `$F.concat("see $INPUT.a here", $INPUT.a)`,
			wantStmt: `concat("see $INPUT.a here", INPUT.a)`,
			wantVars: []VariableExpr{{Source: "INPUT", Variable: "INPUT.a"}},
		},
		{
			name:     "real variable after literal keeps parent chain absorption",
			expr:     `$F.concat("x", $PARENT.$INPUT.a)`,
			wantStmt: `concat("x", PARENT.INPUT.a)`,
			// the bare PARENT entry predates #62 (scan finds `$PARENT` itself);
			// literal isolation must not change this shape.
			wantVars: []VariableExpr{{Source: "PARENT", Variable: "PARENT"}, {Source: "PARENT", Variable: "PARENT.INPUT.a"}},
		},
		{
			name:     "trailing comma outside literal still dropped",
			expr:     `$F.getDictValue($INPUT.test, 'value',)`,
			wantStmt: `getDictValue(INPUT.test, "value")`,
			wantVars: []VariableExpr{{Source: "INPUT", Variable: "INPUT.test"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.expr)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			fe, ok := got.(*funcExpr)
			if !ok {
				t.Fatalf("Parse() = %T, want *funcExpr", got)
			}
			if fe.Statement() != tt.wantStmt {
				t.Errorf("Statement() = %q, want %q", fe.Statement(), tt.wantStmt)
			}
			if !reflect.DeepEqual(fe.Variables(), tt.wantVars) {
				t.Errorf("Variables() = %v, want %v", fe.Variables(), tt.wantVars)
			}
		})
	}
}
