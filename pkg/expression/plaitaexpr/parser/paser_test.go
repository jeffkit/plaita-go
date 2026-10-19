package parser

import (
	"reflect"
	"testing"
)

func Test_parseFunc(t *testing.T) {
	tests := []struct {
		name    string
		expr    string
		want    *funcExpr
		wantErr bool
	}{
		{
			name: "simple function",
			expr: "$F.add(1, 2)",
			want: &funcExpr{
				expr:      "add(1, 2)",
				variables: nil,
			},
			wantErr: false,
		},
		{
			name: "function with one variable",
			expr: "$F.add($INPUT.a, 2)",
			want: &funcExpr{
				expr:      "add(INPUT.a, 2)",
				variables: []VariableExpr{{Source: "INPUT", Variable: "INPUT.a"}},
			},
			wantErr: false,
		},
		{
			name: "function with two difference variable",
			expr: "$F.add($INPUT.a, $NODE.a)",
			want: &funcExpr{
				expr:      "add(INPUT.a, NODE.a)",
				variables: []VariableExpr{{Source: "INPUT", Variable: "INPUT.a"}, {Source: "NODE", Variable: "NODE.a"}},
			},
			wantErr: false,
		},
		{
			name: "function with variable and other function",
			expr: "$F.add($INPUT.a, $F.add($NODE.b, 1))",
			want: &funcExpr{
				expr:      "add(INPUT.a, add(NODE.b, 1))",
				variables: []VariableExpr{{Source: "INPUT", Variable: "INPUT.a"}, {Source: "NODE", Variable: "NODE.b"}},
			},
			wantErr: false,
		},
		{
			name: "function with string variable",
			expr: `$F.concat($INPUT.a, "11")`,
			want: &funcExpr{
				expr:      `concat(INPUT.a, "11")`,
				variables: []VariableExpr{{Source: "INPUT", Variable: "INPUT.a"}},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseFunc(tt.expr)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseFunc() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseFunc() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		expr    string
		want    Expr
		wantErr bool
	}{
		{
			name:    "more than one dollar",
			expr:    "$$a",
			want:    nil,
			wantErr: true,
		},
		{
			name:    "simple var",
			expr:    "$a",
			want:    &variableExpr{variable: VariableExpr{Source: "", Variable: "a", WithoutSourceVariable: "a"}},
			wantErr: false,
		},
		{
			name:    "simple function",
			expr:    "$F.add(1, 2)",
			want:    &funcExpr{expr: "add(1, 2)"},
			wantErr: false,
		},
		{
			name:    "nested function",
			expr:    "$F.add(1, $F.add(2, 3))",
			want:    &funcExpr{expr: "add(1, add(2, 3))"},
			wantErr: false,
		},
		{
			name: "nested function with variable",
			expr: "$F.add(1, $F.add($INPUT.a, 1))",
			want: &funcExpr{
				expr: "add(1, add(INPUT.a, 1))",
				variables: []VariableExpr{
					{Source: "INPUT", Variable: "INPUT.a"},
				},
			},
			wantErr: false,
		},
		{
			name: "INPUT variable",
			expr: "$INPUT.a",
			want: &variableExpr{
				variable: VariableExpr{
					Source:                "INPUT",
					Variable:              "INPUT.a",
					WithoutSourceVariable: "a",
				},
			},
			wantErr: false,
		},
		{
			name: "NODE variable",
			expr: "$NODE.a.b",
			want: &variableExpr{
				variable: VariableExpr{
					Source:                "NODE",
					Variable:              "NODE.a.b",
					WithoutSourceVariable: "a.b",
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.expr)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parse() got = %v, want %v", got, tt.want)
			}
		})
	}
}
