package nodes

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jeffkit/plaita-go/internal/runtime"
)

func TestCondition_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		data    string
		want    *Condition
		wantErr bool
	}{
		{
			name: "test logic condition",
			data: `
			{
					"field": "$INPUT.a",
					"operator": "gt",
					"value": 1
			}
						`,
			want: &Condition{
				ConditionType: conditionTypeLogicCondition,
				LogicCondition: LogicCondition{
					Field:    "$INPUT.a",
					Operator: Gt,
					Value:    float64(1),
				},
			},
			wantErr: false,
		},
		{
			name: "test logic condition group",
			data: `{
					"relation": "and",
					"conditions": [
						{
							"field": "$INPUT.a",
							"operator": "gt",
							"value": 1
						},
						{
							"field": "$INPUT.b",
							"operator": "lt",
							"value": 2
						},
						{
							"field": "$INPUT.c",
							"operator": "eq",
							"value": "3"
						}
					]
				}`,
			want: &Condition{
				ConditionType: conditionTypeLogicConditionGroup,
				LogicConditionGroup: LogicConditionGroup{
					Relation: And,
					Conditions: []ConditionElement{
						LogicCondition{Field: "$INPUT.a", Operator: Gt, Value: float64(1)},
						LogicCondition{Field: "$INPUT.b", Operator: Lt, Value: float64(2)},
						LogicCondition{Field: "$INPUT.c", Operator: Eq, Value: "3"},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "test expression",
			data: `"add($INPUT.a, 1) > 1"`,
			want: &Condition{
				ConditionType:       conditionTypeExpression,
				ConditionExpression: "add($INPUT.a, 1) > 1",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := new(Condition)
			err := got.UnmarshalJSON([]byte(tt.data))
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
		})
	}
}

func TestLogicCondition_Expression(t *testing.T) {
	tests := []struct {
		name    string
		cond    LogicCondition
		want    string
		wantErr bool
	}{
		{
			name: "simple gt",
			cond: LogicCondition{
				Field:    "$INPUT.a",
				Operator: Gt,
				Value:    1,
			},
			want:    "$INPUT.a > 1",
			wantErr: false,
		},
		{
			name: "gt with function and var",
			cond: LogicCondition{
				Field:    "$INPUT.a",
				Operator: Gt,
				Value:    "$F.length($INPUT.b)",
			},
			want:    "$INPUT.a > $F.length($INPUT.b)",
			wantErr: false,
		},
		{
			name: "simple lt",
			cond: LogicCondition{
				Field:    "$INPUT.a",
				Operator: Lt,
				Value:    1.1,
			},
			want:    "$INPUT.a < 1.1",
			wantErr: false,
		},
		{
			name: "simple lte",
			cond: LogicCondition{
				Field:    "$INPUT.a",
				Operator: Lte,
				Value:    1.0,
			},
			want:    "$INPUT.a <= 1",
			wantErr: false,
		},
		{
			name: "simple eq",
			cond: LogicCondition{
				Field:    "$INPUT.a",
				Operator: Eq,
				Value:    1,
			},
			want:    "$INPUT.a == 1",
			wantErr: false,
		},
		{
			name: "simple ne",
			cond: LogicCondition{
				Field:    "$INPUT.a",
				Operator: Ne,
				Value:    1,
			},
			want:    "$INPUT.a != 1",
			wantErr: false,
		},
		{
			name: "simple between",
			cond: LogicCondition{
				Field:    "$INPUT.a",
				Operator: Between,
				Value:    []any{1, 2},
			},
			want:    "between($INPUT.a, 1, 2)",
			wantErr: false,
		},
		{
			name: "simple notBetween",
			cond: LogicCondition{
				Field:    "$INPUT.a",
				Operator: NotBetween,
				Value:    []any{1, 2},
			},
			want:    "notBetween($INPUT.a, 1, 2)",
			wantErr: false,
		},
		{
			name: "simple like",
			cond: LogicCondition{
				Field:    "$INPUT.a",
				Operator: Like,
				Value:    "a%",
			},
			want:    `like($INPUT.a, "a%")`,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.cond.Expression()
			if (err != nil) != tt.wantErr {
				t.Errorf("Expression() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Expression() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLogicCondition_MatchContext(t *testing.T) {
	tests := []struct {
		name       string
		runtimeCtx *runtime.Context
		cond       LogicCondition
		want       bool
		wantErr    bool
	}{
		{
			name:       "simple gt",
			runtimeCtx: &runtime.Context{},
			cond: LogicCondition{
				Field:    "1",
				Operator: Gt,
				Value:    2,
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "gt with var",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"a": 3.0,
				},
			},
			cond: LogicCondition{
				Field:    "$INPUT.a",
				Operator: Gt,
				Value:    2,
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "gt with var and function",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"a": 3.0,
					"b": []any{1, 2},
				},
			},
			cond: LogicCondition{
				Field:    "$INPUT.a",
				Operator: Gt,
				Value:    "$F.length($INPUT.b)",
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple gte",
			runtimeCtx: &runtime.Context{
				CurrentNodeContext: map[string]any{
					"a": 3.0,
				},
			},
			cond: LogicCondition{
				Field:    "$a",
				Operator: Gte,
				Value:    3,
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple lte",
			runtimeCtx: &runtime.Context{
				CurrentNodeContext: map[string]any{
					"a": map[string]any{
						"b": 3.0,
					},
				},
			},
			cond: LogicCondition{
				Field:    "$a.b",
				Operator: Lte,
				Value:    4,
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple lt",
			runtimeCtx: &runtime.Context{
				CurrentNodeContext: map[string]any{
					"a": map[string]any{
						"b": 3.0,
					},
				},
			},
			cond: LogicCondition{
				Field:    "$a.b",
				Operator: Lt,
				Value:    3,
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "simple eq",
			runtimeCtx: &runtime.Context{
				CurrentNodeContext: map[string]any{
					"a": map[string]any{
						"b": 3.0,
					},
				},
			},
			cond: LogicCondition{
				Field:    "$a.b",
				Operator: Eq,
				Value:    3.0,
			},
			want:    true,
			wantErr: false,
		},
		// 测试eq的bool类型
		{
			name: "simple eq bool",
			runtimeCtx: &runtime.Context{
				CurrentNodeContext: map[string]any{
					"a": map[string]any{
						"b": true,
					},
				},
			},
			cond: LogicCondition{
				Field:    "$a.b",
				Operator: Eq,
				Value:    true,
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple ne",
			runtimeCtx: &runtime.Context{
				CurrentNodeContext: map[string]any{
					"a": map[string]any{
						"b": "ab",
					},
				},
			},
			cond: LogicCondition{
				Field:    "$a.b",
				Operator: Ne,
				Value:    "abc",
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple ne bool",
			runtimeCtx: &runtime.Context{
				CurrentNodeContext: map[string]any{
					"a": map[string]any{
						"b": true,
					},
				},
			},
			cond: LogicCondition{
				Field:    "$a.b",
				Operator: Ne,
				Value:    false,
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple between",
			runtimeCtx: &runtime.Context{
				CurrentNodeContext: map[string]any{
					"a": map[string]any{
						"b": 2.0,
					},
				},
			},
			cond: LogicCondition{
				Field:    "$a.b",
				Operator: Between,
				Value:    []any{1, 3},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple not between",
			runtimeCtx: &runtime.Context{
				CurrentNodeContext: map[string]any{
					"a": map[string]any{
						"b": 4.0,
					},
				},
			},
			cond: LogicCondition{
				Field:    "$a.b",
				Operator: NotBetween,
				Value:    []any{1, 3},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "in strings",
			runtimeCtx: &runtime.Context{
				CurrentNodeContext: map[string]any{
					"a": map[string]any{
						"b": "ab",
					},
				},
			},
			cond: LogicCondition{
				Field:    "$a.b",
				Operator: In,
				Value:    []any{"ab", "abc"},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "in ints",
			runtimeCtx: &runtime.Context{
				CurrentNodeContext: map[string]any{
					"a": map[string]any{
						"b": 1.0,
					},
				},
			},
			cond: LogicCondition{
				Field:    "$a.b",
				Operator: In,
				Value:    []any{1.0, 2.0},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "not in ints",
			runtimeCtx: &runtime.Context{
				CurrentNodeContext: map[string]any{
					"a": map[string]any{
						"b": 3.0,
					},
				},
			},
			cond: LogicCondition{
				Field:    "$a.b",
				Operator: NotIn,
				Value:    []any{1.0, 2.0},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple like",
			runtimeCtx: &runtime.Context{
				CurrentNodeContext: map[string]any{
					"a": map[string]any{
						"b": "abc",
					},
				},
			},
			cond: LogicCondition{
				Field:    "$a.b",
				Operator: Like,
				Value:    "ab",
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "nil slice eq",
			runtimeCtx: &runtime.Context{
				CurrentNodeContext: map[string]any{
					"a": map[string]any{
						"b": []int(nil),
					},
				},
			},
			cond: LogicCondition{
				Field:    "$a.b",
				Operator: Eq,
				Value:    "$NULL",
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "slice element eq",
			runtimeCtx: &runtime.Context{
				CurrentNodeContext: map[string]any{
					"a": map[string]any{
						"b": []int{1, 2, 3},
					},
				},
			},
			cond: LogicCondition{
				Field:    "$a.b",
				Operator: Eq,
				Value:    []any{1, 2, 3},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "slice element not eq",
			runtimeCtx: &runtime.Context{
				CurrentNodeContext: map[string]any{
					"a": map[string]any{
						"b": []int{1, 2, 3},
					},
				},
			},
			cond: LogicCondition{
				Field:    "$a.b",
				Operator: Eq,
				Value:    []any{1, 2},
			},
			want:    false,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _, err := tt.cond.MatchContext(runtime.WithWorkflowRuntimeContext(context.TODO(), tt.runtimeCtx))
			if (err != nil) != tt.wantErr {
				t.Errorf("MatchContext() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("MatchContext() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLogicConditionGroup_MatchContext(t *testing.T) {
	tests := []struct {
		name       string
		runtimeCtx *runtime.Context
		condition  LogicConditionGroup
		want       bool
		wantErr    bool
	}{
		{
			name: "and group match",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"a": 2.0,
				},
				CurrentNodeContext: map[string]any{
					"a": map[string]any{
						"b": "yes",
					},
				},
			},
			condition: LogicConditionGroup{
				Relation: And,
				Conditions: []ConditionElement{
					LogicCondition{
						Field:    "$a.b",
						Operator: Eq,
						Value:    "yes",
					},
					LogicCondition{
						Field:    "$INPUT.a",
						Operator: Gt,
						Value:    1.0,
					},
				},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "and group mismatch",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"a": 2.0,
				},
				CurrentNodeContext: map[string]any{
					"a": map[string]any{
						"b": "yes",
					},
				},
			},
			condition: LogicConditionGroup{
				Relation: And,
				Conditions: []ConditionElement{
					LogicCondition{
						Field:    "$a.b",
						Operator: Eq,
						Value:    "yes",
					},
					LogicCondition{
						Field:    "$INPUT.a",
						Operator: Lt,
						Value:    1.0,
					},
				},
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "or group match",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"a": 2.0,
				},
				CurrentNodeContext: map[string]any{
					"a": map[string]any{
						"b": "yes",
					},
				},
			},
			condition: LogicConditionGroup{
				Relation: Or,
				Conditions: []ConditionElement{
					LogicCondition{
						Field:    "$a.b",
						Operator: Eq,
						Value:    "yes",
					},
					LogicCondition{
						Field:    "$INPUT.a",
						Operator: Lt,
						Value:    1.0,
					},
				},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "or group mismatch",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"a": 2.0,
				},
				CurrentNodeContext: map[string]any{
					"a": map[string]any{
						"b": "yes1",
					},
				},
			},
			condition: LogicConditionGroup{
				Relation: Or,
				Conditions: []ConditionElement{
					LogicCondition{
						Field:    "$a.b",
						Operator: Eq,
						Value:    "yes",
					},
					LogicCondition{
						Field:    "$INPUT.a",
						Operator: Lt,
						Value:    1.0,
					},
				},
			},
			want:    false,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, matchInfo, err := tt.condition.MatchContext(runtime.WithWorkflowRuntimeContext(context.TODO(), tt.runtimeCtx))
			if (err != nil) != tt.wantErr {
				t.Errorf("MatchContext() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("MatchContext() got = %v, want %v", got, tt.want)
			}
			// matchInfo应是空的
			if matchInfo != nil {
				t.Errorf("matchInfo should be nil")
			}
		})
	}
	// 再来一次，设置switch:eager_mode为true
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.runtimeCtx.WithGlobalVariable("switch:eagerMode", true)
			got, matchInfo, err := tt.condition.MatchContext(runtime.WithWorkflowRuntimeContext(context.TODO(), tt.runtimeCtx))
			if (err != nil) != tt.wantErr {
				t.Errorf("MatchContext() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("MatchContext() got = %v, want %v", got, tt.want)
			}
			if matchInfo != nil {
				t.Logf("matchInfo: %v", matchInfo)
				t.Logf("children:")
				for _, child := range matchInfo.Children {
					t.Logf("child: %v", child)
				}
			} else {
				t.Errorf("matchInfo is nil")
			}
		})
	}
}

func TestConditionExpression_MatchContext(t *testing.T) {
	tests := []struct {
		name          string
		runtimeCtx    *runtime.Context
		conditionExpr string
		want          bool
		wantErr       bool
	}{
		{
			name: "function expression",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"a": "abc",
				},
			},
			conditionExpr: `$F.contains($INPUT.a, "ab")`,
			want:          true,
			wantErr:       false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Condition{
				ConditionType:       conditionTypeExpression,
				ConditionExpression: tt.conditionExpr,
			}
			got, _, err := c.MatchContext(runtime.WithWorkflowRuntimeContext(context.TODO(), tt.runtimeCtx))
			if (err != nil) != tt.wantErr {
				t.Errorf("MatchContext() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("MatchContext() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_eq(t *testing.T) {
	type args struct {
		l any
		r any
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "case 1 empty slice and nil is equal",
			args: args{
				l: []int{},
				r: []int(nil),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "case 2 empty slice equal",
			args: args{
				l: []int{},
				r: []int{},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "case 3 slice element equal",
			args: args{
				l: []float32{1, 2, 3},
				r: []int{1, 2, 3},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "case 4 different types not equal",
			args: args{
				l: []float32{1, 2, 3},
				r: 1,
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "case 5 number equal",
			args: args{
				l: 1.00,
				r: 1,
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "case 6 bool equal",
			args: args{
				l: true,
				r: true,
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "case 7 string equal",
			args: args{
				l: "abc",
				r: "abc",
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "case 8 string not equal",
			args: args{
				l: "abc",
				r: "abb",
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "case 9 default equal",
			args: args{
				l: struct{}{},
				r: struct{}{},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "case 10 nil equal",
			args: args{
				l: nil,
				r: nil,
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "case 11 empty slice and nil are equal",
			args: args{
				l: []any{},
				r: nil,
			},
			want:    true,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := eq(tt.args.l, tt.args.r)
			if (err != nil) != tt.wantErr {
				t.Errorf("eq() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("eq() got = %v, want %v", got, tt.want)
			}
		})
	}
}
