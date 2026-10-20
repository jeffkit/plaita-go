package nodes

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/expression"
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
			// Python evaluate("1") 原样返回字符串 "1"，`"1" > 2` 是 TypeError——
			// 对齐后 gt 对 字符串vs数字 返回错误（不再静默 cast，issue #3）。
			name:       "gt string against number is TypeError",
			runtimeCtx: &runtime.Context{},
			cond: LogicCondition{
				Field:    "1",
				Operator: Gt,
				Value:    2,
			},
			want:    false,
			wantErr: true,
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
			// 真 nil（None）与 $NULL：None 短路 → eq 做 None 身份比较 → true
			name: "none eq null",
			runtimeCtx: &runtime.Context{
				CurrentNodeContext: map[string]any{
					"a": map[string]any{
						"b": nil,
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
			// 空列表 vs None：Python `[] == None` 是 False（issue #3 矩阵行
			// eq([], None)）；表达式层把 typed-nil slice 归一为空列表，同样落到本行。
			name: "empty slice eq null is false",
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
			want:    false,
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
			// struct 不是 plaita 表达式值域（JSON 值域），对齐 $F.eq：不比较、
			// 不相等、更不 panic。
			name: "case 9 struct values not equal",
			args: args{
				l: struct{}{},
				r: struct{}{},
			},
			want:    false,
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
			// Python `[] == None` 是 False（issue #3 矩阵行 eq([], None)），
			// 旧的"空切片==nil"特判已删。
			name: "case 11 empty slice and nil are not equal",
			args: args{
				l: []any{},
				r: nil,
			},
			want:    false,
			wantErr: false,
		},
		{
			// map 深比较：旧实现对 map 直接 l == r panic（issue #3）。
			name: "case 12 equal maps are deeply equal",
			args: args{
				l: map[string]any{"a": 1.0, "b": []any{"x"}},
				r: map[string]any{"a": 1.0, "b": []any{"x"}},
			},
			want:    true,
			wantErr: false,
		},
		{
			// bool 是数字（Python int 子类）：True == 1。
			name: "case 13 true equals 1",
			args: args{
				l: true,
				r: 1,
			},
			want:    true,
			wantErr: false,
		},
		{
			// 字符串不是数字：Python `"1" == 1` 是 False。
			name: "case 14 string one does not equal number one",
			args: args{
				l: 1,
				r: "1",
			},
			want:    false,
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

// TestConditionOperators_PlaitaMatrix：条件算子与 Python decide.py
// Condition.match + condition_matcher 逐条对齐（issue #3 的 44 条矩阵关键行）。
// 语义与 $F（pkg/expression/plaitaexpr 的 Equal/Compare/Contains）同源：
// bool 是数字、字符串不参与数值 cast、容器深比较、字符串按字典序、
// None 短路仅 eq/ne。实现直调 plaitaexpr 导出版本，无双实现。
func TestConditionOperators_PlaitaMatrix(t *testing.T) {
	match := func(t *testing.T, input map[string]any, field string, op ComparisonOperator, value any) (bool, error) {
		t.Helper()
		ctx := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{CurrentNodeContext: input})
		lc := LogicCondition{Field: field, Operator: op, Value: value}
		matched, _, err := lc.MatchContext(ctx)
		return matched, err
	}
	requireMatch := func(t *testing.T, input map[string]any, field string, op ComparisonOperator, value any, want bool) {
		t.Helper()
		got, err := match(t, input, field, op, value)
		require.NoError(t, err, "%s %v %v", field, op, value)
		require.Equal(t, want, got, "%s %s %v", field, op, value)
	}

	t.Run("eq", func(t *testing.T) {
		requireMatch(t, map[string]any{"l": true}, "$l", Eq, 1, true)            // eq(True, 1)
		requireMatch(t, map[string]any{"l": 1.0}, "$l", Eq, 1, true)             // eq(1, 1) 跨数值类型
		requireMatch(t, map[string]any{"l": 1.0}, "$l", Eq, "1", false)          // eq(1, "1")
		requireMatch(t, map[string]any{"l": 1.0}, "$l", Eq, true, true)          // eq(1, true) 与反向对称
		requireMatch(t, map[string]any{"l": true}, "$l", Eq, 1.0, true)          // eq(true, 1.0)
		requireMatch(t, map[string]any{"l": map[string]any{"a": 1.0}}, "$l", Eq, // eq({"a":1}, {"a":1}) 深比较不 panic
			map[string]any{"a": 1.0}, true)
		requireMatch(t, map[string]any{"l": map[string]any{}}, "$l", Eq, // eq({}, {})
			map[string]any{}, true)
		requireMatch(t, map[string]any{"l": []any{1.0}}, "$l", Eq, []any{1.0}, true) // eq([1], [1])
		requireMatch(t, map[string]any{"l": map[string]any{"a": map[string]any{"b": []any{1.0, 2.0}}}}, "$l", Eq,
			map[string]any{"a": map[string]any{"b": []any{1.0, 2.0}}}, true) // 嵌套容器
		requireMatch(t, map[string]any{"l": []any{}}, "$l", Eq, "$NULL", false) // eq([], None)
		requireMatch(t, map[string]any{"l": nil}, "$l", Eq, "$NULL", true)      // eq(None, None)
		requireMatch(t, map[string]any{"l": nil}, "$l", Eq, 1.0, false)         // eq(None, 1)
	})

	t.Run("ne", func(t *testing.T) {
		requireMatch(t, map[string]any{"l": nil}, "$l", Ne, "$NULL", false) // ne(None, None)
		requireMatch(t, map[string]any{"l": nil}, "$l", Ne, 1.0, true)      // ne(None, 1)
		requireMatch(t, map[string]any{"l": map[string]any{"a": 1.0}}, "$l", Ne,
			map[string]any{"a": 2.0}, true)
	})

	t.Run("gt/gte/lt/lte", func(t *testing.T) {
		requireMatch(t, map[string]any{"l": "b"}, "$l", Gt, "a", true)  // gt("b", "a") 字典序
		requireMatch(t, map[string]any{"l": "2"}, "$l", Gt, "10", true) // gt("2", "10") 字典序非数值
		requireMatch(t, map[string]any{"l": "a"}, "$l", Lt, "b", true)
		requireMatch(t, map[string]any{"l": 2.0}, "$l", Gte, 2.0, true)
		requireMatch(t, map[string]any{"l": 2.0}, "$l", Lte, 3.0, true)
		requireMatch(t, map[string]any{"l": true}, "$l", Gt, false, true) // True > False
		// gt(2, "10")：Python TypeError，不再静默 cast
		_, err := match(t, map[string]any{"l": 2.0}, "$l", Gt, "10")
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported operand types")
		// 字符串 vs 数字同样 TypeError
		_, err = match(t, map[string]any{"l": "2"}, "$l", Gt, 10.0)
		require.Error(t, err)
	})

	t.Run("in/notIn", func(t *testing.T) {
		requireMatch(t, map[string]any{"l": "a"}, "$l", In, "abc", true)                  // in("a", "abc") 子串
		requireMatch(t, map[string]any{"l": "a"}, "$l", In, map[string]any{"a": 1}, true) // in("a", {"a":1}) 键
		requireMatch(t, map[string]any{"l": 1.0}, "$l", In, []any{1.0, 2.0}, true)        // 成员经 == 深比较
		requireMatch(t, map[string]any{"l": 1.0}, "$l", In, []any{"1"}, false)            // "1" != 1
		requireMatch(t, map[string]any{"l": nil}, "$l", In, []any{nil}, false)            // in(None, [None]) None 短路
		requireMatch(t, map[string]any{"l": 3.0}, "$l", NotIn, []any{1.0, 2.0}, true)
		requireMatch(t, map[string]any{"l": "a"}, "$l", NotIn, "abc", false)
		// in(1, "abc")：Python `'in <string>' requires string as left operand`
		_, err := match(t, map[string]any{"l": 1.0}, "$l", In, "abc")
		require.Error(t, err)
		// in("a", 42)：容器必须是 str/list/dict
		_, err = match(t, map[string]any{"l": "a"}, "$l", In, 42.0)
		require.Error(t, err)
	})

	t.Run("contains/notContains", func(t *testing.T) {
		requireMatch(t, map[string]any{"l": "abc"}, "$l", Contains, "a", true)                  // 右∈左：子串
		requireMatch(t, map[string]any{"l": []any{"a"}}, "$l", Contains, "a", true)             // 右∈左：元素
		requireMatch(t, map[string]any{"l": map[string]any{"a": 1}}, "$l", Contains, "a", true) // 右∈左：键
		requireMatch(t, map[string]any{"l": "abc"}, "$l", NotContains, "d", true)
		requireMatch(t, map[string]any{"l": []any{}}, "$l", Contains, "a", false)
		_, err := match(t, map[string]any{"l": 1.0}, "$l", Contains, "a")
		require.Error(t, err)
	})

	t.Run("truthy/falsy", func(t *testing.T) {
		requireMatch(t, map[string]any{"l": ""}, "$l", Truthy, nil, false) // Python bool("")
		requireMatch(t, map[string]any{"l": "x"}, "$l", Truthy, nil, true)
		requireMatch(t, map[string]any{"l": 0.0}, "$l", Truthy, nil, false)
		requireMatch(t, map[string]any{"l": []any{1}}, "$l", Truthy, nil, true) // 非空列表为真
		requireMatch(t, map[string]any{"l": []any{}}, "$l", Truthy, nil, false)
		requireMatch(t, map[string]any{"l": map[string]any{}}, "$l", Truthy, nil, false)
		requireMatch(t, map[string]any{"l": nil}, "$l", Truthy, nil, false)
		requireMatch(t, map[string]any{"l": 0.0}, "$l", Falsy, nil, true)
	})

	t.Run("none short-circuit for other operators", func(t *testing.T) {
		// 任一操作数为 None：eq/ne 之外一律 False，不报错
		requireMatch(t, map[string]any{"l": nil}, "$l", Gt, 1.0, false)
		requireMatch(t, map[string]any{"l": 1.0}, "$l", Gt, "$NULL", false)
		requireMatch(t, map[string]any{"l": nil}, "$l", Contains, "a", false)
	})
}

// TestConditionEq_MatchesDollarF：条件 eq 与表达式引擎的 $F.eq 必须同源
// （issue #3）——同一对操作数经 $F.eq 求值与条件算子 eq 结果一致。两侧实现
// 都直调 plaitaexpr 的 Equal，这里经公开门面 expression.Eval 走真实的 $F
// 求值路径做交叉验证；条件侧的 None 短路真值表与 Equal 对 nil 的处理一致。
func TestConditionEq_MatchesDollarF(t *testing.T) {
	cases := []struct {
		l, r any
	}{
		{true, 1}, {1.0, true}, {1.0, "1"}, {"1", 1.0}, {1.0, 1.0}, {2.0, 2},
		{map[string]any{"a": 1.0}, map[string]any{"a": 1.0}},
		{map[string]any{"a": 1.0}, map[string]any{"a": 2.0}},
		{[]any{1.0, "x"}, []any{1.0, "x"}},
		{[]any{}, nil}, {nil, nil}, {nil, 1.0},
		{"abc", "abc"}, {"abc", "abd"},
	}
	for _, c := range cases {
		ctx := runtime.WithWorkflowRuntimeContext(context.Background(),
			&runtime.Context{WorkflowInput: map[string]any{"a": c.l, "b": c.r}})
		wantV, err := expression.Eval(ctx, "$F.eq($INPUT.a, $INPUT.b)")
		require.NoError(t, err, "$F.eq(%v, %v)", c.l, c.r)
		want, ok := wantV.(bool)
		require.True(t, ok)
		got, err := eq(c.l, c.r)
		require.NoError(t, err)
		require.Equal(t, want, got, "condition eq diverges from $F.eq on (%v, %v)", c.l, c.r)
	}
}
