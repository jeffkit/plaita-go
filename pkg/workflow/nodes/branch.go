package nodes

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/jeffkit/plaita-go/internal/runtime"

	jsoniter "github.com/json-iterator/go"
	"github.com/samber/lo"
	"github.com/spf13/cast"

	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/expression"
)

var jsonAPI = jsoniter.Config{
	EscapeHTML:             true,
	SortMapKeys:            true,
	ValidateJsonRawMessage: true,
	DisallowUnknownFields:  true,
}.Froze()

// Branch 分支
type Branch struct {
	Name      string       `json:"name"`
	Label     string       `json:"label"`
	Desc      string       `json:"desc,omitempty"`
	Condition Condition    `json:"condition,omitempty"`
	Priority  int          `json:"priority,omitempty"`
	IsDefault bool         `json:"isDefault"`
	Next      types.NodeID `json:"next,omitempty"`
}

// MatchContext match branch condition with workflow context.
func (b *Branch) MatchContext(ctx context.Context) (bool, *MatchInfo, error) {
	return b.Condition.MatchContext(ctx)
}

// ComparisonOperator is type for comparison operator.
type ComparisonOperator string

// ComparisonOperator enums
const (
	Gt          ComparisonOperator = "gt"
	Gte         ComparisonOperator = "gte"
	Lt          ComparisonOperator = "lt"
	Lte         ComparisonOperator = "lte"
	Eq          ComparisonOperator = "eq"
	Ne          ComparisonOperator = "ne"
	Between     ComparisonOperator = "between"
	NotBetween  ComparisonOperator = "notBetween"
	In          ComparisonOperator = "in"
	NotIn       ComparisonOperator = "notIn"
	Like        ComparisonOperator = "like"
	Contains    ComparisonOperator = "contains"
	NotContains ComparisonOperator = "notContains"
	Truthy      ComparisonOperator = "truthy"
	Falsy       ComparisonOperator = "falsy"
)

type compareFunc func(l, r any) (bool, error)

var comparisonOperations = map[ComparisonOperator]compareFunc{
	Gt:          gt,
	Gte:         gte,
	Lt:          lt,
	Lte:         lte,
	Eq:          eq,
	Ne:          ne,
	Between:     between,
	NotBetween:  notBetween,
	In:          in,
	NotIn:       notIn,
	Like:        like,
	Contains:    contains,
	NotContains: notContains,
	Truthy:      truthy,
	Falsy:       falsy,
}

// ConditionElement defines a condition element.
type ConditionElement interface {
	IsConditionElement()
	MatchContext(ctx context.Context) (bool, *MatchInfo, error)
	Expression() (string, error)
}

// LogicCondition represents a logic condition.
type LogicCondition struct {
	Field    string             `json:"field"`
	Operator ComparisonOperator `json:"operator"`
	Value    any                `json:"value"`
}

// MatchInfo represents a match info.
type MatchInfo struct {
	Field         string             `json:"field"`
	Operator      ComparisonOperator `json:"operator"`
	Expect        any                `json:"expect"`
	Actual        any                `json:"actual"`
	Matched       bool               `json:"matched"`
	Error         string             `json:"error"`
	Children      []*MatchInfo       `json:"children,omitempty"`
	ChildRelation LogicOperator      `json:"childRelation,omitempty"`
}

// IsConditionElement implements ConditionElement interface
func (l LogicCondition) IsConditionElement() {}

// MatchContext match logic condition with workflow context.
func (l LogicCondition) MatchContext(ctx context.Context) (bool, *MatchInfo, error) {
	info := &MatchInfo{
		Field:    l.Field,
		Operator: l.Operator,
	}
	lhs, err := expression.Eval(ctx, l.Field)
	if err != nil {
		info.Error = err.Error()
		return false, info, err
	}
	info.Actual = lhs

	// truthy/falsy never consult the value (plaita semantics); their JSON
	// condition frequently carries no value at all.
	if l.Operator == Truthy || l.Operator == Falsy {
		matched := isTruthy(lhs)
		if l.Operator == Falsy {
			matched = !matched
		}
		info.Matched = matched
		return matched, info, nil
	}

	rhs, err := expression.Eval(ctx, l.Value)
	if err != nil {
		info.Error = err.Error()
		return false, info, err
	}
	info.Expect = rhs

	// 组装条件信息：字段、操作符、预期值、实际值
	if f, ok := comparisonOperations[l.Operator]; ok {
		matched, err := f(lhs, rhs)
		info.Matched = matched
		if err != nil {
			info.Error = err.Error()
		}
		return matched, info, err
	}
	info.Matched = false
	info.Error = fmt.Sprintf("unsupported operator: %q", l.Operator)
	return false, info, fmt.Errorf("unsupported operator: %q", l.Operator)
}

func castToFloatsE(l, r any) (float64, float64, error) {
	lhs, err := cast.ToFloat64E(l)
	if err != nil {
		return 0, 0, err
	}

	rhs, err := cast.ToFloat64E(r)
	if err != nil {
		return 0, 0, err
	}
	return lhs, rhs, nil
}

func gte(l, r any) (bool, error) {
	lhs, rhs, err := castToFloatsE(l, r)
	if err != nil {
		return false, err
	}
	return lhs >= rhs, nil
}

func gt(l, r any) (bool, error) {
	lhs, rhs, err := castToFloatsE(l, r)
	if err != nil {
		return false, err
	}
	return lhs > rhs, nil
}

func lt(l, r any) (bool, error) {
	lhs, rhs, err := castToFloatsE(l, r)
	if err != nil {
		return false, err
	}
	return lhs < rhs, nil
}

func lte(l, r any) (bool, error) {
	lhs, rhs, err := castToFloatsE(l, r)
	if err != nil {
		return false, err
	}
	return lhs <= rhs, nil
}

func eq(l, r any) (bool, error) {
	if l == nil && r == nil {
		return true, nil
	}

	lValue := reflect.ValueOf(l)
	rValue := reflect.ValueOf(r)
	switch lValue.Kind() {
	case reflect.Bool, reflect.String:
		return l == r, nil
	case reflect.Float32, reflect.Float64, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		// 转换为float64进行比较
		lhs, rhs, err := castToFloatsE(l, r)
		if err != nil {
			return false, err
		}
		return lhs == rhs, nil
	case reflect.Slice:
		// 需要特殊判断使用 $NULL 关键字的情况，空切片和 nil 比较视为相等
		if lValue.Len() == 0 && r == nil {
			return true, nil
		}

		if lValue.Kind() != rValue.Kind() {
			return false, nil
		}
		if lValue.Len() != rValue.Len() {
			return false, nil
		}
		for i := 0; i < lValue.Len(); i++ {
			li := lValue.Index(i)
			ri := rValue.Index(i)
			if !li.CanInterface() || !ri.CanInterface() {
				return false, nil
			}
			ok, err := eq(li.Interface(), ri.Interface())
			if err != nil {
				return false, err
			}
			if !ok {
				return false, nil
			}
		}
		return true, nil
	default:
		return l == r, nil
	}
}

func ne(l, r any) (bool, error) {
	equal, err := eq(l, r)
	if err != nil {
		return false, err
	}
	return !equal, nil
}

func between(l, r any) (bool, error) {
	lhs, err := cast.ToFloat64E(l)
	if err != nil {
		return false, err
	}

	s, err := cast.ToSliceE(r)
	if err != nil {
		return false, err
	}

	if len(s) != 2 {
		return false, fmt.Errorf("value in between/notBetween condition should have two bound, but got len: %d", len(s))
	}

	low, err := cast.ToFloat64E(s[0])
	if err != nil {
		return false, err
	}

	high, err := cast.ToFloat64E(s[1])
	if err != nil {
		return false, err
	}

	return lhs >= low && lhs <= high, nil
}

func notBetween(l, r any) (bool, error) {
	b, err := between(l, r)
	if err != nil {
		return false, err
	}
	return !b, nil
}

func in(l, r any) (bool, error) {
	s, err := cast.ToSliceE(r)
	if err != nil {
		return false, err
	}

	for _, v := range s {
		equals, _ := eq(l, v)
		if equals {
			return true, nil
		}
	}
	return false, nil
}

func notIn(l, r any) (bool, error) {
	b, err := in(l, r)
	if err != nil {
		return false, err
	}
	return !b, nil
}

func like(l, r any) (bool, error) {
	lhs, err := cast.ToStringE(l)
	if err != nil {
		return false, err
	}

	rhs, err := cast.ToStringE(r)
	if err != nil {
		return false, err
	}

	return strings.Contains(lhs, rhs), nil
}

// contains follows plaita semantics: the right side is contained in the left
// (string substring, slice element, or map key).
func contains(l, r any) (bool, error) {
	switch lhs := l.(type) {
	case string:
		rhs, err := cast.ToStringE(r)
		if err != nil {
			return false, err
		}
		return strings.Contains(lhs, rhs), nil
	case []any:
		for _, v := range lhs {
			if equals, _ := eq(v, r); equals {
				return true, nil
			}
		}
		return false, nil
	case map[string]any:
		key, err := cast.ToStringE(r)
		if err != nil {
			return false, err
		}
		_, ok := lhs[key]
		return ok, nil
	default:
		s, err := cast.ToSliceE(l)
		if err != nil {
			return false, fmt.Errorf("contains: left operand must be string/array/map, got %T", l)
		}
		for _, v := range s {
			if equals, _ := eq(v, r); equals {
				return true, nil
			}
		}
		return false, nil
	}
}

func notContains(l, r any) (bool, error) {
	b, err := contains(l, r)
	if err != nil {
		return false, err
	}
	return !b, nil
}

// isTruthy mirrors Python truthiness for the JSON value domain: nil, zero
// numbers, empty string/slice/map and false are falsy; everything else truthy.
func isTruthy(v any) bool {
	switch t := v.(type) {
	case nil:
		return false
	case bool:
		return t
	case string:
		return t != ""
	case json.Number:
		f, err := t.Float64()
		return err == nil && f != 0
	case int:
		return t != 0
	case int64:
		return t != 0
	case float64:
		return t != 0
	case []any:
		return len(t) > 0
	case map[string]any:
		return len(t) > 0
	default:
		rv := reflect.ValueOf(v)
		switch rv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return rv.Int() != 0
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return rv.Uint() != 0
		case reflect.Float32, reflect.Float64:
			return rv.Float() != 0
		case reflect.Slice, reflect.Map, reflect.Array:
			return rv.Len() > 0
		case reflect.Ptr, reflect.Interface:
			return !rv.IsNil()
		default:
			return true
		}
	}
}

func truthy(l, _ any) (bool, error) { return isTruthy(l), nil }

func falsy(l, _ any) (bool, error) { return !isTruthy(l), nil }

// Expression returns the expression of the condition.
// TODO: 尝试使用表达式引擎完成逻辑运算
func (l LogicCondition) Expression() (string, error) {
	switch l.Operator {
	case Gt:
		return fmt.Sprintf("%s > %v", l.Field, l.Value), nil
	case Gte:
		return fmt.Sprintf("%s >= %v", l.Field, l.Value), nil
	case Lt:
		return fmt.Sprintf("%s < %v", l.Field, l.Value), nil
	case Lte:
		return fmt.Sprintf("%s <= %v", l.Field, l.Value), nil
	case Eq:
		return fmt.Sprintf("%s == %v", l.Field, l.Value), nil
	case Ne:
		return fmt.Sprintf("%s != %v", l.Field, l.Value), nil
	case Between:
		low, high, err := parseBetweenBound(l.Value)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("between(%s, %v, %v)", l.Field, low, high), nil
	case NotBetween:
		low, high, err := parseBetweenBound(l.Value)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("notBetween(%s, %v, %v)", l.Field, low, high), nil
	case In:
		s, err := cast.ToSliceE(l.Value)
		if err != nil {
			return "", fmt.Errorf("value should be a slice in in condition: %v", l.Value)
		}
		return fmt.Sprintf("in(%s, %v)", l.Field, s), nil
	case NotIn:
		return fmt.Sprintf("notIn(%s, %v)", l.Field, l.Value), nil
	case Like:
		return fmt.Sprintf("like(%s, %q)", l.Field, l.Value), nil
	default:
		return "", nil
	}
}

func parseBetweenBound(v any) (low, high any, err error) {
	if reflect.TypeOf(v).Kind() != reflect.Slice {
		return 0, 0, fmt.Errorf("value should be a slice in between condition: %v", v)
	}

	bounds := cast.ToSlice(v)
	if len(bounds) != 2 {
		return 0, 0, fmt.Errorf("value should be a slice with 2 elements in between condition: %v", v)
	}

	return bounds[0], bounds[1], nil
}

// LogicOperator is type for logic operator.
type LogicOperator string

// LogicOperator enums
const (
	And LogicOperator = "and"
	Or  LogicOperator = "or"
)

var supportedLogicOperators = []LogicOperator{And, Or}

// Validate validates the logic operator.
func (l LogicOperator) Validate() error {
	if lo.Contains(supportedLogicOperators, l) {
		return nil
	}
	return fmt.Errorf("unsupported logic operator: %s", l)
}

// LogicConditionGroup logic condition group.
type LogicConditionGroup struct {
	Relation   LogicOperator      `json:"relation"`
	Conditions []ConditionElement `json:"conditions"`
}

// Temporary structure to help in unmarshalling
type logicConditionGroupTemp struct {
	Relation   LogicOperator     `json:"relation"`
	Conditions []json.RawMessage `json:"conditions"`
}

// IsConditionElement implements ConditionElement interface
func (l *LogicConditionGroup) IsConditionElement() {}

// UnmarshalJSON implements json.Unmarshaler interface
func (l *LogicConditionGroup) UnmarshalJSON(data []byte) error {
	var temp logicConditionGroupTemp
	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}

	l.Relation = temp.Relation
	l.Conditions = make([]ConditionElement, len(temp.Conditions))

	for i, condData := range temp.Conditions {
		var raw map[string]interface{}
		if err := json.Unmarshal(condData, &raw); err != nil {
			return err
		}

		if _, exists := raw["field"]; exists {
			var lc LogicCondition
			if err := json.Unmarshal(condData, &lc); err != nil {
				return err
			}
			l.Conditions[i] = lc
		} else if _, exists := raw["relation"]; exists {
			var nestedLcg LogicConditionGroup
			if err := json.Unmarshal(condData, &nestedLcg); err != nil {
				return err
			}
			l.Conditions[i] = &nestedLcg
		} else {
			return fmt.Errorf("unknown condition format")
		}
	}

	return nil
}

// MatchContext matches logic condition group with workflow context.
func (l *LogicConditionGroup) MatchContext(ctx context.Context) (bool, *MatchInfo, error) {
	matchInfo := &MatchInfo{
		ChildRelation: l.Relation,
		Children:      make([]*MatchInfo, 0),
	}
	workflowContext := runtime.WorkflowRuntimeContext(ctx)
	eagerMode := workflowContext.GetGlobalVariable("switch:eagerMode")
	IsEagerMode := false
	if eagerMode != nil {
		IsEagerMode, _ = eagerMode.(bool)
	}
	for _, c := range l.Conditions {
		matched, info, err := c.MatchContext(ctx)
		if !IsEagerMode {
			if err != nil {
				return false, nil, err
			}
			if l.Relation == And && !matched {
				return false, nil, nil
			}
			if l.Relation == Or && matched {
				return true, nil, nil
			}
		}
		matchInfo.Children = append(matchInfo.Children, info)
	}
	if !IsEagerMode {
		return l.Relation == And, nil, nil
	}
	// 获取Children中的所有Matched字段，根据Relation进行逻辑运算,返回最终结果
	var result bool
	switch l.Relation {
	case And:
		result = true
		for _, info := range matchInfo.Children {
			result = result && info.Matched
		}
	case Or:
		result = false
		for _, info := range matchInfo.Children {
			result = result || info.Matched
		}
	}
	matchInfo.Matched = result
	return result, matchInfo, nil
}

// Expression returns the expression of the condition group.
func (l *LogicConditionGroup) Expression() (string, error) {
	var exprs []string
	for _, c := range l.Conditions {
		expr, err := c.Expression()
		if err != nil {
			return "", err
		}
		exprs = append(exprs, expr)
	}

	return strings.Join(exprs, string(l.Relation)), nil
}

// Condition represents a logic match condition.
type Condition struct {
	ConditionType       conditionType
	ConditionExpression string
	LogicCondition      LogicCondition
	LogicConditionGroup LogicConditionGroup
}

type conditionType int

const (
	conditionTypeLogicCondition conditionType = iota
	conditionTypeLogicConditionGroup
	conditionTypeExpression
)

// UnmarshalJSON unmarshal json to condition.
func (c *Condition) UnmarshalJSON(data []byte) error {
	var lc LogicCondition
	err := jsonAPI.Unmarshal(data, &lc)
	if err == nil {
		c.LogicCondition = lc
		c.ConditionType = conditionTypeLogicCondition
		return nil
	}
	var lcg LogicConditionGroup
	err = jsonAPI.Unmarshal(data, &lcg)
	if err == nil {
		c.LogicConditionGroup = lcg
		c.ConditionType = conditionTypeLogicConditionGroup
		return nil
	}
	var ce string
	err = jsonAPI.Unmarshal(data, &ce)
	if err == nil {
		c.ConditionExpression = ce
		c.ConditionType = conditionTypeExpression
		return nil
	}
	return fmt.Errorf("unable parse condition from json: %s", data)
}

// MatchContext matches the condition with the workflow node context.
func (c *Condition) MatchContext(ctx context.Context) (bool, *MatchInfo, error) {
	if c.ConditionType == conditionTypeLogicCondition &&
		c.LogicCondition.Field == "" && c.LogicCondition.Operator == "" {
		// zero-value condition (e.g. a plaita loop without one): always match
		return true, &MatchInfo{Matched: true}, nil
	}
	switch c.ConditionType {
	case conditionTypeLogicCondition:
		return c.LogicCondition.MatchContext(ctx)
	case conditionTypeLogicConditionGroup:
		return c.LogicConditionGroup.MatchContext(ctx)
	case conditionTypeExpression:
		return matchExpression(ctx, c.ConditionExpression)
	default:
		return false, nil, fmt.Errorf("unsupported condition type: %q", c.ConditionType)
	}
}

func matchExpression(ctx context.Context, expr string) (bool, *MatchInfo, error) {
	result, err := expression.Eval(ctx, expr)
	return cast.ToBool(result), nil, err
}
