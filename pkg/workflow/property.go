package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"

	"github.com/spf13/cast"

	validation "github.com/go-ozzo/ozzo-validation"
)

// DataType represents type for basic data type
type DataType string

// builtin data type
const (
	String  DataType = "string"
	Integer DataType = "integer"
	Float   DataType = "float"
	Bool    DataType = "bool"
	Object  DataType = "object"
	Array   DataType = "array"
	// Any matches every value (plaita types.ANY).
	Any DataType = "any"
	// Number accepts any numeric value (plaita types.NUMBER, int/float).
	Number DataType = "number"
)

var supportedDataTypes = []DataType{
	String,
	Integer,
	Float,
	Bool,
	Object,
	Array,
	Any,
	Number,
}

// ArrayItemType array item type definition
type ArrayItemType struct {
	itemProperty  *Property
	itemsProperty []*Property
}

// UnmarshalJSON unmarshal json to ItemType
func (a *ArrayItemType) UnmarshalJSON(data []byte) error {
	property := new(Property)
	if err := json.Unmarshal(data, property); err == nil {
		a.itemProperty = property
		return nil
	}

	var properties []*Property
	if err := json.Unmarshal(data, &properties); err != nil {
		return fmt.Errorf("unable to unmarshal %s to array item property", data)
	}
	a.itemsProperty = properties
	return nil
}

// ItemProperty get item property
func (a *ArrayItemType) ItemProperty() *Property {
	if a.itemProperty != nil && len(a.itemProperty.DataType) > 0 {
		return a.itemProperty
	}
	if len(a.itemsProperty) > 0 {
		return a.itemsProperty[0]
	}
	return nil
}

// Property data property definition
type Property struct {
	DataType     DataType            `json:"dataType"`               // 参数取值类型
	Name         string              `json:"name,omitempty"`         // 参数字段名，英文字母及数字、下划线构成
	Label        string              `json:"label,omitempty"`        // 参数显示名
	Desc         string              `json:"desc,omitempty"`         // 参数说明
	DefaultValue any                 `json:"defaultValue,omitempty"` // 默认值
	Properties   map[string]Property `json:"properties,omitempty"`   // 当字段类型为对象时，其对应的字段结构
	IsRequired   bool                `json:"isRequired,omitempty"`   // 是否必填
	MaxLength    int                 `json:"maxLength,omitempty"`    // 最大长度
	Max          *float64            `json:"max,omitempty"`          // 最大值（schema 元数据；校验不使用，与 plaita match 一致）
	Min          *float64            `json:"min,omitempty"`          // 最小值（schema 元数据；校验不使用，与 plaita match 一致）
	// Strict selects the plaita-match-compatible value model: strict type
	// checks, no cast coercion. It is opt-in per property — the ir loader
	// (ir.Load) marks every property of a deployed flow strict, while flows
	// built directly as Go structs keep the historical lenient (cast) model.
	Strict bool `json:"strict,omitempty"`

	// ArrayItemType 当字段类型为Array时，数组元素的数据类型属性
	ArrayItemType ArrayItemType `json:"itemType,omitempty"`
}

// Validate check if the Property definition is valid
func (p *Property) Validate() error {
	return validation.ValidateStruct(p,
		validation.Field(&p.DataType, validation.Required, validation.In(supportedDataTypes)),
		validation.Field(&p.MaxLength, validation.Min(0)))
}

// ValidateValue 检查所给的值是否符合Property定义格式。
//
// 两种值模型（issue #23 的「严格/宽松」双模式，ir.Load 选择严格）：
//
//   - Strict（plaita io.match 口径）：严格类型判断，不做 cast 强转——
//     string 须为非空 string；integer 须为整数（Go 整型，或 JSON 解码产生的
//     整值 float64：Go 的 encoding/json 把数字统一解成 float64，无法还原
//     5 与 5.0 的区别，这是与 Python `type(v) is int` 的已知偏差，见 AGENTS.md）；
//     float/number 接受整型与浮点型（bool 除外）；bool 只接受 bool；
//     object 须为 map；array 须为 slice；any 恒真。max/min 不参与校验
//     （与 plaita match 一致，仅是 schema 元数据）。
//
//   - 宽松（缺省，历史行为）：cast 系强转；max/min 按闭区间校验已被移除，
//     与 plaita 一致不再对边界值单独报错。
func (p *Property) ValidateValue(value any) error {
	if p == nil || len(p.DataType) == 0 {
		return nil
	}

	if value == nil {
		if p.IsRequired {
			return fmt.Errorf("value is required but not provided")
		}
		return nil
	}

	if p.Strict {
		return p.validateValueStrict(value)
	}
	return p.validateValueLenient(value)
}

// --- strict model (plaita io.match) ---

func (p *Property) validateValueStrict(value any) error {
	switch p.DataType {
	case Any:
		return nil
	case String:
		v, ok := value.(string)
		if !ok {
			return typeMismatchError(value, String)
		}
		return p.validateStringValueStrict(v)
	case Integer:
		if !isIntegerValue(value) {
			return typeMismatchError(value, Integer)
		}
		return nil
	case Float, Number:
		if !isFloatValue(value) {
			return typeMismatchError(value, p.DataType)
		}
		return nil
	case Bool:
		if _, ok := value.(bool); !ok {
			return typeMismatchError(value, Bool)
		}
		return nil
	case Object:
		v, ok := toMapStrict(value)
		if !ok {
			return typeMismatchError(value, Object)
		}
		return p.validateObjectValues(v)
	case Array:
		v, ok := toSliceStrict(value)
		if !ok {
			return typeMismatchError(value, Array)
		}
		return p.validateArrayValues(v)
	default:
		return fmt.Errorf("unsupported data type %q", p.DataType)
	}
}

func (p *Property) validateStringValueStrict(value string) error {
	if p.MaxLength > 0 && len(value) > p.MaxLength {
		return fmt.Errorf("expected max string length is %d but got %d", p.MaxLength, len(value))
	}
	if value == "" {
		// plaita match: isinstance(obj, str) and obj — empty strings fail.
		return errors.New("string value must not be empty")
	}
	return nil
}

// isIntegerValue reports whether value is an integer under the strict type
// model: Go integer types always; float64/float32/json.Number only when
// integral (values decoded from JSON lose the int/float distinction in Go).
func isIntegerValue(value any) bool {
	switch v := value.(type) {
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64:
		return true
	case float64:
		return isIntegralFloat(v)
	case float32:
		return isIntegralFloat(float64(v))
	case json.Number:
		_, err := v.Int64()
		return err == nil
	default:
		return false
	}
}

// isFloatValue reports whether value is numeric (int or float) but not bool
// — plaita float/number accepts int and float; bool is rejected even though
// Python's isinstance(True, int) would let it slip through.
func isFloatValue(value any) bool {
	switch v := value.(type) {
	case bool:
		return false
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return true
	case json.Number:
		_, err := v.Float64()
		return err == nil
	default:
		return false
	}
}

func isIntegralFloat(v float64) bool {
	return !math.IsInf(v, 0) && !math.IsNaN(v) && v == math.Trunc(v)
}

// toMapStrict converts a map value without coercion: map[string]any directly,
// other map kinds via reflection (keys stringified, values kept as-is) —
// the Go shape of Python's isinstance(obj, dict).
func toMapStrict(value any) (map[string]any, bool) {
	if m, ok := value.(map[string]any); ok {
		return m, true
	}
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Map {
		return nil, false
	}
	out := make(map[string]any, rv.Len())
	iter := rv.MapRange()
	for iter.Next() {
		out[fmt.Sprint(iter.Key().Interface())] = iter.Value().Interface()
	}
	return out, true
}

// toSliceStrict converts a slice value without coercion ([]any directly,
// other slices via reflection) — the Go shape of Python's isinstance(obj,
// list). []byte is bytes in Python terms, not a list, and is rejected.
func toSliceStrict(value any) ([]any, bool) {
	if v, ok := value.([]any); ok {
		return v, true
	}
	rv := reflect.Indirect(reflect.ValueOf(value))
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			return nil, false
		}
		out := make([]any, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			out[i] = rv.Index(i).Interface()
		}
		return out, true
	default:
		return nil, false
	}
}

// --- lenient model (historical cast behavior, minus max/min) ---

func (p *Property) validateValueLenient(value any) error {
	switch p.DataType {
	case Any:
		return nil
	case String:
		v, err := cast.ToStringE(value)
		if err != nil {
			return typeMismatchError(value, String)
		}
		return p.validateStringValue(v)
	case Integer:
		if _, err := cast.ToIntE(value); err != nil {
			return typeMismatchError(value, Integer)
		}
		return nil
	case Float:
		if _, err := cast.ToFloat64E(value); err != nil {
			return typeMismatchError(value, Float)
		}
		return nil
	case Number:
		if _, err := cast.ToFloat64E(value); err != nil {
			return typeMismatchError(value, Number)
		}
		return nil
	case Bool:
		if _, err := cast.ToBoolE(value); err != nil {
			return typeMismatchError(value, Bool)
		}
		return nil
	case Object:
		v, err := cast.ToStringMapE(value)
		if err != nil {
			return typeMismatchError(value, Object)
		}
		return p.validateObjectValues(v)
	case Array:
		v, err := cast.ToSliceE(value)
		if err != nil {
			return typeMismatchError(value, Array)
		}
		return p.validateArrayValues(v)
	default:
		return fmt.Errorf("unsupported data type %q", p.DataType)
	}
}

func (p *Property) validateStringValue(value string) error {
	if p.MaxLength > 0 && len(value) > p.MaxLength {
		return fmt.Errorf("expected max string length is %d but got %d", p.MaxLength, len(value))
	}
	return nil
}

func (p *Property) validateArrayValues(values []any) error {
	arrayItemProperty := p.ArrayItemType.ItemProperty()
	if arrayItemProperty == nil {
		return errors.New("invalid array item property: undefined")
	}
	for _, v := range values {
		if err := arrayItemProperty.ValidateValue(v); err != nil {
			return fmt.Errorf("validate array item: %v", err)
		}
	}
	return nil
}

func (p *Property) validateObjectValues(value map[string]any) error {
	for key, prop := range p.Properties {
		if err := prop.ValidateValue(value[key]); err != nil {
			return fmt.Errorf("invalid value for property %q: %v", key, err)
		}
	}
	return nil
}

func typeMismatchError(value any, expectedType DataType) error {
	return fmt.Errorf("expected value data type is %q but got type: %T, value: %v", expectedType, value, value)
}
