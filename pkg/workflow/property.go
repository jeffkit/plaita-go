package workflow

import (
	"encoding/json"
	"errors"
	"fmt"

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
)

var supportedDataTypes = []DataType{
	String,
	Integer,
	Float,
	Bool,
	Object,
	Array,
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
	if err := json.Unmarshal(data, &properties); err == nil {
		a.itemsProperty = properties
	}
	return fmt.Errorf("unable to unmarshal %s to array item property", data)
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
	Max          *float64            `json:"max,omitempty"`          // 最大值
	Min          *float64            `json:"min,omitempty"`          // 最小值

	// ArrayItemType 当字段类型为Array时，数组元素的数据类型属性
	ArrayItemType ArrayItemType `json:"itemType,omitempty"`
}

// Validate check if the Property definition is valid
func (p *Property) Validate() error {
	return validation.ValidateStruct(p,
		validation.Field(&p.DataType, validation.Required, validation.In(supportedDataTypes)),
		validation.Field(&p.MaxLength, validation.Min(0)))
}

// ValidateValue 检查所给的值是否符合Property定义格式
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

	switch p.DataType {
	case String:
		v, err := cast.ToStringE(value)
		if err != nil {
			return typeMismatchError(v, String, err)
		}
		return p.validateStringValue(v)
	case Integer:
		v, err := cast.ToIntE(value)
		if err != nil {
			return typeMismatchError(v, Integer, err)
		}
		return p.validateIntegerValue(v)
	case Float:
		v, err := cast.ToFloat64E(value)
		if err != nil {
			return typeMismatchError(v, Float, err)
		}
		return p.validateFloatValue(v)
	case Bool:
		v, err := cast.ToBoolE(value)
		if err != nil {
			return typeMismatchError(v, Bool, err)
		}
		return p.validateBoolValue(v)
	case Object:
		v, err := cast.ToStringMapE(value)
		if err != nil {
			return typeMismatchError(v, Object, err)
		}
		return p.validateObjectValues(v)
	case Array:
		v, err := cast.ToSliceE(value)
		if err != nil {
			return typeMismatchError(v, Array, err)
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

func (p *Property) validateFloatValue(value float64) error {
	if p.Max != nil && gte(value, *p.Max) {
		return fmt.Errorf("expected max value is %f but got %f", *p.Max, value)
	}
	if p.Min != nil && lte(value, *p.Min) {
		return fmt.Errorf("expected min value is %f but got %f", *p.Min, value)
	}
	return nil
}

func (p *Property) validateBoolValue(_ bool) error {
	return nil // Nothing to validate now
}

func (p *Property) validateIntegerValue(value int) error {
	if p.Max != nil && gte(value, *p.Max) {
		return fmt.Errorf("expected max value is %f but got %d", *p.Max, value)
	}
	if p.Min != nil && lte(value, *p.Min) {
		return fmt.Errorf("expected min value is %f but got %d", *p.Min, value)
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

func typeMismatchError(v any, expectedType DataType, err error) error {
	return fmt.Errorf("expected value data type is %q but got type: %T, value: %v, error: %v", expectedType, v, v, err)
}

func gte(value any, min float64) bool {
	return cast.ToFloat64(value) >= min
}

func lte(value any, max float64) bool {
	return cast.ToFloat64(value) <= max
}
