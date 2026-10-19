package model

import (
	"encoding/json"

	"github.com/spf13/cast"
)

type kind int

const (
	kindString kind = iota
	kindInt
	kindFloat
	kindBool
	kindAny
)

// Value is container for any type of value.
type Value struct {
	kind kind
	s    string
	i    int
	f    float64
	b    bool
	v    any
}

// NewValue creates a new Value
func NewValue(v any) Value {
	switch t := v.(type) {
	case string:
		return Value{kind: kindString, s: t}
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return Value{kind: kindInt, i: cast.ToInt(t)}
	case float32, float64:
		return Value{kind: kindFloat, f: cast.ToFloat64(t)}
	case bool:
		return Value{kind: kindBool, b: t}
	default:
		return Value{kind: kindAny, v: t}
	}
}

// FromString creates a new Value from string
func FromString(v string) Value {
	return Value{kind: kindString, s: v}
}

// FromInt creates a new Value from int
func FromInt(v int) Value {
	return Value{kind: kindInt, i: v}
}

// FromFloat creates a new Value from float
func FromFloat(v float64) Value {
	return Value{kind: kindFloat, f: v}
}

// FromBool creates a new Value from bool
func FromBool(v bool) Value {
	return Value{kind: kindBool, b: v}
}

// EmptyValue creates a new empty Value
func EmptyValue() Value {
	return Value{v: struct{}{}}
}

// AsString return the string value
func (v *Value) AsString() string {
	if v.kind == kindString {
		return v.s
	}
	return cast.ToString(v.v)
}

// AsInt return the int value
func (v *Value) AsInt() int {
	if v.kind == kindInt {
		return v.i
	}
	return cast.ToInt(v.v)
}

// AsFloat return the float value
func (v *Value) AsFloat() float64 {
	if v.kind == kindFloat {
		return v.f
	}
	return cast.ToFloat64(v.v)
}

// AsBool return the bool value
func (v *Value) AsBool() bool {
	if v.kind == kindBool {
		return v.b
	}
	return cast.ToBool(v.v)
}

// AsSlice return the slice value
func (v *Value) AsSlice() []any {
	return cast.ToSlice(v.v)
}

// AsMap return the map value
func (v *Value) AsMap() map[string]any {
	return cast.ToStringMap(v.v)
}

// AsStringSlice return the string slice value
func (v *Value) AsStringSlice() []string {
	return cast.ToStringSlice(v.v)
}

// AsIntSlice return the int slice value
func (v *Value) AsIntSlice() []int {
	return cast.ToIntSlice(v.v)
}

// AsBoolSlice return the bool slice value
func (v *Value) AsBoolSlice() []bool {
	return cast.ToBoolSlice(v.v)
}

// AsAny return the `any` value
func (v *Value) AsAny() any {
	switch v.kind {
	case kindString:
		return v.s
	case kindInt:
		return v.i
	case kindFloat:
		return v.f
	case kindBool:
		return v.b
	default:
		return v.v
	}
}

// UnmarshalJSON unmarshal the given json object into the respective Value subtype.
func (v *Value) UnmarshalJSON(data []byte) error {
	var i interface{}
	if err := json.Unmarshal(data, &i); err != nil {
		return err
	}
	switch t := i.(type) {
	case string:
		v.kind = kindString
		v.s = t
	case float64:
		v.kind = kindFloat
		v.f = t
	case bool:
		v.kind = kindBool
		v.b = t
	default:
		v.kind = kindAny
		v.v = t
	}
	return nil
}

// MarshalJSON marshal the Value into the respective json object.
func (v *Value) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.AsAny())
}
