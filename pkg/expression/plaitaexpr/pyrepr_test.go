package plaitaexpr

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPyFloatRepr pins the Python float repr rule (CPython float_repr:
// positional with a guaranteed ".0" for decimal exponents in (-4, 16],
// scientific otherwise). Every expectation below was produced with python3.
func TestPyFloatRepr(t *testing.T) {
	cases := []struct {
		f    float64
		want string
	}{
		{3.0, "3.0"},
		{100.0, "100.0"},
		{1e15, "1000000000000000.0"},
		{1e16, "1e+16"},
		{1e17, "1e+17"},
		{1e20, "1e+20"},
		{123456789.123, "123456789.123"},
		{0.0001, "0.0001"},
		{0.00001, "1e-05"},
		{1.5e-05, "1.5e-05"},
		{2.5, "2.5"},
		{0.1, "0.1"},
		{1234567890123456.0, "1234567890123456.0"},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, pyFloatRepr(c.f), "pyFloatRepr(%v)", c.f)
	}
	assert.Equal(t, "-0.0", pyFloatRepr(negZeroFloat()))
	assert.Equal(t, "inf", pyFloatRepr(infFloat(1)))
	assert.Equal(t, "-inf", pyFloatRepr(infFloat(-1)))
	assert.Equal(t, "nan", pyFloatRepr(nanFloat()))
}

// TestPyStr pins Python str()/repr() rendering for the JSON value domain
// (issue #8 semantic authority). Dict key order is the documented divergence
// (sorted, not insertion order).
func TestPyStr(t *testing.T) {
	cases := []struct {
		name string
		v    any
		want string
	}{
		{"None", nil, "None"},
		{"True", true, "True"},
		{"False", false, "False"},
		{"int", 3, "3"},
		{"int64", int64(-7), "-7"},
		{"float", 2.5, "2.5"},
		{"float integral", 3.0, "3.0"},
		{"string", "txt", "txt"},
		{"empty list", []any{}, "[]"},
		{"list", []any{json.Number("1"), "x", true, nil}, "[1, 'x', True, None]"},
		{"nested list", []any{1.0, []any{"a"}}, "[1.0, ['a']]"},
		{"dict", map[string]any{"b": 2.0, "a": 1.0}, "{'a': 1.0, 'b': 2.0}"},
		{"nested dict", map[string]any{"l": []any{1.0}}, "{'l': [1.0]}"},
		{"json.Number int", json.Number("42"), "42"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, PyStr(c.v))
		})
	}
}

// TestPyReprStrings pins Python repr quoting: single quotes preferred, double
// quotes when the text contains ' but not ".
func TestPyReprStrings(t *testing.T) {
	assert.Equal(t, `'x'`, PyRepr("x"))
	assert.Equal(t, `"it's"`, PyRepr("it's"))
	assert.Equal(t, `'say "hi"'`, PyRepr(`say "hi"`))
	assert.Equal(t, `'a\nb'`, PyRepr("a\nb"))
}

// TestTemplateConstant pins the bare-literal grammar of `{% ... %}` bodies.
func TestTemplateConstant(t *testing.T) {
	cases := []struct {
		in   string
		want any
		ok   bool
	}{
		{"true", true, true},
		{"True", true, true},
		{"false", false, true},
		{"False", false, true},
		{"None", nil, true},
		{"null", nil, true},
		{"1", 1, true},
		{"-3", -3, true},
		{"2.5", 2.5, true},
		{"1e3", 1000.0, true},
		{"'txt'", "txt", true},
		{"world", nil, false},
		{"", nil, false},
		{"@x", nil, false},
	}
	for _, c := range cases {
		got, ok := TemplateConstant(c.in)
		require.Equal(t, c.ok, ok, "TemplateConstant(%q) ok", c.in)
		if c.ok {
			assert.Equal(t, c.want, got, "TemplateConstant(%q)", c.in)
		}
	}
}

func negZeroFloat() float64  { return math.Copysign(0, -1) }
func infFloat(s int) float64 { return math.Inf(s) }
func nanFloat() float64      { return math.NaN() }
