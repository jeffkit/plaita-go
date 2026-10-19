package plaitaexpr

// plaita_functions.go ports the missing plaita expression functions
// (Python: plaita/core/expression.py, _fn_* / default registry) to the Go
// expression layer. Each function keeps the Python semantics documented
// below; deviations forced by Go's type system are called out inline.

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// numericOf reports whether v belongs to plaita's numeric domain and returns
// it as float64. Python semantics: bool IS an int subclass (True == 1), and
// int/float compare cross-type. Strings are deliberately NOT numeric here —
// Python `"1" == 1` is False, so cast-based coercion would corrupt eq/ne.
func numericOf(v any) (float64, bool) {
	switch t := v.(type) {
	case bool:
		if t {
			return 1, true
		}
		return 0, true
	case json.Number:
		f, err := t.Float64()
		return f, err == nil
	case string, nil:
		return 0, false
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(rv.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(rv.Uint()), true
	case reflect.Float32, reflect.Float64:
		return rv.Float(), true
	default:
		return 0, false
	}
}

// isTruthy mirrors Python truthiness (bool(c)) for the JSON value domain:
// nil, false, zero numbers, "" and empty slice/map are falsy; everything
// else is truthy.
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
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int() != 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return rv.Uint() != 0
	case reflect.Float32, reflect.Float64:
		return rv.Float() != 0
	case reflect.Slice, reflect.Map, reflect.Array, reflect.String:
		return rv.Len() > 0
	case reflect.Ptr, reflect.Interface:
		return !rv.IsNil()
	default:
		return true
	}
}

// plaitaEqual implements Python `==` for the expression value domain:
// nil == nil, cross-type numeric equality (1 == 1.0, True == 1), exact string
// equality, and deep structural equality for slices/maps (Python containers
// compare element-wise, so [1] == [1.0] is True). Unrelated types are simply
// not equal (Python == never raises for these).
func plaitaEqual(l, r any) bool {
	if l == nil || r == nil {
		return l == nil && r == nil
	}
	if lf, ok := numericOf(l); ok {
		if rf, ok := numericOf(r); ok {
			return lf == rf
		}
		return false
	}
	if ls, ok := l.(string); ok {
		rs, ok := r.(string)
		return ok && ls == rs
	}
	lv, rv := reflect.ValueOf(l), reflect.ValueOf(r)
	switch lv.Kind() {
	case reflect.Slice, reflect.Array:
		switch rv.Kind() {
		case reflect.Slice, reflect.Array:
			if lv.Len() != rv.Len() {
				return false
			}
			for i := 0; i < lv.Len(); i++ {
				if !plaitaEqual(lv.Index(i).Interface(), rv.Index(i).Interface()) {
					return false
				}
			}
			return true
		}
	case reflect.Map:
		if rv.Kind() == reflect.Map {
			if lv.Len() != rv.Len() {
				return false
			}
			for _, key := range lv.MapKeys() {
				other := rv.MapIndex(key)
				if !other.IsValid() {
					return false
				}
				if !plaitaEqual(lv.MapIndex(key).Interface(), other.Interface()) {
					return false
				}
			}
			return true
		}
	}
	return false
}

// plaitaCompare implements Python's ordering operators (a OP b) for gt/gte/
// lt/lte. plaita's type domain: numbers (int/float/bool cross-type) and
// strings (lexicographic, strings only against strings). Anything else —
// including mixed number/string — raises TypeError in Python, propagated
// here as an error (plaita re-raises native operator errors verbatim).
func plaitaCompare(op string, l, r any) (int, error) {
	if lf, ok := numericOf(l); ok {
		if rf, ok := numericOf(r); ok {
			switch {
			case lf < rf:
				return -1, nil
			case lf > rf:
				return 1, nil
			default:
				return 0, nil
			}
		}
	}
	if ls, ok := l.(string); ok {
		if rs, ok := r.(string); ok {
			return strings.Compare(ls, rs), nil
		}
	}
	return 0, fmt.Errorf("$F.%s: unsupported operand types: %T and %T", op, l, r)
}

// eq is plaita $F.eq(a, b) — Python `a == b` (expression.py _fn_eq).
func eq(l, r any) bool {
	return plaitaEqual(l, r)
}

// ne is plaita $F.ne(a, b) — Python `a != b` (expression.py _fn_ne).
func ne(l, r any) bool {
	return !plaitaEqual(l, r)
}

// gt is plaita $F.gt(a, b) — Python `a > b` (expression.py _fn_gt).
func gt(l, r any) (bool, error) {
	c, err := plaitaCompare("gt", l, r)
	return c > 0, err
}

// gte is plaita $F.gte(a, b) — Python `a >= b` (expression.py _fn_gte).
func gte(l, r any) (bool, error) {
	c, err := plaitaCompare("gte", l, r)
	return c >= 0, err
}

// lt is plaita $F.lt(a, b) — Python `a < b` (expression.py _fn_lt).
func lt(l, r any) (bool, error) {
	c, err := plaitaCompare("lt", l, r)
	return c < 0, err
}

// lte is plaita $F.lte(a, b) — Python `a <= b` (expression.py _fn_lte).
func lte(l, r any) (bool, error) {
	c, err := plaitaCompare("lte", l, r)
	return c <= 0, err
}

// ifelse is plaita $F.ifelse(c, a, b) — Python `a if c else b`
// (expression.py _fn_ifelse). plaita documents the branch evaluation as
// EAGER (both branches are computed before the call — plain function-call
// semantics, no short-circuit); goval evaluates arguments before invoking
// the function, so the eager semantics carry over unchanged. The condition
// uses Python truthiness (0, "", [], {}, nil are falsy).
func ifelse(cond any, trueVal, falseVal any) any {
	if isTruthy(cond) {
		return trueVal
	}
	return falseVal
}

// notContains is plaita $F.notContains(a, b) — Python `b not in a`
// (expression.py _fn_notContains). plaita's containment domain:
//   - a is string: b must be a string, substring test
//   - a is list: membership via == (so 1 is "in" [1.0])
//   - a is dict: b is tested against the keys
//   - anything else: Python raises TypeError, propagated as an error
func notContains(container, needle any) (bool, error) {
	contained, err := plaitaContains(container, needle)
	if err != nil {
		return false, err
	}
	return !contained, nil
}

// plaitaContains implements Python `b in a` for the container kinds listed
// on notContains.
func plaitaContains(container, needle any) (bool, error) {
	switch c := container.(type) {
	case nil:
		return false, fmt.Errorf("$F.notContains: argument of type 'NoneType' is not a container")
	case string:
		s, ok := needle.(string)
		if !ok {
			return false, fmt.Errorf(
				"$F.notContains: 'in <string>' requires string as left operand, got %T", needle)
		}
		return strings.Contains(c, s), nil
	}
	rv := reflect.ValueOf(container)
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		for i := 0; i < rv.Len(); i++ {
			if plaitaEqual(rv.Index(i).Interface(), needle) {
				return true, nil
			}
		}
		return false, nil
	case reflect.Map:
		// Python dict: `b in a` tests keys via hash+==. A needle whose type
		// can never equal the key type is simply not contained.
		if needle == nil {
			return false, nil
		}
		key := reflect.ValueOf(needle)
		if !key.IsValid() || key.Type() != rv.Type().Key() {
			return false, nil
		}
		return rv.MapIndex(key).IsValid(), nil
	default:
		return false, fmt.Errorf(
			"$F.notContains: unsupported container type %T (string, list or dict expected)", container)
	}
}

// pop is plaita $F.pop(a, b) — Python `a.pop(b)` (expression.py _fn_pop,
// registered as a side-effect/mutator). It supports BOTH containers:
//   - list:  list.pop(index) — index required (plaita always passes it),
//     negative index counts from the end, IndexError when out of range;
//     returns the removed element and shrinks the list in place.
//   - dict:  dict.pop(key) — no default variant in plaita, KeyError when the
//     key is missing; returns the removed value and deletes the key in place.
//
// Go note: map deletion is fully in-place (maps are reference types). For a
// slice the elements are shifted and the tail zeroed in the shared backing
// array, but Go cannot shrink the caller's slice header — a subsequent plain
// read of the original variable still reports the old length. The function
// returns the popped element (plaita's contract); callers needing the
// shrunken list should persist `$F.slice(l, 0, $F.len(l)-1)`-style results
// themselves.
func pop(container, key any) (any, error) {
	switch c := container.(type) {
	case nil:
		return nil, fmt.Errorf("$F.pop: 'NoneType' object has no attribute 'pop'")
	case map[string]any:
		k, ok := key.(string)
		if !ok {
			// Python dict.pop(non-string) on a string-keyed dict: KeyError.
			return nil, fmt.Errorf("$F.pop: KeyError: %v", key)
		}
		v, found := c[k]
		if !found {
			return nil, fmt.Errorf("$F.pop: KeyError: %q", k)
		}
		delete(c, k)
		return v, nil
	case string:
		return nil, fmt.Errorf("$F.pop: 'str' object has no attribute 'pop'")
	}
	rv := reflect.ValueOf(container)
	switch rv.Kind() {
	case reflect.Map:
		keyVal := reflect.ValueOf(key)
		if !keyVal.IsValid() || keyVal.Type() != rv.Type().Key() {
			return nil, fmt.Errorf("$F.pop: KeyError: %v", key)
		}
		existing := rv.MapIndex(keyVal)
		if !existing.IsValid() {
			return nil, fmt.Errorf("$F.pop: KeyError: %v", key)
		}
		result := existing.Interface()
		rv.SetMapIndex(keyVal, reflect.Zero(keyVal.Type()))
		return result, nil
	case reflect.Slice:
		idx, err := popIndex(key, rv.Len())
		if err != nil {
			return nil, err
		}
		result := rv.Index(idx).Interface()
		// Shift the tail left in the shared backing array and zero the last
		// slot (the caller's slice header cannot shrink in Go).
		reflect.Copy(rv.Slice(idx, rv.Len()-1), rv.Slice(idx+1, rv.Len()))
		rv.Index(rv.Len() - 1).Set(reflect.Zero(rv.Type().Elem()))
		return result, nil
	default:
		return nil, fmt.Errorf("$F.pop: %T object has no attribute 'pop'", container)
	}
}

// popIndex coerces the pop(a, b) index with Python list.pop semantics:
// negative indices count from the end, out-of-range is an IndexError, and
// non-integers are a TypeError. Integral floats/json.Numbers are accepted
// because Go's JSON decoding yields float64 where Python's json yields int.
func popIndex(key any, length int) (int, error) {
	var idx int
	switch t := key.(type) {
	case int:
		idx = t
	case int64:
		idx = int(t)
	case json.Number:
		i, err := t.Int64()
		if err != nil {
			f, ferr := t.Float64()
			if ferr != nil || f != float64(int64(f)) {
				return 0, fmt.Errorf("$F.pop: %q object cannot be interpreted as an integer", key)
			}
			idx = int(f)
		} else {
			idx = int(i)
		}
	default:
		rv := reflect.ValueOf(key)
		switch rv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			idx = int(rv.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			idx = int(rv.Uint())
		case reflect.Float32, reflect.Float64:
			f := rv.Float()
			if f != float64(int64(f)) {
				return 0, fmt.Errorf("$F.pop: 'float' object cannot be interpreted as an integer")
			}
			idx = int(f)
		default:
			return 0, fmt.Errorf("$F.pop: %T object cannot be interpreted as an integer", key)
		}
	}
	if idx < 0 {
		idx += length
	}
	if idx < 0 || idx >= length {
		return 0, fmt.Errorf("$F.pop: IndexError: pop index out of range")
	}
	return idx, nil
}
