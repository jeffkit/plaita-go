package plaitaexpr

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/maja42/goval"
)

// This file implements plaita engine-level expression semantics on top of
// goval (semantic authority: plaita/core/expression_parser.py module
// docstring + plaita/tests/unit/test_expression_golden.py):
//
//  1. unknown functions fall back to the "undefined" sentinel instead of
//     failing (plaita _UNDEFINED: `lambda *args, **kwargs: "undefined"`);
//  2. `$NODE.raw.<node>[.path...]` is an explicit verbatim accessor that
//     bypasses any re-evaluation (plaita#130);
//  3. missing-key semantics: a missing root key is an error (plaita
//     KeyError), while a missing field on an existing object yields nil and
//     further field access through nil keeps yielding nil (plaita
//     `_get_attr(None, ...) -> None`); index misses are errors (plaita
//     IndexError/TypeError/KeyError).

// undefinedSentinelValue is the exact sentinel plaita returns for unknown
// function calls: the string "undefined" (plaita _UNDEFINED lambda).
const undefinedSentinelValue = "undefined"

// undefinedSentinelFunc is injected for every unregistered function name
// referenced by a statement. goval resolves `functions[name]` at call time,
// *after* evaluating the arguments, so argument evaluation stays eager and a
// failing argument (e.g. a missing root) still errors before the sentinel is
// produced — exactly plaita's order (args are evaluated before the registry
// lookup).
func undefinedSentinelFunc(_ ...interface{}) (interface{}, error) {
	return undefinedSentinelValue, nil
}

// funcHeadRe matches a function-call head `name(` inside a compiled
// statement (the goval statement form has no "$" left in it).
var funcHeadRe = regexp.MustCompile(`([A-Za-z_][A-Za-z0-9_]*)\s*\(`)

// quotedSpanRe matches single- or double-quoted string literals (with
// backslash escapes) so call-like text inside literals cannot inject or
// shadow functions during the unknown-function scan.
var quotedSpanRe = regexp.MustCompile(`"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'`)

// resolveFunctions returns the function map to evaluate a statement with:
// the registered functions, plus an "undefined" sentinel for every function
// name the statement calls that is not registered. When nothing is unknown
// the registered map is returned as-is (no copy on the hot path).
func resolveFunctions(statement string, registered map[string]goval.ExpressionFunction) map[string]goval.ExpressionFunction {
	unknown := scanUnknownFunctions(statement, registered)
	if len(unknown) == 0 {
		return registered
	}
	functions := make(map[string]goval.ExpressionFunction, len(registered)+len(unknown))
	for name, fn := range registered {
		functions[name] = fn
	}
	for _, name := range unknown {
		functions[name] = undefinedSentinelFunc
	}
	return functions
}

// scanUnknownFunctions returns the deduplicated names of functions called by
// a compiled statement that are missing from the registered map. Quoted
// string literals are stripped first so `"add("` inside a literal neither
// registers a bogus sentinel nor could shadow a real function.
func scanUnknownFunctions(statement string, registered map[string]goval.ExpressionFunction) []string {
	stripped := quotedSpanRe.ReplaceAllString(statement, "")
	seen := make(map[string]struct{})
	var unknown []string
	for _, match := range funcHeadRe.FindAllStringSubmatch(stripped, -1) {
		name := match[1]
		if _, ok := registered[name]; ok {
			continue
		}
		if _, dup := seen[name]; dup {
			continue
		}
		seen[name] = struct{}{}
		unknown = append(unknown, name)
	}
	return unknown
}

// rawPathModifier is the explicit verbatim accessor segment of plaita#130.
const rawPathModifier = "raw"

// indexSegmentRe reports whether a dotted path segment denotes an index
// (plaita treats `.n` / `[n]` as index segments, everything else as a field).
var indexSegmentRe = regexp.MustCompile(`^[0-9]+$`)

// nodeRawAccessorPath implements plaita#130's `$NODE.raw.<node>[.path...]`
// verbatim accessor: it returns the path with the `raw.` modifier stripped.
// The modifier only applies when
//   - the path has at least two segments, the first being `raw`,
//   - the second segment is a field (not a numeric index), and
//   - the results container has no real node/key named `raw`
//     (backwards-compatibility collision rule: a node literally named `raw`
//     keeps being resolved as that node).
//
// The node results container is the verbatim data store of the Go runtime,
// so reading it with the modifier stripped returns the stored value as-is —
// strings are never re-evaluated at any depth.
func nodeRawAccessorPath(results map[string]any, path string) (string, bool) {
	segments := strings.Split(path, ".")
	if len(segments) < 2 || segments[0] != rawPathModifier || segments[1] == "" {
		return "", false
	}
	if indexSegmentRe.MatchString(segments[1]) {
		return "", false
	}
	if _, exists := results[rawPathModifier]; exists {
		return "", false
	}
	return strings.Join(segments[1:], "."), true
}

// lookupPathPlaita resolves a dotted path against root with plaita's
// missing-key semantics:
//
//   - a nil root container is an absent root: error (plaita KeyError on
//     `context[root_key]`);
//   - with rootMissIsError, a miss on the first segment is a root miss:
//     error (used when the first segment addresses the expression context,
//     e.g. `$myvar.a`);
//   - a missing field on an existing object yields nil, and further *field*
//     access through nil keeps yielding nil (plaita `_get_attr` returns None
//     for a missing mapping key and for attribute access on None);
//   - an *index* segment (numeric) that misses on a map, is out of bounds,
//     or indexes nil is an error (plaita dict[int] KeyError /
//     list IndexError / None[0] TypeError).
func lookupPathPlaita(root any, path string, rootMissIsError bool) (any, error) {
	if path == "" {
		return root, nil
	}
	if isNilValue(root) {
		return nil, fmt.Errorf("root of path %q not found", path)
	}
	segments := strings.Split(path, ".")
	var cur any = root
	for i, seg := range segments {
		isIndex := indexSegmentRe.MatchString(seg)
		if cur == nil {
			// cur became nil through a field miss: plaita keeps returning
			// None for field access on None but errors on indexing None.
			if isIndex {
				return nil, fmt.Errorf("path %q: cannot index nil at segment %q", path, seg)
			}
			continue
		}
		next, miss := traverseSegment(cur, seg, isIndex)
		if !miss {
			cur = next
			continue
		}
		if (rootMissIsError && i == 0) || isIndex {
			return nil, fmt.Errorf("path %q: key %q not found", path, seg)
		}
		cur = nil
	}
	return cur, nil
}

// traverseSegment reads one path segment off a non-nil value. It reports
// (next, false) on success and (nil, true) on a miss; the caller decides
// between error and nil continuation according to the segment kind and
// position.
func traverseSegment(cur any, seg string, isIndex bool) (any, bool) {
	rv := derefReflectValue(reflect.ValueOf(cur))
	if !rv.IsValid() {
		return nil, true
	}
	switch rv.Kind() {
	case reflect.Map:
		key := mapKeyReflectValue(rv, seg, isIndex)
		if !key.IsValid() {
			return nil, true
		}
		value := rv.MapIndex(key)
		if !value.IsValid() {
			return nil, true
		}
		return value.Interface(), false
	case reflect.Slice, reflect.Array:
		idx, err := strconv.Atoi(seg)
		if err != nil {
			// A field read on a list yields nil (plaita: a list is neither
			// a mapping nor has attributes).
			return nil, true
		}
		if idx < 0 || idx >= rv.Len() {
			return nil, true
		}
		return rv.Index(idx).Interface(), false
	case reflect.Struct:
		field := rv.FieldByName(seg)
		if !field.IsValid() || !field.CanInterface() {
			return nil, true
		}
		return field.Interface(), false
	default:
		// Scalars (string/number/bool): plaita's _get_attr yields None for a
		// field read and an error for an index read (mapped by the caller).
		return nil, true
	}
}

// mapKeyReflectValue builds the reflect key to read a map with. String-keyed
// maps accept every segment; integer-keyed maps accept numeric segments;
// anything else is a miss.
func mapKeyReflectValue(mapValue reflect.Value, seg string, isIndex bool) reflect.Value {
	keyType := mapValue.Type().Key()
	switch keyType.Kind() {
	case reflect.String:
		return reflect.ValueOf(seg).Convert(keyType)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if !isIndex {
			return reflect.Value{}
		}
		idx, err := strconv.ParseInt(seg, 10, 64)
		if err != nil {
			return reflect.Value{}
		}
		return reflect.ValueOf(idx).Convert(keyType)
	default:
		return reflect.Value{}
	}
}

// derefReflectValue peels pointers/interfaces off a value; a nil pointer or
// interface yields the zero Value (treated as nil by the caller).
func derefReflectValue(v reflect.Value) reflect.Value {
	for v.IsValid() && (v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface) {
		if v.IsNil() {
			return reflect.Value{}
		}
		v = v.Elem()
	}
	return v
}

// isNilValue reports whether a generic value is an absent (untyped nil or
// typed-nil) value.
func isNilValue(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr, reflect.Interface, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
		return rv.IsNil()
	default:
		return false
	}
}
