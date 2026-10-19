package plaitaexpr

import (
	"strings"
)

// builtinFuncs mirrors plaita's default expression registry
// (expression.py _build_default_registry). Names NOT in the Python registry
// are Go extensions and marked as such below (issue #21: like/between/
// notBetween have no plaita counterpart; they stay registered for backward
// compatibility with existing Go flows and are documented extensions).
var builtinFuncs = map[string]ExpressionFunc{
	// math functions — Python _register_math
	"add":   mustToExpressionFunc(add),
	"sub":   mustToExpressionFunc(sub),
	"mul":   mustToExpressionFunc(mul),
	"div":   mustToExpressionFunc(div),
	"mod":   mustToExpressionFunc(mod),
	"pow":   mustToExpressionFunc(pow),
	"abs":   mustToExpressionFunc(abs),
	"ceil":  mustToExpressionFunc(ceil),
	"floor": mustToExpressionFunc(floor),
	"round": mustToExpressionFunc(round),
	"trunc": mustToExpressionFunc(trunc),
	"sqrt":  mustToExpressionFunc(sqrt),
	// string functions — Python _register_string
	"lower":      mustToExpressionFunc(lower),
	"upper":      mustToExpressionFunc(upper),
	"capitalize": mustToExpressionFunc(capitalize),
	"title":      mustToExpressionFunc(title),
	"strip":      mustToExpressionFunc(strip),
	"lstrip":     mustToExpressionFunc(lstrip),
	"rstrip":     mustToExpressionFunc(rstrip),
	"replace":    mustToExpressionFunc(replace),
	"split":      mustToExpressionFunc(split),
	"join":       mustToExpressionFunc(join),
	"startswith": mustToExpressionFunc(startswith),
	"endswith":   mustToExpressionFunc(endswith),
	"concat":     mustToExpressionFunc(concat),
	"isDigit":    mustToExpressionFunc(isDigit),
	// array functions — Python _register_array (pop/delListItem/setListItem
	// are the side-effect mutators; the rest return new lists)
	"len":            mustToExpressionFunc(length),
	"length":         mustToExpressionFunc(length),
	"index":          mustToExpressionFunc(index),
	"slice":          mustToExpressionFunc(slice),
	"append":         mustToExpressionFunc(appendItem),
	"extend":         mustToExpressionFunc(extend),
	"insert":         mustToExpressionFunc(insertItem),
	"remove":         mustToExpressionFunc(remove),
	"reverse":        mustToExpressionFunc(reverse),
	"sort":           mustToExpressionFunc(sortSlice),
	"getListItem":    mustToExpressionFunc(getListItem),
	"setListItem":    mustToExpressionFunc(setListItem),
	"addListItem":    mustToExpressionFunc(appendItem),
	"insertListItem": mustToExpressionFunc(insertItem),
	"delListItem":    mustToExpressionFunc(pop), // Python _fn_delListItem == a.pop(b)
	// map functions — Python _register_dict
	"keys":          mustToExpressionFunc(keys),
	"values":        mustToExpressionFunc(values),
	"items":         mustToExpressionFunc(items),
	"get":           mustToExpressionFunc(get),
	"set":           mustToExpressionFunc(set),
	"delete":        mustToExpressionFunc(deleteItem),
	"clear":         mustToExpressionFunc(clearMap),
	"getDictValue":  mustToExpressionFunc(get),
	"delDictValue":  mustToExpressionFunc(deleteItem),
	"setDictValue":  mustToExpressionFunc(set),
	"getDictKeys":   mustToExpressionFunc(keys),
	"getDictValues": mustToExpressionFunc(values),
	"clearDict":     mustToExpressionFunc(clearMap),
	// date functions — Python _register_datetime
	"today": mustToExpressionFunc(today),
	"now":   mustToExpressionFunc(now),
	// json functions — Python _register_json
	"json_loads": mustToExpressionFunc(jsonLoads),
	"json_dumps": mustToExpressionFunc(jsonDumps),
	// logic functions — Python _register_logic
	"and": mustToExpressionFunc(and),
	"or":  mustToExpressionFunc(or),
	"not": mustToExpressionFunc(not),
	// plaita comparison / flow / container functions
	"eq":          mustToExpressionFunc(eq),
	"ne":          mustToExpressionFunc(ne),
	"gt":          mustToExpressionFunc(gt),
	"gte":         mustToExpressionFunc(gte),
	"lt":          mustToExpressionFunc(lt),
	"lte":         mustToExpressionFunc(lte),
	"ifelse":      mustToExpressionFunc(ifelse),
	"notContains": mustToExpressionFunc(notContains),
	"pop":         mustToExpressionFunc(pop),
	// Go extensions (not in plaita's default registry; kept for backward
	// compatibility with existing Go flows — issue #21 documents them).
	"between":    mustToExpressionFunc(between),
	"notBetween": mustToExpressionFunc(notBetween),
	"in":         mustToExpressionFunc(in),
	"notIn":      mustToExpressionFunc(notIn),
	"contains":   mustToExpressionFunc(contains),
	"like":       mustToExpressionFunc(contains),
}

func init() {
	for n, f := range builtinFuncs {
		DefaultRegistry.MustRegister(n, f)
	}
}

func substring(s string, start, length int) string {
	end := start + length

	if start < 0 || end > len(s) {
		return ""
	}

	return s[start:end]
}

func between(v, low, high float64) bool {
	return v >= low && v <= high
}

func notBetween(v, low, high float64) bool {
	return !between(v, low, high)
}

func in(elem any, slice []any) bool {
	for _, v := range slice {
		if v == elem {
			return true
		}
	}
	return false
}

func notIn(elem any, slice []any) bool {
	return !in(elem, slice)
}

func contains(s, pattern string) bool {
	return strings.Contains(s, pattern)
}

// insertItem adapts the variadic-free insert to the registry (the index
// arrives as a generic value and is coerced with Python integer semantics).
func insertItem(s []any, i any, item any) (any, error) {
	idx, err := pyIndexArg("insert", i)
	if err != nil {
		return nil, err
	}
	return insert(s, idx, item), nil
}
