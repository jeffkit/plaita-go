package plaitaexpr

import (
	"strings"
)

var builtinFuncs = map[string]ExpressionFunc{
	// math functions
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
	// string functions
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
	// slice functions
	"len":            mustToExpressionFunc(length),
	"length":         mustToExpressionFunc(length),
	"index":          mustToExpressionFunc(index),
	"slice":          mustToExpressionFunc(slice),
	"append":         mustToExpressionFunc(appendItem),
	"extend":         mustToExpressionFunc(extend),
	"insert":         mustToExpressionFunc(insert),
	"remove":         mustToExpressionFunc(remove),
	"reverse":        mustToExpressionFunc(reverse),
	"sort":           mustToExpressionFunc(sortSlice),
	"getListItem":    mustToExpressionFunc(getListItem),
	"setListItem":    mustToExpressionFunc(setListItem),
	"addListItem":    mustToExpressionFunc(appendItem),
	"insertListItem": mustToExpressionFunc(insert),
	// map functions
	"keys":         mustToExpressionFunc(keys),
	"values":       mustToExpressionFunc(values),
	"items":        mustToExpressionFunc(items),
	"get":          mustToExpressionFunc(get),
	"set":          mustToExpressionFunc(set),
	"delete":       mustToExpressionFunc(deleteItem),
	"clear":        mustToExpressionFunc(clearMap),
	"getDictValue": mustToExpressionFunc(get),
	"delDictValue": mustToExpressionFunc(deleteItem),
	"setDictValue": mustToExpressionFunc(set),
	"getDictKeys":  mustToExpressionFunc(keys),
	"clearDict":    mustToExpressionFunc(clearMap),
	// date functions
	"today": mustToExpressionFunc(today),
	"now":   mustToExpressionFunc(now),
	// json functions
	"json_loads": mustToExpressionFunc(jsonLoads),
	"json_dumps": mustToExpressionFunc(jsonDumps),
	// logic functions
	"and": mustToExpressionFunc(and),
	"or":  mustToExpressionFunc(or),
	"not": mustToExpressionFunc(not),
	// other function
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
