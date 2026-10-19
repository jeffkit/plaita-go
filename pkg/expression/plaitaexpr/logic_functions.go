package plaitaexpr

// and/or/not follow plaita's Python-truthiness semantics: any value usable as
// a condition (0/""/[]/{}/nil are falsy), and `or` returns the first truthy
// argument's VALUE — Python `or` semantics, not a bool fold.

func and(a, b any) any {
	if !isTruthy(a) {
		return a
	}
	return b
}

func or(args ...any) any {
	for _, arg := range args {
		if isTruthy(arg) {
			return arg
		}
	}
	// Python _fn_or: next((arg for arg in args if arg), False) — when every
	// argument is falsy the result is the BOOL False, not the last argument
	// (issue #26: or(0, '') used to yield '' instead of False).
	return false
}

func not(a any) bool {
	return !isTruthy(a)
}
