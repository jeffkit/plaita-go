package plaitaexpr

func and(a, b bool) bool {
	return a && b
}

func or(args ...bool) bool {
	for _, arg := range args {
		if arg {
			return true
		}
	}
	return false
}

func not(a bool) bool {
	return !a
}
