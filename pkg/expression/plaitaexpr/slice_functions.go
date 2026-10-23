package plaitaexpr

// slice_functions.go ports plaita's array $F functions with Python sequence
// semantics (expression.py _fn_index/_fn_slice/_fn_getListItem/_fn_insert/
// _fn_remove and the builtin len/round registrations):
//
//   - len works on str (rune count), list and dict; anything else TypeError;
//   - index works on str (substring) and list (Python == membership) and
//     raises (ValueError) on a miss instead of returning a sentinel;
//   - slice is `a[b:c]`: negative indices count from the end, out-of-range
//     clamps, works on str and list, always returns a NEW value;
//   - getListItem is `a[b]`: negative indices, IndexError out of range;
//   - append/extend/insert/remove/addListItem/insertListItem are PURE: they
//     return new lists and never share (or write through) the backing array
//     of their input — Python's "returns new list" contract, which also
//     protects live context roots from in-place corruption (issue #9);
//   - remove raises ValueError when the item is absent (list.remove).

import (
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"
)

// length is plaita $F.len/$F.length — Python builtin len: str (runes), list
// and dict.
func length(v any) (int, error) {
	switch t := v.(type) {
	case string:
		return utf8.RuneCountInString(t), nil
	case []any:
		return len(t), nil
	case map[string]any:
		return len(t), nil
	}
	return 0, fmt.Errorf("$F.len: object of type '%s' has no len()", pyTypeName(v))
}

// index is plaita $F.index(a, b) — Python `a.index(b)`: substring search on
// str (issue #40: the result counts CHARACTERS like str.index — the raw
// strings.Index byte offset drifted under multi-byte text), == membership
// search on list; a miss is a ValueError (plaita propagates the native
// exception; the Go sentinel -1 masked misses).
func index(container any, item any) (int, error) {
	switch c := container.(type) {
	case string:
		s, ok := item.(string)
		if !ok {
			return 0, fmt.Errorf("$F.index: str.index argument must be str, not %s", pyTypeName(item))
		}
		i := strings.Index(c, s)
		if i < 0 {
			return 0, fmt.Errorf("$F.index: substring not found")
		}
		return utf8.RuneCountInString(c[:i]), nil
	case []any:
		for i, v := range c {
			if Equal(v, item) {
				return i, nil
			}
		}
		return 0, fmt.Errorf("$F.index: %s is not in list", PyRepr(item))
	}
	return 0, fmt.Errorf("$F.index: '%s' object has no attribute 'index'", pyTypeName(container))
}

// pySliceClamp resolves a Python slice index against a sequence length:
// negative counts from the end, out-of-range clamps into [0, len].
func pySliceClamp(i int64, l int) int {
	if i < 0 {
		i += int64(l)
	}
	if i < 0 {
		return 0
	}
	if i > int64(l) {
		return l
	}
	return int(i)
}

// slice is plaita $F.slice(a, b, c) — Python `a[b:c]`: clamped, negative
// indices supported, works on list (result is a new list) and str.
func slice(v any, start, end any) (any, error) {
	lo, err := pyIndexArg("slice", start)
	if err != nil {
		return nil, err
	}
	hi, err := pyIndexArg("slice", end)
	if err != nil {
		return nil, err
	}
	switch t := v.(type) {
	case []any:
		l := len(t)
		a, b := pySliceClamp(lo, l), pySliceClamp(hi, l)
		if b < a {
			a, b = 0, 0 // Python `t[a:b]` with a > b yields []
		}
		out := make([]any, b-a)
		copy(out, t[a:b])
		return out, nil
	case string:
		// Python slices strings by code points.
		runes := []rune(t)
		l := len(runes)
		a, b := pySliceClamp(lo, l), pySliceClamp(hi, l)
		if b < a {
			a, b = 0, 0
		}
		return string(runes[a:b]), nil
	}
	return nil, fmt.Errorf("$F.slice: '%s' object is not subscriptable", pyTypeName(v))
}

// appendItem is plaita $F.append/$F.addListItem — Python `a + [b]`: returns a
// NEW list; the input list (and whatever backing array it shares) is never
// modified (issue #9: the previous append(slice, item) could write through a
// shared backing array). The result-size gate runs before the allocation
// (#64, _fn_append shape: projection len+1, elements unit).
func appendItem(s []any, item any) ([]any, error) {
	if err := checkResultSize(fmt.Sprintf("$F.append(%d + 1)", len(s)), int64(len(s))+1, int64(len(s))+1, "elements"); err != nil {
		return nil, err
	}
	out := make([]any, 0, len(s)+1)
	out = append(out, s...)
	return append(out, item), nil
}

// extend is plaita $F.extend(a, b) — Python `a + b` (same domain as add).
// Same-kind concatenation is gated under extend's own name before delegating
// (#64): Python's _fn_extend wording carries `$F.extend(...)`, not `$F.add`.
func extend(a, b any) (any, error) {
	if as, ok := a.(string); ok {
		if bs, ok2 := b.(string); ok2 {
			if err := gatePairConcat("$F.extend", int64(utf8.RuneCountInString(as)), int64(utf8.RuneCountInString(bs)), "chars"); err != nil {
				return nil, err
			}
		}
	}
	if al, ok := a.([]any); ok {
		if bl, ok2 := b.([]any); ok2 {
			if err := gatePairConcat("$F.extend", int64(len(al)), int64(len(bl)), "elements"); err != nil {
				return nil, err
			}
		}
	}
	return add(a, b)
}

// insert is plaita $F.insert/$F.insertListItem — Python `a[:b] + [c] + a[b:]`:
// negative indices count from the end, out-of-range clamps (inserting at the
// boundary), and the result is a NEW list.
func insert(s []any, i int64, item any) []any {
	at := pySliceClamp(i, len(s))
	out := make([]any, 0, len(s)+1)
	out = append(out, s[:at]...)
	out = append(out, item)
	out = append(out, s[at:]...)
	return out
}

// remove is plaita $F.remove(a, b) — Python
// `a[:a.index(b)] + a[a.index(b) + 1:]`: removes the FIRST == match and
// returns a NEW list; an absent item is a ValueError.
func remove(s []any, item any) ([]any, error) {
	for i, v := range s {
		if Equal(v, item) {
			out := make([]any, 0, len(s)-1)
			out = append(out, s[:i]...)
			out = append(out, s[i+1:]...)
			return out, nil
		}
	}
	return nil, fmt.Errorf("$F.remove: list.remove(%s): %s not in list", PyRepr(item), PyRepr(item))
}

// reverse is plaita $F.reverse(a) — Python `a[::-1]`: returns a NEW list.
func reverse(s []any) []any {
	out := make([]any, len(s))
	copy(out, s)
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

// getListItem is plaita $F.getListItem(a, b) — Python `a[b]`: list/string/
// dict indexing with negative indices; out-of-range is an IndexError (the
// Go nil sentinel masked real errors and made missing items look like None).
// Dict STR keys look up before the integer coercion (issue #42: pyIndexArg
// rejected str first, so the KeyError branch was unreachable dead code).
func getListItem(v any, i any) (any, error) {
	if t, ok := v.(map[string]any); ok {
		// Python `a[b]` on a dict: key lookup, KeyError on miss.
		if k, ok := i.(string); ok {
			val, found := t[k]
			if !found {
				return nil, fmt.Errorf("$F.getListItem: KeyError: %q", k)
			}
			return val, nil
		}
	}
	idx, err := pyIndexArg("getListItem", i)
	if err != nil {
		return nil, err
	}
	switch t := v.(type) {
	case []any:
		n := int64(len(t))
		j := idx
		if j < 0 {
			j += n
		}
		if j < 0 || j >= n {
			return nil, fmt.Errorf("$F.getListItem: IndexError: list index out of range")
		}
		return t[j], nil
	case string:
		runes := []rune(t)
		n := int64(len(runes))
		j := idx
		if j < 0 {
			j += n
		}
		if j < 0 || j >= n {
			return nil, fmt.Errorf("$F.getListItem: IndexError: string index out of range")
		}
		return string(runes[j]), nil
	case map[string]any:
		// Non-str key on a dict: a JSON dict never has such keys, so it is
		// always a miss (Python dict[5] → KeyError: 5).
		return nil, fmt.Errorf("$F.getListItem: KeyError: %v", i)
	}
	return nil, fmt.Errorf("$F.getListItem: '%s' object is not subscriptable", pyTypeName(v))
}

// setListItem is plaita $F.setListItem(a, b, c) — Python `a.__setitem__(b, c)`
// (a side-effect mutator). plaita#69 write protection refuses live context
// roots before the call; for the fresh targets that remain legal, the
// mutation happens on a COPY so nothing shared is ever written through
// (issue #9: the in-place variant corrupted the caller's slice). Python
// __setitem__ returns None and raises IndexError out of range; negative
// indices count from the end.
func setListItem(s []any, i int64, item any) (any, error) {
	n := int64(len(s))
	j := i
	if j < 0 {
		j += n
	}
	if j < 0 || j >= n {
		return nil, fmt.Errorf("$F.setListItem: IndexError: list assignment index out of range")
	}
	out := make([]any, n)
	copy(out, s)
	out[j] = item
	return nil, nil
}

// sortSlice is plaita $F.sort(a[, key][, desc]) — Python
// `sorted(a, key=lambda x: x.get(sort_key) if isinstance(x, dict) else x,
// reverse=desc)`: returns a NEW sorted list (the input is copied first, so a
// live context list is never reordered in place). Python raises TypeError on
// the first incomparable pair (issue #44 — sorted() never silently reorders
// mixed types), so ordering goes through the shared Compare primitive and a
// comparison failure surfaces as a named error instead of a bogus result.
func sortSlice(slice []any, args ...any) (any, error) {
	var (
		desc    bool
		sortKey string
	)
	if len(args) > 0 {
		udfSortKey, ok := args[0].(string)
		if ok {
			sortKey = udfSortKey
		}
	}
	if len(args) > 1 {
		udfDesc, ok := args[1].(bool)
		if ok {
			desc = udfDesc
		}
	}
	newSlice := make([]any, len(slice))
	copy(newSlice, slice)

	var cmpErr error
	sort.SliceStable(newSlice, func(i, j int) bool {
		if cmpErr != nil {
			return false
		}
		a, b := sortKeyOf(newSlice[i], sortKey), sortKeyOf(newSlice[j], sortKey)
		c, err := Compare("$F.sort", a, b)
		if err != nil {
			// Python's exact message shape: '<' is the only operator sorted()
			// uses (reverse=True still compares with <).
			cmpErr = fmt.Errorf("$F.sort: '<' not supported between instances of '%s' and '%s'",
				pyTypeName(a), pyTypeName(b))
			return false
		}
		if desc {
			return c > 0
		}
		return c < 0
	})
	if cmpErr != nil {
		return nil, cmpErr
	}
	return newSlice, nil
}

// sortKeyOf is Python `x.get(sort_key) if isinstance(x, dict) else x`: a dict
// element projects onto sortKey and a MISSING key falls back to None — which
// then fails comparisons exactly like Python (issue #44: the previous
// fallback-to-element silently mixed projected and unprojected values).
func sortKeyOf(v any, key string) any {
	if m, ok := v.(map[string]any); ok {
		return m[key]
	}
	return v
}
