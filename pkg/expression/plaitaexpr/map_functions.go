package plaitaexpr

// map_functions.go ports plaita's dict $F functions (expression.py
// _fn_keys/_fn_values/_fn_items/_fn_get and the has_side_effects mutators
// _fn_set/_fn_delete/_fn_clear and their setDictValue/delDictValue/clearDict
// aliases):
//
//   - keys/values/items keep the existing deterministic key order (see the
//     divergence note below);
//   - delete/delDictValue are Python `a.pop(b)`: a missing key is a KeyError
//     (the Go variant used to silently ignore it, masking real flow bugs);
//   - every mutator (set/delete/clear + DictValue aliases) operates on a COPY
//     and returns None: plaita#69's write protection refuses live context
//     roots before the call, and for the fresh targets that remain legal the
//     copy guarantees the caller's container is never written through
//     (issue #9).
//
// Divergence vs Python (documented, issue #21): Python dicts preserve
// insertion order and keys()/values()/items() follow it; the Go engine's
// maps are unordered map[string]any (order is lost at JSON decode). Sorting
// the keys keeps the output deterministic. This is an intentional,
// documented divergence — NOT Python order semantics.

import (
	"fmt"
	"sort"
)

// keys is plaita $F.keys/$F.getDictKeys — Python dict.keys() materialised to
// a list (sorted keys; see the divergence note above).
func keys(m map[string]any) []any {
	orderedKeys := orderKeys(m)
	result := make([]any, 0, len(m))
	for _, k := range orderedKeys {
		result = append(result, k)
	}
	return result
}

func orderKeys(m map[string]any) []string {
	randomKeys := make([]string, 0, len(m))
	for k := range m {
		randomKeys = append(randomKeys, k)
	}
	sort.SliceStable(randomKeys, func(i, j int) bool {
		return randomKeys[i] < randomKeys[j]
	})
	return randomKeys
}

// values is plaita $F.values/$F.getDictValues (order note as on keys).
func values(m map[string]any) []any {
	orderedKeys := orderKeys(m)
	result := make([]any, 0, len(m))
	for _, k := range orderedKeys {
		result = append(result, m[k])
	}
	return result
}

// items is plaita $F.items — Python dict.items() as [[k, v], ...] (order
// note as on keys).
func items(m map[string]any) [][]any {
	orderedKeys := orderKeys(m)
	result := make([][]any, 0, len(m))
	for _, k := range orderedKeys {
		result = append(result, []any{k, m[k]})
	}
	return result
}

// get is plaita $F.get/$F.getDictValue — Python dict.get(key, default).
func get(m map[string]any, key string, defaultValue ...any) any {
	v, ok := m[key]
	if ok {
		return v
	}

	if len(defaultValue) > 0 {
		return defaultValue[0]
	}
	return nil
}

// copyPyMap deep-copies a map[string]any one level of structure at a time
// (values that are themselves maps/lists are copied recursively so nested
// containers cannot stay aliased to the caller's context).
func copyPyMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = copyPyValue(v)
	}
	return out
}

func copyPyList(s []any) []any {
	out := make([]any, len(s))
	for i, v := range s {
		out[i] = copyPyValue(v)
	}
	return out
}

func copyPyValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		return copyPyMap(t)
	case []any:
		return copyPyList(t)
	default:
		return v
	}
}

// set is plaita $F.set/$F.setDictValue — Python `a.update({b: c})` (returns
// None). plaita#69: the target must be fresh (the evaluator refuses live
// roots with a named error); here the update happens on a deep copy so no
// shared container is mutated (issue #9).
func set(m map[string]any, key string, value any) (any, error) {
	if m == nil {
		return nil, fmt.Errorf("$F.set: 'NoneType' object has no attribute 'update'")
	}
	out := copyPyMap(m)
	out[key] = value
	return nil, nil
}

// deleteItem is plaita $F.delete/$F.delDictValue — Python `a.pop(b)`: the
// removal happens on a deep copy (issue #9) and a missing key is a KeyError.
func deleteItem(m map[string]any, key string) (any, error) {
	if m == nil {
		return nil, fmt.Errorf("$F.delete: KeyError: %q", key)
	}
	if _, ok := m[key]; !ok {
		return nil, fmt.Errorf("$F.delete: KeyError: %q", key)
	}
	out := copyPyMap(m)
	delete(out, key)
	return nil, nil
}

// clearMap is plaita $F.clear/$F.clearDict — Python `a.clear()` (returns
// None); happens on a deep copy so nothing shared is emptied (issue #9).
func clearMap(m map[string]any) (any, error) {
	if m == nil {
		return nil, fmt.Errorf("$F.clear: 'NoneType' object has no attribute 'clear'")
	}
	_ = copyPyMap(m) // the cleared copy is discarded; nothing shared mutates
	return nil, nil
}
