package plaitaexpr

import (
	"sort"

	"github.com/samber/lo"
)

func keys(m map[string]any) []any {
	randomKeys := orderKeys(m)
	return lo.Map(randomKeys, func(k string, _ int) any {
		return k
	})
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

func values(m map[string]any) []any {
	orderedKeys := orderKeys(m)
	result := make([]any, 0, len(m))
	for _, k := range orderedKeys {
		result = append(result, m[k])
	}
	return result
}

func items(m map[string]any) [][]any {
	orderedKeys := orderKeys(m)
	result := make([][]any, 0, len(m))
	for _, k := range orderedKeys {
		result = append(result, []any{k, m[k]})
	}
	return result
}

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

func set(m map[string]any, key string, value any) {
	if m == nil {
		return
	}
	m[key] = value
}

func deleteItem(m map[string]any, key string) {
	if len(m) == 0 {
		return
	}
	delete(m, key)
}

func clearMap(m map[string]any) {
	clear(m)
}
