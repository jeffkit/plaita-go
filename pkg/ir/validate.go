package ir

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// validateDoc ports the subset of plaita's ir_validate DEFAULT_RULES the
// loader enforces:
//
//	check_flow_entry_and_reachability — exactly one start, everything reachable
//	check_node_refs                   — next / branch targets exist
//	check_childflow_reachable_end     — embedded flows reach an end
//	check_parallel_join_branches      — joinBranches ⊆ branches[].name
//
// Expression-level checks (check_expression_functions) live with the
// expression engine, not here.
func validateDoc(doc map[string]any, sc scope) error {
	nodes, err := docNodes(doc)
	if err != nil {
		return err
	}

	ids := map[string]bool{}
	starts := 0
	for _, n := range nodes {
		id, _ := n["id"].(string)
		if id == "" {
			return fmt.Errorf("ir: node %q: missing id", n["type"])
		}
		if ids[id] {
			return fmt.Errorf("ir: flow %q: duplicate node id %q", doc["id"], id)
		}
		ids[id] = true
		if n["type"] == "start" {
			starts++
		}
	}
	if starts != 1 {
		return fmt.Errorf("ir: flow %q: expected exactly one start node, got %d", doc["id"], starts)
	}

	// collect edges: linear next + switch branch next
	edges := map[string][]string{}
	ends := map[string]bool{}
	for _, n := range nodes {
		id, _ := n["id"].(string)
		if next, ok := n["next"].(string); ok && next != "" {
			edges[id] = append(edges[id], next)
		}
		switch n["type"] {
		case "switch":
			for _, rb := range asList(n["branches"]) {
				b, _ := rb.(map[string]any)
				if next, ok := b["next"].(string); ok && next != "" {
					edges[id] = append(edges[id], next)
				}
			}
		case "end":
			ends[id] = true
		}
	}
	for id, outs := range edges {
		for _, out := range outs {
			if !ids[out] {
				return fmt.Errorf("ir: flow %q: node %q references missing node %q", doc["id"], id, out)
			}
		}
	}

	var startID string
	for _, n := range nodes {
		if n["type"] == "start" {
			startID, _ = n["id"].(string)
		}
	}
	reachable := bfs(startID, edges)
	for _, n := range nodes {
		id, _ := n["id"].(string)
		if !reachable[id] {
			return fmt.Errorf("ir: flow %q: node %q is not reachable from start", doc["id"], id)
		}
	}

	if sc == scopeChild {
		anyEndReachable := false
		for id := range ends {
			if reachable[id] {
				anyEndReachable = true
				break
			}
		}
		if !anyEndReachable {
			return fmt.Errorf("ir: embedded flow %q never reaches an end node", doc["id"])
		}
	}

	// parallel joinBranches ⊆ branches[].name
	for _, n := range nodes {
		if n["type"] != "parallel" {
			continue
		}
		names := map[string]bool{}
		for _, rb := range asList(n["branches"]) {
			if b, ok := rb.(map[string]any); ok {
				if name, ok := b["name"].(string); ok {
					names[name] = true
				}
			}
		}
		for _, j := range asList(firstAny(n, "joinBranches", "join_branches")) {
			if s, ok := j.(string); ok && !names[s] {
				return fmt.Errorf("ir: flow %q: parallel %q joins unknown branch %q", doc["id"], n["id"], s)
			}
		}
	}
	return nil
}

func bfs(start string, edges map[string][]string) map[string]bool {
	seen := map[string]bool{start: true}
	queue := []string{start}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, next := range edges[cur] {
			if !seen[next] {
				seen[next] = true
				queue = append(queue, next)
			}
		}
	}
	return seen
}

func docNodes(doc map[string]any) ([]map[string]any, error) {
	list, ok := doc["nodes"].([]any)
	if !ok {
		return nil, fmt.Errorf("ir: flow %q: nodes missing", doc["id"])
	}
	out := make([]map[string]any, 0, len(list))
	for _, rn := range list {
		n, ok := rn.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("ir: flow %q: node entry is not an object", doc["id"])
		}
		out = append(out, n)
	}
	return out, nil
}

// --- small helpers over generic maps ---

func asMeta(doc map[string]any) map[string]string {
	switch m := doc["metadata"].(type) {
	case map[string]string:
		return m
	case map[string]any:
		out := make(map[string]string, len(m))
		for k, v := range m {
			out[k] = fmt.Sprint(v)
		}
		return out
	case nil:
		return map[string]string{}
	default:
		return map[string]string{}
	}
}

func stash(meta map[string]string, key string, v any) {
	if _, exists := meta[key]; !exists {
		meta[key] = fmt.Sprint(v)
	}
}

func foldKey(m map[string]any, from, to string) {
	if v, ok := m[from]; ok {
		if _, exists := m[to]; !exists {
			m[to] = v
		}
		delete(m, from)
	}
}

func firstString(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := m[k].(string); ok {
			return s
		}
	}
	return ""
}

func firstAny(m map[string]any, keys ...string) any {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			return v
		}
	}
	return nil
}

func nextIsEmpty(n map[string]any) bool {
	v, ok := n["next"]
	return !ok || v == nil || v == ""
}

func asList(v any) []any {
	if l, ok := v.([]any); ok {
		return l
	}
	return nil
}

func asFloat(v any) (float64, error) {
	switch t := v.(type) {
	case json.Number:
		return t.Float64()
	case float64:
		return t, nil
	case string:
		return strconv.ParseFloat(strings.TrimSpace(t), 64)
	default:
		return 0, fmt.Errorf("expected a number, got %T", v)
	}
}

func nodeLabel(n map[string]any) string {
	if id, _ := n["id"].(string); id != "" {
		return id
	}
	t, _ := n["type"].(string)
	return t
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
