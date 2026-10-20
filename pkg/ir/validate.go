package ir

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/jeffkit/plaita-go/pkg/expression/plaitaexpr"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// validateDoc ports plaita's ir_validate DEFAULT_RULES the loader enforces:
//
//	check_flow_entry_and_reachability — exactly one start, everything reachable
//	structural target check           — next / branch targets exist
//	check_childflow_reachable_end     — embedded flows reach an end
//	check_parallel_join_branches      — joinBranches ⊆ branches[].name
//	check_node_refs (R5)              — $NODE.<id> exists in the current (sub)flow
//	check_expression_functions (R6)   — $F.<fn>(...) is a registered function
//	check_output_type_literal (R7)    — literal output vs declared outputType
//
// Beyond the rules it mirrors plaita's warn_unknown_keys: node config keys the
// engine has no field for are reported (log + node metadata) instead of being
// silently dropped by the engine's json unmarshal.
//
// Rules run per (sub)flow graph: validateDoc is re-entered for embedded flows
// (childFlow / branches[].flow), so $NODE references resolve against the
// current graph's ids only — plaita's $NODE scope semantics.
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
		case "if":
			// if 的假分支边（translateIf 已把 camel 别名 elseNext 折叠）。
			// Python ir_validate 把 else_next/elseNext 计入可达性边集
			// （_iter_edge_targets 的超集口径）——漏掉它会把仅从假分支可达
			// 的节点误判为不可达。
			if e := firstString(n, "else_next", "elseNext"); e != "" {
				edges[id] = append(edges[id], e)
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

	// per-node rules, mirroring ir_validate's rule hooks (first error wins):
	// they apply to every node incl. end — end.output is the $NODE ref hot spot.
	flowID, _ := doc["id"].(string)
	for _, n := range nodes {
		if err := checkNodeRefs(flowID, n, ids); err != nil {
			return err
		}
		if err := checkExpressionFunctions(flowID, n); err != nil {
			return err
		}
		if err := checkOutputTypeLiteral(flowID, n); err != nil {
			return err
		}
		warnUnknownNodeFields(flowID, n)
	}
	return nil
}

// --- R5 / R6: expression reference scanning (ports _collect_refs) ---

var (
	// $NODE.<id>: id segment takes identifier characters only — dynamic ids
	// ($NODE.$INPUT.k) intentionally do not match (static analysis boundary).
	nodeRefRe = regexp.MustCompile(`\$NODE\.([A-Za-z0-9_\-]+)`)
	// $F.<fn>(
	funcRefRe = regexp.MustCompile(`\$F\.([A-Za-z0-9_]+)\s*\(`)
)

// unscannedKeys mirrors ir_validate._UNSCANNED_KEYS: keys whose whole subtree
// carries no expression references. code is node source (JS), not plaita
// expressions; childFlow/child_flow/flow are nested graphs validated against
// their own id scope; error is node-level only — an end node's failure payload
// is literal data, so "$NODE.x" inside it is text (but an `error` key nested
// anywhere deeper is scanned again).
var unscannedKeys = map[string]struct{}{
	"code": {}, "desc": {}, "description": {}, "name": {}, "title": {},
	"label": {}, "node_name": {}, "id": {}, "type": {}, "next": {},
	"else_next": {}, "elseNext": {}, "default": {}, "flow_id": {},
	"flowId": {}, "source_line": {}, "language": {}, "error": {},
	"childFlow": {}, "child_flow": {}, "flow": {},
}

// unscannedNestedKeys = unscannedKeys − {error} (ir_validate #144 split).
var unscannedNestedKeys = func() map[string]struct{} {
	out := make(map[string]struct{}, len(unscannedKeys)-1)
	for k := range unscannedKeys {
		if k != "error" {
			out[k] = struct{}{}
		}
	}
	return out
}()

// checkNodeRefs (R5): every $NODE.<id> referenced from the node must exist in
// the current (sub)flow — unknown ids evaluate to nil at runtime and the flow
// keeps running on empty values (silent corruption in plaita, late nil here).
func checkNodeRefs(flowID string, n map[string]any, ids map[string]bool) error {
	unknown := unknownRefs(collectRefs(n, nodeRefRe), func(ref string) bool { return ids[ref] })
	if len(unknown) == 0 {
		return nil
	}
	hint := didYouMean(unknown, keysOf(ids))
	return fmt.Errorf("ir: flow %q: node %q: $NODE references unknown node id(s) %v%s — "+
		"they would evaluate to nil at runtime; $NODE resolves within the current flow scope only",
		flowID, nodeLabel(n), unknown, hint)
}

// checkExpressionFunctions (R6): every $F.<fn>(...) must be registered in the
// default expression registry — this engine fails the evaluation on unknown
// functions (fail-closed, matching plaita's default registry NameError).
func checkExpressionFunctions(flowID string, n map[string]any) error {
	refs := collectRefs(n, funcRefRe)
	if len(refs) == 0 {
		return nil
	}
	known := registeredFunctionNames()
	unknown := unknownRefs(refs, func(ref string) bool { return known[ref] })
	if len(unknown) == 0 {
		return nil
	}
	hint := didYouMean(unknown, keysOf(known))
	return fmt.Errorf("ir: flow %q: node %q: $F calls unregistered function(s) %v%s — "+
		"check the spelling; unknown $F functions are rejected at evaluation time",
		flowID, nodeLabel(n), unknown, hint)
}

// registeredFunctionNames snapshots the default expression registry once.
var registeredFunctionNames = sync.OnceValue(func() map[string]bool {
	fns := plaitaexpr.AllFunctions()
	out := make(map[string]bool, len(fns))
	for name := range fns {
		out[name] = true
	}
	return out
})

// collectRefs gathers expression references from the node's strings. Node
// level skips unscannedKeys subtrees entirely; nested levels skip everything
// but the node-only `error` exemption.
func collectRefs(n map[string]any, re *regexp.Regexp) []string {
	var refs []string
	for k, v := range n {
		if _, skip := unscannedKeys[k]; skip {
			continue
		}
		refs = append(refs, scanRefStrings(v, re)...)
	}
	return refs
}

func scanRefStrings(v any, re *regexp.Regexp) []string {
	switch t := v.(type) {
	case string:
		return scanRefs(t, re)
	case []any:
		var out []string
		for _, item := range t {
			out = append(out, scanRefStrings(item, re)...)
		}
		return out
	case map[string]any:
		var out []string
		for k, item := range t {
			if _, skip := unscannedNestedKeys[k]; skip {
				continue
			}
			out = append(out, scanRefStrings(item, re)...)
		}
		return out
	}
	return nil
}

// scanRefs extracts reference names from one string. Matches preceded by an
// identifier character, dot or dollar are not root-scoped references — ports
// the Python negative lookbehind so $PARENT.$NODE.x and pre$NODE.x are left
// alone.
func scanRefs(s string, re *regexp.Regexp) []string {
	var out []string
	for _, loc := range re.FindAllStringSubmatchIndex(s, -1) {
		if loc[0] > 0 {
			p := s[loc[0]-1]
			if p == '_' || p == '.' || p == '$' ||
				(p >= 'a' && p <= 'z') || (p >= 'A' && p <= 'Z') || (p >= '0' && p <= '9') {
				continue
			}
		}
		out = append(out, s[loc[2]:loc[3]])
	}
	return out
}

func unknownRefs(refs []string, known func(string) bool) []string {
	set := map[string]bool{}
	for _, r := range refs {
		if !known(r) {
			set[r] = true
		}
	}
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for r := range set {
		out = append(out, r)
	}
	sort.Strings(out)
	return out
}

func keysOf(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// didYouMean renders a "did you mean" hint from the closest candidates
// (loose port of difflib.get_close_matches, similarity ≥ 0.6, top 3).
func didYouMean(bad []string, candidates []string) string {
	var b strings.Builder
	for _, name := range bad {
		hits := closeMatches(name, candidates)
		if len(hits) == 0 {
			continue
		}
		fmt.Fprintf(&b, " (did you mean %q for $%s?)", strings.Join(hits, `", "`), name)
		break
	}
	return b.String()
}

func closeMatches(bad string, candidates []string) []string {
	type hit struct {
		name  string
		score float64
	}
	var hits []hit
	for _, c := range candidates {
		if s := similarity(bad, c); s >= 0.6 {
			hits = append(hits, hit{c, s})
		}
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].score != hits[j].score {
			return hits[i].score > hits[j].score
		}
		return hits[i].name < hits[j].name
	})
	if len(hits) > 3 {
		hits = hits[:3]
	}
	out := make([]string, 0, len(hits))
	for _, h := range hits {
		out = append(out, h.name)
	}
	return out
}

func similarity(a, b string) float64 {
	if a == b {
		return 1
	}
	longest := len(a)
	if len(b) > longest {
		longest = len(b)
	}
	if longest == 0 {
		return 1
	}
	return 1 - float64(levenshtein(a, b))/float64(longest)
}

func levenshtein(a, b string) int {
	ra, rb := []rune(a), []rune(b)
	prev := make([]int, len(rb)+1)
	cur := make([]int, len(rb)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(ra); i++ {
		cur[0] = i
		for j := 1; j <= len(rb); j++ {
			cost := 1
			if ra[i-1] == rb[j-1] {
				cost = 0
			}
			cur[j] = min(min(cur[j-1]+1, prev[j]+1), prev[j-1]+cost)
		}
		prev, cur = cur, prev
	}
	return prev[len(rb)]
}

// --- R7: outputType literal precheck ---

// isStaticValue reports whether the IR value is final at compile time: no
// expression ($) or template ({%) markers, recursing into containers.
func isStaticValue(v any) bool {
	switch t := v.(type) {
	case nil, bool, json.Number:
		return true
	case string:
		return !strings.Contains(t, "$") && !strings.Contains(t, "{%")
	case []any:
		for _, item := range t {
			if !isStaticValue(item) {
				return false
			}
		}
		return true
	case map[string]any:
		for _, item := range t {
			if !isStaticValue(item) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// checkOutputTypeLiteral (R7): when output is a static literal and the node
// declares an outputType, run the engine's Property.ValidateValue now — the
// literal would otherwise fail (or pass unchecked) only at execution time.
// An invalid outputType itself is not reported here; the engine unmarshal
// reports it, mirroring ir_validate's skip-on-invalid-property behavior.
func checkOutputTypeLiteral(flowID string, n map[string]any) error {
	declared, ok := firstAny(n, "outputType", "output_type").(map[string]any)
	if !ok || len(declared) == 0 {
		return nil
	}
	out, has := n["output"]
	if !has || out == nil || !isStaticValue(out) {
		return nil
	}
	raw, err := json.Marshal(declared)
	if err != nil {
		return nil
	}
	var prop workflow.Property
	if err := json.Unmarshal(raw, &prop); err != nil {
		return nil
	}
	if err := prop.ValidateValue(out); err != nil {
		return fmt.Errorf("ir: flow %q: node %q: output literal %v does not match declared outputType (dataType %q): %v",
			flowID, nodeLabel(n), out, prop.DataType, err)
	}
	return nil
}

// --- unknown node key hygiene (ports warn_unknown_keys) ---

// nodeKnownKeys computes the JSON keys each registered node type accepts by
// reflecting over the registered node factory's struct (flattening embedded
// base nodes), so the check stays in sync with the node definitions. The
// loader itself adds `metadata` during normalization.
var (
	nodeKeysMu    sync.Mutex
	nodeKeysCache = map[string]map[string]struct{}{}
)

func nodeKnownKeys(nodeType string) map[string]struct{} {
	nodeKeysMu.Lock()
	defer nodeKeysMu.Unlock()
	if ks, ok := nodeKeysCache[nodeType]; ok {
		return ks
	}
	ks := map[string]struct{}{"metadata": {}}
	if factory := workflow.GetNodeFactory(workflow.NodeType(nodeType)); factory != nil {
		collectJSONKeys(reflect.TypeOf(factory(context.Background())), ks)
	}
	nodeKeysCache[nodeType] = ks
	return ks
}

func collectJSONKeys(t reflect.Type, set map[string]struct{}) {
	for t.Kind() == reflect.Ptr || t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return
	}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.Anonymous {
			collectJSONKeys(f.Type, set)
			continue
		}
		tag := f.Tag.Get("json")
		if tag == "-" {
			continue
		}
		name := strings.Split(tag, ",")[0]
		if name == "" {
			if !f.IsExported() {
				continue
			}
			name = f.Name
		}
		set[name] = struct{}{}
	}
}

// warnUnknownNodeFields surfaces node config keys the engine has no field
// for — encoding/json would drop them silently and the flow would run with
// the wrong config (plaita logs a warning via warn_unknown_keys). The keys
// are reported on the engine log and stashed into the node's metadata so
// they stay observable on the loaded workflow.
func warnUnknownNodeFields(flowID string, n map[string]any) {
	nodeType, _ := n["type"].(string)
	known := nodeKnownKeys(nodeType)
	var unknown []string
	for k := range n {
		if _, ok := known[k]; !ok {
			unknown = append(unknown, k)
		}
	}
	if len(unknown) == 0 {
		return
	}
	sort.Strings(unknown)
	log.Printf("[ir] flow %q node %q (%s): unknown config key(s) %v will be ignored by the engine (possible typo?)",
		flowID, nodeLabel(n), nodeType, unknown)
	if meta, ok := n["metadata"].(map[string]string); ok {
		meta["unknownFields"] = strings.Join(unknown, ",")
	}
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
