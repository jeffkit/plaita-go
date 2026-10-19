package ir

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/jeffkit/plaita-go/pkg/duration"
	"github.com/jeffkit/plaita-go/pkg/sandbox"
)

// normalizeDoc rewrites a deployment-form flow document in place into the
// engine's native JSON shape: alias keys folded, handlers canonicalized,
// timeouts converted to ISO 8601, lossy-but-informative fields stashed into
// metadata. Embedded flows (childFlow / branches[].flow) are normalized
// recursively.
func normalizeDoc(doc map[string]any, sc scope) error {
	meta := asMeta(doc)

	// flow id: plaita emits flow_id; console exports may carry id; flowId is
	// the camel spelling. flow_id wins over id (plaita's own rule). The engine
	// requires a non-blank id, so anonymous roots get a placeholder.
	if id := firstString(doc, "flowId", "flow_id"); id != "" {
		doc["id"] = id
	}
	delete(doc, "flowId")
	delete(doc, "flow_id")
	if s, _ := doc["id"].(string); s == "" {
		doc["id"] = "plaita-flow"
	}

	// global context: plaita global_context -> engine context.global
	for _, k := range []string{"globalContext", "global_context"} {
		if v, ok := doc[k]; ok {
			if _, exists := doc["context"]; !exists {
				doc["context"] = map[string]any{"global": v}
			}
			delete(doc, k)
		}
	}

	// exposeEnv: plaita emits expose_env (model_dump) and exposeEnv (deploy
	// camel). Fold to the contract spelling and pass through at doc top level —
	// the engine restricts $ENV to this allowlist (workflow.Workflow.ExposeEnv).
	// Invalid entries are rejected at load (fail-closed), the rules of plaita's
	// _safe_environment pulled forward from runtime to build time.
	foldKey(doc, "expose_env", "exposeEnv")
	if v, ok := doc["exposeEnv"]; ok {
		if err := validateExposeEnv(v); err != nil {

			return fmt.Errorf("ir: flow %q: %v", doc["id"], err)
		}
	}
	doc["metadata"] = meta

	if v, ok := doc["timeout"]; ok {
		s, err := normalizeTimeout(v)
		if err != nil {
			return fmt.Errorf("ir: flow %q: %v", doc["id"], err)
		}
		if s == "" {
			delete(doc, "timeout")
		} else {
			doc["timeout"] = s
		}
	}

	for _, k := range []string{"inputType", "input_type", "outputType", "output_type"} {
		if v, ok := doc[k]; ok {
			m, ok := v.(map[string]any)
			if !ok {
				return fmt.Errorf("ir: flow %q: %s must be an object", doc["id"], k)
			}
			if err := normalizeProperty(m); err != nil {
				return fmt.Errorf("ir: flow %q: %s: %v", doc["id"], k, err)
			}
			if k == "input_type" {
				doc["inputType"] = m
				delete(doc, "input_type")
			} else if k == "output_type" {
				doc["outputType"] = m
				delete(doc, "output_type")
			}
		}
	}

	rawNodes, ok := doc["nodes"].([]any)
	if !ok || len(rawNodes) == 0 {
		return fmt.Errorf("ir: flow %q: nodes must be a non-empty array", doc["id"])
	}
	for i, rn := range rawNodes {
		n, ok := rn.(map[string]any)
		if !ok {
			return fmt.Errorf("ir: flow %q: nodes[%d] is not an object", doc["id"], i)
		}
		if err := normalizeNode(n); err != nil {
			return fmt.Errorf("ir: flow %q: node %q: %v", doc["id"], nodeLabel(n), err)
		}
		// embedded flows: synthesize an engine-required id, then normalize and
		// validate recursively (mirrors plaita's check_childflow_reachable_end).
		if err := normalizeEmbedded(doc["id"].(string), n); err != nil {
			return err
		}
	}
	return nil
}

// normalizeEmbedded recursively normalizes and validates the flow documents
// embedded in loop/child/parallel nodes. The engine requires a non-blank
// workflow id, which plaita's embedded flows never carry, so one is
// synthesized from the parent node id.
func normalizeEmbedded(flowID string, n map[string]any) error {
	embed := func(sub map[string]any, name string) error {
		if sub == nil {
			return nil
		}
		if s, _ := sub["id"].(string); s == "" {
			sub["id"] = fmt.Sprintf("%s.%s", n["id"], name)
		}
		if err := normalizeDoc(sub, scopeChild); err != nil {
			return fmt.Errorf("ir: flow %q: node %q %s: %v", flowID, nodeLabel(n), name, err)
		}
		if err := validateDoc(sub, scopeChild); err != nil {
			return fmt.Errorf("ir: flow %q: node %q %s: %v", flowID, nodeLabel(n), name, err)
		}
		return nil
	}
	switch n["type"] {
	case "loop", "child":
		if cf, ok := n["childFlow"].(map[string]any); ok {
			return embed(cf, "childFlow")
		}
	case "parallel":
		branches, _ := n["branches"].([]any)
		for i, rb := range branches {
			b, ok := rb.(map[string]any)
			if !ok {
				return fmt.Errorf("ir: flow %q: node %q branches[%d] is not an object", flowID, nodeLabel(n), i)
			}
			name, _ := b["name"].(string)
			if name == "" {
				name = fmt.Sprintf("branch.%d", i)
			}
			if f, ok := b["flow"].(map[string]any); ok {
				if err := embed(f, "branch."+name); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func normalizeNode(n map[string]any) error {
	t, _ := n["type"].(string)
	if t == "" {
		return fmt.Errorf("missing type")
	}
	if !supportedNodeTypes[t] {
		if hint, known := knownUnsupported[t]; known {
			return fmt.Errorf("node type %q is not supported: %s", t, hint)
		}
		return fmt.Errorf("node type %q is not supported by this engine (supported: %v)",
			t, sortedKeys(supportedNodeTypes))
	}

	meta := asMeta(n)

	// handler keys: snake spellings fold into camel (camel wins).
	foldKey(n, "timeout_handler", "timeoutHandler")
	foldKey(n, "error_handler", "errorHandler")
	if err := normalizeHandler(n, "timeoutHandler", meta, false); err != nil {
		return err
	}
	if err := normalizeHandler(n, "errorHandler", meta, true); err != nil {
		return err
	}

	// source_line: compile-time provenance, keep observable via metadata.
	if v := firstAny(n, "sourceLine", "source_line"); v != nil {
		stash(meta, "sourceLine", v)
	}
	delete(n, "sourceLine")
	delete(n, "source_line")
	n["metadata"] = meta

	if v, ok := n["timeout"]; ok {
		s, err := normalizeTimeout(v)
		if err != nil {
			return err
		}
		if s == "" {
			delete(n, "timeout")
		} else {
			n["timeout"] = s
		}
	}
	if v, ok := n["outputType"].(map[string]any); ok {
		if err := normalizeProperty(v); err != nil {
			return fmt.Errorf("outputType: %v", err)
		}
	}
	foldKey(n, "output_type", "outputType")

	foldNodeFieldAliases(n)

	switch t {
	case "switch":
		return translateSwitch(n)
	case "parallel":
		return translateParallel(n)
	case "loop":
		return translateLoop(n)
	case "child":
		return translateChild(n)
	case "http":
		return translateHTTP(n)
	case "code":
		return translateCode(n)
	case "end":
		return translateEnd(n)
	case "agentrun":
		return translateAgentRun(n)
	}
	return nil
}

// translateAgentRun folds the agentrun node's camel spellings into the
// snake field names plaita-nodes defines, and validates the split between
// the host path (repo) and the sandbox path (workspace):
//
//   - workspace and repo are mutually exclusive;
//   - a literal workspace name must exist in the infra registry
//     (.plaita/sandboxes.json, fail-closed — never created on the fly);
//   - expression workspaces cannot be checked statically; the runtime branch
//     applies the same fail-closed guard after evaluation.
func translateAgentRun(n map[string]any) error {
	foldKey(n, "timeoutSecs", "timeout_secs")
	foldKey(n, "dryRun", "dry_run")

	workspace, present := n["workspace"]
	if !present || workspace == nil {
		return nil
	}
	workspaceName, ok := workspace.(string)
	if !ok {
		return fmt.Errorf("agentrun workspace must be a string (registry name or expression)")
	}
	workspaceName = strings.TrimSpace(workspaceName)
	if workspaceName == "" {
		return nil
	}
	if repo := firstAny(n, "repo"); repo != nil && repo != "" {
		return fmt.Errorf("agentrun workspace and repo are mutually exclusive")
	}
	if sandbox.IsDynamicExpression(workspaceName) {
		return nil
	}
	specs, err := sandbox.LoadSandboxes("")
	if err != nil {
		return fmt.Errorf("agentrun workspace %q: %v", workspaceName, err)
	}
	if _, registered := specs[workspaceName]; !registered {
		return fmt.Errorf("agentrun workspace %q is not registered (fail-closed): "+
			"define it in .plaita/sandboxes.json", workspaceName)
	}
	return nil
}

// normalizeHandler canonicalizes an error/timeout handler: strategy spelling,
// plaita error_code/error_message folded into the engine's code/message
// handler fields, retry spelling folded.
func normalizeHandler(n map[string]any, key string, meta map[string]string, recoverable bool) error {
	raw, ok := n[key]
	if !ok {
		return nil
	}
	h, ok := raw.(map[string]any)
	if !ok {
		return fmt.Errorf("%s must be an object", key)
	}
	foldKey(h, "default_value", "defaultValue")
	foldKey(h, "retry_times", "retryTimes")
	if s, ok := h["strategy"].(string); ok && s == "continue_with" {
		h["strategy"] = "continue-with"
	}
	// plaita error_code/error_message (aliases code/message) fold into the
	// handler object: the engine policy structs carry them (abort uses them
	// as the flow error code/message — plaita NodeExecutionError).
	foldKey(h, "error_code", "code")
	foldKey(h, "error_message", "message")
	return nil
}

// nodeFieldAliases folds the camelCase spellings of plaita-nodes node
// fields into their canonical snake_case JSON tags (the Go nodes use the
// Python field names as tags; these aliases cover the mixed-case fields).
var nodeFieldAliases = [][2]string{
	{"timeoutSecs", "timeout_secs"},
	{"maxRetries", "max_retries"},
	{"dryRun", "dry_run"},
	{"storeDir", "store_dir"},
	{"baseDir", "base_dir"},
	{"gateName", "gate_name"},
	{"bodyIsHtml", "body_is_html"},
	{"timeoutMs", "timeout_ms"},
	{"raiseOnError", "raise_on_error"},
	{"allowRedirects", "allow_redirects"},
	{"joinFields", "join_fields"},
	{"workspaceRoot", "workspace_root"},
	{"worktreeDir", "worktree_dir"},
	{"branchName", "branch_name"},
	{"commitMessage", "commit_message"},
	{"planFile", "plan_file"},
	{"issueNumber", "issue_number"},
	{"mergeMode", "merge_mode"},
	{"mainClone", "main_clone"},
	{"baseBranch", "base_branch"},
	{"apiBase", "api_base"},
	{"apiKey", "api_key"},
	{"maxTokens", "max_tokens"},
	{"jsonMode", "json_mode"},
	{"extraBody", "extra_body"},
	{"minConfidence", "min_confidence"},
	{"onLowConfidence", "on_low_confidence"},
	{"defaultChoice", "default_choice"},
	{"baseUrl", "base_url"},
	{"pollInterval", "poll_interval"},
	{"waitReply", "wait_reply"},
}

// foldNodeFieldAliases applies the alias table to a node doc (harmless when
// keys are absent).
func foldNodeFieldAliases(n map[string]any) {
	for _, pair := range nodeFieldAliases {
		foldKey(n, pair[0], pair[1])
	}
}

// translateSwitch: plaita branches allow name-as-target (branch_name_as_target);
// fill next from name when absent.
func translateSwitch(n map[string]any) error {
	branches, _ := n["branches"].([]any)
	for i, rb := range branches {
		b, ok := rb.(map[string]any)
		if !ok {
			return fmt.Errorf("branches[%d] is not an object", i)
		}
		if nextIsEmpty(b) {
			name, _ := b["name"].(string)
			if name == "" {
				return fmt.Errorf("branches[%d] has neither next nor name", i)
			}
			b["next"] = name
		}
		if c, ok := b["condition"].(map[string]any); ok {
			if err := normalizeCondition(c); err != nil {
				return fmt.Errorf("branches[%d] condition: %v", i, err)
			}
		}
	}
	return nil
}

// normalizeCondition folds plaita condition group spellings (recursive).
func normalizeCondition(c map[string]any) error {
	if conds, ok := c["conditions"].([]any); ok {
		for _, rc := range conds {
			rc, ok := rc.(map[string]any)
			if !ok {
				return fmt.Errorf("condition group member is not an object")
			}
			if _, isGroup := rc["conditions"]; isGroup {
				if err := normalizeCondition(rc); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func translateParallel(n map[string]any) error {
	foldKey(n, "join_branches", "joinBranches")
	// mode: plaita thread/process/coroutine all map onto goroutine execution;
	// pass known values, stash unknown ones.
	if m, ok := n["mode"].(string); ok {
		switch m {
		case "thread", "process", "coroutine", "artificial":
		default:
			stash(asMeta(n), "parallelModeIgnored", m)
			n["mode"] = "thread"
		}
	}
	branches, _ := n["branches"].([]any)
	for i, rb := range branches {
		b, ok := rb.(map[string]any)
		if !ok {
			return fmt.Errorf("branches[%d] is not an object", i)
		}
		if inp, ok := b["input"]; ok && inp != nil {
			if _, isMap := inp.(map[string]any); !isMap {
				return fmt.Errorf("branches[%d] input must be an object (plaita dict-only child input)", i)
			}
		}
		if c, ok := b["condition"].(map[string]any); ok {
			if err := normalizeCondition(c); err != nil {
				return fmt.Errorf("branches[%d] condition: %v", i, err)
			}
		}
	}
	return nil
}

func translateLoop(n map[string]any) error {
	if v, ok := n["itemType"].(map[string]any); ok {
		if err := normalizeProperty(v); err != nil {
			return fmt.Errorf("itemType: %v", err)
		}
	}
	foldKey(n, "item_type", "itemType")
	return nil
}

func translateChild(n map[string]any) error {
	if _, hasCF := n["childFlow"]; hasCF {
		return nil
	}
	// reference-style child (flowId/flowVersion) needs a flow provider —
	// fail closed instead of silently degrading.
	for _, k := range []string{"flowId", "flow_id", "flowVersion", "flow_version"} {
		if _, ok := n[k]; ok {
			return fmt.Errorf("reference-style child flow (%s) is not supported; inline the flow as childFlow", k)
		}
	}
	return fmt.Errorf("child node requires an inline childFlow document")
}

func translateHTTP(n map[string]any) error {
	foldKey(n, "content_type", "contentType")
	meta := asMeta(n)
	// plaita SSRF hardening fields: this engine does not implement the guard
	// yet, so flows that rely on it must not load (fail closed).
	for _, k := range []string{"allowedHosts", "allowed_hosts"} {
		if v, ok := n[k]; ok {
			if l, ok := v.([]any); ok && len(l) > 0 {
				return fmt.Errorf("http %q: %s is not enforced by this engine; remove it or run this flow on the plaita runtime", n["id"], k)
			}
			delete(n, k)
		}
	}
	for _, k := range []string{"deniedHosts", "denied_hosts"} {
		if v, ok := n[k]; ok {
			if l, ok := v.([]any); ok && len(l) > 0 {
				return fmt.Errorf("http %q: %s is not enforced by this engine; remove it or run this flow on the plaita runtime", n["id"], k)
			}
			delete(n, k)
		}
	}
	for _, k := range []string{"blockPrivateNetworks", "block_private_networks"} {
		if v, ok := n[k]; ok {
			if b, ok := v.(bool); ok && b {
				return fmt.Errorf("http %q: blockPrivateNetworks is not enforced by this engine; remove it or run this flow on the plaita runtime", n["id"])
			}
			delete(n, k)
		}
	}
	// requestTimeout (seconds, float) folds into the node timeout when absent.
	for _, k := range []string{"requestTimeout", "request_timeout"} {
		if v, ok := n[k]; ok {
			secs, err := asFloat(v)
			if err != nil {
				return fmt.Errorf("requestTimeout: %v", err)
			}
			if secs > 0 {
				if cur, has := n["timeout"]; !has || cur == "" || cur == nil {
					n["timeout"] = "PT" + strconv.FormatFloat(secs, 'f', -1, 64) + "S"
				} else {
					stash(meta, "requestTimeoutIgnored", v)
				}
			}
			delete(n, k)
		}
	}
	if v, ok := n["maxRedirects"]; ok {
		stash(meta, "maxRedirects", v)
		delete(n, "maxRedirects")
	}
	foldKey(n, "max_redirects", "maxRedirects")
	return nil
}

func translateCode(n map[string]any) error {
	lang, _ := n["language"].(string)
	if lang != "javascript" {
		return fmt.Errorf("code node language %q is not supported (this engine runs javascript only)", lang)
	}
	for _, k := range []string{"sandboxBackend", "sandbox_backend"} {
		if v, ok := n[k]; ok && v != nil && v != "" {
			return fmt.Errorf("code node sandboxBackend %v is not supported in this engine", v)
		}
		delete(n, k)
	}
	return nil
}

func translateEnd(n map[string]any) error {
	rt, ok := n["resultType"].(string)
	if !ok || rt == "" {
		rt = firstString(n, "result_type")
		if rt != "" {
			n["resultType"] = rt
		}
	}
	delete(n, "result_type")
	switch n["resultType"] {
	case nil, "", "success", "error":
	case "nop":
		return fmt.Errorf("end resultType \"nop\" (silent end) is not supported; use success and drop the output")
	default:
		return fmt.Errorf("end resultType %q is invalid (success|error)", n["resultType"])
	}
	if e, ok := n["error"].(map[string]any); ok {
		if code, ok := e["code"]; ok {
			e["code"] = fmt.Sprint(code)
		}
	}
	return nil
}

// normalizeProperty folds plaita property spellings into the engine schema,
// recursively for object properties. Every property of a deployed flow is
// marked strict: value validation uses the plaita io.match model (no cast
// coercion). Properties built directly as Go structs keep the lenient model.
func normalizeProperty(p map[string]any) error {
	p["strict"] = true
	if _, has := p["dataType"]; !has {
		if t, ok := p["type"]; ok {
			p["dataType"] = t
		}
	}
	delete(p, "type")
	foldKey(p, "title", "label")
	foldKey(p, "description", "desc")
	foldKey(p, "is_required", "isRequired")
	foldKey(p, "default_value", "defaultValue")
	if _, has := p["defaultValue"]; !has {
		if d, ok := p["default"]; ok {
			p["defaultValue"] = d
		}
	}
	delete(p, "default")
	foldKey(p, "item_type", "itemType")
	if it, ok := p["itemType"].(map[string]any); ok {
		if err := normalizeProperty(it); err != nil {
			return err
		}
	}
	// nested object properties: engine key is properties; plaita internal
	// spelling is children; console exports use properties directly.
	if _, has := p["properties"]; !has {
		if c, ok := p["children"]; ok {
			p["properties"] = c
		}
	}
	delete(p, "children")
	// plaita Property.required (io.py _handle_required): bool form folds into
	// is_required; list form marks the named children required.
	switch req := p["required"].(type) {
	case bool:
		if _, has := p["isRequired"]; !has {
			p["isRequired"] = req
		}
	case []any:
		props, _ := p["properties"].(map[string]any)
		for _, name := range req {
			s, ok := name.(string)
			if !ok || props == nil {
				continue
			}
			if child, ok := props[s].(map[string]any); ok {
				if _, has := child["isRequired"]; !has {
					child["isRequired"] = true
				}
			}
		}
	}
	delete(p, "required")
	if props, ok := p["properties"].(map[string]any); ok {
		for _, v := range props {
			if m, ok := v.(map[string]any); ok {
				if err := normalizeProperty(m); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// validateExposeEnv enforces plaita _safe_environment's entry rules at load
// time: an array of exact environment variable names — no wildcards, no blank
// or whitespace-padded entries (Python raises ValueError at runtime; this
// engine rejects the flow earlier).
func validateExposeEnv(v any) error {
	l, ok := v.([]any)
	if !ok {
		return fmt.Errorf("exposeEnv must be an array of environment variable names, got %T", v)
	}
	var bad []string
	for _, item := range l {
		s, isStr := item.(string)
		switch {
		case !isStr:
			bad = append(bad, fmt.Sprint(item))
		case s == "" || strings.TrimSpace(s) != s || strings.Contains(s, "*"):
			bad = append(bad, s)
		}
	}
	if len(bad) > 0 {
		return fmt.Errorf("exposeEnv contains invalid entries %q: no wildcards, no blank or "+
			"whitespace-padded keys; list exact environment variable names", bad)
	}
	return nil

}

// normalizeTimeout converts plaita timeout spellings to ISO 8601.
// Accepted: "" / nil (absent = no budget), millisecond number / numeric
// string, "PT..." ISO string. An explicit 0 ("PT0S") is a zero budget —
// the strictest limit, never "no timeout" (plaita#138); negative and
// non-finite numbers are rejected instead of silently degrading to
// unlimited (plaita#139).
func normalizeTimeout(v any) (string, error) {
	switch t := v.(type) {
	case nil:
		return "", nil
	case string:
		s := strings.TrimSpace(t)
		if s == "" {
			return "", nil
		}
		if strings.HasPrefix(s, "P") {
			// validate the ISO spelling here so a bad budget fails at load,
			// not at execute
			if _, err := duration.ParseISO8601(s); err != nil {
				return "", fmt.Errorf("timeout %q: %v", t, err)
			}
			return s, nil
		}
		ms, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return "", fmt.Errorf("timeout %q is neither ISO 8601 nor milliseconds", t)
		}
		return normalizeMillis(ms)
	case json.Number:
		ms, err := t.Float64()
		if err != nil {
			return "", fmt.Errorf("timeout %v is not a number", v)
		}
		return normalizeMillis(ms)
	case int:
		return normalizeMillis(float64(t))
	case int64:
		return normalizeMillis(float64(t))
	case float64:
		return normalizeMillis(t)
	default:
		return "", fmt.Errorf("timeout has unsupported type %T", v)
	}
}

func normalizeMillis(ms float64) (string, error) {
	if math.IsInf(ms, 0) || math.IsNaN(ms) {
		return "", fmt.Errorf("timeout %v must be a finite number", ms)
	}
	if ms < 0 {
		return "", fmt.Errorf("timeout %v must be a non-negative number", ms)
	}
	if ms == 0 {
		return "PT0S", nil
	}
	return millisToISO(ms), nil
}

func millisToISO(ms float64) string {
	if ms == float64(int64(ms)) && int64(ms)%1000 == 0 {
		return "PT" + strconv.FormatInt(int64(ms)/1000, 10) + "S"
	}
	return "PT" + strconv.FormatFloat(ms/1000, 'f', -1, 64) + "S"
}
