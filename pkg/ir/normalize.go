package ir

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
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

	// exposeEnv has no engine field yet; keep it observable via metadata.
	for _, k := range []string{"exposeEnv", "expose_env"} {
		if v, ok := doc[k]; ok {
			if err := stashList(meta, "exposeEnv", v); err != nil {
				return fmt.Errorf("ir: flow %q: %v", doc["id"], err)
			}
			delete(doc, k)
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
	}
	return nil
}

// normalizeHandler canonicalizes an error/timeout handler: strategy spelling,
// plaita-only fields (code/message) moved to metadata, retry spelling folded.
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
	// plaita error_code/error_message (aliases code/message) have no engine
	// counterpart; preserve them as metadata instead of dropping.
	for _, pair := range [][2]string{{"error_code", "errorCode"}, {"error_message", "errorMessage"}, {"code", "errorCode"}, {"message", "errorMessage"}} {
		if v, ok := h[pair[0]]; ok {
			stash(meta, pair[1], v)
			delete(h, pair[0])
		}
	}
	return nil
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
// recursively for object properties.
func normalizeProperty(p map[string]any) error {
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

// normalizeTimeout converts plaita timeout spellings to ISO 8601.
// Accepted: "" (absent), millisecond number / numeric string, "PT..." string.
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
			return s, nil
		}
		ms, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return "", fmt.Errorf("timeout %q is neither ISO 8601 nor milliseconds", t)
		}
		return millisToISO(ms), nil
	case json.Number:
		ms, err := t.Float64()
		if err != nil {
			return "", fmt.Errorf("timeout %v is not a number", v)
		}
		return millisToISO(ms), nil
	case int:
		return millisToISO(float64(t)), nil
	case int64:
		return millisToISO(float64(t)), nil
	case float64:
		return millisToISO(t), nil
	default:
		return "", fmt.Errorf("timeout has unsupported type %T", v)
	}
}

func millisToISO(ms float64) string {
	if ms <= 0 {
		return ""
	}
	if ms == float64(int64(ms)) && int64(ms)%1000 == 0 {
		return "PT" + strconv.FormatInt(int64(ms)/1000, 10) + "S"
	}
	return "PT" + strconv.FormatFloat(ms/1000, 'f', -1, 64) + "S"
}
