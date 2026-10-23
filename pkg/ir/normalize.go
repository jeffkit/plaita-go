package ir

import (
	"bytes"
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
// recursively. res carries the optional FlowProvider for reference nodes
// (nil when loading without one).
func normalizeDoc(doc map[string]any, sc scope, res *flowResolver) error {
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
		if err := normalizeNode(n, res); err != nil {
			return fmt.Errorf("ir: flow %q: node %q: %v", doc["id"], nodeLabel(n), err)
		}
		// embedded flows: synthesize an engine-required id, then normalize and
		// validate recursively (mirrors plaita's check_childflow_reachable_end).
		if err := normalizeEmbedded(doc["id"].(string), n, res); err != nil {
			return err
		}
	}
	return nil
}

// normalizeEmbedded recursively normalizes and validates the flow documents
// embedded in loop/child/parallel nodes. The engine requires a non-blank
// workflow id, which plaita's embedded flows never carry, so one is
// synthesized from the parent node id.
func normalizeEmbedded(flowID string, n map[string]any, res *flowResolver) error {
	embed := func(sub map[string]any, name string) error {
		if sub == nil {
			return nil
		}
		if s, _ := sub["id"].(string); s == "" {
			sub["id"] = fmt.Sprintf("%s.%s", n["id"], name)
		}
		if err := normalizeDoc(sub, scopeChild, res); err != nil {
			return fmt.Errorf("ir: flow %q: node %q %s: %v", flowID, nodeLabel(n), name, err)
		}
		if err := validateDoc(sub, scopeChild); err != nil {
			return fmt.Errorf("ir: flow %q: node %q %s: %v", flowID, nodeLabel(n), name, err)
		}
		return nil
	}
	switch n["type"] {
	case "loop", "child", "map", "filter", "find", "reduce", "while":
		if cf, ok := n["childFlow"].(map[string]any); ok {
			return embed(cf, "childFlow")
		}
	case "reference":
		// translateReference 已把 provider 解析出的子流程注入 childFlow；这里
		// 与其他嵌入流程同路归一化+校验。深度计数随本子树闭合归还——菱形引用
		// 不受误伤，真正的环撞 maxReferenceResolveDepth。
		if cf, ok := n["childFlow"].(map[string]any); ok && res != nil {
			err := embed(cf, "childFlow")
			res.depth--
			return err
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

func normalizeNode(n map[string]any, res *flowResolver) error {
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
	case "case":
		// plaita 在模型校验期把 case 脱糖成 Switch（decide.py SwitchLegacy）；
		// 脱糖后走 switch 的分支归一化。
		if err := translateCase(n); err != nil {
			return err
		}
		return translateSwitch(n)
	case "if":
		return translateIf(n)
	case "parallel":
		return translateParallel(n)
	case "loop":
		return translateLoop(n)
	case "map", "filter", "find", "reduce":
		return translateCollection(n)
	case "while":
		return translateWhile(n)
	case "child":
		return translateChild(n)
	case "reference":
		return translateReference(n, res)
	case "http":
		return translateHTTP(n)
	case "code":
		return translateCode(n)
	case "end":
		return translateEnd(n)
	case "agentrun":
		return translateAgentRun(n)
	case "event":
		return translateEvent(n)
	case "hitl_await":
		return translateHitlAwait(n)
	case "delay":
		return translateDelay(n)
	case "approval":
		return translateApproval(n)
	case "redis_queue":
		return translateRedisQueue(n)
	case "kafka_queue":
		return translateKafkaQueue(n)
	case "http_callback":
		return translateHTTPCallback(n)
	}
	return nil
}

// translateEvent folds the event node's camel spellings and validates the
// fields plaita validates at build time: a non-empty event_type (empty would
// be treated as a wildcard by the matcher) and a positive subscription
// timeout (0 would expire the subscription immediately).
func translateEvent(n map[string]any) error {
	foldKey(n, "eventType", "event_type")
	foldKey(n, "eventFilter", "event_filter")
	foldKey(n, "subscriptionTimeout", "subscription_timeout")
	eventType, _ := n["event_type"].(string)
	if strings.TrimSpace(eventType) == "" {
		return fmt.Errorf("event node requires a non-empty event_type (an empty type is treated as a wildcard)")
	}
	if v, ok := n["event_filter"]; ok && v != nil {
		if _, isMap := v.(map[string]any); !isMap {
			return fmt.Errorf("event_filter must be an object, got %T", v)
		}
	}
	if v, ok := n["subscription_timeout"]; ok && v != nil {
		seconds, valid := normalizeSeconds(v)
		if !valid || seconds <= 0 {
			return fmt.Errorf("subscription_timeout must be a positive number of seconds, got %v", v)
		}
	}
	return nil
}

// translateHitlAwait folds the camel spellings of the suspending HITL node.
// timeout_secs / event_type accept expressions, so they are not narrowed
// here (the node falls back exactly like plaita's resolve_* helpers).
func translateHitlAwait(n map[string]any) error {
	foldKey(n, "baseUrl", "base_url")
	foldKey(n, "timeoutSecs", "timeout_secs")
	foldKey(n, "eventType", "event_type")
	foldKey(n, "eventFilter", "event_filter")
	foldKey(n, "dryRun", "dry_run")
	if v, ok := n["event_filter"]; ok && v != nil {
		if _, isMap := v.(map[string]any); !isMap {
			return fmt.Errorf("event_filter must be an object, got %T", v)
		}
	}
	return nil
}

// normalizeSeconds decodes a JSON number (the IR decoder uses json.Number)
// into seconds. ok=false for non-numeric input.
func normalizeSeconds(v any) (float64, bool) {
	switch n := v.(type) {
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(n), 64)
		return f, err == nil
	}
	return 0, false
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

// translateCase 转正 plaita 的 case 节点（decide.py SwitchLegacy，等值匹配
// 方言）。Python 在模型校验期把它脱糖成普通 Switch：每条 case 变成
// Branch(name=case.id, condition={field:target, operator:eq, value:case.value},
// next=case.id)，外加恒置 isDefault 的兜底分支；本函数在 IR 归一化层做同款
// 脱糖（节点 type 改写为 switch + 构造 branches，metadata 记 caseDialect 留
// 痕），命中/输出语义与 switch 完全一致。拍板依据（r78 Python 探针）：
//
//   - cases[].id 必填 = 跳转目标（Python Branch(next=case["id"]) 硬编码 id，
//     builder 的 next→id 归一化只在 id 缺省时兜底；cases[].next 不参与路由）；
//     cases[].value 必填（null 合法，eq None 比较）；cases[].name 可选 = label。
//   - default 缺省或 null → 字面 "default" 节点 id（Python
//     values.get("default", "default") + Switch branch_name_as_target 合成目标，
//     实测命中 id 为 "default" 的节点）；非空字符串 → 该 id。
//   - target 必须是表达式字符串或缺省：缺省/None 时条件恒 None，只有
//     value:null 的 case 可命中——desugar 以引擎的 $NULL 字面量承载（引擎侧
//     None 字面量，构造出的条件不回流 Python）；字面量 target 是常量条件，
//     加载期拒绝（Python evaluate(字面量) 语义无部署意义）。
//   - cases 必须是数组；条目缺 id/value 加载期拒绝（Python 模型层 ValueError
//     前置）；空 cases 合法（只剩兜底分支，实测走 default）。
func translateCase(n map[string]any) error {
	target := n["target"]
	if target != nil {
		if _, ok := target.(string); !ok {
			return fmt.Errorf("case target must be an expression string (or absent), got %T", target)
		}
	} else {
		// Python condition field=None → evaluate(None) → None；引擎侧等价形。
		target = "$NULL"
	}
	rawCases, ok := n["cases"].([]any)
	if n["cases"] != nil && !ok {
		return fmt.Errorf("cases must be an array, got %T", n["cases"])
	}
	branches := make([]any, 0, len(rawCases)+1)
	for i, rc := range rawCases {
		c, isMap := rc.(map[string]any)
		if !isMap {
			return fmt.Errorf("cases[%d] is not an object (got %v) — every case needs id/value, optional name", i, rc)
		}
		id, _ := c["id"].(string)
		if id == "" {
			return fmt.Errorf("cases[%d] missing required \"id\" (the jump target) — every case needs id/value, optional name", i)
		}
		if _, has := c["value"]; !has {
			return fmt.Errorf("cases[%d] (%q) missing required \"value\"", i, id)
		}
		b := map[string]any{
			"name": id,
			"condition": map[string]any{
				"field":    target,
				"operator": "eq",
				"value":    c["value"],
			},
			"next": id,
		}
		if name, isStr := c["name"].(string); isStr && name != "" {
			b["label"] = name
		}
		branches = append(branches, b)
	}
	defaultNext := "default"
	if d, has := n["default"]; has && d != nil {
		s, isStr := d.(string)
		if !isStr || s == "" {
			return fmt.Errorf("default must be a node id string, got %v", d)
		}
		defaultNext = s
	}
	branches = append(branches, map[string]any{
		"name":      "default",
		"next":      defaultNext,
		"isDefault": true,
	})
	n["type"] = "switch"
	n["branches"] = branches
	delete(n, "target")
	delete(n, "cases")
	delete(n, "default")
	stash(asMeta(n), "caseDialect", "desugared-to-switch")
	return nil
}

// maxReferenceResolveDepth caps reference-chain resolution (a cycle must fail
// the load with a named error, not recurse forever).
const maxReferenceResolveDepth = 32

// flowResolver carries the FlowProvider through normalization; depth counts
// open reference resolutions (incremented in translateReference, returned
// when the injected subtree finishes in normalizeEmbedded).
type flowResolver struct {
	provider FlowProvider
	depth    int
}

// translateReference 转正 plaita 的 reference 节点（child.py ReferenceFlow，
// 引用逻辑）。plaita 的调度器在运行前按 flow_id/flow_version 把 child_flow
// 注入节点；Go 的对应物在加载期完成——经 FlowProvider 取回部署形态子流程
// JSON 注入 childFlow，随后与 inline childFlow 同路走 scopeChild 归一化+校验
// （normalizeEmbedded），嵌套 reference 在递归里继续解析。拍板依据（r78
// Python 实测）：
//
//   - inline childFlow 在 reference 上拒绝：plaita 运行时把它丢掉
//     （ReferenceFlow.setup_child_flow 与基类 FlowNode 同名遮蔽，child_flow
//     恒 None）并在执行期炸 "child_flow for reference flow node is required"。
//     Go 在加载期具名拒绝并指路（child 节点内联、或 flowId+provider），不给
//     「Go 能跑、plaita 跑不了」的跨运行时假象。
//   - flowId 缺省（且无 inline）拒绝：调度器无从注入。
//   - provider 未配置/解析失败/文档非法 → 加载失败（fail-closed）。
func translateReference(n map[string]any, res *flowResolver) error {
	foldKey(n, "flow_id", "flowId")
	foldKey(n, "flow_version", "flowVersion")
	if cf, has := n["childFlow"]; has && cf != nil {
		return fmt.Errorf("reference node takes no inline childFlow: plaita's runtime drops it (validator shadowing) and fails at run time with " +
			"\"child_flow for reference flow node is required\"; inline the sub-flow in a child node, or declare flowId/flowVersion and load via ir.LoadWithFlowProvider")
	}
	flowID := firstString(n, "flowId")
	if flowID == "" {
		return fmt.Errorf("reference node requires flowId/flow_id (plaita's scheduler injects the referenced flow by id; this engine resolves it at load time)")
	}
	flowVersion := firstString(n, "flowVersion")
	if res == nil || res.provider == nil {
		return fmt.Errorf("reference node %q references flow %q: no flow provider configured — use ir.LoadWithFlowProvider (plaita injects reference children by flowId at scheduling time)", n["id"], flowID)
	}
	if res.depth >= maxReferenceResolveDepth {
		return fmt.Errorf("reference node %q: reference chain deeper than %d while resolving flow %q (cycle?)", n["id"], maxReferenceResolveDepth, flowID)
	}
	raw, err := res.provider.LoadFlowJSON(flowID, flowVersion)
	if err != nil {
		return fmt.Errorf("reference node %q: resolving flow %q (version %q): %w", n["id"], flowID, flowVersion, err)
	}
	sub := map[string]any{}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&sub); err != nil {
		return fmt.Errorf("reference node %q: flow %q is not a JSON flow document: %w", n["id"], flowID, err)
	}
	res.depth++
	n["childFlow"] = sub
	stash(asMeta(n), "resolvedFrom", flowID+"@"+flowVersion)
	return nil
}

// translateIf 转正 plaita 的 if 节点（decide.py Bool，node_type "if"）。
// 部署 IR 的权威形状（codeflow 编译产物 + ir_validate 边集）：
//
//	{"type":"if","id":…,"condition":{…},"next":"<真分支目标>","else_next":"<假分支目标>"}
//
// 校验照 switch 模式（形状/别名在此，目标存在性与可达性在 validateDoc 边集）：
//
//   - camel 别名折叠：elseNext → else_next（ir_validate 认可的双拼写；
//     codeflow/builder/sexpr 三个前端都产出 snake，elseNext 只是部署残留）。
//   - next / else_next 必须非空（ir_validate :690-699 的 if 结构规则：缺真/
//     假分支目标是加载错误，不做 Python 模型层的 "true"/"false" 占位默认）。
//   - condition 形状 fail-closed：Python 对解析不出 shape 的 condition
//     （_parse_condition_content 返回 None）告警后永不命中、恒走假分支——
//     部署面等价于静默假成功（decide.py:168-175 自认的缺陷形态），这里改为
//     加载期拒绝；空 conditions 组同样拒绝（Go 组求值按 relation 走
//     and→true / or→false，与 Python 的恒真语义分歧，编译器不产出该形状）。
//     condition 缺省 / null 合法：照 Python condition=None 恒走假分支。
func translateIf(n map[string]any) error {
	foldKey(n, "elseNext", "else_next")

	if nextIsEmpty(n) {
		return fmt.Errorf("if node requires a true-branch target (next)")
	}
	if s := firstString(n, "else_next"); s == "" {
		return fmt.Errorf("if node requires a false-branch target (else_next/elseNext)")
	}

	if v, ok := n["condition"]; ok && v != nil {
		c, isMap := v.(map[string]any)
		if !isMap {
			return fmt.Errorf("if condition must be an object ({field,operator,value} or {relation,conditions}), got %T", v)
		}
		_, hasField := c["field"]
		_, hasOperator := c["operator"]
		_, hasValue := c["value"]
		conditions, _ := c["conditions"].([]any)
		_, hasRelation := c["relation"]
		switch {
		case hasField && hasOperator && hasValue:
			// 单条件（value 允许 null：eq/null 比较是合法形状）。
		case hasRelation && len(conditions) > 0:
			if err := normalizeCondition(c); err != nil {
				return fmt.Errorf("if condition: %v", err)
			}
		default:
			return fmt.Errorf("if condition is malformed: needs {field,operator,value} or " +
				"{relation,conditions[>0]}; a partial condition would always take the else branch")
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

// translateCollection 校验 map/filter/find/reduce 四个集合节点的部署形状
// （权威 = plaita codeflow 编译器 _compile_for 的产物，#80 phase 1）：
//
//		{"type":"map|filter|find|reduce","id":…,"collection":"<expr>","childFlow":{…}}
//
//	  - collection：表达式字符串 / 数组字面量 / null（空集合）；缺省同 null，
//	    与 Python evaluate(None) → _coerce_collection → [] 一致，形状以外的
//	    求值期错误（不可迭代标量）留在执行期（同 loop）。
//	  - itemType / typeDefs：别名折叠 + normalizeProperty（同 loop；typeDefs
//	    是 BaseCollectionNode 的遗留别名）。
//	  - map：concurrent 的历史别名 async 折叠（Python _normalize_concurrent）；
//	    maxConcurrent 缺省/0 = 不设限（Python `or None`），负数加载期拒绝
//	    （Python 到线程池构造期才 ValueError，这里前置到 Load）。
//	  - reduce：initial 为表达式，不做形状约束（R5/R6 引用扫描照常覆盖）。
//	  - childFlow 必须内联存在（plaita FlowNode.setup_child_flow 的
//	    "child_flow is required" 前置到加载期，nil ChildFlow 不给执行期炸点）。
func translateCollection(n map[string]any) error {
	t, _ := n["type"].(string)
	if v, ok := n["itemType"].(map[string]any); ok {
		if err := normalizeProperty(v); err != nil {
			return fmt.Errorf("itemType: %v", err)
		}
	}
	foldKey(n, "item_type", "itemType")
	foldKey(n, "typeDefs", "itemType")
	if t == "map" {
		foldKey(n, "async", "concurrent")
		// Python 模型字段是 max_concurrent（camel maxConcurrent 落 extra 被忽略
		// ——plaita 侧实测）；Go 引擎 tag 是 maxConcurrent。两边收拼写的并集。
		foldKey(n, "max_concurrent", "maxConcurrent")
		if v, ok := n["maxConcurrent"]; ok && v != nil {
			f, err := asFloat(v)
			if err != nil {
				return fmt.Errorf("maxConcurrent must be an integer, got %v", v)
			}
			if f < 0 {
				return fmt.Errorf("maxConcurrent must be >= 0 (0 = unbounded), got %v", v)
			}
		}
	}
	if _, ok := n["childFlow"].(map[string]any); !ok {
		return fmt.Errorf("%s node requires an inline childFlow document", t)
	}
	return nil
}

// translateWhile 转正 while 条件循环节点（plaita/node/loop.py While；#80
// phase 1）。部署 IR 双拼写都收：编译器 _compile_while 产 snake 的
// child_flow/max_iterations，console 正典产 camel（plaita#89 dual-read）。
//
//		{"type":"while","id":…,"condition":{…},"child_flow"|"childFlow":{…},
//		 "max_iterations"|"maxIterations":N}
//
//	  - condition 形状 fail-closed 同 if：畸形 condition 在 Python 里解析失败
//	    后永不命中、零轮静默 None（decide.py 自认的缺陷形态），这里加载期拒绝；
//	    缺省/null 合法（Python condition=None：只执行一轮）。
//	  - max_iterations 必须是整数；缺省 1000，触顶仅告警（执行期语义），
//	    ≤0 合法 = 零轮（Python `index < max_iterations` 直接不进循环）。
func translateWhile(n map[string]any) error {
	foldKey(n, "child_flow", "childFlow")
	foldKey(n, "maxIterations", "max_iterations")
	if v, ok := n["max_iterations"]; ok && v != nil {
		f, err := asFloat(v)
		if err != nil || f != math.Trunc(f) {
			return fmt.Errorf("max_iterations must be an integer, got %v", v)
		}
	}
	if v, ok := n["condition"]; ok && v != nil {
		c, isMap := v.(map[string]any)
		if !isMap {
			return fmt.Errorf("while condition must be an object ({field,operator,value} or {relation,conditions}), got %T", v)
		}
		_, hasField := c["field"]
		_, hasOperator := c["operator"]
		_, hasValue := c["value"]
		conditions, _ := c["conditions"].([]any)
		_, hasRelation := c["relation"]
		switch {
		case hasField && hasOperator && hasValue:
			// 单条件（value 允许 null：eq/null 比较是合法形状）。
		case hasRelation && len(conditions) > 0:
			if err := normalizeCondition(c); err != nil {
				return fmt.Errorf("while condition: %v", err)
			}
		default:
			return fmt.Errorf("while condition is malformed: needs {field,operator,value} or " +
				"{relation,conditions[>0]}; a malformed condition would silently run zero rounds")
		}
	}
	if _, ok := n["childFlow"].(map[string]any); !ok {
		return fmt.Errorf("while node requires an inline childFlow document")
	}
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

// translateCode admits the code node's two engine-executable languages.
//
// python (plaita CodeNode default): run through the python3 subprocess
// sidecar (nodes/code_python.go), which mirrors plaita's subprocess backend —
// operator-trusted code, wall-clock timeout, process-group kill; NOT an
// in-process or container isolation boundary. sandbox_backend is therefore
// adjudicated as: absent/""/"subprocess" accepted; "restricted"/"docker"/
// "unsafe" rejected fail-closed (the Go engine cannot honour those isolation
// tiers and must not silently downgrade them). An absent language defaults to
// python exactly like plaita's model_validator.
//
// javascript: unchanged — the engine JS interpreter is in-process and has no
// backend tiers, so any explicitly declared backend stays rejected.
func translateCode(n map[string]any) error {
	lang, _ := n["language"].(string)
	if n["language"] == nil || lang == "" {
		// plaita: language None → LANGUAGE_PYTHON
		n["language"] = "python"
		lang = "python"
	}
	switch lang {
	case "python":
		foldKey(n, "sandboxBackend", "sandbox_backend")
		switch backend, _ := n["sandbox_backend"].(string); backend {
		case "", "subprocess":
			// accepted: the Go sidecar is the subprocess-form backend; keep
			// the folded field so the engine node carries the declaration
			n["sandboxBackend"] = backend
			delete(n, "sandbox_backend")
		case "restricted", "docker", "unsafe":
			return fmt.Errorf(
				"code node sandboxBackend %q has no Go implementation: the Go engine "+
					"runs python code only in the subprocess form (fresh python3 process, "+
					"wall-clock timeout, process-group kill — not an in-process or container "+
					"isolation boundary). Set sandbox_backend to \"subprocess\" or drop it; "+
					"flows that need in-process (restricted) or container (docker) isolation "+
					"must run on the plaita runtime",
				backend)
		default:
			return fmt.Errorf("code node sandboxBackend %v is invalid for language python (expected absent or \"subprocess\")", n["sandbox_backend"])
		}
	case "javascript":
		for _, k := range []string{"sandboxBackend", "sandbox_backend"} {
			if v, ok := n[k]; ok && v != nil && v != "" {
				return fmt.Errorf("code node sandboxBackend %v is not supported in this engine (the javascript interpreter is in-process and has no sandbox backend tiers)", v)
			}
			delete(n, k)
		}
	default:
		return fmt.Errorf("code node language %q is not supported (this engine runs javascript or python)", lang)
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
			d, err := duration.ParseISO8601(s)
			if err != nil {
				return "", fmt.Errorf("timeout %q: %v", t, err)
			}
			// #57 belt-and-suspenders: ParseISO8601 now rejects
			// unrepresentable budgets, but no declared spelling should ever
			// yield a negative budget either — reject one explicitly so a
			// future Duration producer cannot reintroduce the wrap.
			if d.TimeDuration() < 0 {
				return "", fmt.Errorf("timeout %q: parsed to a negative budget (%s)", t, d)
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
