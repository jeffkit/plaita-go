// Package ir loads plaita deployment-form flow JSON into an executable
// engine workflow.
//
// The deployment JSON is the single authoritative contract between the
// plaita Python frontend (@flow compile / console publish) and this engine.
// It is produced by plaita's codeflow compiler: top-level
// `flow_id`/`inputType`/`nodes`/`runtime`, camelCase node fields with a few
// snake residues the loader tolerates (`source_line`, `flow_id`,
// `timeout_handler`, ...). Legacy console exports (go-era shape with `id`,
// `required`/`default`/`children` property keys) are normalized too.
//
// The loader is a strict accept-list by design (fail-closed): plaita node
// types that this engine has not ported (collection variants,
// python code, ...) are rejected at load with an explicit message instead of
// being silently skipped. Contract changes must keep the round-trip with
// plaita's ir_validate rules; see AGENTS.md at the repo root.
package ir

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/jeffkit/plaita-go/pkg/workflow"
	// register the builtin node types the loader accepts
	_ "github.com/jeffkit/plaita-go/pkg/workflow/nodes"
)

// scope distinguishes the root flow from embedded flows (loop/child bodies,
// parallel branches). Embedded flows must additionally reach an end node,
// mirroring plaita's check_childflow_reachable_end.
type scope bool

const (
	scopeRoot  scope = false
	scopeChild scope = true
)

// supportedNodeTypes is the strict accept-list of plaita node types this
// engine implements.
var supportedNodeTypes = map[string]bool{
	"start":      true,
	"end":        true,
	"assignment": true,
	"switch":     true,
	"if":         true,
	"parallel":   true,
	"loop":       true,
	"child":      true,
	"http":       true,
	"redis":      true,
	"code":       true,
	"agentrun":   true,

	// plaita-nodes 自包含节点集（2026-10-19 Wave 2 对齐）
	"gate":             true,
	"rate_limit":       true,
	"report":           true,
	"notify":           true,
	"feishu_webhook":   true,
	"dingtalk_webhook": true,
	"slack_webhook":    true,
	"wecom_webhook":    true,
	"generic_webhook":  true,
	"email_send":       true,
	"api_request":      true,
	"parse_json":       true,
	"writefile":        true,

	// Wave 2.5/2.6（2026-10-19）：本地命令/远端写 + LLM 原子 + 人工确认
	"capture":        true,
	"git_publish":    true,
	"github_comment": true,
	"llm":            true,
	"decision":       true,
	"hitl":           true,

	// M6 checkpoint v1.5（2026-10-19）：挂起恢复地基解锁的挂起节点
	"event":      true,
	"hitl_await": true,

	// #80 phase 1（2026-10-22）：核心集合/循环节点按 Python 语义补齐
	// （plaita/node/loop.py BaseCollectionNode 家族 + While）
	"map":    true,
	"filter": true,
	"find":   true,
	"reduce": true,
	"while":  true,

	// #80 phase 2（2026-10-22，r78）：
	//   - case：plaita 的等值匹配方言（decide.py SwitchLegacy），Python 在模型
	//     校验期把它脱糖成普通 Switch——translateCase 在归一化层做同款脱糖，
	//     引擎侧与 switch 同一执行面（工厂按别名注册）。
	//   - mock：数据固定 pin 占位（console 把被固定节点替换成本类型）。
	//   - reference：引用逻辑（调度器按 flowId 注入 childFlow）——Go 在加载期
	//     经 FlowProvider 解析注入，注入的子流程过全量静态校验。
	"case":      true,
	"mock":      true,
	"reference": true,

	// #80 phase 3（2026-10-22，r79）：server 扩展节点五类（挂起型，恢复由外部
	// service 消费 {prefix}:{subtype}:queue 后发布事件驱动；部署依赖见
	// capability.go 的 NodeDependencies）。
	"delay":         true,
	"approval":      true,
	"redis_queue":   true,
	"kafka_queue":   true,
	"http_callback": true,

	// #84（2026-10-22，r84）：SQL 查询连接器（plaita-nodes database.py）。
	// database/sql + 纯 Go 驱动矩阵（pgx/mysql/modernc sqlite，无 cgo）；
	// 凭据面 / 上限 fail-closed（plaita#205）/ 参数改写 / 错误分类见
	// pkg/workflow/nodes/sql_query.go 与 AGENTS.md「sql_query 节点」节。
	"sql_query": true,
}

// knownUnsupported types carry a targeted hint; anything else unknown gets a
// generic message listing what is available.
//
// calculate / ignore 的结论（r78，对齐的是 plaita 运行时行为而非模块存在性）：
// 两者都不是注册节点类型——plaita 的 Flow.model_validate 对它们报
// "unRecognized node type"（calculate.py 模块存在但未注册，是遗留物；ignore
// 只是 Node 基类的 node_type 占位 ClassVar）。Go 保持拒载，错误信息刻意保留
// 同款子串作逐字对拍（node_parity2_golden 的 reject_* 用例）。
var knownUnsupported = map[string]string{
	"calculate": "unRecognized node type: calculate — the type is not registered in the plaita runtime " +
		"(the calculate.py module is vestigial); use assignment/code expressions instead",
	"ignore":   "unRecognized node type: ignore — not a registered node type (it is the base Node class placeholder); plaita itself rejects flows carrying it",
	"sleep":    "sleep node is not part of the plaita dialect",
	"trpc":     "internal RPC node was removed from this engine",
	"mdb":      "internal database node was removed from this engine",
	"readmdb":  "internal database node was removed from this engine",
	"writemdb": "internal database node was removed from this engine",
}

// FlowProvider supplies deployment-form flow JSON for reference nodes — the
// Go counterpart of plaita's scheduler, which injects child_flow into
// ReferenceFlow nodes by flow_id/flow_version before running. Returned
// documents go through the same normalization + validation as inline
// childFlows (embedded-flow scope: reachable end, own start, ...), so a
// resolved reference is statically checked exactly like an inlined one.
//
// This interface is deliberately deployment-JSON-shaped (not
// providers.WorkflowProvider, which yields parsed engine workflows): reference
// resolution must re-enter the ir pipeline, not bypass it.
type FlowProvider interface {
	LoadFlowJSON(flowID, flowVersion string) ([]byte, error)
}

// Load parses plaita deployment-form flow JSON and builds the engine
// workflow. Structural problems (missing start, dangling refs, unreachable
// nodes, unsupported node types) are reported as errors before execution.
// A reference node carrying flowId needs LoadWithFlowProvider.
func Load(data []byte) (*workflow.Workflow, error) {
	return load(data, nil)
}

// LoadWithFlowProvider is Load with reference resolution: flowId/flowVersion
// references are fetched from the provider and inlined as childFlow at load
// time (provenance stashed in node metadata). Provider misses and malformed
// documents fail the load (fail-closed). Nested references resolve
// recursively; a reference cycle is rejected at a fixed depth.
func LoadWithFlowProvider(data []byte, provider FlowProvider) (*workflow.Workflow, error) {
	if provider == nil {
		return Load(data)
	}
	return load(data, &flowResolver{provider: provider})
}

func load(data []byte, res *flowResolver) (*workflow.Workflow, error) {
	var doc map[string]any
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("ir: parse flow json: %w", err)
	}
	// The decoder folds lone surrogate escapes to U+FFFD; the Python
	// authority refuses such definitions at its hash's .encode("utf-8").
	// Reject by name so no pool runs what the fleet cannot fingerprint.
	if err := RejectLoneSurrogateEscapes(data); err != nil {
		return nil, fmt.Errorf("ir: parse flow json: %w", err)
	}
	if err := normalizeDoc(doc, scopeRoot, res); err != nil {
		return nil, err
	}
	if err := validateDoc(doc, scopeRoot); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("ir: re-encode flow: %w", err)
	}
	wf := new(workflow.Workflow)
	if err := json.Unmarshal(raw, wf); err != nil {
		return nil, fmt.Errorf("ir: build workflow: %w", err)
	}
	return wf, nil
}
