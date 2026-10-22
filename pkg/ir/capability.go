package ir

import "sort"

// Capability contract（plaita#300 的 worker 能力清单数据源，#80 phase 1 ③）。
//
// plaita#300 固定的 manifest JSON 结构：
//
//	{
//	  "engine": "plaita-go",
//	  "ir_contract_version": "2026-10-22",
//	  "supported_node_types": ["start", "end", ...],
//	  "unsupported_node_types": ["case", "mock", ...],
//	  "features": ["checkpoint", "hitl_await", "suspend"]
//	}
//
// 纪律：supported/unsupported 一律由本包的 accept-list（supportedNodeTypes /
// knownUnsupported）自动生成，不允许手写第二份清单（#300 的硬性要求）——
// 白名单变更后 manifest 即时跟随，capability_test.go 钉住这一不变量。
// consumer（worker registry metadata / console publish gate）在 #300 的
// plaita-go 侧落地时直接序列化 CapabilityManifest()。

// ContractVersion is the IR capability contract version carried in the
// manifest (plaita#300: ir_contract_version). Bump when the accept-list or
// normalization semantics change shape in a way consumers must notice.
// 2026-10-22.4 = #84（r84）：sql_query 转正（accept-list 变更，无新 manifest
// 键）（.3 = #80 phase 3：server 扩展节点五类转正 + manifest 新增
// node_dependencies 部署依赖标注键；.2 = #80 phase 2：case/mock/reference
// 转正、calculate/ignore 确认拒载；同日 phase 1 为初始 "2026-10-22"）。
const ContractVersion = "2026-10-22.4"

// EngineName is the manifest engine identifier（console ENGINE_TO_RUNTIME 把
// 它映射到 go runtime 池；写错/漂移会让清单被 console 判为无效条目、能力门
// 退回 fail-open——单一来源，registry metadata 的 engine 键同用本常量）。
const EngineName = "plaita-go"

// supportedFeatures names the engine-level capabilities the manifest
// advertises (plaita#300 example set). Sorted.
var supportedFeatures = []string{"checkpoint", "hitl_await", "suspend"}

// SupportedNodeTypes returns the sorted accept-list of node types this
// engine loads. Single source: supportedNodeTypes (ir.go).
func SupportedNodeTypes() []string {
	return sortedKeys(supportedNodeTypes)
}

// UnsupportedNodeTypes returns the sorted list of known-unsupported node
// types (those carrying a targeted rejection hint). Single source:
// knownUnsupported (ir.go).
func UnsupportedNodeTypes() []string {
	out := make([]string, 0, len(knownUnsupported))
	for t := range knownUnsupported {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// nodeDependencies 标注 server 扩展节点（#80 phase 3）的部署期依赖：这五类
// 是挂起型节点，Go worker 只负责「挂起 + 按 {prefix}:{subtype}:queue 派发服
// 务任务 + 经事件总线接收恢复」——真正的等待/监听/审批决策在消费该队列的外
// 部 service（plaita Python 侧 ServiceManager 同族，或控制面手工事件）。缺
// 消费者时节点会一直 suspended（订阅超时兜底除外）。纯计算/动作节点无此依
// 赖，不入表。
var nodeDependencies = map[string]string{
	"delay":         "external delay service consumes {prefix}:delay:queue (Redis ZSET schedule) and publishes the resume event",
	"approval":      "external approval service consumes {prefix}:approval:queue; decisions arrive via a control-plane entry (submit_approval_decision of the plaita service family)",
	"redis_queue":   "external listener service connects to the configured Redis (list/stream/pubsub) and publishes redis_message",
	"kafka_queue":   "external listener service connects to the configured Kafka broker and publishes kafka_message",
	"http_callback": "external HTTP ingress routes callback requests into the callback service entry; registration state lives in Redis",
}

// NodeDependencies returns a sorted copy of the deployment-dependency
// annotations carried in the manifest (additive key, plaita#300 five-key
// shape untouched semantically — consumers reading the five keys are
// unaffected).
func NodeDependencies() map[string]string {
	out := make(map[string]string, len(nodeDependencies))
	for k, v := range nodeDependencies {
		out[k] = v
	}
	return out
}

// Capability is the plaita#300 worker capability manifest（+phase 3 的
// node_dependencies 部署依赖标注键）。
type Capability struct {
	Engine               string            `json:"engine"`
	IRContractVersion    string            `json:"ir_contract_version"`
	SupportedNodeTypes   []string          `json:"supported_node_types"`
	UnsupportedNodeTypes []string          `json:"unsupported_node_types"`
	Features             []string          `json:"features"`
	NodeDependencies     map[string]string `json:"node_dependencies,omitempty"`
}

// CapabilityManifest builds the capability manifest from the live
// accept-list. Never cache: callers after whitelist changes (tests, future
// registry wiring) must observe the current state.
func CapabilityManifest() Capability {
	return Capability{
		Engine:               EngineName,
		IRContractVersion:    ContractVersion,
		SupportedNodeTypes:   SupportedNodeTypes(),
		UnsupportedNodeTypes: UnsupportedNodeTypes(),
		Features:             supportedFeatures,
		NodeDependencies:     NodeDependencies(),
	}
}
