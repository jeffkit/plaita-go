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
const ContractVersion = "2026-10-22"

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

// Capability is the plaita#300 worker capability manifest.
type Capability struct {
	Engine               string   `json:"engine"`
	IRContractVersion    string   `json:"ir_contract_version"`
	SupportedNodeTypes   []string `json:"supported_node_types"`
	UnsupportedNodeTypes []string `json:"unsupported_node_types"`
	Features             []string `json:"features"`
}

// CapabilityManifest builds the capability manifest from the live
// accept-list. Never cache: callers after whitelist changes (tests, future
// registry wiring) must observe the current state.
func CapabilityManifest() Capability {
	return Capability{
		Engine:               "plaita-go",
		IRContractVersion:    ContractVersion,
		SupportedNodeTypes:   SupportedNodeTypes(),
		UnsupportedNodeTypes: UnsupportedNodeTypes(),
		Features:             supportedFeatures,
	}
}
