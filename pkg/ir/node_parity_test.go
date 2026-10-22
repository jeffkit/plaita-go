package ir

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
)

// The Python↔Go node parity gate (#80 需求 4)：Go 的 accept-list 与 Python
// 权威清单（scripts/extract_python_node_types.py 从 plaita NodeRegistry 现场
// 提取）的差集必须与下面的显式例外表完全一致——任何一侧单方面新增/删除节点
// 而不改表，本测试红。例外条目过期（差集里已没有它）同样红，逼表格与事实
// 一致：Go 侧补齐某类型后必须同步摘条。
//
// 跳过语义：PLAITA_NODE_TYPES_JSON 未设时 skip——普通 `go test ./...` 没有
// plaita checkout，不该红；`make node-parity` / CI 的 parity 步骤负责设置。
func TestNodeParityWithPython(t *testing.T) {
	path := os.Getenv("PLAITA_NODE_TYPES_JSON")
	if path == "" {
		t.Skip("PLAITA_NODE_TYPES_JSON not set; run `make node-parity` (extracts the " +
			"plaita NodeRegistry list and points this test at it)")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read PLAITA_NODE_TYPES_JSON: %v", err)
	}
	var doc struct {
		Sources     map[string]string `json:"sources"`
		EntryPoints []string          `json:"entry_points"`
		NodeTypes   []string          `json:"node_types"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	if len(doc.NodeTypes) == 0 {
		t.Fatalf("authority list in %s is empty — extractor output is not a real registry dump", path)
	}

	python := make(map[string]bool, len(doc.NodeTypes))
	for _, nt := range doc.NodeTypes {
		python[nt] = true
	}
	goTypes := SupportedNodeTypes()
	goSet := make(map[string]bool, len(goTypes))
	for _, nt := range goTypes {
		goSet[nt] = true
	}

	var pythonOnly, goOnly []string
	for nt := range python {
		if !goSet[nt] {
			pythonOnly = append(pythonOnly, nt)
		}
	}
	for _, nt := range goTypes {
		if !python[nt] {
			goOnly = append(goOnly, nt)
		}
	}
	slices.Sort(pythonOnly)
	slices.Sort(goOnly)

	// python-only 例外：Python 有、Go 有意不实现，条目写明去向/理由。
	// sql_query 曾在此（deferred to #84）——#84 已落地（2026-10-22，r84，
	// database/sql + 纯 Go 驱动矩阵），例外同步摘除，双向差集应自然为零。
	pythonOnlyExceptions := map[string]string{}
	// go-only 例外：Go 有意实装、Python 默认 registry 不暴露的类型。两者都是
	// Python 侧「模块在、注册表外」的 opt-in 形态（plaita/node/__init__.py）。
	goOnlyExceptions := map[string]string{
		// CodeNode 执行任意用户代码，Python 默认 registry 刻意排除、须
		// register_code_node() 显式 opt-in；Go 侧是受限 JS（语言白名单
		// fail-closed，plaita#29），语义不等价于 Python 的裸 exec。
		"code": "Python keeps CodeNode out of the default registry (sandbox opt-in " +
			"via register_code_node); Go ships the restricted-JS node with a " +
			"fail-closed language whitelist (plaita#29)",
		// plaita/node/redis.py 的 RedisNode 未进 _BUILTIN_NODES 也未声明 entry
		// point（要用需显式注册）；Go 把它作为历史 33 类内建常驻，#80 未要求
		// 移除。
		"redis": "Python's RedisNode (plaita/node/redis.py) is not auto-registered " +
			"(absent from _BUILTIN_NODES and entry points); Go ships it as a builtin " +
			"of the historical 33-type set, which #80 does not ask to remove",
	}

	var problems []string
	for _, nt := range pythonOnly {
		reason, ok := pythonOnlyExceptions[nt]
		if !ok {
			problems = append(problems, fmt.Sprintf(
				"python-only node %q is not in the exception table — implement it in Go "+
					"(pkg/ir/ir.go supportedNodeTypes + node + golden + AGENTS.md matrix) "+
					"or add an explicit exception entry with a reason", nt))
		} else {
			t.Logf("python-only exception applied: %q — %s", nt, reason)
			delete(pythonOnlyExceptions, nt)
		}
	}
	for _, nt := range goOnly {
		reason, ok := goOnlyExceptions[nt]
		if !ok {
			problems = append(problems, fmt.Sprintf(
				"go-only node %q is not in the exception table — Python's authoritative "+
					"registry does not expose it; either remove it from Go's accept-list "+
					"or add an explicit exception entry with a reason", nt))
		} else {
			t.Logf("go-only exception applied: %q — %s", nt, reason)
			delete(goOnlyExceptions, nt)
		}
	}
	// 过期例外条目：差集已不含它 = 表格与事实漂移（通常是 Go 侧补齐了该类型）。
	for nt := range pythonOnlyExceptions {
		problems = append(problems, fmt.Sprintf(
			"stale python-only exception %q: it no longer differs (Go implements it or "+
				"Python dropped it) — remove the entry to keep the table honest", nt))
	}
	for nt := range goOnlyExceptions {
		problems = append(problems, fmt.Sprintf(
			"stale go-only exception %q: it no longer differs — remove the entry", nt))
	}

	if len(problems) > 0 {
		slices.Sort(problems)
		t.Fatalf("Python<->Go node parity drifted (authority sources %v, entry points %d):\n  %s\n"+
			"authority list: %s", doc.Sources, len(doc.EntryPoints),
			strings.Join(problems, "\n  "), strings.Join(doc.NodeTypes, ", "))
	}
}
