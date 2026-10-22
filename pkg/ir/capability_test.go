package ir

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/jeffkit/plaita-go/pkg/workflow"
	"github.com/jeffkit/plaita-go/pkg/workflow/nodes"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// manifest 的唯一真源纪律（plaita#300 硬性要求）：导出清单必须与白名单逐项
// 相等，不允许手写第二份。
func TestCapabilityManifestMatchesWhitelist(t *testing.T) {
	cap := CapabilityManifest()

	assert.Equal(t, sortedKeys(supportedNodeTypes), cap.SupportedNodeTypes)
	assert.Equal(t, sortedKeys(supportedNodeTypes), SupportedNodeTypes())

	unsupported := UnsupportedNodeTypes()
	require.Len(t, unsupported, len(knownUnsupported))
	assert.Equal(t, cap.UnsupportedNodeTypes, unsupported)
	for _, t2 := range unsupported {
		assert.Contains(t, knownUnsupported, t2)
	}

	// 两个清单互斥：一个类型不可能既支持又有拒绝提示
	banned := map[string]bool{}
	for _, t2 := range cap.UnsupportedNodeTypes {
		banned[t2] = true
	}
	for _, t2 := range cap.SupportedNodeTypes {
		assert.False(t, banned[t2], "type %q is both supported and unsupported", t2)
	}
}

// 白名单里的每个类型都必须有可执行实现（注册过的节点工厂），否则清单在
// 对外宣称一个跑不了的能力。
func TestSupportedTypesAreRegistered(t *testing.T) {
	for _, nodeType := range SupportedNodeTypes() {
		require.NotNil(t, workflow.GetNodeFactory(workflow.NodeType(nodeType)),
			"supported node type %q has no registered factory", nodeType)
	}
}

// #80 phase 1 负控：五个集合/循环节点必须在 supported 清单里、且不在
// unsupported 里（pre-fix 基线它们在 knownUnsupported，此处双红）。
func TestCapabilityIncludesCollectionNodes(t *testing.T) {
	cap := CapabilityManifest()
	supported := map[string]bool{}
	for _, t2 := range cap.SupportedNodeTypes {
		supported[t2] = true
	}
	unsupported := map[string]bool{}
	for _, t2 := range cap.UnsupportedNodeTypes {
		unsupported[t2] = true
	}
	for _, t2 := range []string{"map", "filter", "find", "reduce", "while"} {
		assert.True(t, supported[t2], "%q must be in supported_node_types", t2)
		assert.False(t, unsupported[t2], "%q must not be in unsupported_node_types", t2)
	}
	// phase 2 余量仍在 unsupported（拒绝提示不丢）
	for _, t2 := range []string{"case", "calculate", "mock", "reference"} {
		assert.Contains(t, cap.UnsupportedNodeTypes, t2)
	}
}

// manifest JSON 形状按 plaita#300：engine / ir_contract_version /
// supported_node_types / unsupported_node_types / features。
func TestCapabilityManifestJSONShape(t *testing.T) {
	data, err := json.Marshal(CapabilityManifest())
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(data, &m))
	assert.Equal(t, "plaita-go", m["engine"])
	assert.Equal(t, ContractVersion, m["ir_contract_version"])
	for _, key := range []string{"supported_node_types", "unsupported_node_types", "features"} {
		require.Contains(t, m, key)
		assert.IsType(t, []any{}, m[key], key)
	}
	assert.Len(t, m["features"], 3)

	// 与 #300 issue 文本的结构对拍：仅这五个键
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	assert.ElementsMatch(t, []string{
		"engine", "ir_contract_version", "supported_node_types",
		"unsupported_node_types", "features",
	}, keys)
}

// 加载冒烟：五个转正类型各自的最小 flow 必须能 Load（pre-fix 基线在
// knownUnsupported，这里全红）。
func TestLoadAcceptsCollectionNodes(t *testing.T) {
	collectionFlow := func(nodeType string, extra string) string {
		return fmt.Sprintf(`{
			"nodes": [
				{"type": "start", "id": "s", "next": "c"},
				{"type": %q, "id": "c", "collection": "$INPUT.items", %s
				 "childFlow": {"nodes": [
					{"type": "start", "id": "start", "next": "e"},
					{"type": "end", "id": "e", "resultType": "success", "output": "$INPUT.item"}
				 ]},
				 "next": "out"},
				{"type": "end", "id": "out", "resultType": "success", "output": "$NODE.c"}
			]
		}`, nodeType, extra)
	}
	for _, tc := range []struct{ nodeType, extra string }{
		{"loop", ""},
		{"map", ""},
		{"filter", ""},
		{"find", ""},
		{"reduce", `"initial": 0,`},
		{"while", `"condition": {"field": "$LOOP-INDEX", "operator": "lt", "value": 3},`},
	} {
		t.Run(tc.nodeType, func(t *testing.T) {
			_, err := Load([]byte(collectionFlow(tc.nodeType, tc.extra)))
			require.NoError(t, err)
		})
	}
}

// while 的 snake/camel 双拼写（编译器产 snake、console 正典产 camel，#89）。
func TestLoadWhileDualSpellings(t *testing.T) {
	base := func(childKey, maxKey string) string {
		return fmt.Sprintf(`{
			"nodes": [
				{"type": "start", "id": "s", "next": "w"},
				{"type": "while", "id": "w",
				 "condition": {"field": "$LOOP-INDEX", "operator": "lt", "value": 2},
				 %q: {"nodes": [
					{"type": "start", "id": "start", "next": "e"},
					{"type": "end", "id": "e", "resultType": "success", "output": "$INPUT.index"}
				 ]},
				 %q: 2,
				 "next": "out"},
				{"type": "end", "id": "out", "resultType": "success", "output": "$NODE.w"}
			]
		}`, childKey, maxKey)
	}
	for _, keys := range [][2]string{{"child_flow", "max_iterations"}, {"childFlow", "maxIterations"}} {
		t.Run(keys[0]+"/"+keys[1], func(t *testing.T) {
			wf, err := Load([]byte(base(keys[0], keys[1])))
			require.NoError(t, err)
			got, err := wf.Execute(t.Context(), map[string]any{})
			require.NoError(t, err)
			assert.Equal(t, 1, got) // 轮次 0、1 → 末轮 index=1
		})
	}
}

// map 的历史别名 async 折叠为 concurrent（Python _normalize_concurrent 同型）；
// 折叠不可通过输出观察（并发与否结果同形），直接断言加载后的节点字段。
func TestLoadMapAsyncAliasFoldsToConcurrent(t *testing.T) {
	data := `{"nodes":[
		{"type":"start","id":"s","next":"m"},
		{"type":"map","id":"m","collection":"$INPUT.items","async":true,"maxConcurrent":2,
		 "childFlow":{"nodes":[{"type":"start","id":"start","next":"e"},
		 	{"type":"end","id":"e","resultType":"success","output":"$INPUT.item"}]},
		 "next":"o"},
		{"type":"end","id":"o","resultType":"success","output":"$NODE.m"}
	]}`
	wf, err := Load([]byte(data))
	require.NoError(t, err)
	for _, n := range wf.Nodes {
		if n.GetID() == "m" {
			m, ok := n.(*nodes.Map)
			require.True(t, ok, "map node type: %T", n)
			assert.True(t, m.Concurrent, "async must fold into concurrent")
			require.NotNil(t, m.MaxConcurrent)
			assert.Equal(t, 2, *m.MaxConcurrent)
		}
	}
}

// max_iterations ≤0 合法 = 零轮（Python `index < max_iterations` 不进循环，
// 结果 None——语义有定义，不拒载；非整数才拒）。
func TestLoadWhileZeroMaxIterations(t *testing.T) {
	data := `{"nodes":[
		{"type":"start","id":"s","next":"w"},
		{"type":"while","id":"w","condition":{"field":"$INPUT.on","operator":"eq","value":true},
		 "max_iterations":0,"next":"o",
		 "childFlow":{"nodes":[{"type":"start","id":"start","next":"e"},{"type":"end","id":"e","resultType":"success","output":"ran"}]}},
		{"type":"end","id":"o","resultType":"success","output":"$NODE.w"}
	]}`
	wf, err := Load([]byte(data))
	require.NoError(t, err)
	got, err := wf.Execute(t.Context(), map[string]any{"on": true})
	require.NoError(t, err)
	assert.Nil(t, got)
}

// while 的缺省 childFlow / 畸形 condition / 负 maxConcurrent 加载期拒绝
// （fail-closed 前置，不给执行期炸点）。
func TestLoadRejectsMalformedCollectionNodes(t *testing.T) {
	cases := []struct {
		name string
		doc  string
		want string
	}{
		{
			name: "map without childFlow",
			doc:  `{"nodes":[{"type":"start","id":"s","next":"c"},{"type":"map","id":"c","collection":"$INPUT.items","next":"o"},{"type":"end","id":"o"}]}`,
			want: "requires an inline childFlow",
		},
		{
			name: "while with malformed condition",
			doc:  `{"nodes":[{"type":"start","id":"s","next":"c"},{"type":"while","id":"c","condition":{"field":"$LOOP-INDEX"},"childFlow":{"nodes":[{"type":"start","id":"start","next":"e"},{"type":"end","id":"e"}]}}]}`,
			want: "malformed",
		},
		{
			name: "while with fractional max_iterations",
			doc:  `{"nodes":[{"type":"start","id":"s","next":"c"},{"type":"while","id":"c","max_iterations":2.5,"childFlow":{"nodes":[{"type":"start","id":"start","next":"e"},{"type":"end","id":"e"}]}}]}`,
			want: "integer",
		},
		{
			name: "map with negative maxConcurrent",
			doc:  `{"nodes":[{"type":"start","id":"s","next":"c"},{"type":"map","id":"c","collection":"$INPUT.items","maxConcurrent":-2,"childFlow":{"nodes":[{"type":"start","id":"start","next":"e"},{"type":"end","id":"e"}]}}]}`,
			want: "maxConcurrent",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load([]byte(tc.doc))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}
