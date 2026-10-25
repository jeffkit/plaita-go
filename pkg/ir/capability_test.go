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

// #80 负控：phase 1 集合/循环五节点、phase 2 的 case/mock/reference、phase 3
// 的 server 扩展节点五类都必须在 supported 清单里、且不在 unsupported 里
// （pre-fix 基线它们不在 accept-list，此处红）。calculate/ignore 不是 plaita
// 注册节点类型（r78 实测：Flow.model_validate 报 unRecognized node type）——
// capability 如实标注 python 侧同样拒载，留痕见 issue #80。
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
	for _, t2 := range []string{"map", "filter", "find", "reduce", "while", "case", "mock", "reference",
		"delay", "approval", "redis_queue", "kafka_queue", "http_callback"} {
		assert.True(t, supported[t2], "%q must be in supported_node_types", t2)
		assert.False(t, unsupported[t2], "%q must not be in unsupported_node_types", t2)
	}
	// 未注册类型与前身引擎遗产留在 unsupported（拒绝提示不丢）。
	for _, t2 := range []string{"calculate", "ignore", "sleep", "trpc"} {
		assert.Contains(t, cap.UnsupportedNodeTypes, t2)
	}
}

// phase 3：server 扩展节点的部署依赖标注——挂起型节点挂起后由外部 service
// 消费 {prefix}:{subtype}:queue 并发布恢复事件；无消费者则执行停留 suspended。
// 只有这五类入表，纯计算/动作节点不得出现。
func TestCapabilityNodeDependencies(t *testing.T) {
	deps := NodeDependencies()
	assert.ElementsMatch(t,
		[]string{"delay", "approval", "redis_queue", "kafka_queue", "http_callback"},
		mapKeys(deps))
	for _, dep := range deps {
		assert.NotEmpty(t, dep)
	}
	cap := CapabilityManifest()
	assert.Equal(t, deps, cap.NodeDependencies)
}

func mapKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// manifest JSON 形状按 plaita#300：engine / ir_contract_version /
// supported_node_types / unsupported_node_types / features；#80 phase 3 起
// 增设第六键 node_dependencies（部署依赖标注，additive——只读前五键的
// consumer 不受影响，ContractVersion 已随 bump）。
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
	require.Contains(t, m, "node_dependencies")
	assert.IsType(t, map[string]any{}, m["node_dependencies"])

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	assert.ElementsMatch(t, []string{
		"engine", "ir_contract_version", "supported_node_types",
		"unsupported_node_types", "features", "node_dependencies",
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

// ---- 执行台阶对拍（2026-10-25 两级终版，plaita fb36f085 配套） ----

// TestExecutionTierParityWithPythonLedger：Go 侧两级分类与 Python 账本锚逐项
// 相等。锚取自 plaita/docs/governance/architecture.yaml gov.capability_matrix
// （值=两值台阶字符串；导出/对账 scripts/export_capability_matrix.py --json，
// nightly 由 ci-slow 对账）。Go=frozen：本测试是只读对拍，不改引擎行为——
// Python 侧改声明 → 改本测试的锚并跑 ledger --check，两侧同 PR。
func TestExecutionTierParityWithPythonLedger(t *testing.T) {
	// Python 账本 managed 面（2026-10-25 实况）：七个挂起节点。
	managed := map[string]bool{
		"event": true, "hitl_await": true,
		"delay": true, "approval": true, "http_callback": true,
		"redis_queue": true, "kafka_queue": true,
	}
	// Python 账本 inline 锚（控制流/计算/出网代表）。
	inline := map[string]bool{
		"start": true, "end": true, "assignment": true, "case": true,
		"calculate": true, "http": true, "parallel": true,
		"loop": true, "child": true, "code": true,
	}
	for nodeType := range managed {
		assert.Equal(t, ExecutionTierManaged, ExecutionTier(nodeType),
			"node %q should be managed per python ledger", nodeType)
	}
	for nodeType := range inline {
		// code 在 Go 侧是拒载（knownUnsupported），工厂缺席 → inline（未知
		// 类型同 inline 的口径）；其余必须是真 inline。
		assert.Equal(t, ExecutionTierInline, ExecutionTier(nodeType),
			"node %q should be inline per python ledger", nodeType)
	}
}

// TestWakeServiceNodeTypesMatchNodeDependencies：唤醒源集合单源 =
// nodeDependencies keys——与 Python WAKE_SERVICE_NODE_TYPES / 节点 wake_service
// 字段互为对拍锚（五族：delay/approval/http_callback/redis_queue/kafka_queue）。
func TestWakeServiceNodeTypesMatchNodeDependencies(t *testing.T) {
	wake := WakeServiceNodeTypes()
	deps := NodeDependencies()
	require.Len(t, wake, len(deps))
	expected := []string{"approval", "delay", "http_callback", "kafka_queue", "redis_queue"}
	assert.Equal(t, expected, wake)
	for _, k := range expected {
		assert.Contains(t, deps, k)
		// 唤醒源节点必是 managed（台阶与接线自洽）
		assert.Equal(t, ExecutionTierManaged, ExecutionTier(k))
	}
}

// TestExecutionTierCoversAllSupportedTypes：supported 白名单每个类型都能归入
// 恰好一级（词汇全覆盖，无第三态）。
func TestExecutionTierCoversAllSupportedTypes(t *testing.T) {
	for _, nodeType := range SupportedNodeTypes() {
		tier := ExecutionTier(nodeType)
		assert.True(t, tier == ExecutionTierInline || tier == ExecutionTierManaged,
			"node %q returned unknown tier %q", nodeType, tier)
	}
}
