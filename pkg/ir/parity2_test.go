// #80 phase 2（r78）加载面负控与语义测试：case 脱糖、mock 转正、reference
// 的 provider 解析/循环防护/inline 拒绝、calculate/ignore 的拒载对拍结论。
// Python 权威语义的探针记录见 scripts/export_node_parity2_golden.py 头注释与
// AGENTS.md「节点对齐 phase 2」节。

package ir

import (
	"fmt"
	"testing"

	"github.com/jeffkit/plaita-go/pkg/workflow/nodes"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mapProvider(reg map[string]string) FlowProvider {
	return providerFunc(func(flowID, _ string) ([]byte, error) {
		raw, ok := reg[flowID]
		if !ok {
			return nil, fmt.Errorf("flow %q not found", flowID)
		}
		return []byte(raw), nil
	})
}

type providerFunc func(flowID, flowVersion string) ([]byte, error)

func (f providerFunc) LoadFlowJSON(flowID, flowVersion string) ([]byte, error) {
	return f(flowID, flowVersion)
}

// case 在 Python 里是模型校验期脱糖成 Switch 的等值匹配方言（decide.py
// SwitchLegacy）；Go 在 IR 归一化层做同款脱糖：加载后节点就是 *Switch，分支
// 形状逐项对齐 Python 构造的 Branch（name/next=case.id、condition eq on
// target、isDefault 兜底、label=name）。
func TestLoadCaseDesugarsToSwitch(t *testing.T) {
	data := `{"nodes":[
		{"type":"start","id":"s","next":"c"},
		{"type":"case","id":"c","target":"$INPUT.x",
		 "cases":[{"id":"hit","value":1,"name":"Hit"},{"id":"other","value":"$INPUT.expect"}],
		 "default":"fb","next":"o"},
		{"type":"assignment","id":"hit","output":"H","next":"o"},
		{"type":"assignment","id":"other","output":"O","next":"o"},
		{"type":"assignment","id":"fb","output":"F","next":"o"},
		{"type":"end","id":"o","resultType":"success","output":"$NODE.c"}
	]}`
	wf, err := Load([]byte(data))
	require.NoError(t, err)

	var sw *nodes.Switch
	for _, n := range wf.Nodes {
		if n.GetID() == "c" {
			s, ok := n.(*nodes.Switch)
			require.True(t, ok, "case must load as *Switch, got %T", n)
			sw = s
		}
	}
	require.NotNil(t, sw)
	require.Len(t, sw.Branches, 3)

	hit := sw.Branches[0]
	assert.Equal(t, "hit", hit.Name)
	assert.Equal(t, "hit", string(hit.Next))
	assert.False(t, hit.IsDefault)
	assert.Equal(t, "Hit", hit.Label)
	require.NotNil(t, hit.Condition)
	assert.Equal(t, "eq", string(hit.Condition.LogicCondition.Operator))
	assert.Equal(t, "$INPUT.x", hit.Condition.LogicCondition.Field)
	assert.Equal(t, float64(1), hit.Condition.LogicCondition.Value)

	other := sw.Branches[1]
	assert.Equal(t, "other", other.Name)
	assert.Equal(t, "$INPUT.expect", other.Condition.LogicCondition.Value)

	fb := sw.Branches[2]
	assert.True(t, fb.IsDefault)
	assert.Equal(t, "fb", string(fb.Next))
	assert.Equal(t, "default", fb.Name)

	// metadata 留痕：方言脱糖可观察，不静默。
	// （节点 metadata 经引擎 unmarshal 不可直接读，这里只钉行为面。）
	got, err := wf.Execute(t.Context(), map[string]any{"x": 1, "expect": 1})
	require.NoError(t, err)
	assert.Equal(t, "hit", got)
	got, err = wf.Execute(t.Context(), map[string]any{"x": "zzz", "expect": 1})
	require.NoError(t, err)
	assert.Equal(t, "fb", got)
}

// default 键缺省 → 字面 "default" 节点 id（Python values.get("default",
// "default") + branch_name_as_target 的合成目标）。
func TestCaseDefaultImplicitNode(t *testing.T) {
	data := `{"nodes":[
		{"type":"start","id":"s","next":"c"},
		{"type":"case","id":"c","target":"$INPUT.x","cases":[{"id":"hit","value":"a"}]},
		{"type":"assignment","id":"hit","output":"H","next":"o"},
		{"type":"assignment","id":"default","output":"D","next":"o"},
		{"type":"end","id":"o","resultType":"success","output":"$NODE.c"}
	]}`
	wf, err := Load([]byte(data))
	require.NoError(t, err)
	got, err := wf.Execute(t.Context(), map[string]any{"x": "zzz"})
	require.NoError(t, err)
	assert.Equal(t, "default", got)
}

// target 缺省（Python field=None）→ 恒 None，只有 value:null 的 case 能命中；
// desugar 以引擎的 $NULL 字面量承载（evaluate(None) → None 的等价形）。
func TestCaseAbsentTargetNoneSemantics(t *testing.T) {
	data := `{"nodes":[
		{"type":"start","id":"s","next":"c"},
		{"type":"case","id":"c","cases":[{"id":"hit","value":null},{"id":"other","value":1}],"default":"fb"},
		{"type":"assignment","id":"hit","output":"H","next":"o"},
		{"type":"assignment","id":"other","output":"O","next":"o"},
		{"type":"assignment","id":"fb","output":"F","next":"o"},
		{"type":"end","id":"o","resultType":"success","output":"$NODE.c"}
	]}`
	wf, err := Load([]byte(data))
	require.NoError(t, err)
	got, err := wf.Execute(t.Context(), map[string]any{})
	require.NoError(t, err)
	assert.Equal(t, "hit", got)
}

// 畸形 case 形状加载期拒绝（Python 模型层 ValueError 前置）。
func TestLoadRejectsMalformedCase(t *testing.T) {
	cases := []struct {
		name string
		doc  string
		want string
	}{
		{
			name: "case entry without id",
			doc:  `{"nodes":[{"type":"start","id":"s","next":"c"},{"type":"case","id":"c","target":"$INPUT.x","cases":[{"value":1}],"default":"fb"},{"type":"assignment","id":"fb","next":"o"},{"type":"end","id":"o"}]}`,
			want: `"id"`,
		},
		{
			name: "case entry without value",
			doc:  `{"nodes":[{"type":"start","id":"s","next":"c"},{"type":"case","id":"c","target":"$INPUT.x","cases":[{"id":"hit"}],"default":"fb"},{"type":"assignment","id":"fb","next":"o"},{"type":"end","id":"o"}]}`,
			want: `"value"`,
		},
		{
			name: "non-string target",
			doc:  `{"nodes":[{"type":"start","id":"s","next":"c"},{"type":"case","id":"c","target":42,"cases":[{"id":"hit","value":1}],"default":"fb"},{"type":"assignment","id":"fb","next":"o"},{"type":"end","id":"o"}]}`,
			want: "expression string",
		},
		{
			name: "cases not a list",
			doc:  `{"nodes":[{"type":"start","id":"s","next":"c"},{"type":"case","id":"c","target":"$INPUT.x","cases":{"id":"hit"},"default":"fb"},{"type":"assignment","id":"fb","next":"o"},{"type":"end","id":"o"}]}`,
			want: "must be an array",
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

// mock 转正：加载冒烟 + manifest 纳入由 capability_test 钉。
func TestLoadAcceptsMock(t *testing.T) {
	data := `{"nodes":[
		{"type":"start","id":"s","next":"m"},
		{"type":"mock","id":"m","value":{"pin":true},"next":"o"},
		{"type":"end","id":"o","resultType":"success","output":"$NODE.m"}
	]}`
	wf, err := Load([]byte(data))
	require.NoError(t, err)
	got, err := wf.Execute(t.Context(), map[string]any{})
	require.NoError(t, err)
	gotJSON, err := marshalCanonical(got)
	require.NoError(t, err)
	assert.JSONEq(t, `{"pin":true}`, gotJSON)
}

// reference：provider 解析在加载期完成（调度器注入的 Go 对应物），注入的
// 子流程走 scopeChild 归一化+校验；flowId/camel 与 snake 双拼写都收。
func TestLoadReferenceWithProvider(t *testing.T) {
	reg := map[string]string{
		"sub": `{"flow_id":"sub","nodes":[
			{"type":"start","id":"a","next":"b"},
			{"type":"end","id":"b","resultType":"success","output":"$F.concat($INPUT.p, \"-sub\")"}
		]}`,
	}
	data := `{"nodes":[
		{"type":"start","id":"s","next":"r"},
		{"type":"reference","id":"r","input":{"p":"$INPUT.name"},"flow_id":"sub","next":"o"},
		{"type":"end","id":"o","resultType":"success","output":"$NODE.r"}
	]}`
	wf, err := LoadWithFlowProvider([]byte(data), mapProvider(reg))
	require.NoError(t, err)
	got, err := wf.Execute(t.Context(), map[string]any{"name": "kong"})
	require.NoError(t, err)
	assert.Equal(t, "kong-sub", got)
}

// 无 provider 时 reference 具名拒绝（fail-closed，不给执行期炸点）。
func TestLoadReferenceWithoutProviderRejected(t *testing.T) {
	data := `{"nodes":[
		{"type":"start","id":"s","next":"r"},
		{"type":"reference","id":"r","input":"$INPUT","flowId":"ghost","next":"o"},
		{"type":"end","id":"o","resultType":"success","output":"$NODE.r"}
	]}`
	_, err := Load([]byte(data))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "flow provider")
}

// provider 找不到 flow → 加载失败（fail-closed，不静默降级为空节点）。
func TestLoadReferenceProviderMiss(t *testing.T) {
	data := `{"nodes":[
		{"type":"start","id":"s","next":"r"},
		{"type":"reference","id":"r","flow_id":"ghost","next":"o"},
		{"type":"end","id":"o"}
	]}`
	_, err := LoadWithFlowProvider([]byte(data), mapProvider(map[string]string{}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ghost")
}

// 循环引用（A→B→A）在深度上限处具名拒绝；菱形（A→B,A→C,B→D,C→D）不受
// 误伤——深度计数随子树闭合归还。
func TestLoadReferenceCycleAndDiamond(t *testing.T) {
	aFlow := `{"flow_id":"a","nodes":[
		{"type":"start","id":"s","next":"r"},
		{"type":"reference","id":"r","flow_id":"b","next":"o"},
		{"type":"end","id":"o","resultType":"success","output":"A"}
	]}`
	bFlow := `{"flow_id":"b","nodes":[
		{"type":"start","id":"s","next":"r"},
		{"type":"reference","id":"r","flow_id":"a","next":"o"},
		{"type":"end","id":"o","resultType":"success","output":"B"}
	]}`
	_, err := LoadWithFlowProvider([]byte(aFlow), mapProvider(map[string]string{"a": aFlow, "b": bFlow}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reference")

	diamond := map[string]string{
		"root": `{"flow_id":"root","nodes":[
			{"type":"start","id":"s","next":"r"},
			{"type":"reference","id":"r","flow_id":"b1","next":"o"},
			{"type":"end","id":"o","resultType":"success","output":"R"}
		]}`,
		"b1": `{"flow_id":"b1","nodes":[
			{"type":"start","id":"s","next":"r"},
			{"type":"reference","id":"r","flow_id":"leaf","next":"o"},
			{"type":"end","id":"o","resultType":"success","output":"B1"}
		]}`,
		"b2": `{"flow_id":"b2","nodes":[
			{"type":"start","id":"s","next":"r"},
			{"type":"reference","id":"r","flow_id":"leaf","next":"o"},
			{"type":"end","id":"o","resultType":"success","output":"B2"}
		]}`,
		"leaf": `{"flow_id":"leaf","nodes":[
			{"type":"start","id":"s","next":"o"},
			{"type":"end","id":"o","resultType":"success","output":"LEAF"}
		]}`,
	}
	// root→b1（→leaf）与 b2（→leaf）：leaf 解析两次，不是环。
	rootFlow := `{"flow_id":"root","nodes":[
		{"type":"start","id":"s","next":"r"},
		{"type":"reference","id":"r","flow_id":"b1","next":"r2"},
		{"type":"reference","id":"r2","flow_id":"b2","next":"o"},
		{"type":"end","id":"o","resultType":"success","output":"R"}
	]}`
	_, err = LoadWithFlowProvider([]byte(rootFlow), mapProvider(diamond))
	require.NoError(t, err)
}

// inline childFlow 在 reference 上拒绝：plaita 运行时把它丢掉并在执行期炸
// （validator 同名遮蔽，r78 实测 child_flow=None → assert）；Go 在加载期
// 拒绝并指路（child 节点或 flowId+provider），不给跨运行时假象。
func TestLoadReferenceRejectsInlineChildFlow(t *testing.T) {
	data := `{"nodes":[
		{"type":"start","id":"s","next":"r"},
		{"type":"reference","id":"r","input":"$INPUT","flow_id":"sub",
		 "childFlow":{"nodes":[{"type":"start","id":"a","next":"b"},{"type":"end","id":"b"}]},
		 "next":"o"},
		{"type":"end","id":"o"}
	]}`
	_, err := LoadWithFlowProvider([]byte(data), mapProvider(map[string]string{}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "inline childFlow")
}

// calculate / ignore：plaita 运行时未注册（r78 实测 Flow.model_validate 报
// unRecognized node type），Go 保持拒载——对齐的是运行时行为，不是模块存在性。
func TestLoadRejectsUnregisteredPythonTypes(t *testing.T) {
	for _, tc := range []struct{ nodeType, want string }{
		{"calculate", "unRecognized node type: calculate"},
		{"ignore", "unRecognized node type: ignore"},
	} {
		t.Run(tc.nodeType, func(t *testing.T) {
			data := fmt.Sprintf(`{"nodes":[
				{"type":"start","id":"s","next":"n"},
				{"type":%q,"id":"n","next":"o"},
				{"type":"end","id":"o"}
			]}`, tc.nodeType)
			_, err := Load([]byte(data))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}
