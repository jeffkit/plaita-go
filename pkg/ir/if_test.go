package ir

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// if 节点（plaita decide.py Bool，node_type "if"）的加载契约：
// 部署 IR 形状（codeflow 编译产物）= {"type":"if","condition":{…},"next":…,
// "else_next":…}；结构规则对齐 plaita ir_validate（next/else_next 非空、
// 目标存在、可达性含假分支边）。

// 真/假两路 + $NODE.<if> 结果 + snake else_next 契约拼写。
func TestLoadIf_TrueFalsePaths(t *testing.T) {
	data := []byte(`{
		"flow_id": "if-flow",
		"nodes": [
			{"type": "start", "id": "s", "next": "myif"},
			{"type": "if", "id": "myif",
			 "condition": {"field": "$INPUT.a", "operator": "gt", "value": 5},
			 "next": "e1", "else_next": "e2"},
			{"type": "end", "id": "e1", "resultType": "success", "output": "$NODE.myif"},
			{"type": "end", "id": "e2", "resultType": "success", "output": "small"}
		]
	}`)
	wf, err := Load(data)
	require.NoError(t, err)

	got, err := wf.Execute(t.Context(), map[string]any{"a": 10})
	require.NoError(t, err)
	assert.Equal(t, "e1", got, "命中真分支：$NODE.myif = next 目标")

	got, err = wf.Execute(t.Context(), map[string]any{"a": 1})
	require.NoError(t, err)
	assert.Equal(t, "small", got, "未命中走 else_next")
}

// camel 别名折叠：elseNext 与 snake else_next 同载。
func TestLoadIf_ElseNextCamelAlias(t *testing.T) {
	base := func(elseKey string) []byte {
		return []byte(`{
		"nodes": [
			{"type": "start", "id": "s", "next": "myif"},
			{"type": "if", "id": "myif",
			 "condition": {"field": "$INPUT.v", "operator": "eq", "value": "x"},
			 "next": "hit", "` + elseKey + `": "miss"},
			{"type": "end", "id": "hit", "resultType": "success", "output": "hit"},
			{"type": "end", "id": "miss", "resultType": "success", "output": "miss"}
		]
	}`)
	}
	for _, key := range []string{"else_next", "elseNext"} {
		wf, err := Load(base(key))
		require.NoError(t, err, "%s 拼写必须可载", key)
		got, err := wf.Execute(t.Context(), map[string]any{"v": "y"})
		require.NoError(t, err)
		assert.Equal(t, "miss", got, "%s: 未命中走假分支", key)
	}
}

// condition 组（and/or）与真值算子（A1：空容器 falsy）。
func TestLoadIf_ConditionGroupsAndTruthy(t *testing.T) {
	load := func(t *testing.T, condition string) *workflow.Workflow {
		t.Helper()
		data := []byte(`{
			"nodes": [
				{"type": "start", "id": "s", "next": "myif"},
				{"type": "if", "id": "myif", "condition": ` + condition + `,
				 "next": "hit", "else_next": "miss"},
				{"type": "end", "id": "hit", "resultType": "success", "output": "hit"},
				{"type": "end", "id": "miss", "resultType": "success", "output": "miss"}
			]
		}`)
		wf, err := Load(data)
		require.NoError(t, err)
		return wf
	}
	run := func(t *testing.T, wf *workflow.Workflow, input map[string]any) string {
		t.Helper()
		got, err := wf.Execute(t.Context(), input)
		require.NoError(t, err)
		return got.(string)
	}

	andGroup := `{"relation": "and", "conditions": [
		{"field": "$INPUT.a", "operator": "gt", "value": 5},
		{"field": "$INPUT.b", "operator": "eq", "value": "go"}]}`
	wf := load(t, andGroup)
	assert.Equal(t, "hit", run(t, wf, map[string]any{"a": 10, "b": "go"}))
	assert.Equal(t, "miss", run(t, wf, map[string]any{"a": 10, "b": "rs"}))

	orGroup := `{"relation": "or", "conditions": [
		{"field": "$INPUT.a", "operator": "lt", "value": 5},
		{"field": "$INPUT.b", "operator": "eq", "value": "go"}]}`
	wf = load(t, orGroup)
	assert.Equal(t, "hit", run(t, wf, map[string]any{"a": 1, "b": "rs"}))
	assert.Equal(t, "miss", run(t, wf, map[string]any{"a": 9, "b": "rs"}))

	// `if items:`：空列表 truthy=false（Python bool 真值语义）。
	truthy := `{"field": "$INPUT.items", "operator": "truthy", "value": true}`
	wf = load(t, truthy)
	assert.Equal(t, "miss", run(t, wf, map[string]any{"items": []any{}}))
	assert.Equal(t, "hit", run(t, wf, map[string]any{"items": []any{1}}))

	// 嵌套组：or(and(...), ...)。
	nested := `{"relation": "or", "conditions": [
		{"relation": "and", "conditions": [
			{"field": "$INPUT.a", "operator": "gt", "value": 5},
			{"field": "$INPUT.b", "operator": "eq", "value": "go"}]},
		{"field": "$INPUT.c", "operator": "contains", "value": "ell"}]}`
	wf = load(t, nested)
	assert.Equal(t, "hit", run(t, wf, map[string]any{"a": 10, "b": "go", "c": "xxx"}))
	assert.Equal(t, "hit", run(t, wf, map[string]any{"a": 1, "b": "rs", "c": "hello"}))
	assert.Equal(t, "miss", run(t, wf, map[string]any{"a": 1, "b": "rs", "c": "xxx"}))
}

// condition 缺省：照 Python condition=None 语义恒走假分支。
func TestLoadIf_NoConditionTakesElse(t *testing.T) {
	data := []byte(`{
		"nodes": [
			{"type": "start", "id": "s", "next": "myif"},
			{"type": "if", "id": "myif", "next": "hit", "else_next": "miss"},
			{"type": "end", "id": "hit", "resultType": "success", "output": "hit"},
			{"type": "end", "id": "miss", "resultType": "success", "output": "miss"}
		]
	}`)
	wf, err := Load(data)
	require.NoError(t, err)
	got, err := wf.Execute(t.Context(), map[string]any{})
	require.NoError(t, err)
	assert.Equal(t, "miss", got)
}

// 结构拒绝（fail-closed）：缺真/假分支目标、悬空目标、残缺 condition。
func TestLoadIf_RejectsBrokenStructures(t *testing.T) {
	cases := []struct {
		name  string
		ifDoc string
		want  string
	}{
		{
			name:  "missing next",
			ifDoc: `{"type": "if", "id": "myif", "else_next": "e2"}`,
			want:  "true-branch target",
		},
		{
			name:  "missing else_next",
			ifDoc: `{"type": "if", "id": "myif", "next": "e1"}`,
			want:  "false-branch target",
		},
		{
			name:  "dangling else target",
			ifDoc: `{"type": "if", "id": "myif", "next": "e1", "else_next": "ghost"}`,
			want:  `references missing node "ghost"`,
		},
		{
			name:  "malformed empty condition",
			ifDoc: `{"type": "if", "id": "myif", "condition": {}, "next": "e1", "else_next": "e2"}`,
			want:  "condition is malformed",
		},
		{
			name:  "condition missing value key",
			ifDoc: `{"type": "if", "id": "myif", "condition": {"field": "$INPUT.a", "operator": "gt"}, "next": "e1", "else_next": "e2"}`,
			want:  "condition is malformed",
		},
		{
			name:  "empty condition group",
			ifDoc: `{"type": "if", "id": "myif", "condition": {"relation": "and", "conditions": []}, "next": "e1", "else_next": "e2"}`,
			want:  "condition is malformed",
		},
		{
			name:  "condition not an object",
			ifDoc: `{"type": "if", "id": "myif", "condition": "$INPUT.a", "next": "e1", "else_next": "e2"}`,
			want:  "condition must be an object",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := []byte(`{
				"nodes": [
					{"type": "start", "id": "s", "next": "myif"},
					` + tc.ifDoc + `,
					{"type": "end", "id": "e1", "resultType": "success", "output": "hit"},
					{"type": "end", "id": "e2", "resultType": "success", "output": "miss"}
				]
			}`)
			_, err := Load(data)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

// 仅从假分支可达的节点不算不可达（ir_validate 可达性边集含 else_next——
// 这是 if 转正后真实 if 流程能加载的前提）。
func TestLoadIf_ElseOnlyReachableNodeLoads(t *testing.T) {
	data := []byte(`{
		"nodes": [
			{"type": "start", "id": "s", "next": "myif"},
			{"type": "if", "id": "myif",
			 "condition": {"field": "$INPUT.v", "operator": "eq", "value": "x"},
			 "next": "e1", "else_next": "e2"},
			{"type": "end", "id": "e1", "resultType": "success", "output": "hit"},
			{"type": "end", "id": "e2", "resultType": "success", "output": "miss"}
		]
	}`)
	// e2 只经 else_next 可达；若边集漏收假分支边，这里会误报 not reachable。
	wf, err := Load(data)
	require.NoError(t, err)
	got, err := wf.Execute(t.Context(), map[string]any{"v": "y"})
	require.NoError(t, err)
	assert.Equal(t, "miss", got)
}

// 契约保持（round-trip）：normalize 不改写 if 的契约键——type 保持 "if"，
// next/else_next/condition 原样保留，camel 别名折叠后 idempotent。
func TestIfNormalizePreservesContractKeys(t *testing.T) {
	doc := map[string]any{
		"nodes": []any{
			map[string]any{"type": "start", "id": "s", "next": "myif"},
			map[string]any{
				"type": "if", "id": "myif",
				"condition": map[string]any{"field": "$INPUT.a", "operator": "gt", "value": json.Number("5")},
				"next":      "e1", "elseNext": "e2",
			},
			map[string]any{"type": "end", "id": "e1", "resultType": "success", "output": "hit"},
			map[string]any{"type": "end", "id": "e2", "resultType": "success", "output": "miss"},
		},
	}
	require.NoError(t, normalizeDoc(doc, scopeRoot, nil))

	ifNode := doc["nodes"].([]any)[1].(map[string]any)
	assert.Equal(t, "if", ifNode["type"], "type 不得被改写成 switch")
	assert.Equal(t, "e1", ifNode["next"])
	assert.Equal(t, "e2", ifNode["else_next"], "elseNext 必须折叠为 else_next")
	assert.NotContains(t, ifNode, "elseNext")
	assert.Equal(t, map[string]any{"field": "$INPUT.a", "operator": "gt", "value": json.Number("5")},
		ifNode["condition"], "condition 对象必须原样保留")

	// 幂等：二次 normalize 不再变化。
	before := len(ifNode)
	require.NoError(t, normalizeDoc(doc, scopeRoot, nil))
	ifNode = doc["nodes"].([]any)[1].(map[string]any)
	assert.Equal(t, before, len(ifNode))
	assert.Equal(t, "e2", ifNode["else_next"])
}

// 与 switch 的等价性（IR 面）：同一判定分别用 if 与二分支 switch 表达，
// 装载执行两路输入输出一致。
func TestLoadIf_EquivalentToTwoBranchSwitch(t *testing.T) {
	ifFlow := []byte(`{
		"nodes": [
			{"type": "start", "id": "s", "next": "d"},
			{"type": "if", "id": "d",
			 "condition": {"field": "$INPUT.a", "operator": "gt", "value": 5},
			 "next": "e1", "else_next": "e2"},
			{"type": "end", "id": "e1", "resultType": "success", "output": "big"},
			{"type": "end", "id": "e2", "resultType": "success", "output": "small"}
		]
	}`)
	switchFlow := []byte(`{
		"nodes": [
			{"type": "start", "id": "s", "next": "d"},
			{"type": "switch", "id": "d", "branches": [
				{"name": "then", "condition": {"field": "$INPUT.a", "operator": "gt", "value": 5}, "next": "e1"},
				{"name": "else", "isDefault": true, "next": "e2"}
			]},
			{"type": "end", "id": "e1", "resultType": "success", "output": "big"},
			{"type": "end", "id": "e2", "resultType": "success", "output": "small"}
		]
	}`)
	ifWf, err := Load(ifFlow)
	require.NoError(t, err)
	swWf, err := Load(switchFlow)
	require.NoError(t, err)

	for _, input := range []map[string]any{{"a": 10}, {"a": 1}, {"a": 5}} {
		ifGot, err := ifWf.Execute(t.Context(), input)
		require.NoError(t, err)
		swGot, err := swWf.Execute(t.Context(), input)
		require.NoError(t, err)
		assert.Equal(t, swGot, ifGot, "input %v: if 与二分支 switch 必须等价", input)
	}
}
