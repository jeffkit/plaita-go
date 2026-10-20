package nodes

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jeffkit/plaita-go/internal/runtime"

	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// 语义权威：plaita/plaita/node/decide.py Bool（node_type "if"，:293-329），
// 执行继承 Switch.execute（:279-290）——setup_branches 展开出的两条固定分支：
// true 分支（condition，priority 0）、false 分支（无 condition，isDefault）。

func ifRuntimeCtx(input map[string]any) context.Context {
	return runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: input,
	})
}

// 条件真/假两路：命中走 next，未命中走 else_next；目标即节点结果
// （$NODE.<if> / $BRANCH 均为它，#6 形态同 Switch）。
func TestIf_Execute_TrueFalsePaths(t *testing.T) {
	newIf := func() *If {
		return &If{
			BaseNode: BaseNode{ID: "myif", Next: "then-node"},
			Condition: &Condition{
				LogicCondition: LogicCondition{Field: "$INPUT.a", Operator: Gt, Value: 5},
			},
			ElseNext: "else-node",
		}
	}

	got, err := newIf().Execute(ifRuntimeCtx(map[string]any{"a": 10}))
	require.NoError(t, err)
	require.Equal(t, workflow.NodeID("then-node"), got.NextNode)
	require.Equal(t, "then-node", got.Result)

	got, err = newIf().Execute(ifRuntimeCtx(map[string]any{"a": 1}))
	require.NoError(t, err)
	require.Equal(t, workflow.NodeID("else-node"), got.NextNode)
	require.Equal(t, "else-node", got.Result)
}

// Python 真值语义：condition 为 None 时真分支被短路（decide.py:283
// `if branch.condition and ...`），恒走假分支——不走 Condition 零值的
// "恒真"回退（那是 loop/switch 无条件的语义）。
func TestIf_Execute_NilConditionTakesElse(t *testing.T) {
	n := &If{
		BaseNode: BaseNode{ID: "myif", Next: "then-node"},
		ElseNext: "else-node",
	}
	got, err := n.Execute(ifRuntimeCtx(map[string]any{"a": 10}))
	require.NoError(t, err)
	require.Equal(t, workflow.NodeID("else-node"), got.NextNode)
}

// condition 组（and/or）。
func TestIf_Execute_ConditionGroups(t *testing.T) {
	newIf := func(group *LogicConditionGroup) *If {
		return &If{
			BaseNode: BaseNode{ID: "myif", Next: "then-node"},
			Condition: &Condition{
				ConditionType:       conditionTypeLogicConditionGroup,
				LogicConditionGroup: *group,
			},
			ElseNext: "else-node",
		}
	}
	input := map[string]any{"a": 10, "b": "xyz"}

	andHit := &LogicConditionGroup{Relation: And, Conditions: []ConditionElement{
		LogicCondition{Field: "$INPUT.a", Operator: Gt, Value: 5},
		LogicCondition{Field: "$INPUT.b", Operator: Eq, Value: "xyz"},
	}}
	got, err := newIf(andHit).Execute(ifRuntimeCtx(input))
	require.NoError(t, err)
	require.Equal(t, workflow.NodeID("then-node"), got.NextNode, "and 组全真 → 真分支")

	andMiss := &LogicConditionGroup{Relation: And, Conditions: []ConditionElement{
		LogicCondition{Field: "$INPUT.a", Operator: Gt, Value: 5},
		LogicCondition{Field: "$INPUT.b", Operator: Eq, Value: "nope"},
	}}
	got, err = newIf(andMiss).Execute(ifRuntimeCtx(input))
	require.NoError(t, err)
	require.Equal(t, workflow.NodeID("else-node"), got.NextNode, "and 组一假 → 假分支")

	orHit := &LogicConditionGroup{Relation: Or, Conditions: []ConditionElement{
		LogicCondition{Field: "$INPUT.a", Operator: Lt, Value: 5},
		LogicCondition{Field: "$INPUT.b", Operator: Eq, Value: "xyz"},
	}}
	got, err = newIf(orHit).Execute(ifRuntimeCtx(input))
	require.NoError(t, err)
	require.Equal(t, workflow.NodeID("then-node"), got.NextNode, "or 组一真 → 真分支")

	orMiss := &LogicConditionGroup{Relation: Or, Conditions: []ConditionElement{
		LogicCondition{Field: "$INPUT.a", Operator: Lt, Value: 5},
		LogicCondition{Field: "$INPUT.b", Operator: Eq, Value: "nope"},
	}}
	got, err = newIf(orMiss).Execute(ifRuntimeCtx(input))
	require.NoError(t, err)
	require.Equal(t, workflow.NodeID("else-node"), got.NextNode, "or 组全假 → 假分支")
}

// 真值算子（Python bool(x)，A1 修复形态）：空列表/空串 falsy，非空 truthy。
func TestIf_Execute_TruthyFalsyOperators(t *testing.T) {
	newIf := func(op ComparisonOperator) *If {
		return &If{
			BaseNode: BaseNode{ID: "myif", Next: "then-node"},
			Condition: &Condition{
				LogicCondition: LogicCondition{Field: "$INPUT.v", Operator: op},
			},
			ElseNext: "else-node",
		}
	}

	// `if items:` 空列表 → falsy → 假分支（历史 (x != False) 降级会误走真分支）。
	got, err := newIf(Truthy).Execute(ifRuntimeCtx(map[string]any{"v": []any{}}))
	require.NoError(t, err)
	require.Equal(t, workflow.NodeID("else-node"), got.NextNode)

	got, err = newIf(Truthy).Execute(ifRuntimeCtx(map[string]any{"v": []any{1}}))
	require.NoError(t, err)
	require.Equal(t, workflow.NodeID("then-node"), got.NextNode)

	got, err = newIf(Falsy).Execute(ifRuntimeCtx(map[string]any{"v": 0}))
	require.NoError(t, err)
	require.Equal(t, workflow.NodeID("then-node"), got.NextNode)

	got, err = newIf(Falsy).Execute(ifRuntimeCtx(map[string]any{"v": "x"}))
	require.NoError(t, err)
	require.Equal(t, workflow.NodeID("else-node"), got.NextNode)
}

// 目标缺失：命中路无 next / 未命中路无 else_next → 显式报错不静默跳空
// （Python resolve_branch_target 返回 None → 调度层报错）。
func TestIf_Execute_MissingTargets(t *testing.T) {
	trueMissing := &If{
		BaseNode: BaseNode{ID: "myif"},
		Condition: &Condition{
			LogicCondition: LogicCondition{Field: "$INPUT.a", Operator: Gt, Value: 5},
		},
		ElseNext: "else-node",
	}
	_, err := trueMissing.Execute(ifRuntimeCtx(map[string]any{"a": 10}))
	require.Error(t, err)
	require.Contains(t, err.Error(), "true branch has no next target")

	falseMissing := &If{
		BaseNode: BaseNode{ID: "myif", Next: "then-node"},
		Condition: &Condition{
			LogicCondition: LogicCondition{Field: "$INPUT.a", Operator: Gt, Value: 5},
		},
	}
	_, err = falseMissing.Execute(ifRuntimeCtx(map[string]any{"a": 1}))
	require.Error(t, err)
	require.Contains(t, err.Error(), "false branch has no next target")
}

// 条件求值错误向上冒泡为节点错误（decide.py Condition.match 异常传播）。
func TestIf_Execute_ConditionErrorPropagates(t *testing.T) {
	n := &If{
		BaseNode: BaseNode{ID: "myif", Next: "then-node"},
		Condition: &Condition{
			LogicCondition: LogicCondition{Field: "$INPUT.a", Operator: ComparisonOperator("bogus"), Value: 1},
		},
		ElseNext: "else-node",
	}
	got, err := n.Execute(ifRuntimeCtx(map[string]any{"a": 10}))
	require.Error(t, err)
	require.Nil(t, got)
	require.Contains(t, err.Error(), `execute if node ("myif")`)
}

// $NODE.<if> 结果 + $BRANCH：端节点经表达式读到命中目标（#6 形态 +
// Python run_node branch=result 契约）。
func TestIf_Execute_NodeResultAndBranchInFlow(t *testing.T) {
	run := func(input map[string]any) any {
		t.Helper()
		flow := &workflow.Workflow{
			ID:      "if-result-flow",
			Runtime: workflow.RuntimeGo,
			Version: "v1.0",
			Nodes: workflow.Nodes{
				&Start{BaseNode{ID: "start", Next: "myif"}},
				&If{
					BaseNode: BaseNode{ID: "myif", Next: "e1"},
					Condition: &Condition{
						LogicCondition: LogicCondition{Field: "$INPUT.a", Operator: Gt, Value: 5},
					},
					ElseNext: "e2",
				},
				&End{BaseNode: BaseNode{ID: "e1", Output: "$NODE.myif"}},
				&End{BaseNode: BaseNode{ID: "e2", Output: map[string]any{"target": "$NODE.myif", "branch": "$BRANCH"}}},
			},
		}
		got, err := flow.Execute(context.Background(), input)
		require.NoError(t, err)
		return got
	}

	require.Equal(t, "e1", run(map[string]any{"a": 10}), "$NODE.myif 必须是真分支目标")
	require.Equal(t, map[string]any{"target": "e2", "branch": "e2"},
		run(map[string]any{"a": 1}), "$NODE.myif / $BRANCH 必须是假分支目标")
}

// 与 switch 的等价性：同一判定用 if 与二分支 switch（default 兜底）表达，
// 两路输入的输出完全一致（Bool 是 Switch 的二值特化）。
func TestIf_VsSwitch_Equivalence(t *testing.T) {
	cond := func() Condition {
		return Condition{LogicCondition: LogicCondition{Field: "$INPUT.a", Operator: Gt, Value: 5}}
	}
	ifFlow := &workflow.Workflow{
		ID: "if-flow", Runtime: workflow.RuntimeGo, Version: "v1.0",
		Nodes: workflow.Nodes{
			&Start{BaseNode{ID: "start", Next: "d"}},
			&If{
				BaseNode:  BaseNode{ID: "d", Next: "e1"},
				Condition: &Condition{LogicCondition: LogicCondition{Field: "$INPUT.a", Operator: Gt, Value: 5}},
				ElseNext:  "e2",
			},
			&End{BaseNode: BaseNode{ID: "e1", Output: "big"}},
			&End{BaseNode: BaseNode{ID: "e2", Output: "small"}},
		},
	}
	switchFlow := &workflow.Workflow{
		ID: "switch-flow", Runtime: workflow.RuntimeGo, Version: "v1.0",
		Nodes: workflow.Nodes{
			&Start{BaseNode{ID: "start", Next: "d"}},
			&Switch{
				BaseNode: BaseNode{ID: "d"},
				Branches: []*Branch{
					{Name: "then", Condition: cond(), Next: "e1"},
					{Name: "else", IsDefault: true, Next: "e2"},
				},
			},
			&End{BaseNode: BaseNode{ID: "e1", Output: "big"}},
			&End{BaseNode: BaseNode{ID: "e2", Output: "small"}},
		},
	}

	for _, input := range []map[string]any{{"a": 10}, {"a": 1}, {"a": 5}} {
		ifGot, err := ifFlow.Execute(context.Background(), input)
		require.NoError(t, err)
		swGot, err := switchFlow.Execute(context.Background(), input)
		require.NoError(t, err)
		require.Equal(t, swGot, ifGot, "input %v: if 与二分支 switch 必须等价", input)
	}
}

// JSON 反序列化（ir 加载路径）：registry 按 type "if" 构造、条件/目标字段
// 落位，执行语义不变——部署 JSON 形状即 ir 权威形状（snake else_next）。
func TestIf_UnmarshalFromIRShapeJSON(t *testing.T) {
	data := []byte(`{
		"id": "rt-flow",
		"nodes": [
			{"type": "start", "id": "s", "next": "myif"},
			{"type": "if", "id": "myif",
			 "condition": {"relation": "and", "conditions": [
				{"field": "$INPUT.a", "operator": "gt", "value": 5},
				{"field": "$INPUT.b", "operator": "eq", "value": "go"}
			 ]},
			 "next": "e1", "else_next": "e2"},
			{"type": "end", "id": "e1", "resultType": "success", "output": "hit"},
			{"type": "end", "id": "e2", "resultType": "success", "output": "miss"}
		]
	}`)
	var wf workflow.Workflow
	require.NoError(t, json.Unmarshal(data, &wf))

	got, err := wf.Execute(context.Background(), map[string]any{"a": 10, "b": "go"})
	require.NoError(t, err)
	require.Equal(t, "hit", got)

	got, err = wf.Execute(context.Background(), map[string]any{"a": 10, "b": "no"})
	require.NoError(t, err)
	require.Equal(t, "miss", got)
}

// registry 登记：type "if" 可经工厂构造，GetType 回报 "if"。
func TestIf_RegisteredNodeType(t *testing.T) {
	factory := workflow.GetNodeFactory(workflow.NodeType("if"))
	require.NotNil(t, factory, "if node type must be registered")
	n := factory(context.Background())
	require.Equal(t, workflow.NodeType("if"), n.GetType())
	ifNode, ok := n.(*If)
	require.True(t, ok, "factory must build *If")
	ifNode.ID = "factory-if"
	require.NoError(t, ifNode.Validate())
}
