package nodes

import (
	"context"
	"fmt"

	"github.com/jeffkit/plaita-go/internal/runtime"

	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func init() {
	workflow.RegisterNodeType(
		nodeTypeIf,
		func(ctx context.Context) workflow.Node { return new(If) },
	)
}

// nodeTypeIf is plaita's "if" node type (decide.py Bool).
const nodeTypeIf types.NodeType = "if"

// If is the two-way conditional branch node — plaita decide.py Bool
// (node_type "if"), the binary specialization of Switch.
//
// Python 语义权威（plaita/plaita/node/decide.py:293-329）：
//
//   - 字段：condition（Condition / ConditionGroup）、next（真分支目标，由
//     BaseNode.Next 承载）、else_next（假分支目标）。Bool.setup_branches 把
//     三者展开成两条固定分支：true 分支（condition，priority 0）与 false
//     分支（无 condition，isDefault）——执行即 Switch.execute 退化为
//     "condition 命中走 next，否则走 else_next"，本节点的 Execute 直接实现
//     该退化形态，不再落 Branches 结构。
//   - 真值语义：condition 为 None 时真分支被 `if branch.condition and ...`
//     短路 → 恒走假分支（decide.py:283）。所以这里的 Condition 用指针承载：
//     nil（JSON 缺省 / null）不走 Condition 零值的"恒真"回退，而是照 Python
//     走假分支；非空 condition 的判定与算子语义复用 Branch 的条件原语
//     （plaitaexpr.Equal/Compare/Contains + truthy/falsy Python 真值）。
//   - 条件求值错误向上冒泡为节点错误（decide.py Condition.match 异常传播，
//     Switch.execute 同型包装）。
//   - 结果语义（#6 形态，同 Switch）：命中分支的目标作为节点结果写入
//     $NODE.<if>，并记录为 $BRANCH（Python run_node: branch = result if
//     node.branching，decide.py "输出命中的分支名"契约——Bool 的分支目标即
//     next/else_next 本身），供下游引用。
//   - 无命中且无 else：部署 IR 在加载期拒绝（ir translateIf）；直连引擎构造
//     时 Execute 显式报错，不静默跳空（Python resolve_branch_target 返回
//     None → 调度层报错，flow.py:471 对 true/false 占位目标另附提示）。
type If struct {
	BaseNode
	// Condition is the true-branch condition. nil (absent or JSON null)
	// always takes the else branch — Python Bool with condition=None
	// short-circuits the true branch (decide.py:283), it does NOT hit the
	// zero-value "always match" fallback shared by loop/switch conditions.
	Condition *Condition `json:"condition,omitempty"`
	// ElseNext is the false-branch target. The IR contract spelling is
	// snake else_next; the camel alias elseNext is folded by ir normalize.
	ElseNext workflow.NodeID `json:"else_next,omitempty"`
}

// GetType returns the node type.
func (n *If) GetType() workflow.NodeType {
	return nodeTypeIf
}

// Execute evaluates the condition and routes to the true/false target,
// writing the taken target as the node result ($NODE.<if> / $BRANCH).
func (n *If) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	var matched bool
	var info *MatchInfo
	if n.Condition != nil {
		m, mi, err := n.Condition.MatchContext(ctx)
		if err != nil {
			return nil, fmt.Errorf("execute if node (%q): %w", n.GetID(), err)
		}
		matched, info = m, mi
	}

	// Python Bool.setup_branches 的两条固定分支：true 分支带 condition，
	// false 分支 isDefault 兜底——命中与否分别取 next / else_next。
	target := n.Next
	branch := "true"
	if !matched {
		target = n.ElseNext
		branch = "false"
	}
	if target == "" {
		// 无命中且无 else（或真分支未声明 next）：Python 解析目标为 None，
		// 调度层报错（flow.py:471）——这里显式报错，不静默跳空。
		return nil, fmt.Errorf("execute if node (%q): %s branch has no next target (next=%q, else_next=%q)",
			n.GetID(), branch, n.Next, n.ElseNext)
	}

	// #6 形态（同 Switch）：命中分支目标即节点结果。
	workflowContext := runtime.WorkflowRuntimeContext(ctx)
	values := workflowContext.GetCurrentNodeContext(extraResultKey)
	extraResult, _ := values.(map[string]any)
	if extraResult == nil {
		extraResult = make(map[string]any)
	}
	if info != nil {
		extraResult["matchInfos"] = []*MatchInfo{info}
	}
	workflowContext.WithCurrentNodeContext(extraResultKey, extraResult)
	// Python run_node：branching 节点的返回值即 last_branch（runner.py:470）。
	workflowContext.WithLastBranch(string(target))
	return &workflow.ExecuteResult{Result: string(target), NextNode: target}, nil
}
