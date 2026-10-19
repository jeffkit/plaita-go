package nodes

import (
	"context"
	"fmt"
	"sort"

	"github.com/jeffkit/plaita-go/internal/runtime"

	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func init() {
	workflow.RegisterNodeType(
		types.NodeTypeSwitch,
		func(ctx context.Context) workflow.Node { return new(Switch) },
	)
}

// Switch node is used to route the execution of the flow based on the condition.
type Switch struct {
	BaseNode
	Branches []*Branch `json:"branches"`
}

// Validate validates the switch node.
func (sw *Switch) Validate() error {
	var defaultBranchCount int
	for _, b := range sw.Branches {
		if b.IsDefault {
			defaultBranchCount++
		}
	}

	if defaultBranchCount > 1 {
		return fmt.Errorf("there should be at most one default branch in switch node: %q", sw.GetID())
	}
	return nil
}

// GetType returns the node type.
func (sw *Switch) GetType() workflow.NodeType {
	return types.NodeTypeSwitch
}

// Execute executes the switch node.
func (sw *Switch) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	// 按 priority 降序排序——但必须排副本：branches 属于共享的 Workflow 定义，
	// 原地 sort 会永久改写节点状态，同一 Workflow 对象被并发 run 时是数据竞争
	//（Python decide.py 用非原地 sorted，注释即为此；issue #17）。
	branches := make([]*Branch, len(sw.Branches))
	copy(branches, sw.Branches)
	sort.SliceStable(branches, func(i, j int) bool { return branches[i].Priority > branches[j].Priority })
	// 给所有Branch设置eagerMode
	var selectedBranch *Branch
	var matchInfos []*MatchInfo
	workflowContext := runtime.WorkflowRuntimeContext(ctx)
	eagerMode := workflowContext.GetGlobalVariable("switch:eagerMode")
	IsEagerMode := false
	if eagerMode != nil {
		IsEagerMode, _ = eagerMode.(bool)
	}
	for _, branch := range branches {
		if branch.IsDefault {
			continue
		}

		match, info, err := branch.MatchContext(ctx)
		if err != nil {
			if !IsEagerMode {
				return nil, fmt.Errorf("execute switch node (%q): %w", sw.GetID(), err)
			}
			continue
		}
		if match {
			if selectedBranch == nil {
				selectedBranch = branch
			}
			if !IsEagerMode {
				break
			}
		}
		matchInfos = append(matchInfos, info)
	}

	if selectedBranch == nil {
		selectedBranch = sw.defaultBranch()
	}
	if selectedBranch == nil {
		return nil, fmt.Errorf("execute switch node (%q): no branch matched and no default branch", sw.GetID())
	}

	// plaita 语义：命中分支的目标作为节点输出，供下游以 $NODE.<switch> 引用
	//（decide.py resolve_branch_target：next 优先；next 为空时按 Switch 的
	// branch_name_as_target 语义回落 branch name——IR normalize 在加载时做过
	// 一次，运行期对直连引擎构造的节点同样显式兜底）。无目标显式报错，
	// 不静默跳空（Python resolve 返回 None 由 runner 报错）。
	target := selectedBranch.Next
	if target == "" {
		target = types.NodeID(selectedBranch.Name)
	}
	if target == "" {
		return nil, fmt.Errorf("execute switch node (%q): branch %q has no next target", sw.GetID(), selectedBranch.Name)
	}

	values := workflowContext.GetCurrentNodeContext(extraResultKey)
	extraResult, _ := values.(map[string]any)
	if extraResult == nil {
		extraResult = make(map[string]any)
	}
	extraResult["matchInfos"] = matchInfos
	workflowContext.WithCurrentNodeContext(extraResultKey, extraResult)
	return &workflow.ExecuteResult{Result: string(target), NextNode: target}, nil
}

func (sw *Switch) defaultBranch() *Branch {
	for _, b := range sw.Branches {
		if b.IsDefault {
			return b
		}
	}
	return nil
}
