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
	// Sort branches by priority.
	sort.SliceStable(sw.Branches, func(i, j int) bool { return sw.Branches[i].Priority > sw.Branches[j].Priority })
	// 给所有Branch设置eagerMode
	var selectedBranch *Branch
	var matchInfos []*MatchInfo
	workflowContext := runtime.WorkflowRuntimeContext(ctx)
	eagerMode := workflowContext.GetGlobalVariable("switch:eagerMode")
	IsEagerMode := false
	if eagerMode != nil {
		IsEagerMode, _ = eagerMode.(bool)
	}
	for _, branch := range sw.Branches {
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

	values := workflowContext.GetCurrentNodeContext(extraResultKey)
	extraResult, _ := values.(map[string]any)
	if extraResult == nil {
		extraResult = make(map[string]any)
	}
	extraResult["matchInfos"] = matchInfos
	workflowContext.WithCurrentNodeContext(extraResultKey, extraResult)
	return &workflow.ExecuteResult{NextNode: selectedBranch.Next}, nil
}

func (sw *Switch) defaultBranch() *Branch {
	for _, b := range sw.Branches {
		if b.IsDefault {
			return b
		}
	}
	return nil
}
