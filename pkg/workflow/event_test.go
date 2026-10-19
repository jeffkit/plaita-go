package workflow_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/jeffkit/plaita-go/pkg/workflow"
	"github.com/jeffkit/plaita-go/pkg/workflow/nodes"
)

// test event, flow start, flow end, node start, node end
func TestFlowEvent(t *testing.T) {
	// 创建一个workFlow
	flow := &workflow.Workflow{
		ID: "flow-event",
		InputType: workflow.Property{
			DataType: workflow.Object,
			Properties: map[string]workflow.Property{
				"name": {
					DataType: workflow.String,
				},
				"age": {
					DataType: workflow.Integer,
				},
			},
		},
		Nodes: workflow.Nodes{
			&nodes.Start{BaseNode: nodes.BaseNode{ID: "start", Next: "switch"}},
			// 判断节点
			&nodes.Switch{
				BaseNode: nodes.BaseNode{ID: "switch", Metadata: map[string]string{"filter_no": "1"}},
				Branches: []*nodes.Branch{
					{
						Condition: nodes.Condition{
							LogicCondition: nodes.LogicCondition{
								Field:    "$INPUT.name",
								Operator: nodes.Eq,
								Value:    "zhangsan",
							},
						},
						Next: "zhangsan_end",
					},
					{
						IsDefault: true,
						Next:      "default_end",
					},
				},
			},
			&nodes.End{BaseNode: nodes.BaseNode{ID: "default_end", Output: "$INPUT.age"}},
			&nodes.End{BaseNode: nodes.BaseNode{ID: "zhangsan_end", Output: "$INPUT.age"}},
		},
	}
	workflow.OnFlowStart(func(ctx context.Context, flow *workflow.Workflow) {
		t.Log("flow start", flow.ID)
	})
	workflow.OnFlowEnd(func(ctx context.Context, flow *workflow.Workflow, result any, err error) {
		t.Log("flow end")
	})
	workflow.OnNodeStart(func(ctx context.Context, flow *workflow.Workflow, node workflow.Node) {
		t.Log("node start: ", node.GetID())
	})
	workflow.OnNodeEnd(func(ctx context.Context, flow *workflow.Workflow, node workflow.Node, result any, err error, args map[string]interface{}) {
		t.Log("node end")
		if node.GetType() == "switch" {
			// 判断metadata是否有filter_no
			filterNo := node.GetMetadata()["filter_no"]
			if filterNo == "1" {
				matchInfo := args["matchInfos"]
				matchBytes, _ := json.Marshal(matchInfo)
				t.Logf("matchInfo: %s", matchBytes)
				assert.Equal(t, []*nodes.MatchInfo{{
					Field:         "$INPUT.name",
					Operator:      "eq",
					Expect:        "zhangsan",
					Actual:        "zhangsan",
					Matched:       true,
					Error:         "",
					Children:      nil,
					ChildRelation: "",
				}}, matchInfo)
			}
		}
	})
	ctx := flow.SetFlowGlobalVariable(context.Background(), "switch:eagerMode", true)
	result, err := flow.Execute(ctx, map[string]any{"name": "zhangsan", "age": 18})
	if result != 18 {
		t.Error("result not equal 18")
	}
	if err != nil {
		t.Error(err)
	}
}
