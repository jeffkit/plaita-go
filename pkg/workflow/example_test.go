package workflow_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"github.com/jeffkit/plaita-go/pkg/workflow"
	"github.com/jeffkit/plaita-go/pkg/workflow/nodes"
)

func TestWorkflowAssignment(t *testing.T) {
	jsonBytes, err := os.ReadFile("./testdata/assignment.json")
	require.NoError(t, err)

	wf := new(workflow.Workflow)
	err = json.Unmarshal(jsonBytes, wf)
	require.NoError(t, err)

	got, err := wf.Execute(
		context.Background(),
		map[string]any{
			"bb": "123",
		},
	)
	require.NoError(t, err)

	data, _ := json.MarshalIndent(got, "", "  ")
	fmt.Printf("%s\n", data)
	// 断言结果
	exp, _ := json.MarshalIndent(map[string]string{"dd": "456"}, "", "  ")
	assert.Equal(t, string(data), string(exp), "不符合期望")
}

func TestLoop(t *testing.T) {
	jsonBytes, err := os.ReadFile("./testdata/loop.json")
	require.NoError(t, err)

	wf := new(workflow.Workflow)
	err = json.Unmarshal(jsonBytes, wf)
	require.NoError(t, err)

	got, err := wf.Execute(
		context.Background(),
		[]map[string]any{{"name": "test"}, {"name": "test2"}, {"name": "test3"}},
	)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"go": "test3"}, got)
	data, _ := json.MarshalIndent(got, "", "  ")
	fmt.Printf("%s\n", data)
}

func TestSwitchWorkflow(t *testing.T) {
	jsonBytes, err := os.ReadFile("./testdata/switch.json")
	require.NoError(t, err)

	wf := new(workflow.Workflow)
	err = json.Unmarshal(jsonBytes, wf)
	require.NoError(t, err)

	got, err := wf.Execute(
		context.Background(),
		map[string]any{"checkup_state": 4, "positive_content": 1543606, "is_search": 1543606, "playright": "1+2", "data_flag": "1"},
	)
	require.NoError(t, err)

	fmt.Printf("%s\n", got)

}

func init() {
	workflow.RegisterNodeType("test2", func(ctx context.Context) workflow.Node { return new(test2Node) })
}

type test2Node struct {
	nodes.BaseNode
}

func (t *test2Node) GetType() workflow.NodeType {
	return "test2"
}

func (t *test2Node) Execute(_ context.Context) (*workflow.ExecuteResult, error) {
	return &workflow.ExecuteResult{NextNode: t.NextNode(), Result: map[string]any{"bb": "123"}}, nil
}

func TestReference(t *testing.T) {
	jsonBytes, err := os.ReadFile("./testdata/reference.json")
	require.NoError(t, err)

	wf := new(workflow.Workflow)
	err = json.Unmarshal(jsonBytes, wf)
	require.NoError(t, err)

	got, err := wf.Execute(
		context.Background(),
		map[string]any{
			"str": "123",
		},
	)
	require.NoError(t, err)

	data, _ := json.MarshalIndent(got, "", "  ")
	fmt.Printf("%s\n", data)
}

func TestNestedCondition(t *testing.T) {
	jsonBytes, err := os.ReadFile("./testdata/condition_nested.json")
	require.NoError(t, err)

	wf := new(workflow.Workflow)
	err = json.Unmarshal(jsonBytes, wf)
	require.NoError(t, err)
}

func TestWorkflowSetDefaultGlobalVariable(t *testing.T) {
	jsonBytes, err := os.ReadFile("./testdata/global_variable.json")
	require.NoError(t, err)

	wf := new(workflow.Workflow)
	err = json.Unmarshal(jsonBytes, wf)
	require.NoError(t, err)

	got, err := wf.Execute(
		context.Background(),
		map[string]any{},
	)
	require.NoError(t, err)
	assert.Equal(t, "default", got)
}

func TestWorkflowChooseGlobalEnv(t *testing.T) {
	jsonBytes, err := os.ReadFile("./testdata/global_variable.json")
	require.NoError(t, err)

	wf := new(workflow.Workflow)
	err = json.Unmarshal(jsonBytes, wf)
	require.NoError(t, err)
	wf.SetEnvName("production")
	got, err := wf.Execute(
		context.Background(),
		map[string]any{},
	)
	require.NoError(t, err)
	assert.Equal(t, "production", got)
}

func TestWorkflowUseFunction(t *testing.T) {
	jsonBytes, err := os.ReadFile("./testdata/function.json")
	require.NoError(t, err)

	wf := new(workflow.Workflow)
	err = json.Unmarshal(jsonBytes, wf)
	require.NoError(t, err)
	got, err := wf.Execute(
		context.Background(),
		map[string]any{
			"name":    "nick",
			"age":     10,
			"hobbies": []any{"sports", "music", "games"},
			"family": map[string]any{
				"jack":  "brother",
				"peter": "brother",
			},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, map[string]any{
		"upper_name":  "NICK",
		"double_age":  20,
		"first_hobby": "sports",
		"brothers":    []any{"jack", "peter"},
		"name":        "NICK",
	}, got)
}

func TestWorkflowHttpError(t *testing.T) {
	jsonBytes, err := os.ReadFile("./testdata/http_error.json")
	require.NoError(t, err)

	wf := new(workflow.Workflow)
	err = json.Unmarshal(jsonBytes, wf)
	require.NoError(t, err)
	result, err := wf.Execute(
		context.Background(),
		map[string]any{
			"title": "test",
		},
	)
	require.NoError(t, err)
	require.Equal(t, 1002, result)
}
