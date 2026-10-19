package parser

import (
	_ "embed"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jeffkit/plaita-go/pkg/workflow"
	"github.com/jeffkit/plaita-go/pkg/workflow/nodes"
)

func TestFromFile(t *testing.T) {
	tests := []struct {
		name         string
		path         string
		wantErr      bool
		wantWorkflow *workflow.Workflow
	}{
		{
			name:    "test addition-flow json",
			path:    "./testdata/addition-flow.json",
			wantErr: false,
			wantWorkflow: &workflow.Workflow{
				ID:      "addition-flow",
				Name:    "Addition Flow",
				Runtime: "go",
				Version: "v1.0.0",
				Nodes: []workflow.Node{
					&nodes.Start{BaseNode: nodes.BaseNode{ID: "start", Type: "start", Next: "addition"}},
					&nodes.Assignment{
						BaseNode: nodes.BaseNode{
							ID:         "addition",
							Type:       "assignment",
							Next:       "end",
							OutputType: workflow.Property{DataType: workflow.Float},
							Output:     "$F.add($INPUT.a, $INPUT.b)",
						},
					},
					&nodes.End{
						BaseNode:   nodes.BaseNode{ID: "end", Type: "end", Output: "$NODE.addition"},
						ResultType: nodes.ResultTypeSuccess,
					},
				},
				InputType: workflow.Property{
					DataType: workflow.Object,
					Properties: map[string]workflow.Property{
						"a": {DataType: workflow.Integer},
						"b": {DataType: workflow.Integer},
					},
				},
				OutputType: workflow.Property{
					DataType: workflow.Float,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wf, err := FromFile(tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("FromFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			require.Equal(t, tt.wantWorkflow, wf)
		})
	}
}
