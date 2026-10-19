package workflow_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/jeffkit/plaita-go/pkg/duration"
	"github.com/jeffkit/plaita-go/pkg/workflow"
	"github.com/jeffkit/plaita-go/pkg/workflow/nodes"
)

func TestWorkflow_Execute(t *testing.T) {
	tests := []struct {
		name        string
		wf          *workflow.Workflow
		executeArgs workflow.ExecuteArgs
		want        any
		wantErr     bool
	}{
		{
			name: "test simple workflow",
			wf: &workflow.Workflow{
				ID:         "simple-workflow",
				Runtime:    workflow.RuntimeGo,
				Version:    "v1.0",
				InputType:  workflow.Property{DataType: workflow.Integer},
				OutputType: workflow.Property{DataType: workflow.String},
				Nodes: workflow.Nodes{
					&nodes.Start{BaseNode: nodes.BaseNode{ID: "start", Next: "sleep"}},
					&nodes.Sleep{BaseNode: nodes.BaseNode{ID: "sleep", Next: "end"}, Duration: 10 * time.Millisecond},
					&nodes.End{
						BaseNode: nodes.BaseNode{
							ID:         "end",
							Output:     "Hello Plaita!",
							OutputType: workflow.Property{DataType: workflow.String},
						},
					},
				},
			},
			executeArgs: nil,
			want:        "Hello Plaita!",
			wantErr:     false,
		},
		{
			name: "test execute workflow with timeout and abort policy",
			wf: &workflow.Workflow{
				ID:         "simple-workflow",
				Runtime:    workflow.RuntimeGo,
				Version:    "v1.0",
				InputType:  workflow.Property{DataType: workflow.Integer},
				OutputType: workflow.Property{DataType: workflow.String},
				Timeout:    duration.MustParseDuration("PT1S"),
				TimeoutHandlePolicy: workflow.TimeoutHandlePolicy{
					Strategy:     workflow.HandleStrategyAbort,
					DefaultValue: "Hello Timeout",
				},
				Nodes: workflow.Nodes{
					&nodes.Start{BaseNode: nodes.BaseNode{ID: "start", Next: "sleep1"}},
					&nodes.Sleep{BaseNode: nodes.BaseNode{ID: "sleep1", Next: "sleep2"}, Duration: 1 * time.Second},
					&nodes.Sleep{BaseNode: nodes.BaseNode{ID: "sleep2", Next: "end"}, Duration: 1 * time.Second},
					&nodes.End{
						BaseNode: nodes.BaseNode{
							ID:         "end",
							Output:     "Hello Plaita!",
							OutputType: workflow.Property{DataType: workflow.String},
						},
					},
				},
			},
			executeArgs: nil,
			want:        nil,
			wantErr:     true,
		},
		{
			name: "test execute workflow with timeout and continue policy",
			wf: &workflow.Workflow{
				ID:         "simple-workflow",
				Runtime:    workflow.RuntimeGo,
				Version:    "v1.0",
				InputType:  workflow.Property{DataType: workflow.Integer},
				OutputType: workflow.Property{DataType: workflow.String},
				Timeout:    duration.MustParseDuration("PT1S"),
				TimeoutHandlePolicy: workflow.TimeoutHandlePolicy{
					Strategy:     workflow.HandleStrategyContinue,
					DefaultValue: "Hello Timeout",
				},
				Nodes: workflow.Nodes{
					&nodes.Start{BaseNode: nodes.BaseNode{ID: "start", Next: "sleep1"}},
					&nodes.Sleep{BaseNode: nodes.BaseNode{ID: "sleep1", Next: "sleep2"}, Duration: 1 * time.Second},
					&nodes.Sleep{BaseNode: nodes.BaseNode{ID: "sleep2", Next: "end"}, Duration: 1 * time.Second},
					&nodes.End{
						BaseNode: nodes.BaseNode{
							ID:         "end",
							Output:     "Hello Plaita!",
							OutputType: workflow.Property{DataType: workflow.String},
						},
					},
				},
			},
			executeArgs: nil,
			want:        nil,
			wantErr:     false,
		},
		{
			name: "test execute workflow with timeout and continue-with policy",
			wf: &workflow.Workflow{
				ID:         "simple-workflow",
				Runtime:    workflow.RuntimeGo,
				Version:    "v1.0",
				InputType:  workflow.Property{DataType: workflow.Integer},
				OutputType: workflow.Property{DataType: workflow.String},
				Timeout:    duration.MustParseDuration("PT1S"),
				TimeoutHandlePolicy: workflow.TimeoutHandlePolicy{
					Strategy:     workflow.HandleStrategyContinueWith,
					DefaultValue: "Hello Timeout",
				},
				Nodes: workflow.Nodes{
					&nodes.Start{BaseNode: nodes.BaseNode{ID: "start", Next: "sleep1"}},
					&nodes.Sleep{BaseNode: nodes.BaseNode{ID: "sleep1", Next: "sleep2"}, Duration: 1 * time.Second},
					&nodes.Sleep{BaseNode: nodes.BaseNode{ID: "sleep2", Next: "end"}, Duration: 1 * time.Second},
					&nodes.End{
						BaseNode: nodes.BaseNode{
							ID:         "end",
							Output:     "Hello Plaita!",
							OutputType: workflow.Property{DataType: workflow.String},
						},
					},
				},
			},
			executeArgs: nil,
			want:        "Hello Timeout",
			wantErr:     false,
		},
		{
			name: "test execute workflow with error node",
			wf: &workflow.Workflow{
				ID:         "error-workflow",
				Runtime:    workflow.RuntimeGo,
				Version:    "v1.0",
				InputType:  workflow.Property{DataType: workflow.Integer},
				OutputType: workflow.Property{DataType: workflow.String},
				Nodes: workflow.Nodes{
					&nodes.Start{BaseNode: nodes.BaseNode{ID: "start", Next: "http"}},
					&nodes.HTTP{
						BaseNode: nodes.BaseNode{
							ID:   "http",
							Next: "end",
							ErrorHandlePolicy: workflow.RecoverableErrorHandlePolicy{
								Strategy:     workflow.HandleStrategyContinue,
								DefaultValue: nil,
								RetryTimes:   0,
							},
						},
						RawURL:        "http://invalid-url",
						RawAddressing: &nodes.RawAddressing{},
					},
					&nodes.End{
						BaseNode: nodes.BaseNode{
							ID:         "end",
							Output:     "$ERROR.http.Code",
							OutputType: workflow.Property{DataType: workflow.String},
						},
					},
				},
			},
			executeArgs: nil,
			want:        1002,
			wantErr:     false,
		},
		{
			name: "test execute workflow with http request info",
			wf: &workflow.Workflow{
				ID:         "error-workflow",
				Runtime:    workflow.RuntimeGo,
				Version:    "v1.0",
				InputType:  workflow.Property{DataType: workflow.Integer},
				OutputType: workflow.Property{DataType: workflow.String},
				Nodes: workflow.Nodes{
					&nodes.Start{BaseNode: nodes.BaseNode{ID: "start", Next: "http"}},
					&nodes.HTTP{
						BaseNode: nodes.BaseNode{
							ID:   "http",
							Next: "end",
							ErrorHandlePolicy: workflow.RecoverableErrorHandlePolicy{
								Strategy:     workflow.HandleStrategyContinue,
								DefaultValue: nil,
								RetryTimes:   0,
							},
						},
						RawURL:        "http://tencent.com",
						RawAddressing: &nodes.RawAddressing{},
					},
					&nodes.End{
						BaseNode: nodes.BaseNode{
							ID:         "end",
							Output:     "$ERROR.http.Info.Request.Method",
							OutputType: workflow.Property{DataType: workflow.String},
						},
					},
				},
			},
			executeArgs: nil,
			want:        "GET",
			wantErr:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.wf.Execute(context.Background(), tt.executeArgs)
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Execute() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWorkflow_DebugExecute(t *testing.T) {
	// 构造一个简单的workflow，调用flow的GetDebugExecutor获得DebugExecutor，然后调用DebugExecutor的NextStep方法实现分步调试
	// 1. 构造一个简单的workflow
	flow := &workflow.Workflow{
		ID:         "simple-workflow",
		Runtime:    workflow.RuntimeGo,
		Version:    "v1.0",
		InputType:  workflow.Property{DataType: workflow.Integer},
		OutputType: workflow.Property{DataType: workflow.String},

		Nodes: workflow.Nodes{
			&nodes.Start{BaseNode: nodes.BaseNode{ID: "start", Next: "assign"}},
			// 创建一个赋值节点，结束节点把结果赋值给output
			&nodes.Assignment{BaseNode: nodes.BaseNode{ID: "assign", Next: "end", Output: "KongJie"}},
			&nodes.End{
				BaseNode: nodes.BaseNode{
					ID:         "end",
					Output:     "$NODE.assign",
					OutputType: workflow.Property{DataType: workflow.String},
				},
				ResultType: nodes.ResultTypeSuccess,
			},
		},
	}
	executor, err := flow.GetDebugExecute(context.Background(), nil)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	// 2. 调用DebugExecutor的NextStep方法实现分步调试
	step := executor.NextStep()
	if step.ID != "start" {
		t.Errorf("Expected start, got %s", step.ID)
	}
	step = executor.NextStep()
	if step.ID != "assign" {
		t.Errorf("Expected assign, got %s", step.ID)
	}
	step = executor.NextStep()
	if step.ID != "end" {
		t.Errorf("Expected end, got %s", step.ID)
	}
	if step.Result.Result != "KongJie" {
		t.Errorf("Expected KongJie, got %s", step.Result)
	}
	// 检查Context
	if step.Context == nil {
		t.Errorf("Expected context, got nil")
	}
	// marshal context
	_, err = json.Marshal(step.Context)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}
