package nodes

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func TestChildrenFlow_Execute(t *testing.T) {
	tests := []struct {
		name         string
		runtimeCtx   *runtime.Context
		childrenNode *Child
		want         *workflow.ExecuteResult
		wantErr      bool
	}{
		{
			name:       "test simple child workflow node",
			runtimeCtx: &runtime.Context{},
			childrenNode: &Child{
				BaseNode: BaseNode{
					ID:   "child",
					Next: "next-node",
				},
				ChildFlow: &workflow.Workflow{
					ID: "test-workflow",
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
					OutputType: workflow.Property{
						DataType: workflow.String,
					},
					Nodes: workflow.Nodes{
						&Start{BaseNode{ID: "start", Next: "end"}},
						&End{BaseNode: BaseNode{ID: "end", Output: "children workflow execute completed"}},
					},
				},
				Input: map[string]any{
					"name": "John",
					"age":  18,
				},
			},
			want: &workflow.ExecuteResult{
				Result:   "children workflow execute completed",
				NextNode: "next-node",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.childrenNode.Execute(runtime.WithWorkflowRuntimeContext(context.Background(), tt.runtimeCtx))
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

// plaita 实测（r78）：child 的 input 缺省（求值为 None）→ 子流程继承调用方
// $INPUT——child 与 reference 同款语义（Python 子执行 params=None 走继承）。
func TestChildrenFlow_AbsentInputInheritsCallerInput(t *testing.T) {
	childNode := &Child{
		BaseNode: BaseNode{ID: "child", Next: "next-node"},
		ChildFlow: &workflow.Workflow{
			ID: "test-workflow",
			Nodes: workflow.Nodes{
				&Start{BaseNode{ID: "start", Next: "end"}},
				&End{BaseNode: BaseNode{ID: "end", Output: "$INPUT"}},
			},
		},
	}
	parentCtx := runtime.WithWorkflowRuntimeContext(context.Background(),
		&runtime.Context{WorkflowInput: map[string]any{"z": 42}})
	got, err := childNode.Execute(parentCtx)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	want := map[string]any{"z": 42}
	if !reflect.DeepEqual(got.Result, want) {
		t.Errorf("Execute() got = %v, want %v", got.Result, want)
	}
}

// issue #77 负控：child 节点 outputType 校验失败必须携带真实原因并保持错
// 误链——修复前条件表达式丢弃 ValidateValue 返回值、%w 拼的是上一行已为
// nil 的 err，产出 "%!w(<nil>)" 残片且 Unwrap == nil。
func TestChildrenFlow_OutputTypeMismatchCarriesRealError(t *testing.T) {
	childNode := &Child{
		BaseNode: BaseNode{
			ID: "child",
			OutputType: workflow.Property{
				DataType: workflow.Integer,
				Strict:   true,
			},
		},
		ChildFlow: &workflow.Workflow{
			ID: "child-flow",
			Nodes: workflow.Nodes{
				&Start{BaseNode{ID: "start", Next: "end"}},
				&End{BaseNode: BaseNode{ID: "end", Output: "not-an-int"}},
			},
		},
		Input: "seed",
	}
	_, err := childNode.Execute(runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{}))
	if err == nil {
		t.Fatal("expected output type mismatch error, got nil")
	}
	msg := err.Error()
	if strings.Contains(msg, "%!w(") {
		t.Errorf("error still carries the nil-wrap fragment: %s", msg)
	}
	if !strings.Contains(msg, `expected value data type is "integer"`) {
		t.Errorf("error missing real mismatch reason: %s", msg)
	}
	if errors.Unwrap(err) == nil {
		t.Errorf("error chain broken: Unwrap = nil for %v", err)
	}
}

// issue #77 负控第二形：宽松模型下 required 字段缺失（返回值 nil）同样
// 走这条被吞错的路径，错误文本须带 "value is required but not provided"。
func TestChildrenFlow_OutputTypeRequiredMissingCarriesReason(t *testing.T) {
	childNode := &Child{
		BaseNode: BaseNode{
			ID: "child",
			OutputType: workflow.Property{
				DataType:   workflow.Integer,
				IsRequired: true,
			},
		},
		ChildFlow: &workflow.Workflow{
			ID: "child-flow",
			Nodes: workflow.Nodes{
				&Start{BaseNode{ID: "start", Next: "end"}},
				&End{BaseNode: BaseNode{ID: "end"}}, // 无 Output：子流程返回 nil
			},
		},
		Input: "seed",
	}
	_, err := childNode.Execute(runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{}))
	if err == nil {
		t.Fatal("expected required-missing error, got nil")
	}
	msg := err.Error()
	if strings.Contains(msg, "%!w(") {
		t.Errorf("error still carries the nil-wrap fragment: %s", msg)
	}
	if !strings.Contains(msg, "value is required but not provided") {
		t.Errorf("error missing required-missing reason: %s", msg)
	}
	if errors.Unwrap(err) == nil {
		t.Errorf("error chain broken: Unwrap = nil for %v", err)
	}
}

// issue #77 守恒：input 路径本就正确，文案与错误链不得被本修复波及。
func TestChildrenFlow_InputTypeMismatchMessageUnchanged(t *testing.T) {
	childNode := &Child{
		BaseNode: BaseNode{ID: "child"},
		ChildFlow: &workflow.Workflow{
			ID: "child-flow",
			InputType: workflow.Property{
				DataType: workflow.Integer,
				Strict:   true,
			},
			Nodes: workflow.Nodes{
				&Start{BaseNode{ID: "start", Next: "end"}},
				&End{BaseNode: BaseNode{ID: "end", Output: "done"}},
			},
		},
		Input: "not-an-int",
	}
	_, err := childNode.Execute(runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{}))
	if err == nil {
		t.Fatal("expected input type mismatch error, got nil")
	}
	want := `failed to validate input for children workflow ("child-flow"): expected value data type is "integer" but got type: string, value: not-an-int`
	if err.Error() != want {
		t.Errorf("input path message changed:\n got: %s\nwant: %s", err.Error(), want)
	}
}
