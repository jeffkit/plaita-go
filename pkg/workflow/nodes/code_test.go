package nodes

import (
	"context"
	"reflect"
	"testing"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func TestCode_Execute(t *testing.T) {
	tests := []struct {
		name       string
		runtimeCtx *runtime.Context
		codeNode   *Code
		want       *workflow.ExecuteResult
		wantErr    bool
	}{
		{
			name: "test simple golang code node",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"name": "foo",
				},
			},
			codeNode: &Code{
				BaseNode: BaseNode{
					ID:         "code-node",
					Next:       "end",
					OutputType: workflow.Property{DataType: workflow.String},
				},
				Language: Go,
				Code: `
				import "context"

				func run(_ context.Context, input any) (any, error) {
					return "hello " + input.(string), nil
				}
				`,
				Input: "$INPUT.name",
			},
			want:    &workflow.ExecuteResult{Result: "hello foo", NextNode: "end"},
			wantErr: false,
		},
		{
			name: "test go code node with input args and global vars",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"name": "foo",
				},
			},
			codeNode: &Code{
				BaseNode: BaseNode{
					ID:         "code-node",
					Next:       "end",
					OutputType: workflow.Property{DataType: workflow.String},
				},
				Language: Go,
				Code: `
				import (
					"context"
					"errors"
					"fmt"
				)

				const prefix = "foo"

				func parseInput(input any) (string, error) {
					if v, ok := input.(string); ok {
						return v, nil
					}
					return "", errors.New("invalid input")
				}

				func run(_ context.Context, input any) (any, error) {
					name, err := parseInput(input)
					if err != nil {
						return nil, err
					}
					return fmt.Sprintf("%s: hello, %s", prefix, name), nil
				}
				`,
				Input: "$INPUT.name",
			},
			want:    &workflow.ExecuteResult{Result: "foo: hello, foo", NextNode: "end"},
			wantErr: false,
		},
		{
			name: "test go code node with object type input args and global vars",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"name": "kong",
					"age":  18,
				},
			},
			codeNode: &Code{
				BaseNode: BaseNode{
					ID:         "code-node",
					Next:       "end",
					OutputType: workflow.Property{DataType: workflow.String},
				},
				Language: Go,
				Code: `
				import (
					"context"
					"errors"
					"fmt"
				)

				const prefix = "foo"

				func parseInput(input any) (map[string]any, error) {
					if v, ok := input.(map[string]any); ok {
						return v, nil
					}
					return nil, errors.New("invalid input")
				}

				func run(_ context.Context, input any) (any, error) {
					fmt.Println(input)
					info, err := parseInput(input)
					if err != nil {
						return nil, err
					}
					return fmt.Sprintf("%s: hello, %s", prefix, info["name"]), nil
				}
				`,
				Input: "$INPUT",
			},
			want:    &workflow.ExecuteResult{Result: "foo: hello, kong", NextNode: "end"},
			wantErr: false,
		},
		{
			name: "test simple js code node",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"name": "foo",
				},
			},
			codeNode: &Code{
				BaseNode: BaseNode{
					ID:         "code-node",
					Next:       "end",
					OutputType: workflow.Property{DataType: workflow.String},
				},
				Language: JS,
				Code: `
				function run(name) {
				  return "hello "+name;
				}
				`,
				Input: "$INPUT.name",
			},
			want:    &workflow.ExecuteResult{Result: "hello foo", NextNode: "end"},
			wantErr: false,
		},
		{
			name: "test go code node with input args and global vars",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"name": "foo",
				},
			},
			codeNode: &Code{
				BaseNode: BaseNode{
					ID:         "code-node",
					Next:       "end",
					OutputType: workflow.Property{DataType: workflow.String},
				},
				Language: JS,
				Code: `
				const prefix = "foo";
				function parseInput(input) {
				  if (typeof input === "string") {
					return input;
				  }
				  throw new Error("invalid input");
				}
		
				function run(input) {
				  try {
					const info = parseInput(input);
					return "hello "+info;
				  } catch (err) {
					return err;
				  }
				}
				`,
				Input: "$INPUT.name",
			},
			want:    &workflow.ExecuteResult{Result: "hello foo", NextNode: "end"},
			wantErr: false,
		},
		{
			name: "test go code node with object type input args and global vars",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"name": "kong",
					"age":  18,
				},
			},
			codeNode: &Code{
				BaseNode: BaseNode{
					ID:         "code-node",
					Next:       "end",
					OutputType: workflow.Property{DataType: workflow.String},
				},
				Language: JS,
				Code: `
					const prefix = "foo";
		
					function parseInput(input) {
					  if (typeof input === "object" && input !== null) {
						return input;
					  }
					  throw new Error("invalid input");
					}
		
					function run(input) {
					  try {
						const info = parseInput(input);
						return prefix + ": hello " + info.name;
					  } catch (err) {
						return err;
					  }
					}
				`,
				Input: "$INPUT",
			},
			want:    &workflow.ExecuteResult{Result: "foo: hello kong", NextNode: "end"},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.codeNode.Execute(runtime.WithWorkflowRuntimeContext(context.Background(), tt.runtimeCtx))
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
