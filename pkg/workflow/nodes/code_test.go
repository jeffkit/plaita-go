package nodes

import (
	"context"
	"errors"
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
			name: "test js code node with input args and global vars",
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
			name: "test js code node with object type input args and global vars",
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

// TestCodeGoBackendRefused pins A62-04: the legacy yaegi backend is gone. A
// flow-authored Go source must be refused at construction (Validate) and, as
// defense in depth, at execution for callers that construct the node directly
// — it must never reach an in-process interpreter with stdlib access.
func TestCodeGoBackendRefused(t *testing.T) {
	node := &Code{
		BaseNode: BaseNode{ID: "code-node", Next: "end"},
		Language: Go,
		Code: `import "os"
func run() (any, error) { return os.ReadFile("/etc/passwd") }`,
		Input: "$INPUT",
	}
	if err := node.Validate(); !errors.Is(err, ErrInProcessGoUnsupported) {
		t.Fatalf("Validate() error = %v, want ErrInProcessGoUnsupported", err)
	}
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{},
	})
	if _, err := node.Execute(ctx); !errors.Is(err, ErrInProcessGoUnsupported) {
		t.Fatalf("Execute() error = %v, want ErrInProcessGoUnsupported", err)
	} else if workflow.IsRetryableError(err) {
		t.Error("a backend refusal must be deterministic (no retry storm)")
	}
}

// TestCodeValidateAdmitsSupportedBackends is the positive control: the two
// implemented backends still pass construction validation.
func TestCodeValidateAdmitsSupportedBackends(t *testing.T) {
	for _, node := range []*Code{
		{BaseNode: BaseNode{ID: "js"}, Language: JS, Code: "function run(i){return i}", Input: "$INPUT"},
		{BaseNode: BaseNode{ID: "py"}, Language: LanguagePython, Code: "def run(input):\n    return input", Input: "$INPUT"},
	} {
		if err := node.Validate(); err != nil {
			t.Errorf("Validate(%s) = %v", node.Language, err)
		}
	}
}
