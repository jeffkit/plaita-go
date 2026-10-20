package nodes

import (
	"context"
	"errors"
	"fmt"

	validation "github.com/go-ozzo/ozzo-validation"
	"github.com/samber/lo"

	"github.com/jeffkit/plaita-go/internal/interpreter"
	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func init() {
	workflow.RegisterNodeType(
		types.NodeTypeCode,
		func(ctx context.Context) workflow.Node { return new(Code) },
	)
}

// LanguageType represents the programming language supported by code node
type LanguageType string

// supported language types.
const (
	// Go is the legacy in-process (yaegi) backend. The constant stays so old
	// IR documents are recognized and rejected by name; the interpreter itself
	// was removed (A62-04 — see ErrInProcessGoUnsupported).
	Go LanguageType = "go"
	// Shell and PHP are recognized historical values, not admitted backends.
	Shell LanguageType = "shell"
	PHP   LanguageType = "php"
	JS    LanguageType = "javascript"
	// LanguagePython routes to the python3 subprocess sidecar, mirroring the
	// plaita CodeNode "subprocess" backend (plaita/node/code.py): user code
	// embedded in a runner script, JSON via stdin/stdout, wall-clock timeout
	// and process-group kill. See code_python.go for the contract.
	LanguagePython LanguageType = "python"
)

const (
	// JS_FUNC_NAME is the entrypoint the javascript source must define.
	JS_FUNC_NAME = "run"
	// PY_FUNC_NAME mirrors plaita PYTHON_FUNC_NAME: the user script must
	// define a function named "run" taking the decoded JSON input.
	PY_FUNC_NAME = "run"
)

// builtinLanguageTypes is the admitted backend set. Go/yaegi is deliberately
// absent (A62-04): executing flow-authored Go source in the worker process
// gives it stdlib-level filesystem and network access with no isolation and no
// interruption boundary.
var builtinLanguageTypes = []LanguageType{JS, LanguagePython}

// ErrInProcessGoUnsupported is the construction-time refusal of the yaegi
// backend (A62-04). Go code has no sandboxed execution path in this engine;
// the plaita runtime's sandboxed code backends are the supported home for it.
var ErrInProcessGoUnsupported = errors.New(
	"code node: in-process go (yaegi) execution is not supported by this engine")

// JsArgs represents the arguments for js interpreter.
type JsArgs struct {
	FileName string `json:"fileName"`
	FuncName string `json:"funcName"`
}

// Validate validates the js args.
func (g *JsArgs) Validate() error {
	return validation.ValidateStruct(g,
		validation.Field(&g.FuncName, validation.Required.Error("function name is required")))
}

// Code is a node that executes a block of code.
type Code struct {
	BaseNode

	Language LanguageType `json:"language"`
	Code     string       `json:"code"`
	Input    any          `json:"input,omitempty"`
	// SandboxBackend carries the plaita sandbox_backend declaration
	// (snake_case folded to camelCase by the ir loader). The Go engine
	// implements only the "subprocess" form for python — see code_python.go;
	// anything else is rejected in Validate (fail-closed, same split as the
	// ir loader's translateCode).
	SandboxBackend string `json:"sandboxBackend,omitempty"`
}

// pyBackendSubprocess is the only sandbox_backend value the Go engine
// implements for python (plaita CodeNode backend tier of the same name).
const pyBackendSubprocess = "subprocess"

// Validate validates the code node.
func (c *Code) Validate() error {
	// Fail closed on the legacy in-process Go backend before the generic
	// language check, so the error names the real reason (A62-04).
	if c.Language == Go {
		return fmt.Errorf("%w: language %q is refused at construction time; javascript (goja) and "+
			"python (subprocess sidecar) are the supported backends, or run the flow on the "+
			"plaita runtime", ErrInProcessGoUnsupported, c.Language)
	}
	if err := validation.ValidateStruct(c,
		validation.Field(&c.BaseNode, validation.Required),
		validation.Field(
			&c.Language,
			validation.Required.Error("language type is required"),
			validation.In(lo.ToAnySlice(builtinLanguageTypes)...).
				Error(fmt.Sprintf("language must be one of %v", builtinLanguageTypes)),
		),
		validation.Field(&c.Code, validation.Required.Error("source code is required"))); err != nil {
		return err
	}
	switch c.SandboxBackend {
	case "", pyBackendSubprocess:
	default:
		return fmt.Errorf(
			"code node: sandboxBackend %q is not supported in this engine: the Go "+
				"sidecar runs python code only in the subprocess form (fresh python3 "+
				"process, wall-clock timeout, process-group kill) and provides no "+
				"in-process or container isolation; drop the field, set %q, or run "+
				"such flows on the plaita runtime",
			c.SandboxBackend, pyBackendSubprocess)
	}
	if c.Language == LanguagePython && !pyDefinesRunFunction(c.Code) {
		return fmt.Errorf("code node: python code validation failed: No %s function found", PY_FUNC_NAME)
	}
	return nil
}

// GetType returns the type of the code node.
func (c *Code) GetType() workflow.NodeType {
	return types.NodeTypeCode
}

// Execute executes the code node, it invokes language interpreter to interpret the code.
func (c *Code) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	arguments, err := expression.Eval(ctx, c.Input)
	if err != nil {
		return nil, fmt.Errorf("code node: failed to evaluate input: %w", err)
	}

	var result any
	switch c.Language {
	case Go:
		// Defense in depth: callers that construct the node directly (without
		// Validate) must hit the same refusal, never the removed yaegi path.
		// Deterministic: a retry cannot make an unsupported backend runnable.
		return nil, workflow.NonRetryable(fmt.Errorf("%w: refusing language %q",
			ErrInProcessGoUnsupported, c.Language))
	case PHP:
		result, err = c.interpretPHPCode(ctx, arguments)
	case JS:
		result, err = c.interpretJsCode(ctx, arguments)
	case LanguagePython:
		result, err = c.interpretPythonCode(ctx, arguments)
	default:
		return nil, fmt.Errorf("code node: unsupported language type: %s", c.Language)
	}

	if err != nil {
		return nil, fmt.Errorf("code node: failed to interpret code: %w", err)
	}

	if err := c.OutputType.ValidateValue(result); err != nil {
		return nil, fmt.Errorf("code node: invalid output value: %w, result value: %v", err, result)
	}

	return &workflow.ExecuteResult{Result: result, NextNode: c.NextNode()}, nil
}

func (c *Code) interpretPHPCode(ctx context.Context, arguments any) (any, error) {

	return nil, errors.New("code node: interpret php code is not implemented")
}

func (c *Code) interpretJsCode(ctx context.Context, arguments any) (any, error) {
	evalArgs := &interpreter.JsEvalArgs{
		FuncName:   JS_FUNC_NAME,
		Parameters: arguments,
	}

	codeStr := c.Code
	return interpreter.NewJsInterpreter().Eval(ctx, codeStr, interpreter.WithJsEvalArgs(evalArgs))
}

// interpretPythonCode runs the python sidecar (code_python.go): the wrapper
// contract mirrors the plaita CodeNode subprocess backend verbatim.
func (c *Code) interpretPythonCode(ctx context.Context, arguments any) (any, error) {
	return runPythonSubprocess(ctx, c.Code, arguments)
}
