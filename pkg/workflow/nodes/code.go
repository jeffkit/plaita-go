package nodes

import (
	"context"
	"errors"
	"fmt"
	"regexp"

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
	Go    LanguageType = "go"
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
	GO_PACKAGE_NAME = "plaita_go_inline"
	GO_FUNC_NAME    = "run"
	JS_FUNC_NAME    = "run"
	// PY_FUNC_NAME mirrors plaita PYTHON_FUNC_NAME: the user script must
	// define a function named "run" taking the decoded JSON input.
	PY_FUNC_NAME = "run"
)

var builtinLanguageTypes = []LanguageType{Go, JS, LanguagePython}

// GoArgs represents the arguments for go interpreter.
type GoArgs struct {
	PackageName string `json:"packageName"`
	FuncName    string `json:"funcName"`
}

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

// Validate validates the go args.
func (g *GoArgs) Validate() error {
	return validation.ValidateStruct(g,
		validation.Field(&g.PackageName, validation.Required.Error("package name is required")),
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
		result, err = c.interpretGoCode(ctx, arguments)
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

func (c *Code) interpretGoCode(ctx context.Context, arguments any) (any, error) {
	evalArgs := &interpreter.GoEvalArgs{
		PackageName: GO_PACKAGE_NAME,
		FuncName:    GO_FUNC_NAME,
		Parameters:  arguments,
	}

	codeStr := c.Code
	r := regexp.MustCompile(`package (\w+)`)
	if r.MatchString(codeStr) {
		codeStr = r.ReplaceAllString(codeStr, "package "+GO_PACKAGE_NAME)
	} else {
		codeStr = "package " + GO_PACKAGE_NAME + "\n" + codeStr
	}

	// TODO(scottzhlin): optimize me, don't new interpreter per eval, use a global interpreter pool.
	return interpreter.NewGoInterpreter().Eval(ctx, codeStr, interpreter.WithGoEvalArgs(evalArgs))
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
