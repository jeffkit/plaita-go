package interpreter

import (
	"context"
	"fmt"

	"github.com/dop251/goja"
	validation "github.com/go-ozzo/ozzo-validation"
)

// NewJsInterpreter creates a new js interpreter.
func NewJsInterpreter() Interpreter {
	vm := goja.New()
	return &jsInterpreter{
		interpreter: vm,
	}
}

type jsInterpreter struct {
	interpreter *goja.Runtime
}

// JsEvalArgs represents the arguments for js interpreter.
type JsEvalArgs struct {
	FuncName   string
	Parameters any
}

// Validate validates the js eval args.
func (g *JsEvalArgs) Validate() error {
	return validation.ValidateStruct(g,
		validation.Field(&g.FuncName, validation.Required.Error("func name is required")),
	)
}

// Eval evaluates the js source code.
func (i *jsInterpreter) Eval(ctx context.Context, src string, opts ...Option) (any, error) {
	o := applyOptions(opts...)
	if o.jsEvalArgs == nil {
		return nil, fmt.Errorf("js interpreter: js eval args is required")
	}
	if err := o.jsEvalArgs.Validate(); err != nil {
		return nil, fmt.Errorf("js interpreter: validate js eval args: %w", err)
	}
	return i.eval(ctx, src, o.jsEvalArgs)
}

func (i *jsInterpreter) eval(ctx context.Context, src string, evalArgs *JsEvalArgs) (any, error) {
	_, err := i.interpreter.RunString(src)
	if err != nil {
		return nil, fmt.Errorf("js interpreter: eval source code failed: %v", err)
	}
	jsFunction, ok := goja.AssertFunction(i.interpreter.Get(evalArgs.FuncName))
	if !ok {
		return nil, fmt.Errorf("js interpreter: failed to eval func, funcname is %s", evalArgs.FuncName)
	}
	result, err := jsFunction(goja.Undefined(), i.interpreter.ToValue(evalArgs.Parameters))
	if err != nil {
		return nil, fmt.Errorf("js interpreter: run js function failed: %v", err)
	}
	return result.Export(), nil
}
