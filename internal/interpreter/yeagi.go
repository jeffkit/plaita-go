package interpreter

import (
	"context"
	"fmt"

	validation "github.com/go-ozzo/ozzo-validation"
	"github.com/traefik/yaegi/interp"
	"github.com/traefik/yaegi/stdlib"
)

// EvalFunc represents the entrypoint function that evaluates the Go programming language source code.
type EvalFunc = func(ctx context.Context, parameters any) (any, error)

// NewGoInterpreter returns a new go interpreter.
func NewGoInterpreter() Interpreter {
	i := interp.New(interp.Options{})
	if err := i.Use(stdlib.Symbols); err != nil {
		panic(fmt.Errorf("go interpreter: failed to use stdlib: %w", err))
	}
	return &goInterpreter{
		interpreter: i,
	}
}

type goInterpreter struct {
	interpreter *interp.Interpreter
}

// GoEvalArgs represents the run arguments for go interpreter eval.
type GoEvalArgs struct {
	PackageName string
	FuncName    string
	Parameters  any // func parameters
}

// Validate validates the go eval args.
func (g *GoEvalArgs) Validate() error {
	return validation.ValidateStruct(g,
		validation.Field(&g.PackageName, validation.Required.Error("package name is required")),
		validation.Field(&g.FuncName, validation.Required.Error("func name is required")))
}

// PackageFuncName returns the func name with package.
func (g *GoEvalArgs) PackageFuncName() string {
	return fmt.Sprintf("%s.%s", g.PackageName, g.FuncName)
}

// Eval evaluates the Go source code.
// TODO(scottzhlin): 添加解析器缓存 避免每次Eval调用都重新编译和解析源代码
func (i *goInterpreter) Eval(ctx context.Context, src string, opts ...Option) (any, error) {
	o := applyOptions(opts...)

	if o.goEvalArgs == nil {
		return nil, fmt.Errorf("go interpreter: go eval args is required")
	}

	if err := o.goEvalArgs.Validate(); err != nil {
		return nil, fmt.Errorf("go interpreter: validate go eval args: %w", err)
	}

	return i.eval(ctx, src, o.goEvalArgs)
}

func (i *goInterpreter) eval(ctx context.Context, src string, evalArgs *GoEvalArgs) (any, error) {
	_, err := i.interpreter.EvalWithContext(ctx, src)
	if err != nil {
		return nil, fmt.Errorf("go interpreter: eval source code: %w", err)
	}

	pfn := evalArgs.PackageFuncName()
	v, err := i.interpreter.EvalWithContext(ctx, pfn)
	if err != nil {
		return nil, fmt.Errorf("go interpreter: failed to eval func %q: %w", pfn, err)
	}

	f, ok := v.Interface().(EvalFunc)
	if !ok {
		return nil, fmt.Errorf("go interpreter: invalid func type, type: %T", v.Interface())
	}

	return f(ctx, evalArgs.Parameters)
}
