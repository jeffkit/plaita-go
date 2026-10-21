package interpreter

import (
	"context"
	"errors"
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

// errJsInterrupted is the cause handed to Runtime.Interrupt when the context
// is done; it surfaces wrapped in *goja.InterruptedError at the pending
// RunString/function call, letting eval re-classify the abort against ctx.Err().
var errJsInterrupted = errors.New("js interpreter: interrupted by context")

func (i *jsInterpreter) eval(ctx context.Context, src string, evalArgs *JsEvalArgs) (any, error) {
	// #48: the vm is ctx-blind by itself — an infinite loop in flow-authored
	// JS pinned the single consumer past the execution budget forever, and
	// the deadline's terminal classification (which only runs once Execute
	// returns) never fired. Interrupt is safe to call from another goroutine
	// while code is running; fired while idle it aborts the next run.
	if ctx != nil {
		if done := ctx.Done(); done != nil {
			stop := make(chan struct{})
			defer close(stop)
			go func() {
				select {
				case <-done:
					i.interpreter.Interrupt(errJsInterrupted)
				case <-stop:
				}
			}()
		}
	}
	_, err := i.interpreter.RunString(src)
	if err != nil {
		if interrupted := jsInterruptedByContext(ctx, err); interrupted != nil {
			return nil, interrupted
		}
		return nil, fmt.Errorf("js interpreter: eval source code failed: %v", err)
	}
	jsFunction, ok := goja.AssertFunction(i.interpreter.Get(evalArgs.FuncName))
	if !ok {
		return nil, fmt.Errorf("js interpreter: failed to eval func, funcname is %s", evalArgs.FuncName)
	}
	result, err := jsFunction(goja.Undefined(), i.interpreter.ToValue(evalArgs.Parameters))
	if err != nil {
		if interrupted := jsInterruptedByContext(ctx, err); interrupted != nil {
			return nil, interrupted
		}
		return nil, fmt.Errorf("js interpreter: run js function failed: %v", err)
	}
	return result.Export(), nil
}

// jsInterruptedByContext re-classifies a goja interruption that the context
// caused. The error wraps ctx.Err() — context.DeadlineExceeded for a node
// timeout (absorbable by the timeout policy, as any ctx-aware node) and
// context.Canceled for a worker cancellation (NOT a contextTimeoutError, so
// a deadline/cancel cannot be swallowed by a continue-with timeout handler).
func jsInterruptedByContext(ctx context.Context, err error) error {
	if ctx == nil || ctx.Err() == nil {
		return nil
	}
	var ie *goja.InterruptedError
	if errors.As(err, &ie) {
		return fmt.Errorf("js interpreter: execution interrupted: %w", ctx.Err())
	}
	return nil
}
