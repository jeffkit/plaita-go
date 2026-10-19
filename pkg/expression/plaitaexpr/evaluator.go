// Package plaitaexpr provides plaita expression
package plaitaexpr

import (
	"context"
	"errors"
	"fmt"

	"github.com/jinzhu/copier"
	"github.com/maja42/goval"
	"github.com/mcuadros/go-lookup"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/expression/plaitaexpr/parser"
	"github.com/jeffkit/plaita-go/pkg/model"
)

// Evaluator plaita expression evaluator
type Evaluator struct{}

// NewEvaluator creates a new plaita expression evaluator
func NewEvaluator() *Evaluator {
	return new(Evaluator)
}

// Eval evaluates a plaita expression
func (e *Evaluator) Eval(
	ctx context.Context,
	expr string,
	parameters map[string]any,
) (model.Value, error) {
	parsedExpr, err := parser.Parse(expr)
	if err != nil {
		return model.Value{}, err
	}

	switch parsedExpr.Type() {
	case parser.ExprFunction:
		return e.evalFunc(ctx, parsedExpr, parameters)
	case parser.ExprValue:
		return parsedExpr.Value(), nil
	case parser.ExprVariable:
		return e.evalVariable(ctx, parsedExpr, parameters)
	default:
		return model.Value{}, errors.New("unknown expression type")
	}
}

func execArgsFromRuntimeCtx(ctx context.Context) map[string]any {
	if ctx == nil {
		return nil
	}
	runtimeCtx := runtime.WorkflowRuntimeContext(ctx)
	execArgs := map[string]any{
		string(parser.ValueSourceInput):  runtimeCtx.WorkflowInput,
		string(parser.ValueSourceNode):   runtimeCtx.ExecuteNodeResults,
		string(parser.ValueSourceParent): execArgsFromRuntimeCtx(runtimeCtx.ParentWorkflowContext),
		string(parser.ValueSourceENV):    runtimeCtx.EnvVariables,
		string(parser.ValueSourceERROR):  runtimeCtx.ExecuteNodeErrors,
		string(parser.ValueSourceGlobal): runtimeCtx.GlobalVariables,
		string(parser.ValueNULL):         nil,
		string(parser.ValueEmptyString):  "",
	}
	deepCopy(runtimeCtx.CurrentNodeContext, &execArgs)
	return execArgs
}

func (e *Evaluator) evalFunc(
	ctx context.Context,
	expr parser.Expr,
	parameters map[string]any,
) (model.Value, error) {
	execArgs := execArgsFromRuntimeCtx(ctx)
	deepCopy(parameters, &execArgs)
	result, err := goval.NewEvaluator().Evaluate(expr.Statement(), execArgs, AllFunctions())
	return model.NewValue(result), err
}

func (e *Evaluator) evalVariable(
	ctx context.Context,
	expr parser.Expr,
	parameters map[string]any,
) (model.Value, error) {
	runtimeCtx := runtime.WorkflowRuntimeContext(ctx)
	variables := expr.Variables()
	if len(variables) == 0 {
		return model.Value{}, nil
	}

	variable := variables[0]
	switch variable.Source {
	case parser.ValueSourceInput:
		if variable.Variable == "INPUT" && len(variable.WithoutSourceVariable) == 0 {
			return model.NewValue(runtimeCtx.WorkflowInput), nil // TODO: deep copy input map
		}
		val, err := lookup.LookupString(runtimeCtx.WorkflowInput, variable.WithoutSourceVariable)
		if err != nil {
			return model.Value{}, fmt.Errorf("failed to lookup input variable %q: %w", variable.Variable, err)
		}
		return model.NewValue(val.Interface()), nil
	case parser.ValueSourceNode:
		val, err := lookup.LookupString(runtimeCtx.ExecuteNodeResults, variable.WithoutSourceVariable)
		if err != nil {
			return model.Value{}, fmt.Errorf("failed to lookup executed node result var %q: %w", variable.Variable, err)
		}
		return model.NewValue(val.Interface()), nil
	case parser.ValueSourceParent:
		newExpr, err := parser.NewVariableExpr(variable.WithoutSourceVariable)
		if err != nil {
			return model.Value{}, fmt.Errorf("failed to create new variable expression: %w", err)
		}
		return e.evalVariable(runtimeCtx.ParentWorkflowContext, newExpr, parameters)
	case parser.ValueSourceERROR:
		val, err := lookup.LookupStringI(runtimeCtx.ExecuteNodeErrors, variable.WithoutSourceVariable)
		if err != nil {
			return model.Value{}, fmt.Errorf("failed to lookup executed error result var %q: %w", variable.Variable,
				err)
		}
		return model.NewValue(val.Interface()), nil
	case parser.ValueSourceENV:
		val, err := lookup.LookupString(runtimeCtx.EnvVariables, variable.WithoutSourceVariable)
		if err != nil {
			return model.Value{}, fmt.Errorf("failed to lookup executed error result var %q: %w", variable.Variable,
				err)
		}
		return model.NewValue(val.Interface()), nil
	case parser.ValueSourceGlobal:
		val, err := lookup.LookupString(runtimeCtx.GlobalVariables, variable.WithoutSourceVariable)
		if err != nil {
			return model.Value{}, fmt.Errorf("failed to lookup executed error result var %q: %w", variable.Variable,
				err)
		}
		return model.NewValue(val.Interface()), nil
	case parser.ValueNULL:
		return model.NewValue(nil), nil
	case parser.ValueEmptyString:
		return model.NewValue(""), nil
	default:
		execArgs := make(map[string]any)
		deepCopy(runtimeCtx.CurrentNodeContext, &execArgs)
		deepCopy(parameters, &execArgs)
		val, err := lookup.LookupString(execArgs, variable.Variable)
		if err != nil {
			return model.Value{}, fmt.Errorf("failed to lookup parameter var %q: %w", variable.Variable, err)
		}
		return model.NewValue(val.Interface()), nil
	}
}

func deepCopy(from, to any) {
	_ = copier.CopyWithOption(to, from, copier.Option{IgnoreEmpty: true, DeepCopy: true})
}
