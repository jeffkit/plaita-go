// Package plaitaexpr provides plaita expression
package plaitaexpr

import (
	"context"
	"errors"
	"fmt"
	"strings"

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
	applyNodeRawAccessorToExecArgs(expr, execArgs)
	// Unknown $F.<name> calls resolve to the "undefined" sentinel instead of
	// failing (plaita semantics); see semantics.go. Arguments stay eager:
	// goval evaluates them before resolving the function, so a failing
	// argument still errors like plaita's KeyError does.
	functions := resolveFunctions(expr.Statement(), AllFunctions())
	result, err := goval.NewEvaluator().Evaluate(expr.Statement(), execArgs, functions)
	return model.NewValue(result), err
}

// applyNodeRawAccessorToExecArgs materializes the `$NODE.raw.<node>...`
// variables of a function statement into the exec args copy. goval resolves
// statement variables by plain map navigation, which cannot interpret the
// plaita#130 raw modifier, so each raw path is pre-resolved with the plaita
// traversal and materialized under a synthetic `raw` subtree of a copied NODE
// map (the real node results container is never mutated).
func applyNodeRawAccessorToExecArgs(expr parser.Expr, execArgs map[string]any) {
	results, _ := execArgs[string(parser.ValueSourceNode)].(map[string]any)
	if results == nil {
		return
	}
	var synthetic map[string]any
	for _, variable := range expr.Variables() {
		if variable.Source != parser.ValueSourceNode {
			continue
		}
		// Function-statement variables carry the full dotted path (source
		// prefix included) in Variable; WithoutSourceVariable is unset.
		nodePath := strings.TrimPrefix(variable.Variable, string(parser.ValueSourceNode)+".")
		rawPath, ok := nodeRawAccessorPath(results, nodePath)
		if !ok {
			continue
		}
		val, err := lookupPathPlaita(results, rawPath, false)
		if err != nil {
			val = nil
		}
		if synthetic == nil {
			synthetic = make(map[string]any)
		}
		setNestedValue(synthetic, strings.Split(rawPath, "."), val)
	}
	if synthetic == nil {
		return
	}
	nodeCopy := make(map[string]any, len(results)+1)
	for k, v := range results {
		nodeCopy[k] = v
	}
	nodeCopy[rawPathModifier] = synthetic
	execArgs[string(parser.ValueSourceNode)] = nodeCopy
}

// setNestedValue writes value into m at the given segment path, creating
// intermediate maps as needed.
func setNestedValue(m map[string]any, segments []string, value any) {
	cur := m
	for i, seg := range segments {
		if i == len(segments)-1 {
			cur[seg] = value
			return
		}
		next, ok := cur[seg].(map[string]any)
		if !ok {
			next = make(map[string]any)
			cur[seg] = next
		}
		cur = next
	}
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
		if len(variable.WithoutSourceVariable) == 0 {
			return model.NewValue(runtimeCtx.WorkflowInput), nil // TODO: deep copy input map
		}
		// plaita missing-key semantics: a missing field on an existing input
		// yields nil; only a nil/absent input container or an index miss is
		// an error (see semantics.go).
		val, err := lookupPathPlaita(runtimeCtx.WorkflowInput, variable.WithoutSourceVariable, false)
		if err != nil {
			return model.Value{}, fmt.Errorf("failed to lookup input variable %q: %w", variable.Variable, err)
		}
		return model.NewValue(val), nil
	case parser.ValueSourceNode:
		if len(variable.WithoutSourceVariable) == 0 {
			return model.NewValue(runtimeCtx.ExecuteNodeResults), nil
		}
		// plaita#130: `$NODE.raw.<node>[.path...]` reads the stored node
		// result verbatim (the modifier is stripped; strings are never
		// re-evaluated at any depth). A node literally named `raw` keeps
		// being resolved as that node.
		path := variable.WithoutSourceVariable
		if rawPath, ok := nodeRawAccessorPath(runtimeCtx.ExecuteNodeResults, path); ok {
			path = rawPath
		}
		val, err := lookupPathPlaita(runtimeCtx.ExecuteNodeResults, path, false)
		if err != nil {
			return model.Value{}, fmt.Errorf("failed to lookup executed node result var %q: %w", variable.Variable, err)
		}
		return model.NewValue(val), nil
	case parser.ValueSourceParent:
		newExpr, err := parser.NewVariableExpr(variable.WithoutSourceVariable)
		if err != nil {
			return model.Value{}, fmt.Errorf("failed to create new variable expression: %w", err)
		}
		return e.evalVariable(runtimeCtx.ParentWorkflowContext, newExpr, parameters)
	case parser.ValueSourceERROR:
		// $ERROR is a Go-runtime-specific root with no plaita counterpart;
		// it keeps its legacy case-insensitive lookup and error-on-miss
		// semantics unchanged.
		val, err := lookup.LookupStringI(runtimeCtx.ExecuteNodeErrors, variable.WithoutSourceVariable)
		if err != nil {
			return model.Value{}, fmt.Errorf("failed to lookup executed error result var %q: %w", variable.Variable,
				err)
		}
		return model.NewValue(val.Interface()), nil
	case parser.ValueSourceENV:
		val, err := lookupPathPlaita(runtimeCtx.EnvVariables, variable.WithoutSourceVariable, false)
		if err != nil {
			return model.Value{}, fmt.Errorf("failed to lookup executed error result var %q: %w", variable.Variable,
				err)
		}
		return model.NewValue(val), nil
	case parser.ValueSourceGlobal:
		val, err := lookupPathPlaita(runtimeCtx.GlobalVariables, variable.WithoutSourceVariable, false)
		if err != nil {
			return model.Value{}, fmt.Errorf("failed to lookup executed error result var %q: %w", variable.Variable,
				err)
		}
		return model.NewValue(val), nil
	case parser.ValueNULL:
		return model.NewValue(nil), nil
	case parser.ValueEmptyString:
		return model.NewValue(""), nil
	default:
		execArgs := make(map[string]any)
		deepCopy(runtimeCtx.CurrentNodeContext, &execArgs)
		deepCopy(parameters, &execArgs)
		// The first segment names a context root: a miss there is plaita's
		// KeyError (error); misses deeper down follow the field-miss-yields-
		// nil semantics.
		val, err := lookupPathPlaita(execArgs, variable.Variable, true)
		if err != nil {
			return model.Value{}, fmt.Errorf("failed to lookup parameter var %q: %w", variable.Variable, err)
		}
		return model.NewValue(val), nil
	}
}

func deepCopy(from, to any) {
	_ = copier.CopyWithOption(to, from, copier.Option{IgnoreEmpty: true, DeepCopy: true})
}
