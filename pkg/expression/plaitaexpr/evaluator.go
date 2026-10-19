// Package plaitaexpr provides the plaita expression language
package plaitaexpr

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/jinzhu/copier"
	"github.com/maja42/goval"
	"github.com/mcuadros/go-lookup"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/expression/plaitaexpr/parser"
	"github.com/jeffkit/plaita-go/pkg/model"
)

// Evaluator evaluates plaita expressions
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
	statement, err := preResolveStatementVariables(expr, expr.Statement(), execArgs)
	if err != nil {
		return model.Value{}, err
	}
	// Unknown $F.<name> calls fail loudly: goval panics with
	// `no such function` and its caller recovers it as an error — plaita's
	// default-registry NameError. Arguments stay eager: goval evaluates them
	// before resolving the function, so a failing argument errors first.
	result, err := goval.NewEvaluator().Evaluate(statement, execArgs, AllFunctions())
	return model.NewValue(result), err
}

// preResolvedPrefix is the synthetic identifier prefix under which
// pre-resolved statement variables are stored in the exec args.
const preResolvedPrefix = "__pv"

// preResolveStatementVariables resolves every built-in-root variable of a
// function statement with the plaita traversal (raw modifier, bracket index
// segments, missing-key semantics) and rewrites the statement so goval only
// sees plain synthetic identifiers. goval's own member navigation cannot
// interpret plaita path semantics; $PARENT variables and parameter
// identifiers keep goval's navigation.
func preResolveStatementVariables(expr parser.Expr, statement string, execArgs map[string]any) (string, error) {
	vars := expr.Variables()
	if len(vars) == 0 {
		return statement, nil
	}

	type rewrite struct{ from, to string }
	var rewrites []rewrite
	// longest paths first so INPUT.a.b is rewritten before INPUT.a
	sorted := make([]parser.VariableExpr, len(vars))
	copy(sorted, vars)
	sort.Slice(sorted, func(i, j int) bool { return len(sorted[i].Variable) > len(sorted[j].Variable) })

	for i, v := range sorted {
		if !isBuiltinStatementSource(v.Source) {
			continue
		}
		// a whole-root variable ($INPUT itself) has an empty path
		pathOnly := ""
		if rest, ok := strings.CutPrefix(v.Variable, string(v.Source)+"."); ok {
			pathOnly = rest
		}
		container := execArgs[string(v.Source)]
		if v.Source == parser.ValueSourceNode {
			if results, ok := container.(map[string]any); ok {
				if rawPath, ok2 := nodeRawAccessorPath(results, pathOnly); ok2 {
					pathOnly = rawPath
				}
			}
		}
		val, err := lookupPathPlaita(container, pathOnly, false)
		if err != nil {
			return "", fmt.Errorf("failed to lookup $%s: %w", v.Variable, err)
		}
		synthetic := fmt.Sprintf("%s%d", preResolvedPrefix, i)
		execArgs[synthetic] = val
		rewrites = append(rewrites, rewrite{from: v.Variable, to: synthetic})
	}

	for _, r := range rewrites {
		statement = replaceStatementVariable(statement, r.from, r.to)
	}
	return statement, nil
}

// isBuiltinStatementSource reports whether a statement variable addresses a
// runtime-context root that the plaita traversal should resolve.
func isBuiltinStatementSource(source parser.ValueSource) bool {
	switch source {
	case parser.ValueSourceInput, parser.ValueSourceNode, parser.ValueSourceENV,
		parser.ValueSourceGlobal, parser.ValueSourceERROR:
		return true
	}
	return false
}

// replaceStatementVariable replaces every boundary-safe occurrence of the
// dotted variable path with the synthetic identifier.
func replaceStatementVariable(statement, from, to string) string {
	re := regexp.MustCompile(`(^|[^A-Za-z0-9_])` + regexp.QuoteMeta(from) + `($|[^A-Za-z0-9_])`)
	return re.ReplaceAllString(statement, "${1}"+to+"${2}")
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
