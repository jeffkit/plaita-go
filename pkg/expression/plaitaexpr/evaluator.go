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
	// Map roots are read through snapshot accessors: the owning flow may keep
	// writing node results/globals while a background parallel branch
	// evaluates $PARENT.<...> against this context (issue #17 -race).
	execArgs := map[string]any{
		string(parser.ValueSourceInput):       runtimeCtx.GetWorkflowInput(),
		string(parser.ValueSourceNode):        runtimeCtx.GetExecuteNodeResults(),
		string(parser.ValueSourceParent):      execArgsFromRuntimeCtx(runtimeCtx.ParentWorkflowContext),
		string(parser.ValueSourceENV):         runtimeCtx.GetEnvVariables(),
		string(parser.ValueSourceERROR):       runtimeCtx.GetExecuteNodeErrors(),
		string(parser.ValueSourceGlobal):      runtimeCtx.GetGlobalVariables(),
		string(parser.ValueSourceBranch):      runtimeCtx.GetLastBranch(),
		string(parser.ValueSourceFlowID):      runtimeCtx.GetFlowID(),
		string(parser.ValueSourceFlow):        runtimeCtx.GetFlowID(),
		string(parser.ValueSourceLastNode):    lastNodeRootValue(runtimeCtx),
		string(parser.ValueSourceExecutionID): runtimeCtx.GetExecutionID(),
		string(parser.ValueNULL):              nil,
		string(parser.ValueEmptyString):       "",
	}
	deepCopy(runtimeCtx.GetCurrentNodeContexts(), &execArgs)
	return execArgs
}

// lastNodeRootValue renders $LAST_NODE: the last completed node's id, or nil
// before any node has completed (plaita context.last_node is None then).
func lastNodeRootValue(runtimeCtx *runtime.Context) any {
	if id := runtimeCtx.GetLastNodeID(); id != "" {
		return string(id)
	}
	return nil
}

func (e *Evaluator) evalFunc(
	ctx context.Context,
	expr parser.Expr,
	parameters map[string]any,
) (model.Value, error) {
	execArgs := execArgsFromRuntimeCtx(ctx)
	deepCopy(parameters, &execArgs)
	// plaita#69 write protection (issue #9): a mutator whose target is a live
	// context root ($INPUT/$NODE/$GLOBAL/$ENV/$ERROR/$PARENT path) is a named
	// error BEFORE anything evaluates — the ValueError plaita raises in
	// _gate_mutator_argument. Must run before the variable pre-resolution,
	// which would hide the roots behind synthetic identifiers.
	if err := gateMutatorLiveRoot(expr.Statement()); err != nil {
		return model.Value{}, err
	}
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

// mutatorFuncs are the $F functions Python's expression.py registers with
// has_side_effects=True — the ones plaita#69's gate covers. delete and
// delDictValue are `a.pop(b)`; delListItem is `a.pop(b)` too.
var mutatorFuncs = map[string]bool{
	"set":          true,
	"setDictValue": true,
	"delete":       true,
	"delDictValue": true,
	"clear":        true,
	"clearDict":    true,
	"pop":          true,
	"delListItem":  true,
	"setListItem":  true,
}

// freshListProducers are the pure $F calls whose result is a NEW list by
// construction (Python's shallow producers): mutating such a result never
// reaches a persisted context root, so they are the one legal target shape
// for a mutator (`$F.pop($F.append($INPUT.list, 4), 0)`).
var freshListProducers = map[string]bool{
	"append":         true,
	"addListItem":    true,
	"extend":         true,
	"insert":         true,
	"insertListItem": true,
	"remove":         true,
	"slice":          true,
	"reverse":        true,
	"sort":           true,
	"split":          true,
	"keys":           true,
	"values":         true,
	"items":          true,
	"getDictKeys":    true,
	"getDictValues":  true,
	"json_loads":     true,
}

// mutatorHeadRe matches the statement head: a mutator call identifier and its
// opening paren (`set(INPUT.map, "c", 3)`).
var mutatorHeadRe = regexp.MustCompile(`^\s*([A-Za-z_][A-Za-z0-9_]*)\s*\(`)

// calleeHeadRe matches a callee head inside a target argument
// (`append(INPUT.list, 4)`).
var calleeHeadRe = regexp.MustCompile(`^\s*([A-Za-z_][A-Za-z0-9_]*)\s*\(`)

// statementRootRefRe matches a live context root reference in a
// dollar-stripped statement (`INPUT.list`, `PARENT.INPUT.a`, bare `INPUT`).
// A root token in field position (preceded by '.') is NOT a root reference —
// same rule as plaita's _scan_root_refs_impl.
var statementRootRefRe = regexp.MustCompile(
	`(?:^|[^A-Za-z0-9_.$])(INPUT|NODE|GLOBAL|ENV|ERROR|PARENT)(?:\.|[^A-Za-z0-9_.$]|$)`)

// gateMutatorLiveRoot implements the plaita#69 mutator write protection for
// the Go engine: when the statement calls a mutator and its TARGET argument
// (position 0) references a live context root, the call is refused with a
// named error before evaluation — mirroring plaita's `_gate_mutator_argument`
// ValueError ("mutation of a persisted root").
//
// The statement is the dollar-stripped goval form (parser.parseFunc already
// removed every "$"), and quoted literals are masked before scanning so a
// string like "set(INPUT...)" inside an argument cannot trip the gate.
func gateMutatorLiveRoot(statement string) error {
	head := mutatorHeadRe.FindStringSubmatch(statement)
	if head == nil || !mutatorFuncs[head[1]] {
		return nil
	}
	fn := head[1]
	target, ok := firstStatementArg(statement[len(head[0]):])
	if !ok {
		// No argument to classify (cannot happen for the registered mutators,
		// which all take a target) — let evaluation surface the arity error.
		return nil
	}
	if mutatorTargetAllowed(target) {
		return nil
	}
	return fmt.Errorf(
		"$F.%s: cannot mutate live context path '%s' (plaita#69 write protection: mutator targets must be fresh containers, not $INPUT/$NODE/$GLOBAL/$PARENT/$ENV paths)",
		fn, target)
}

// mutatorTargetAllowed classifies the target argument text: a fresh list
// producer call is allowed outright; any other target is allowed only when it
// references no live context root.
func mutatorTargetAllowed(target string) bool {
	trimmed := strings.TrimSpace(target)
	if callee := calleeHeadRe.FindStringSubmatch(trimmed); callee != nil {
		if freshListProducers[callee[1]] {
			return true
		}
	}
	return !referencesLiveRoot(trimmed)
}

// referencesLiveRoot reports whether a dollar-stripped statement fragment
// references one of the live context roots outside of quoted literals.
func referencesLiveRoot(fragment string) bool {
	masked := quotedSpanRe.ReplaceAllString(fragment, " ")
	return statementRootRefRe.MatchString(masked)
}

// firstStatementArg extracts the first top-level argument of the argument
// list beginning at src (which starts right after the head function's opening
// paren) and reports whether one exists.
func firstStatementArg(src string) (string, bool) {
	depth := 0
	var b strings.Builder
	for i := 0; i < len(src); i++ {
		c := src[i]
		switch c {
		case '"':
			// copy the literal verbatim (escapes included)
			b.WriteByte(c)
			i++
			for i < len(src) {
				b.WriteByte(src[i])
				if src[i] == '\\' && i+1 < len(src) {
					i++
					b.WriteByte(src[i])
				} else if src[i] == '"' {
					break
				}
				i++
			}
		case '(', '[', '{':
			depth++
			b.WriteByte(c)
		case ')', ']', '}':
			if depth == 0 {
				return strings.TrimSpace(b.String()), b.Len() > 0
			}
			depth--
			b.WriteByte(c)
		case ',':
			if depth == 0 {
				return strings.TrimSpace(b.String()), b.Len() > 0
			}
			b.WriteByte(c)
		default:
			b.WriteByte(c)
		}
	}
	return strings.TrimSpace(b.String()), b.Len() > 0
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

// isKnownRootSource reports whether source names one of the expression
// grammar's root identifiers (the evalVariable switch). A bare identifier is
// not one — it addresses the variable/parameter context.
func isKnownRootSource(source parser.ValueSource) bool {
	switch source {
	case parser.ValueSourceInput, parser.ValueSourceNode, parser.ValueSourceParent,
		parser.ValueSourceERROR, parser.ValueSourceENV, parser.ValueSourceGlobal,
		parser.ValueSourceBranch, parser.ValueSourceFlowID, parser.ValueSourceFlow,
		parser.ValueSourceLastNode, parser.ValueSourceExecutionID,
		parser.ValueNULL, parser.ValueEmptyString:
		return true
	}
	return false
}

// firstVariableSource returns the source of the expression's first variable
// (empty when the expression names no variable).
func firstVariableSource(expr parser.Expr) parser.ValueSource {
	vars := expr.Variables()
	if len(vars) == 0 {
		return ""
	}
	return vars[0].Source
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
			return model.NewValue(runtimeCtx.GetWorkflowInput()), nil // TODO: deep copy input map
		}
		// plaita missing-key semantics: a missing field on an existing input
		// yields nil; only a nil/absent input container or an index miss is
		// an error (see semantics.go).
		val, err := lookupPathPlaita(runtimeCtx.GetWorkflowInput(), variable.WithoutSourceVariable, false)
		if err != nil {
			return model.Value{}, fmt.Errorf("failed to lookup input variable %q: %w", variable.Variable, err)
		}
		return model.NewValue(val), nil
	case parser.ValueSourceNode:
		results := runtimeCtx.GetExecuteNodeResults()
		if len(variable.WithoutSourceVariable) == 0 {
			return model.NewValue(results), nil
		}
		// plaita#130: `$NODE.raw.<node>[.path...]` reads the stored node
		// result verbatim (the modifier is stripped; strings are never
		// re-evaluated at any depth). A node literally named `raw` keeps
		// being resolved as that node.
		path := variable.WithoutSourceVariable
		if rawPath, ok := nodeRawAccessorPath(results, path); ok {
			path = rawPath
		}
		val, err := lookupPathPlaita(results, path, false)
		if err != nil {
			return model.Value{}, fmt.Errorf("failed to lookup executed node result var %q: %w", variable.Variable, err)
		}
		return model.NewValue(val), nil
	case parser.ValueSourceParent:
		rest := variable.WithoutSourceVariable
		newExpr, err := parser.NewVariableExpr(rest)
		if err != nil {
			return model.Value{}, fmt.Errorf("failed to create new variable expression: %w", err)
		}
		// $PARENT.<bare-path> is a field read on the parent snapshot object:
		// plaita's _get_attr yields None for a missing mapping key (r78
		// probes: sub-flows read $PARENT.<x> as null, never KeyError), so the
		// bare-field form resolves against the parent's variable context with
		// field-miss→nil semantics instead of the rewrite's root-miss error.
		// $-rooted remainders ($PARENT.$INPUT.x) keep the rewrite — their own
		// root already implements nil-on-field-miss.
		if rest != "" && !isKnownRootSource(firstVariableSource(newExpr)) {
			parentRC := runtime.WorkflowRuntimeContext(runtimeCtx.ParentWorkflowContext)
			execArgs := make(map[string]any)
			deepCopy(parentRC.GetCurrentNodeContexts(), &execArgs)
			deepCopy(parameters, &execArgs)
			val, err := lookupPathPlaita(execArgs, rest, false)
			if err != nil {
				return model.Value{}, fmt.Errorf("failed to lookup parent var %q: %w", variable.Variable, err)
			}
			return model.NewValue(val), nil
		}
		return e.evalVariable(runtimeCtx.ParentWorkflowContext, newExpr, parameters)
	case parser.ValueSourceERROR:
		// $ERROR is a Go-runtime-specific root with no plaita counterpart;
		// it keeps its legacy case-insensitive lookup and error-on-miss
		// semantics unchanged.
		val, err := lookup.LookupStringI(runtimeCtx.GetExecuteNodeErrors(), variable.WithoutSourceVariable)
		if err != nil {
			return model.Value{}, fmt.Errorf("failed to lookup executed error result var %q: %w", variable.Variable,
				err)
		}
		return model.NewValue(val.Interface()), nil
	case parser.ValueSourceENV:
		val, err := lookupPathPlaita(runtimeCtx.GetEnvVariables(), variable.WithoutSourceVariable, false)
		if err != nil {
			return model.Value{}, fmt.Errorf("failed to lookup executed error result var %q: %w", variable.Variable,
				err)
		}
		return model.NewValue(val), nil
	case parser.ValueSourceGlobal:
		val, err := lookupPathPlaita(runtimeCtx.GetGlobalVariables(), variable.WithoutSourceVariable, false)
		if err != nil {
			return model.Value{}, fmt.Errorf("failed to lookup executed error result var %q: %w", variable.Variable,
				err)
		}
		return model.NewValue(val), nil
	case parser.ValueSourceBranch:
		// $BRANCH is the branch target taken by the most recent branching
		// node (plaita runner: context.last_branch); nil until one has run
		// (plaita None). Scalar root — no sub-paths.
		return model.NewValue(runtimeCtx.GetLastBranch()), nil
	case parser.ValueSourceFlowID, parser.ValueSourceFlow:
		// $FLOW_ID (alias $FLOW) is the id of the flow this execution runs
		// (plaita setup_flow); a child flow without its own id keeps the
		// parent's value. Scalar root — no sub-paths.
		return model.NewValue(runtimeCtx.GetFlowID()), nil
	case parser.ValueSourceLastNode:
		// $LAST_NODE is the id of the last completed node (plaita runner:
		// context.last_node); nil before any node has run (plaita None).
		// Scalar root — no sub-paths.
		return model.NewValue(lastNodeRootValue(runtimeCtx)), nil
	case parser.ValueSourceExecutionID:
		// $EXECUTION_ID is the distributed execution id (plaita setup_flow);
		// every level of one execution (root/child/branch) shares it.
		// Scalar root — no sub-paths.
		return model.NewValue(runtimeCtx.GetExecutionID()), nil
	case parser.ValueNULL:
		return model.NewValue(nil), nil
	case parser.ValueEmptyString:
		return model.NewValue(""), nil
	default:
		execArgs := make(map[string]any)
		deepCopy(runtimeCtx.GetCurrentNodeContexts(), &execArgs)
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
