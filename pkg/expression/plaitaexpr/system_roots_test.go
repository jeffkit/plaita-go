package plaitaexpr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/expression/plaitaexpr/parser"
	"github.com/jeffkit/plaita-go/pkg/model"
)

// The plaita system variable roots (#11): $FLOW_ID / $FLOW / $LAST_NODE /
// $EXECUTION_ID / $BRANCH / $EMPTYSTRING / $GLOBAL.flow_id — the acceptance
// table of issue #11, Python semantics as the authority
// (plaita/core/context.py setup_flow + accessors, runner.py per-node writes).

func systemRootsContext() context.Context {
	rc := runtime.NewContext()
	rc.WithFlowID("probe-flow")
	rc.WithExecutionID("exec-1234")
	rc.WithGlobalVariables(map[string]any{"flow_id": "probe-flow"})
	return runtime.WithWorkflowRuntimeContext(context.Background(), rc)
}

func TestParseSystemRootSources(t *testing.T) {
	cases := map[string]parser.ValueSource{
		"$FLOW_ID":        parser.ValueSourceFlowID,
		"$FLOW":           parser.ValueSourceFlow,
		"$LAST_NODE":      parser.ValueSourceLastNode,
		"$EXECUTION_ID":   parser.ValueSourceExecutionID,
		"$BRANCH":         parser.ValueSourceBranch,
		"$EMPTYSTRING":    parser.ValueEmptyString,
		"$GLOBAL.plain":   parser.ValueSourceGlobal,
		"$GLOBAL.flow_id": parser.ValueSourceGlobal,
	}
	for expr, want := range cases {
		parsed, err := parser.Parse(expr)
		require.NoError(t, err, expr)
		vars := parsed.Variables()
		require.Len(t, vars, 1, expr)
		assert.Equal(t, want, vars[0].Source, expr)
	}
}

func TestEvalSystemRoots(t *testing.T) {
	e := NewEvaluator()
	ctx := systemRootsContext()

	t.Run("acceptance table (#11)", func(t *testing.T) {
		cases := map[string]model.Value{
			"$FLOW_ID":        model.NewValue("probe-flow"),
			"$FLOW":           model.NewValue("probe-flow"),
			"$GLOBAL.flow_id": model.NewValue("probe-flow"),
			"$EXECUTION_ID":   model.NewValue("exec-1234"),
			"$EMPTYSTRING":    model.NewValue(""),
			"$BRANCH":         model.NewValue(nil), // no branching node ran yet (plaita None)
			"$LAST_NODE":      model.NewValue(nil), // no node completed yet (plaita None)
		}
		for expr, want := range cases {
			got, err := e.Eval(ctx, expr, nil)
			require.NoError(t, err, expr)
			assert.Equal(t, want, got, expr)
		}
	})

	t.Run("LAST_NODE and BRANCH after a node ran", func(t *testing.T) {
		rc := runtime.NewContext()
		rc.WithFlowID("probe-flow")
		rc.WithExecutionID("exec-1234")
		rc.WithLastNodeID("s")
		rc.WithLastBranch("e1")
		got, err := e.Eval(runtime.WithWorkflowRuntimeContext(context.Background(), rc), "$LAST_NODE", nil)
		require.NoError(t, err)
		assert.Equal(t, model.NewValue("s"), got)
		got, err = e.Eval(runtime.WithWorkflowRuntimeContext(context.Background(), rc), "$BRANCH", nil)
		require.NoError(t, err)
		assert.Equal(t, model.NewValue("e1"), got)
	})

	t.Run("inside $F statements (goval identifier resolution)", func(t *testing.T) {
		got, err := e.Eval(ctx, `$F.eq($FLOW_ID, "probe-flow")`, nil)
		require.NoError(t, err)
		assert.Equal(t, model.NewValue(true), got)

		got, err = e.Eval(ctx, `$F.eq($FLOW, $FLOW_ID)`, nil)
		require.NoError(t, err)
		assert.Equal(t, model.NewValue(true), got)

		got, err = e.Eval(ctx, `$F.eq($EXECUTION_ID, "exec-1234")`, nil)
		require.NoError(t, err)
		assert.Equal(t, model.NewValue(true), got)

		got, err = e.Eval(ctx, `$F.eq($EMPTYSTRING, "")`, nil)
		require.NoError(t, err)
		assert.Equal(t, model.NewValue(true), got)

		got, err = e.Eval(ctx, `$F.ifelse($F.eq($GLOBAL.flow_id, "probe-flow"), "hit", "miss")`, nil)
		require.NoError(t, err)
		assert.Equal(t, model.NewValue("hit"), got)
	})

	t.Run("child execution: own id wins, missing id falls back to the parent", func(t *testing.T) {
		parent := runtime.NewContext()
		parent.WithFlowID("parent-flow")
		parent.WithExecutionID("root-exec")
		parentCtx := runtime.WithWorkflowRuntimeContext(context.Background(), parent)

		// a child seed the way child/loop nodes build it: $PARENT chains to
		// the parent execution context.
		seed := runtime.WithWorkflowRuntimeContext(parentCtx,
			&runtime.Context{ParentWorkflowContext: parentCtx})

		// child with its own id: $FLOW_ID is the child's.
		own := runtime.NewExecutionRuntimeContext(seed)
		own.WithFlowID("child-flow")
		ownCtx := runtime.WithWorkflowRuntimeContext(parentCtx, own)
		got, err := e.Eval(ownCtx, "$FLOW_ID", nil)
		require.NoError(t, err)
		assert.Equal(t, model.NewValue("child-flow"), got)
		got, err = e.Eval(ownCtx, "$PARENT.$FLOW_ID", nil)
		require.NoError(t, err)
		assert.Equal(t, model.NewValue("parent-flow"), got)
		// the execution id is shared by every level of one execution.
		got, err = e.Eval(ownCtx, "$EXECUTION_ID", nil)
		require.NoError(t, err)
		assert.Equal(t, model.NewValue("root-exec"), got)

		// child without its own id: $FLOW_ID falls back to the parent's.
		inherited := runtime.NewExecutionRuntimeContext(seed)
		inheritedCtx := runtime.WithWorkflowRuntimeContext(parentCtx, inherited)
		got, err = e.Eval(inheritedCtx, "$FLOW_ID", nil)
		require.NoError(t, err)
		assert.Equal(t, model.NewValue("parent-flow"), got)
	})
}
