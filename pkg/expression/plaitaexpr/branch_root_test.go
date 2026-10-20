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

// $BRANCH — the plaita system root carrying the branch target taken by the
// most recent branching node (runner.py: context.last_branch; checkpoint
// schema SYSTEM_KEY_NAMES "BRANCH").

func TestParseBranchSource(t *testing.T) {
	expr, err := parser.Parse("$BRANCH")
	require.NoError(t, err)
	vars := expr.Variables()
	require.Len(t, vars, 1)
	assert.Equal(t, parser.ValueSourceBranch, vars[0].Source)
	assert.Equal(t, "BRANCH", vars[0].Variable)
}

func TestEvalBranchRoot(t *testing.T) {
	e := NewEvaluator()

	t.Run("nil until a branching node ran (plaita None)", func(t *testing.T) {
		got, err := e.Eval(runtime.WithWorkflowRuntimeContext(
			context.Background(), runtime.NewContext()), "$BRANCH", nil)
		require.NoError(t, err)
		assert.Equal(t, model.NewValue(nil), got)
	})

	t.Run("whole root returns the branch target", func(t *testing.T) {
		rc := runtime.NewContext()
		rc.WithLastBranch("e1")
		got, err := e.Eval(runtime.WithWorkflowRuntimeContext(context.Background(), rc), "$BRANCH", nil)
		require.NoError(t, err)
		assert.Equal(t, model.NewValue("e1"), got)
	})

	t.Run("inside $F statements (goval identifier resolution)", func(t *testing.T) {
		rc := runtime.NewContext()
		rc.WithLastBranch("yes")
		ctx := runtime.WithWorkflowRuntimeContext(context.Background(), rc)

		got, err := e.Eval(ctx, `$F.eq($BRANCH, "yes")`, nil)
		require.NoError(t, err)
		assert.Equal(t, model.NewValue(true), got)

		got, err = e.Eval(ctx, `$F.ifelse($F.eq($BRANCH, "yes"), "hit", "miss")`, nil)
		require.NoError(t, err)
		assert.Equal(t, model.NewValue("hit"), got)
	})

	t.Run("child execution resolves its own branch, parent via $PARENT", func(t *testing.T) {
		parent := runtime.NewContext()
		parent.WithLastBranch("parent-branch")
		parentCtx := runtime.WithWorkflowRuntimeContext(context.Background(), parent)

		// a child seed the way child/loop nodes build it: $PARENT chains to
		// the parent execution context.
		seed := runtime.WithWorkflowRuntimeContext(parentCtx,
			&runtime.Context{ParentWorkflowContext: parentCtx})
		child := runtime.NewExecutionRuntimeContext(seed)
		child.WithLastBranch("child-branch")
		childCtx := runtime.WithWorkflowRuntimeContext(parentCtx, child)

		got, err := e.Eval(childCtx, "$BRANCH", nil)
		require.NoError(t, err)
		assert.Equal(t, model.NewValue("child-branch"), got)

		got, err = e.Eval(childCtx, "$PARENT.$BRANCH", nil)
		require.NoError(t, err)
		assert.Equal(t, model.NewValue("parent-branch"), got)
	})
}

// The exported comparison primitives are the single source shared with the
// switch condition operators (pkg/workflow/nodes/branch.go) — smoke-assert
// the three contracts the condition matrix relies on.
func TestExportedComparisonPrimitives(t *testing.T) {
	assert.True(t, Equal(1, 1.0))
	assert.True(t, Equal(true, 1))
	assert.True(t, Equal([]any{1.0}, []any{1}))
	assert.False(t, Equal(1, "1"))

	c, err := Compare("gt", "b", "a")
	require.NoError(t, err)
	assert.Equal(t, 1, c)
	_, err = Compare("gt", 2.0, "10")
	require.ErrorContains(t, err, "unsupported operand types")

	contained, err := Contains("contains", "abc", "b")
	require.NoError(t, err)
	assert.True(t, contained)
	contained, err = Contains("contains", map[string]any{"a": 1}, "a")
	require.NoError(t, err)
	assert.True(t, contained)
	_, err = Contains("contains", 42.0, "a")
	require.ErrorContains(t, err, "unsupported container type")
}
