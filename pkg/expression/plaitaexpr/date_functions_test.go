package plaitaexpr

import (
	"context"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/model"
)

// Date semantics pinned here follow plaita's Python implementations
// (expression.py _fn_now/_fn_today = datetime.now().strftime(fmt) /
// date.today().strftime(fmt)). Issue #22: the optional format is a STRFTIME
// layout — the Go reference layout made today('%Y') return "%Y" literally.

func evalDateExpr(t *testing.T, expr string) (model.Value, error) {
	t.Helper()
	e := &Evaluator{}
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: 10,
	})
	return e.Eval(ctx, expr, nil)
}

// Test_now_today_eval pins $F.now/$F.today against a frozen clock. Both
// functions share ONE gomonkey patch: repeatedly patching time.Now within a
// single test binary is unreliable on darwin/arm64 (full-suite flake).
func Test_now_today_eval(t *testing.T) {
	patches := gomonkey.ApplyFuncReturn(time.Now, time.Unix(1704038400, 0).Local())
	defer patches.Reset()
	// default format matches Python's "%Y-%m-%d %H:%M:%S" / "%Y-%m-%d"
	result1, err1 := evalDateExpr(t, `$F.now()`)
	require.NoError(t, err1)
	assert.Equal(t, model.NewValue("2024-01-01 00:00:00"), result1)
	// strftime directives, not Go reference layouts
	result2, err2 := evalDateExpr(t, `$F.now("%Y.%m.%d")`)
	require.NoError(t, err2)
	assert.Equal(t, model.NewValue("2024.01.01"), result2)
	// issue #22 acceptance row: now('%H') renders the directive, not the
	// literal format string
	result3, err3 := evalDateExpr(t, `$F.now('%H')`)
	require.NoError(t, err3)
	assert.Equal(t, model.NewValue("00"), result3)

	result4, err4 := evalDateExpr(t, `$F.today()`)
	require.NoError(t, err4)
	assert.Equal(t, model.NewValue("2024-01-01"), result4)
	result5, err5 := evalDateExpr(t, `$F.today("%Y.%m.%d")`)
	require.NoError(t, err5)
	assert.Equal(t, model.NewValue("2024.01.01"), result5)
	// issue #22 acceptance row: today('%Y') == '2024', not '%Y'
	result6, err6 := evalDateExpr(t, `$F.today('%Y')`)
	require.NoError(t, err6)
	assert.Equal(t, model.NewValue("2024"), result6)
}

func Test_strftime_directives(t *testing.T) {
	// Direct-unit coverage of the strftime layer: `2026`/`18` acceptance rows
	// (issue #22) are pinned end-to-end in Test_today_eval / Test_now_eval.
	ts := time.Date(2026, 10, 19, 18, 30, 45, 123456000, time.UTC)
	cases := []struct {
		layout string
		want   string
	}{
		{"%Y", "2026"},
		{"%y", "26"},
		{"%m", "10"},
		{"%d", "19"},
		{"%H", "18"},
		{"%I", "06"},
		{"%M", "30"},
		{"%S", "45"},
		{"%f", "123456"},
		{"%j", "292"},
		{"%p", "PM"},
		{"%a", "Mon"},
		{"%A", "Monday"},
		{"%b", "Oct"},
		{"%B", "October"},
		{"%Y-%m-%d %H:%M:%S", "2026-10-19 18:30:45"},
		{"100%%", "100%"},
		{"%Q", "%Q"}, // unknown directives pass through (platform strftime)
		{"%-d", "19"},
	}
	for _, c := range cases {
		t.Run(c.layout, func(t *testing.T) {
			assert.Equal(t, c.want, pyStrftime(ts, c.layout))
		})
	}
}

func Test_now_strftime_acceptance(t *testing.T) {
	// Kept on the pyStrftime layer directly (gomonkey double-patching
	// time.Now in one process is unreliable): today('%Y')/now('%H') must
	// render the directive (issue #22 acceptance).
	ts := time.Date(2026, 10, 19, 18, 30, 45, 0, time.Local)
	assert.Equal(t, "2026", pyStrftime(ts, "%Y"))
	assert.Equal(t, "18", pyStrftime(ts, "%H"))
}
