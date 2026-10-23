package plaitaexpr

// #64 negative controls: the string-amplification $F functions must run
// Python's pre-allocation result-size gate (_check_result_size →
// _size_refusal wording). HEAD allocated the projected result outright —
// `$F.replace(100000×'a', "a", 1000×'b')` built a 10^8-char string (95.4MB
// TotalAlloc, err=nil) where Python raises a named ValueError.

import (
	"context"
	stdruntime "runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/model"
)

func evalR64(t *testing.T, expr string, input any) (model.Value, error) {
	t.Helper()
	e := &Evaluator{}
	ctx := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: input,
	})
	return e.Eval(ctx, expr, nil)
}

// totalAllocDelta runs fn with GC settled and reports how much total
// allocation the call added.
func totalAllocDelta(t *testing.T, fn func()) int64 {
	t.Helper()
	stdruntime.GC()
	var before, after stdruntime.MemStats
	stdruntime.ReadMemStats(&before)
	fn()
	stdruntime.ReadMemStats(&after)
	return int64(after.TotalAlloc - before.TotalAlloc)
}

func TestR64ReplaceAmplificationRefused(t *testing.T) {
	input := map[string]any{
		"s":   strings.Repeat("a", 100000),
		"old": "a",
		"r":   strings.Repeat("b", 1000),
	}
	var v model.Value
	var err error
	delta := totalAllocDelta(t, func() {
		v, err = evalR64(t, `$F.replace($INPUT.s, $INPUT.old, $INPUT.r)`, input)
	})
	require.Error(t, err, "got value len=%d", len(v.AsString()))
	assert.Contains(t, err.Error(), "$F.replace(100000 chars, 100000 hits) result too large:")
	assert.Contains(t, err.Error(), "100000000 chars (limit 404004)")
	assert.Contains(t, err.Error(), "Refusing to build an unbounded result.")
	assert.Less(t, delta, int64(10*1024*1024), "refusal must happen before the allocation (got %d bytes)", delta)
}

func TestR64ReplaceEmptyPatternRefused(t *testing.T) {
	input := map[string]any{
		"s": strings.Repeat("a", 100000),
		"r": strings.Repeat("x", 100),
	}
	_, err := evalR64(t, `$F.replace($INPUT.s, "", $INPUT.r)`, input)
	// Python: $F.replace(100000 chars, 100000 hits) result too large:
	// 10100100 chars (limit 400400). Refusing to build an unbounded result.
	require.Error(t, err)
	assert.Contains(t, err.Error(), "$F.replace(100000 chars, 100000 hits) result too large: 10100100 chars (limit 400400)")
}

func TestR64ReplaceEmptyPatternUnderLimitPasses(t *testing.T) {
	// Reverse control: projection 300002 ≤ 400008 — Python returns the
	// interleaved string; the gate must not reject it.
	input := map[string]any{
		"s": strings.Repeat("a", 100000),
		"r": "xy",
	}
	v, err := evalR64(t, `$F.replace($INPUT.s, "", $INPUT.r)`, input)
	require.NoError(t, err)
	assert.Equal(t, 300002, len(v.AsString()))
}

func TestR64ReplaceNonAmplifyingPatternPasses(t *testing.T) {
	// len(r) == len(old): projection = len(a), never amplifying.
	input := map[string]any{
		"s":   strings.Repeat("a", 100000),
		"old": "a",
		"r":   "b",
	}
	v, err := evalR64(t, `$F.replace($INPUT.s, $INPUT.old, $INPUT.r)`, input)
	require.NoError(t, err)
	assert.Equal(t, 100000, len(v.AsString()))
}

func TestR64ConcatFourLargePasses(t *testing.T) {
	// Reverse control: 4×100KB == 400000 chars; Python passes (the concat
	// input size equals its projection, so only the hard cap can bind).
	s := strings.Repeat("a", 100000)
	v, err := evalR64(t, `$F.concat($INPUT.s, $INPUT.s, $INPUT.s, $INPUT.s)`, map[string]any{"s": s})
	require.NoError(t, err)
	assert.Equal(t, 400000, len(v.AsString()))
}

func TestR64ConcatOverHardLimitRefused(t *testing.T) {
	if testing.Short() {
		t.Skip("heavy memory case")
	}
	s := strings.Repeat("a", 40_000_000) // 3×40MB chars, projection 120M > 1e8
	_, err := evalR64(t, `$F.concat($INPUT.s, $INPUT.s, $INPUT.s)`, map[string]any{"s": s})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "$F.concat(3 args) result too large:")
	assert.Contains(t, err.Error(), "Refusing to build an unbounded result.")
}

func TestR64AddStringsOverHardLimitRefused(t *testing.T) {
	if testing.Short() {
		t.Skip("heavy memory case")
	}
	s := strings.Repeat("a", 55_000_000)
	_, err := evalR64(t, `$F.add($INPUT.s, $INPUT.s)`, map[string]any{"s": s})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "$F.add(55000000 + 55000000) result too large: 110000000 chars (limit 100000000)")
	assert.Contains(t, err.Error(), "Refusing to build an unbounded result.")
}

func TestR64ExtendStringsOverHardLimitRefused(t *testing.T) {
	if testing.Short() {
		t.Skip("heavy memory case")
	}
	s := strings.Repeat("a", 55_000_000)
	_, err := evalR64(t, `$F.extend($INPUT.s, $INPUT.s)`, map[string]any{"s": s})
	require.Error(t, err)
	// Python's _fn_extend wording carries its own function name.
	assert.Contains(t, err.Error(), "$F.extend(55000000 + 55000000) result too large: 110000000 chars (limit 100000000)")
}

func TestR64MulParityUnchanged(t *testing.T) {
	// #45 对照：$F.mul 的文案与行为逐字不变（引擎 limit，无 4× 投影）。
	_, err := evalR64(t, `$F.mul($INPUT.s, 100000)`, map[string]any{"s": strings.Repeat("a", 100000)})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "$F.mul(100000 * 100000) result too large: 10000000000 chars (limit 100000000)")
}

// White-box rows for the paths whose over-limit shapes cannot be built in
// test memory (a >1e8-element slice is ~1.6GB): the gate math and the exact
// Python refusal wording, per unit.
func TestR64GateShapesWhiteBox(t *testing.T) {
	err := gatePairConcat("$F.add", 60_000_000, 60_000_000, "elements")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "$F.add(60000000 + 60000000) result too large: 120000000 elements (limit 100000000)")

	err = checkResultSize("$F.append(100000000 + 1)", 100_000_001, 100_000_001, "elements")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "$F.append(100000000 + 1) result too large: 100000001 elements (limit 100000000)")

	// under the hard cap the gate never binds (projection == input size)
	require.NoError(t, gatePairConcat("$F.add", 10_000_000, 10_000_000, "elements"))
}
