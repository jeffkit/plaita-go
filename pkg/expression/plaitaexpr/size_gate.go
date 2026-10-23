package plaitaexpr

// size_gate.go ports plaita's pre-allocation result-size gate
// (expression.py _check_result_size / _allowed_result_size / _size_refusal)
// to the $F functions that can amplify data (#64): replace, concat, add,
// append and extend. $F.mul keeps its dedicated guard (pyRepeatCount, #45)
// with the engine limit and wording — this gate reuses the same lenLimit as
// the hard cap.

import (
	"encoding/json"
	"fmt"
	"math"
	"unicode/utf8"
)

const (
	// pyMinResultSize is Python's _EXPR_MAX_RESULT_SIZE floor (the ~100k
	// soft limit). The PLAITA_EXPR_* environment knobs stay a Python-side
	// decision (#64 边界) — the Go constants mirror the defaults.
	pyMinResultSize int64 = 100_000
	// pyResultGrowthFactor is Python's _EXPR_GROWTH_FACTOR: the allowed
	// result is at least growth × input size.
	pyResultGrowthFactor int64 = 4
)

// allowedResultSize mirrors expression.py _allowed_result_size:
// min(max(floor, growth × inputSize), hard).
func allowedResultSize(inputSize int64) int64 {
	allowed := pyMinResultSize
	if g := pyResultGrowthFactor * inputSize; g > allowed {
		allowed = g
	}
	if allowed > int64(lenLimit) {
		allowed = int64(lenLimit)
	}
	return allowed
}

// checkResultSize refuses a projected result over the allowed bound BEFORE
// anything allocates, with Python's verbatim refusal wording. projected
// should be clamped to math.MaxInt64 by the caller on integer overflow.
func checkResultSize(what string, projected, inputSize int64, unit string) error {
	allowed := allowedResultSize(inputSize)
	if projected > allowed {
		return fmt.Errorf("%s result too large: %d %s (limit %d). Refusing to build an unbounded result.",
			what, projected, unit, allowed)
	}
	return nil
}

// gatePairConcat gates a same-kind sequence concat (`a + b`, expression.py
// _fn_add/_fn_extend): the projection equals the input size, so only the hard
// cap can bind — legal concatenation of any size below 1e8 stays untouched.
func gatePairConcat(fn string, la, lb int64, unit string) error {
	total, overflowed := intAddOverflow(la, lb)
	if overflowed {
		total = math.MaxInt64
	}
	return checkResultSize(fmt.Sprintf("%s(%d + %d)", fn, la, lb), total, total, unit)
}

// pyRenderBound estimates the Python str() render size of a value before
// rendering it (expression.py _render_size's provable-bound shape): strings
// count runes, scalars are short constants, containers sum their elements
// (Python's 16 + Σ(item + 2) shape, keys and punctuation folded into the
// per-item overhead). report "unbounded" for types outside the engine's
// value domain. The recursion returns early once the bound passes the hard
// cap — a lower bound over the cap is refusal-grade information already.
func pyRenderBound(v any) (int64, bool) {
	switch t := v.(type) {
	case nil:
		return 4, true // "None"
	case bool:
		return 5, true // "false" renders at most "False"
	case string:
		return int64(utf8.RuneCountInString(t)), true
	case int:
		return 20, true
	case int8:
		return 20, true
	case int16:
		return 20, true
	case int32:
		return 20, true
	case int64:
		return 20, true
	case uint:
		return 20, true
	case uint8:
		return 20, true
	case uint16:
		return 20, true
	case uint32:
		return 20, true
	case uint64:
		return 20, true
	case float32:
		return 24, true
	case float64:
		return 24, true
	case json.Number:
		return int64(len(t)), true
	case []any:
		var total int64 = 2
		for _, e := range t {
			b, ok := pyRenderBound(e)
			if !ok {
				return 0, false
			}
			total += b + 2
			if total >= int64(lenLimit) {
				return total, true
			}
		}
		return total, true
	case map[string]any:
		var total int64 = 2
		for k, e := range t {
			b, ok := pyRenderBound(e)
			if !ok {
				return 0, false
			}
			// key (quoted) + ": " + item + ", "
			total += int64(len(k)) + b + 6
			if total >= int64(lenLimit) {
				return total, true
			}
		}
		return total, true
	}
	return 0, false
}
