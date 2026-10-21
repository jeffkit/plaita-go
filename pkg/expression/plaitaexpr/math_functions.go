package plaitaexpr

// math_functions.go ports plaita's math $F functions with Python operator
// semantics (expression.py _fn_add/_fn_sub/_fn_mul/_fn_div/_fn_mod/_fn_pow
// and the builtin/math registrations abs/ceil/floor/round/trunc/sqrt):
//
//   - `+` concatenates str/str and list/list, `*` repeats str/list by int;
//   - int×int stays int, any float operand makes the result float;
//   - `/` is true division (always float) and `1/0` is a ZeroDivisionError;
//   - `%` on strings is printf-style formatting, numeric % follows Python's
//     sign rule (result takes the divisor's sign);
//   - round is Python's builtin: banker's rounding, 1-or-2 args, int result
//     when ndigits is omitted, float result for a float input otherwise;
//   - abs/ceil/floor/trunc keep the int type (math.ceil(2.3) == 2, an int);
//   - sqrt of a negative number is a math domain error.
//
// Type notes: int-typed values are int/int64/bool (bool is an int subclass,
// as in Python) and integral json.Number (the engine's UseNumber decoding);
// float-typed values are float32/64 and non-integral json.Number.

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"
)

// intAddOverflow adds two int64s, reporting overflow.
func intAddOverflow(a, b int64) (int64, bool) {
	s := a + b
	if (a > 0 && b > 0 && s < 0) || (a < 0 && b < 0 && s >= 0) {
		return 0, true
	}
	return s, false
}

func intMulOverflow(a, b int64) (int64, bool) {
	if a == 0 || b == 0 {
		return 0, false
	}
	p := a * b
	if p/b != a {
		return 0, true
	}
	return p, false
}

// pyOperatorSymbols maps a $F math function to the Python operator it ports,
// for TypeError messages shaped like Python's.
var pyOperatorSymbols = map[string]string{
	"add": "+",
	"sub": "-",
	"mul": "*",
	"div": "/",
	"mod": "%",
	"pow": "**",
}

// typeErr builds the Python TypeError message shape for an unsupported
// operand combination.
func typeErr(op string, a, b any) error {
	return fmt.Errorf("$F.%s: unsupported operand type(s) for %s: '%s' and '%s'",
		op, pyOperatorSymbols[op], pyTypeName(a), pyTypeName(b))
}

// add is plaita $F.add(a, b) — Python `a + b`: numbers add (int×int → int),
// strings concatenate, lists concatenate into a NEW list; anything else is a
// TypeError.
func add(a, b any) (any, error) {
	if ai, aok := pyIntValue(a); aok {
		if bi, bok := pyIntValue(b); bok {
			s, ok := intAddOverflow(ai, bi)
			if ok {
				return nil, fmt.Errorf("$F.add: integer addition result too large")
			}
			return s, nil
		}
		if bf, bok := pyFloatValue(b); bok {
			return float64(ai) + bf, nil
		}
	}
	if af, aok := pyFloatValue(a); aok {
		if bn, bok := pyNumericValue(b); bok {
			return af + bn, nil
		}
	}
	if as, aok := a.(string); aok {
		if bs, bok := b.(string); bok {
			return as + bs, nil
		}
	}
	if al, aok := a.([]any); aok {
		if bl, bok := b.([]any); bok {
			out := make([]any, 0, len(al)+len(bl))
			out = append(out, al...)
			out = append(out, bl...)
			return out, nil
		}
	}
	return nil, typeErr("add", a, b)
}

// sub is plaita $F.sub(a, b) — Python `a - b` (numbers only).
func sub(a, b any) (any, error) {
	if ai, aok := pyIntValue(a); aok {
		if bi, bok := pyIntValue(b); bok {
			s, ok := intAddOverflow(ai, -bi)
			if ok {
				return nil, fmt.Errorf("$F.sub: integer subtraction result too large")
			}
			return s, nil
		}
		if bf, bok := pyFloatValue(b); bok {
			return float64(ai) - bf, nil
		}
	}
	if af, aok := pyFloatValue(a); aok {
		if bn, bok := pyNumericValue(b); bok {
			return af - bn, nil
		}
	}
	return nil, typeErr("sub", a, b)
}

// mul is plaita $F.mul(a, b) — Python `a * b`: numbers multiply and
// str/list repeat by an int count (both directions, negative → empty),
// returning a NEW sequence.
func mul(a, b any) (any, error) {
	ai, aInt := pyIntValue(a)
	bi, bInt := pyIntValue(b)
	_, aFloat := pyFloatValue(a)
	_, bFloat := pyFloatValue(b)
	if aInt && bInt {
		p, ok := intMulOverflow(ai, bi)
		if ok {
			return nil, fmt.Errorf("$F.mul: integer multiplication result too large")
		}
		return p, nil
	}
	if (aInt || aFloat) && (bInt || bFloat) {
		return pyNumericToF(a) * pyNumericToF(b), nil
	}
	// sequence repetition: seq * int or int * seq
	if as, ok := a.(string); ok && bInt {
		n, err := pyRepeatCount(int64(utf8.RuneCountInString(as)), "chars", bi)
		if err != nil {
			return nil, err
		}
		return strings.Repeat(as, n), nil
	}
	if bs, ok := b.(string); ok && aInt {
		n, err := pyRepeatCount(int64(utf8.RuneCountInString(bs)), "chars", ai)
		if err != nil {
			return nil, err
		}
		return strings.Repeat(bs, n), nil
	}
	if al, ok := a.([]any); ok && bInt {
		return repeatSlice(al, bi)
	}
	if bl, ok := b.([]any); ok && aInt {
		return repeatSlice(bl, ai)
	}
	return nil, typeErr("mul", a, b)
}

func pyNumericToF(v any) float64 {
	if i, ok := pyIntValue(v); ok {
		return float64(i)
	}
	f, _ := pyFloatValue(v)
	return f
}

// lenLimit bounds sequence repetition to keep Go allocations sane; Python
// guards the same blow-up with its result-size gate (#68). Per issue #45 边界
// the limit itself stays at the engine's 1e8 budget (lowering it to Python's
// ~100k soft limit is a separate decision) — only the failure mode changed:
// over-limit is a named error, never a silently emptied sequence.
const lenLimit = 100_000_000

// pyRepeatCount resolves a repetition count with the Python result-size guard
// shape (expression.py _fn_mul → _check_result_size): negative/zero repeats
// yield an empty sequence, and a PROJECTED result over lenLimit is refused
// before allocating (issue #45: the previous clamp to 0 returned "" for
// absurd counts, deferring the failure to an unattributable downstream site).
// The message mirrors Python's _size_refusal wording with the engine limit.
func pyRepeatCount(seqLen int64, unit string, n int64) (int, error) {
	if n <= 0 {
		return 0, nil
	}
	if seqLen > 0 && n > int64(lenLimit)/seqLen {
		projected := seqLen * n
		if projected < 0 || projected/n != seqLen { // int64 overflow on absurd inputs
			projected = math.MaxInt64
		}
		return 0, fmt.Errorf("$F.mul(%d * %d) result too large: %d %s (limit %d). "+
			"Refusing to build an unbounded result.",
			seqLen, n, projected, unit, int64(lenLimit))
	}
	return int(n), nil
}

func repeatSlice(s []any, n int64) ([]any, error) {
	if len(s) == 0 {
		return []any{}, nil // `[] * n` is empty for any n, Python included
	}
	count, err := pyRepeatCount(int64(len(s)), "elements", n)
	if err != nil {
		return nil, err
	}
	out := make([]any, 0, len(s)*count)
	for i := 0; i < count; i++ {
		out = append(out, s...)
	}
	return out, nil
}

// div is plaita $F.div(a, b) — Python `a / b`: true division, ALWAYS float
// (4/2 == 2.0), division by zero is a ZeroDivisionError.
func div(a, b any) (any, error) {
	fa, aok := pyNumericValue(a)
	fb, bok := pyNumericValue(b)
	if !aok || !bok {
		return nil, typeErr("div", a, b)
	}
	if fb == 0 {
		return nil, fmt.Errorf("$F.div: division by zero")
	}
	return fa / fb, nil
}

// mod is plaita $F.mod(a, b) — Python `a % b`: printf-style string formatting
// when a is a string, Python-sign numeric modulo otherwise (int×int → int).
func mod(a, b any) (any, error) {
	if as, ok := a.(string); ok {
		return pyFormat(as, b)
	}
	if ai, aok := pyIntValue(a); aok {
		if bi, bok := pyIntValue(b); bok {
			if bi == 0 {
				return nil, fmt.Errorf("$F.mod: division by zero")
			}
			r := ai % bi
			if r != 0 && (r < 0) != (bi < 0) {
				r += bi
			}
			return r, nil
		}
	}
	fa, aok := pyNumericValue(a)
	fb, bok := pyNumericValue(b)
	if !aok || !bok {
		return nil, typeErr("mod", a, b)
	}
	if fb == 0 {
		return nil, fmt.Errorf("$F.mod: division by zero")
	}
	r := math.Mod(fa, fb)
	if r != 0 && (r < 0) != (fb < 0) {
		r += fb
	}
	return r, nil
}

// pow is plaita $F.pow(a, b) — Python `a ** b`. int ** non-negative int stays
// int; int ** negative int is a float; 0 ** negative is a ZeroDivisionError;
// negative base with fractional exponent has a complex result in Python,
// which the JSON value domain cannot represent — refused with an error
// (documented divergence, see issue #20 边界).
func pow(a, b any) (any, error) {
	ai, aInt := pyIntValue(a)
	bi, bInt := pyIntValue(b)
	if aInt && bInt {
		if bi < 0 {
			if ai == 0 {
				return nil, fmt.Errorf("$F.pow: 0.0 cannot be raised to a negative power")
			}
			return math.Pow(float64(ai), float64(bi)), nil
		}
		return intPow(ai, bi)
	}
	fa, aok := pyNumericValue(a)
	fb, bok := pyNumericValue(b)
	if !aok || !bok {
		return nil, typeErr("pow", a, b)
	}
	if fa == 0 && fb < 0 {
		return nil, fmt.Errorf("$F.pow: 0.0 cannot be raised to a negative power")
	}
	r := math.Pow(fa, fb)
	if math.IsNaN(r) && !math.IsNaN(fa) && !math.IsNaN(fb) {
		// Python returns a complex number here; not representable.
		return nil, fmt.Errorf("$F.pow: complex result not supported (negative base with fractional exponent)")
	}
	return r, nil
}

func intPow(base, exp int64) (any, error) {
	r := int64(1)
	for exp > 0 {
		if exp&1 == 1 {
			var ok bool
			if r, ok = intMulOverflow(r, base); ok {
				return nil, fmt.Errorf("$F.pow: integer result too large")
			}
		}
		exp >>= 1
		if exp == 0 {
			break
		}
		var ok bool
		if base, ok = intMulOverflow(base, base); ok {
			return nil, fmt.Errorf("$F.pow: integer result too large")
		}
	}
	return r, nil
}

// abs is plaita $F.abs(a) — Python abs(): int stays int, float stays float.
func abs(a any) (any, error) {
	if i, ok := pyIntValue(a); ok {
		if i < 0 {
			i = -i
		}
		return i, nil
	}
	if f, ok := pyFloatValue(a); ok {
		return math.Abs(f), nil
	}
	return nil, fmt.Errorf("$F.abs: bad operand type for abs(): '%s'", pyTypeName(a))
}

// pyToInt converts a numeric value to int64 for ceil/floor/trunc; non-numbers
// are a Python TypeError.
func pyToInt(fn string, a any) (int64, error) {
	if i, ok := pyIntValue(a); ok {
		return i, nil
	}
	if f, ok := pyFloatValue(a); ok {
		if math.IsNaN(f) || math.IsInf(f, 0) || f > math.MaxInt64 || f < math.MinInt64 {
			return 0, fmt.Errorf("$F.%s: cannot convert float to integer", fn)
		}
		return int64(f), nil
	}
	return 0, fmt.Errorf("$F.%s: bad operand type: '%s'", fn, pyTypeName(a))
}

// ceil is plaita $F.ceil(a) — Python math.ceil: returns an int.
func ceil(a any) (any, error) {
	if _, ok := pyIntValue(a); ok {
		return a, nil
	}
	f, ok := pyFloatValue(a)
	if !ok {
		return nil, fmt.Errorf("$F.ceil: bad operand type: '%s'", pyTypeName(a))
	}
	return int64(math.Ceil(f)), nil
}

// floor is plaita $F.floor(a) — Python math.floor: returns an int.
func floor(a any) (any, error) {
	if _, ok := pyIntValue(a); ok {
		return a, nil
	}
	f, ok := pyFloatValue(a)
	if !ok {
		return nil, fmt.Errorf("$F.floor: bad operand type: '%s'", pyTypeName(a))
	}
	return int64(math.Floor(f)), nil
}

// trunc is plaita $F.trunc(a) — Python math.trunc: returns an int.
func trunc(a any) (any, error) {
	return pyToInt("trunc", a)
}

// sqrt is plaita $F.sqrt(a) — Python math.sqrt: always float; a negative
// operand is a math domain error.
func sqrt(a any) (any, error) {
	f, ok := pyNumericValue(a)
	if !ok {
		return nil, fmt.Errorf("$F.sqrt: bad operand type: '%s'", pyTypeName(a))
	}
	if f < 0 {
		return nil, fmt.Errorf("$F.sqrt: math domain error")
	}
	return math.Sqrt(f), nil
}

// round is plaita $F.round(a[, n]) — Python's builtin round: banker's
// rounding on the exact binary value; int result when n is omitted; a float
// input stays float when n is given (round(2.5, 0) == 2.0) and an int input
// passes through unchanged.
func round(a any, ndigits ...any) (any, error) {
	// int input: Python round(int) / round(int, n) both return the int as-is.
	if i, ok := pyIntValue(a); ok {
		return i, nil
	}
	f, ok := pyFloatValue(a)
	if !ok {
		return nil, fmt.Errorf("$F.round: type %s doesn't define __round__ method", pyTypeName(a))
	}
	if len(ndigits) == 0 {
		return pyRoundToInt(f)
	}
	n, err := pyIndexArg("round", ndigits[0])
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return pyRoundToIntKeepFloat(f), nil
	}
	return pyRoundToDigits(f, int(n)), nil
}

// pyRoundToInt implements Python round(float) → int (ties to even on the
// exact binary value). strconv.FormatFloat rounds the exact value with
// ties-to-even, matching CPython's _Py_dg_dtoa based builtin.
func pyRoundToInt(f float64) (int64, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, fmt.Errorf("$F.round: cannot convert float %s to integer", pyFloatRepr(f))
	}
	s := strconv.FormatFloat(f, 'f', 0, 64)
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("$F.round: rounded integer too large")
	}
	return i, nil
}

// pyRoundToIntKeepFloat is round(x, 0) for a float x: same rounding, float
// result (Python round(2.5, 0) == 2.0).
func pyRoundToIntKeepFloat(f float64) float64 {
	s := strconv.FormatFloat(f, 'f', 0, 64)
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return f
	}
	return v
}

// pyRoundToDigits implements round(x, n) for float x with n != 0. For n > 0
// the exact-value decimal rounding of FormatFloat matches Python; for n < 0
// Python rounds to a multiple of 10**-n (still returning a float).
func pyRoundToDigits(f float64, n int) float64 {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return f
	}
	if n < 0 {
		scale := math.Pow10(-n)
		shifted := f / scale
		s := strconv.FormatFloat(shifted, 'f', 0, 64)
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return f
		}
		return v * scale
	}
	// Guard absurd digit counts (Python allows them; they are no-ops beyond
	// the shortest round-trip digits of a float64).
	if n > 17 {
		n = 17
	}
	s := strconv.FormatFloat(f, 'f', n, 64)
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return f
	}
	return v
}

// pyIndexArg coerces an index-like argument the way Python requires integers
// ("object cannot be interpreted as an integer" on mismatch). Integral floats
// are accepted because the engine's JSON decoding yields float64 for numbers.
func pyIndexArg(fn string, v any) (int64, error) {
	if i, ok := pyIntValue(v); ok {
		return i, nil
	}
	if f, ok := pyFloatValue(v); ok {
		if f == float64(int64(f)) {
			return int64(f), nil
		}
		return 0, fmt.Errorf("$F.%s: 'float' object cannot be interpreted as an integer", fn)
	}
	return 0, fmt.Errorf("$F.%s: '%s' object cannot be interpreted as an integer", fn, pyTypeName(v))
}
