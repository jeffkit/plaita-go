package plaitaexpr

import (
	"math"

	"github.com/shopspring/decimal"
	"github.com/spf13/cast"
)

func add(a, b any) float64 {
	return cast.ToFloat64(a) + cast.ToFloat64(b)
}

func sub(a, b any) float64 {
	return cast.ToFloat64(a) - cast.ToFloat64(b)
}

func mul(a, b any) float64 {
	return cast.ToFloat64(a) * cast.ToFloat64(b)
}

func div(a, b any) float64 {
	fa, fb := cast.ToFloat64(a), cast.ToFloat64(b)
	if fb == 0 {
		return 0
	}
	return fa / fb
}

func mod(a, b any) float64 {
	return math.Mod(cast.ToFloat64(a), cast.ToFloat64(b))
}

func pow(a, b any) float64 {
	return math.Pow(cast.ToFloat64(a), cast.ToFloat64(b))
}

func abs(a any) float64 {
	return math.Abs(cast.ToFloat64(a))
}

func ceil(a any) float64 {
	return math.Ceil(cast.ToFloat64(a))
}

func floor(a any) float64 {
	return math.Floor(cast.ToFloat64(a))
}

func round(a any, b any) float64 {
	return decimal.NewFromFloat(cast.ToFloat64(a)).Round(cast.ToInt32(b)).InexactFloat64()
}

func trunc(a any) float64 {
	return math.Trunc(cast.ToFloat64(a))
}

func sqrt(a any) float64 {
	return math.Sqrt(cast.ToFloat64(a))
}
