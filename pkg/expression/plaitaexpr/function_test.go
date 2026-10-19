package plaitaexpr

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuiltinFunctions(t *testing.T) {
	require.EqualValues(t, 3.0, add(1.0, 2.0))
	require.EqualValues(t, 2.0, sub(3.0, 1.0))
	require.EqualValues(t, 6.0, mul(2.0, 3.0))
	require.EqualValues(t, 2.0, div(6.0, 3.0))
	require.EqualValues(t, 0.0, div(6.0, 0.0))
	require.EqualValues(t, 8.0, pow(2.0, 3.0))
	require.EqualValues(t, "abc", concat("a", "b", "c"))
	require.EqualValues(t, "bc", substring("abc", 1, 2))
	require.EqualValues(t, "ABC", upper("abc"))
	require.EqualValues(t, "abc", lower("ABC"))
	require.EqualValues(t, "abc", strip(" abc "))
	require.EqualValues(t, "ab", replace("abc", "c", ""))
	require.EqualValues(t, true, between(1, 0, 2))
}
