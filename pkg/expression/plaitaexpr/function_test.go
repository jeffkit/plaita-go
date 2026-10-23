package plaitaexpr

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuiltinFunctions(t *testing.T) {
	v, err := add(1.0, 2.0)
	require.NoError(t, err)
	require.EqualValues(t, 3.0, v)

	v, err = sub(3.0, 1.0)
	require.NoError(t, err)
	require.EqualValues(t, 2.0, v)

	v, err = mul(2.0, 3.0)
	require.NoError(t, err)
	require.EqualValues(t, 6.0, v)

	v, err = div(6.0, 3.0)
	require.NoError(t, err)
	require.EqualValues(t, 2.0, v)

	// division by zero is a ZeroDivisionError (issue #20), not 0
	_, err = div(6.0, 0.0)
	require.ErrorContains(t, err, "division by zero")

	v, err = pow(2.0, 3.0)
	require.NoError(t, err)
	require.EqualValues(t, 8.0, v)

	concatOut, err := concat("a", "b", "c")
	require.NoError(t, err)
	require.Equal(t, "abc", concatOut)
	require.Equal(t, "bc", substring("abc", 1, 2))
	require.Equal(t, "ABC", upper("abc"))
	require.Equal(t, "abc", lower("ABC"))
	require.Equal(t, "abc", strip(" abc "))
	replaceOut, err := replace("abc", "c", "")
	require.NoError(t, err)
	require.Equal(t, "ab", replaceOut)
	require.Equal(t, true, between(1, 0, 2))
}
