package ir

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// #62 end to end: an end.output whose expression carries `$`-bearing business
// text in a string argument loads and executes to the Python value (the engine
// must not rewrite the literal nor leak its `__pvN` internal naming).

func TestR62LiteralRewriteEndToEnd(t *testing.T) {
	flow := []byte(`{
		"flow_id": "r62_literal_e2e",
		"inputType": {"dataType": "object"},
		"nodes": [
			{"id": "s", "next": "e", "type": "start"},
			{"id": "e", "output": "$F.concat(\"see $INPUT.a here\", $INPUT.a)",
			 "resultType": "success", "type": "end"}
		],
		"runtime": "python"
	}`)
	wf, err := Load(flow)
	require.NoError(t, err)

	out, execErr := wf.Execute(t.Context(), map[string]any{"a": 5})
	require.NoError(t, execErr)
	assert.Equal(t, "see $INPUT.a here5", out)
}
