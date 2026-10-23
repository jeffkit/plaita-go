package ir

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// #63 end to end: an end.output whose expression carries syntax beyond the
// variable grammar must fail the node with a parse error — Python raises
// ParseException for the same flow (HEAD loaded, executed and returned nil).

func TestR63TrailingTextEndToEnd(t *testing.T) {
	flow := []byte(`{
		"flow_id": "r63_parse_all_e2e",
		"inputType": {"dataType": "object"},
		"nodes": [
			{"id": "s", "next": "e", "type": "start"},
			{"id": "e", "output": "$INPUT.a + 1", "resultType": "success", "type": "end"}
		],
		"runtime": "python"
	}`)
	wf, err := Load(flow)
	require.NoError(t, err)

	_, execErr := wf.Execute(t.Context(), map[string]any{"a": 5})
	require.Error(t, execErr)
	assert.Contains(t, execErr.Error(), "unable to parse")
}
