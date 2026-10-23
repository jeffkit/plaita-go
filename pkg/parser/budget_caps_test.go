package parser

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// #49: the documented bare-engine entrypoint (FromJSONSource, also the
// filesystem provider's loader) must reject budget-cap violations with a
// named error at load time. Pre-fix, all four sources loaded clean.

// The "start" node carries the probe fields: it is a plain BaseNode embed
// and the only factory-registered type that needs no provider config.
func budgetProbeFlowJSON(nodeFields string) []byte {
	return []byte(fmt.Sprintf(`{
	  "id": "budget-probe",
	  "runtime": "go",
	  "version": "1",
	  "nodes": [
	    {"id": "start", "type": "start"%s, "next": "end"},
	    {"id": "end", "type": "end", "resultType": "success", "output": "done"}
	  ]
	}`, nodeFields))
}

func TestBudgetCapsAtParserEntrypoint(t *testing.T) {
	cases := []struct {
		name        string
		source      []byte
		wantErrText string
	}{
		{
			name:        "flow retryTimes=1e9",
			source:      budgetProbeFlowWithHandler(`, "errorHandler": {"strategy": "abort", "retryTimes": 1000000000}`),
			wantErrText: "cap 10",
		},
		{
			name:        "node retryTimes=11",
			source:      budgetProbeFlowJSON(`, "errorHandler": {"strategy": "abort", "retryTimes": 11}`),
			wantErrText: "cap 10",
		},
		{
			name:        "node timeout=P1DT1S (25h1s > 24h)",
			source:      budgetProbeFlowJSON(`, "timeout": "P1DT1S"`),
			wantErrText: "24h0m0s",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := FromJSONSource(tc.source)
			require.Error(t, err, "budget-cap violation must be a named load-time rejection")
			require.Contains(t, err.Error(), tc.wantErrText)
		})
	}
}

func budgetProbeFlowWithHandler(flowHandler string) []byte {
	return []byte(fmt.Sprintf(`{
	  "id": "budget-probe",
	  "runtime": "go",
	  "version": "1"%s,
	  "nodes": [
	    {"id": "start", "type": "start", "next": "end"},
	    {"id": "end", "type": "end", "resultType": "success", "output": "done"}
	  ]
	}`, flowHandler))
}

// Caps-edge shapes stay legal at the entrypoint: retryTimes=10 everywhere
// and a node timeout of exactly 24h.
func TestBudgetCapsLegalAtParserEntrypoint(t *testing.T) {
	wf, err := FromJSONSource(budgetProbeFlowJSON(`, "errorHandler": {"strategy": "continue", "retryTimes": 10}, "timeout": "PT24H"`))
	require.NoError(t, err)
	require.NotNil(t, wf)
}
