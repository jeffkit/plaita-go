package ir

import (
	"fmt"
	"strings"
	"testing"
)

// #57: unrepresentable ISO timeout budgets (past time.Duration's ~292-year
// int64-nanosecond domain) must fail the load with the named error — not
// wrap into a negative / zero / shortened budget that only detonates at
// execute time (node GetTimeout() used to read "PT-9223371273.70955S",
// "PT0S", or "PT1526.290448384S" for the three wrap shapes below).
func TestLoadRejectsUnrepresentableTimeouts(t *testing.T) {
	cases := []struct {
		name    string
		timeout string
	}{
		{"node timeout wraps negative", "P106752D"},
		{"node timeout wraps to zero", "P281474976710656D"},
		{"node timeout wraps shortened", "P213504D"},
	}
	for _, tc := range cases {
		doc := fmt.Sprintf(`{"flow_id":"to","runtime":"go","version":"1","nodes":[
{"id":"s","type":"start","next":"e"},
{"id":"e","type":"end","resultType":"success","output":"done","timeout":%q}]}`, tc.timeout)
		_, err := Load([]byte(doc))
		if err == nil {
			t.Errorf("%s: %q loaded, want the not-representable load error", tc.name, tc.timeout)
			continue
		}
		if !strings.Contains(err.Error(), "not representable") {
			t.Errorf("%s: %q error = %q, want the named not-representable reason", tc.name, tc.timeout, err)
		}
		if strings.Contains(err.Error(), "cap") {
			t.Errorf("%s: %q error = %q must not be phrased as the 24h cap", tc.name, tc.timeout, err)
		}
	}

	// flow-level timeout: same overflow, same named rejection (#35: flow
	// level has no cap, so the wrap is never seen by a second gate)
	flowDoc := `{"flow_id":"tf","runtime":"go","version":"1","timeout":"P106752D","nodes":[
{"id":"s","type":"start","next":"e"},
{"id":"e","type":"end","resultType":"success","output":"done"}]}`
	if _, err := Load([]byte(flowDoc)); err == nil || !strings.Contains(err.Error(), "not representable") {
		t.Errorf("flow-level P106752D: err = %v, want the named not-representable load error", err)
	}
}

// #57 positive controls: representable spellings keep loading with the same
// rendered budgets, and the P106751D rejection keeps its 24h cap wording.
func TestLoadTimeoutPositiveControlsUnchanged(t *testing.T) {
	cases := []struct {
		timeout any
		want    string
	}{
		{"PT24H", "PT86400S"},
		{"PT86400S", "PT86400S"},
		{"PT0S", "PT0S"},
		{"PT", "PT0S"},
		{"P1D", "PT86400S"},
		{86400000, "PT86400S"}, // milliseconds number
		{300, "PT0.3S"},
		{0, "PT0S"},
	}
	for _, tc := range cases {
		var tv string
		if s, ok := tc.timeout.(string); ok {
			tv = fmt.Sprintf("%q", s)
		} else {
			tv = fmt.Sprintf("%v", tc.timeout)
		}
		doc := fmt.Sprintf(`{"flow_id":"tp","runtime":"go","version":"1","nodes":[
{"id":"s","type":"start","next":"e"},
{"id":"e","type":"end","resultType":"success","output":"done","timeout":%s}]}`, tv)
		wf, err := Load([]byte(doc))
		if err != nil {
			t.Errorf("timeout %v: load = %v, want success", tc.timeout, err)
			continue
		}
		for _, n := range wf.Nodes {
			if n.GetID() != "e" {
				continue
			}
			if got := n.GetTimeout().String(); got != tc.want {
				t.Errorf("timeout %v: GetTimeout() = %s, want %s", tc.timeout, got, tc.want)
			}
			if n.GetTimeout().TimeDuration() < 0 {
				t.Errorf("timeout %v: GetTimeout() = %s: a negative budget must be impossible", tc.timeout, n.GetTimeout())
			}
		}
	}

	// just inside the domain: still parses, still rejected by the 24h cap,
	// and the cap wording (not the representability wording) is the reason.
	// P1DT0.5H (24h30m) sits over the cap too — parseable but not loadable.
	for _, in := range []string{"P106751D", "P1DT0.5H"} {
		doc := fmt.Sprintf(`{"flow_id":"tc","runtime":"go","version":"1","nodes":[
{"id":"s","type":"start","next":"e"},
{"id":"e","type":"end","resultType":"success","output":"done","timeout":%q}]}`, in)
		_, err := Load([]byte(doc))
		if err == nil {
			t.Fatalf("%s loaded, want the 24h cap rejection", in)
		}
		if !strings.Contains(err.Error(), "exceeds") || strings.Contains(err.Error(), "not representable") {
			t.Errorf("%s error = %q, want the 24h cap wording", in, err)
		}
	}
}
