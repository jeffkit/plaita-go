package ir

import (
	"fmt"
	"strings"
	"testing"

	"github.com/jeffkit/plaita-go/pkg/workflow/nodes"
)

// #58: requestTimeout (seconds; the per-request whole-request budget incl.
// all redirect hops) used to be folded into the node timeout and deleted
// from the node map, so nodes.HTTP.RequestTimeoutSecs could never receive
// the declared value via the deployment IR and the transport fell back to
// the 30s default. The declaration must survive normalization and land on
// the node; a coexisting node timeout keeps its own meaning (total node
// budget incl. retries — #35/r64), and metadata.requestTimeoutIgnored is
// gone (no production reader reads stashed metadata).
func TestLoadHTTPRequestTimeoutReachesTheNode(t *testing.T) {
	cases := []struct {
		name      string
		extra     string
		wantSecs  float64
		wantISO   string
		wantUnset bool
	}{
		{"coexisting node timeout", `"timeout":300000,"requestTimeout":5`, 5, "PT300S", false},
		{"camel alone", `"requestTimeout":600`, 600, "", true},
		{"snake alone", `"request_timeout":600`, 600, "", true},
	}
	for _, tc := range cases {
		doc := fmt.Sprintf(`{"flow_id":"h","runtime":"go","version":"1","nodes":[
{"id":"s","type":"start","next":"h"},
{"id":"h","type":"http","url":"http://example.com/x",%s,"next":"e"},
{"id":"e","type":"end","resultType":"success","output":"done"}]}`, tc.extra)
		wf, err := Load([]byte(doc))
		if err != nil {
			t.Errorf("%s: load = %v, want success", tc.name, err)
			continue
		}
		var h *nodes.HTTP
		for _, n := range wf.Nodes {
			if hn, ok := n.(*nodes.HTTP); ok {
				h = hn
			}
		}
		if h == nil {
			t.Fatalf("%s: no http node in the loaded workflow", tc.name)
		}
		if h.RequestTimeoutSecs != tc.wantSecs {
			t.Errorf("%s: RequestTimeoutSecs = %v, want %v", tc.name, h.RequestTimeoutSecs, tc.wantSecs)
		}
		if tc.wantUnset && !h.GetTimeout().IsZero() {
			t.Errorf("%s: GetTimeout() = %s, want unset (no synthesized node timeout)", tc.name, h.GetTimeout())
		}
		if !tc.wantUnset && h.GetTimeout().String() != tc.wantISO {
			t.Errorf("%s: GetTimeout() = %s, want the declared node timeout %s", tc.name, h.GetTimeout(), tc.wantISO)
		}
		if _, has := h.GetMetadata()["requestTimeoutIgnored"]; has {
			t.Errorf("%s: metadata still carries requestTimeoutIgnored = %q", tc.name, h.GetMetadata()["requestTimeoutIgnored"])
		}
	}

	// no declaration: field stays zero and nothing is synthesized
	doc := `{"flow_id":"h","runtime":"go","version":"1","nodes":[
{"id":"s","type":"start","next":"h"},
{"id":"h","type":"http","url":"http://example.com/x","next":"e"},
{"id":"e","type":"end","resultType":"success","output":"done"}]}`
	wf, err := Load([]byte(doc))
	if err != nil {
		t.Fatalf("plain http node: load = %v, want success", err)
	}
	for _, n := range wf.Nodes {
		if h, ok := n.(*nodes.HTTP); ok {
			if h.RequestTimeoutSecs != 0 {
				t.Errorf("plain http node: RequestTimeoutSecs = %v, want 0", h.RequestTimeoutSecs)
			}
			if !h.GetTimeout().IsZero() {
				t.Errorf("plain http node: GetTimeout() = %s, want unset", h.GetTimeout())
			}
		}
	}
}

// #58: the declaration is validated fail-closed at load, mirroring plaita's
// model validator (plaita/node/http.py MAX_REQUEST_TIMEOUT_SECONDS=600).
// Pre-fix: 0/-1 were silently deleted (load OK), 601/86400 loaded by folding
// into the node timeout, and 86401/1e9 were misphrased as the 24h cap.
func TestLoadHTTPRequestTimeoutRejectsInvalid(t *testing.T) {
	cases := []string{"0", "-1", "601", "86400", "1000000000", `"abc"`}
	for _, v := range cases {
		doc := fmt.Sprintf(`{"flow_id":"h","runtime":"go","version":"1","nodes":[
{"id":"s","type":"start","next":"h"},
{"id":"h","type":"http","url":"http://example.com/x","requestTimeout":%s,"next":"e"},
{"id":"e","type":"end","resultType":"success","output":"done"}]}`, v)
		_, err := Load([]byte(doc))
		if err == nil {
			t.Errorf("requestTimeout %s loaded, want the fail-closed load error", v)
			continue
		}
		if !strings.Contains(err.Error(), "requestTimeout") {
			t.Errorf("requestTimeout %s error = %q, want the field name", v, err)
		}
		if v != `"abc"` && !strings.Contains(err.Error(), "600") {
			t.Errorf("requestTimeout %s error = %q, want the 600 cap in the message", v, err)
		}
	}
}
