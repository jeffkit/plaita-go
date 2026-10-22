package ir

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRejectLoneSurrogateEscapes pins the G-1 decode-strictness scan: lone
// \udXXX escapes are outside the UTF-8 hash domain (A129-01), paired escapes
// and literal U+FFFD stay legal, and escaped-backslash text must never read
// as an escape.
func TestRejectLoneSurrogateEscapes(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		wantErr bool
	}{
		{name: "lone high", raw: `{"a":"x\ud800y"}`, wantErr: true},
		{name: "lone low", raw: `{"a":"x\udc00y"}`, wantErr: true},
		{name: "lone high uppercase hex", raw: `{"a":"\uD800"}`, wantErr: true},
		{name: "high then non-pairing low", raw: `{"a":"\ud800\ud800"}`, wantErr: true},
		{name: "high followed by plain text", raw: `{"a":"\ud800x"}`, wantErr: true},
		{name: "lone low at string end", raw: `{"a":"\udfff"}`, wantErr: true},
		{name: "second key lone surrogate", raw: `{"a":"ok","b\ud800c":"v"}`, wantErr: true},
		{name: "inside array", raw: `{"a":["\ude00"]}`, wantErr: true},

		{name: "paired emoji escape accepted", raw: `{"a":"\ud83d\ude00"}`},
		{name: "two pairs accepted", raw: `{"a":"\ud83d\ude00\ud83e\uddfd"}`},
		{name: "literal U+FFFD escape accepted", raw: `{"a":"x\ufffdy"}`},
		{name: "plain BMP escape accepted", raw: `{"a":"caf\u00e9"}`},
		{
			// "\\" + "ud800" is an escaped backslash followed by literal
			// text — Python loads it to a plain ASCII string; scanning the
			// second backslash as an escape would false-reject.
			name: "escaped backslash before ud800 text accepted",
			raw:  `{"a":"\\ud800"}`,
		},
		{
			// An escaped quote must not flip string state mid-scan.
			name: "escaped quotes around ud800 text accepted",
			raw:  `{"a":"say \"hi\" ud800 now"}`,
		},
		{name: "no escapes at all", raw: `{"a":"emoji 🚀 raw"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := RejectLoneSurrogateEscapes([]byte(tc.raw))
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "lone surrogate")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// The named rejection must survive the full engine load path with the
// standard error prefix.
func TestLoadLoneSurrogateErrorShape(t *testing.T) {
	_, err := Load([]byte(`{"flow_id":"probe","nodes":[{"id":"s","type":"start","next":"e","name":"a\ud800b"},{"id":"e","type":"end"}]}`))
	require.Error(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "ir: parse flow json: "), "got: %s", err.Error())
	assert.Contains(t, err.Error(), `lone surrogate escape \ud800`)
}
