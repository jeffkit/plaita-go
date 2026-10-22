package ir

import (
	"bytes"
	"encoding/json"
	"log"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// Tests for the ir_validate rule port (issue #15: R5 $NODE refs, R6 $F
// functions, R7 outputType literal precheck, unknown-key hygiene) and the
// exposeEnv passthrough contract (issue #1, engine side).

// normalizedDoc runs the Load pipeline up to (and including) validation and
// hands back the normalized document — the exact map Load re-encodes before
// unmarshalling it into workflow.Workflow.
func normalizedDoc(t *testing.T, data []byte) map[string]any {
	t.Helper()
	var doc map[string]any
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	require.NoError(t, dec.Decode(&doc))
	require.NoError(t, normalizeDoc(doc, scopeRoot, nil))
	require.NoError(t, validateDoc(doc, scopeRoot))
	return doc
}

func captureIrLog(t *testing.T, fn func()) string {
	t.Helper()
	var buf bytes.Buffer
	out, flags := log.Writer(), log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	defer func() { log.SetOutput(out); log.SetFlags(flags) }()
	fn()
	return buf.String()
}

// --- issue #15 acceptance table ---

func TestLoadRejectsUnknownNodeRef(t *testing.T) {
	_, err := Load([]byte(`{"nodes":[
		{"type":"start","id":"s","next":"e"},
		{"type":"end","id":"e","resultType":"success","output":"$NODE.ghost"}]}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ghost")
	assert.Contains(t, err.Error(), "$NODE")
}

func TestLoadRejectsUnregisteredFunction(t *testing.T) {
	_, err := Load([]byte(`{"nodes":[
		{"type":"start","id":"s","next":"e"},
		{"type":"end","id":"e","resultType":"success","output":"$F.nope(1)"}]}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nope")
	assert.Contains(t, err.Error(), "$F")
}

func TestLoadRejectsLiteralOutputTypeMismatch(t *testing.T) {
	_, err := Load([]byte(`{"nodes":[
		{"type":"start","id":"s","next":"e"},
		{"type":"end","id":"e","resultType":"success","output":"abc","outputType":{"type":"integer"}}]}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "outputType")
}

func TestUnknownNodeFieldWarnsNotSilent(t *testing.T) {
	data := []byte(`{"nodes":[
		{"type":"start","id":"s","next":"a"},
		{"type":"assignment","id":"a","typo_field":1,"next":"e"},
		{"type":"end","id":"e","resultType":"success","output":1}]}`)
	logged := captureIrLog(t, func() {
		wf, err := Load(data)
		require.NoError(t, err)
		var assignment workflow.Node
		for _, n := range wf.Nodes {
			if n.GetID() == "a" {
				assignment = n
			}
		}
		require.NotNil(t, assignment)
		assert.Equal(t, "typo_field", assignment.GetMetadata()["unknownFields"])
	})
	assert.Contains(t, logged, "unknown config key")
	assert.Contains(t, logged, "typo_field")
}

// --- acceptance: legitimate references must not be rejected ---

func TestLegitReferencesLoadAndExecute(t *testing.T) {
	data := []byte(`{"nodes":[
		{"type":"start","id":"s","next":"e"},
		{"type":"end","id":"e","resultType":"success","output":"$F.eq(1,1)"}]}`)
	wf, err := Load(data)
	require.NoError(t, err)
	got, err := wf.Execute(t.Context(), map[string]any{})
	require.NoError(t, err)
	assert.Equal(t, true, got)
}

func TestRefScanBoundaries(t *testing.T) {
	cases := []struct {
		name string
		node map[string]any
	}{
		{"dynamic id not scanned", map[string]any{
			"type": "end", "id": "e", "resultType": "success", "output": "$NODE.$INPUT.k"}},
		{"identifier prefix not a root ref", map[string]any{
			"type": "end", "id": "e", "resultType": "success", "output": "pre$NODE.ghost"}},
		{"$PARENT.$NODE not this graph's ref", map[string]any{
			"type": "end", "id": "e", "resultType": "success", "output": "$PARENT.$NODE.ghost"}},
		{"node-level error payload is literal data", map[string]any{
			"type": "end", "id": "e", "resultType": "error",
			"error": map[string]any{"code": "E", "message": "see $NODE.ghost"}}},
		{"template without refs", map[string]any{
			"type": "end", "id": "e", "resultType": "success", "output": "plain text"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			doc := map[string]any{"nodes": []any{
				map[string]any{"type": "start", "id": "s", "next": "e"},
				tc.node,
			}}
			require.NoError(t, normalizeDoc(doc, scopeRoot, nil))
			require.NoError(t, validateDoc(doc, scopeRoot))
		})
	}
}

func TestRefScanCatchesNestedContainers(t *testing.T) {
	// refs inside nested dicts/lists are scanned; a nested `error` key is NOT
	// exempt (only the node-level error payload is, ir_validate #144).
	data := []byte(`{"nodes":[
		{"type":"start","id":"s","next":"e"},
		{"type":"end","id":"e","resultType":"success","output":{"error":"$NODE.ghost"}}]}`)
	_, err := Load(data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ghost")
}

func TestRefScanSkipsCodeBody(t *testing.T) {
	data := []byte(`{"nodes":[
		{"type":"start","id":"s","next":"c"},
		{"type":"code","id":"c","language":"javascript","code":"return $NODE.ghost","next":"e"},
		{"type":"end","id":"e","resultType":"success","output":1}]}`)
	_, err := Load(data)
	require.NoError(t, err)
}

func TestNodeRefHint(t *testing.T) {
	_, err := Load([]byte(`{"nodes":[
		{"type":"start","id":"start","next":"end"},
		{"type":"end","id":"end","resultType":"success","output":"$NODE.strt"}]}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `did you mean "start"`)
}

// --- acceptance: embedded childFlow / branches are scanned recursively ---

func TestChildFlowRefScanScopedToSubflow(t *testing.T) {
	// $NODE.s refers to the parent's start id — invisible inside the childflow
	// scope ($NODE resolves per subflow), so it must be rejected here.
	data := []byte(`{"nodes":[
		{"type":"start","id":"s","next":"c"},
		{"type":"child","id":"c","childFlow":{"nodes":[
			{"type":"start","id":"cs","next":"ce"},
			{"type":"end","id":"ce","resultType":"success","output":"$NODE.s"}]}}]}`)
	_, err := Load(data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "s")
}

func TestEmbeddedFlowsScanned(t *testing.T) {
	childDoc := `{"nodes":[
		{"type":"start","id":"s","next":"c"},
		{"type":"child","id":"c","childFlow":{"nodes":[
			{"type":"start","id":"cs","next":"ce"},
			{"type":"end","id":"ce","resultType":"success","output":"$NODE.ghost"}]}}]}`
	_, err := Load([]byte(childDoc))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ghost")

	branchDoc := `{"nodes":[
		{"type":"start","id":"s","next":"p"},
		{"type":"parallel","id":"p","joinBranches":["a"],"branches":[{"name":"a","flow":{"nodes":[
			{"type":"start","id":"st","next":"e"},
			{"type":"end","id":"e","resultType":"success","output":"$F.nope(1)"}]}}]}]}`
	_, err = Load([]byte(branchDoc))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nope")
}

// --- R7 edges ---

func TestOutputTypePrecheckSkipsDynamicOutput(t *testing.T) {
	data := []byte(`{"nodes":[
		{"type":"start","id":"s","next":"a"},
		{"type":"assignment","id":"a","output":"$INPUT.x","outputType":{"type":"integer"},"next":"e"},
		{"type":"end","id":"e","resultType":"success","output":"$NODE.a"}]}`)
	wf, err := Load(data)
	require.NoError(t, err)
	got, err := wf.Execute(t.Context(), map[string]any{"x": 3})
	require.NoError(t, err)
	assert.Equal(t, 3, got) // input passed as Go value, so int flows through
}

func TestOutputTypePrecheckPassesMatchingLiteral(t *testing.T) {
	data := []byte(`{"nodes":[
		{"type":"start","id":"s","next":"a"},
		{"type":"assignment","id":"a","output":7,"outputType":{"type":"integer"},"next":"e"},
		{"type":"end","id":"e","resultType":"success","output":1}]}`)
	_, err := Load(data)
	require.NoError(t, err)
}

// --- exposeEnv passthrough (issue #1, ir side) ---

func TestExposeEnvPassesThroughAtDocTopLevel(t *testing.T) {
	data := []byte(`{"flow_id":"envy","exposeEnv":["HOME"],
		"nodes":[
			{"type":"start","id":"s","next":"e"},
			{"type":"end","id":"e","resultType":"success","output":"ok"}]}`)

	doc := normalizedDoc(t, data)

	// contract: folded to the top-level engine spelling, values intact
	env, ok := doc["exposeEnv"].([]any)
	require.True(t, ok, "exposeEnv must stay at doc top level")
	require.Len(t, env, 1)
	assert.Equal(t, "HOME", env[0])

	// old behavior removed: no metadata stash
	meta, ok := doc["metadata"].(map[string]string)
	require.True(t, ok)
	assert.NotContains(t, meta, "exposeEnv")

	// cross-team contract: the bytes Load hands to the engine unmarshal carry
	// top-level exposeEnv as []string — once workflow.Workflow grows the
	// agreed ExposeEnv field (`json:"exposeEnv,omitempty"`), these exact bytes
	// populate it.
	raw, err := json.Marshal(doc)
	require.NoError(t, err)
	var probe struct {
		ExposeEnv []string `json:"exposeEnv"`
	}
	require.NoError(t, json.Unmarshal(raw, &probe))
	assert.Equal(t, []string{"HOME"}, probe.ExposeEnv)

	// if the engine field has landed on this branch, assert the full arrival
	if f, found := reflect.TypeOf(workflow.Workflow{}).FieldByName("ExposeEnv"); found {
		assert.Equal(t, "exposeEnv,omitempty", f.Tag.Get("json"))
		wf, err := Load(data)
		require.NoError(t, err)
		got := reflect.ValueOf(wf).Elem().FieldByName("ExposeEnv")
		assert.Equal(t, []string{"HOME"}, got.Interface())
	} else {
		t.Log("workflow.Workflow.ExposeEnv not landed yet (engine-side issue #1); doc-level contract asserted above")
	}

	// end-to-end: the flow still loads and executes
	wf, err := Load(data)
	require.NoError(t, err)
	got, err := wf.Execute(t.Context(), map[string]any{})
	require.NoError(t, err)
	assert.Equal(t, "ok", got)
}

func TestExposeEnvSnakeAliasFolds(t *testing.T) {
	data := []byte(`{"flow_id":"envy","expose_env":["A_B"],
		"nodes":[
			{"type":"start","id":"s","next":"e"},
			{"type":"end","id":"e","resultType":"success","output":"ok"}]}`)
	doc := normalizedDoc(t, data)
	require.Len(t, doc["exposeEnv"], 1)
	assert.NotContains(t, doc, "expose_env")
}

func TestExposeEnvCamelWinsWhenBothSpellingsPresent(t *testing.T) {
	data := []byte(`{"expose_env":["SNAKE"],"exposeEnv":["CAMEL"],
		"nodes":[
			{"type":"start","id":"s","next":"e"},
			{"type":"end","id":"e","resultType":"success","output":"ok"}]}`)
	doc := normalizedDoc(t, data)
	assert.Equal(t, "CAMEL", doc["exposeEnv"].([]any)[0])
}

func TestExposeEnvEmptyListLoads(t *testing.T) {
	data := []byte(`{"exposeEnv":[],
		"nodes":[
			{"type":"start","id":"s","next":"e"},
			{"type":"end","id":"e","resultType":"success","output":"ok"}]}`)
	_, err := Load(data)
	require.NoError(t, err)
}

func TestExposeEnvInvalidEntriesRejected(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{"wildcard", `["*"]`},
		{"whitespace padded", `[" HOME"]`},
		{"blank", `[""]`},
		{"non-string", `[42]`},
		{"bare string not an array", `"HOME"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := []byte(`{"exposeEnv":` + tc.raw + `,"nodes":[
				{"type":"start","id":"s","next":"e"},
				{"type":"end","id":"e","resultType":"success","output":"ok"}]}`)
			_, err := Load(data)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "exposeEnv")
		})
	}
}

func TestExposeEnvOnEmbeddedFlowValidated(t *testing.T) {
	data := []byte(`{"nodes":[
		{"type":"start","id":"s","next":"c"},
		{"type":"child","id":"c","childFlow":{"exposeEnv":["*"],"nodes":[
			{"type":"start","id":"cs","next":"ce"},
			{"type":"end","id":"ce","resultType":"success","output":"ok"}]}}]}`)
	_, err := Load(data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exposeEnv")
}

// known camel aliases fold before validation and must not warn.
func TestKnownAliasFieldsDoNotWarn(t *testing.T) {
	data := []byte(`{"nodes":[
		{"type":"start","id":"s","next":"g"},
		{"type":"gate","id":"g","gateName":"check","command":"true","timeoutSecs":30,"dryRun":false,"next":"e"},
		{"type":"end","id":"e","resultType":"success","output":1}]}`)
	logged := captureIrLog(t, func() {
		_, err := Load(data)
		require.NoError(t, err)
	})
	assert.NotContains(t, logged, "unknown config key")
	assert.True(t, !strings.Contains(logged, "gateName") && !strings.Contains(logged, "timeoutSecs"))
}
