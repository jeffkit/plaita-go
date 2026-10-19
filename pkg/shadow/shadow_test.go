package shadow

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func jsonValue(t *testing.T, raw string) any {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	var value any
	require.NoError(t, dec.Decode(&value))
	return value
}

func TestCompareValues(t *testing.T) {
	cases := []struct {
		name  string
		ref   any
		act   any
		want  []string // expected diff paths
		kinds []string
	}{
		{
			name: "numeric equivalence int float",
			ref:  jsonValue(t, `{"a": 1, "b": 2.0}`),
			act:  jsonValue(t, `{"b": 2, "a": 1.0}`),
			want: nil,
		},
		{
			name:  "nested value mismatch",
			ref:   jsonValue(t, `{"a": {"b": 1}}`),
			act:   jsonValue(t, `{"a": {"b": 2}}`),
			want:  []string{"$.a.b"},
			kinds: []string{DiffValueMismatch},
		},
		{
			name:  "missing key",
			ref:   jsonValue(t, `{"a": 1, "b": 2}`),
			act:   jsonValue(t, `{"a": 1}`),
			want:  []string{"$.b"},
			kinds: []string{DiffMissing},
		},
		{
			name:  "unexpected key",
			ref:   jsonValue(t, `{"a": 1}`),
			act:   jsonValue(t, `{"a": 1, "extra": true}`),
			want:  []string{"$.extra"},
			kinds: []string{DiffUnexpected},
		},
		{
			name:  "array length",
			ref:   jsonValue(t, `[1, 2]`),
			act:   jsonValue(t, `[1, 2, 3]`),
			want:  []string{"$"},
			kinds: []string{DiffLength},
		},
		{
			name:  "bool is not a number",
			ref:   jsonValue(t, `true`),
			act:   jsonValue(t, `1`),
			want:  []string{"$"},
			kinds: []string{DiffTypeMismatch},
		},
		{
			name:  "type mismatch object vs array",
			ref:   jsonValue(t, `{"a": 1}`),
			act:   jsonValue(t, `[1]`),
			want:  []string{"$"},
			kinds: []string{DiffTypeMismatch},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			diffs := CompareValues(tc.ref, tc.act)
			if len(tc.want) == 0 {
				assert.Empty(t, diffs)
				return
			}
			require.Len(t, diffs, len(tc.want))
			for i, path := range tc.want {
				assert.Equal(t, path, diffs[i].Path)
				assert.Equal(t, tc.kinds[i], diffs[i].Kind)
			}
		})
	}
}

func TestCompareValuesGoTypedContainers(t *testing.T) {
	ref := map[string]any{"items": []any{json.Number("1"), json.Number("2.5")}}
	act := map[string]any{"items": []int{1, 2} /* second differs */}
	diffs := CompareValues(ref, act)
	require.Len(t, diffs, 1)
	assert.Equal(t, "$.items[1]", diffs[0].Path)
}

func TestCompareTracesGroupsFlows(t *testing.T) {
	ref := []TraceEvent{
		{FlowID: "root", NodeID: "start", Type: "start", Result: nil},
		{FlowID: "branch_a", NodeID: "start", Type: "start", Result: nil},
		{FlowID: "root", NodeID: "p", Type: "parallel", Result: map[string]any{"a": 1}},
		{FlowID: "branch_a", NodeID: "end", Type: "end", Result: 1},
	}
	// Interleaving across flows differs; order inside each flow does not.
	act := []TraceEvent{
		{FlowID: "root", NodeID: "start", Type: "start", Result: nil},
		{FlowID: "root", NodeID: "p", Type: "parallel", Result: map[string]any{"a": 1}},
		{FlowID: "branch_a", NodeID: "start", Type: "start", Result: nil},
		{FlowID: "branch_a", NodeID: "end", Type: "end", Result: json.Number("1")},
	}
	assert.Empty(t, CompareTraces(ref, act))
}

func TestCompareTracesDetectsDifferences(t *testing.T) {
	ref := []TraceEvent{
		{FlowID: "root", NodeID: "start", Type: "start", Result: nil},
		{FlowID: "root", NodeID: "a", Type: "assignment", Result: json.Number("1")},
	}
	act := []TraceEvent{
		{FlowID: "root", NodeID: "start", Type: "start", Result: nil},
		{FlowID: "root", NodeID: "b", Type: "assignment", Result: json.Number("2")},
	}
	diffs := CompareTraces(ref, act)
	require.Len(t, diffs, 2)
	assert.Equal(t, DiffNodeID, diffs[0].Kind)
	assert.Equal(t, DiffValueMismatch, diffs[1].Kind)
}

func TestCompareTracesLength(t *testing.T) {
	ref := []TraceEvent{
		{FlowID: "root", NodeID: "start", Type: "start"},
		{FlowID: "root", NodeID: "end", Type: "end"},
	}
	act := []TraceEvent{{FlowID: "root", NodeID: "start", Type: "start"}}
	diffs := CompareTraces(ref, act)
	require.Len(t, diffs, 1)
	assert.Equal(t, DiffTraceLength, diffs[0].Kind)
}

func TestCompareTracesFlowMissing(t *testing.T) {
	ref := []TraceEvent{{FlowID: "root", NodeID: "start", Type: "start"}}
	act := []TraceEvent{{FlowID: "other", NodeID: "start", Type: "start"}}
	diffs := CompareTraces(ref, act)
	require.Len(t, diffs, 2)
	assert.Equal(t, DiffTraceFlowMiss, diffs[0].Kind)
	assert.Equal(t, DiffTraceFlowExtra, diffs[1].Kind)
}

func TestRunGoInvalidFlow(t *testing.T) {
	outcome := RunGo(t.Context(), []byte(`{"nodes":[]}`), nil)
	require.NotNil(t, outcome)
	assert.False(t, outcome.OK)
	assert.NotEmpty(t, outcome.Error)
}

func TestCompareExpectationDetectsBothEnginesWrong(t *testing.T) {
	sample := Sample{Expect: json.RawMessage(`{"a": 1}`)}
	python := &Outcome{OK: true, Result: jsonValue(t, `{"a": 2}`)}
	goOutcome := &Outcome{OK: true, Result: jsonValue(t, `{"a": 2}`)}
	diffs := compareExpectation(sample, python, goOutcome)
	require.Len(t, diffs, 2)
	assert.Equal(t, "$expect.python.a", diffs[0].Path)
	assert.Equal(t, "$expect.go.a", diffs[1].Path)
}

func TestLoadSamples(t *testing.T) {
	samples, err := LoadSamples(filepath.Join("testdata", "samples"))
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(samples), 10, "M2 requires at least 10 synthetic samples")

	forbidden := map[string]bool{
		"http": true, "redis": true, "agentrun": true, "code": true,
		"git_publish": true, "github_comment": true, "writefile": true,
		"notify": true, "feishu_webhook": true, "dingtalk_webhook": true,
		"slack_webhook": true, "wecom_webhook": true, "generic_webhook": true,
		"email_send": true, "api_request": true, "llm": true, "hitl": true,
		"decision": true, "capture": true, "sql_query": true,
	}
	seen := map[string]bool{}
	for _, sample := range samples {
		assert.NotEmpty(t, sample.Name)
		assert.False(t, seen[sample.Name], "duplicate sample name %q", sample.Name)
		seen[sample.Name] = true
		assert.NotEmpty(t, sample.Description, "sample %s missing description", sample.Name)
		assert.NotEmpty(t, sample.Expect, "sample %s missing expect", sample.Name)
		assert.NotEmpty(t, sample.Flow)

		var flow map[string]any
		require.NoError(t, json.Unmarshal(sample.Flow, &flow))
		for _, nodeType := range collectNodeTypes(flow) {
			assert.False(t, forbidden[nodeType],
				"sample %s uses side-effect node %q (M2 samples must be pure computation)",
				sample.Name, nodeType)
		}
	}
}

func collectNodeTypes(flow map[string]any) []string {
	var types []string
	nodes, _ := flow["nodes"].([]any)
	for _, rawNode := range nodes {
		node, ok := rawNode.(map[string]any)
		if !ok {
			continue
		}
		if nodeType, ok := node["type"].(string); ok {
			types = append(types, nodeType)
		}
		for _, key := range []string{"childFlow", "flow"} {
			if child, ok := node[key].(map[string]any); ok {
				types = append(types, collectNodeTypes(child)...)
			}
		}
		branches, _ := node["branches"].([]any)
		for _, rawBranch := range branches {
			branch, ok := rawBranch.(map[string]any)
			if !ok {
				continue
			}
			if child, ok := branch["flow"].(map[string]any); ok {
				types = append(types, collectNodeTypes(child)...)
			}
		}
	}
	return types
}

// TestSamplesRunGo always exercises the Go side; the Python comparison is
// covered by TestSamplesAgainstPython (skipped without a local plaita repo).
func TestSamplesRunGo(t *testing.T) {
	samples, err := LoadSamples(filepath.Join("testdata", "samples"))
	require.NoError(t, err)
	for _, sample := range samples {
		t.Run(sample.Name, func(t *testing.T) {
			outcome := RunGo(t.Context(), sample.Flow, sample.Input)
			require.True(t, outcome.OK, "go run failed: %s", outcome.Error)
			assert.NotEmpty(t, outcome.Trace)
		})
	}
}

func TestSamplesAgainstPython(t *testing.T) {
	repo := os.Getenv("PLAITA_PY_REPO")
	if repo == "" {
		repo = DefaultPythonRepo
	}
	if !pythonRepoAvailable(t, repo) {
		t.Skipf("python plaita repo not available at %s; skipping cross-engine comparison", repo)
	}

	samples, err := LoadSamples(filepath.Join("testdata", "samples"))
	require.NoError(t, err)
	runner := NewPythonRunner("python3", repo)
	runner.Timeout = 90 * time.Second

	for _, sample := range samples {
		t.Run(sample.Name, func(t *testing.T) {
			result := RunSample(t.Context(), sample, runner)
			if !result.Pass {
				if result.Python != nil && !result.Python.OK {
					t.Logf("python error: %s", result.Python.Error)
				}
				if result.Go != nil && !result.Go.OK {
					t.Logf("go error: %s", result.Go.Error)
				}
				for _, diff := range result.Diffs {
					t.Errorf("diff %s [%s]: %s (python=%v go=%v)",
						diff.Path, diff.Kind, diff.Detail, diff.Reference, diff.Actual)
				}
			}
			assert.True(t, result.Pass, "sample %s must pass", sample.Name)
		})
	}
}

func pythonRepoAvailable(t *testing.T, repo string) bool {
	t.Helper()
	if _, err := os.Stat(filepath.Join(repo, "plaita")); err != nil {
		return false
	}
	if _, err := exec.LookPath("python3"); err != nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "python3", "-c", "import plaita; import plaita_nodes")
	cmd.Dir = repo
	cmd.Env = pythonEnv(repo)
	return cmd.Run() == nil
}

func TestReportMarkdown(t *testing.T) {
	results := []SampleResult{{
		Name: "demo", Description: "demo sample", Pass: true,
		Python: &Outcome{Engine: "python", OK: true, Result: 1},
		Go:     &Outcome{Engine: "go", OK: true, Result: 1},
	}}
	report := NewReport(results, "pkg/shadow/testdata/samples/", "/tmp/plaita", "python3", "go1.24")
	report.PythonVersion = "Python 3.13"
	var buf bytes.Buffer
	report.WriteMarkdown(&buf)
	text := buf.String()
	assert.Contains(t, text, "1 / 1 样本一致")
	assert.Contains(t, text, "demo")
	assert.Contains(t, text, "声明分歧")
	assert.Contains(t, text, "pkg/shadow/testdata/samples/")
}
