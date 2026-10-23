package shadow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Sample drivers. "" / "fresh" runs the flow once in each engine (M2
// posture). The #317 execution-semantics cases need the resume entry points:
//
//   - event-resume: run to the suspending event node, resolve it with the
//     resume carrier from DriverOptions, run to completion (plaita
//     distributed resume + go Workflow.ResumeSuspension);
//   - continue-after-error: let a node fail terminally, then redeliver the
//     continue carrier (plaita run_distributed continue from the checkpoint;
//     go worker posture = full re-entry from the flow start — the declared
//     replay divergence).
const (
	DriverFresh              = "fresh"
	DriverEventResume        = "event-resume"
	DriverContinueAfterError = "continue-after-error"
)

// Sample dimensions (#317): what the case pins. "" means "pure" — the M2
// pure-computation posture (no side-effect nodes at all).
const (
	DimensionPure                = "pure"
	DimensionErrorClassification = "error-classification"
	DimensionResultShape         = "result-shape"
	DimensionSuspendResume       = "suspend-resume"
)

// Sample is one synthetic reconciliation case: a flow plus its input.
// Pure-computation samples (dimension "" / "pure") must not contain
// side-effect nodes (http, redis, agentrun, git_publish, notify/webhook/email,
// ...). #317 cases (error-classification / result-shape / suspend-resume) may
// use code and hermetic http nodes; the loader guard in shadow_test.go
// enforces the boundary per dimension.
//
// Expect is the expected final output, checked against both engines. It is a
// raw message so an explicit JSON null (sample 09: empty loop) is
// distinguishable from a missing expectation. Samples that terminate in an
// error instead set expectError (and keep expect null).
type Sample struct {
	Name                string          `json:"name"`
	Description         string          `json:"description"`
	Input               map[string]any  `json:"input"`
	Expect              json.RawMessage `json:"expect"`
	Flow                json.RawMessage `json:"flow"`
	DeclaredDivergences []string        `json:"declaredDivergences,omitempty"`
	Dimension           string          `json:"dimension,omitempty"`
	Driver              string          `json:"driver,omitempty"`
	DriverOptions       map[string]any  `json:"driverOptions,omitempty"`
	ExpectError         json.RawMessage `json:"expectError,omitempty"`
	Path                string          `json:"-"`
}

type rawSample struct {
	Name                string          `json:"name"`
	Description         string          `json:"description"`
	Input               map[string]any  `json:"input"`
	Expect              json.RawMessage `json:"expect"`
	Flow                json.RawMessage `json:"flow"`
	DeclaredDivergences []string        `json:"declaredDivergences"`
	Dimension           string          `json:"dimension"`
	Driver              string          `json:"driver"`
	DriverOptions       map[string]any  `json:"driverOptions"`
	ExpectError         json.RawMessage `json:"expectError"`
}

// LoadSamples reads every *.json under dir, sorted by file name.
func LoadSamples(dir string) ([]Sample, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read sample dir %s: %w", dir, err)
	}
	var files []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		files = append(files, entry.Name())
	}
	sort.Strings(files)

	samples := make([]Sample, 0, len(files))
	for _, name := range files {
		path := filepath.Join(dir, name)
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read sample %s: %w", name, err)
		}
		sample, err := parseSample(raw)
		if err != nil {
			return nil, fmt.Errorf("parse sample %s: %w", name, err)
		}
		sample.Name = firstNonEmpty(sample.Name, strings.TrimSuffix(name, filepath.Ext(name)))
		sample.Path = path
		samples = append(samples, *sample)
	}
	return samples, nil
}

func parseSample(raw []byte) (*Sample, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var rs rawSample
	if err := dec.Decode(&rs); err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(rs.Flow)) == 0 || bytes.Equal(bytes.TrimSpace(rs.Flow), []byte("null")) {
		return nil, fmt.Errorf("missing flow")
	}
	if len(bytes.TrimSpace(rs.Expect)) == 0 {
		return nil, fmt.Errorf("missing expect")
	}
	if _, err := decodeRawValue(rs.Expect); err != nil {
		return nil, fmt.Errorf("invalid expect: %w", err)
	}
	if rs.Input == nil {
		rs.Input = map[string]any{}
	}
	switch rs.Driver {
	case "", DriverFresh, DriverEventResume, DriverContinueAfterError:
	default:
		return nil, fmt.Errorf("unknown driver %q", rs.Driver)
	}
	switch rs.Dimension {
	case "", DimensionPure, DimensionErrorClassification, DimensionResultShape, DimensionSuspendResume:
	default:
		return nil, fmt.Errorf("unknown dimension %q", rs.Dimension)
	}
	return &Sample{
		Name:                rs.Name,
		Description:         rs.Description,
		Input:               rs.Input,
		Expect:              rs.Expect,
		Flow:                rs.Flow,
		DeclaredDivergences: rs.DeclaredDivergences,
		Dimension:           rs.Dimension,
		Driver:              rs.Driver,
		DriverOptions:       rs.DriverOptions,
		ExpectError:         rs.ExpectError,
	}, nil
}

// ExpectErrorSpec is the terminal-error expectation of an error-mode sample.
// Match, when non-empty, must appear in BOTH engines' terminal error strings
// (the retry-attribution signal is the per-node attempt count carried on the
// trace; the error text itself differs per engine by construction —
// NodeExecutionError vs HandlerError wrappers).
type ExpectErrorSpec struct {
	Match string `json:"match"`
}

// ExpectedError decodes the sample's expectError block (empty = not an
// error-mode sample).
func (s Sample) ExpectedError() (ExpectErrorSpec, error) {
	var spec ExpectErrorSpec
	raw := bytes.TrimSpace(s.ExpectError)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return spec, nil
	}
	if err := json.Unmarshal(raw, &spec); err != nil {
		return spec, err
	}
	return spec, nil
}

// IsErrorMode reports whether the sample expects a terminal error in both
// engines.
func (s Sample) IsErrorMode() bool {
	raw := bytes.TrimSpace(s.ExpectError)
	return len(raw) > 0 && !bytes.Equal(raw, []byte("null"))
}

// HasDeclaration reports whether the sample declares the given divergence.
func (s Sample) HasDeclaration(name string) bool {
	for _, d := range s.DeclaredDivergences {
		if d == name {
			return true
		}
	}
	return false
}

// Declared divergence vocabulary (#317). Entries live in the sample JSON's
// declaredDivergences list; the comparator honors each with a specific,
// narrow normalization — a declaration never disables result expectations.
const (
	// DeclaredReplayFromStart: the go worker re-enters a running execution
	// from the flow start on a redelivered continue carrier (AGENTS.md
	// §worker 二期, Stage-1 convergence pending), so already-completed
	// prefix nodes reappear in the go trace. The comparator projects both
	// traces to the first occurrence per node before comparing.
	DeclaredReplayFromStart = "replay-from-start:go"
	// DeclaredLoadRefusal: the go loader fail-closed rejects the construct
	// (e.g. end resultType "nop") that the Python engine accepts. The case
	// then pins the Python behavior via expect AND asserts the go refusal.
	DeclaredLoadRefusal = "load-refusal:go"
)

// Expected decodes the sample's expected final output.
func (s Sample) Expected() (any, error) {
	return decodeRawValue(s.Expect)
}

func decodeRawValue(raw json.RawMessage) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var value any
	if err := dec.Decode(&value); err != nil {
		return nil, err
	}
	return value, nil
}

// LoadFlowSample builds a single ad-hoc sample from a deployment-flow file and
// an optional input JSON file (input must be an object; omitted = {}).
func LoadFlowSample(flowPath, inputPath string) (Sample, error) {
	flowRaw, err := os.ReadFile(flowPath)
	if err != nil {
		return Sample{}, fmt.Errorf("read flow %s: %w", flowPath, err)
	}
	var flow map[string]any
	if err := json.Unmarshal(flowRaw, &flow); err != nil {
		return Sample{}, fmt.Errorf("parse flow %s: %w", flowPath, err)
	}
	input := map[string]any{}
	if inputPath != "" {
		inputRaw, err := os.ReadFile(inputPath)
		if err != nil {
			return Sample{}, fmt.Errorf("read input %s: %w", inputPath, err)
		}
		dec := json.NewDecoder(bytes.NewReader(inputRaw))
		dec.UseNumber()
		if err := dec.Decode(&input); err != nil {
			return Sample{}, fmt.Errorf("parse input %s (must be a JSON object): %w", inputPath, err)
		}
		if input == nil {
			input = map[string]any{}
		}
	}
	return Sample{
		Name:        strings.TrimSuffix(filepath.Base(flowPath), filepath.Ext(flowPath)),
		Description: flowPath,
		Input:       input,
		Flow:        flowRaw,
		Path:        flowPath,
	}, nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
