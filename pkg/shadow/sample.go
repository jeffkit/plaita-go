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

// Sample is one synthetic reconciliation case: a pure-computation flow plus
// its input. Samples must not contain side-effect nodes (http, redis,
// agentrun, git_publish, notify/webhook/email, ...).
//
// Expect is the expected final output, checked against both engines. It is a
// raw message so an explicit JSON null (sample 09: empty loop) is
// distinguishable from a missing expectation.
type Sample struct {
	Name                string          `json:"name"`
	Description         string          `json:"description"`
	Input               map[string]any  `json:"input"`
	Expect              json.RawMessage `json:"expect"`
	Flow                json.RawMessage `json:"flow"`
	DeclaredDivergences []string        `json:"declaredDivergences,omitempty"`
	Path                string          `json:"-"`
}

type rawSample struct {
	Name                string          `json:"name"`
	Description         string          `json:"description"`
	Input               map[string]any  `json:"input"`
	Expect              json.RawMessage `json:"expect"`
	Flow                json.RawMessage `json:"flow"`
	DeclaredDivergences []string        `json:"declaredDivergences"`
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
	return &Sample{
		Name:                rs.Name,
		Description:         rs.Description,
		Input:               rs.Input,
		Expect:              rs.Expect,
		Flow:                rs.Flow,
		DeclaredDivergences: rs.DeclaredDivergences,
	}, nil
}

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
