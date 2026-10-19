package shadow

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// SampleResult is the reconciliation outcome for one sample.
type SampleResult struct {
	Name                string       `json:"name"`
	Description         string       `json:"description"`
	Pass                bool         `json:"pass"`
	DeclaredDivergences []string     `json:"declared_divergences,omitempty"`
	Diffs               []Difference `json:"diffs,omitempty"`
	ExpectDiffs         []Difference `json:"expect_diffs,omitempty"`
	Python              *Outcome     `json:"python"`
	Go                  *Outcome     `json:"go"`
	PythonDurationMS    int64        `json:"python_duration_ms"`
	GoDurationMS        int64        `json:"go_duration_ms"`
}

// RunSample executes one sample in both engines and compares the outcomes.
func RunSample(ctx context.Context, sample Sample, python *PythonRunner) SampleResult {
	result := SampleResult{
		Name:                sample.Name,
		Description:         sample.Description,
		DeclaredDivergences: sample.DeclaredDivergences,
	}

	start := time.Now()
	result.Python = python.Run(ctx, sample.Flow, sample.Input)
	result.PythonDurationMS = time.Since(start).Milliseconds()

	start = time.Now()
	result.Go = RunGo(ctx, sample.Flow, sample.Input)
	result.GoDurationMS = time.Since(start).Milliseconds()

	result.Diffs = CompareOutcomes(result.Python, result.Go)
	result.ExpectDiffs = compareExpectation(sample, result.Python, result.Go)
	result.Pass = result.Python.OK && result.Go.OK && len(result.Diffs) == 0 && len(result.ExpectDiffs) == 0
	return result
}

// compareExpectation checks both engines against the sample's expected final
// output. This catches the case where Python and Go agree with each other but
// both diverge from the documented expectation.
func compareExpectation(sample Sample, python, goOutcome *Outcome) []Difference {
	if len(strings.TrimSpace(string(sample.Expect))) == 0 {
		return nil // ad-hoc flows (-flow) may omit the expectation check
	}
	expected, err := sample.Expected()
	if err != nil {
		return []Difference{{
			Path:   "$expect",
			Kind:   DiffExecution,
			Detail: "cannot decode sample expectation: " + err.Error(),
		}}
	}
	var diffs []Difference
	if python.OK {
		diffs = append(diffs, prefixDiffs("$expect.python", CompareValues(expected, python.Result))...)
	}
	if goOutcome.OK {
		diffs = append(diffs, prefixDiffs("$expect.go", CompareValues(expected, goOutcome.Result))...)
	}
	return diffs
}

func prefixDiffs(prefix string, diffs []Difference) []Difference {
	for i := range diffs {
		diffs[i].Path = prefix + strings.TrimPrefix(diffs[i].Path, "$")
	}
	return diffs
}

// CompareOutcomes compares a reference (Python) outcome with the Go outcome:
// final result first, then the per-flow node trace.
func CompareOutcomes(reference, actual *Outcome) []Difference {
	diffs := make([]Difference, 0)
	switch {
	case reference.OK && actual.OK:
		diffs = append(diffs, CompareValues(reference.Result, actual.Result)...)
		diffs = append(diffs, CompareTraces(reference.Trace, actual.Trace)...)
	case !reference.OK && !actual.OK:
		diffs = append(diffs, Difference{
			Path:   "$execution",
			Kind:   DiffExecution,
			Detail: fmt.Sprintf("both engines failed (python: %s; go: %s)", reference.Error, actual.Error),
		})
	case !reference.OK:
		diffs = append(diffs, Difference{
			Path:   "$execution",
			Kind:   DiffExecution,
			Detail: "python engine failed: " + reference.Error,
		})
	default:
		diffs = append(diffs, Difference{
			Path:   "$execution",
			Kind:   DiffExecution,
			Detail: "go engine failed: " + actual.Error,
		})
	}
	return diffs
}
