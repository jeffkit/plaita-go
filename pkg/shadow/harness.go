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
	Dimension           string       `json:"dimension,omitempty"`
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
// The driver decides how each engine is exercised (fresh / event-resume /
// continue-after-error); declared divergences tune the comparison (never the
// expectations).
func RunSample(ctx context.Context, sample Sample, python *PythonRunner) SampleResult {
	dimension := sample.Dimension
	if dimension == "" {
		dimension = DimensionPure
	}
	result := SampleResult{
		Name:                sample.Name,
		Description:         sample.Description,
		Dimension:           dimension,
		DeclaredDivergences: sample.DeclaredDivergences,
	}

	start := time.Now()
	result.Python = python.RunDrive(ctx, sample.Flow, sample.Input, sample.Driver, sample.DriverOptions)
	result.PythonDurationMS = time.Since(start).Milliseconds()

	start = time.Now()
	result.Go = RunGoDrive(ctx, sample.Flow, sample.Input, sample.Driver, sample.DriverOptions)
	result.GoDurationMS = time.Since(start).Milliseconds()

	spec, err := sample.ExpectedError()
	if err != nil {
		result.Diffs = []Difference{{
			Path: "$expectError", Kind: DiffExecution,
			Detail: "cannot decode expectError: " + err.Error(),
		}}
		return result
	}
	errorMode := sample.IsErrorMode()
	if sample.Driver == DriverEventResume {
		// The event-resume posture compares the resumed node state through
		// the deterministic common core (see normalizeEventResumeOutcome);
		// attempt counts are exempt — a resume is not a retry, and the
		// retry attribution signal lives in the error-classification cases.
		normalizeEventResumeOutcome(result.Python)
		normalizeEventResumeOutcome(result.Go)
	}

	switch {
	case sample.HasDeclaration(DeclaredLoadRefusal):
		// The go loader fail-closed rejects the construct; the case pins the
		// Python behavior via expect AND asserts the refusal itself.
		result.ExpectDiffs = pythonExpectDiffs(sample, result.Python)
		switch {
		case result.Python == nil || !result.Python.OK:
			result.Diffs = []Difference{{
				Path: "$execution", Kind: DiffExecution,
				Detail: "python engine failed: " + pythonError(result.Python),
			}}
		case result.Go != nil && result.Go.OK:
			result.Diffs = []Difference{{
				Path: "$execution", Kind: DiffExecution,
				Detail: "declared load-refusal violated: the go engine loaded and ran the flow",
			}}
		}
		result.Pass = result.Python != nil && result.Python.OK &&
			result.Go != nil && !result.Go.OK && len(result.ExpectDiffs) == 0
		return result

	case errorMode:
		// Both engines must fail terminally; the attempt counts and node
		// sequence come from the trace comparison, the error text from the
		// expectError match. Result values are not compared (both nil).
		// A declared replay divergence (R2: the go continue carrier re-enters
		// from the flow start) applies here too — the terminal failure is
		// exactly the R2 posture, so the projection must not be silently
		// dropped in error mode.
		result.Diffs = compareOutcomes(result.Python, result.Go, compareOptions{
			ErrorMode:        true,
			ReplayProjection: sample.HasDeclaration(DeclaredReplayFromStart),
		})
		if spec.Match != "" {
			for engine, outcome := range map[string]*Outcome{"python": result.Python, "go": result.Go} {
				if outcome == nil || outcome.OK || !strings.Contains(outcome.Error, spec.Match) {
					result.ExpectDiffs = append(result.ExpectDiffs, Difference{
						Path: "$expectError." + engine, Kind: DiffExecution,
						Detail: fmt.Sprintf("terminal error must contain %q (got: %s)",
							spec.Match, errorText(outcome)),
					})
				}
			}
		}
		result.Pass = result.Python != nil && !result.Python.OK &&
			result.Go != nil && !result.Go.OK &&
			len(result.Diffs) == 0 && len(result.ExpectDiffs) == 0
		return result

	default:
		result.Diffs = compareOutcomes(result.Python, result.Go, compareOptions{
			ReplayProjection: sample.HasDeclaration(DeclaredReplayFromStart),
			IgnoreAttempts:   sample.Driver == DriverEventResume,
		})
		result.ExpectDiffs = compareExpectation(sample, result.Python, result.Go)
		result.Pass = result.Python.OK && result.Go.OK &&
			len(result.Diffs) == 0 && len(result.ExpectDiffs) == 0
		return result
	}
}

func pythonError(outcome *Outcome) string {
	if outcome == nil {
		return "<no outcome>"
	}
	return outcome.Error
}

func errorText(outcome *Outcome) string {
	if outcome == nil {
		return "<no outcome>"
	}
	if outcome.Error == "" {
		return "<engine did not fail>"
	}
	return outcome.Error
}

func pythonExpectDiffs(sample Sample, python *Outcome) []Difference {
	if python == nil || !python.OK {
		return nil
	}
	expected, err := sample.Expected()
	if err != nil {
		return []Difference{{
			Path: "$expect", Kind: DiffExecution,
			Detail: "cannot decode sample expectation: " + err.Error(),
		}}
	}
	return prefixDiffs("$expect.python", CompareValues(expected, python.Result))
}

// compareOptions tunes the structural comparison for a sample's declared
// posture.
type compareOptions struct {
	// ErrorMode: both engines are expected to fail terminally; the trace is
	// still compared (node sequence, attempts, error presence) and no
	// synthetic "$execution" difference is emitted for the shared failure.
	ErrorMode bool
	// ReplayProjection: project both traces to the first occurrence per node
	// before comparing (declared divergence replay-from-start:go).
	ReplayProjection bool
	// IgnoreAttempts skips the attempt-count check (event-resume posture: a
	// resume is not a retry).
	IgnoreAttempts bool
}

func compareOutcomes(reference, actual *Outcome, opts compareOptions) []Difference {
	refTrace, actTrace := reference.Trace, actual.Trace
	traceOpts := TraceCompareOptions{
		IgnoreAttempts: opts.IgnoreAttempts || opts.ReplayProjection,
	}
	if opts.ReplayProjection {
		refTrace = projectFirstOccurrences(refTrace)
		actTrace = projectFirstOccurrences(actTrace)
	}
	diffs := make([]Difference, 0)
	switch {
	case reference.OK && actual.OK:
		diffs = append(diffs, CompareValues(reference.Result, actual.Result)...)
		diffs = append(diffs, CompareTracesOpts(refTrace, actTrace, traceOpts)...)
	case !reference.OK && !actual.OK:
		if !opts.ErrorMode {
			diffs = append(diffs, Difference{
				Path:   "$execution",
				Kind:   DiffExecution,
				Detail: fmt.Sprintf("both engines failed (python: %s; go: %s)", reference.Error, actual.Error),
			})
		}
		diffs = append(diffs, CompareTracesOpts(refTrace, actTrace, traceOpts)...)
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

// projectFirstOccurrences keeps, per flow, only the first event of each node
// id (in first-appearance order). Used for the declared replay-from-start
// divergence: the go worker's full re-entry re-executes the completed prefix,
// which must not fail the comparison but stays visible in the raw traces.
func projectFirstOccurrences(events []TraceEvent) []TraceEvent {
	seen := map[string]map[string]bool{}
	out := make([]TraceEvent, 0, len(events))
	for _, event := range events {
		flowSeen := seen[event.FlowID]
		if flowSeen == nil {
			flowSeen = map[string]bool{}
			seen[event.FlowID] = flowSeen
		}
		if flowSeen[event.NodeID] {
			continue
		}
		flowSeen[event.NodeID] = true
		out = append(out, event)
	}
	return out
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
