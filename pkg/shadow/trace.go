package shadow

import (
	"context"
	"fmt"
	"sync"

	"github.com/jeffkit/plaita-go/pkg/ir"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// TraceEvent is one node-end observation. Both engines emit the same shape:
// flow_id identifies the (embedded) flow, node_id/type the executed node and
// result the node's output value (nil for start nodes and failed nodes).
type TraceEvent struct {
	FlowID string `json:"flow_id"`
	NodeID string `json:"node_id"`
	Type   string `json:"type"`
	Result any    `json:"result"`
	Error  string `json:"error,omitempty"`
}

// Outcome is the result of one engine execution.
type Outcome struct {
	Engine    string       `json:"engine"`
	OK        bool         `json:"ok"`
	Result    any          `json:"result"`
	Trace     []TraceEvent `json:"trace"`
	Error     string       `json:"error,omitempty"`
	ErrorType string       `json:"error_type,omitempty"`
}

type traceSinkKey struct{}

type traceSink struct {
	mu     sync.Mutex
	events []TraceEvent
}

func (s *traceSink) append(e TraceEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
}

func (s *traceSink) snapshot() []TraceEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]TraceEvent, len(s.events))
	copy(out, s.events)
	return out
}

var registerTraceListener sync.Once

// registerGoTraceListener installs the process-wide node-end listener once.
// Dispatch is per-execution via a context value, so concurrent runs (and runs
// that do not use this package at all) are unaffected.
func registerGoTraceListener() {
	registerTraceListener.Do(func() {
		workflow.OnNodeEnd(func(ctx context.Context, flow *workflow.Workflow,
			node workflow.Node, result any, err error, _ map[string]interface{}) {
			sink, _ := ctx.Value(traceSinkKey{}).(*traceSink)
			if sink == nil {
				return
			}
			event := TraceEvent{
				FlowID: string(flow.ID),
				NodeID: string(node.GetID()),
				Type:   string(node.GetType()),
				Result: result,
			}
			if err != nil {
				event.Error = err.Error()
			}
			sink.append(event)
		})
	})
}

// RunGo loads the deployment-form flow through pkg/ir and executes it with the
// given input, capturing the node trace.
func RunGo(ctx context.Context, flowJSON []byte, input map[string]any) (out *Outcome) {
	registerGoTraceListener()

	out = &Outcome{Engine: "go"}
	defer func() {
		if r := recover(); r != nil {
			out.OK = false
			out.Result = nil
			out.Error = fmt.Sprintf("panic: %v", r)
			out.ErrorType = "panic"
		}
	}()

	wf, err := ir.Load(flowJSON)
	if err != nil {
		return failedOutcome(out, err)
	}

	sink := new(traceSink)
	runCtx := context.WithValue(ctx, traceSinkKey{}, sink)
	result, err := wf.Execute(runCtx, input)
	out.Trace = normalizeTrace(sink.snapshot())
	if err != nil {
		return failedOutcome(out, err)
	}
	normalized, ok := canonical(result)
	if !ok {
		out.OK = false
		out.Error = fmt.Sprintf("go output is not JSON-serializable: %v", result)
		out.ErrorType = "serialization"
		return out
	}
	out.OK = true
	out.Result = normalized
	return out
}

func failedOutcome(out *Outcome, err error) *Outcome {
	out.OK = false
	out.Result = nil
	out.Error = err.Error()
	out.ErrorType = fmt.Sprintf("%T", err)
	return out
}

// normalizeTrace canonicalizes node results so they compare in the same JSON
// value domain as Python's output.
func normalizeTrace(events []TraceEvent) []TraceEvent {
	for i := range events {
		if normalized, ok := canonical(events[i].Result); ok {
			events[i].Result = normalized
		}
	}
	return events
}

// CompareTraces compares node traces grouped by flow_id, preserving the event
// order inside each flow. Parallel branch interleaving is therefore ignored,
// while node order, node ids/types, node results and error presence are still
// checked per flow.
func CompareTraces(reference, actual []TraceEvent) []Difference {
	refOrder, refGroups := groupTrace(reference)
	actOrder, actGroups := groupTrace(actual)

	var diffs []Difference
	for _, flowID := range refOrder {
		actEvents, found := actGroups[flowID]
		if !found {
			diffs = append(diffs, Difference{
				Path: fmt.Sprintf("trace[%q]", flowID), Kind: DiffTraceFlowMiss,
				Reference: len(refGroups[flowID]),
				Detail:    "flow present in python trace but missing in go trace",
			})
			continue
		}
		compareTraceGroup(flowID, refGroups[flowID], actEvents, &diffs)
	}
	for _, flowID := range actOrder {
		if _, found := refGroups[flowID]; !found {
			diffs = append(diffs, Difference{
				Path: fmt.Sprintf("trace[%q]", flowID), Kind: DiffTraceFlowExtra,
				Actual: len(actGroups[flowID]),
				Detail: "flow present in go trace but missing in python trace",
			})
		}
	}
	return diffs
}

func compareTraceGroup(flowID string, ref, act []TraceEvent, diffs *[]Difference) {
	if len(ref) != len(act) {
		*diffs = append(*diffs, Difference{
			Path: fmt.Sprintf("trace[%q]", flowID), Kind: DiffTraceLength,
			Reference: len(ref), Actual: len(act),
			Detail: fmt.Sprintf("node event count %d vs %d", len(ref), len(act)),
		})
	}
	n := len(ref)
	if len(act) < n {
		n = len(act)
	}
	for i := 0; i < n; i++ {
		path := fmt.Sprintf("trace[%q][%d]", flowID, i)
		if ref[i].NodeID != act[i].NodeID {
			*diffs = append(*diffs, Difference{
				Path: path + ".node_id", Kind: DiffNodeID,
				Reference: ref[i].NodeID, Actual: act[i].NodeID,
			})
		}
		if ref[i].Type != act[i].Type {
			*diffs = append(*diffs, Difference{
				Path: path + ".type", Kind: DiffNodeType,
				Reference: ref[i].Type, Actual: act[i].Type,
			})
		}
		if (ref[i].Error != "") != (act[i].Error != "") {
			*diffs = append(*diffs, Difference{
				Path: path + ".error", Kind: DiffNodeError,
				Reference: ref[i].Error, Actual: act[i].Error,
				Detail: "error presence differs",
			})
		}
		compareValue(path+".result", ref[i].Result, act[i].Result, diffs)
	}
}

// groupTrace groups events by flow_id while preserving first-appearance order
// of flows and per-flow event order.
func groupTrace(events []TraceEvent) ([]string, map[string][]TraceEvent) {
	order := make([]string, 0, len(events))
	groups := make(map[string][]TraceEvent)
	for _, event := range events {
		if _, found := groups[event.FlowID]; !found {
			order = append(order, event.FlowID)
		}
		groups[event.FlowID] = append(groups[event.FlowID], event)
	}
	return order, groups
}
