package shadow

import (
	"context"
	"fmt"

	"github.com/jeffkit/plaita-go/pkg/ir"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// memorySubscriptionRegistry is the harness's in-memory wake-up subscription
// store (the SubscriptionRegistry host wiring the engine needs before it may
// park a flow). It observes registrations so the driver can assert a
// subscription was set up for the suspend.
type memorySubscriptionRegistry struct {
	registered   []*workflow.SubscriptionRequest
	unregistered []string
}

func (r *memorySubscriptionRegistry) Register(_ context.Context, req *workflow.SubscriptionRequest) error {
	if req.SubscriptionID == "" {
		req.SubscriptionID = fmt.Sprintf("sub-%s-%d", req.NodeID, len(r.registered)+1)
	}
	r.registered = append(r.registered, req)
	return nil
}

func (r *memorySubscriptionRegistry) Unregister(_ context.Context, subscriptionID, _ string) error {
	r.unregistered = append(r.unregistered, subscriptionID)
	return nil
}

// driverOptionsBinder extracts typed driver options.
type driverOptionsBinder map[string]any

func (o driverOptionsBinder) eventType() string {
	s, _ := o["eventType"].(string)
	return s
}

func (o driverOptionsBinder) eventData() map[string]any {
	m, _ := o["eventData"].(map[string]any)
	return m
}

// RunGoDrive exercises the go engine according to the sample's driver. It is
// the go-side counterpart of the pybridge drivers (fresh execute, distributed
// event resume, worker-posture continue re-entry).
func RunGoDrive(ctx context.Context, flowJSON []byte, input map[string]any,
	driver string, driverOptions map[string]any,
) (out *Outcome) {
	registerGoTraceListener()
	switch driver {
	case "", DriverFresh:
		return RunGo(ctx, flowJSON, input)
	case DriverEventResume:
		return runGoEventResume(ctx, flowJSON, input, driverOptions)
	case DriverContinueAfterError:
		return runGoContinueAfterError(ctx, flowJSON, input)
	default:
		out = &Outcome{Engine: "go"}
		return failedOutcome(out, fmt.Errorf("unknown driver %q", driver))
	}
}

// runGoEventResume runs the flow under suspend wiring (memory checkpoint
// store + in-memory subscription registry), resolves the suspension with the
// event carrier built from the driver options plus the subscription the
// engine registered, and runs to completion — the go analogue of the plaita
// distributed resume (_handle_resume + EventNode.resume).
func runGoEventResume(ctx context.Context, flowJSON []byte, input map[string]any, driverOptions map[string]any) (out *Outcome) {
	out = &Outcome{Engine: "go"}
	defer func() {
		if r := recover(); r != nil {
			out = &Outcome{Engine: "go", OK: false}
			out.Error = fmt.Sprintf("panic: %v", r)
			out.ErrorType = "panic"
		}
	}()

	wf, err := ir.Load(flowJSON)
	if err != nil {
		return failedOutcome(out, err)
	}

	sink := new(traceSink)
	registry := &memorySubscriptionRegistry{}
	opts := &workflow.SuspendOptions{
		ExecutionID:   "shadow-event-resume",
		FlowID:        string(wf.ID),
		Store:         workflow.NewMemoryCheckpointStore(),
		Subscriptions: registry,
	}
	sinkCtx := context.WithValue(ctx, traceSinkKey{}, sink)
	runCtx := workflow.WithSuspendOptions(sinkCtx, opts)

	_, err = wf.Execute(runCtx, input)
	susp := workflow.AsSuspend(err)
	if susp == nil {
		if err != nil {
			out.Trace = normalizeTrace(sink.snapshot())
			return failedOutcome(out, fmt.Errorf("flow did not suspend: %v", err))
		}
		out.Trace = normalizeTrace(sink.snapshot())
		out.Error = "flow completed without suspending; event-resume drive needs a suspending node"
		out.ErrorType = "driver"
		return out
	}

	options := driverOptionsBinder(driverOptions)
	carrier := map[string]any{
		"subscription_id": susp.SubscriptionID,
		"event_type":      options.eventType(),
		"event_data":      options.eventData(),
	}
	result, err := wf.ResumeSuspension(runCtx, susp, workflow.ResumeEvent, carrier)
	out.Trace = normalizeTrace(sink.snapshot())
	if err != nil {
		return failedOutcome(out, err)
	}
	return succeededOutcome(out, result)
}

// runGoContinueAfterError mirrors the go worker's continue-carrier posture
// (AGENTS.md §worker 二期): with no mid-flow entry yet, a redelivered
// continue carrier re-enters the engine from the flow start, so the completed
// prefix nodes replay. The harness therefore runs the flow twice — first to
// the terminal failure, then the full re-entry — accumulating the trace.
func runGoContinueAfterError(ctx context.Context, flowJSON []byte, input map[string]any) (out *Outcome) {
	out = &Outcome{Engine: "go"}
	defer func() {
		if r := recover(); r != nil {
			out = &Outcome{Engine: "go", OK: false}
			out.Error = fmt.Sprintf("panic: %v", r)
			out.ErrorType = "panic"
		}
	}()

	sink := new(traceSink)
	sinkCtx := context.WithValue(ctx, traceSinkKey{}, sink)

	first, err := ir.Load(flowJSON)
	if err != nil {
		return failedOutcome(out, err)
	}
	if _, err := first.Execute(sinkCtx, input); err != nil {
		if workflow.AsSuspend(err) != nil {
			out.Trace = normalizeTrace(sink.snapshot())
			return failedOutcome(out, fmt.Errorf("flow suspended; continue-after-error drive needs a failing flow: %v", err))
		}
		// Terminal failure is the resume setup; fall through to the re-entry.
	} else {
		out.Trace = normalizeTrace(sink.snapshot())
		out.Error = "flow completed on the first run; continue-after-error drive needs a failing flow"
		out.ErrorType = "driver"
		return out
	}

	// The worker re-loads the definition and re-enters from the start (a
	// Workflow instance cannot re-run concurrently, and the worker's re-entry
	// is a fresh engine instance by construction).
	second, err := ir.Load(flowJSON)
	if err != nil {
		return failedOutcome(out, err)
	}
	result, err := second.Execute(sinkCtx, input)
	out.Trace = normalizeTrace(sink.snapshot())
	if err != nil {
		return failedOutcome(out, err)
	}
	return succeededOutcome(out, result)
}

// eventResumeScrubKeys are the fields of an event node's state that cannot
// compare across engines by construction: runtime-assigned ids (the py
// registry mints UUIDs, a fake go registry mints its own) and millisecond
// timestamps, plus the host-shape extras where the two engines structurally
// differ on the suspended/resumed posture (plaita-go AGENTS.md「恢复状态形状
// （Python 实测钉死）」). The compared surface for the event-resume driver is
// the deterministic common core: status flip + event_type + event_data —
// the same口径 the plaita distributed resume pins.
var eventResumeScrubKeys = [6]string{
	"event_id", "subscription_id", "subscription_tenant",
	"event_filter", "is_async", "subscription_timeout",
}

// normalizeEventResumeOutcome projects one outcome into the event-resume
// comparison domain: event-node results are scrubbed of the non-deterministic
// keys above (recursively — the resume carrier is echoed whole into
// event_data on both engines, subscription id included), and per flow only
// the last event-node occurrence is kept (both engines fire node-end at
// suspend AND at resume; the suspend-time pending state is posture, not
// divergence — the resumed state is the compared one, matching the plaita
// distributed-resume semantics where only the resumed node itself is
// processed).
func normalizeEventResumeOutcome(outcome *Outcome) {
	if outcome == nil {
		return
	}
	outcome.Result = scrubEventCarrierKeys(outcome.Result)
	perFlowLast := map[string]bool{}
	kept := outcome.Trace[:0:0]
	for i := len(outcome.Trace) - 1; i >= 0; i-- {
		event := outcome.Trace[i]
		if event.Type == "event" {
			event.Result = scrubEventCarrierKeys(event.Result)
			if perFlowLast[event.FlowID] {
				continue
			}
			perFlowLast[event.FlowID] = true
		}
		kept = append([]TraceEvent{event}, kept...)
	}
	outcome.Trace = kept
}

func scrubEventCarrierKeys(value any) any {
	switch v := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, val := range v {
			scrubbed := false
			for _, key2 := range eventResumeScrubKeys {
				if key == key2 {
					scrubbed = true
					break
				}
			}
			if !scrubbed {
				out[key] = scrubEventCarrierKeys(val)
			}
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, val := range v {
			out[i] = scrubEventCarrierKeys(val)
		}
		return out
	default:
		return value
	}
}

func succeededOutcome(out *Outcome, result any) *Outcome {
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
