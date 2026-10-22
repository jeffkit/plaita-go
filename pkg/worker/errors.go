package worker

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/jeffkit/plaita-go/internal/errs"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// Worker error taxonomy, aligned with the protocol disposition table (§9.2):
//
//   - transient errors (Redis failures, FlowDefinitionLoadError) → leave the
//     message pending for redelivery / reclaim;
//   - deterministic failures (persist failures with a lost lease, flow
//     missing) → terminal state or poison ack;
//   - payload/schema failures → dead-letter + ack (§2.4).
var (
	// ErrExecutionLeaseConflict is the ExecutionLeaseError analogue: another
	// holder owns the execution lease (§3.5 disposition).
	ErrExecutionLeaseConflict = errors.New("execution lease held by another worker")
	// ErrLeaseLost means the lease was lost mid-execution (renew/CAS
	// compare failed, §3.4 watchdog semantics).
	ErrLeaseLost = errors.New("execution lease lost")
	// ErrFenceGenerationInconsistent (#54, A2R25-05): the fence key matched
	// but the never-cleared fencegen counter disagrees — a partial restore
	// brought back an old state+fence pair while fencegen stayed at the
	// newer generation. The stale token must not re-hit the CAS. Derived
	// from ErrLeaseLost so every consumer keeps the lost-write-authority
	// semantics (Python raises ExecutionLeaseError for both branches).
	ErrFenceGenerationInconsistent = fmt.Errorf("%w: fence/fencegen generation inconsistent", ErrLeaseLost)
	// ErrStatePersist is the StatePersistError analogue: the state row could
	// not be saved — the message must NOT be acked (§5.2).
	ErrStatePersist = errors.New("execution state persist failed")
	// ErrStateLoad is the ExecutionStateLoadError analogue: the state row
	// exists but is unreadable — never swallowed as "missing" (§5.2).
	ErrStateLoad = errors.New("execution state load failed")
	// ErrFlowNotFound: the flow definition key does not exist in
	// RedisFlowStorage (deterministic — poison ack, §7).
	ErrFlowNotFound = errors.New("flow definition not found in engine storage")
	// ErrFlowLoad is the FlowDefinitionLoadError analogue: a transient read
	// failure while loading the flow definition (§5.2 read-side contract).
	ErrFlowLoad = errors.New("flow definition load failed")
	// ErrInvalidPayload is the InvalidTaskPayload analogue: the payload is
	// not JSON / not an object — DLQ + ack at the parse point (§2.4).
	ErrInvalidPayload = errors.New("invalid task payload")
	// ErrUnsupportedSchema: envelope schema_version above this build's write
	// version — reject into DLQ (§2.2).
	ErrUnsupportedSchema = errors.New("unsupported task schema version")
	// ErrMaxDeliveriesExceeded: delivery budget exhausted — guarded
	// dead-letter (§2.4).
	ErrMaxDeliveriesExceeded = errors.New("max deliveries exceeded")
	// ErrDrainAborted means the in-flight step was abandoned because a drain
	// force-stop cancelled it (§9.3 D2). The message must stay pending so
	// another worker can take over; no terminal state is written.
	ErrDrainAborted = errors.New("execution abandoned by drain force-stop")
	// ErrHandoverCounterUnavailable: the TaskNotForThisWorker counter could
	// not be incremented — fail-closed, never hand over without a bound
	// (§9.2).
	ErrHandoverCounterUnavailable = errors.New("affinity handover counter unavailable")
	// ErrTenantQuotaDeferred: the tenant's start quota is full (#30) — the
	// carrier is re-enqueued at the stream tail (quota_defers counter) and
	// the original acked; it is never executed nor dead-lettered by this
	// path. After the defer budget the copy is admitted with quota_bypass.
	ErrTenantQuotaDeferred = errors.New("tenant start quota full, carrier deferred")
)

// NodeRetryableError is the NodeExecutionRetryableError analogue (§9.1): the
// engine's node failed in a retryable way below the worker-level budget, so
// the consumer loop must re-enqueue a fresh carrier instead of writing an
// error terminal.
type NodeRetryableError struct {
	ExecutionID string
	FailedNode  string
	// Attempt is the per-node counter value after this failure (1-based).
	Attempt int
	// Budget is the per-node retry budget this attempt was measured against
	// (queue max_deliveries, §9.1).
	Budget int
	Err    error
}

func (e *NodeRetryableError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("node %s retryable failure (attempt %d/%d): %v",
		e.FailedNode, e.Attempt, e.Budget, e.Err)
}

func (e *NodeRetryableError) Unwrap() error { return e.Err }

// flowErrorCode is plaita's _FLOW_ERROR_CODE fallback (§5.4: code=-500 when
// neither a node-level nor handler error code is available).
const flowErrorCode = -500

// NormalizedError is the terminal state.error structure (protocol §5.4,
// plaita#93). Serialized as a plain dict into ExecutionState.Error.
type NormalizedError struct {
	Message     string         `json:"message"`
	Code        int            `json:"code"`
	ErrorType   string         `json:"error_type"`
	Response    map[string]any `json:"response,omitempty"`
	Details     map[string]any `json:"details,omitempty"`
	NodeRetries int            `json:"node_retries,omitempty"`
	FailedNode  string         `json:"failed_node,omitempty"`
}

// NormalizeEngineError maps an engine error into the §5.4 structure:
// code from HandlerError / engine Error, falling back to -500; error_type
// from the outermost meaningful Go error type name (the analogue of the
// Python "exception class name fallback"); failed_node supplied by the
// caller from the run's node-error tracking.
func NormalizeEngineError(err error, failedNode string, nodeRetries int) *NormalizedError {
	ne := &NormalizedError{
		Message:    err.Error(),
		Code:       flowErrorCode,
		ErrorType:  errorTypeName(err),
		FailedNode: failedNode,
	}
	var he *workflow.HandlerError
	if errors.As(err, &he) {
		ne.Code = he.Code
		ne.ErrorType = "HandlerError"
		return ne
	}
	var ee *errs.Error
	if errors.As(err, &ee) {
		ne.Code = ee.Code
		// engine Error keeps the Go type name for error_type (below) unless
		// a more specific classification exists.
		return ne
	}
	return ne
}

// WorkerError builds a worker-level normalized error with a fixed error_type
// (§5.4: worker-judged paths name themselves, never invent node codes).
func WorkerError(errorType string, code int, message string) *NormalizedError {
	if code == 0 {
		code = flowErrorCode
	}
	return &NormalizedError{Message: message, Code: code, ErrorType: errorType}
}

func errorTypeName(err error) string {
	for e := err; e != nil; {
		t := reflect.TypeOf(e)
		if t != nil {
			if t.Kind() == reflect.Pointer {
				t = t.Elem()
			}
			if t.Name() != "" && t.Name() != "retryableError" && t.Name() != "wrapErr" {
				return t.Name()
			}
		}
		e = errors.Unwrap(e)
	}
	return "error"
}

// terminalErrorState returns the terminal error dict for state persistence.
func terminalErrorState(ne *NormalizedError) map[string]any {
	if ne == nil {
		return map[string]any{"message": "unknown error"}
	}
	out := map[string]any{
		"message":    ne.Message,
		"code":       ne.Code,
		"error_type": ne.ErrorType,
	}
	if ne.Response != nil {
		out["response"] = ne.Response
	}
	if ne.Details != nil {
		out["details"] = ne.Details
	}
	if ne.NodeRetries > 0 {
		out["node_retries"] = ne.NodeRetries
	}
	if ne.FailedNode != "" {
		out["failed_node"] = ne.FailedNode
	}
	return out
}

// errChainHasCancellation walks the error chain looking for context.Canceled
// (the Go analogue of _chain_has FlowCancelledException, protocol §4.2 #4:
// cancellation wins over every other classification, never persisted as
// error).
func errChainHasCancellation(err error) bool {
	return errors.Is(err, errCancelledMark)
}

// errCancelledMark marks a worker-initiated cancellation. The engine surfaces
// context.Canceled when the run context is cancelled; we treat both that and
// our own marker as cancellation evidence.
var errCancelledMark = errors.New("flow cancelled by worker")

// cancelledError wraps a cancellation for the terminal write path.
func cancelledError(reason string) error {
	return fmt.Errorf("%w: %s", errCancelledMark, reason)
}
