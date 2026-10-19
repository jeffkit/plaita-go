package workflow

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/sethvargo/go-retry"

	validation "github.com/go-ozzo/ozzo-validation"

	"github.com/jeffkit/plaita-go/internal/errs"
)

// Workflow execute error definitions
var (
	ErrStartNodeNotFound = errors.New("start node not found")
	ErrEndNodeNotFound   = errors.New("end node not found")
	ErrNodeNotFound      = errors.New("workflow node not found")
)

// DefaultHandlerErrorCode is the abort code used when a handler with a
// configured message (or code slot) aborts but no explicit code was set —
// plaita ErrorHandler.error_code's default (-9527).
const DefaultHandlerErrorCode = -9527

// Node error codes shared with the builtin nodes (pkg/workflow/nodes/http).
const (
	// nodeGenRequestErrorCode marks deterministic request-generation errors
	// (bad configuration/parameters): never retried (plaita is_retryable_error).
	nodeGenRequestErrorCode = 1001
	// transient node codes: sending/processing errors — retried.
	nodeDoRequestErrorCode = 1002
	nodeExecErrorCode      = 1003
)

// HandleStrategy is the strategy for error or timeout handle
type HandleStrategy string

// HandleStrategy type enums
const (
	HandleStrategyAbort        HandleStrategy = "abort"         // abort execute
	HandleStrategyContinue     HandleStrategy = "continue"      // continue execute
	HandleStrategyContinueWith HandleStrategy = "continue-with" // continue execute with default value
)

// HandlerError is the typed error raised when a handler with a configured
// code/message aborts the flow — the Go analogue of plaita's
// NodeExecutionError / NodeTimeoutError carrying error_code/error_message.
type HandlerError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	cause   error
}

// Error returns the error description.
func (e *HandlerError) Error() string {
	if e == nil {
		return ""
	}
	if e.cause != nil {
		return fmt.Sprintf("code: %d, message: %s, cause: %v", e.Code, e.Message, e.cause)
	}
	return fmt.Sprintf("code: %d, message: %s", e.Code, e.Message)
}

// Unwrap exposes the original error so errors.Is/As keep working (e.g. a
// wrapped timeout stays recognizable as context.DeadlineExceeded).
func (e *HandlerError) Unwrap() error {
	return e.cause
}

// wrapHandlerError applies the handler's abort code/message to err. Handlers
// without any configured code/message keep their historical (raw) error, and
// an already handler-wrapped error is never wrapped twice.
func wrapHandlerError(err error, strategy HandleStrategy, code *int, message string) error {
	if err == nil || strategy != HandleStrategyAbort {
		return err
	}
	if code == nil && message == "" {
		return err
	}
	var he *HandlerError
	if errors.As(err, &he) {
		return err
	}
	c := DefaultHandlerErrorCode
	if code != nil {
		c = *code
	}
	msg := message
	if msg == "" {
		msg = err.Error()
	}
	return &HandlerError{Code: c, Message: msg, cause: err}
}

// RecoverableErrorHandlePolicy 可恢复错误处理策略
type RecoverableErrorHandlePolicy struct {
	Strategy     HandleStrategy `json:"strategy"`
	DefaultValue any            `json:"defaultValue,omitempty"`
	RetryTimes   uint           `json:"retryTimes"`
	// Code is the flow error code carried by an abort (plaita error_code;
	// nil keeps the historical raw error unless Message is set too).
	Code *int `json:"code,omitempty"`
	// Message is the flow error message carried by an abort (plaita
	// error_message).
	Message string `json:"message,omitempty"`
}

// Validate validates the RecoverableErrorHandlePolicy
func (r *RecoverableErrorHandlePolicy) Validate() error {
	if r == nil || len(r.Strategy) == 0 {
		return nil
	}
	return validation.ValidateStruct(r,
		validation.Field(&r.Strategy, validation.In(
			HandleStrategyAbort,
			HandleStrategyContinue,
			HandleStrategyContinueWith,
		)),
		validation.Field(&r.RetryTimes, validation.Min(0)))
}

// Backoff returns the backoff strategy: plaita's exponential retry backoff,
// 1s × 2^n capped at 30s, RetryTimes attempts.
func (r *RecoverableErrorHandlePolicy) Backoff() Backoff {
	b := retry.NewExponential(1 * time.Second)
	b = retry.WithCappedDuration(30*time.Second, b)
	b = retry.WithMaxRetries(uint64(r.RetryTimes), b)
	return b
}

// Retrier returns the retrier by error handle policy
func (r *RecoverableErrorHandlePolicy) Retrier() Retrier {
	return newLowDelayRetrier(r.Backoff())
}

// HandleFinalError handles the final error with the error handle strategy
func (r *RecoverableErrorHandlePolicy) HandleFinalError(err error) (any, error) {
	switch r.Strategy {
	case HandleStrategyAbort:
		return nil, wrapHandlerError(err, r.Strategy, r.Code, r.Message)
	case HandleStrategyContinue:
		return nil, nil
	case HandleStrategyContinueWith:
		return r.DefaultValue, nil
	default:
		return nil, err
	}
}

// IsContinue check the strategy is continued
func (r *RecoverableErrorHandlePolicy) IsContinue() bool {
	switch r.Strategy {
	case HandleStrategyAbort:
		return false
	case HandleStrategyContinue, HandleStrategyContinueWith:
		return true
	default:
		return false
	}
}

// DeterministicError marks an error as deterministic (bad configuration,
// invalid parameters — plaita's non-retryable ValueError class): the engine
// never retries it regardless of retryTimes. Node implementations can wrap
// their errors with NonRetryable to opt out of blind retries.
type DeterministicError struct {
	Err error
}

// Error returns the error description.
func (e *DeterministicError) Error() string {
	if e == nil || e.Err == nil {
		return "<nil>"
	}
	return e.Err.Error()
}

// Unwrap exposes the wrapped error.
func (e *DeterministicError) Unwrap() error {
	return e.Err
}

// NonRetryable wraps an error as deterministic: the retry loop executes the
// node exactly once and hands the error to the error handler.
func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return &DeterministicError{Err: err}
}

// deterministicErrorTypeNames lists node-side deterministic error types by
// name. Those types live in pkg/workflow/nodes, which pkg/workflow cannot
// import (import cycle), so the engine bridges the classification over the
// concrete type name — the wiring AGENTS.md lists as pending. If a node type
// here is renamed, its errors degrade to "unknown" and keep the historical
// retry behavior (fail-open, same as before this bridge).
var deterministicErrorTypeNames = map[string]bool{
	"NotifyDeterministicError": true, // notify family: credential/config errors
}

// IsRetryableError reports whether err is worth retrying, aligned with
// plaita runner.is_retryable_error:
//
//   - deterministic errors (NonRetryable and the node-side classified types)
//     never retry;
//   - node code 1001 (request generation = configuration/parameter error)
//     never retries; 1002/1003 (send/processing = transient) retry;
//   - every other unknown error keeps the historical behavior (retry).
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}
	var det *DeterministicError
	if errors.As(err, &det) {
		return false
	}
	if hasDeterministicErrorType(err) {
		return false
	}
	switch errs.Internal(err).Code {
	case nodeGenRequestErrorCode:
		return false
	case nodeDoRequestErrorCode, nodeExecErrorCode:
		return true
	}
	return true
}

func hasDeterministicErrorType(err error) bool {
	for err != nil {
		if t := reflect.TypeOf(err); t != nil {
			if t.Kind() == reflect.Pointer {
				t = t.Elem()
			}
			if deterministicErrorTypeNames[t.Name()] {
				return true
			}
		}
		err = errors.Unwrap(err)
	}
	return false
}
