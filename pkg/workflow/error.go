package workflow

import (
	"errors"
	"time"

	"github.com/sethvargo/go-retry"

	validation "github.com/go-ozzo/ozzo-validation"
)

// Workflow execute error definitions
var (
	ErrStartNodeNotFound = errors.New("start node not found")
	ErrEndNodeNotFound   = errors.New("end node not found")
	ErrNodeNotFound      = errors.New("workflow node not found")
)

// HandleStrategy is the strategy for error or timeout handle
type HandleStrategy string

// HandleStrategy type enums
const (
	HandleStrategyAbort        HandleStrategy = "abort"         // abort execute
	HandleStrategyContinue     HandleStrategy = "continue"      // continue execute
	HandleStrategyContinueWith HandleStrategy = "continue-with" // continue execute with default value
)

// RecoverableErrorHandlePolicy 可恢复错误处理策略
type RecoverableErrorHandlePolicy struct {
	Strategy     HandleStrategy `json:"strategy"`
	DefaultValue any            `json:"defaultValue,omitempty"`
	RetryTimes   uint           `json:"retryTimes"`
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

// Backoff returns the backoff strategy
func (r *RecoverableErrorHandlePolicy) Backoff() Backoff {
	b := retry.NewConstant(10 * time.Millisecond)
	b = retry.WithMaxRetries(uint64(r.RetryTimes), b)
	return b
}

// Retrier returns the retrier by error handle policy
func (r *RecoverableErrorHandlePolicy) Retrier() Retrier {
	return &lowDelayRetrier{
		backoff:     r.Backoff(),
		maxAttempts: r.RetryTimes,
	}
}

// HandleFinalError handles the final error with the error handle strategy
func (r *RecoverableErrorHandlePolicy) HandleFinalError(err error) (any, error) {
	switch r.Strategy {
	case HandleStrategyAbort:
		return nil, err
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
