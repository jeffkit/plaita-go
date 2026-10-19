package workflow

import (
	"context"
	"errors"

	validation "github.com/go-ozzo/ozzo-validation"
)

// TimeoutHandlePolicy is a policy for workflow timeout handler
type TimeoutHandlePolicy struct {
	Strategy     HandleStrategy `json:"strategy"`
	DefaultValue any            `json:"defaultValue,omitempty"`
}

// Validate validates the TimeoutHandlePolicy
func (t *TimeoutHandlePolicy) Validate() error {
	if t == nil || len(t.Strategy) == 0 {
		return nil
	}
	return validation.ValidateStruct(t,
		validation.Field(&t.Strategy, validation.In(
			HandleStrategyAbort,
			HandleStrategyContinue,
			HandleStrategyContinueWith,
		)))
}

// Handle handles the timeout error
func (t *TimeoutHandlePolicy) Handle(err error) (any, error) {
	if !contextTimeoutError(err) {
		return nil, err
	}

	switch t.Strategy {
	case HandleStrategyAbort:
		return nil, err
	case HandleStrategyContinue:
		return nil, nil
	case HandleStrategyContinueWith:
		return t.DefaultValue, nil
	default:
		return nil, err
	}
}

func contextTimeoutError(err error) bool {
	return errors.Is(err, context.DeadlineExceeded)
}
