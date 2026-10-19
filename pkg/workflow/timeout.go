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
	// Code is the flow error code carried by an abort (plaita error_code;
	// nil keeps the historical raw error unless Message is set too).
	Code *int `json:"code,omitempty"`
	// Message is the flow error message carried by an abort (plaita
	// error_message; plaita's default timeout message is
	// "Node execution timed out" when only the code is configured).
	Message string `json:"message,omitempty"`
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
		return nil, wrapHandlerError(err, t.Strategy, t.Code, t.Message)
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
