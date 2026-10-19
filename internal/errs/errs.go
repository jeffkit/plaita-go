// Package errs plaita internal error implementation
package errs

import (
	"errors"
	"fmt"
)

// plaita internal error code
const (
	RetOK = 0 // success

	RuntimeErrorCode = 10000 // runtime error code
)

// Error represents plaita internal error
type Error struct {
	Code    int    `json:"code" structs:"code"`       // error code
	Message string `json:"message" structs:"message"` // error message
	Info    any    `json:"info" structs:"info"`       // extra error info

	cause error // original error
}

// Error returns the error description
func (e *Error) Error() string {
	if e == nil {
		return ""
	}

	if e.cause != nil {
		return fmt.Sprintf("code: %d, message: %s, info: %v, cause: %v", e.Code, e.Message, e.Info, e.cause)
	}
	return fmt.Sprintf("code: %d, message: %s, info: %v", e.Code, e.Message, e.Info)
}

// Wrap error by plaita error
func Wrap(err error, code int, message string) error {
	return &Error{
		Code:    code,
		Message: message,
		cause:   err,
	}
}

// WrapWithInfo error by plaita error with extra info
func WrapWithInfo(err error, code int, message string, info any) error {
	return &Error{
		Code:    code,
		Message: message,
		Info:    info,
		cause:   err,
	}
}

// Internal error
func Internal(err error) *Error {
	if err == nil {
		return nil
	}

	// plaita error
	var e *Error
	if errors.As(err, &e) {
		return e
	}

	// non plaita error
	return &Error{
		Code:    RuntimeErrorCode,
		Message: err.Error(),
		cause:   err,
	}
}
