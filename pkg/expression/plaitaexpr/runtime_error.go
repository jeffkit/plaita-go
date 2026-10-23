package plaitaexpr

import (
	"errors"
	"fmt"
)

// ErrExpressionRuntime marks a Go runtime error recovered from expression
// evaluation (#56): goval deliberately re-panics runtime.Error (its recover
// contract targets trusted host programs), but a flow expression is untrusted
// input — the error must surface as a named error, never escape Eval and kill
// the worker process. ZeroDivisionError is a ValueError-class failure in
// plaita semantics: the same input divides by zero again, so the engine side
// classifies it deterministic (never retried).
var ErrExpressionRuntime = errors.New("expression runtime error")

// RuntimeError carries a recovered Go runtime error from expression
// evaluation. A named type (not a bare error) so the engine-side
// deterministic bridge can classify it by type name without an import cycle —
// pkg/workflow cannot import this package's error types by identity.
type RuntimeError struct {
	Err error
}

// Error renders the sentinel plus the original runtime text ("runtime error:
// integer divide by zero") — the message is the operator's only diagnostic.
func (e *RuntimeError) Error() string {
	if e == nil || e.Err == nil {
		return ErrExpressionRuntime.Error()
	}
	return fmt.Sprintf("%v: %v", ErrExpressionRuntime, e.Err)
}

// Unwrap exposes the sentinel + the recovered runtime error (both
// errors.Is-able).
func (e *RuntimeError) Unwrap() []error {
	return []error{ErrExpressionRuntime, e.Err}
}
