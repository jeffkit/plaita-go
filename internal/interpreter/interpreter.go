// Package interpreter provides an interpreter that interprets the programming language source code.
package interpreter

import (
	"context"
)

// Interpreter represents an interpreter that interprets the programming language source code.
type Interpreter interface {
	Eval(ctx context.Context, src string, opts ...Option) (any, error)
}
