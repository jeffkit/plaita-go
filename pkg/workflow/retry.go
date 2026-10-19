package workflow

import (
	"context"

	"github.com/sethvargo/go-retry"
)

// Backoff is a backoff policy implementation for retry.
type Backoff = retry.Backoff

// RetryFunc is a retry function.
type RetryFunc = retry.RetryFunc

// RetryableError is a retryable error.
var RetryableError = retry.RetryableError

// Retrier is a retry interface.
type Retrier interface {
	Backoff() Backoff
	Do(ctx context.Context, f RetryFunc) error
}

type lowDelayRetrier struct {
	backoff     Backoff
	maxAttempts uint
}

// Backoff returns the backoff policy.
func (r *lowDelayRetrier) Backoff() Backoff {
	return r.backoff
}

// Do retries the given function.
func (r *lowDelayRetrier) Do(ctx context.Context, f RetryFunc) error {
	return retry.Do(ctx, r.Backoff(), f)
}
