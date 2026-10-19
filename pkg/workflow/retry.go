package workflow

import (
	"context"
	"errors"
	"time"
)

// Backoff is a backoff policy implementation for retry.
type Backoff = retryBackoff

// RetryFunc is a retry function.
type RetryFunc = func(ctx context.Context) error

// retryableError marks an error as retryable. Its Error() keeps the
// historical "retryable: " prefix: node implementations (and their tests)
// detect the marker by that prefix.
type retryableError struct {
	err error
}

// RetryableError marks an error as retryable.
func RetryableError(err error) error {
	if err == nil {
		return nil
	}
	return &retryableError{err}
}

// Unwrap implements error wrapping.
func (e *retryableError) Unwrap() error {
	return e.err
}

// Error returns the error description.
func (e *retryableError) Error() string {
	if e.err == nil {
		return "retryable: <nil>"
	}
	return "retryable: " + e.err.Error()
}

func asRetryableError(err error) bool {
	var rerr *retryableError
	return errors.As(err, &rerr)
}

// Retrier is a retry interface.
type Retrier interface {
	Backoff() Backoff
	Do(ctx context.Context, f RetryFunc) error
}

// retrySleep suspends between two retry attempts. It is a package variable so
// tests can swap in a fake clock and assert the requested delays without
// actually sleeping (plaita swaps _backoff_sleep for the same reason).
var retrySleep = func(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// lowDelayRetrier drives the retry loop over a backoff policy. Unlike
// go-retry's Do it always gives the function a first attempt (flow/node
// retryFuncs run trigger callbacks and their own expired-context guard) and
// returns the ORIGINAL error when retries are exhausted, keeping code/message
// intact.
type lowDelayRetrier struct {
	backoff Backoff
}

func newLowDelayRetrier(b Backoff) *lowDelayRetrier {
	return &lowDelayRetrier{backoff: b}
}

// Backoff returns the backoff policy.
func (r *lowDelayRetrier) Backoff() Backoff {
	return r.backoff
}

// Do retries the given function.
func (r *lowDelayRetrier) Do(ctx context.Context, f RetryFunc) error {
	backoff := r.backoff
	for {
		err := f(ctx)
		if err == nil {
			return nil
		}
		// not marked retryable (deterministic error / handled result):
		// stop and surface the original error.
		if !asRetryableError(err) {
			return err
		}
		// budget gone mid-attempt: surface the context error so timeout
		// attribution kicks in.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		next, stop := backoff.Next()
		if stop {
			// retries exhausted: surface the ORIGINAL error, keeping its
			// code/message intact (go-retry unwraps here too).
			var rerr *retryableError
			if errors.As(err, &rerr) && rerr.err != nil {
				return rerr.err
			}
			return err
		}
		if sleepErr := retrySleep(ctx, next); sleepErr != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return sleepErr
		}
	}
}

// retryBackoff is the local backoff abstraction; sethvargo/go-retry provides
// the implementations (see RecoverableErrorHandlePolicy.Backoff).
type retryBackoff interface {
	Next() (delay time.Duration, stop bool)
}
