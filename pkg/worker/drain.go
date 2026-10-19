package worker

import (
	"context"
	"sync"
	"time"
)

// Three-state drain (protocol §9.3, S18 semantics):
//
//	D1 draining   RequestDrain: stop taking new tasks, registry status
//	              draining flushed immediately, start the bounded wait timer.
//	D2 force-stop ForceStop: unconditional "stop consuming", best-effort
//	              cancel of every in-flight run, interruptible backoff.
//	D3 signal     the signal handler only calls RequestDrain; the consumption
//	              loop exits at the task boundary (read slices ≤1s).
//
// A message delivered after draining began is never dispatched nor acked: it
// stays in the PEL for another worker's reclaim. A step abandoned by
// force-stop writes no terminal state — the message stays pending too.

// drainState is embedded in Worker.
type drainState struct {
	drainMu    sync.Mutex
	draining   bool
	forced     bool
	drainTimer *time.Timer

	runsMu     sync.Mutex
	activeRuns map[*executionRun]context.CancelFunc
}

// Draining reports whether the worker has left the "accept new work" state.
func (w *Worker) Draining() bool {
	w.drainMu.Lock()
	defer w.drainMu.Unlock()
	return w.draining
}

// ForceStopped reports whether D2 fired (backoff and new dispatches abort).
func (w *Worker) ForceStopped() bool {
	w.drainMu.Lock()
	defer w.drainMu.Unlock()
	return w.forced
}

// RequestDrain implements D1. Idempotent: a second call never resets the
// drain start or the bounded-wait timer.
func (w *Worker) RequestDrain(reason string) {
	w.drainMu.Lock()
	if w.draining {
		w.drainMu.Unlock()
		return
	}
	w.draining = true
	timeout := w.cfg.DrainTimeout
	timer := time.AfterFunc(timeout, func() {
		w.ForceStop("drain timeout exceeded")
	})
	w.drainTimer = timer
	w.drainMu.Unlock()

	w.logger.Info("worker entering draining: no new tasks, waiting for in-flight work",
		"reason", reason, "drain_timeout", timeout)
	// §9.3 D1: the registry status must be visible NOW (readiness probes and
	// dispatch routing use it), with the three short retries of
	// flush_registry_status.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := w.registry.SetStatusFlush(ctx, RegStatusDraining, 3); err != nil {
		w.logger.Warn("draining registry flush failed (status kept in memory)", "error", err)
	}
}

// ForceStop implements D2: stop waiting, cancel every in-flight run. It is
// unconditional — the "timer fired just as the last task finished" window must
// not leave the worker accepting tasks.
func (w *Worker) ForceStop(reason string) {
	w.drainMu.Lock()
	if w.forced {
		w.drainMu.Unlock()
		return
	}
	w.forced = true
	w.draining = true
	timer := w.drainTimer
	w.drainMu.Unlock()
	if timer != nil {
		timer.Stop()
	}

	w.runsMu.Lock()
	runs := make([]*executionRun, 0, len(w.activeRuns))
	cancels := make([]context.CancelFunc, 0, len(w.activeRuns))
	for run, cancel := range w.activeRuns {
		runs = append(runs, run)
		cancels = append(cancels, cancel)
	}
	w.runsMu.Unlock()

	w.logger.Warn("worker force-stop: cancelling in-flight executions (best-effort, no hard interrupt yet)",
		"reason", reason, "inflight", len(cancels))
	for i, cancel := range cancels {
		if runs[i] != nil {
			runs[i].markDrainCancelled()
		}
		cancel()
	}
}

// stopDrainTimer releases the D2 timer (graceful shutdown path).
func (w *Worker) stopDrainTimer() {
	w.drainMu.Lock()
	timer := w.drainTimer
	w.drainTimer = nil
	w.drainMu.Unlock()
	if timer != nil {
		timer.Stop()
	}
}

// registerRun tracks an in-flight run so ForceStop can cancel it.
func (w *Worker) registerRun(run *executionRun, cancel context.CancelFunc) {
	w.runsMu.Lock()
	w.activeRuns[run] = cancel
	w.runsMu.Unlock()
}

func (w *Worker) unregisterRun(run *executionRun) {
	w.runsMu.Lock()
	delete(w.activeRuns, run)
	w.runsMu.Unlock()
}

// waitIdle blocks until no task is in flight or the timeout elapses (§9.3
// stop ordering). Returns true when idle.
func (w *Worker) waitIdle(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if w.active.Load() == 0 {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// nodeRetryBackoffSlice is A46-06's slice: a long sleep must not pin shutdown.
const nodeRetryBackoffSlice = 200 * time.Millisecond

// interruptibleSleep sleeps in slices; it returns true when D2 interrupted the
// wait (the caller then abandons enqueue/ack: the message stays pending).
func (w *Worker) interruptibleSleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return w.ForceStopped()
	}
	deadline := time.Now().Add(d)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return w.ForceStopped()
		}
		slice := nodeRetryBackoffSlice
		if remaining < slice {
			slice = remaining
		}
		select {
		case <-ctx.Done():
			return true
		case <-time.After(slice):
		}
		if w.ForceStopped() {
			return true
		}
	}
}
