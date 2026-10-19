package worker

import (
	"context"
	"errors"
	"time"
)

// Orphan watch (protocol §3.4 + §8.2). After a lease loss the execution object
// may still be advancing: the watchdog therefore publishes one progress
// heartbeat, marks the lease lost, aborts the in-flight step, and hands the run
// to this watch. From here on the watch ONLY publishes progress heartbeats —
// it never renews the lease, never cancels again, never writes a terminal.
//
// Why it matters: keeper's zombie judgment uses the newest node timings /
// last_update_time; a healthy run whose row stopped moving is killed after
// 1800s. The fenced CAS still succeeds while the old fence generation is
// current (the fence key outlives the lease by 7 days), so the orphan can keep
// the row fresh until the execution really ends or a new holder takes over.

// orphanEntry is one post-lease-loss observation target.
type orphanEntry struct {
	executionID string
	tenantID    string
	fenceToken  int64
	run         *executionRun
	rentedAt    time.Time
	rounds      int
}

// registerOrphan starts observing an execution whose lease was lost.
func (w *Worker) registerOrphan(entry *orphanEntry) {
	if entry == nil || entry.executionID == "" {
		return
	}
	entry.rentedAt = time.Now()
	w.orphanMu.Lock()
	w.orphans[entry.executionID] = entry
	w.orphanMu.Unlock()
	w.logger.Warn("execution entered orphan watch after lease loss (progress heartbeats only)",
		"execution_id", entry.executionID, "fence", entry.fenceToken)
}

// dropOrphan stops observing an execution (run ended, handed back, or a new
// holder was fenced in).
func (w *Worker) dropOrphan(executionID, reason string) {
	w.orphanMu.Lock()
	entry, ok := w.orphans[executionID]
	delete(w.orphans, executionID)
	w.orphanMu.Unlock()
	if ok {
		w.logger.Info("orphan watch dropped",
			"execution_id", executionID, "reason", reason, "rounds", entry.rounds)
	}
}

// orphanCount is the observation-set size (test/probe surface).
func (w *Worker) orphanCount() int {
	w.orphanMu.Lock()
	defer w.orphanMu.Unlock()
	return len(w.orphans)
}

// orphanLoop is the periodic half of the watch; it ticks at the watchdog
// cadence (lease ttl/3, floor 1s) until the worker context ends.
func (w *Worker) orphanLoop(ctx context.Context) {
	interval := time.Duration(w.cfg.LeaseTTLSeconds/3) * time.Second
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.publishOrphans()
		}
	}
}

// publishOrphans walks the observation set once and publishes a progress
// heartbeat for every still-advancing orphan.
func (w *Worker) publishOrphans() {
	w.orphanMu.Lock()
	entries := make([]*orphanEntry, 0, len(w.orphans))
	for _, e := range w.orphans {
		entries = append(entries, e)
	}
	w.orphanMu.Unlock()

	for _, entry := range entries {
		if !entry.run.isAdvancing() {
			w.dropOrphan(entry.executionID, "execution ended / no open node")
			continue
		}
		w.publishOrphanProgress(entry.tenantID, entry.fenceToken, entry)
		w.orphanMu.Lock()
		if current, ok := w.orphans[entry.executionID]; ok && current == entry {
			current.rounds++
		}
		w.orphanMu.Unlock()
	}
}

// publishOrphanProgress merges the run's current node_timings into a FRESH
// load of the state row (never overwrite another writer's snapshot) and
// refreshes last_update_time. Failures only warn except a fence mismatch,
// which means a new holder owns the execution — the watch then drops it.
func (w *Worker) publishOrphanProgress(tenantID string, fenceToken int64, entry *orphanEntry) {
	if entry == nil || entry.run == nil {
		return
	}
	// The run context is cancelled when the lease is lost; use a detached,
	// bounded context so the heartbeat still lands.
	hbCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	state, err := w.store.LoadExecutionState(hbCtx, tenantID, entry.executionID)
	if err != nil {
		w.logger.Warn("orphan progress heartbeat: state load failed", "error", err)
		return
	}
	if state == nil || TerminalStatuses[state.Status] {
		w.dropOrphan(entry.executionID, "state missing or terminal")
		return
	}
	merged := map[string]any{}
	for k, v := range state.NodeTimings {
		merged[k] = v
	}
	for k, v := range entry.run.snapshotNodeTimings() {
		merged[k] = v
	}
	state.NodeTimings = merged
	state.LastUpdateTime = nowISO()
	if err := w.store.SaveExecutionState(hbCtx, tenantID, state, fenceToken); err != nil {
		if errors.Is(err, ErrLeaseLost) {
			// A new generation owns the row: this process's heartbeats are no
			// longer trustworthy.
			w.dropOrphan(entry.executionID, "fence generation taken over")
			return
		}
		w.logger.Warn("orphan progress heartbeat failed (observation only)", "error", err)
	}
}
