package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/internal/version"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// Consumer loop and execution write-back points (protocol §4, §5, §8, §9.2).
// The disposition table:
//
//	handler returned nil          → terminal state persisted → ack (+XDEL)
//	handler returned error        → transient/deliberate leave-pending
//	lease conflict                → §3.5 ack-safe disposition
//	payload/schema/max-deliveries → DLQ + ack (queue layer)
//
// MVP boundaries — deliberately NOT implemented (phase 2, see the final
// report):
//
//	TODO(phase-2): orphan watch (_lease_orphan_watch) — after a lease loss the
//	        worker must keep publishing progress heartbeats so the keeper's
//	        1800s staleness never kills a still-running execution (§3.4/§8.2).
//	TODO(phase-2): suspended/resume carrier semantics (A47-F2) — real resume
//	        continuation, resume flow-hash gate, decision-carrier handling
//	        beyond the conflict disposition (§3.4/§7).
//	TODO(phase-2): dispatch_ledger write/read parity (§6.3) and the full
//	        TaskNotForThisWorker handover protocol with counter keys (§9.2).
//	TODO(phase-2): DLQ replay (plaita:dlq:replay) and the dead-letter guard's
//	        recovery-clone re-enqueue branch (§2.4/§9.2).
//	TODO(phase-2): three-state drain (draining/force-stop/signal, §9.3) and
//	        node-level retry budgets + exponential-backoff re-enqueue (§9.1).
//	TODO(phase-2): per-step state persistence (PERSIST_EVERY_N_STEPS=1, §5.2)
//	        — the progress heartbeat (ttl/3) stands in for it in this MVP.
//	TODO(phase-2): task age gate (PLAITA_TASK_MAX_AGE_SECONDS, §1.2) and the
//	        tenant quota / dedup-key idempotency paths.

// runContextKey carries the executionRun through the engine execution
// context so the global engine event listeners can correlate events to the
// owning worker + execution (protocol U7: Go passes explicitly what Python
// does with ContextVars).
type runContextKey struct{}

func withRun(ctx context.Context, run *executionRun) context.Context {
	return context.WithValue(ctx, runContextKey{}, run)
}

func runFromCtx(ctx context.Context) *executionRun {
	if ctx == nil {
		return nil
	}
	run, _ := ctx.Value(runContextKey{}).(*executionRun)
	return run
}

// Worker is the runtime worker MVP.
type Worker struct {
	cfg    *Config
	rdb    *redis.Client
	logger Logger

	queue    *TaskQueue
	lease    *ExecutionLease
	store    *ExecutionStore
	flows    *FlowSource
	bus      *EventPublisher
	registry *Registry

	active       atomic.Int64
	runCtx       context.Context
	runCtxCancel context.CancelFunc
	wg           sync.WaitGroup
	started      atomic.Bool
}

// New assembles a worker. The engine event callbacks (pkg/workflow/event.go)
// are the wiring point for timeline events + node timings; listeners are
// process-global so each worker filters by run ownership.
func New(cfg *Config, rdb *redis.Client, logger Logger) *Worker {
	w := &Worker{
		cfg:      cfg,
		rdb:      rdb,
		logger:   logger,
		queue:    NewTaskQueue(rdb, cfg, logger),
		lease:    NewExecutionLease(rdb, cfg),
		store:    NewExecutionStore(rdb, cfg),
		flows:    NewFlowSource(rdb, cfg, logger),
		bus:      NewEventPublisher(rdb, cfg),
		registry: NewRegistry(rdb, cfg, logger),
	}
	w.queue.SetDeadLetterGuard(w.deadLetterGuard)
	w.registerEngineListeners()
	return w
}

func (w *Worker) registerEngineListeners() {
	workflow.OnNodeStart(func(ctx context.Context, flow *workflow.Workflow, node workflow.Node) {
		run := runFromCtx(ctx)
		if run == nil || run.owner != w {
			return
		}
		ev := run.beginNode(string(node.GetID()), string(node.GetType()), node.GetName())
		w.publishExecutionEvent(ctx, run.executionID, ev)
	})
	workflow.OnNodeEnd(func(ctx context.Context, flow *workflow.Workflow, node workflow.Node,
		result any, err error, args map[string]interface{}) {
		run := runFromCtx(ctx)
		if run == nil || run.owner != w {
			return
		}
		ev := run.endNode(string(node.GetID()), string(node.GetType()), node.GetName(), err != nil)
		w.publishExecutionEvent(ctx, run.executionID, ev)
	})
	workflow.OnFlowStart(func(ctx context.Context, flow *workflow.Workflow) {
		run := runFromCtx(ctx)
		if run == nil || run.owner != w {
			return
		}
		// Only the root flow records the run-level start (child/loop/parallel
		// sub-flows execute inside the same execution).
		if flow == run.rootFlow {
			w.publishExecutionEvent(ctx, run.executionID, run.recordTimeline("run_start", nil))
		}
	})
	workflow.OnFlowEnd(func(ctx context.Context, flow *workflow.Workflow, result any, err error) {
		run := runFromCtx(ctx)
		if run == nil || run.owner != w {
			return
		}
		if flow != run.rootFlow {
			return
		}
		// Capture the root execution's runtime context for the checkpoint.
		run.rootRuntime = ctx
		if err != nil {
			// Failed attempt marker (§5.2: 失败尝试补 run_end(final=False);
			// the terminal write appends final=True).
			w.publishExecutionEvent(ctx, run.executionID,
				run.recordTimeline("run_end", map[string]any{"final": false}))
		}
	})
}

// Run consumes until ctx is cancelled. Registry lifecycle: starting →
// running → (caller cancels) → unregistered (§8.1, §9.3).
func (w *Worker) Run(ctx context.Context) error {
	if !w.started.CompareAndSwap(false, true) {
		return errors.New("worker already started")
	}
	w.runCtx, w.runCtxCancel = context.WithCancel(ctx)
	ctx = w.runCtx

	w.registry.Start(w.cfg.RedisURL)
	if err := w.queue.EnsureGroup(ctx); err != nil {
		return fmt.Errorf("ensure consumer group: %w", err)
	}
	if err := w.queue.SweepAckedResidue(ctx); err != nil {
		w.logger.Warn("startup acked-residue sweep failed", "error", err)
	}
	w.registry.SetStatus(ctx, RegStatusRunning)

	w.wg.Add(2)
	go func() {
		defer w.wg.Done()
		w.registry.HeartbeatLoop(ctx)
	}()
	go func() {
		defer w.wg.Done()
		w.sweepLoop(ctx)
	}()
	defer func() {
		w.registry.SetStatus(context.Background(), RegStatusStopping)
		w.registry.Unregister(context.Background())
	}()

	backoff := time.Duration(0)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		handled, err := w.ProcessOne(ctx)
		if err != nil {
			// Transient family: warn + backoff, never blow up the loop (§2.4).
			w.logger.Warn("task processing left message pending", "error", err)
			backoff = reconnectBackoff
			continue
		}
		backoff = 0
		_ = handled
	}
}

func (w *Worker) sweepLoop(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.SweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.queue.SweepAckedResidue(ctx); err != nil {
				w.logger.Warn("acked-residue sweep failed", "error", err)
			}
		}
	}
}

// Delivery pairs an envelope with its parsed task.
type Delivery struct {
	Envelope *Envelope
	Task     *Task
}

// ProcessOne performs exactly one consume round (reclaim + read + dispatch).
// A nil error after a non-nil delivery means the message was terminal
// (acked, dead-lettered or poison-acked); a non-nil error leaves the message
// pending for redelivery/reclaim.
func (w *Worker) ProcessOne(ctx context.Context) (bool, error) {
	delivery, err := w.queue.Read(ctx)
	if err != nil || delivery == nil {
		return false, err
	}
	task := delivery.Task
	w.active.Add(1)
	w.registry.SetActiveTasks(ctx, int(w.active.Load()))
	defer func() {
		w.active.Add(-1)
		w.registry.SetActiveTasks(context.Background(), int(w.active.Load()))
	}()

	// Capacity-routed dispatch target check (§6.3). The full handover
	// protocol (TaskNotForThisWorker → counter keys → re-enqueue) is phase 2;
	// MVP leaves the message pending so the target worker's reclaim or the
	// delivery ceiling disposes of it.
	if task.TargetInstanceID != "" && task.TargetInstanceID != w.cfg.InstanceID {
		w.logger.Warn("task routed to another worker, leaving pending (handover is phase 2)",
			"target", task.TargetInstanceID, "self", w.cfg.InstanceID, "id", task.MessageID)
		return true, nil
	}

	switch task.Type {
	case "start":
		if err := w.handleStart(ctx, task); err != nil {
			return true, err
		}
	case "resume":
		if err := w.handleResume(ctx, task); err != nil {
			return true, err
		}
	default:
		// Unknown type → ValueError class → poison ack (§2.3, §9.2).
		w.logger.Warn("unknown task type, poison ack", "type", task.Type, "id", task.MessageID)
	}
	return true, w.queue.Ack(ctx, task.MessageID)
}

// handleStart executes a start task: cancel entry checkpoint → lease acquire
// (BEFORE the running row, §3.4) → running row → engine execution → terminal
// write-back (§5.2) → release.
func (w *Worker) handleStart(ctx context.Context, task *Task) error {
	tenant := task.TenantID
	executionID := task.ExecutionID
	if executionID == "" {
		executionID = newExecutionID() // legacy messages: mint in place (§2.3)
		task.ExecutionID = executionID
	}

	// Entry checkpoint (§4.2 #1): cancel intent before anything else.
	if hit, _, err := w.store.IsCancelRequested(ctx, tenant, executionID); err != nil {
		return fmt.Errorf("cancel probe: %w", err)
	} else if hit {
		return w.persistCancelAtEntry(ctx, tenant, executionID, task)
	}

	// Lease acquire strictly before the running row (§3.4).
	holder := NewHolderToken("start", w.cfg.InstanceID)
	handle, err := w.lease.Acquire(ctx, tenant, executionID, holder, w.cfg.LeaseTTLSeconds)
	if err != nil {
		if errors.Is(err, ErrExecutionLeaseConflict) {
			return w.leaseConflictDisposition(ctx, task)
		}
		return fmt.Errorf("lease acquire: %w", err)
	}

	// Flow definition (§7). Missing = deterministic (poison ack after an
	// error terminal); transient read failure = leave pending.
	def, err := w.flows.GetFlowDefinition(ctx, tenant, task.FlowID, task.Version)
	if err != nil {
		_ = w.lease.Release(ctx, handle)
		if errors.Is(err, ErrFlowNotFound) {
			w.logger.Error("flow definition not loadable, error terminal",
				"flow_id", task.FlowID, "error", err)
			state := w.newRunningState(task, executionID, nil)
			return w.persistErrorTerminal(ctx, tenant, handle.FenceToken, state, nil,
				WorkerError("flow_not_found", flowErrorCode, err.Error()))
		}
		return fmt.Errorf("flow load: %w", err)
	}

	state := w.newRunningState(task, executionID, def)
	state.Status = StatusRunning
	state.LastUpdateTime = nowISO()
	if err := w.store.SaveExecutionState(ctx, tenant, state, handle.FenceToken); err != nil {
		_ = w.lease.Release(ctx, handle)
		return fmt.Errorf("running row persist: %w", err)
	}

	return w.executeAndFinalize(ctx, executePlan{
		task:   task,
		def:    def,
		handle: handle,
		state:  state,
	})
}

// executePlan bundles what executeAndFinalize needs.
type executePlan struct {
	task   *Task
	def    *FlowDefinition
	handle *LeaseHandle
	state  *ExecutionState
}

// persistCancelAtEntry terminalizes a start task that was cancelled before
// its running row existed (§4.2 #1: 入口检查点 → cancelled).
func (w *Worker) persistCancelAtEntry(ctx context.Context, tenant, executionID string, task *Task) error {
	w.logger.Info("start cancelled at entry checkpoint", "execution_id", executionID)
	state := w.newRunningState(task, executionID, nil)
	state.Status = StatusCancelled
	state.StartTime = nowISO()
	state.LastUpdateTime = state.StartTime
	state.EndTime = state.StartTime
	if err := w.store.SaveExecutionState(ctx, tenant, state, 0); err != nil {
		return err
	}
	return nil
}

// executeAndFinalize runs the engine with cancel polling + lease watchdog and
// performs the terminal write-back (§5.2 state machine).
func (w *Worker) executeAndFinalize(ctx context.Context, plan executePlan) error {
	task, def, handle := plan.task, plan.def, plan.handle
	tenant := task.TenantID

	run := &executionRun{
		owner:       w,
		executionID: task.ExecutionID,
		tenantID:    tenant,
		rootFlow:    def.Workflow,
		nodeTimings: map[string]map[string]any{},
		enqueuedAt:  task.Timestamp,
	}
	if wait, ok := queueWaitMillis(task.Timestamp); ok {
		run.queueWaitMS = &wait
	}

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	// loopCtx outlives runCtx: the terminal persist + lease release must run
	// on the consumer-loop context, since runCtx is cancelled as soon as the
	// engine returns (by cancellation, lease loss or plain completion).
	loopCtx := ctx
	ctx = withRun(runCtx, run)

	// Cancel polling (§4.2): entry/step boundaries are covered by the flag
	// checks around Execute; this poller covers the in-execution gaps.
	pollDone := make(chan struct{})
	var pollWG sync.WaitGroup
	pollWG.Add(1)
	go func() {
		defer pollWG.Done()
		ticker := time.NewTicker(time.Duration(w.cfg.CancelPollMS) * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-pollDone:
				return
			case <-ticker.C:
				hit, _, err := w.store.IsCancelRequested(runCtx, tenant, task.ExecutionID)
				if err != nil {
					w.logger.Warn("cancel poll failed", "error", err)
					continue
				}
				if hit {
					run.mu.Lock()
					run.cancelHit = true
					run.mu.Unlock()
					cancelRun()
					return
				}
			}
		}
	}()

	// Lease watchdog + progress heartbeat (§3.4, §8.1 layer 3): renew every
	// ttl/3; on success merge the node_timings snapshot into the state row.
	watchDone := make(chan struct{})
	var watchWG sync.WaitGroup
	watchWG.Add(1)
	var leaseLost atomic.Bool
	go func() {
		defer watchWG.Done()
		interval := time.Duration(w.cfg.LeaseTTLSeconds/3) * time.Second
		if interval < time.Second {
			interval = time.Second
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-watchDone:
				return
			case <-ticker.C:
				ok, err := w.lease.Renew(runCtx, handle, w.cfg.LeaseTTLSeconds)
				if err != nil {
					// Redis 瞬断不判死 (§3.4).
					w.logger.Warn("lease renew errored (transient)", "error", err)
					continue
				}
				if !ok {
					// Lease lost: stop the execution; the fenced terminal
					// write would be rejected anyway — leave pending.
					w.logger.Error("execution lease lost mid-run, aborting execution",
						"execution_id", task.ExecutionID)
					leaseLost.Store(true)
					cancelRun()
					return
				}
				if err := w.progressHeartbeat(ctx, tenant, handle.FenceToken, plan.state, run); err != nil {
					w.logger.Warn("progress heartbeat failed", "error", err)
				}
			}
		}
	}()

	result, execErr := def.Workflow.Execute(ctx, task.Params)
	close(pollDone)
	close(watchDone)
	pollWG.Wait()
	watchWG.Wait()
	cancelRun()
	if execErr == nil {
		w.logger.Info("flow result ready", "execution_id", task.ExecutionID, "result", result)
	}

	// Cancellation classification (§4.2 #4): the flag is the source of
	// truth; re-check it after Execute so a hit during the last node is not
	// missed. (Runs on loopCtx — runCtx is already cancelled.)
	cancelHit := run.cancelHit
	if !cancelHit {
		if hit, _, err := w.store.IsCancelRequested(loopCtx, tenant, task.ExecutionID); err == nil {
			cancelHit = hit
		}
	}

	switch {
	case leaseLost.Load():
		_ = w.lease.Release(loopCtx, handle)
		return ErrLeaseLost
	case cancelHit:
		w.logger.Info("execution cancelled", "execution_id", task.ExecutionID)
		err := run.finalize(func() error {
			run.recordTimeline("run_end", map[string]any{"final": true})
			plan.state.Context = w.buildCheckpoint(run, plan.state.Context)
			plan.state.Status = StatusCancelled
			plan.state.EndTime = nowISO()
			plan.state.LastUpdateTime = plan.state.EndTime
			plan.state.NodeTimings = run.snapshotNodeTimings()
			plan.state.Timeline = run.snapshotTimeline()
			return w.store.SaveExecutionState(loopCtx, tenant, plan.state, handle.FenceToken)
		})
		if err != nil {
			_ = w.lease.Release(loopCtx, handle)
			return fmt.Errorf("cancelled persist: %w", err)
		}
	case execErr != nil:
		ne := NormalizeEngineError(execErr, run.failedNode, run.nodeRetries)
		if err := run.finalize(func() error {
			return w.persistErrorTerminal(loopCtx, tenant, handle.FenceToken, plan.state, run, ne)
		}); err != nil {
			_ = w.lease.Release(loopCtx, handle)
			return fmt.Errorf("error terminal persist: %w", err)
		}
	default:
		w.logger.Info("execution completed", "execution_id", task.ExecutionID)
		err := run.finalize(func() error {
			run.recordTimeline("run_end", map[string]any{"final": true})
			plan.state.Context = w.buildCheckpoint(run, plan.state.Context)
			plan.state.Status = StatusCompleted
			plan.state.EndTime = nowISO()
			plan.state.LastUpdateTime = plan.state.EndTime
			plan.state.NodeTimings = run.snapshotNodeTimings()
			plan.state.Timeline = run.snapshotTimeline()
			return w.store.SaveExecutionState(loopCtx, tenant, plan.state, handle.FenceToken)
		})
		if err != nil {
			_ = w.lease.Release(loopCtx, handle)
			return fmt.Errorf("completed persist: %w", err)
		}
	}

	if err := w.lease.Release(loopCtx, handle); err != nil {
		w.logger.Warn("lease release failed", "execution_id", task.ExecutionID, "error", err)
	}
	return nil
}

// persistErrorTerminal writes the error terminal state (§5.2 error branch,
// §5.4 normalized error structure). Called under run.finalize.
func (w *Worker) persistErrorTerminal(ctx context.Context, tenant string, fenceGen int64,
	state *ExecutionState, run *executionRun, ne *NormalizedError) error {
	if run != nil {
		run.recordTimeline("run_end", map[string]any{"final": true})
		state.NodeTimings = run.snapshotNodeTimings()
		state.Timeline = run.snapshotTimeline()
		state.Context = w.buildCheckpoint(run, state.Context)
	}
	state.Status = StatusError
	state.EndTime = nowISO()
	state.LastUpdateTime = state.EndTime
	state.Error = terminalErrorState(ne)
	return w.store.SaveExecutionState(ctx, tenant, state, fenceGen)
}

// progressHeartbeat merges the current node_timings snapshot + fresh
// last_update_time into the running row (§8.1 layer 3). Failures only warn;
// a finished run never saves.
func (w *Worker) progressHeartbeat(ctx context.Context, tenant string, fenceGen int64, state *ExecutionState, run *executionRun) error {
	return run.heartbeatSave(func() error {
		st := *state
		st.LastUpdateTime = nowISO()
		st.NodeTimings = run.snapshotNodeTimings()
		st.Timeline = run.snapshotTimeline()
		return w.store.SaveExecutionState(ctx, tenant, &st, fenceGen)
	})
}

// leaseConflictDisposition implements §3.5: decision carriers are never
// acked; otherwise ack only when the holder's registry entry proves a live
// running worker. Everything else stays pending for reclaim.
func (w *Worker) leaseConflictDisposition(ctx context.Context, task *Task) error {
	if task.SuspendedResumeChain {
		w.logger.Warn("lease conflict on a decision resume carrier, leaving pending",
			"execution_id", task.ExecutionID, "resume_type", task.ResumeType)
		return fmt.Errorf("%w: decision carrier not acked", ErrExecutionLeaseConflict)
	}
	holder, err := w.lease.GetHolder(ctx, task.TenantID, task.ExecutionID)
	if err != nil {
		return fmt.Errorf("lease conflict probe: %w", err)
	}
	if holder == "" {
		// The lease vanished between acquire and probe: leave pending, the
		// next reclaim pass takes it cleanly.
		return fmt.Errorf("%w: lease vanished during conflict check", ErrExecutionLeaseConflict)
	}
	instanceID, ok := HolderInstanceID(holder)
	if !ok {
		return fmt.Errorf("%w: holder %q unparsable", ErrExecutionLeaseConflict, holder)
	}
	status, err := w.registry.LookupStatus(ctx, instanceID)
	if err != nil {
		// Probe failure → conservatively keep the message (§2.4 guard rule).
		return fmt.Errorf("holder liveness probe failed: %w", err)
	}
	if status == RegStatusRunning {
		// Live holder: ack to avoid burning deliveries against a busy
		// execution (§3.5 point 4).
		w.logger.Info("lease held by live worker, acking message",
			"execution_id", task.ExecutionID, "holder", instanceID)
		return nil
	}
	return fmt.Errorf("%w: holder %q not running (status=%q)", ErrExecutionLeaseConflict, instanceID, status)
}

// handleResume — MVP boundary: the suspended/resume carrier semantics
// (A47-F2) are phase 2. A resume task never silently disappears: cancellation
// intent still wins (§4.2), and everything else fails deterministically with
// an error terminal on the target execution.
func (w *Worker) handleResume(ctx context.Context, task *Task) error {
	tenant := task.TenantID
	executionID := task.ExecutionID

	holder := NewHolderToken("resume", w.cfg.InstanceID)
	handle, err := w.lease.Acquire(ctx, tenant, executionID, holder, w.cfg.LeaseTTLSeconds)
	if err != nil {
		if errors.Is(err, ErrExecutionLeaseConflict) {
			return w.leaseConflictDisposition(ctx, task)
		}
		return fmt.Errorf("resume lease acquire: %w", err)
	}
	defer func() { _ = w.lease.Release(ctx, handle) }()

	// Cancellation priority (§4.2 #4): a cancel flag on the target
	// execution terminalizes cancelled regardless of carrier type.
	if hit, _, err := w.store.IsCancelRequested(ctx, tenant, executionID); err != nil {
		return fmt.Errorf("cancel probe: %w", err)
	} else if hit {
		state, err := w.store.LoadExecutionState(ctx, tenant, executionID)
		if err != nil {
			return err
		}
		if state == nil {
			w.logger.Warn("resume for missing execution with cancel flag, acking",
				"execution_id", executionID)
			return nil
		}
		now := nowISO()
		state.Status = StatusCancelled
		state.EndTime = now
		state.LastUpdateTime = now
		if err := w.store.SaveExecutionState(ctx, tenant, state, handle.FenceToken); err != nil {
			return err
		}
		w.logger.Info("resume terminalized cancelled execution", "execution_id", executionID)
		return nil
	}

	state, err := w.store.LoadExecutionState(ctx, tenant, executionID)
	if err != nil {
		return err
	}
	if state == nil {
		w.logger.Warn("resume for missing execution, poison ack",
			"execution_id", executionID, "resume_type", task.ResumeType)
		return nil
	}
	ne := WorkerError("resume_not_supported", flowErrorCode,
		fmt.Sprintf("resume carrier (type=%s) not supported by plaita-go worker MVP; suspended/resume semantics are phase 2 (A47-F2)", task.ResumeType))
	if err := w.persistErrorTerminal(ctx, tenant, handle.FenceToken, state, nil, ne); err != nil {
		return err
	}
	w.logger.Warn("resume not supported, deterministic error terminal written",
		"execution_id", executionID, "resume_type", task.ResumeType)
	return nil
}

// deadLetterGuard is the MVP guard (§9.2): schema rejections pass; missing /
// terminal state passes; a running/suspended state with an active lease
// skips; anything else passes WITHOUT the recovery-clone re-enqueue (phase
// 2). Load failures skip (conservative).
func (w *Worker) deadLetterGuard(ctx context.Context, task *Task, reason string) bool {
	if reason == ReasonUnsupportedSchema {
		return true
	}
	state, err := w.store.LoadExecutionState(ctx, task.TenantID, task.ExecutionID)
	if err != nil {
		w.logger.Warn("dead letter guard: state load failed, skipping", "error", err)
		return false
	}
	if state == nil || TerminalStatuses[state.Status] {
		return true
	}
	holder, err := w.lease.GetHolder(ctx, task.TenantID, task.ExecutionID)
	if err != nil {
		w.logger.Warn("dead letter guard: lease probe failed, skipping", "error", err)
		return false
	}
	if holder != "" {
		w.logger.Warn("dead letter guard: lease active, skipping", "execution_id", task.ExecutionID)
		return false
	}
	// Phase 2 would re-enqueue a recovery clone here (§9.2 guard last row).
	return true
}

// newRunningState seeds the state row for a start task (§5.1 field set).
func (w *Worker) newRunningState(task *Task, executionID string, def *FlowDefinition) *ExecutionState {
	now := nowISO()
	state := &ExecutionState{
		ExecutionID:    executionID,
		FlowID:         task.FlowID,
		TenantID:       task.TenantID,
		Status:         StatusRunning,
		StartTime:      now,
		LastUpdateTime: now,
		Invoker:        "worker",
		EngineVersion:  "plaita-go " + version.Info(),
		Context:        map[string]any{"$INPUT": task.Params, "$FLOW_ID": task.FlowID},
	}
	if def != nil {
		state.FlowVersion = def.Version
		state.FlowHash = def.Hash
		state.FlowHashAlgo = def.HashAlgo
		if def.Workflow != nil {
			state.FlowName = def.Workflow.Name
		}
	}
	return state
}

// buildCheckpoint maps the engine runtime context into the checkpoint dict
// (§5.1 context: `$INPUT/$NODE/$LAST_NODE/$FLOW_ID/$ENV…`).
func (w *Worker) buildCheckpoint(run *executionRun, seed map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range seed {
		out[k] = v
	}
	if ctx, ok := run.rootRuntime.(context.Context); ok && ctx != nil {
		rc := runtime.WorkflowRuntimeContext(ctx)
		if rc != nil {
			out["$INPUT"] = rc.WorkflowInput
			out["$NODE"] = rc.CurrentNodeContext
			out["$LAST_NODE"] = string(rc.LastNodeID)
			out["$ENV"] = rc.EnvVariables
			out["$GLOBAL"] = rc.GlobalVariables
			if len(rc.ExecuteNodeErrors) > 0 {
				out["$ERROR"] = rc.ExecuteNodeErrors
			}
		}
	}
	out["$FLOW_ID"] = string(run.rootFlow.ID)
	return out
}

// queueWaitMillis parses the producer timestamp (naive local wall clock,
// §2.3) into a queue wait in milliseconds. Unparsable → not reported.
func queueWaitMillis(ts string) (float64, bool) {
	if ts == "" {
		return 0, false
	}
	layouts := []string{
		"2006-01-02T15:04:05.999999",
		"2006-01-02T15:04:05",
		time.RFC3339Nano,
		time.RFC3339,
	}
	for _, l := range layouts {
		if t, err := time.ParseInLocation(l, ts, time.Local); err == nil {
			return float64(time.Since(t).Milliseconds()), true
		}
	}
	return 0, false
}

// newExecutionID mints a uuid4 hex (§2.3: uuid4 hex form).
func newExecutionID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return hex.EncodeToString(b[:])
}
