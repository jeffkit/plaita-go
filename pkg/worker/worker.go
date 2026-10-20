package worker

import (
	"context"
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/internal/version"
	"github.com/jeffkit/plaita-go/pkg/sandbox"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// Consumer loop and execution write-back points (protocol §4, §5, §8, §9.2).
// The disposition table:
//
//	handler returned nil          → terminal state persisted → ack (+XDEL)
//	handler returned error        → transient/deliberate leave-pending
//	node retryable failure        → backoff + re-enqueue a fresh carrier → ack
//	lease conflict                → §3.5 ack-safe disposition
//	payload/schema/max-deliveries → DLQ + ack (queue layer, guarded)
//
// Phase-2 surfaces implemented here (M4):
//
//	orphan watch (§3.4/§8.2)      — after a lease loss only progress
//	                                heartbeats are published, so keeper never
//	                                kills a still-running execution at 1800s.
//	resume carriers (A47-F2)      — decision carriers are never acked in the
//	                                lease-conflict window; terminal executions
//	                                short-circuit idempotently; suspended +
//	                                continue is a no-op short-circuit (cancel
//	                                intent wins) that re-dispatches a pending
//	                                suspend service task when owed.
//	dispatch ledger (§6.3)        — append-only decision stream + the full
//	                                TaskNotForThisWorker handover (bounded by
//	                                affinityhandover counter keys, no DLQ).
//	DLQ replay + guard (§2.4/§9.2) — see dlqreplay.go; the dead-letter guard
//	                                re-enqueues a recovery clone and
//	                                terminalizes budget-exhausted runs.
//	drain (§9.3)                  — D1 draining / D2 force-stop / D3 signals,
//	                                see drain.go.
//	node retry budgets (§9.1)     — per-node/refetch/nofail counters, see
//	                                retry.go.
//
// M6 checkpoint v1.5 (2026-10-19, this build):
//
//	suspend    — a node declaring IsSuspending() parks the flow; the engine
//	             registers its wake-up subscription, snapshots the context
//	             (cycle-safe / depth-capped) and writes
//	             {ns}:execution:checkpoint:{id}. The worker writes the
//	             status=suspended row (§5.2).
//	resume     — event/timeout carriers restore the checkpoint, dispatch to
//	             the node's Resume(event|timeout|cancel) and keep stepping.
//	             Suspended + continue/retry stays a no-op carrier.
//	wake-ups   — Redis Pub/Sub {ns}:events:* plus the subscription timeout
//	             index enqueue the decision carriers (see eventbus.go).
//
// Still out of scope (phase 3):
//
//	TODO(phase-3): per-step state persistence (PERSIST_EVERY_N_STEPS=1, §5.2)
//	        — the progress heartbeat (ttl/3) stands in for it.
//
// Wave 3 (2026-10-20, issues #29/#30/#31):
//
//	age gate (#29)   — carriers past PLAITA_TASK_MAX_AGE_SECONDS are acked
//	                   without executing (dual-source age: payload timestamp
//	                   vs Stream ID ms, max wins), see agegate.go.
//	start idempotency
//	                 — a start carrier for an execution that already has a
//	                   state row (running/suspended/terminal) is an idempotent
//	                   ack; terminal rows are never overwritten and duplicate
//	                   starts never reach the DLQ; an id owned by another flow
//	                   is poison-acked (ExecutionIdConflict parity), see
//	                   handleStart.
//	tenant quota (#30)
//	                 — per-tenant start semaphore (ZSET + TTL, Lua admit),
//	                   over-quota starts deferred to the stream tail, slot
//	                   released at the terminal-write choke point, see
//	                   quota.go.
//	metrics (#31)    — the Metrics interface (no-op default, host-injectable)
//	                   instrumented at the key path points, see metrics.go.

//	Landed since M6: the node-retry carrier conversion (§9.1 #91) — a
//	        retryable failure re-enqueues a resume/continue carrier whenever a
//	        state row exists (retry.go), after snapshotting a failure-boundary
//	        checkpoint; the continue arrival path advances the live execution
//	        (continueRunning) through the engine's mid-flow entry
//	        (Workflow.ResumeFrom) whenever a usable checkpoint exists,
//	        falling back to a flow-start re-entry (with a timeline event)
//	        only when it does not (§9.1 fresh-run parity).

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

// Worker is the runtime worker.
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
	ledger   *DispatchLedger
	retries  *RetryBudgets
	quota    *TenantQuota
	// metrics is the #31 observability face; always non-nil (no-op default,
	// host-injectable via SetMetrics).
	metrics Metrics

	// M6 checkpoint v1.5: resumable snapshots + suspended-node wake-up
	// subscriptions (§9.2 decision carriers); #32 per-step checkpoints ride
	// the same store (kind=per-step) as the mid-flow resume position.
	checkpoints *RedisCheckpointStore
	eventSubs   *EventSubscriptions

	active       atomic.Int64
	runCtx       context.Context
	runCtxCancel context.CancelFunc
	wg           sync.WaitGroup
	started      atomic.Bool

	drainState
	orphanMu sync.Mutex
	orphans  map[string]*orphanEntry
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
		ledger:   NewDispatchLedger(rdb, cfg, logger),
		retries:  NewRetryBudgets(rdb, cfg, logger),
		quota:    NewTenantQuota(rdb, cfg, logger),
		metrics:  NoopMetrics{},
		orphans:  map[string]*orphanEntry{},

		checkpoints: NewRedisCheckpointStore(rdb, cfg),
		eventSubs:   NewEventSubscriptions(rdb, cfg, logger),
	}
	w.drainState.activeRuns = map[*executionRun]context.CancelFunc{}
	w.queue.SetDeadLetterGuard(w.deadLetterGuard)
	// #30: the execution store's terminal-write choke point releases the
	// tenant start slot (suspended keeps it; the entry TTL backstops a crash).
	w.store.SetOnTerminalSave(func(ctx context.Context, tenantID, executionID string) {
		w.quota.Release(ctx, tenantID, executionID)
	})
	w.registerEngineListeners()
	return w
}

// SetMetrics injects the host's metrics collector (#31). A nil argument keeps
// the no-op default. Call before Run; safe afterwards for collectors that are
// themselves concurrency-safe.
func (w *Worker) SetMetrics(m Metrics) {
	if m == nil {
		m = NoopMetrics{}
	}
	w.metrics = m
	w.queue.SetMetrics(m)
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

// Run consumes until ctx is cancelled or a drain completes. Registry
// lifecycle: starting → running → (drain) draining → stopping → unregistered
// (§8.1, §9.3).
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

	w.wg.Add(5)
	go func() {
		defer w.wg.Done()
		w.registry.HeartbeatLoop(ctx)
	}()
	go func() {
		defer w.wg.Done()
		w.sweepLoop(ctx)
	}()
	go func() {
		defer w.wg.Done()
		w.orphanLoop(ctx)
	}()
	go func() {
		defer w.wg.Done()
		w.eventBridgeLoop(ctx)
	}()
	go func() {
		defer w.wg.Done()
		w.eventTimeoutLoop(ctx)
	}()
	defer func() {
		w.runCtxCancel()
		w.wg.Wait()
		w.stopDrainTimer()
		w.registry.SetStatus(context.Background(), RegStatusStopping)
		w.registry.Unregister(context.Background())
	}()

	backoff := time.Duration(0)
	for {
		// §9.3 D1: the loop exits at the task boundary while draining; the
		// synchronous ProcessOne means every in-flight step is finished (or
		// was force-cancelled) by the time we get here.
		if w.Draining() {
			w.logger.Info("draining: consumption loop stopped", "inflight", w.active.Load())
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		// #31: queue_depth gauge (XLEN is O(1); sampling errors never break
		// the loop).
		if n, err := w.queue.Depth(ctx); err == nil {
			w.metrics.SetGauge(MetricQueueDepth, float64(n), nil)
		}
		handled, err := w.ProcessOne(ctx)
		if err != nil {
			// Transient family: warn + backoff, never blow up the loop (§2.4).
			// A D2 force-stop abandons the in-flight step; the message stays
			// pending and no retry/re-enqueue is attempted (A46-06).
			if errors.Is(err, ErrDrainAborted) || w.ForceStopped() {
				w.logger.Warn("task abandoned by drain force-stop (message left pending)", "error", err)
				continue
			}
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
	// §9.3 D1 post-read gate: a message delivered after draining began is
	// neither dispatched nor acked — it stays in the PEL for another worker.
	if w.Draining() || w.ForceStopped() {
		w.logger.Info("message delivered after drain began, leaving pending",
			"id", delivery.Task.MessageID)
		return true, nil
	}
	task := delivery.Task
	w.active.Add(1)
	w.registry.SetActiveTasks(ctx, int(w.active.Load()))
	defer func() {
		w.active.Add(-1)
		w.registry.SetActiveTasks(context.Background(), int(w.active.Load()))
	}()

	// Capacity-routed dispatch target check (§6.3, §9.2): a foreign target
	// goes through the bounded handover protocol; it is never dead-lettered
	// by this worker.
	if task.TargetInstanceID != "" && task.TargetInstanceID != w.cfg.InstanceID {
		return w.handleForeignTarget(ctx, delivery)
	}

	// #29 age gate: carriers past the horizon are acked without executing
	// and the drop is observable (log + counter + live execution event).
	// Placement mirrors Python's dispatch gate: after routing/handover (the
	// receiving worker re-checks its own copy) and before the quota gate (an
	// expired carrier must not consume a slot) and before any execution.
	if w.carrierTooOld(task, delivery.Envelope.MessageID) {
		w.metrics.IncCounter(MetricTaskExpiredTotal, metricLabels("tenant", task.TenantID, "type", task.Type))
		w.logger.Warn("task past the age horizon, acked without executing",
			"type", task.Type, "execution_id", task.ExecutionID,
			"flow_id", task.FlowID, "message_id", task.MessageID,
			"age_seconds", taskAgeOf(task, delivery.Envelope.MessageID))
		w.publishExecutionEvent(ctx, task.ExecutionID, map[string]any{
			"type": "task_expired", "at": nowISO(),
			"age_seconds": taskAgeOf(task, delivery.Envelope.MessageID),
		})
		return true, w.queue.Ack(ctx, task.MessageID)
	}

	switch task.Type {
	case "start":
		if err := w.handleStart(ctx, task); err != nil {
			// #30: an over-quota start is deferred to the stream tail (never
			// executed, never dead-lettered); the enqueue→ack happens there.
			if errors.Is(err, ErrTenantQuotaDeferred) {
				return w.deferOverQuotaStart(ctx, delivery)
			}
			return w.dispositionHandlerError(ctx, delivery, err)
		}
	case "resume":
		if err := w.handleResume(ctx, task); err != nil {
			return w.dispositionHandlerError(ctx, delivery, err)
		}
	default:
		// Unknown type → ValueError class → poison ack (§2.3, §9.2).
		w.logger.Warn("unknown task type, poison ack", "type", task.Type, "id", task.MessageID)
	}
	return true, w.queue.Ack(ctx, task.MessageID)
}

// dispositionHandlerError maps a handler error to the §9.2 disposition. Only
// the node-retryable class is acked (after enqueueing a fresh carrier); every
// other error leaves the message pending.
func (w *Worker) dispositionHandlerError(ctx context.Context, delivery *Delivery, err error) (bool, error) {
	var nre *NodeRetryableError
	if errors.As(err, &nre) {
		return w.requeueRetryableFailure(ctx, delivery, nre)
	}
	return true, err
}

// handleStart executes a start task: lease acquire (BEFORE the running row,
// §3.4) → idempotency gate (#29) → cancel entry checkpoint (§4.2 #1) →
// tenant quota admit (#30) → running row → engine execution → terminal
// write-back (§5.2) → release.
func (w *Worker) handleStart(ctx context.Context, task *Task) error {
	tenant := task.TenantID
	executionID := task.ExecutionID
	if executionID == "" {
		executionID = newExecutionID() // legacy messages: mint in place (§2.3)
		task.ExecutionID = executionID
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
	w.metrics.IncCounter(MetricLeaseAcquired, metricLabels("tenant", tenant))

	// #29 start idempotency gate: a carrier for an execution that already has
	// a state row is an idempotent ack — running rows are never re-run from a
	// duplicate start (the lease only blocks *simultaneous* double runs, this
	// gate blocks the *sequential* ones), terminal rows are never overwritten,
	// suspended executions wait for their decision carrier, and duplicate
	// starts never reach the DLQ (tombstone semantics alignment: "重复 start
	// 不进 DLQ"). An id already owned by ANOTHER flow is the Python
	// ExecutionIdConflictError parity: poison ack (redelivery cannot self-heal).
	if prev, perr := w.store.LoadExecutionState(ctx, tenant, executionID); perr != nil {
		_ = w.lease.Release(ctx, handle)
		return fmt.Errorf("state probe: %w", perr)
	} else if prev != nil {
		if prev.FlowID != "" && task.FlowID != "" && prev.FlowID != task.FlowID {
			_ = w.lease.Release(ctx, handle)
			w.logger.Error("execution_id already belongs to another flow; duplicate start poison-acked",
				"execution_id", executionID, "carrier_flow", task.FlowID, "stored_flow", prev.FlowID)
			w.metrics.IncCounter(MetricTaskDedupHitTotal, metricLabels("tenant", tenant, "reason", "flow_conflict"))
			return nil
		}
		_ = w.lease.Release(ctx, handle)
		w.metrics.IncCounter(MetricTaskDedupHitTotal, metricLabels("tenant", tenant, "status", prev.Status))
		if prev.Status == StatusSuspended {
			w.logger.Info("start carrier for a suspended execution, idempotent ack (use a resume carrier to wake it)",
				"execution_id", executionID)
		} else {
			w.logger.Info("duplicate start for an existing execution, idempotent ack (no overwrite)",
				"execution_id", executionID, "status", prev.Status)
		}
		return nil
	}

	// A brand-new execution id must not inherit retry budgets from a previous
	// life of the same id (§8.3. 删除执行时一并清). An existing row (retry
	// re-enqueue / duplicate carrier) keeps its counters.
	_ = w.retries.ResetAll(ctx, tenant, executionID)
	// A brand-new execution id must not inherit a stale checkpoint from a
	// previous life of the same id.
	w.deleteCheckpoint(ctx, tenant, executionID)

	// Entry checkpoint (§4.2 #1): cancel intent before the running row exists.
	// (Reached only for brand-new executions — the #29 gate above consumed the
	// existing-row cases, so a duplicate start can no longer overwrite a row
	// via the cancel path; a live run sees the flag through its poller and a
	// suspended one through its cancel resume carrier.)
	if hit, _, err := w.store.IsCancelRequested(ctx, tenant, executionID); err != nil {
		return fmt.Errorf("cancel probe: %w", err)
	} else if hit {
		return w.persistCancelAtEntry(ctx, tenant, executionID, task)
	}

	// #30 tenant quota admit: after the idempotency gate (a duplicate must not
	// retake a slot the live execution already holds), before the running row
	// (a denied start never executes). Unconfigured tenants never touch Redis
	// here (default = unlimited). A quota_bypass carrier (defer budget
	// exhausted) skips the gate entirely — liveness over the soft quota.
	if !task.QuotaBypass {
		if allowed, running := w.quota.AdmitStart(ctx, tenant, executionID); !allowed {
			_ = w.lease.Release(ctx, handle)
			w.metrics.IncCounter(MetricQuotaExceededTotal, metricLabels("tenant", tenant))
			w.metrics.SetGauge(MetricQuotaRunning, float64(running), metricLabels("tenant", tenant))
			w.logger.Info("tenant start quota full, deferring the carrier",
				"tenant", tenant, "execution_id", executionID, "running", running)
			return ErrTenantQuotaDeferred
		} else if running > 0 {
			w.metrics.SetGauge(MetricQuotaRunning, float64(running), metricLabels("tenant", tenant))
		}
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
	// resume, when non-nil, turns the engine call into Workflow.Resume
	// (suspended → event/timeout/cancel carrier) instead of Workflow.Execute.
	resume *resumeInput
	// midflow, when non-nil, turns the engine call into Workflow.ResumeFrom
	// (#32): a continue/retry carrier advancing a live execution enters at
	// the checkpoint's recorded position instead of replaying from the flow
	// start. Only set for a usable mid-flow checkpoint (kind=per-step or a
	// legacy continue-typed snapshot).
	midflow *workflow.Checkpoint
	// continuation marks a continue/retry carrier advancing a live (running)
	// execution (§9.1 conversion target); the row's node_timings/timeline are
	// carried forward so the terminal row reads as one execution (§5.1).
	continuation bool
}

// resumeInput carries the checkpoint + carrier for a resume execution.
type resumeInput struct {
	checkpoint *workflow.Checkpoint
	resumeType string
	data       map[string]any
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
	// #31: exec_duration / exec_result labels — low cardinality only (tenant,
	// flow); execution_id is deliberately never a label.
	execLabels := metricLabels("tenant", tenant, "flow", task.FlowID)
	execStarted := time.Now()
	emitResult := func(status string) {
		w.metrics.IncCounter(MetricExecResult, metricLabels("tenant", tenant, "flow", task.FlowID, "status", status))
		w.metrics.ObserveDuration(MetricExecDuration, time.Since(execStarted), execLabels)
	}

	run := &executionRun{
		owner:       w,
		executionID: task.ExecutionID,
		tenantID:    tenant,
		rootFlow:    def.Workflow,
		nodeTimings: map[string]map[string]any{},
		enqueuedAt:  task.Timestamp,
	}
	if plan.resume != nil || plan.continuation {
		// A resume/continuation is a continuation of the same execution:
		// carry the previous row's node timings + timeline forward so the
		// terminal write does not erase the earlier history (§5.1).
		seedRunFromState(run, plan.state)
	}
	if wait, ok := queueWaitMillis(task.Timestamp); ok {
		run.queueWaitMS = &wait
	}
	if plan.resume != nil || plan.continuation {
		// #31: wake latency — enqueue→execution of any carrier advancing an
		// existing execution (event/timeout decision carrier or a converted
		// continue), the observable half of "挂起/受挫 → 唤醒".
		if wait, ok := queueWaitMillis(task.Timestamp); ok {
			w.metrics.ObserveDuration(MetricEventWakeLatency,
				time.Duration(wait)*time.Millisecond,
				metricLabels("tenant", tenant, "resume_type", task.ResumeType))
		}
	}

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	// loopCtx outlives runCtx: the terminal persist + lease release must run
	// on the consumer-loop context, since runCtx is cancelled as soon as the
	// engine returns (by cancellation, lease loss or plain completion).
	loopCtx := ctx
	ctx = withRun(runCtx, run)
	// M6: the engine writes the checkpoint and registers the wake-up
	// subscription at a suspend boundary; the worker owns both stores.
	// #32: StepCheckpoint=1 (§5.2 PERSIST_EVERY_N_STEPS=1) also records the
	// live execution's position after every completed root node, so a
	// continue/retry carrier enters mid-flow instead of replaying.
	ctx = workflow.WithSuspendOptions(ctx, &workflow.SuspendOptions{
		ExecutionID:    task.ExecutionID,
		TenantID:       tenant,
		FlowID:         task.FlowID,
		Store:          w.checkpoints,
		Subscriptions:  w.eventSubs,
		StepCheckpoint: 1,
	})
	// The real execution id for the engine context ($EXECUTION_ID): the
	// sandbox scope reads it via sandbox.ExecutionIDFromContext, the engine
	// runtime seeds $EXECUTION_ID from the same value.
	ctx = sandbox.WithExecutionID(ctx, task.ExecutionID)
	ctx = runtime.WithExecutionID(ctx, task.ExecutionID)
	w.registerRun(run, cancelRun)
	defer w.unregisterRun(run)
	defer w.dropOrphan(task.ExecutionID, "run ended")
	// §9.1: any node that completed successfully clears its retry counter
	// (and, on a fully successful run, the consecutive-failure counters).
	defer w.resetRetryProgress(loopCtx, tenant, task.ExecutionID, run)

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
					// §3.4 失租：**先发布一次进度**（执行可能仍在推进——不让
					// keeper 在「租约已丢但 impl 仍在跑」的窗口里判死健康 run），
					// 再标记 lease_lost、请求中止当前步，并转交 orphan watch
					// （只发心跳，不续租、不再 cancel）。
					w.logger.Error("execution lease lost mid-run, publishing orphan progress and aborting step",
						"execution_id", task.ExecutionID)
					w.publishOrphanProgress(tenant, handle.FenceToken, &orphanEntry{
						executionID: task.ExecutionID,
						tenantID:    tenant,
						fenceToken:  handle.FenceToken,
						run:         run,
					})
					w.registerOrphan(&orphanEntry{
						executionID: task.ExecutionID,
						tenantID:    tenant,
						fenceToken:  handle.FenceToken,
						run:         run,
					})
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

	// §9.1 fresh-run parity made observable (#32): a continue/retry carrier
	// whose execution has no usable mid-flow checkpoint degrades to a
	// flow-start re-entry — record that in the timeline so operators can see
	// the fallback (and its replayed predecessors) instead of reading the
	// silence as a mid-flow continuation.
	if plan.continuation && plan.midflow == nil {
		ev := run.recordTimeline("continue_fallback_start", map[string]any{
			"reason": "no mid-flow checkpoint; re-entering at flow start",
		})
		w.publishExecutionEvent(ctx, run.executionID, ev)
	}

	var result any
	var execErr error
	switch {
	case plan.resume != nil:
		result, execErr = def.Workflow.Resume(ctx, plan.resume.checkpoint, plan.resume.resumeType, plan.resume.data)
	case plan.midflow != nil:
		// #32 mid-flow entry: continue the live execution at the checkpoint's
		// recorded position; completed predecessors are not replayed. A
		// REFUSED entry (stale checkpoint, guard rejection, unrestorable
		// snapshot) happens strictly before any node executes, so the issue's
		// fallback applies: re-enter at the flow start with the fallback on
		// the record (§9.1 fresh-run parity).
		result, execErr = def.Workflow.ResumeFrom(ctx, plan.midflow, "")
		if midFlowEntryRefused(execErr) {
			ev := run.recordTimeline("continue_fallback_start", map[string]any{
				"reason": fmt.Sprintf("mid-flow entry refused: %v", execErr),
			})
			w.publishExecutionEvent(ctx, run.executionID, ev)
			result, execErr = def.Workflow.Execute(ctx, task.Params)
		}
	default:
		result, execErr = def.Workflow.Execute(ctx, task.Params)
	}
	susp := workflow.AsSuspend(execErr)
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
		w.metrics.IncCounter(MetricLeaseLost, metricLabels("tenant", tenant))
		_ = w.lease.Release(loopCtx, handle)
		return ErrLeaseLost
	case cancelHit:
		w.logger.Info("execution cancelled", "execution_id", task.ExecutionID)
		emitResult("cancelled")
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
		w.deleteCheckpoint(loopCtx, tenant, task.ExecutionID)
	case execErr != nil && run.drainAborted():
		// §9.3 D2: the step was abandoned by force-stop. Deliberately NO
		// terminal write — the execution stays resumable and the message
		// remains pending for another worker (the Python drain docstring's
		// "留 pending 由其他 worker 续跑" contract; this build has no hard
		// interrupt, so aborting the step is the faithful part). A flow that
		// finished cleanly before the force-stop keeps its completed row.
		w.logger.Warn("execution abandoned by drain force-stop; message left pending",
			"execution_id", task.ExecutionID)
		_ = w.lease.Release(loopCtx, handle)
		return ErrDrainAborted
	case plan.resume != nil && isResumeCarrierRejection(execErr):
		// A stale/invalid carrier (ResumeGuardError) or a failing node resume
		// handler (ResumeError) is not an execution failure: the run stays
		// suspended and the message must stay pending so the real decision
		// carrier can still arrive (§9.2 "execution stays suspended").
		run.markFinished()
		_ = w.lease.Release(loopCtx, handle)
		w.logger.Warn("resume carrier rejected; execution stays suspended, message left pending",
			"execution_id", task.ExecutionID, "resume_type", task.ResumeType, "error", execErr)
		return fmt.Errorf("resume carrier rejected: %w", execErr)
	case susp != nil:
		// §5.2 suspended: the node parked the flow after the engine wrote the
		// checkpoint + registered the wake-up subscription. The row keeps
		// status=suspended with no end_time; the lease is released so the
		// resume carrier can take it.
		w.logger.Info("execution suspended", "execution_id", task.ExecutionID,
			"node", string(susp.NodeID), "resume_type", "event")
		emitResult("suspended")
		err := run.finalize(func() error {
			return w.persistSuspended(loopCtx, tenant, handle, plan.state, run, susp)
		})
		if err != nil {
			_ = w.lease.Release(loopCtx, handle)
			return fmt.Errorf("suspended persist: %w", err)
		}
	case execErr != nil:
		terminalized, retryErr, err := w.classifyNodeFailure(loopCtx, tenant, handle, plan.state, run, execErr)
		if err != nil {
			_ = w.lease.Release(loopCtx, handle)
			return err
		}
		if !terminalized {
			// Budget left: hand the retry carrier decision to the consumer
			// loop, which performs the backoff + enqueue→ack choreography.
			emitResult("failed")
			run.markFinished()
			_ = w.lease.Release(loopCtx, handle)
			return retryErr
		}
		emitResult("failed")
	default:
		w.logger.Info("execution completed", "execution_id", task.ExecutionID)
		emitResult("completed")
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
		w.deleteCheckpoint(loopCtx, tenant, task.ExecutionID)
	}

	if err := w.lease.Release(loopCtx, handle); err != nil {
		w.logger.Warn("lease release failed", "execution_id", task.ExecutionID, "error", err)
	}
	return nil
}

// seedRunFromState carries the persisted timeline + node timings of a
// suspended execution into the resume run. The timeline sequence stays
// monotonic so the final row reads as one execution, not two.
func seedRunFromState(run *executionRun, state *ExecutionState) {
	if run == nil || state == nil {
		return
	}
	run.mu.Lock()
	defer run.mu.Unlock()
	for nodeID, raw := range state.NodeTimings {
		entry, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		clone := make(map[string]any, len(entry))
		for k, v := range entry {
			clone[k] = v
		}
		run.nodeTimings[nodeID] = clone
	}
	if state.Timeline == nil {
		return
	}
	run.dropped = toInt(state.Timeline["dropped"])
	events, _ := state.Timeline["events"].([]any)
	for _, raw := range events {
		event, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		run.events = append(run.events, event)
		if seq := toInt(event["seq"]); seq > int(run.seq) {
			run.seq = int64(seq)
		}
	}
}

// persistSuspended writes the §5.2 suspended row: no end_time, the engine's
// checkpoint as the context, and the node timings/timeline collected so far.
// The checkpoint itself was already written by the engine at the suspend
// boundary; it is re-saved through the worker store so the key layout is
// owned by the worker even if a different store was injected.
func (w *Worker) persistSuspended(ctx context.Context, tenant string, handle *LeaseHandle,
	state *ExecutionState, run *executionRun, susp *workflow.SuspendError) error {
	cp := susp.Checkpoint
	if cp == nil {
		return fmt.Errorf("%w: suspension carried no checkpoint", ErrStatePersist)
	}
	state.Status = StatusSuspended
	state.LastUpdateTime = nowISO()
	state.EndTime = ""
	state.Context = cp.Context
	state.Error = nil
	if run != nil {
		state.NodeTimings = run.snapshotNodeTimings()
		state.Timeline = run.snapshotTimeline()
	}
	if err := w.checkpoints.SaveCheckpoint(ctx, cp); err != nil {
		return err
	}
	return w.store.SaveExecutionState(ctx, tenant, state, handle.FenceToken)
}

// deleteCheckpoint removes the execution's checkpoint after a terminal write.
// A failure only warns: the state row is authoritative and the key expires
// with the execution's lifecycle (no checkpoint TTL is set while suspended).
func (w *Worker) deleteCheckpoint(ctx context.Context, tenant, executionID string) {
	if err := w.checkpoints.DeleteCheckpoint(ctx, tenant, executionID); err != nil {
		w.logger.Warn("checkpoint cleanup failed", "execution_id", executionID, "error", err)
	}
}

// resumeSuspended performs the real M6 wake-up: load the flow + checkpoint,
// mark the row running, and execute Workflow.Resume under the already-held
// lease (executeAndFinalize owns the terminal write and the release).
func (w *Worker) resumeSuspended(ctx context.Context, task *Task, state *ExecutionState, handle *LeaseHandle) error {
	tenant := task.TenantID
	flowID := task.FlowID
	if flowID == "" {
		flowID = state.FlowID
	}
	def, err := w.loadContinuationDefinition(ctx, task, state, flowID, handle.FenceToken)
	if err != nil || def == nil {
		// Deterministic terminal already written (nil def), or a transient
		// load failure that leaves the message pending (non-nil err).
		return err
	}

	checkpoint, err := w.checkpoints.LoadCheckpoint(ctx, tenant, task.ExecutionID)
	if err != nil {
		return err
	}
	if checkpoint == nil {
		// Fallback: older workers only kept the checkpoint inside the state
		// row's context (§5.1). A context without $LAST_NODE cannot express a
		// resume position, so it is treated as missing.
		checkpoint = checkpointFromStateContext(state, task.ExecutionID)
	}
	if checkpoint == nil {
		w.logger.Error("resume: no checkpoint for suspended execution",
			"execution_id", task.ExecutionID, "resume_type", task.ResumeType)
		ne := WorkerError("resume_checkpoint_missing", flowErrorCode, fmt.Sprintf(
			"resume carrier (type=%s) cannot advance execution %s: no checkpoint found",
			task.ResumeType, task.ExecutionID))
		w.deleteCheckpoint(ctx, tenant, task.ExecutionID)
		return w.persistErrorTerminal(ctx, tenant, handle.FenceToken, state, nil, ne)
	}

	// The row stays suspended while the resume carrier is being applied: if
	// the carrier is stale (ResumeGuardError) or the node's resume handler
	// fails (ResumeError), the execution must remain resumable and the
	// message stays pending. A terminal outcome overwrites the status below.
	return w.executeAndFinalize(ctx, executePlan{
		task:   task,
		def:    def,
		handle: handle,
		state:  state,
		resume: &resumeInput{
			checkpoint: checkpoint,
			resumeType: task.ResumeType,
			data:       task.Data,
		},
	})
}

// continueRunning advances a live (running) execution carrying a
// continue/retry carrier — the arrival half of the §9.1 retry-carrier
// conversion (Python resume_flow: running + continue/retry share one stepping
// path, "必须真实重派"). Python resumes from its per-step checkpoint; this
// build enters the engine mid-flow through Workflow.ResumeFrom (#32) whenever
// a usable checkpoint exists (kind=per-step, or a legacy continue-typed
// snapshot): the execution continues at the checkpoint's next node and
// completed predecessors are NOT replayed. Without one the carrier degrades
// to the fresh-run parity of a flow-start re-entry (§9.1) and the fallback is
// recorded in the timeline (executeAndFinalize). The surrounding dispositions
// are the resume path's either way: flow-id gate, §7 fingerprint hard gate,
// history carry (executeAndFinalize seeding), checkpoint lifecycle and the
// §5.2 terminal write.
func (w *Worker) continueRunning(ctx context.Context, task *Task, state *ExecutionState, handle *LeaseHandle) error {
	tenant := task.TenantID
	flowID := task.FlowID
	if flowID == "" {
		flowID = state.FlowID
	}
	def, err := w.loadContinuationDefinition(ctx, task, state, flowID, handle.FenceToken)
	if err != nil || def == nil {
		return err
	}
	plan := executePlan{
		task:         task,
		def:          def,
		handle:       handle,
		state:        state,
		continuation: true,
	}
	if cp, cerr := w.checkpoints.LoadCheckpoint(ctx, tenant, task.ExecutionID); cerr != nil {
		// A real read failure stays pending; a missing checkpoint proceeds.
		return fmt.Errorf("continue checkpoint probe: %w", cerr)
	} else if midFlowEntryUsable(cp) {
		plan.midflow = cp
		w.logger.Info("continue carrier entering the execution mid-flow",
			"execution_id", task.ExecutionID, "resume_type", task.ResumeType,
			"checkpoint_node", string(cp.SuspendedNode), "entry_node", string(cp.NextNode))
	} else {
		w.logger.Warn("continue carrier resuming a live execution without a mid-flow checkpoint (fresh-run parity)",
			"execution_id", task.ExecutionID, "resume_type", task.ResumeType)
	}
	return w.executeAndFinalize(ctx, plan)
}

// midFlowEntryRefused reports whether a Workflow.ResumeFrom error means the
// mid-flow entry itself was refused BEFORE any node executed (guard rejection
// on a stale/inapplicable checkpoint, or an unrestorable snapshot) — the #32
// fallback then re-enters at the flow start. Node/flow failures after the
// entry are NOT refusals: they take the ordinary §9.1 retry classification.
func midFlowEntryRefused(err error) bool {
	if err == nil {
		return false
	}
	var guard *workflow.ResumeGuardError
	if errors.As(err, &guard) {
		return true
	}
	var snap *workflow.StateSnapshotError
	return errors.As(err, &snap)
}

// midFlowEntryUsable reports whether cp records a position Workflow.ResumeFrom
// may enter at: an engine per-step checkpoint (kind=per-step) or a legacy
// continue-typed snapshot (kind empty, resume_type=continue — written by
// workers before the kind field existed), and only when it carries an entry
// node. A suspend checkpoint is NOT usable: its position is a parked
// suspending node, and stepping past it would skip the pending state — that
// path belongs to Workflow.Resume.
func midFlowEntryUsable(cp *workflow.Checkpoint) bool {
	if cp == nil || cp.NextNode == "" {
		return false
	}
	switch cp.Kind {
	case workflow.CheckpointKindPerStep:
		return true
	case "":
		return cp.ResumeType == workflow.ResumeContinue
	default:
		return false
	}
}

// loadContinuationDefinition resolves the flow for a resume-path advance and
// enforces the §7 admission gates shared by both resume paths:
//
//   - flow-id mismatch (carrier vs stored row) → poison ack, row untouched
//     (Python _load_and_guard_resume_state: ValueError);
//   - flow definition missing → deterministic flow_not_found error terminal;
//   - definition changed since the start (fingerprint mismatch) →
//     flow_hash_mismatch error terminal, unless the carrier carries
//     allow_flow_hash_change (Python data.allow_flow_hash_change).
//
// A nil def with a nil error means a deterministic terminal was already
// written; a non-nil error leaves the message pending (transient).
func (w *Worker) loadContinuationDefinition(ctx context.Context, task *Task,
	state *ExecutionState, flowID string, fenceGen int64) (*FlowDefinition, error) {
	tenant := task.TenantID
	if state.FlowID != "" && flowID != "" && state.FlowID != flowID {
		w.logger.Warn("resume carrier flow_id does not match the stored row, poison ack",
			"execution_id", task.ExecutionID, "carrier_flow", flowID, "stored_flow", state.FlowID)
		return nil, nil
	}
	def, err := w.flows.GetFlowDefinition(ctx, tenant, flowID, state.FlowVersion)
	if err != nil {
		if errors.Is(err, ErrFlowNotFound) {
			w.logger.Error("resume: flow definition not loadable, error terminal",
				"flow_id", flowID, "version", state.FlowVersion, "error", err)
			ne := WorkerError("flow_not_found", flowErrorCode, err.Error())
			w.deleteCheckpoint(ctx, tenant, task.ExecutionID)
			return nil, w.persistErrorTerminal(ctx, tenant, fenceGen, state, nil, ne)
		}
		return nil, fmt.Errorf("resume flow load: %w", err)
	}

	// §7 flow-fingerprint hard gate: resuming a checkpoint against a changed
	// flow silently corrupts the run. The explicit opt-out is the carrier's
	// allow_flow_hash_change (Python data.allow_flow_hash_change).
	allowHashChange, _ := task.Data["allow_flow_hash_change"].(bool)
	if !allowHashChange && state.FlowHash != "" && def.Hash != "" && state.FlowHash != def.Hash {
		w.logger.Error("resume: flow definition changed since the checkpoint was written",
			"execution_id", task.ExecutionID,
			"stored_flow_hash", state.FlowHash, "current_flow_hash", def.Hash)
		ne := WorkerError("flow_hash_mismatch", flowErrorCode, fmt.Sprintf(
			"flow definition changed since execution %s started (stored=%s current=%s); "+
				"resume refused (set allow_flow_hash_change=true on the carrier to override)",
			task.ExecutionID, state.FlowHash, def.Hash))
		ne.Details = map[string]any{
			"stored_flow_hash":       state.FlowHash,
			"current_flow_hash":      def.Hash,
			"stored_flow_hash_algo":  state.FlowHashAlgo,
			"current_flow_hash_algo": def.HashAlgo,
			"upgrade_suspected":      state.FlowHashAlgo != "" && def.HashAlgo != "" && state.FlowHashAlgo != def.HashAlgo,
		}
		return nil, w.persistErrorTerminal(ctx, tenant, fenceGen, state, nil, ne)
	}
	return def, nil
}

// checkpointFromStateContext rebuilds a minimal checkpoint from a state row's
// context (legacy carriers). The dedicated checkpoint key is authoritative.
func checkpointFromStateContext(state *ExecutionState, executionID string) *workflow.Checkpoint {
	if state == nil || len(state.Context) == 0 {
		return nil
	}
	lastNode, _ := state.Context["$LAST_NODE"].(string)
	if lastNode == "" {
		return nil
	}
	return &workflow.Checkpoint{
		SchemaVersion: workflow.CheckpointSchemaVersion,
		ExecutionID:   executionID,
		TenantID:      state.TenantID,
		FlowID:        state.FlowID,
		SuspendedNode: workflow.NodeID(lastNode),
		ResumeType:    workflow.ResumeEvent,
		Context:       state.Context,
		CreatedAt:     nowISO(),
	}
}

// isResumeCarrierRejection reports the engine's resume-admission failures: a
// stale carrier (ResumeGuardError) or a node resume handler failure
// (ResumeError). Neither is a flow failure — the execution stays suspended.
func isResumeCarrierRejection(err error) bool {
	if err == nil {
		return false
	}
	var guard *workflow.ResumeGuardError
	if errors.As(err, &guard) {
		return true
	}
	var resumeErr *workflow.ResumeError
	return errors.As(err, &resumeErr)
}

// classifyNodeFailure applies the §9.1 node-retry decision to an engine

// terminalized=true  — the error terminal (normal OR budget-exhausted) was
//
//	written; the caller only releases the lease.
//
// terminalized=false — a retryable failure below budget; retryErr carries
//
//	the NodeRetryableError for the re-enqueue path.
func (w *Worker) classifyNodeFailure(ctx context.Context, tenant string, handle *LeaseHandle,
	state *ExecutionState, run *executionRun, execErr error) (bool, error, error) {
	if !isRetryableNodeFailure(execErr, run) {
		// Deterministic failure: record it (§9.1 layer 3: the counter is the
		// only convergence signal for non-retryable node failures) and write
		// the ordinary error terminal.
		if n, cerr := w.retries.RecordDeterministic(ctx, tenant, state.ExecutionID); cerr != nil {
			w.logger.Warn("deterministic failure counter increment failed", "error", cerr)
		} else if n >= DeterministicFailureMax {
			w.logger.Error("deterministic node failures reached the ceiling; execution stays terminalized",
				"execution_id", state.ExecutionID, "count", n, "max", DeterministicFailureMax)
		}
		ne := NormalizeEngineError(execErr, run.failedNode, run.nodeRetries)
		if err := run.finalize(func() error {
			return w.persistErrorTerminal(ctx, tenant, handle.FenceToken, state, run, ne)
		}); err != nil {
			return false, nil, fmt.Errorf("error terminal persist: %w", err)
		}
		return true, nil, nil
	}

	nodeID := run.failedNode
	attempt, err := w.retries.RecordNodeRetry(ctx, tenant, state.ExecutionID, nodeID)
	if err != nil {
		// Counter unreadable: treat as attempt 1 (never block a retry on
		// bookkeeping — §9.1 fallback semantics).
		w.logger.Warn("node retry counter increment failed (treated as attempt 1)", "error", err)
		attempt = 1
	}
	budget := w.cfg.MaxDeliveries
	total, terr := w.retries.RecordTotal(ctx, tenant, state.ExecutionID)
	if terr != nil {
		w.logger.Warn("total retry counter increment failed", "error", terr)
	}
	if attempt >= budget || (terr == nil && total >= NodeRetryTotalMax) {
		reason := fmt.Sprintf("node %s failed %d/%d times; retry budget exhausted: %v", nodeID, attempt, budget, execErr)
		if terr == nil && total >= NodeRetryTotalMax {
			reason = fmt.Sprintf("execution retried %d times (fallback ceiling %d); node %s keeps failing: %v",
				total, NodeRetryTotalMax, nodeID, execErr)
		}
		w.logger.Error("node retry budget exhausted, terminalizing error",
			"execution_id", state.ExecutionID, "node", nodeID, "attempt", attempt, "budget", budget, "total", total)
		ne := WorkerError("node_retry_budget_exhausted", DefaultNodeAbortCode, reason)
		ne.NodeRetries = attempt
		ne.FailedNode = nodeID
		if err := w.terminalizeNodeRetryExhausted(ctx, tenant, handle, state, run, ne, attempt); err != nil {
			return false, nil, fmt.Errorf("node retry exhausted persist: %w", err)
		}
		return true, nil, nil
	}

	w.logger.Warn("node retryable failure below budget, re-enqueueing after backoff",
		"execution_id", state.ExecutionID, "node", nodeID, "attempt", attempt, "budget", budget)
	// §9.1 conversion support: snapshot the execution at the failure boundary
	// so the converted resume carrier finds a resume position (M6 checkpoint
	// machinery applied at the failure boundary). Best-effort evidence — the
	// carrier conversion itself keys on the ROW's existence (Python
	// _requeue_body_for_retry), and a continue carrier without a checkpoint
	// still advances (fresh-run parity).
	w.saveFailureCheckpoint(ctx, tenant, state, run)
	return false, &NodeRetryableError{
		ExecutionID: state.ExecutionID,
		FailedNode:  nodeID,
		Attempt:     attempt,
		Budget:      budget,
		Err:         execErr,
	}, nil
}

// saveFailureCheckpoint snapshots the runtime context captured at flow end
// (rootRuntime holds the live context even for a failed attempt) into the
// execution's checkpoint key. The snapshot carries the completed-node results
// ($NODE), $LAST_NODE and the failure's $ERROR; kind=per-step with
// next_node=failed node marks it as a mid-flow entry (#32): the converted
// continue/retry carrier re-executes the failed node and steps on, instead of
// replaying the flow from its start. Snapshot/persist failures only warn:
// losing the evidence degrades the next carrier to fresh-run parity, never
// blocks the retry.
func (w *Worker) saveFailureCheckpoint(ctx context.Context, tenant string, state *ExecutionState, run *executionRun) {
	if run == nil {
		return
	}
	rootRuntime, _ := run.rootRuntime.(context.Context)
	if rootRuntime == nil {
		return
	}
	opts := &workflow.SuspendOptions{
		ExecutionID: state.ExecutionID,
		TenantID:    tenant,
		FlowID:      state.FlowID,
	}
	snap, err := workflow.SnapshotRuntimeContext(rootRuntime, opts)
	if err != nil {
		w.logger.Warn("failure checkpoint snapshot failed; retry carrier continues without one",
			"execution_id", state.ExecutionID, "error", err)
		return
	}
	lastNode := ""
	if rc := runtime.WorkflowRuntimeContext(rootRuntime); rc != nil {
		lastNode = string(rc.LastNodeID)
	}
	cp := &workflow.Checkpoint{
		SchemaVersion: workflow.CheckpointSchemaVersion,
		Kind:          workflow.CheckpointKindPerStep,
		ExecutionID:   state.ExecutionID,
		TenantID:      tenant,
		FlowID:        state.FlowID,
		SuspendedNode: workflow.NodeID(lastNode),
		ResumeType:    workflow.ResumeContinue,
		Context:       snap,
		CreatedAt:     nowISO(),
	}
	// Retry semantics: the failed node never completed, so the mid-flow entry
	// re-executes IT (not its successor). Without a known failed node there is
	// no entry position — the checkpoint stays without next_node and the next
	// carrier degrades to fresh-run parity (midFlowEntryUsable).
	if nodeID := run.failedNode; nodeID != "" {
		cp.SuspendedNode = workflow.NodeID(nodeID)
		cp.NextNode = workflow.NodeID(nodeID)
	}
	if err := w.checkpoints.SaveCheckpoint(ctx, cp); err != nil {
		w.logger.Warn("failure checkpoint persist failed; retry carrier continues without one",
			"execution_id", state.ExecutionID, "error", err)
	}
}

// resetRetryProgress clears the retry counters for every node that advanced
// (§9.1). The total/deterministic counters are cleared only when the run
// completed: a mid-flow continuation leaves the consecutive-failure ceilings
// alone until the whole execution reaches its terminal row.
func (w *Worker) resetRetryProgress(ctx context.Context, tenant, executionID string, run *executionRun) {
	if run == nil {
		return
	}
	succeeded := run.succeededNodeIDs()
	if len(succeeded) == 0 {
		return
	}
	if err := w.retries.ResetOnProgress(ctx, tenant, executionID, succeeded); err != nil {
		return
	}
	state, err := w.store.LoadExecutionState(ctx, tenant, executionID)
	if err != nil || state == nil || state.Status != StatusCompleted {
		return
	}
	_ = w.retries.ResetConsecutive(ctx, tenant, executionID)
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
	if err := w.store.SaveExecutionState(ctx, tenant, state, fenceGen); err != nil {
		return err
	}
	w.deleteCheckpoint(ctx, tenant, state.ExecutionID)
	return nil
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
		if task.Type == "resume" {
			// plaita#53: an acked resume means THIS resumption did not take
			// effect — say so, or operators read the silence as success.
			w.logger.Warn("resume carrier acked while a live holder owns the lease — this resume had no effect",
				"execution_id", task.ExecutionID, "resume_type", task.ResumeType, "holder", instanceID)
		}
		return nil
	}
	return fmt.Errorf("%w: holder %q not running (status=%q)", ErrExecutionLeaseConflict, instanceID, status)
}

// handleResume implements the A47-F2 carrier semantics (§3.5, §4.2 #5, §5.3,
// §9.2) on top of M6 checkpoint resume:
//
//   - terminal executions short-circuit idempotently (already_terminal);
//   - cancel intent (flag or resume_type=cancel) wins over every shortcut;
//   - suspended + continue/retry is a no-op short-circuit that takes NO lease,
//     except to compensate a pending suspend service dispatch (plaita#233);
//   - an event/timeout carrier on a *running* execution missed the suspend
//     point and is normalized to a continue carrier (enqueue → ack, #258);
//   - an event/timeout carrier on a *suspended* execution performs the real
//     wake-up: restore the checkpoint, call the node's Resume handler and
//     keep stepping (resumeSuspended).
func (w *Worker) handleResume(ctx context.Context, task *Task) error {
	tenant := task.TenantID
	executionID := task.ExecutionID
	if executionID == "" {
		w.logger.Warn("resume without execution_id, poison ack", "resume_type", task.ResumeType)
		return nil
	}

	// Pre-lease probes (no lease taken on the short-circuit paths).
	cancelHit, _, err := w.store.IsCancelRequested(ctx, tenant, executionID)
	if err != nil {
		return fmt.Errorf("cancel probe: %w", err)
	}
	state, err := w.store.LoadExecutionState(ctx, tenant, executionID)
	if err != nil {
		return err
	}
	// A missing row is NOT a pre-lease decision: the lease-conflict window must
	// still be observed (a decision carrier routed at a live holder is never
	// acked, §3.5), so the poison ack happens inside the lease path below.
	if state != nil && TerminalStatuses[state.Status] {
		// §5.3: already_terminal is an idempotent ack — never overwrite a
		// finished execution with a resume error.
		w.logger.Info("resume for terminal execution, idempotent ack",
			"execution_id", executionID, "status", state.Status, "resume_type", task.ResumeType)
		return nil
	}

	// Cancellation priority (§4.2 #4/#5): a cancel flag (or the cancel carrier
	// itself) must be consumed under the lease; it also overrides the
	// suspended+continue short-circuit below (台账第三十四轮 ④).
	if cancelHit || task.ResumeType == "cancel" {
		return w.resumeCancelUnderLease(ctx, task)
	}

	// §4.2 #5 / #33: suspended + continue|retry carries no progress semantics.
	// No lease is taken unless a dispatch_pending credential needs one.
	if state != nil && state.Status == StatusSuspended &&
		(task.ResumeType == "continue" || task.ResumeType == "retry") {
		compensated, cerr := w.compensateSuspendDispatch(ctx, task, state)
		if cerr != nil {
			return cerr // not acked: the service dispatch must be retried
		}
		w.logger.Info("suspended execution: resume carrier carries no progress semantics, idempotent ack",
			"execution_id", executionID, "resume_type", task.ResumeType, "dispatch_compensated", compensated)
		return nil
	}

	// Everything else advances (or deterministically fails) under the lease.
	return w.resumeUnderLease(ctx, task)
}

// resumeUnderLease acquires the execution lease, re-reads the state (§3.4
// resume TOCTOU), re-runs the preconditions and performs the disposition.
func (w *Worker) resumeUnderLease(ctx context.Context, task *Task) error {
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
	w.metrics.IncCounter(MetricLeaseAcquired, metricLabels("tenant", tenant))
	defer func() { _ = w.lease.Release(ctx, handle) }()

	// In-lease re-check (plaita#241): the entry snapshot may already be stale.
	if hit, _, err := w.store.IsCancelRequested(ctx, tenant, executionID); err != nil {
		return fmt.Errorf("cancel probe: %w", err)
	} else if hit {
		return w.writeResumeCancelled(ctx, tenant, executionID, handle.FenceToken)
	}
	state, err := w.store.LoadExecutionState(ctx, tenant, executionID)
	if err != nil {
		return err
	}
	if state == nil {
		w.logger.Warn("resume: execution vanished under the lease, poison ack",
			"execution_id", executionID)
		return nil
	}
	if TerminalStatuses[state.Status] {
		w.logger.Info("resume: execution terminalized under the lease, idempotent ack",
			"execution_id", executionID, "status", state.Status)
		return nil
	}
	if task.ResumeType == "cancel" {
		return w.writeResumeCancelled(ctx, tenant, executionID, handle.FenceToken)
	}
	if state.Status == StatusSuspended && (task.ResumeType == "continue" || task.ResumeType == "retry") {
		compensated, cerr := w.dispatchPendingService(ctx, task, state, handle.FenceToken)
		if cerr != nil {
			return cerr
		}
		w.logger.Info("suspended execution (in-lease recheck): idempotent ack",
			"execution_id", executionID, "dispatch_compensated", compensated)
		return nil
	}
	if state.Status == StatusRunning && (task.ResumeType == "event" || task.ResumeType == "timeout") {
		// #258: the carrier arrived after the suspend point was consumed —
		// normalize it into a continue carrier instead of acking the only
		// recovery path.
		return w.requeueNormalizedContinue(ctx, task)
	}

	if state.Status == StatusSuspended &&
		(task.ResumeType == workflow.ResumeEvent || task.ResumeType == workflow.ResumeTimeout) {
		// M6: real wake-up — restore the checkpoint, dispatch to the suspended
		// node's Resume and continue the flow.
		return w.resumeSuspended(ctx, task, state, handle)
	}

	if state.Status == StatusRunning &&
		(task.ResumeType == workflow.ResumeContinue || task.ResumeType == workflow.ResumeRetry) {
		// §9.1 / #91: a converted retry carrier (or any continue/retry
		// carrier) landing on a live execution continues THIS execution.
		// Python: "running 执行没有失败节点可重跑……retry 在此的语义 = 从
		// checkpoint 派发后继节点（与 continue 同路），必须真实重派".
		return w.continueRunning(ctx, task, state, handle)
	}

	// Any other carrier/state combination has no advance semantics in this
	// build (an error execution has no checkpoint to retry from; a running
	// one is not suspended). Fail closed with an observable terminal instead
	// of acking the carrier silently.
	ne := WorkerError("resume_not_supported", flowErrorCode, fmt.Sprintf(
		"resume carrier (type=%s) cannot advance an execution in status %s", task.ResumeType, state.Status))
	if err := w.persistErrorTerminal(ctx, tenant, handle.FenceToken, state, nil, ne); err != nil {
		return err
	}
	w.logger.Warn("resume carrier has no applicable state transition, deterministic error terminal written",
		"execution_id", executionID, "resume_type", task.ResumeType, "status", state.Status)
	return nil
}

// resumeCancelUnderLease terminalizes a cancel intent. The lease mutual
// exclusion keeps two carriers from writing concurrently; the write itself is
// idempotent.
func (w *Worker) resumeCancelUnderLease(ctx context.Context, task *Task) error {
	tenant := task.TenantID
	executionID := task.ExecutionID
	holder := NewHolderToken("resume", w.cfg.InstanceID)
	handle, err := w.lease.Acquire(ctx, tenant, executionID, holder, w.cfg.LeaseTTLSeconds)
	if err != nil {
		if errors.Is(err, ErrExecutionLeaseConflict) {
			return w.leaseConflictDisposition(ctx, task)
		}
		return fmt.Errorf("resume cancel lease acquire: %w", err)
	}
	w.metrics.IncCounter(MetricLeaseAcquired, metricLabels("tenant", tenant))
	defer func() { _ = w.lease.Release(ctx, handle) }()

	state, err := w.store.LoadExecutionState(ctx, tenant, executionID)
	if err != nil {
		return err
	}
	if state == nil || TerminalStatuses[state.Status] {
		w.logger.Info("resume cancel: nothing to do (missing or terminal execution), ack",
			"execution_id", executionID)
		return nil
	}
	return w.writeResumeCancelled(ctx, tenant, executionID, handle.FenceToken)
}

func (w *Worker) writeResumeCancelled(ctx context.Context, tenant, executionID string, fenceGen int64) error {
	state, err := w.store.LoadExecutionState(ctx, tenant, executionID)
	if err != nil {
		return err
	}
	if state == nil {
		return nil
	}
	if TerminalStatuses[state.Status] {
		return nil
	}
	now := nowISO()
	state.Status = StatusCancelled
	state.EndTime = now
	state.LastUpdateTime = now
	if err := w.store.SaveExecutionState(ctx, tenant, state, fenceGen); err != nil {
		return err
	}
	w.deleteCheckpoint(ctx, tenant, executionID)
	w.logger.Info("resume terminalized cancelled execution", "execution_id", executionID)
	return nil
}

// requeueNormalizedContinue implements the §9.2 ResumeProtocolError
// normalization: an event/timeout carrier that missed the suspend point is
// replaced by a continue carrier. The caller acks the original only after the
// enqueue succeeded (ProcessOne's standard ack).
func (w *Worker) requeueNormalizedContinue(ctx context.Context, task *Task) error {
	body := map[string]any{
		"type":         "resume",
		"flow_id":      task.FlowID,
		"execution_id": task.ExecutionID,
		"resume_type":  "continue",
		"timestamp":    nowISO(),
	}
	if task.TenantID != "" {
		body["tenant_id"] = task.TenantID
	}
	if len(task.Params) > 0 {
		body["params"] = task.Params
	}
	if _, err := w.queue.Enqueue(ctx, body, task.EnvelopeSchemaVer); err != nil {
		w.logger.Warn("normalized continue re-enqueue failed; leaving the original pending",
			"execution_id", task.ExecutionID, "error", err)
		return fmt.Errorf("normalized continue enqueue: %w", err)
	}
	w.logger.Warn("resume carrier missed the suspend point; normalized to a continue carrier",
		"execution_id", task.ExecutionID, "resume_type", task.ResumeType)
	return nil
}

// pendingSuspendDispatch returns the dispatch_pending credential when present
// and still pending (§5.1, plaita#233).
func pendingSuspendDispatch(state *ExecutionState) map[string]any {
	if state == nil || state.DispatchPending == nil {
		return nil
	}
	if pending, _ := state.DispatchPending["pending"].(bool); !pending {
		return nil
	}
	return state.DispatchPending
}

// compensateSuspendDispatch re-dispatches a pending suspend service task for
// the suspended+continue short-circuit. It takes a short "dispatch" lease
// unless another worker holds one (that holder owns the re-dispatch); the
// lease protects the credential-clear write from clobbering a concurrent
// advance.
func (w *Worker) compensateSuspendDispatch(ctx context.Context, task *Task, state *ExecutionState) (bool, error) {
	if pendingSuspendDispatch(state) == nil {
		return false, nil
	}
	holder := NewHolderToken("dispatch", w.cfg.InstanceID)
	handle, err := w.lease.Acquire(ctx, task.TenantID, task.ExecutionID, holder, w.cfg.LeaseTTLSeconds)
	if err != nil {
		if errors.Is(err, ErrExecutionLeaseConflict) {
			w.logger.Warn("suspend dispatch compensation skipped: execution lease held by another worker (it will re-dispatch)",
				"execution_id", task.ExecutionID)
			return false, nil
		}
		return false, fmt.Errorf("suspend dispatch lease: %w", err)
	}
	w.metrics.IncCounter(MetricLeaseAcquired, metricLabels("tenant", task.TenantID))
	defer func() { _ = w.lease.Release(ctx, handle) }()

	fresh, err := w.store.LoadExecutionState(ctx, task.TenantID, task.ExecutionID)
	if err != nil {
		return false, err
	}
	if fresh == nil || fresh.Status != StatusSuspended {
		return false, nil
	}
	return w.dispatchPendingService(ctx, task, fresh, handle.FenceToken)
}

// dispatchPendingService performs the actual re-dispatch + credential clear
// under a held lease.
func (w *Worker) dispatchPendingService(ctx context.Context, task *Task, state *ExecutionState, fenceGen int64) (bool, error) {
	marker := pendingSuspendDispatch(state)
	if marker == nil {
		return false, nil
	}
	serviceConfig, _ := marker["service_config"].(map[string]any)
	if serviceConfig == nil {
		serviceConfig = map[string]any{}
	}
	subtype, _ := marker["service_type"].(string)
	if subtype == "" {
		subtype, _ = serviceConfig["type"].(string)
	}
	if subtype == "" {
		w.logger.Warn("suspend dispatch credential has no service type; clearing it",
			"execution_id", task.ExecutionID)
	} else {
		body := make(map[string]any, len(serviceConfig)+2)
		for k, v := range serviceConfig {
			body[k] = v
		}
		if _, ok := body["execution_id"]; !ok {
			body["execution_id"] = task.ExecutionID
		}
		if _, ok := body["tenant_id"]; !ok {
			body["tenant_id"] = task.TenantID
		}
		blob, err := json.Marshal(body)
		if err != nil {
			return false, fmt.Errorf("suspend dispatch marshal: %w", err)
		}
		queueKey := w.cfg.KeyPrefix + ":" + subtype + ":queue"
		if err := w.rdb.RPush(ctx, queueKey, string(blob)).Err(); err != nil {
			// Must NOT ack: the message is the only carrier that can retry
			// the dispatch (ServiceDispatchError semantics, §5.2).
			w.logger.Error("suspend service dispatch failed; message will be left pending",
				"execution_id", task.ExecutionID, "queue", queueKey, "error", err)
			return false, fmt.Errorf("suspend dispatch rpush: %w", err)
		}
		w.logger.Warn("suspend service task re-dispatched",
			"execution_id", task.ExecutionID, "queue", queueKey, "node", marker["node_id"])
	}
	// Credential and "dispatched" marker share one write (§5.2).
	state.DispatchPending = nil
	if err := w.store.SaveExecutionState(ctx, task.TenantID, state, fenceGen); err != nil {
		w.logger.Error("suspend dispatch succeeded but clearing the credential failed; message stays pending",
			"execution_id", task.ExecutionID, "error", err)
		return false, err
	}
	return true, nil
}

// deadLetterGuard implements §9.2's _dead_letter_guard:
//
//	schema rejection                 → allow, never clone (A49-01)
//	state missing / terminal         → allow
//	state load / lease probe failure → skip (conservative)
//	lease active                     → skip (a live worker owns it)
//	deleted execution (tombstone)    → allow, never clone (A51-01)
//	running + retry budget exhausted → terminalize error, then allow
//	non-terminal + lease empty       → re-enqueue a recovery clone, then allow
//
// The clone inherits the SOURCE envelope version (§2.2 / A44-02): a copy
// written by this build must not masquerade as newer than its source.
func (w *Worker) deadLetterGuard(ctx context.Context, env *Envelope, task *Task, reason string) bool {
	if reason == ReasonUnsupportedSchema {
		return true
	}
	if task == nil || task.ExecutionID == "" {
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
	// The state snapshot may predate a deletion; check the tombstone after the
	// lease probe and before cloning/terminalizing (A51-01).
	tombstoned, terr := w.executionTombstoned(ctx, task.TenantID, task.ExecutionID)
	if terr != nil {
		w.logger.Warn("dead letter guard: tombstone check failed, skipping", "error", terr)
		return false
	}
	if tombstoned {
		w.logger.Warn("dead letter guard: execution deleted, allowing the dead letter without a recovery clone",
			"execution_id", task.ExecutionID)
		return true
	}
	// Retry-budget exhaustion (plaita#242: only a running execution is
	// "stuck"; suspended waits for an external decision and must never be
	// terminalized here).
	if state.Status == StatusRunning {
		retryCount := w.retries.MaxNodeRetry(ctx, task.TenantID, task.ExecutionID)
		totalExhausted := w.retries.TotalExhausted(ctx, task.TenantID, task.ExecutionID)
		if retryCount >= w.cfg.MaxDeliveries || totalExhausted {
			w.logger.Error("dead letter guard: retry budget exhausted, terminalizing before the dead letter",
				"execution_id", task.ExecutionID, "node_retries", retryCount, "total_exhausted", totalExhausted)
			state.Status = StatusError
			state.EndTime = nowISO()
			state.LastUpdateTime = state.EndTime
			state.Error = map[string]any{
				"message":       fmt.Sprintf("node execution kept failing (%d retries); retry budget exhausted", retryCount),
				"node_retries":  retryCount,
				"code":          DefaultNodeAbortCode,
				"error_type":    "node_retry_budget_exhausted",
				"failed_reason": reason,
			}
			if err := w.store.SaveExecutionState(ctx, task.TenantID, state, 0); err != nil {
				w.logger.Warn("dead letter guard: terminalization failed, skipping", "error", err)
				return false
			}
			return true
		}
	}
	// Non-terminal + no lease: the only recovery carrier is this message.
	// Re-enqueue a fresh copy (delivery restarts at 1) before allowing the
	// original into the DLQ.
	payload := env.PayloadRaw
	if payload == "" {
		w.logger.Warn("dead letter guard: no raw payload to clone, skipping")
		return false
	}
	if _, err := w.queue.EnqueueRaw(ctx, payload, task.EnvelopeSchemaVer); err != nil {
		w.logger.Warn("dead letter guard: recovery clone enqueue failed, skipping", "error", err)
		return false
	}
	w.logger.Warn("dead letter guard: recovery carrier re-enqueued; original may now dead-letter",
		"execution_id", task.ExecutionID)
	return true
}

// executionTombstoned reports whether the execution carries a deletion
// tombstone for this tenant (§8.3 platform-root key; cross-tenant same ids are
// not treated as deleted).
func (w *Worker) executionTombstoned(ctx context.Context, tenantID, executionID string) (bool, error) {
	raw, err := w.rdb.Get(ctx, w.tombstoneKey(executionID)).Result()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var tomb map[string]any
	if json.Unmarshal([]byte(raw), &tomb) != nil {
		return false, nil
	}
	tombTenant, _ := tomb["tenant"].(string)
	if tombTenant == "" {
		tombTenant = w.cfg.DefaultTenantID
	}
	carrierTenant := tenantID
	if carrierTenant == "" {
		carrierTenant = w.cfg.DefaultTenantID
	}
	return tombTenant == carrierTenant, nil
}

// requeueRetryableFailure implements §9.1's explicit re-enqueue: backoff
// (interruptible by D2), enqueue a fresh carrier, then ack the original. The
// enqueue→ack order is load-bearing: a crash in between duplicates (the lease
// serializes), never loses.
func (w *Worker) requeueRetryableFailure(ctx context.Context, delivery *Delivery, nre *NodeRetryableError) (bool, error) {
	task := delivery.Task
	if delay := nodeRetryBackoff(nre.Attempt); delay > 0 {
		w.logger.Warn("node retryable failure: backing off before re-enqueue",
			"execution_id", task.ExecutionID, "node", nre.FailedNode,
			"attempt", nre.Attempt, "budget", nre.Budget, "delay", delay)
		if w.interruptibleSleep(ctx, delay) {
			w.logger.Warn("retry backoff aborted by drain force-stop; not re-enqueueing, message left pending",
				"execution_id", task.ExecutionID, "attempt", nre.Attempt)
			return true, fmt.Errorf("%w: retry backoff aborted", ErrDrainAborted)
		}
	}
	body, cerr := w.retryCarrierBody(ctx, task)
	if cerr != nil {
		// Tombstone probe failure: fail closed, the original stays pending
		// (Python raises in the same position — 绝不 fail-open 重排).
		return true, fmt.Errorf("retry carrier body: %w", cerr)
	}
	if body == nil {
		// Deleted execution (tombstone): ack and drop — a re-enqueued copy
		// would resurrect it (§9.1 / A51-01, flow_worker.py:313-389).
		w.logger.Warn("retryable failure on a deleted execution; original acked without a re-enqueue",
			"execution_id", task.ExecutionID, "node", nre.FailedNode)
		return true, w.queue.Ack(ctx, task.MessageID)
	}
	messageID, err := w.queue.Enqueue(ctx, body, task.EnvelopeSchemaVer)
	if err != nil {
		w.logger.Warn("retry carrier enqueue failed; original left pending for reclaim",
			"execution_id", task.ExecutionID, "error", err)
		return true, fmt.Errorf("retry carrier enqueue: %w", err)
	}
	if err := w.queue.Ack(ctx, task.MessageID); err != nil {
		w.logger.Warn("retry carrier enqueued but the original ack failed (duplicate delivery, lease-serialized)",
			"execution_id", task.ExecutionID, "error", err)
		return true, nil
	}
	// #31: one retry carrier actually went out (requeue, not budget exhaust).
	w.metrics.IncCounter(MetricRetryTotal, metricLabels("tenant", task.TenantID, "flow", task.FlowID, "node", nre.FailedNode))
	w.logger.Warn("retry carrier re-enqueued and original acked",
		"execution_id", task.ExecutionID, "node", nre.FailedNode,
		"attempt", nre.Attempt, "budget", nre.Budget, "message_id", messageID)
	return true, nil
}

// handleForeignTarget implements the TaskNotForThisWorker handover (§9.2):
// never a DLQ, bounded by a per-execution INCR counter key (7d TTL), enqueue
// before ack, and a broadcast fallback when the target instance is gone.
func (w *Worker) handleForeignTarget(ctx context.Context, delivery *Delivery) (bool, error) {
	task := delivery.Task
	status, err := w.registry.LookupStatus(ctx, task.TargetInstanceID)
	if err != nil {
		w.recordHandoverLedger(ctx, task, DecisionTargeted, ReasonHandoverProbeFailure, "")
		return true, fmt.Errorf("dispatch target probe failed: %w", err)
	}

	body := map[string]any{}
	if err := json.Unmarshal([]byte(delivery.Envelope.PayloadRaw), &body); err != nil || body == nil {
		body = taskPayloadDict(task)
	}

	// A46-02: a gone target must not be kept — otherwise every non-target
	// worker hands the task over again until the counter exhausts a runnable
	// task. Strip the target and broadcast.
	if status != RegStatusRunning {
		delete(body, "target_instance_id")
		newID, err := w.queue.Enqueue(ctx, body, task.EnvelopeSchemaVer)
		if err != nil {
			w.recordHandoverLedger(ctx, task, DecisionBroadcast, ReasonTargetUnavailable, "")
			return true, fmt.Errorf("broadcast handover enqueue: %w", err)
		}
		w.recordHandoverLedger(ctx, task, DecisionBroadcast, ReasonTargetUnavailable, newID)
		w.logger.Warn("dispatch target unavailable: task re-enqueued as broadcast",
			"target", task.TargetInstanceID, "status", status, "message_id", newID)
		return true, w.queue.Ack(ctx, task.MessageID)
	}

	count, err := w.recordHandover(ctx, task, delivery)
	if err != nil {
		// Fail-closed: without a bound we never hand over (the old boolean
		// marker was the unbounded-handover incident).
		w.recordHandoverLedger(ctx, task, DecisionTargeted, ReasonHandoverUnbounded, "")
		return true, fmt.Errorf("%w: %v", ErrHandoverCounterUnavailable, err)
	}
	if count > AffinityHandoverMax {
		w.recordHandoverLedger(ctx, task, DecisionTargeted, ReasonHandoverExhausted, "")
		w.logger.Error("handover budget exhausted; execution terminalized, message left pending",
			"execution_id", task.ExecutionID, "count", count, "max", AffinityHandoverMax)
		return w.terminalizeHandoverExhausted(ctx, task, count)
	}

	newID, err := w.queue.Enqueue(ctx, body, task.EnvelopeSchemaVer)
	if err != nil {
		w.recordHandoverLedger(ctx, task, DecisionTargeted, ReasonHandover, "")
		return true, fmt.Errorf("handover enqueue: %w", err)
	}
	w.recordHandoverLedger(ctx, task, DecisionTargeted, ReasonHandover, newID)
	w.logger.Info("task handed over to its targeted worker",
		"target", task.TargetInstanceID, "count", count, "message_id", newID)
	return true, w.queue.Ack(ctx, task.MessageID)
}

// recordHandover INCRs the handover counter with a sliding 7-day TTL. A
// failure is returned: the counter is the ONLY termination condition for a
// handover loop (new copies always restart delivery at 1).
func (w *Worker) recordHandover(ctx context.Context, task *Task, delivery *Delivery) (int, error) {
	key := w.handoverKey(task, delivery)
	pipe := w.rdb.TxPipeline()
	incr := pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, counterTTLSeconds*time.Second)
	if _, err := pipe.Exec(ctx); err != nil {
		w.logger.Error("handover counter increment failed (bound unavailable, refusing to hand over)",
			"key", key, "error", err)
		return 0, err
	}
	return int(incr.Val()), nil
}

func (w *Worker) handoverKey(task *Task, delivery *Delivery) string {
	if task.ExecutionID != "" {
		return w.cfg.Namespace(task.TenantID) + ":execution:affinityhandover:" + task.ExecutionID
	}
	// No execution id: hash the canonical body (Go marshals map keys sorted,
	// matching Python's json.dumps(sort_keys=True)).
	canonical := delivery.Envelope.PayloadRaw
	var body map[string]any
	if json.Unmarshal([]byte(canonical), &body) == nil && body != nil {
		if blob, err := json.Marshal(body); err == nil {
			canonical = string(blob)
		}
	}
	sum := sha1.Sum([]byte(canonical))
	return w.cfg.KeyPrefix + ":affinity:handover:" + hex.EncodeToString(sum[:])
}

// terminalizeHandoverExhausted makes "no host can run this" observable. An
// already-terminal execution is acked (the message can never be processed);
// otherwise the run is terminalized and the message left pending — the 7-day
// counter keeps every later reclaim from producing another copy, so the loop
// is bounded even though this worker does not ack.
func (w *Worker) terminalizeHandoverExhausted(ctx context.Context, task *Task, count int) (bool, error) {
	if task.ExecutionID == "" {
		return true, ErrHandoverCounterUnavailable
	}
	state, err := w.store.LoadExecutionState(ctx, task.TenantID, task.ExecutionID)
	if err != nil {
		return true, err
	}
	if state == nil {
		return true, nil
	}
	if TerminalStatuses[state.Status] {
		if err := w.queue.Ack(ctx, task.MessageID); err != nil {
			return true, err
		}
		w.logger.Warn("handover exhausted on an already-terminal execution: message acked to stop the reclaim noise",
			"execution_id", task.ExecutionID)
		return true, nil
	}
	state.Status = StatusError
	state.EndTime = nowISO()
	state.LastUpdateTime = state.EndTime
	state.Error = map[string]any{
		"message":            fmt.Sprintf("no worker accepted this task after %d handovers", count),
		"code":               DefaultNodeAbortCode,
		"type":               "AffinityHandoverExhausted",
		"affinity_handovers": count,
		"affinity_reason":    fmt.Sprintf("target %s never consumed the carrier", task.TargetInstanceID),
	}
	if err := w.store.SaveExecutionState(ctx, task.TenantID, state, 0); err != nil {
		return true, err
	}
	return true, nil
}

func (w *Worker) recordHandoverLedger(ctx context.Context, task *Task, decision, reason, messageID string) {
	entry := w.handoverEntry(task, decision, reason, messageID)
	if _, err := w.ledger.Record(ctx, entry); err != nil {
		// Record already logged; a ledger failure never blocks the handover.
		return
	}
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
			// $NODE is the expression root: the map of executed node results.
			// (CurrentNodeContext is per-node scratch and lives under $CURRENT.)
			out["$NODE"] = rc.ExecuteNodeResults
			out["$LAST_NODE"] = string(rc.LastNodeID)
			out["$ENV"] = rc.EnvVariables
			out["$GLOBAL"] = rc.GlobalVariables
			if len(rc.CurrentNodeContext) > 0 {
				out["$CURRENT"] = rc.CurrentNodeContext
			}
			if len(rc.ExecuteNodeErrors) > 0 {
				out["$ERROR"] = rc.ExecuteNodeErrors
			}
		}
	}
	out["$FLOW_ID"] = string(run.rootFlow.ID)
	return out
}

// parseProducerTimestamp parses the producer timestamp (naive local wall
// clock without timezone, or RFC3339, §2.3). Shared by the queue-wait
// computation and the #29 age gate.
func parseProducerTimestamp(ts string) (time.Time, bool) {
	layouts := []string{
		"2006-01-02T15:04:05.999999",
		"2006-01-02T15:04:05",
		time.RFC3339Nano,
		time.RFC3339,
	}
	for _, l := range layouts {
		if t, err := time.ParseInLocation(l, ts, time.Local); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// queueWaitMillis parses the producer timestamp (naive local wall clock,
// §2.3) into a queue wait in milliseconds. Unparsable → not reported.
func queueWaitMillis(ts string) (float64, bool) {
	if ts == "" {
		return 0, false
	}
	t, ok := parseProducerTimestamp(ts)
	if !ok {
		return 0, false
	}
	return float64(time.Since(t).Milliseconds()), true
}

// newExecutionID mints a uuid4 hex (§2.3: uuid4 hex form).
func newExecutionID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return hex.EncodeToString(b[:])
}
