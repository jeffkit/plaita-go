package worker

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// Node-level retry budgets (protocol §9.1). Three Redis counters, all sliding
// 7-day TTLs — deliberately NOT the message delivery_count (whose reclaim
// semantics cannot express "retries of THIS node"):
//
//	{ns}:execution:noderetry:{id}:{node}   per-node budget = max_deliveries
//	{ns}:execution:noderetry:{id}          per-execution fallback when the
//	                                       failed node id is unknown
//	{ns}:execution:noderefetch:{id}        total ceiling = 100
//	{ns}:execution:nofail:{id}             deterministic-failure ceiling = 12
//
// Any successful node advance clears all of them (§9.1: the counters express
// *consecutive* failures).

// RetryBudgets owns the counter keys.
type RetryBudgets struct {
	client *redis.Client
	cfg    *Config
	logger Logger
}

func NewRetryBudgets(client *redis.Client, cfg *Config, logger Logger) *RetryBudgets {
	return &RetryBudgets{client: client, cfg: cfg, logger: logger}
}

func (r *RetryBudgets) nodeKey(tenantID, executionID, nodeID string) string {
	base := r.cfg.Namespace(tenantID) + ":execution:noderetry:" + executionID
	if nodeID == "" {
		return base
	}
	return base + ":" + nodeID
}

func (r *RetryBudgets) totalKey(tenantID, executionID string) string {
	return r.cfg.Namespace(tenantID) + ":execution:noderefetch:" + executionID
}

func (r *RetryBudgets) deterministicKey(tenantID, executionID string) string {
	return r.cfg.Namespace(tenantID) + ":execution:nofail:" + executionID
}

// incrCounter INCRs + refreshes the sliding TTL in one transaction (§9.1).
func (r *RetryBudgets) incrCounter(ctx context.Context, key string) (int, error) {
	pipe := r.client.TxPipeline()
	incr := pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, counterTTLSeconds*time.Second)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	return int(incr.Val()), nil
}

// RecordNodeRetry returns the per-node attempt count including this failure.
func (r *RetryBudgets) RecordNodeRetry(ctx context.Context, tenantID, executionID, nodeID string) (int, error) {
	return r.incrCounter(ctx, r.nodeKey(tenantID, executionID, nodeID))
}

// RecordTotal returns the fallback total attempt count.
func (r *RetryBudgets) RecordTotal(ctx context.Context, tenantID, executionID string) (int, error) {
	return r.incrCounter(ctx, r.totalKey(tenantID, executionID))
}

// RecordDeterministic returns the deterministic-failure count.
func (r *RetryBudgets) RecordDeterministic(ctx context.Context, tenantID, executionID string) (int, error) {
	return r.incrCounter(ctx, r.deterministicKey(tenantID, executionID))
}

// ReadNodeRetry returns the counter for one node (0 when absent/unreadable).
func (r *RetryBudgets) ReadNodeRetry(ctx context.Context, tenantID, executionID, nodeID string) int {
	raw, err := r.client.Get(ctx, r.nodeKey(tenantID, executionID, nodeID)).Result()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			r.logger.Warn("node retry counter read failed (treated as 0)", "error", err)
		}
		return 0
	}
	n, _ := strconv.Atoi(raw)
	return n
}

// MaxNodeRetry is the dead-letter guard's view: the max across the per-node
// keys and the execution-level fallback key (§9.2; the guard cannot know which
// node failed, any node at budget means the execution is stuck).
func (r *RetryBudgets) MaxNodeRetry(ctx context.Context, tenantID, executionID string) int {
	base := r.nodeKey(tenantID, executionID, "")
	best := r.ReadNodeRetry(ctx, tenantID, executionID, "")
	iter := r.client.Scan(ctx, 0, base+":*", 100).Iterator()
	for iter.Next(ctx) {
		if raw, err := r.client.Get(ctx, iter.Val()).Result(); err == nil {
			if n, cerr := strconv.Atoi(raw); cerr == nil && n > best {
				best = n
			}
		}
	}
	if err := iter.Err(); err != nil {
		r.logger.Warn("node retry counter scan failed (using what was read)", "error", err)
	}
	return best
}

// TotalExhausted reports whether the 100-retry fallback ceiling is reached.
func (r *RetryBudgets) TotalExhausted(ctx context.Context, tenantID, executionID string) bool {
	return r.readCounter(ctx, r.totalKey(tenantID, executionID)) >= NodeRetryTotalMax
}

// DeterministicExhausted reports whether 12 consecutive deterministic
// failures have been recorded.
func (r *RetryBudgets) DeterministicExhausted(ctx context.Context, tenantID, executionID string) bool {
	return r.readCounter(ctx, r.deterministicKey(tenantID, executionID)) >= DeterministicFailureMax
}

func (r *RetryBudgets) readCounter(ctx context.Context, key string) int {
	raw, err := r.client.Get(ctx, key).Result()
	if err != nil {
		return 0
	}
	n, _ := strconv.Atoi(raw)
	return n
}

// ResetOnProgress clears the counters of every node that completed
// successfully (§9.1: 任一节点成功推进即 DEL 清零). The total/deterministic
// ceilings are reset by ResetConsecutive on a fully successful run only: this
// build replays the flow from the start on retry, so the always-successful
// start node must not clear the consecutive-failure ceilings.
func (r *RetryBudgets) ResetOnProgress(ctx context.Context, tenantID, executionID string, succeeded []string) error {
	if len(succeeded) == 0 {
		return nil
	}
	keys := make([]string, 0, len(succeeded))
	for _, node := range succeeded {
		keys = append(keys, r.nodeKey(tenantID, executionID, node))
	}
	if err := r.client.Del(ctx, keys...).Err(); err != nil {
		r.logger.Warn("retry counter reset failed (harmless: counters keep accumulating)",
			"execution_id", executionID, "error", err)
		return err
	}
	return nil
}

// ResetConsecutive clears the fallback total + deterministic-failure counters
// (a fully successful run resets the consecutive-failure ceilings).
func (r *RetryBudgets) ResetConsecutive(ctx context.Context, tenantID, executionID string) error {
	return r.client.Del(ctx, r.totalKey(tenantID, executionID), r.deterministicKey(tenantID, executionID)).Err()
}

// ResetAll deletes every retry counter of an execution (start of a fresh
// execution carrying a recycled id must not inherit the old budgets).
func (r *RetryBudgets) ResetAll(ctx context.Context, tenantID, executionID string) error {
	base := r.nodeKey(tenantID, executionID, "")
	keys := []string{base, r.totalKey(tenantID, executionID), r.deterministicKey(tenantID, executionID)}
	iter := r.client.Scan(ctx, 0, base+":*", 100).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	return r.client.Del(ctx, keys...).Err()
}

// Node retry backoff (§9.1): min(30s, 1s * 2^(attempt-1)). The base is a
// package variable so tests can collapse the wait (plaita exposes _sleep for
// the same reason).
var nodeRetryBackoffBase = time.Second

const nodeRetryBackoffCap = 30 * time.Second

func nodeRetryBackoff(attempt int) time.Duration {
	if attempt < 1 || nodeRetryBackoffBase <= 0 {
		return 0
	}
	d := nodeRetryBackoffBase << (attempt - 1)
	if d <= 0 || d > nodeRetryBackoffCap {
		return nodeRetryBackoffCap
	}
	return d
}

// isRetryableNodeFailure mirrors _is_retryable_node_failure (§9.1): a node
// failed (run.failedNode set), it is not a timeout/cancellation, and the
// engine classifies the error as retryable.
func isRetryableNodeFailure(err error, run *executionRun) bool {
	if err == nil || run == nil || run.failedNode == "" {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return workflow.IsRetryableError(err)
}

// retryCarrierBody builds the re-enqueued carrier for a retryable node
// failure — the Go port of `_requeue_body_for_retry` (§9.1,
// flow_worker.py:313-389, #91 载体转换): before re-enqueueing, a start
// carrier is checked against the state row and converted to a
// resume/continue carrier whenever a row exists, so the redelivered copy
// continues the SAME execution instead of replaying it from the first node
// (which would re-run side-effectful predecessors up to budget times).
// Branch table, in the Python docstring's order:
//
//   - a non-start task passes through unchanged (it already carries
//     checkpoint semantics);
//   - no execution_id → start copy (fresh-start semantics unchanged);
//   - deletion tombstone → (nil, nil): the caller must ack the original and
//     NOT re-enqueue (A51-01 — a re-enqueued copy would resurrect the
//     deleted execution);
//   - tombstone probe failure → (nil, err): fail closed, the original stays
//     pending (Python raises in the same position — 绝不 fail-open 重排);
//   - state read failure → start copy (fail-safe: the conversion is an
//     optimization and must never cost the message its cadence);
//   - no state row (crash before the first persist) → start copy;
//   - stored flow_id ≠ carrier flow_id → start copy (a converted copy would
//     be poison-acked by the resume flow-id gate);
//   - a row exists → {"type":"resume","resume_type":"continue", …} with
//     params passed through when non-empty (#122). The version is
//     deliberately dropped: the resume path resolves the definition from the
//     state row's flow_version (§7).
func (w *Worker) retryCarrierBody(ctx context.Context, task *Task) (map[string]any, error) {
	if task.Type != "start" {
		return taskPayloadDict(task), nil
	}
	if task.ExecutionID == "" {
		return startCarrierBody(task), nil
	}
	tombstoned, err := w.executionTombstoned(ctx, task.TenantID, task.ExecutionID)
	if err != nil {
		w.logger.Warn("retry carrier tombstone probe failed; original left pending",
			"execution_id", task.ExecutionID, "error", err)
		return nil, err
	}
	if tombstoned {
		w.logger.Warn("execution deleted (tombstone), retry re-enqueue dropped: the original will be acked",
			"execution_id", task.ExecutionID, "tenant_id", task.TenantID)
		return nil, nil
	}
	state, err := w.store.LoadExecutionState(ctx, task.TenantID, task.ExecutionID)
	if err != nil {
		// Read failure does not block the re-enqueue (原样 start, fail-safe).
		w.logger.Warn("retry carrier state probe failed, re-enqueueing a start copy",
			"execution_id", task.ExecutionID, "error", err)
		return startCarrierBody(task), nil
	}
	if state == nil {
		return startCarrierBody(task), nil
	}
	if state.FlowID != "" && state.FlowID != task.FlowID {
		w.logger.Warn("retry carrier flow_id differs from the stored row, re-enqueueing a start copy",
			"execution_id", task.ExecutionID, "carrier_flow", task.FlowID, "stored_flow", state.FlowID)
		return startCarrierBody(task), nil
	}
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
	w.logger.Info("retry carrier converted to resume/continue (state row exists, continuing the same execution)",
		"execution_id", task.ExecutionID, "status", state.Status)
	return body, nil
}

// startCarrierBody is the unchanged start copy (fresh-start semantics).
func startCarrierBody(task *Task) map[string]any {
	body := map[string]any{
		"type":         "start",
		"flow_id":      task.FlowID,
		"execution_id": task.ExecutionID,
		"timestamp":    nowISO(),
	}
	if task.TenantID != "" {
		body["tenant_id"] = task.TenantID
	}
	if task.Version != "" {
		body["version"] = task.Version
	}
	if len(task.Params) > 0 {
		body["params"] = task.Params
	}
	return body
}

// terminalizeNodeRetryExhausted writes the worker-judged error terminal for an
// exhausted per-node budget (§5.4 fixed error_type + DEFAULT_NODE_ABORT_CODE).
func (w *Worker) terminalizeNodeRetryExhausted(ctx context.Context, tenantID string,
	handle *LeaseHandle, state *ExecutionState, run *executionRun, ne *NormalizedError, attempt int) error {
	if ne == nil {
		ne = WorkerError("node_retry_budget_exhausted", DefaultNodeAbortCode,
			fmt.Sprintf("node failed %d times; retry budget exhausted", attempt))
	}
	ne.Code = DefaultNodeAbortCode
	ne.ErrorType = "node_retry_budget_exhausted"
	ne.NodeRetries = attempt
	if run != nil && run.failedNode != "" {
		ne.FailedNode = run.failedNode
	}
	return run.finalize(func() error {
		return w.persistErrorTerminal(ctx, tenantID, handle.FenceToken, state, run, ne)
	})
}
