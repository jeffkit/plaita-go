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
// failure.
//
// Protocol §9.1 converts a start carrier into a resume/continue carrier when a
// checkpoint row exists. M6 writes checkpoints only at suspend boundaries, so
// an error execution has no resume position; a continue carrier would fail
// admission. The carrier therefore stays a start copy of the same
// execution_id: the budget/backoff/re-enqueue semantics are identical, only
// the replay起点 differs (whole-flow replay instead of step-boundary resume).
// The conversion returns once per-step checkpoints exist (phase 3).
func (w *Worker) retryCarrierBody(ctx context.Context, task *Task) map[string]any {
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
