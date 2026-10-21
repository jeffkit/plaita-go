package workflow

import "time"

// Construction-time budget caps (#35, plaita#267 alignment): a LEGAL flow
// must not be able to express an unbounded occupancy of a shared worker.
// plaita capped these at the model layer (B2 r47: retryTimes ≤ 10;
// #267: node.timeout ≤ 24h — a longer budget must be an explicit flow-level
// timeout). The worker adds the execution-level deadline on top
// (worker.Config.ExecutionDeadline): the engine caps bound what one flow can
// EXPRESS, the worker budget bounds what one execution can OCCUPY.
const (
	// MaxRetryTimes caps errorHandler.retryTimes at every level (flow and
	// node). Enforced in RecoverableErrorHandlePolicy.Validate (engine) and
	// in the IR loader (named rejection at load time).
	MaxRetryTimes = 10

	// MaxNodeTimeout caps a single node's timeout budget (plaita#267
	// MAX_NODE_TIMEOUT_MS = 24h). A node that can pin a worker for days must
	// not be expressible; budgets beyond 24h belong in an explicit flow-level
	// timeout — which still sits under the worker execution deadline.
	MaxNodeTimeout = 24 * time.Hour
)
