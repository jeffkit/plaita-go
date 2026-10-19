package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Execution state persistence (protocol §5): the full ExecutionState field
// set (§5.1, snake_case keys exactly as the Python model_dump writes them),
// the write-back points (§5.2) and the fenced CAS write shape (§3.4 last
// bullet: a single Lua script compares the fence key against the context
// generation before SET + ZADD index).

// Execution statuses (§5.1).
const (
	StatusRunning   = "running"
	StatusSuspended = "suspended"
	StatusCompleted = "completed"
	StatusError     = "error"
	StatusCancelled = "cancelled"
)

// TerminalStatuses mirrors TERMINAL_EXECUTION_STATUSES (storage/redis.py:199).
var TerminalStatuses = map[string]bool{StatusCompleted: true, StatusError: true, StatusCancelled: true}

// ExecutionState is the state-row schema (§5.1 field table). JSON keys are
// snake_case to stay field-compatible with the Python model_dump form; the
// shadow reconciliation diffs key SHAPES (§10.4 #3), not values.
type ExecutionState struct {
	ExecutionID     string         `json:"execution_id"`
	FlowID          string         `json:"flow_id,omitempty"`
	FlowName        string         `json:"flow_name,omitempty"`
	FlowVersion     string         `json:"flow_version,omitempty"`
	FlowHash        string         `json:"flow_hash,omitempty"`
	FlowHashAlgo    string         `json:"flow_hash_algo,omitempty"`
	EngineVersion   string         `json:"engine_version,omitempty"`
	TenantID        string         `json:"tenant_id,omitempty"`
	Context         map[string]any `json:"context"`
	Status          string         `json:"status"`
	StartTime       string         `json:"start_time"`
	LastUpdateTime  string         `json:"last_update_time"`
	EndTime         string         `json:"end_time,omitempty"`
	Error           map[string]any `json:"error,omitempty"`
	Invoker         string         `json:"invoker,omitempty"`
	NodeTimings     map[string]any `json:"node_timings,omitempty"`
	Timeline        map[string]any `json:"timeline,omitempty"`
	Usage           map[string]any `json:"usage,omitempty"`
	DispatchPending map[string]any `json:"dispatch_pending,omitempty"`
}

// nowISO is the naive local wall clock format the Python side writes
// (datetime.now().isoformat() — no timezone suffix, §5.1).
func nowISO() string { return time.Now().Format("2006-01-02T15:04:05.000000") }

// ExecutionStore reads/writes execution state rows and the cancel flag.
type ExecutionStore struct {
	client *redis.Client
	cfg    *Config
}

func NewExecutionStore(client *redis.Client, cfg *Config) *ExecutionStore {
	return &ExecutionStore{client: client, cfg: cfg}
}

func (s *ExecutionStore) stateKey(tenantID, executionID string) string {
	return s.cfg.Namespace(tenantID) + ":execution:" + executionID
}

func (s *ExecutionStore) indexKey(tenantID string) string {
	return s.cfg.Namespace(tenantID) + ":execution:index"
}

func (s *ExecutionStore) fenceKey(tenantID, executionID string) string {
	return s.cfg.Namespace(tenantID) + ":execution:fence:" + executionID
}

// CancelKey is the cancel intent flag (§4.1): value = ISO cancel time,
// TTL 7 days. Written by the console; the worker only reads it (the flag is
// never deleted by the worker, it expires naturally).
func (s *ExecutionStore) CancelKey(tenantID, executionID string) string {
	return s.cfg.Namespace(tenantID) + ":execution:cancel:" + executionID
}

// IsCancelRequested reads the cancel flag; the returned string is the flag
// value (ISO time) when present.
func (s *ExecutionStore) IsCancelRequested(ctx context.Context, tenantID, executionID string) (bool, string, error) {
	v, err := s.client.Get(ctx, s.CancelKey(tenantID, executionID)).Result()
	if err == redis.Nil {
		return false, "", nil
	}
	if err != nil {
		return false, "", err
	}
	return true, v, nil
}

// luaFencedStateSave is the fenced CAS write (protocol §3.4 write-side
// defense, storage/fenced.py:56-69): the state row is written only while the
// fence key still equals the holder's generation; the index ZADD rides in the
// same atomic script. KEYS[1]=state key, KEYS[2]=fence key, KEYS[3]=index
// ZSET; ARGV[1]=state JSON, ARGV[2]=generation, ARGV[3]=TTL seconds (0 = no
// expiry), ARGV[4]=index score (start_time epoch). Returns 1 on write, 0 on
// generation mismatch.
//
// NOTE: the Python script text is not quoted in the protocol document — this
// is a semantic reconstruction from §3.4's description; keep the CAS + index
// pairing identical.
var luaFencedStateSave = redis.NewScript(`
if redis.call('get', KEYS[2]) ~= ARGV[2] then
  return 0
end
if tonumber(ARGV[3]) > 0 then
  redis.call('set', KEYS[1], ARGV[1], 'EX', ARGV[3])
else
  redis.call('set', KEYS[1], ARGV[1])
end
redis.call('zadd', KEYS[3], ARGV[4], KEYS[1])
return 1
`)

// SaveExecutionState persists the state row (§5.2 write shape: pipeline
// SET + terminal TTL + ZADD index; fenced path = Lua CAS). fenceGen > 0
// routes through the fenced CAS; a 0 generation (start without lease /
// rollback mode) falls back to the plain write (§3.4). A CAS mismatch is
// ErrLeaseLost; callers translate it into "do not ack".
func (s *ExecutionStore) SaveExecutionState(ctx context.Context, tenantID string, state *ExecutionState, fenceGen int64) error {
	blob, err := json.Marshal(state)
	if err != nil {
		// StateSerializationError analogue: a state that cannot serialize is
		// a permanent failure, never a retry (§5.2).
		return fmt.Errorf("%w: marshal: %v", ErrStatePersist, err)
	}
	stateKey := s.stateKey(tenantID, state.ExecutionID)
	indexKey := s.indexKey(tenantID)
	score, serr := strconv.ParseFloat(stateStartTimeScore(state), 64)
	if serr != nil {
		score = float64(time.Now().Unix())
	}
	terminalTTL := time.Duration(0)
	if TerminalStatuses[state.Status] {
		terminalTTL = time.Duration(s.cfg.StateTTLDays) * 24 * time.Hour
	}
	if fenceGen > 0 {
		n, err := luaFencedStateSave.Run(ctx, s.client,
			[]string{stateKey, s.fenceKey(tenantID, state.ExecutionID), indexKey},
			string(blob), strconv.FormatInt(fenceGen, 10),
			int64(terminalTTL/time.Second), score,
		).Int64()
		if err != nil {
			return fmt.Errorf("%w: %v", ErrStatePersist, err)
		}
		if n == 0 {
			return fmt.Errorf("%w: fence generation mismatch (gen=%d)", ErrLeaseLost, fenceGen)
		}
		return nil
	}
	pipe := s.client.Pipeline()
	if terminalTTL > 0 {
		pipe.Set(ctx, stateKey, string(blob), terminalTTL)
	} else {
		pipe.Set(ctx, stateKey, string(blob), 0)
	}
	pipe.ZAdd(ctx, indexKey, redis.Z{Score: score, Member: stateKey})
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("%w: %v", ErrStatePersist, err)
	}
	return nil
}

// stateStartTimeScore parses the start_time ISO string into an epoch score
// for the index ZSET (protocol §5.2 says "score=start_time"; the Python
// ZADD needs a number, so the Go side writes the epoch — documented
// ambiguity).
func stateStartTimeScore(state *ExecutionState) string {
	if t, err := time.Parse("2006-01-02T15:04:05.999999", state.StartTime); err == nil {
		return strconv.FormatFloat(float64(t.UnixNano())/1e9, 'f', 6, 64)
	}
	if t, err := time.Parse(time.RFC3339Nano, state.StartTime); err == nil {
		return strconv.FormatFloat(float64(t.UnixNano())/1e9, 'f', 6, 64)
	}
	return strconv.FormatInt(time.Now().Unix(), 10)
}

// LoadExecutionState reads the state row. Missing key → (nil, nil); a
// present-but-corrupt row or a Redis failure → ErrStateLoad (never swallowed
// as "missing", §5.2 read-side contract).
func (s *ExecutionStore) LoadExecutionState(ctx context.Context, tenantID, executionID string) (*ExecutionState, error) {
	v, err := s.client.Get(ctx, s.stateKey(tenantID, executionID)).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrStateLoad, err)
	}
	var state ExecutionState
	if err := json.Unmarshal([]byte(v), &state); err != nil {
		return nil, fmt.Errorf("%w: corrupt state json: %v", ErrStateLoad, err)
	}
	return &state, nil
}
