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
	// StatusQueued is the console's placeholder row (plaita#273): written
	// BEFORE the start carrier is enqueued so the id is immediately GET-able
	// and cancellable. This worker never writes it — a queued row is
	// acceptance evidence, not processing evidence (#83).
	StatusQueued    = "queued"
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
	// onTerminal fires after a successful terminal-status save. It is the
	// single release choke point for the per-tenant start quota (#30,
	// Python _persist_state_or_raise 收口 release_tenant_start): completed /
	// error / cancelled return the slot; suspended keeps it. Best-effort —
	// a callback failure never fails the save.
	onTerminal func(ctx context.Context, tenantID, executionID string)
}

// SetOnTerminalSave wires the terminal-save callback (worker → quota release).
func (s *ExecutionStore) SetOnTerminalSave(fn func(ctx context.Context, tenantID, executionID string)) {
	s.onTerminal = fn
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

// fenceGenKey is the never-cleared generation counter (§3.1 ABA anchor);
// the fenced CAS compares it alongside the fence key (#54, A2R25-05).
func (s *ExecutionStore) fenceGenKey(tenantID, executionID string) string {
	return s.cfg.Namespace(tenantID) + ":execution:fencegen:" + executionID
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
// defense, storage/fenced.py:56-79): the state row is written only while the
// fence key still equals the holder's generation AND the never-cleared
// fencegen counter agrees (A2R25-05: a partial restore that brings back an
// old state+fence pair leaves fencegen at the newer generation — the stale
// token must not re-hit the CAS); the index ZADD rides in the same atomic
// script. KEYS[1]=state key, KEYS[2]=fence key, KEYS[3]=index ZSET,
// KEYS[4]=fencegen counter; ARGV[1]=state JSON, ARGV[2]=generation,
// ARGV[3]=TTL seconds (0 = no expiry), ARGV[4]=index score (start_time
// epoch). Returns 1 on write, 0 on fence mismatch (key missing counts), -1
// on fence/fencegen divergence (partial restore / counter rollback).
//
// NOTE: the Python script text is not quoted in the protocol document — this
// is a semantic reconstruction from §3.4's description; keep the CAS + index
// pairing identical.
var luaFencedStateSave = redis.NewScript(`
if redis.call('get', KEYS[2]) ~= ARGV[2] then
  return 0
end
if redis.call('get', KEYS[4]) ~= ARGV[2] then
  return -1
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
// ErrLeaseLost; a fence/fencegen divergence is the named
// ErrFenceGenerationInconsistent (both in the ErrLeaseLost family) — callers
// translate them into "do not ack".
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
			[]string{stateKey, s.fenceKey(tenantID, state.ExecutionID), indexKey, s.fenceGenKey(tenantID, state.ExecutionID)},
			string(blob), strconv.FormatInt(fenceGen, 10),
			int64(terminalTTL/time.Second), score,
		).Int64()
		if err != nil {
			return fmt.Errorf("%w: %v", ErrStatePersist, err)
		}
		if n == -1 {
			// Fence matched but fencegen disagrees (#54): the write is
			// refused the same way Python refuses it (ExecutionLeaseError) —
			// a refused save never reaches notifyTerminal.
			return fmt.Errorf("%w (gen=%d); state write refused — partial restore suspected", ErrFenceGenerationInconsistent, fenceGen)
		}
		if n == 0 {
			return fmt.Errorf("%w: fence generation mismatch (gen=%d)", ErrLeaseLost, fenceGen)
		}
		s.notifyTerminal(ctx, tenantID, state)
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
	s.notifyTerminal(ctx, tenantID, state)
	return nil
}

// notifyTerminal fires the post-terminal-save callback (quota slot release,
// #30). A failed save never reaches it — a CAS-mismatched write must not
// release a slot still owned by the live holder.
func (s *ExecutionStore) notifyTerminal(ctx context.Context, tenantID string, state *ExecutionState) {
	if s.onTerminal == nil || !TerminalStatuses[state.Status] {
		return
	}
	s.onTerminal(ctx, tenantID, state.ExecutionID)
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
