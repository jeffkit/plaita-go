package worker

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Execution lease protocol (protocol §3). The three Lua scripts below are
// VERBATIM transplants of plaita/server/execution_lease.py (§3.2 quotes them
// with file:line references); their semantics must not be "improved".

// luaLeaseAcquire is fenced acquire (execution_lease.py:68-80).
// KEYS[1]=lease key, KEYS[2]=fence key, KEYS[3]=fencegen key;
// ARGV[1]=holder, ARGV[2]=ttl seconds, ARGV[3]=fence TTL seconds.
// Returns the fence generation (>0) or 0 = already held.
var luaLeaseAcquire = redis.NewScript(`
if redis.call('get', KEYS[1]) then
  return 0
end
local floor = tonumber(redis.call('get', KEYS[2])) or 0
local gen = redis.call('incr', KEYS[3])
if gen <= floor then
  gen = redis.call('incrby', KEYS[3], floor - gen + 1)
end
redis.call('set', KEYS[2], tostring(gen), 'EX', ARGV[3])
redis.call('set', KEYS[1], ARGV[1] .. ':' .. tostring(gen), 'EX', ARGV[2])
return gen
`)

// luaLeaseRelease compares-and-deletes the ENTIRE lease value
// (execution_lease.py:33-39). ARGV[1] = full "{holder}:{gen}" value.
var luaLeaseRelease = redis.NewScript(`
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
else
  return 0
end
`)

// luaLeaseRenew re-EXPIREs only when the full value still matches
// (execution_lease.py:42-48).
var luaLeaseRenew = redis.NewScript(`
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('expire', KEYS[1], ARGV[2])
else
  return 0
end
`)

// ExecutionLease manages the tenant-routed lease/fence/fencegen key triple.
type ExecutionLease struct {
	client *redis.Client
	cfg    *Config
}

func NewExecutionLease(client *redis.Client, cfg *Config) *ExecutionLease {
	return &ExecutionLease{client: client, cfg: cfg}
}

// LeaseHandle carries everything the worker needs to release/renew/CAS-write
// under a held lease.
type LeaseHandle struct {
	ExecutionID string
	TenantID    string
	Holder      string
	// LeaseValue is the full "{holder}:{gen}" string stored in Redis — the
	// compare token for release/renew (§3.2).
	LeaseValue string
	// FenceToken is the acquired generation (>0 when fencing is on).
	FenceToken int64
}

// keys follow §3.1: {ns}:execution:lease|fence|fencegen:{id}. The fencegen
// counter is NEVER deleted and has no TTL (ABA anchor).
func (l *ExecutionLease) leaseKey(tenantID, executionID string) string {
	return l.cfg.Namespace(tenantID) + ":execution:lease:" + executionID
}

func (l *ExecutionLease) fenceKey(tenantID, executionID string) string {
	return l.cfg.Namespace(tenantID) + ":execution:fence:" + executionID
}

func (l *ExecutionLease) fenceGenKey(tenantID, executionID string) string {
	return l.cfg.Namespace(tenantID) + ":execution:fencegen:" + executionID
}

// Acquire runs the fenced acquire script. gen == 0 → ErrExecutionLeaseConflict.
func (l *ExecutionLease) Acquire(ctx context.Context, tenantID, executionID, holder string, ttlSeconds int) (*LeaseHandle, error) {
	gen, err := luaLeaseAcquire.Run(ctx, l.client,
		[]string{l.leaseKey(tenantID, executionID), l.fenceKey(tenantID, executionID), l.fenceGenKey(tenantID, executionID)},
		holder, ttlSeconds, fenceKeyTTLSeconds,
	).Int64()
	if err != nil {
		return nil, fmt.Errorf("lease acquire %s: %w", executionID, err)
	}
	if gen == 0 {
		return nil, ErrExecutionLeaseConflict
	}
	return &LeaseHandle{
		ExecutionID: executionID,
		TenantID:    tenantID,
		Holder:      holder,
		LeaseValue:  holder + ":" + strconv.FormatInt(gen, 10),
		FenceToken:  gen,
	}, nil
}

// Release deletes the lease only when the full value matches (no-op → 0).
func (l *ExecutionLease) Release(ctx context.Context, h *LeaseHandle) error {
	_, err := luaLeaseRelease.Run(ctx, l.client,
		[]string{l.leaseKey(h.TenantID, h.ExecutionID)}, h.LeaseValue).Int64()
	return err
}

// Renew refreshes the lease TTL when the value still matches; a 0 return is
// the "lost lease" signal (§3.4 watchdog).
func (l *ExecutionLease) Renew(ctx context.Context, h *LeaseHandle, ttlSeconds int) (bool, error) {
	n, err := luaLeaseRenew.Run(ctx, l.client,
		[]string{l.leaseKey(h.TenantID, h.ExecutionID)}, h.LeaseValue, ttlSeconds).Int64()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// GetHolder returns the current full lease value, or "" when absent.
func (l *ExecutionLease) GetHolder(ctx context.Context, tenantID, executionID string) (string, error) {
	v, err := l.client.Get(ctx, l.leaseKey(tenantID, executionID)).Result()
	if err == redis.Nil {
		return "", nil
	}
	return v, err
}

// NewHolderToken builds the lease holder token (§3.3): with a registry
// instance id → "{prefix}:{instance_id}:{uuid4hex[:16]}"; the LAST segment is
// always exactly 16 hex chars — that property is part of the wire format
// (holder_instance_id parsing, #155).
func NewHolderToken(prefix, instanceID string) string {
	if instanceID == "" {
		return prefix + ":" + randomHex16()
	}
	return prefix + ":" + instanceID + ":" + randomHex16()
}

func randomHex16() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failure is catastrophic; fall back to a time-derived
		// value rather than panic the consumer loop.
		sum := sha256.Sum256([]byte(time.Now().String()))
		copy(b[:], sum[:8])
	}
	return hex.EncodeToString(b[:])
}

// HolderInstanceID reverse-parses a lease value back to the holder's registry
// instance id (§3.3): 3 or 4 colon-separated segments with the first in
// (start, resume, worker); a 3-segment value whose last part is a non-16-hex
// pure number is the legacy "{holder}:{gen}" format → not parseable.
func HolderInstanceID(leaseValue string) (string, bool) {
	if leaseValue == "" {
		return "", false
	}
	parts := splitColon(leaseValue)
	// lease value = holder (+ ":gen" when fenced): drop a trailing 4th part.
	if len(parts) == 4 {
		parts = parts[:3]
	}
	if len(parts) != 3 {
		return "", false
	}
	switch parts[0] {
	case "start", "resume", "worker":
	default:
		return "", false
	}
	if len(parts[2]) != 16 || !isHex(parts[2]) {
		return "", false
	}
	if parts[1] == "" {
		return "", false
	}
	return parts[1], true
}

func splitColon(s string) []string {
	var out []string
	cur := ""
	for _, r := range s {
		if r == ':' {
			out = append(out, cur)
			cur = ""
			continue
		}
		cur += string(r)
	}
	out = append(out, cur)
	return out
}

func isHex(s string) bool {
	for _, r := range s {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
			return false
		}
	}
	return true
}
