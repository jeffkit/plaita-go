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

// Per-tenant start admission quota (issue #30, Python plaita#28 /
// server/tenant_quota.py 最小公平性原语). One ZSET semaphore per tenant:
//
//	{prefix}:tenant_quota          HASH tenant_id → max concurrent starts
//	{prefix}:tenant_running:{t}    ZSET member=execution_id, score=expiry
//
// Both are platform mechanism keys under the namespace root (NOT partitioned
// per tenant) so any worker instance enforces the same view; the shadow
// prefix keeps test/shadow runs off production keys.
//
// Semantics:
//
//   - DEFAULT IS UNLIMITED: with no configured cap the gate is bypassed
//     entirely (no Redis writes) — quota enforcement requires an explicit
//     PLAITA_TENANT_MAX_RUNNING or a per-tenant HASH entry (Python fail-open
//     matrix: ≤0 / unparsable / missing / Redis failure = unlimited).
//   - ADMIT (AdmitStart) runs one atomic Lua: purge expired members (crash
//     self-heal), idempotently refresh an existing member (message redelivery
//     safe), ZADD + key EXPIRE when below the cap, deny when full.
//   - RELEASE happens at the single terminal-write choke point
//     (ExecutionStore.SetOnTerminalSave — the Python _persist_state_or_raise
//     收口 release_tenant_start): completed/error/cancelled ZREM the slot;
//     suspended keeps it (the execution is still inside its admitted
//     lifecycle and the resume carrier reuses the slot). A worker crash
//     between admit and terminal self-heals via the entry score TTL
//     (TenantRunningTTLSeconds, default 86400).
//   - OVER-LIMIT starts are deferred, never executed and never dead-lettered:
//     the carrier is re-enqueued at the stream TAIL (other tenants' earlier
//     messages get priority — the FIFO fairness that keeps one tenant from
//     starving the others) with a quota_defers counter, and the original is
//     acked (enqueue→ack order; a crash in between only duplicates, the lease
//     + idempotency gate serialize). After QuotaDeferMax defers the copy
//     carries quota_bypass=true and is admitted over the cap — liveness wins
//     over the soft quota. Only START carriers are gated; resume is rescue
//     traffic and never deferred.
//   - Rollback: PLAITA_DISABLE_TENANT_QUOTA=1 bypasses the whole gate.

// luaAdmitTenantStart is the atomic admit (port of tenant_quota._ADMIT_LUA,
// returning {allowed, running} for the quota gauge). KEYS[1]=running ZSET;
// ARGV: now, ttl, cap, member.
var luaAdmitTenantStart = redis.NewScript(`
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
if redis.call('ZSCORE', KEYS[1], ARGV[4]) then
  redis.call('ZADD', KEYS[1], tonumber(ARGV[1]) + tonumber(ARGV[2]), ARGV[4])
  redis.call('EXPIRE', KEYS[1], math.ceil(tonumber(ARGV[2])) + 60)
  return {1, redis.call('ZCARD', KEYS[1])}
end
if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[3]) then
  return {0, redis.call('ZCARD', KEYS[1])}
end
redis.call('ZADD', KEYS[1], tonumber(ARGV[1]) + tonumber(ARGV[2]), ARGV[4])
redis.call('EXPIRE', KEYS[1], math.ceil(tonumber(ARGV[2])) + 60)
return {1, redis.call('ZCARD', KEYS[1])}
`)

// TenantQuota owns the per-tenant start semaphore.
type TenantQuota struct {
	client *redis.Client
	cfg    *Config
	logger Logger
}

func NewTenantQuota(client *redis.Client, cfg *Config, logger Logger) *TenantQuota {
	return &TenantQuota{client: client, cfg: cfg, logger: logger}
}

func (q *TenantQuota) quotaKey() string { return q.cfg.KeyPrefix + ":tenant_quota" }

func (q *TenantQuota) runningKey(tenantID string) string {
	tenant := tenantID
	if tenant == "" {
		tenant = q.cfg.DefaultTenantID
	}
	return q.cfg.KeyPrefix + ":tenant_running:" + tenant
}

// resolveCap resolves the tenant's max concurrent starts; ok=false = unlimited
// (no cap configured / rollback switch / lookup failure — all fail-open).
// Priority: per-tenant HASH entry > global env default (Config.TenantMaxRunning).
func (q *TenantQuota) resolveCap(ctx context.Context, tenantID string) (int, bool) {
	if q.cfg.DisableTenantQuota {
		return 0, false
	}
	raw, err := q.client.HGet(ctx, q.quotaKey(), q.runningTenant(tenantID)).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		// A quota-gate outage must not amplify into a global rejection.
		q.logger.Warn("tenant quota lookup failed, treating as unlimited",
			"tenant", q.runningTenant(tenantID), "error", err)
		return 0, false
	}
	if raw != "" {
		if n, perr := strconv.Atoi(raw); perr == nil && n > 0 {
			return n, true
		}
		return 0, false // recorded but ≤0/garbage = unlimited (0=全拒 is tenant_status's job)
	}
	if q.cfg.TenantMaxRunning > 0 {
		return q.cfg.TenantMaxRunning, true
	}
	return 0, false
}

func (q *TenantQuota) runningTenant(tenantID string) string {
	if tenantID == "" {
		return q.cfg.DefaultTenantID
	}
	return tenantID
}

// AdmitStart takes one concurrent-start slot for the tenant. allowed=false
// means the tenant's quota is full. Unconfigured/failing gates admit
// everything (fail-open), so the default deployment sees zero behavior
// change. running is the post-admit occupancy for the quota gauge (0 when
// unlimited).
func (q *TenantQuota) AdmitStart(ctx context.Context, tenantID, executionID string) (allowed bool, running int) {
	if executionID == "" {
		return true, 0 // untrackable legacy carrier: nothing to semaphore on
	}
	limit, ok := q.resolveCap(ctx, tenantID)
	if !ok {
		return true, 0
	}
	now := float64(time.Now().UnixNano()) / 1e9
	res, err := luaAdmitTenantStart.Run(ctx, q.client, []string{q.runningKey(tenantID)},
		strconv.FormatFloat(now, 'f', 6, 64),
		strconv.FormatFloat(float64(q.cfg.TenantRunningTTLSeconds), 'f', 6, 64),
		strconv.Itoa(limit), executionID,
	).Slice()
	if err != nil {
		q.logger.Warn("tenant start admission failed (fail-open admit)",
			"tenant", q.runningTenant(tenantID), "execution_id", executionID, "error", err)
		return true, 0
	}
	if len(res) != 2 {
		return true, 0 // unexpected script shape: fail-open like any other gate outage
	}
	allowed = toInt(res[0]) == 1
	running = toInt(res[1])
	return allowed, running
}

// Release returns one start slot (terminal executions; idempotent,
// best-effort — the entry TTL is the leak backstop).
func (q *TenantQuota) Release(ctx context.Context, tenantID, executionID string) {
	if executionID == "" {
		return
	}
	if err := q.client.ZRem(ctx, q.runningKey(tenantID), executionID).Err(); err != nil {
		q.logger.Warn("tenant start slot release failed (entry TTL backstop)",
			"tenant", q.runningTenant(tenantID), "execution_id", executionID, "error", err)
	}
}

// deferOverQuotaStart implements the over-quota disposition (issue #30, Python
// _defer_over_quota_start): re-enqueue the carrier at the stream TAIL with a
// quota_defers counter, then ack the original — the enqueue→ack order is
// load-bearing (a crash in between duplicates; the lease + the #29
// idempotency gate serialize, never lose). The tail placement gives the FIFO
// fairness that keeps one full tenant from starving the others, and the fresh
// copy restarts delivery at 1 so deferral never burns the delivery budget
// into a dead-letter. Bounded: after QuotaDeferMax defers the copy carries
// quota_bypass=true and is admitted over the cap (liveness > soft quota).
// Returns (handled=true, err) with a non-nil error only when the re-enqueue
// failed and the original must stay pending for reclaim.
func (w *Worker) deferOverQuotaStart(ctx context.Context, delivery *Delivery) (bool, error) {
	task := delivery.Task
	// Preserve unknown payload fields byte-for-byte when a raw envelope is
	// available (same pattern as the handover path).
	body := map[string]any{}
	if err := json.Unmarshal([]byte(delivery.Envelope.PayloadRaw), &body); err != nil || body == nil {
		body = taskPayloadDict(task)
	}
	defers := task.QuotaDefers + 1
	body["quota_defers"] = defers
	if defers > w.cfg.QuotaDeferMax {
		body["quota_bypass"] = true
		w.metrics.IncCounter(MetricQuotaBypassedTotal, metricLabels("tenant", task.TenantID))
		w.logger.Warn("tenant start quota still full after the defer budget; admitting with quota_bypass (liveness over soft quota)",
			"tenant", task.TenantID, "execution_id", task.ExecutionID, "defers", defers-1)
	} else {
		w.metrics.IncCounter(MetricQuotaDeferredTotal, metricLabels("tenant", task.TenantID))
		w.logger.Info("tenant start quota full; carrier re-enqueued at the stream tail",
			"tenant", task.TenantID, "execution_id", task.ExecutionID,
			"defers", defers, "max", w.cfg.QuotaDeferMax)
	}
	if _, err := w.queue.Enqueue(ctx, body, task.EnvelopeSchemaVer); err != nil {
		w.logger.Warn("quota defer re-enqueue failed; original left pending for reclaim",
			"execution_id", task.ExecutionID, "error", err)
		return true, fmt.Errorf("quota defer enqueue: %w", err)
	}
	if err := w.queue.Ack(ctx, task.MessageID); err != nil {
		w.logger.Warn("quota defer enqueued but the original ack failed (duplicate delivery, gate-serialized)",
			"execution_id", task.ExecutionID, "error", err)
		return true, nil
	}
	return true, nil
}
