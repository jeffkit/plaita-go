package worker

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// runningKeyOf exposes the platform-level semaphore key for assertions.
func runningKeyOf(cfg *Config, tenant string) string {
	if tenant == "" {
		tenant = cfg.DefaultTenantID
	}
	return cfg.KeyPrefix + ":tenant_running:" + tenant
}

func seedRunningSlot(t *testing.T, rdb *redis.Client, cfg *Config, tenant, executionID string, ttl time.Duration) {
	t.Helper()
	if err := rdb.ZAdd(context.Background(),
		runningKeyOf(cfg, tenant),
		redis.Z{Score: float64(time.Now().Add(ttl).UnixNano()) / 1e9, Member: executionID},
	).Err(); err != nil {
		t.Fatal(err)
	}
}

// presetHelloWorldTenant presets the hello-world IR into a TENANT namespace
// (quota tests route carriers through tenant_id, and the flow store is
// tenant-partitioned like production).
func presetHelloWorldTenant(t *testing.T, w *Worker, tenant string) {
	t.Helper()
	raw, err := osReadHelloWorld()
	if err != nil {
		t.Fatal(err)
	}
	if err := w.flows.PresetFlowDefinition(context.Background(), tenant, "hello_world_go", "latest", raw); err != nil {
		t.Fatal(err)
	}
}

func tenantStateKeyOf(cfg *Config, tenant, executionID string) string {
	return cfg.Namespace(tenant) + ":execution:" + executionID
}

func loadTenantStateRow(t *testing.T, rdb *redis.Client, cfg *Config, tenant, executionID string) *ExecutionState {
	t.Helper()
	raw, err := rdb.Get(context.Background(), tenantStateKeyOf(cfg, tenant, executionID)).Result()
	if err != nil {
		t.Fatalf("tenant state row read: %v", err)
	}
	var state ExecutionState
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		t.Fatalf("tenant state row decode: %v", err)
	}
	return &state
}

// TestTenantQuotaUnlimitedByDefault (#30 验收「缺省不限」): with no configured
// cap the gate never touches Redis — no semaphore key exists and the start
// executes.
func TestTenantQuotaUnlimitedByDefault(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	if w.cfg.TenantMaxRunning != 0 {
		t.Fatalf("default TenantMaxRunning = %d, want 0 (unlimited)", w.cfg.TenantMaxRunning)
	}
	ctx := context.Background()
	presetHelloWorldTenant(t, w, "acme")

	payload := baseStartPayload(newExecutionID())
	payload["tenant_id"] = "acme"
	enqueueTask(t, rdb, cfg, payload, "")
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	if n := rdb.Exists(ctx, runningKeyOf(cfg, "acme")).Val(); n != 0 {
		t.Fatal("semaphore key created despite no configured cap (must be bypassed entirely)")
	}
}

// TestTenantQuotaCapDefersThenRunsAfterRelease (#30 验收「单租户并发不超过配
// 置 / 配额释放正确」): a full tenant defers the carrier to the tail (acked,
// never executed, never dead-lettered); after a slot is released the deferred
// copy executes and its own slot is released at the terminal write.
func TestTenantQuotaCapDefersThenRunsAfterRelease(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	w.cfg.TenantMaxRunning = 1
	rec := newRecordingMetrics()
	w.SetMetrics(rec)
	ctx := context.Background()
	presetHelloWorldTenant(t, w, "acme")

	// Simulate an in-flight admitted execution of tenant acme.
	seedRunningSlot(t, rdb, cfg, "acme", "occupied-exec", time.Hour)

	executionID := newExecutionID()
	payload := baseStartPayload(executionID)
	payload["tenant_id"] = "acme"
	enqueueTask(t, rdb, cfg, payload, "")

	// Round 1: quota full → deferred copy at the tail, original acked.
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("deferred round ProcessOne: %v", err)
	}
	if _, err := rdb.Get(ctx, stateKeyOf(cfg, executionID)).Result(); err == nil {
		t.Fatal("over-quota start must not execute")
	}
	msgs := rdb.XRange(ctx, cfg.QueueName, "-", "+").Val()
	if len(msgs) != 1 {
		t.Fatalf("queue entries = %d, want 1 (the deferred copy)", len(msgs))
	}
	var copy map[string]any
	if err := json.Unmarshal([]byte(msgs[0].Values["payload"].(string)), &copy); err != nil {
		t.Fatal(err)
	}
	if int(toInt(copy["quota_defers"])) != 1 {
		t.Fatalf("quota_defers = %v, want 1", copy["quota_defers"])
	}
	if bypass, _ := copy["quota_bypass"].(bool); bypass {
		t.Fatal("quota_bypass must be absent below the defer budget")
	}
	if got := rec.total(MetricQuotaExceededTotal); got != 1 {
		t.Fatalf("tenant_quota_exceeded_total = %d, want 1", got)
	}
	if got := rec.total(MetricQuotaDeferredTotal); got != 1 {
		t.Fatalf("tenant_quota_deferred_total = %d, want 1", got)
	}
	if n := pendingCount(t, rdb, cfg); n != 0 {
		t.Fatalf("PEL = %d, want 0 (original acked)", n)
	}

	// Release the occupied slot (the terminal-write hook ZREMs in production).
	rdb.ZRem(ctx, runningKeyOf(cfg, "acme"), "occupied-exec")

	// Round 2: the deferred copy executes and its slot is released on terminal.
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("admitted round ProcessOne: %v", err)
	}
	if state := loadTenantStateRow(t, rdb, cfg, "acme", executionID); state.Status != StatusCompleted {
		t.Fatalf("status = %q, want completed", state.Status)
	}
	if n := rdb.ZCard(ctx, runningKeyOf(cfg, "acme")).Val(); n != 0 {
		t.Fatalf("running ZCARD = %d, want 0 (terminal release, no leak)", n)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0", n)
	}
}

// TestTenantQuotaCrossTenantIsolation (#30 验收「其他租户不被饿死」): tenant a
// is full; its carrier is deferred to the tail and tenant b's LATER carrier
// executes ahead of it.
func TestTenantQuotaCrossTenantIsolation(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	w.cfg.TenantMaxRunning = 1
	ctx := context.Background()
	presetHelloWorldTenant(t, w, "tenant-a")
	presetHelloWorldTenant(t, w, "tenant-b")

	seedRunningSlot(t, rdb, cfg, "tenant-a", "occupied-a", time.Hour)

	aID, bID := newExecutionID(), newExecutionID()
	aPayload := baseStartPayload(aID)
	aPayload["tenant_id"] = "tenant-a"
	bPayload := baseStartPayload(bID)
	bPayload["tenant_id"] = "tenant-b"
	enqueueTask(t, rdb, cfg, aPayload, "")
	enqueueTask(t, rdb, cfg, bPayload, "")

	// Round 1 reads tenant-a's carrier (FIFO) and defers it to the tail.
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("round 1: %v", err)
	}
	// Round 2 reads tenant-b's carrier (queued BEFORE the deferred copy) and
	// executes it — tenant-b is not starved by tenant-a's full quota.
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("round 2: %v", err)
	}
	if state := loadTenantStateRow(t, rdb, cfg, "tenant-b", bID); state.Status != StatusCompleted {
		t.Fatalf("tenant-b status = %q, want completed (cross-tenant isolation)", state.Status)
	}
	if _, err := rdb.Get(ctx, tenantStateKeyOf(cfg, "tenant-a", aID)).Result(); err == nil {
		t.Fatal("tenant-a over-quota start must not execute")
	}
	if n := rdb.ZCard(ctx, runningKeyOf(cfg, "tenant-b")).Val(); n != 0 {
		t.Fatalf("tenant-b running ZCARD = %d, want 0 (terminal release)", n)
	}
}

// TestTenantQuotaBypassAfterDeferBudget (#30): at the defer budget the copy
// carries quota_bypass and is admitted over the cap (liveness > soft quota).
func TestTenantQuotaBypassAfterDeferBudget(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	w.cfg.TenantMaxRunning = 1
	rec := newRecordingMetrics()
	w.SetMetrics(rec)
	ctx := context.Background()
	presetHelloWorldTenant(t, w, "acme")

	seedRunningSlot(t, rdb, cfg, "acme", "occupied-exec", time.Hour)

	executionID := newExecutionID()
	payload := baseStartPayload(executionID)
	payload["tenant_id"] = "acme"
	payload["quota_defers"] = 64 // == QuotaDeferMax → next defer bypasses
	enqueueTask(t, rdb, cfg, payload, "")

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("defer round: %v", err)
	}
	if got := rec.total(MetricQuotaBypassedTotal); got != 1 {
		t.Fatalf("tenant_quota_bypassed_total = %d, want 1", got)
	}
	if got := rec.total(MetricQuotaDeferredTotal); got != 0 {
		t.Fatalf("tenant_quota_deferred_total = %d, want 0 (bypassed, not deferred)", got)
	}
	msgs := rdb.XRange(ctx, cfg.QueueName, "-", "+").Val()
	if len(msgs) != 1 {
		t.Fatalf("queue entries = %d, want 1 (the bypass copy)", len(msgs))
	}
	var copy map[string]any
	if err := json.Unmarshal([]byte(msgs[0].Values["payload"].(string)), &copy); err != nil {
		t.Fatal(err)
	}
	if int(toInt(copy["quota_defers"])) != 65 {
		t.Fatalf("quota_defers = %v, want 65", copy["quota_defers"])
	}
	if bypass, _ := copy["quota_bypass"].(bool); !bypass {
		t.Fatalf("quota_bypass = %v, want true at the defer budget", copy["quota_bypass"])
	}

	// The bypass copy executes despite the still-full quota.
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("bypass round: %v", err)
	}
	if state := loadTenantStateRow(t, rdb, cfg, "acme", executionID); state.Status != StatusCompleted {
		t.Fatalf("status = %q, want completed (quota_bypass admitted)", state.Status)
	}
}

// TestTenantQuotaPerTenantHashOverride: the HASH overrides the global default;
// garbage/≤0 entries mean unlimited (fail-open).
func TestTenantQuotaPerTenantHashOverride(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	w.cfg.TenantMaxRunning = 100 // global default
	ctx := context.Background()
	for _, tenant := range []string{"capped", "garbage", "zero"} {
		presetHelloWorldTenant(t, w, tenant)
	}
	quotaKey := cfg.KeyPrefix + ":tenant_quota"
	rdb.HSet(ctx, quotaKey, "capped", "1")
	rdb.HSet(ctx, quotaKey, "garbage", "abc")
	rdb.HSet(ctx, quotaKey, "zero", "0")

	seedRunningSlot(t, rdb, cfg, "capped", "occupied-capped", time.Hour)

	// garbage/zero run FIRST: uncapped tenants execute without interference
	// (after "capped" they would read capped's deferred tail copies).
	for _, tenant := range []string{"garbage", "zero"} {
		id := newExecutionID()
		payload := baseStartPayload(id)
		payload["tenant_id"] = tenant
		enqueueTask(t, rdb, cfg, payload, "")
		if _, err := w.ProcessOne(ctx); err != nil {
			t.Fatalf("%s round: %v", tenant, err)
		}
		if state := loadTenantStateRow(t, rdb, cfg, tenant, id); state.Status != StatusCompleted {
			t.Fatalf("%s status = %q, want completed (≤0/garbage = unlimited)", tenant, state.Status)
		}
	}

	// capped: HASH override (1) beats the global 100 → deferred.
	cappedID := newExecutionID()
	cappedPayload := baseStartPayload(cappedID)
	cappedPayload["tenant_id"] = "capped"
	enqueueTask(t, rdb, cfg, cappedPayload, "")
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("capped round: %v", err)
	}
	if _, err := rdb.Get(ctx, tenantStateKeyOf(cfg, "capped", cappedID)).Result(); err == nil {
		t.Fatal("HASH override cap=1 with a full slot must defer")
	}
}

// TestTenantQuotaDisabledSwitch: PLAITA_DISABLE_TENANT_QUOTA bypasses the gate.
func TestTenantQuotaDisabledSwitch(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	w.cfg.TenantMaxRunning = 1
	w.cfg.DisableTenantQuota = true
	ctx := context.Background()
	presetHelloWorld(t, w)

	seedRunningSlot(t, rdb, cfg, "acme", "occupied-exec", time.Hour)
	payload := baseStartPayload(newExecutionID())
	payload["tenant_id"] = "acme"
	enqueueTask(t, rdb, cfg, payload, "")
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
}

// TestTenantQuotaAdmitIdempotentAndExpiryPurge: the Lua admit is idempotent
// for a redelivered carrier and purges expired slots (crash self-heal).
func TestTenantQuotaAdmitIdempotentAndExpiryPurge(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	w.cfg.TenantMaxRunning = 1
	ctx := context.Background()
	q := NewTenantQuota(rdb, cfg, w.logger)

	// Idempotent member refresh: the same execution admits twice.
	allowed, running := q.AdmitStart(ctx, "acme", "e1")
	if !allowed || running != 1 {
		t.Fatalf("first admit = %v/%d, want true/1", allowed, running)
	}
	allowed, running = q.AdmitStart(ctx, "acme", "e1")
	if !allowed || running != 1 {
		t.Fatalf("redelivered admit = %v/%d, want true/1 (idempotent)", allowed, running)
	}
	// Cap full for a different execution.
	if allowed, _ := q.AdmitStart(ctx, "acme", "e2"); allowed {
		t.Fatal("admit over the cap must be denied")
	}
	// Release e1 (the terminal-write hook does this in production)…
	q.Release(ctx, "acme", "e1")
	// …then a crashed worker's EXPIRED slot must be purged by the admit, so
	// e2 fits even though the stale entry is still in the ZSET.
	rdb.ZAdd(ctx, runningKeyOf(cfg, "acme"),
		redis.Z{Score: float64(time.Now().Add(-time.Hour).UnixNano()) / 1e9, Member: "e0"})
	if allowed, _ := q.AdmitStart(ctx, "acme", "e2"); !allowed {
		t.Fatal("expired slot must be purged (crash self-heal) and admit allowed")
	}
	// Release is idempotent.
	q.Release(ctx, "acme", "e1")
	if n := rdb.ZCard(ctx, runningKeyOf(cfg, "acme")).Val(); n != 1 {
		t.Fatalf("ZCARD = %d, want 1 (only e2 remains)", n)
	}
}

// TestTenantQuotaReleaseHookOnTerminalSave: the store's terminal-write choke
// point releases the slot; a suspended save keeps it (the resume reuses the
// admitted lifecycle).
func TestTenantQuotaReleaseHookOnTerminalSave(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	w.cfg.TenantMaxRunning = 1
	ctx := context.Background()

	allowed, _ := w.quota.AdmitStart(ctx, "acme", "e1")
	if !allowed {
		t.Fatal("admit must succeed")
	}

	suspended := &ExecutionState{
		ExecutionID: "e1", TenantID: "acme", Status: StatusSuspended,
		StartTime: nowISO(), Context: map[string]any{},
	}
	if err := w.store.SaveExecutionState(ctx, "acme", suspended, 0); err != nil {
		t.Fatal(err)
	}
	if n := rdb.ZCard(ctx, runningKeyOf(cfg, "acme")).Val(); n != 1 {
		t.Fatalf("ZCARD after suspended save = %d, want 1 (suspended keeps the slot)", n)
	}

	terminal := &ExecutionState{
		ExecutionID: "e1", TenantID: "acme", Status: StatusCompleted,
		StartTime: nowISO(), EndTime: nowISO(), Context: map[string]any{},
	}
	if err := w.store.SaveExecutionState(ctx, "acme", terminal, 0); err != nil {
		t.Fatal(err)
	}
	if n := rdb.ZCard(ctx, runningKeyOf(cfg, "acme")).Val(); n != 0 {
		t.Fatalf("ZCARD after terminal save = %d, want 0 (released at the choke point)", n)
	}
}
