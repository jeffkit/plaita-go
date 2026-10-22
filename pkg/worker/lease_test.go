package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestLeaseAcquireReleaseConflict covers the fenced acquire/release protocol
// (§3.2, §3.4): mutual exclusion, generation monotonicity, value-compare
// release/renew.
func TestLeaseAcquireReleaseConflict(t *testing.T) {
	_, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	lease := NewExecutionLease(rdb, cfg)
	executionID := newExecutionID()

	h1, err := lease.Acquire(ctx, "", executionID, NewHolderToken("start", "inst-a"), 60)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if h1.FenceToken != 1 {
		t.Fatalf("first gen = %d, want 1", h1.FenceToken)
	}
	if h1.LeaseValue != h1.Holder+":1" {
		t.Fatalf("lease value = %q, want %q", h1.LeaseValue, h1.Holder+":1")
	}
	// Lease value shape on the wire: {holder}:{gen} (§3.2).
	if got := rdb.Get(ctx, cfg.KeyPrefix+":execution:lease:"+executionID).Val(); got != h1.LeaseValue {
		t.Fatalf("stored lease = %q, want %q", got, h1.LeaseValue)
	}

	// Held by another → conflict, generation unchanged.
	if _, err := lease.Acquire(ctx, "", executionID, NewHolderToken("start", "inst-b"), 60); !errors.Is(err, ErrExecutionLeaseConflict) {
		t.Fatalf("second acquire = %v, want ErrExecutionLeaseConflict", err)
	}

	// Renew with the right value extends; with the wrong value reports loss.
	if ok, err := lease.Renew(ctx, h1, 60); err != nil || !ok {
		t.Fatalf("renew: ok=%v err=%v", ok, err)
	}
	forged := &LeaseHandle{ExecutionID: executionID, LeaseValue: "start:inst-a:ffffffffffffffff:1"}
	if ok, _ := lease.Renew(ctx, forged, 60); ok {
		t.Fatal("renew with wrong value must report loss")
	}

	// Release only on exact value match.
	if err := lease.Release(ctx, h1); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := lease.Release(ctx, h1); err != nil {
		t.Fatalf("double release must be a no-op success, got %v", err)
	}

	// Generation strictly increases across acquires (ABA anchor, §3.1).
	h2, err := lease.Acquire(ctx, "", executionID, NewHolderToken("start", "inst-b"), 60)
	if err != nil {
		t.Fatalf("re-acquire: %v", err)
	}
	if h2.FenceToken != 2 {
		t.Fatalf("second gen = %d, want 2", h2.FenceToken)
	}
	// fencegen counter never cleared, no TTL (§3.1).
	if ttl := rdb.TTL(ctx, cfg.KeyPrefix+":execution:fencegen:"+executionID).Val(); ttl > 0 {
		t.Fatalf("fencegen TTL = %v, want -1 (no expiry)", ttl)
	}
	// Fence key TTL ~ 7 days.
	if ttl := rdb.TTL(ctx, cfg.KeyPrefix+":execution:fence:"+executionID).Val(); ttl < 7*24*time.Hour-time.Hour {
		t.Fatalf("fence TTL = %v, want ~7d", ttl)
	}
}

// TestLeaseTTLExpiry: a lease past its TTL is acquirable again.
func TestLeaseTTLExpiry(t *testing.T) {
	_, rdb, mr, cfg := newTestWorker(t)
	ctx := context.Background()
	lease := NewExecutionLease(rdb, cfg)
	executionID := newExecutionID()
	if _, err := lease.Acquire(ctx, "", executionID, NewHolderToken("start", "inst-a"), 1); err != nil {
		t.Fatal(err)
	}
	mr.FastForward(2 * time.Second)
	h2, err := lease.Acquire(ctx, "", executionID, NewHolderToken("start", "inst-b"), 60)
	if err != nil {
		t.Fatalf("acquire after TTL: %v", err)
	}
	if h2.FenceToken <= 1 {
		t.Fatalf("gen after expiry = %d, want > 1", h2.FenceToken)
	}
}

// TestHolderTokenFormat: the holder token wire format (§3.3) —
// {prefix}:{instance_id}:{uuid4hex[:16]}, last segment exactly 16 hex chars.
func TestHolderTokenFormat(t *testing.T) {
	tok := NewHolderToken("start", "my-instance")
	parts := strings.Split(tok, ":")
	if len(parts) != 3 || parts[0] != "start" || parts[1] != "my-instance" {
		t.Fatalf("token = %q, want start:my-instance:<hex16>", tok)
	}
	if len(parts[2]) != 16 || !isHex(parts[2]) {
		t.Fatalf("token tail = %q, want 16 hex chars", parts[2])
	}
	// Unique per call.
	if NewHolderToken("start", "my-instance") == tok {
		t.Fatal("holder tokens must be unique per call")
	}
	// Fallback without instance id.
	bare := NewHolderToken("start", "")
	if !strings.HasPrefix(bare, "start:") || len(strings.Split(bare, ":")) != 2 {
		t.Fatalf("bare token = %q, want start:<hex16>", bare)
	}
}

// TestHolderInstanceIDParse: reverse parsing rules (§3.3 holder_instance_id).
func TestHolderInstanceIDParse(t *testing.T) {
	cases := []struct {
		value string
		want  string
		ok    bool
	}{
		{"start:inst:0123456789abcdef:7", "inst", true},  // fenced new format
		{"resume:inst:0123456789abcdef", "inst", true},   // unfenced new format
		{"worker:inst:0123456789ABCDEF:1", "inst", true}, // uppercase hex ok
		{"start:0123456789abcdef", "", false},            // 2 segments
		{"foo:inst:0123456789abcdef:1", "", false},       // unknown prefix
		{"start:inst:123:1", "", false},                  // legacy holder+gen tail
		{"start:inst:0123456789abcdefg:1", "", false},    // 17 chars
		{"start::0123456789abcdef:1", "", false},         // empty instance
		{"", "", false},
	}
	for _, c := range cases {
		got, ok := HolderInstanceID(c.value)
		if got != c.want || ok != c.ok {
			t.Fatalf("HolderInstanceID(%q) = (%q,%v), want (%q,%v)", c.value, got, ok, c.want, c.ok)
		}
	}
}

// TestTenantNamespaceRouting: ns = plaita for default/empty, plaita:{tenant}
// otherwise (§3.1).
func TestTenantNamespaceRouting(t *testing.T) {
	cfg := testConfig()
	if cfg.Namespace("") != cfg.KeyPrefix {
		t.Fatal("empty tenant must use the plain prefix")
	}
	if cfg.Namespace("default") != cfg.KeyPrefix {
		t.Fatal("default tenant must use the plain prefix")
	}
	if cfg.Namespace("acme") != cfg.KeyPrefix+":acme" {
		t.Fatalf("tenant ns = %q", cfg.Namespace("acme"))
	}
}

// TestFencedStateSave: the CAS write rejects stale generations (§3.4 write
// side).
func TestFencedStateSave(t *testing.T) {
	_, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	store := NewExecutionStore(rdb, cfg)
	lease := NewExecutionLease(rdb, cfg)
	executionID := newExecutionID()

	h, err := lease.Acquire(ctx, "", executionID, NewHolderToken("start", "inst"), 60)
	if err != nil {
		t.Fatal(err)
	}
	state := &ExecutionState{
		ExecutionID: executionID, Status: StatusRunning,
		StartTime: nowISO(), LastUpdateTime: nowISO(),
		Context: map[string]any{},
	}
	if err := store.SaveExecutionState(ctx, "", state, h.FenceToken); err != nil {
		t.Fatalf("fenced save: %v", err)
	}
	if got, err := store.LoadExecutionState(ctx, "", executionID); err != nil || got == nil || got.Status != StatusRunning {
		t.Fatalf("state after fenced save = %+v err=%v", got, err)
	}

	// Stale generation (simulates a lost lease) → ErrLeaseLost.
	if err := store.SaveExecutionState(ctx, "", state, h.FenceToken+41); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale save = %v, want ErrLeaseLost", err)
	}

	// Plain write path (gen=0) still works (start-without-lease / rollback).
	state2 := &ExecutionState{
		ExecutionID: newExecutionID(), Status: StatusRunning,
		StartTime: nowISO(), LastUpdateTime: nowISO(), Context: map[string]any{},
	}
	if err := store.SaveExecutionState(ctx, "", state2, 0); err != nil {
		t.Fatalf("plain save: %v", err)
	}

	// Terminal rows carry the state TTL (§5.2: 30 days).
	state.Status = StatusCompleted
	state.EndTime = nowISO()
	if err := store.SaveExecutionState(ctx, "", state, h.FenceToken); err != nil {
		t.Fatalf("terminal save: %v", err)
	}
	ttl := rdb.TTL(ctx, cfg.KeyPrefix+":execution:"+executionID).Val()
	if ttl <= 0 {
		t.Fatalf("terminal state TTL = %v, want positive", ttl)
	}

	// Corrupt row → ErrStateLoad, never swallowed as missing (§5.2).
	rdb.Set(ctx, cfg.KeyPrefix+":execution:"+state2.ExecutionID, "{broken", 0)
	if _, err := store.LoadExecutionState(ctx, "", state2.ExecutionID); !errors.Is(err, ErrStateLoad) {
		t.Fatalf("corrupt load = %v, want ErrStateLoad", err)
	}
	// Missing row → (nil, nil).
	if st, err := store.LoadExecutionState(ctx, "", newExecutionID()); err != nil || st != nil {
		t.Fatalf("missing load = (%v,%v), want (nil,nil)", st, err)
	}
}

// TestFencedStateSaveRefusesStaleGenerationAfterPartialRestore (#54,
// A2R25-05): a partial restore brings back an old state+fence pair while the
// never-cleared fencegen counter stays at the newer generation. The stale
// token must NOT re-hit the CAS: the save is refused with the named
// fencegen-mismatch error (Python returns -1 → ExecutionLeaseError).
func TestFencedStateSaveRefusesStaleGenerationAfterPartialRestore(t *testing.T) {
	_, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	store := NewExecutionStore(rdb, cfg)
	executionID := newExecutionID()

	state := &ExecutionState{
		ExecutionID: executionID, Status: StatusRunning,
		StartTime: nowISO(), LastUpdateTime: nowISO(),
		Context: map[string]any{},
	}
	// Post-restore shape: fence=3 came back, fencegen stayed at 5.
	rdb.Set(ctx, cfg.KeyPrefix+":execution:fence:"+executionID, "3", 0)
	rdb.Set(ctx, cfg.KeyPrefix+":execution:fencegen:"+executionID, "5", 0)
	rdb.Set(ctx, stateKeyOf(cfg, executionID),
		fmt.Sprintf(`{"execution_id":%q,"status":"running"}`, executionID), 0)

	err := store.SaveExecutionState(ctx, "", state, 3)
	if !errors.Is(err, ErrFenceGenerationInconsistent) {
		t.Fatalf("stale-token save after partial restore = %v, want ErrFenceGenerationInconsistent", err)
	}
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("fencegen mismatch must keep the lost-write-authority semantics, got %v", err)
	}
	if got, _ := store.LoadExecutionState(ctx, "", executionID); got == nil || got.Status != StatusRunning {
		t.Fatalf("row was modified by the refused save: %+v", got)
	}

	// A missing fencegen key is the same divergence (Python: -1 as well).
	rdb.Del(ctx, cfg.KeyPrefix+":execution:fencegen:"+executionID)
	if err := store.SaveExecutionState(ctx, "", state, 3); !errors.Is(err, ErrFenceGenerationInconsistent) {
		t.Fatalf("save with missing fencegen = %v, want ErrFenceGenerationInconsistent", err)
	}

	// Consistent fence/fencegen pair → the write goes through.
	rdb.Set(ctx, cfg.KeyPrefix+":execution:fencegen:"+executionID, "3", 0)
	if err := store.SaveExecutionState(ctx, "", state, 3); err != nil {
		t.Fatalf("consistent fence/fencegen save: %v", err)
	}
}

// TestCancelFlag: the cancel intent key (§4.1) reads back as requested.
func TestCancelFlag(t *testing.T) {
	_, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	store := NewExecutionStore(rdb, cfg)
	executionID := newExecutionID()

	if hit, _, err := store.IsCancelRequested(ctx, "", executionID); err != nil || hit {
		t.Fatalf("fresh execution cancel = (%v,%v), want (false,nil)", hit, err)
	}
	rdb.Set(ctx, store.CancelKey("", executionID), nowISO(), time.Minute)
	hit, val, err := store.IsCancelRequested(ctx, "", executionID)
	if err != nil || !hit || val == "" {
		t.Fatalf("cancel after flag = (%v,%q,%v), want (true,iso,nil)", hit, val, err)
	}
}
