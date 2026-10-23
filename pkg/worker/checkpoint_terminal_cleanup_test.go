package worker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// #51: the checkpoint key has no TTL — the terminal delete is its only
// reclamation. Two terminal writes skipped it (deadLetterGuard's
// retry-budget exhaustion and terminalizeHandoverExhausted), leaving
// permanent orphans after the state row's 30-day TTL. These tests pin the
// invariant: every terminal write deletes the checkpoint; the suspend
// lifecycle keeps it (no TTL while suspended, §9.4).
//
// PTTL conventions: -2 = key absent (deleted), -1 = present without TTL.

func seedCheckpoint(t *testing.T, rdb *redis.Client, cfg *Config, executionID string) {
	t.Helper()
	rdb.Set(context.Background(),
		cfg.KeyPrefix+":execution:checkpoint:"+executionID,
		`{"version":1,"step":1,"context":{"$INPUT":{}}}`, 0)
}

func checkpointPTTL(t *testing.T, rdb *redis.Client, cfg *Config, executionID string) time.Duration {
	t.Helper()
	return rdb.PTTL(context.Background(), cfg.KeyPrefix+":execution:checkpoint:"+executionID).Val()
}

// Terminal exit 1/2: the DLQ guard's retry-budget exhaustion terminalizes a
// running execution without going through persistErrorTerminal — the
// checkpoint must still be reclaimed.
func TestCheckpointDeletedOnDeadLetterGuardTerminalize(t *testing.T) {
	_, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	cfg.MaxDeliveries = 1
	w := New(cfg, rdb, silentLogger{})
	q := w.queue
	if err := q.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}

	executionID := newExecutionID()
	rdb.Set(ctx, stateKeyOf(cfg, executionID),
		fmt.Sprintf(`{"execution_id":%q,"status":"running"}`, executionID), 0)
	seedCheckpoint(t, rdb, cfg, executionID)
	rdb.Set(ctx, cfg.KeyPrefix+":execution:noderetry:"+executionID+":boom", "1", time.Hour)
	seedPending(t, rdb, cfg, baseStartPayload(executionID))
	time.Sleep(120 * time.Millisecond)

	if _, err := q.Read(ctx); err != nil {
		t.Fatalf("queue read: %v", err)
	}
	state := loadStateRow(t, rdb, cfg, executionID)
	if state.Status != StatusError {
		t.Fatalf("status = %q, want error (guard must terminalize)", state.Status)
	}
	if ttl := checkpointPTTL(t, rdb, cfg, executionID); ttl != -2*time.Nanosecond {
		t.Fatalf("checkpoint PTTL = %v after the guard terminal write, want -2 (deleted); "+
			"a no-TTL key with no terminal delete lives forever", ttl)
	}
}

// Terminal exit 2/2: affinity-handover exhaustion terminalizes through
// terminalizeHandoverExhausted — same reclamation duty.
func TestCheckpointDeletedOnHandoverExhaustedTerminalize(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	executionID := newExecutionID()
	rdb.Set(ctx, cfg.KeyPrefix+":registry:flow_worker:worker-b",
		`{"instance_id":"worker-b","status":"running"}`, time.Minute)
	rdb.Set(ctx, stateKeyOf(cfg, executionID),
		fmt.Sprintf(`{"execution_id":%q,"flow_id":"f9","status":"running"}`, executionID), 0)
	seedCheckpoint(t, rdb, cfg, executionID)
	rdb.Set(ctx, cfg.KeyPrefix+":execution:fence:"+executionID, "2", 7*24*time.Hour)
	rdb.Set(ctx, cfg.KeyPrefix+":execution:fencegen:"+executionID, "2", 0)
	rdb.Set(ctx, cfg.KeyPrefix+":execution:affinityhandover:"+executionID,
		fmt.Sprintf("%d", AffinityHandoverMax), 7*24*time.Hour)

	payload := baseStartPayload(executionID)
	payload["target_instance_id"] = "worker-b"
	enqueueTask(t, rdb, cfg, payload, "")

	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	state := loadStateRow(t, rdb, cfg, executionID)
	if state.Status != StatusError || state.Error["type"] != "AffinityHandoverExhausted" {
		t.Fatalf("status = %q error = %v, want AffinityHandoverExhausted", state.Status, state.Error)
	}
	if ttl := checkpointPTTL(t, rdb, cfg, executionID); ttl != -2*time.Nanosecond {
		t.Fatalf("checkpoint PTTL = %v after the handover-exhausted terminal write, want -2 (deleted)", ttl)
	}
}

// Conservation anchor (§9.4): a suspended execution keeps its checkpoint,
// without a TTL — a human approval may wait arbitrarily long and the resume
// path needs the snapshot.
func TestCheckpointKeptWhileSuspended(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	presetFlowJSON(t, w, "suspend_anchor_flow", "1", testEventFlow("suspend_anchor_flow", ""))

	executionID := newExecutionID()
	enqueueTask(t, rdb, cfg, startPayloadFor("suspend_anchor_flow", "1", executionID), "")
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("start ProcessOne: %v", err)
	}
	if state := loadStateRow(t, rdb, cfg, executionID); state.Status != StatusSuspended {
		t.Fatalf("status = %q, want suspended", state.Status)
	}
	if ttl := checkpointPTTL(t, rdb, cfg, executionID); ttl != -1*time.Nanosecond {
		t.Fatalf("checkpoint PTTL = %v while suspended, want -1 (present, no TTL)", ttl)
	}
}
