package worker

// #50 negative controls: SweepEventTimeouts used to treat the timeout index
// as a re-scannable set — the ZRangeByScore page had no Offset and the
// state-unreadable / dedup-hit exits never removed the member, so 100
// unresolvable zombies pinned the front page and starved every due
// subscription behind them. All three tests were red on the pre-fix tree.

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// seedDueTimeoutSubscription writes a subscription record and arms the
// timeout index at an already-due score, bypassing Register so the test
// controls the ZSET members (score ties break lexicographically by member
// name — the zombies below sort before the valid subscription on purpose).
func seedDueTimeoutSubscription(t *testing.T, rdb *redis.Client, cfg *Config, subID, correlationID string) string {
	t.Helper()
	subKey := cfg.KeyPrefix + ":event:subscriptions:" + subID
	sub := EventSubscription{
		SchemaVersion:  eventSubscriptionSchemaVersion,
		SubscriptionID: subID,
		EventType:      "approval",
		CorrelationID:  correlationID,
		FlowID:         "some_flow",
		NodeID:         "wait",
		TenantID:       "",
		CreatedAt:      nowISO(),
	}
	blob, err := json.Marshal(sub)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	rdb.Set(ctx, subKey, string(blob), 7*24*time.Hour)
	rdb.ZAdd(ctx, cfg.KeyPrefix+":event:subscriptions:timeouts", redis.Z{Score: 1.0, Member: subKey})
	return subKey
}

// TestSweepTimeoutsZombiePageDoesNotBlockValidSubscription (验收 1): 100
// zombies whose execution state row is gone + 1 valid due subscription — a
// SINGLE sweep must produce the valid subscription's timeout carrier. Pre-fix
// the zombies filled page 1 and were never removed, so the valid member
// (sorted last) was never reached.
func TestSweepTimeoutsZombiePageDoesNotBlockValidSubscription(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()

	rdb.Set(ctx, stateKeyOf(cfg, "e-valid"),
		`{"execution_id":"e-valid","status":"suspended","flow_id":"f1","tenant_id":""}`, 0)
	seedDueTimeoutSubscription(t, rdb, cfg, "zzz-valid", "e-valid")
	for i := 0; i < 100; i++ {
		seedDueTimeoutSubscription(t, rdb, cfg, fmt.Sprintf("zomb-%03d", i), fmt.Sprintf("e-zomb-%03d", i))
	}
	timeoutsKey := cfg.KeyPrefix + ":event:subscriptions:timeouts"
	if n := rdb.ZCard(ctx, timeoutsKey).Val(); n != 101 {
		t.Fatalf("seeded ZCard = %d, want 101", n)
	}

	if err := w.SweepEventTimeouts(ctx); err != nil {
		t.Fatalf("SweepEventTimeouts: %v", err)
	}

	msgs := rdb.XRange(ctx, cfg.QueueName, "-", "+").Val()
	if len(msgs) != 1 {
		t.Fatalf("timeout carrier count = %d, want 1 from ONE sweep (page pinned by zombies?)", len(msgs))
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(msgs[0].Values["payload"].(string)), &body); err != nil {
		t.Fatalf("carrier decode: %v", err)
	}
	if body["resume_type"] != "timeout" {
		t.Fatalf("carrier resume_type = %v, want timeout", body["resume_type"])
	}
	if data, _ := body["data"].(map[string]any); data["subscription_id"] != "zzz-valid" {
		t.Fatalf("carrier subscription_id = %v, want zzz-valid (got %v)", data["subscription_id"], data)
	}
	if n := rdb.ZCard(ctx, timeoutsKey).Val(); n != 0 {
		t.Fatalf("ZCard after sweep = %d, want 0 (zombies must leave the index)", n)
	}
}

// TestSweepTimeoutsEnqueueDropsIndexMember (验收 2): after a successful
// timeout enqueue the member must leave the index (ZCard decreases) so the
// next sweep neither re-scans it nor re-arms the dedup after its 1h window.
func TestSweepTimeoutsEnqueueDropsIndexMember(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()

	rdb.Set(ctx, stateKeyOf(cfg, "e1"),
		`{"execution_id":"e1","status":"suspended","flow_id":"f1","tenant_id":""}`, 0)
	seedDueTimeoutSubscription(t, rdb, cfg, "sub-1", "e1")
	timeoutsKey := cfg.KeyPrefix + ":event:subscriptions:timeouts"

	if err := w.SweepEventTimeouts(ctx); err != nil {
		t.Fatalf("SweepEventTimeouts: %v", err)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 1 {
		t.Fatalf("timeout carrier count = %d, want 1", n)
	}
	if n := rdb.ZCard(ctx, timeoutsKey).Val(); n != 0 {
		t.Fatalf("ZCard after enqueue = %d, want 0 (member must not pin the page for the dedup window)", n)
	}
	if n := rdb.HLen(ctx, cfg.KeyPrefix+":event:subscriptions:timeout_attempts").Val(); n != 0 {
		t.Fatalf("attempts bookkeeping left behind: %d", n)
	}

	// Second sweep: nothing left to re-arm — no duplicate carrier.
	if err := w.SweepEventTimeouts(ctx); err != nil {
		t.Fatalf("second SweepEventTimeouts: %v", err)
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 1 {
		t.Fatalf("carrier count after second sweep = %d, want 1 (no duplicate)", n)
	}
}

// TestSweepTimeoutsStateLoadFailureRequeuesThenDrops: a momentarily unreadable
// state row must not sit at the front of the page forever — requeued with an
// attempts ceiling, dropped with the bookkeeping cleaned once exceeded, and
// never turned into a carrier.
func TestSweepTimeoutsStateLoadFailureRequeuesThenDrops(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()

	rdb.Set(ctx, stateKeyOf(cfg, "e-bad"), "{not-json", 0) // LoadExecutionState errors
	subKey := seedDueTimeoutSubscription(t, rdb, cfg, "sub-bad", "e-bad")
	timeoutsKey := cfg.KeyPrefix + ":event:subscriptions:timeouts"
	attemptsKey := cfg.KeyPrefix + ":event:subscriptions:timeout_attempts"

	dropped := false
	for i := 0; i < 12 && !dropped; i++ {
		if err := w.SweepEventTimeouts(ctx); err != nil {
			t.Fatalf("sweep %d: %v", i+1, err)
		}
		if rdb.ZCard(ctx, timeoutsKey).Val() == 0 {
			dropped = true
			break
		}
		// Collapse the requeue wait: the sweep parked the member in the
		// future; pull it back into the due window for the next round.
		rdb.ZAdd(ctx, timeoutsKey, redis.Z{Score: 1.0, Member: subKey})
	}
	if !dropped {
		t.Fatalf("member still in the index after 12 sweeps (pinned at the page front?)")
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("carrier enqueued for an unreadable state: XLEN=%d", n)
	}
	if n := rdb.HLen(ctx, attemptsKey).Val(); n != 0 {
		t.Fatalf("attempts bookkeeping left behind on drop: %d", n)
	}
}
