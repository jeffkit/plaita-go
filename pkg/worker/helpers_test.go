package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// testConfig builds a shadow-namespaced config with test-friendly timings.
func testConfig() *Config {
	cfg := configFromEnv(func(string) string { return "" })
	cfg.KeyPrefix = ShadowKeyPrefix
	cfg.QueueName = ShadowKeyPrefix + ":flow:queue"
	cfg.ConsumerGroup = ShadowKeyPrefix + "-workers"
	cfg.DLQKey = cfg.QueueName + ":dlq"
	cfg.ClaimMinIdleMS = 20
	cfg.ReadBlockMS = 100
	cfg.CancelPollMS = 40
	cfg.LeaseTTLSeconds = 120
	cfg.HeartbeatInterval = time.Hour // keep heartbeat ticks out of test windows
	cfg.SweepInterval = time.Hour
	return cfg
}

// newTestWorker wires a worker against a fresh miniredis.
func newTestWorker(t *testing.T) (*Worker, *redis.Client, *miniredis.Miniredis, *Config) {
	t.Helper()
	mr := miniredis.RunT(t)
	cfg := testConfig()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	w := New(cfg, rdb, logger)
	return w, rdb, mr, cfg
}

// enqueueTask XADDs one start-task envelope.
func enqueueTask(t *testing.T, rdb *redis.Client, cfg *Config, payload map[string]any, schemaVersion string) string {
	t.Helper()
	if schemaVersion == "" {
		schemaVersion = fmt.Sprintf("%d", TaskSchemaVersion)
	}
	blob, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return rdb.XAdd(context.Background(), &redis.XAddArgs{
		Stream: cfg.QueueName,
		Values: map[string]any{"payload": string(blob), "schema_version": schemaVersion},
	}).Val()
}

func stateKeyOf(cfg *Config, executionID string) string {
	return cfg.KeyPrefix + ":execution:" + executionID
}

// loadStateRow fetches and decodes the execution state row.
func loadStateRow(t *testing.T, rdb *redis.Client, cfg *Config, executionID string) *ExecutionState {
	t.Helper()
	raw, err := rdb.Get(context.Background(), stateKeyOf(cfg, executionID)).Result()
	if err != nil {
		t.Fatalf("state row read: %v", err)
	}
	var state ExecutionState
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		t.Fatalf("state row decode: %v", err)
	}
	return &state
}

// waitFor polls cond until it holds or the timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(15 * time.Millisecond)
	}
	return cond()
}

func streamLen(t *testing.T, rdb *redis.Client, key string) int64 {
	t.Helper()
	return rdb.XLen(context.Background(), key).Val()
}

func pendingCount(t *testing.T, rdb *redis.Client, cfg *Config) int64 {
	t.Helper()
	n, err := rdb.XPending(context.Background(), cfg.QueueName, cfg.ConsumerGroup).Result()
	if err != nil {
		t.Fatalf("xpending: %v", err)
	}
	return n.Count
}

func dlqReason(t *testing.T, rdb *redis.Client, cfg *Config) (string, map[string]any) {
	t.Helper()
	msgs := rdb.XRange(context.Background(), cfg.DLQKey, "-", "+").Val()
	if len(msgs) == 0 {
		t.Fatal("expected a DLQ entry, got none")
	}
	payload, _ := msgs[0].Values["payload"].(string)
	var entry map[string]any
	if err := json.Unmarshal([]byte(payload), &entry); err != nil {
		t.Fatalf("dlq envelope decode: %v", err)
	}
	reason, _ := entry["reason"].(string)
	return reason, entry
}

// osReadHelloWorld loads the repo's hello-world IR fixture.
func osReadHelloWorld() ([]byte, error) {
	return os.ReadFile("../ir/testdata/hello_world.json")
}

func mustHelloWorld(t *testing.T) []byte {
	t.Helper()
	raw, err := osReadHelloWorld()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func redisXAddArgs(cfg *Config, payload, schemaVersion string) *redis.XAddArgs {
	return &redis.XAddArgs{
		Stream: cfg.QueueName,
		Values: map[string]any{"payload": payload, "schema_version": schemaVersion},
	}
}

func redisXReadGroupArgs(cfg *Config) *redis.XReadGroupArgs {
	return &redis.XReadGroupArgs{
		Group:    cfg.ConsumerGroup,
		Consumer: cfg.ConsumerName,
		Streams:  []string{cfg.QueueName, ">"},
		Count:    1,
		Block:    100 * time.Millisecond,
	}
}

func redisXPendingExtArgs(cfg *Config) *redis.XPendingExtArgs {
	return &redis.XPendingExtArgs{
		Stream: cfg.QueueName,
		Group:  cfg.ConsumerGroup,
		Start:  "-",
		End:    "+",
		Count:  100,
	}
}

func baseStartPayload(executionID string) map[string]any {
	return map[string]any{
		"type":         "start",
		"flow_id":      "hello_world_go",
		"execution_id": executionID,
		"params":       map[string]any{"who": "worker-test"},
		"tenant_id":    "",
		"timestamp":    nowISO(),
	}
}
