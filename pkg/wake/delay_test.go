package wake

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/jeffkit/plaita-go/pkg/worker"
)

func newTestService(t *testing.T, mutate func(*Config)) (*DelayService, *miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	cfg := &Config{
		KeyPrefix:          "plaita",
		InstanceID:         "delay-test",
		DefaultTenantID:    "default",
		EventTTLSeconds:    7 * 86400,
		PollInterval:       10 * time.Millisecond,
		HeartbeatInterval:  20 * time.Millisecond,
		RegistryTTL:        time.Second,
		MaxTriggerAttempts: 5,
		RetryBackoffBase:   50 * time.Millisecond,
		RetryBackoffMax:    200 * time.Millisecond,
		ConveyBatch:        200,
		DueBatch:           50,
	}
	if mutate != nil {
		mutate(cfg)
	}
	cfg.normalize()
	svc, err := NewDelayService(cfg, client)
	if err != nil {
		t.Fatal(err)
	}
	return svc, mr, client
}

func delayTaskRaw(t *testing.T, fields map[string]any) string {
	t.Helper()
	base := map[string]any{
		"type":         "delay",
		"node_id":      "wait",
		"event_type":   "delay_trigger",
		"execution_id": "exec-1",
		"flow_id":      "flow-1",
		"tenant_id":    "default",
		"delay_ms":     float64(1000),
	}
	for k, v := range fields {
		base[k] = v
	}
	blob, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	return string(blob)
}

func zcard(t *testing.T, client *redis.Client, key string) int64 {
	t.Helper()
	n, err := client.ZCard(context.Background(), key).Result()
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestConveyMovesListToScheduled(t *testing.T) {
	svc, _, client := newTestService(t, nil)
	ctx := context.Background()
	past := delayTaskRaw(t, map[string]any{"trigger_timestamp": float64(time.Now().UnixMilli() - 1000)})
	future := delayTaskRaw(t, map[string]any{"trigger_timestamp": float64(time.Now().UnixMilli() + 60_000)})
	if err := client.RPush(ctx, svc.queueKey, past, future).Err(); err != nil {
		t.Fatal(err)
	}
	svc.conveyOnce(ctx)
	if n := client.LLen(ctx, svc.queueKey).Val(); n != 0 {
		t.Fatalf("list not drained: %d", n)
	}
	if got := zcard(t, client, svc.scheduledKey); got != 2 {
		t.Fatalf("scheduled=%d want 2", got)
	}
	score, err := client.ZScore(ctx, svc.scheduledKey, past).Result()
	if err != nil {
		t.Fatal(err)
	}
	want := float64(time.Now().UnixMilli() - 1000)
	if diff := score - want; diff > 2000 || diff < -2000 {
		t.Fatalf("past task score=%f want ~%f (trigger_timestamp must win over delay_ms)", score, want)
	}
}

func TestConveyDropsInvalidEntries(t *testing.T) {
	svc, _, client := newTestService(t, nil)
	ctx := context.Background()
	bad := `{not json`
	noEvent := delayTaskRaw(t, nil)
	noEvent = strings.Replace(noEvent, `"event_type":"delay_trigger",`, "", 1)
	noDelay := delayTaskRaw(t, nil)
	noDelay = strings.Replace(noDelay, `"delay_ms":1000,`, "", 1)
	good := delayTaskRaw(t, map[string]any{"node_id": "ok"})
	if err := client.RPush(ctx, svc.queueKey, bad, noEvent, noDelay, good).Err(); err != nil {
		t.Fatal(err)
	}
	svc.conveyOnce(ctx)
	if got := zcard(t, client, svc.scheduledKey); got != 1 {
		t.Fatalf("scheduled=%d want 1 (only the valid task)", got)
	}
	if n := client.LLen(ctx, svc.queueKey).Val(); n != 0 {
		t.Fatalf("invalid entries must leave the list: %d", n)
	}
}

func TestConveyIdempotentOnCrashWindow(t *testing.T) {
	svc, _, client := newTestService(t, nil)
	ctx := context.Background()
	raw := delayTaskRaw(t, nil)
	// Simulate a crash between ZADD and LREM: the entry is in both.
	if err := client.RPush(ctx, svc.queueKey, raw).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.ZAdd(ctx, svc.scheduledKey, redis.Z{Score: float64(time.Now().UnixMilli()), Member: raw}).Err(); err != nil {
		t.Fatal(err)
	}
	svc.conveyOnce(ctx)
	if got := zcard(t, client, svc.scheduledKey); got != 1 {
		t.Fatalf("ZADD must be idempotent per member: scheduled=%d", got)
	}
	if n := client.LLen(ctx, svc.queueKey).Val(); n != 0 {
		t.Fatalf("list must drain: %d", n)
	}
}

func TestDueTaskPublishesResumeEvent(t *testing.T) {
	svc, _, client := newTestService(t, nil)
	ctx := context.Background()
	raw := delayTaskRaw(t, map[string]any{"trigger_timestamp": float64(time.Now().UnixMilli() - 100)})
	if err := client.ZAdd(ctx, svc.scheduledKey, redis.Z{Score: float64(time.Now().UnixMilli() - 100), Member: raw}).Err(); err != nil {
		t.Fatal(err)
	}
	sub := client.Subscribe(ctx, "plaita:events:delay_trigger")
	defer sub.Close()
	svc.submitDueOnce(ctx)

	deadline := time.Now().Add(10 * time.Second) // 3s 在 CI 慢载下不稳（waitFor 同因）
	var msg *redis.Message
	for msg == nil && time.Now().Before(deadline) {
		m, err := sub.ReceiveTimeout(ctx, 200*time.Millisecond)
		if err != nil {
			continue
		}
		if mm, ok := m.(*redis.Message); ok {
			msg = mm
		}
	}
	if msg == nil {
		t.Fatal("no resume event published on plaita:events:delay_trigger")
	}
	var ev struct {
		EventID       string         `json:"event_id"`
		EventType     string         `json:"event_type"`
		CorrelationID string         `json:"correlation_id"`
		TenantID      string         `json:"tenant_id"`
		Data          map[string]any `json:"data"`
	}
	if err := json.Unmarshal([]byte(msg.Payload), &ev); err != nil {
		t.Fatalf("event payload undecodable: %v", err)
	}
	if ev.CorrelationID != "exec-1" || ev.TenantID != "default" {
		t.Fatalf("correlation/tenant wiring broken: %+v", ev)
	}
	if ev.Data["trigger_type"] != "delay_completed" || ev.Data["success"] != true {
		t.Fatalf("event_data shape diverges from Python handle_task: %+v", ev.Data)
	}
	if ev.Data["planned_trigger_timestamp"] == nil || ev.Data["actual_trigger_timestamp"] == nil {
		t.Fatalf("timestamps missing: %+v", ev.Data)
	}
	// Confirmed publish ⇒ dequeued; double-write storage present.
	if got := zcard(t, client, svc.scheduledKey); got != 0 {
		t.Fatalf("confirmed trigger must ZREM: scheduled=%d", got)
	}
	keys, _ := client.Keys(ctx, "plaita:event:events:*").Result()
	if len(keys) != 1 {
		t.Fatalf("event storage double-write missing: %v", keys)
	}
}

func TestPublishFailureDefersThenRecovers(t *testing.T) {
	svc, _, client := newTestService(t, nil)
	ctx := context.Background()
	raw := delayTaskRaw(t, nil)
	if err := client.ZAdd(ctx, svc.scheduledKey, redis.Z{Score: float64(time.Now().UnixMilli()), Member: raw}).Err(); err != nil {
		t.Fatal(err)
	}
	fail := true
	svc.publishFn = func(_ context.Context, ev *worker.Event) error {
		if fail {
			return errors.New("redis down")
		}
		return nil
	}
	svc.processTask(ctx, raw)
	if got := zcard(t, client, svc.scheduledKey); got != 1 {
		t.Fatalf("failed publish must stay scheduled: %d", got)
	}
	recJSON, err := client.HGet(ctx, svc.attemptsKey, raw).Result()
	if err != nil {
		t.Fatalf("attempt record missing: %v", err)
	}
	var rec attemptRecord
	if err := json.Unmarshal([]byte(recJSON), &rec); err != nil {
		t.Fatal(err)
	}
	// Next 的「未来性」不断言：backoff 仅 50ms，慢载下检查执行时早已过期
	// （实证 flake）——过期语义由下半段用 next:0 显式模拟覆盖
	if rec.Attempts != 1 || rec.Next <= 0 {
		t.Fatalf("attempt record wrong: %+v", rec)
	}
	// Backoff expired + publish recovered ⇒ trigger confirmed, counter cleared.
	fail = false
	if err := client.HSet(ctx, svc.attemptsKey, raw, `{"attempts":1,"next":0}`).Err(); err != nil {
		t.Fatal(err)
	}
	svc.processTask(ctx, raw)
	if got := zcard(t, client, svc.scheduledKey); got != 0 {
		t.Fatalf("recovered trigger must ZREM: %d", got)
	}
	if n := client.HLen(ctx, svc.attemptsKey).Val(); n != 0 {
		t.Fatalf("attempt counter must be cleared: %d", n)
	}
}

func TestDeadLetterAfterMaxAttempts(t *testing.T) {
	svc, _, client := newTestService(t, func(c *Config) { c.MaxTriggerAttempts = 2 })
	ctx := context.Background()
	raw := delayTaskRaw(t, nil)
	if err := client.ZAdd(ctx, svc.scheduledKey, redis.Z{Score: float64(time.Now().UnixMilli()), Member: raw}).Err(); err != nil {
		t.Fatal(err)
	}
	svc.publishFn = func(_ context.Context, ev *worker.Event) error {
		if ev.EventType == "delay_trigger" {
			return errors.New("redis down")
		}
		return svc.pub.Publish(ctx, ev) // service_error broadcast goes through
	}
	svc.processTask(ctx, raw) // attempt 1 → backoff
	svc.processTask(ctx, raw) // attempt 2 = max → dead letter
	if n := client.LLen(ctx, svc.deadKey).Val(); n != 1 {
		t.Fatalf("dead list must hold the entry: %d", n)
	}
	if got := zcard(t, client, svc.scheduledKey); got != 0 {
		t.Fatalf("dead-lettered entry must leave the schedule: %d", got)
	}
	if n := client.HLen(ctx, svc.attemptsKey).Val(); n != 0 {
		t.Fatalf("attempt counter must be cleared on dead-letter: %d", n)
	}
	keys, _ := client.Keys(ctx, "plaita:event:events:*").Result()
	if len(keys) != 1 {
		t.Fatalf("service_error event must be broadcast: %v", keys)
	}
	blob, _ := client.Get(ctx, keys[0]).Result()
	if !strings.Contains(blob, "delay_trigger_dead_letter") {
		t.Fatalf("service_error payload wrong: %s", blob)
	}
}

func TestShutdownKeepsTaskParked(t *testing.T) {
	svc, _, client := newTestService(t, nil)
	raw := delayTaskRaw(t, nil)
	if err := client.ZAdd(context.Background(), svc.scheduledKey, redis.Z{Score: float64(time.Now().UnixMilli()), Member: raw}).Err(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	svc.processTask(ctx, raw)
	if got := zcard(t, client, svc.scheduledKey); got != 1 {
		t.Fatalf("shutdown must leave the entry parked: %d", got)
	}
	if n := client.HLen(ctx, svc.attemptsKey).Val(); n != 0 {
		t.Fatalf("shutdown must not count an attempt: %d", n)
	}
}

func TestRegistryLifecycle(t *testing.T) {
	svc, _, client := newTestService(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = svc.Run(ctx); close(done) }()
	key := "plaita:registry:delay:delay-test"
	deadline := time.Now().Add(10 * time.Second) // 3s 在 CI 慢载下不稳（waitFor 同因）
	var blob string
	for time.Now().Before(deadline) {
		blob, _ = client.Get(ctx, key).Result()
		if strings.Contains(blob, `"status":"running"`) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !strings.Contains(blob, `"service_type":"delay"`) || !strings.Contains(blob, `"status":"running"`) {
		t.Fatalf("registry record wrong/never running: %q", blob)
	}
	cancel()
	<-done
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if n, _ := client.Exists(ctx, key).Result(); n == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("registry key not removed after stop")
}

func TestRunEndToEndDispatchToWake(t *testing.T) {
	svc, _, client := newTestService(t, func(c *Config) { c.PollInterval = 10 * time.Millisecond })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = svc.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	sub := client.Subscribe(ctx, "plaita:events:delay_trigger")
	defer sub.Close()
	// The worker-side dispatch shape: RPUSH {prefix}:delay:queue with the
	// service task body (service_config + execution_id + tenant_id).
	raw := delayTaskRaw(t, map[string]any{"trigger_timestamp": float64(time.Now().UnixMilli() - 100)})
	if err := client.RPush(ctx, svc.queueKey, raw).Err(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		m, err := sub.ReceiveTimeout(ctx, 200*time.Millisecond)
		if err != nil {
			if zcard(t, client, svc.scheduledKey) == 0 && client.LLen(ctx, svc.queueKey).Val() == 0 {
				// 事件可能发在订阅确认往返完成前（pubsub 无留存）——重投任务
				// 再等，而不是立判死；Run 真坏时队列不会被清空，终会落到 deadline。
				if err := client.RPush(ctx, svc.queueKey, raw).Err(); err != nil {
					t.Fatal(err)
				}
			}
			continue
		}
		if msg, ok := m.(*redis.Message); ok && strings.Contains(msg.Payload, `"trigger_type":"delay_completed"`) {
			return
		}
	}
	t.Fatal("end-to-end dispatch→wake did not produce a resume event in time")
}

func TestUnknownSourceRejected(t *testing.T) {
	_, _, client := newTestService(t, nil)
	_, err := NewService("bogus", &Config{Logger: testLogger()}, client)
	if err == nil || !strings.Contains(err.Error(), "delay") {
		t.Fatalf("unknown source must name the implemented set: %v", err)
	}
}

func TestEmptyDelayQueueEnvRefused(t *testing.T) {
	t.Setenv("PLAITA_DELAY_QUEUE", "   ")
	cfg := &Config{KeyPrefix: "plaita"}
	cfg.normalize()
	_, err := cfg.DelayQueueKey()
	if err == nil || !strings.Contains(err.Error(), "empty value is not allowed") {
		t.Fatalf("empty override must be refused (Python B2R31-03): %v", err)
	}
}

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
