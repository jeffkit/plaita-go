package wake

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/jeffkit/plaita-go/pkg/worker"
)

func newTestSource(t *testing.T, source string, mutate func(*Config)) (*Config, *miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	cfg := &Config{
		KeyPrefix:          "plaita",
		InstanceID:         source + "-test",
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
	return cfg, mr, client
}

func subscribeFor(t *testing.T, client *redis.Client, channel string) chan string {
	t.Helper()
	sub := client.Subscribe(context.Background(), channel)
	t.Cleanup(func() { _ = sub.Close() })
	out := make(chan string, 8)
	go func() {
		for {
			m, err := sub.ReceiveTimeout(context.Background(), 100*time.Millisecond)
			if err != nil {
				continue
			}
			if msg, ok := m.(*redis.Message); ok {
				select {
				case out <- msg.Payload:
				default:
				}
			}
		}
	}()
	return out
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	// 10s：CI 慢载（同包前后台 goroutine 累积）下 3s 窗口会被调度延迟击穿
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

// ---- approval ----

func approvalTask(t *testing.T, overrides map[string]any) string {
	t.Helper()
	base := map[string]any{
		"type":         "approval",
		"node_id":      "sign",
		"event_type":   "approval_decision",
		"execution_id": "exec-ap",
		"flow_id":      "flow-ap",
		"tenant_id":    "default",
		"approval_id":  "ap-1",
		"approval_config": map[string]any{
			"title": "上线审批", "content": "请审批",
		},
		"approver_config": map[string]any{
			"approvers": []any{"alice", "bob"}, "strategy": "any",
		},
	}
	for k, v := range overrides {
		base[k] = v
	}
	blob, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	return string(blob)
}

func TestApprovalIntakeCreatesRecord(t *testing.T) {
	cfg, _, client := newTestSource(t, SourceApproval, nil)
	svc, err := NewApprovalSource(cfg, client)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	raw := approvalTask(t, nil)
	if err := client.RPush(ctx, svc.queueKey, raw).Err(); err != nil {
		t.Fatal(err)
	}
	svc.cycleOnce(ctx)
	recordKey := "plaita:approval:pending:ap-1"
	waitFor(t, "approval record", func() bool {
		v, _ := client.Get(ctx, recordKey).Result()
		return strings.Contains(v, `"status":"pending"`) &&
			strings.Contains(v, `"strategy":"any"`) &&
			strings.Contains(v, `"approval_id":"ap-1"`)
	})
	if n := client.LLen(ctx, svc.queueKey).Val(); n != 0 {
		t.Fatalf("queue must drain after intake: %d", n)
	}
}

func TestApprovalIntakeDropsInvalid(t *testing.T) {
	cfg, _, client := newTestSource(t, SourceApproval, nil)
	svc, err := NewApprovalSource(cfg, client)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	noApprovers := approvalTask(t, map[string]any{
		"approver_config": map[string]any{"approvers": []any{}, "strategy": "any"},
	})
	if err := client.RPush(ctx, svc.queueKey, noApprovers).Err(); err != nil {
		t.Fatal(err)
	}
	svc.cycleOnce(ctx)
	waitFor(t, "invalid entry dropped", func() bool {
		return client.LLen(ctx, svc.queueKey).Val() == 0
	})
	if n, _ := client.Exists(ctx, "plaita:approval:pending:ap-1").Result(); n != 0 {
		t.Fatal("invalid task must not create a record")
	}
}

func TestApprovalDuplicateDispatchSkipsRebuild(t *testing.T) {
	cfg, _, client := newTestSource(t, SourceApproval, nil)
	svc, err := NewApprovalSource(cfg, client)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	existing := map[string]any{
		"approval_id": "ap-1", "status": "pending",
		"created_time": 1, "approvals": []any{map[string]any{"approver_id": "alice", "decision": "approve"}},
		"required_approvers": []any{"alice", "bob"}, "strategy": "all",
		"task_config": map[string]any{"node_id": "sign", "event_type": "approval_decision"},
	}
	blob, _ := json.Marshal(existing)
	recordKey := "plaita:approval:pending:ap-1"
	if err := client.SetEx(ctx, recordKey, string(blob), 7*24*time.Hour).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.RPush(ctx, svc.queueKey, approvalTask(t, nil)).Err(); err != nil {
		t.Fatal(err)
	}
	svc.cycleOnce(ctx)
	waitFor(t, "duplicate acked", func() bool {
		return client.LLen(ctx, svc.queueKey).Val() == 0
	})
	v, _ := client.Get(ctx, recordKey).Result()
	if !strings.Contains(v, `"decision":"approve"`) {
		t.Fatalf("duplicate dispatch must not rebuild the record: %s", v)
	}
}

func decisionEventSeen(t *testing.T, client *redis.Client) map[string]any {
	t.Helper()
	ch := subscribeFor(t, client, "plaita:events:approval_decision")
	var payload string
	waitFor(t, "approval_decision event", func() bool {
		select {
		case payload = <-ch:
			return true
		default:
			return false
		}
	})
	var ev map[string]any
	if err := json.Unmarshal([]byte(payload), &ev); err != nil {
		t.Fatalf("event undecodable: %v", err)
	}
	return ev
}

func TestApprovalDecisionAnyCompletes(t *testing.T) {
	cfg, _, client := newTestSource(t, SourceApproval, nil)
	svc, err := NewApprovalSource(cfg, client)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if !svc.handleIntake(ctx, approvalTask(t, nil), mustTask(t, approvalTask(t, nil))) {
		t.Fatal("intake failed")
	}
	ch := subscribeFor(t, client, "plaita:events:approval_decision")
	res := svc.SubmitApprovalDecision(ctx, "ap-1", "alice", "approve", "lgtm")
	ev := waitPayload(t, ch)
	if res.Status != "success" || res.FinalDecision != "approve" {
		t.Fatalf("decision result wrong: %+v", res)
	}
	if n, _ := client.Exists(ctx, "plaita:approval:pending:ap-1").Result(); n != 0 {
		t.Fatal("completed record must be deleted")
	}
	if ev["correlation_id"] != "exec-ap" || ev["event_type"] != "approval_decision" {
		t.Fatalf("event wiring wrong: %+v", ev)
	}
	data := ev["data"].(map[string]any)
	if data["trigger_type"] != "approval_completed" || data["final_decision"] != "approve" {
		t.Fatalf("event data wrong: %+v", data)
	}
	if data["approval_details"] == nil {
		t.Fatal("approval_details missing")
	}
}

func TestApprovalDecisionAllNeedsAll(t *testing.T) {
	cfg, _, client := newTestSource(t, SourceApproval, nil)
	svc, err := NewApprovalSource(cfg, client)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	task := approvalTask(t, map[string]any{
		"approver_config": map[string]any{"approvers": []any{"alice", "bob"}, "strategy": "all"},
	})
	if !svc.handleIntake(ctx, task, mustTask(t, task)) {
		t.Fatal("intake failed")
	}
	ch := subscribeFor(t, client, "plaita:events:approval_decision")
	res := svc.SubmitApprovalDecision(ctx, "ap-1", "alice", "approve", "")
	if res.Status != "success" || res.FinalDecision != "" {
		t.Fatalf("first decision must stay pending: %+v", res)
	}
	select {
	case p := <-ch:
		t.Fatalf("no event expected before threshold: %s", p)
	default:
	}
	v, _ := client.Get(ctx, "plaita:approval:pending:ap-1").Result()
	if !strings.Contains(v, `"approver_id":"alice"`) {
		t.Fatalf("decision must be recorded: %s", v)
	}
	res = svc.SubmitApprovalDecision(ctx, "ap-1", "bob", "approve", "")
	if res.Status != "success" || res.FinalDecision != "approve" {
		t.Fatalf("second decision must complete: %+v", res)
	}
	// 完成事件经 pubsub relay goroutine 转发，非阻塞 select 会和调度赛跑
	// （CI 慢机必踩）——改有界等待；事件真缺失时照样在 3s 后失败。
	waitFor(t, "completion event", func() bool {
		select {
		case <-ch:
			return true
		default:
			return false
		}
	})
}

func TestApprovalDecisionPermissionAndDuplicate(t *testing.T) {
	cfg, _, client := newTestSource(t, SourceApproval, nil)
	svc, err := NewApprovalSource(cfg, client)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if !svc.handleIntake(ctx, approvalTask(t, nil), mustTask(t, approvalTask(t, nil))) {
		t.Fatal("intake failed")
	}
	if res := svc.SubmitApprovalDecision(ctx, "ap-1", "mallory", "approve", ""); res.Status != "error" || !strings.Contains(res.Message, "无审批权限") {
		t.Fatalf("permission check broken: %+v", res)
	}
	if res := svc.SubmitApprovalDecision(ctx, "ap-1", "alice", "approve", ""); res.Status != "success" {
		t.Fatalf("first submit failed: %+v", res)
	}
	// Record deleted on completion → "不存在" is the next submission's fate.
	if res := svc.SubmitApprovalDecision(ctx, "ap-1", "alice", "approve", ""); res.Status != "error" || !strings.Contains(res.Message, "不存在") {
		t.Fatalf("post-completion submit must fail: %+v", res)
	}
}

func TestApprovalPublishFailureKeepsRecordUntouched(t *testing.T) {
	cfg, _, client := newTestSource(t, SourceApproval, nil)
	svc, err := NewApprovalSource(cfg, client)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if !svc.handleIntake(ctx, approvalTask(t, nil), mustTask(t, approvalTask(t, nil))) {
		t.Fatal("intake failed")
	}
	svc.publishFn = func(_ context.Context, _ *worker.Event) error {
		return context.DeadlineExceeded
	}
	res := svc.SubmitApprovalDecision(ctx, "ap-1", "alice", "approve", "")
	if res.Status != "error" {
		t.Fatalf("publish failure must surface: %+v", res)
	}
	v, _ := client.Get(ctx, "plaita:approval:pending:ap-1").Result()
	if strings.Contains(v, `"approver_id":"alice"`) {
		t.Fatal("failed decision must NOT persist (retry would wedge on 已经审批过)")
	}
}

// waitPayload pops one pubsub payload and returns it parsed.
func waitPayload(t *testing.T, ch chan string) map[string]any {
	t.Helper()
	var payload string
	waitFor(t, "pubsub event", func() bool {
		select {
		case payload = <-ch:
			return true
		default:
			return false
		}
	})
	var m map[string]any
	if err := json.Unmarshal([]byte(payload), &m); err != nil {
		t.Fatalf("event undecodable: %v (%s)", err, payload)
	}
	return m
}

func mustTask(t *testing.T, raw string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// ---- redis_queue ----

func redisQueueTask(t *testing.T, mr *miniredis.Miniredis, overrides map[string]any) string {
	t.Helper()
	host, port := mr.Host(), mr.Port()
	pf, _ := json.Number(port).Float64()
	base := map[string]any{
		"type":         "redis_queue",
		"node_id":      "listen",
		"event_type":   "redis_message",
		"execution_id": "exec-rq",
		"flow_id":      "flow-rq",
		"tenant_id":    "default",
		"redis_config": map[string]any{"host": host, "port": pf, "db": float64(0)},
		"queue_config": map[string]any{"name": "user:q", "type": "list", "message_format": "json"},
		"listen_config": map[string]any{
			"max_wait_after_message": 0.3,
		},
	}
	for k, v := range overrides {
		base[k] = v
	}
	blob, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	return string(blob)
}

func TestRedisQueueListEndToEnd(t *testing.T) {
	cfg, mr, client := newTestSource(t, SourceRedisQueue, nil)
	svc, err := NewRedisQueueSource(cfg, client)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	raw := redisQueueTask(t, mr, nil)
	if err := client.RPush(ctx, svc.queueKey, raw).Err(); err != nil {
		t.Fatal(err)
	}
	ch := subscribeFor(t, client, "plaita:events:redis_message")
	svc.cycleOnce(ctx) // spawns the listener goroutine
	// message into the user list (the USER redis is the same miniredis here)
	if err := client.RPush(ctx, "user:q", `{"k":"v"}`).Err(); err != nil {
		t.Fatal(err)
	}
	var payload string
	waitFor(t, "redis_message event", func() bool {
		select {
		case payload = <-ch:
			return true
		default:
			return false
		}
	})
	var ev map[string]any
	if err := json.Unmarshal([]byte(payload), &ev); err != nil {
		t.Fatal(err)
	}
	if ev["correlation_id"] != "exec-rq" {
		t.Fatalf("event wiring wrong: %+v", ev)
	}
	data := ev["data"].(map[string]any)
	if data["trigger_type"] != "redis_message" || data["queue_type"] != "list" {
		t.Fatalf("event data wrong: %+v", data)
	}
	msg, ok := data["message"].(map[string]any)
	if !ok || msg["k"] != "v" {
		t.Fatalf("json message must be parsed: %+v", data["message"])
	}
	// Idle window (0.3s) elapses → listener completes + acks the entry.
	waitFor(t, "dispatch entry acked", func() bool {
		return client.LLen(ctx, svc.queueKey).Val() == 0
	})
	v, _ := client.Get(ctx, "task_complete:exec-rq").Result()
	if v != "1" {
		t.Fatalf("task_complete must be set: %q", v)
	}
}

func TestRedisQueueStreamEndToEnd(t *testing.T) {
	cfg, mr, client := newTestSource(t, SourceRedisQueue, nil)
	svc, err := NewRedisQueueSource(cfg, client)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	raw := redisQueueTask(t, mr, map[string]any{
		"execution_id": "exec-stream",
		"queue_config": map[string]any{"name": "user:stream", "type": "stream", "message_format": "json"},
	})
	if err := client.RPush(ctx, svc.queueKey, raw).Err(); err != nil {
		t.Fatal(err)
	}
	ch := subscribeFor(t, client, "plaita:events:redis_message")
	svc.cycleOnce(ctx)
	var payload string
	// XREAD 从 "$" 起读、只见监听起来后的新条目：XAdd 抢在监听起来前会被静默
	// 跳过（慢 CI 上裸 sleep 兜不住）——窗口内重投，直到事件到达。
	waitFor(t, "stream redis_message event", func() bool {
		if _, err := client.XAdd(ctx, &redis.XAddArgs{Stream: "user:stream", Values: map[string]any{"data": `{"x":1}`}}).Result(); err != nil {
			t.Fatal(err)
		}
		select {
		case payload = <-ch:
			return true
		default:
			return false
		}
	})
	var ev map[string]any
	if err := json.Unmarshal([]byte(payload), &ev); err != nil {
		t.Fatal(err)
	}
	data := ev["data"].(map[string]any)
	if data["queue_type"] != "stream" || data["message_id"] == nil {
		t.Fatalf("stream event shape wrong: %+v", data)
	}
	if msg, ok := data["message"].(map[string]any); !ok || msg["x"] != float64(1) {
		t.Fatalf("stream data field must be json-parsed: %+v", data["message"])
	}
	waitFor(t, "stream dispatch acked", func() bool {
		return client.LLen(ctx, svc.queueKey).Val() == 0
	})
}

func TestRedisQueuePubsubEndToEnd(t *testing.T) {
	cfg, mr, client := newTestSource(t, SourceRedisQueue, nil)
	svc, err := NewRedisQueueSource(cfg, client)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	raw := redisQueueTask(t, mr, map[string]any{
		"execution_id": "exec-ps",
		"queue_config": map[string]any{"name": "user:chan", "type": "pubsub", "message_format": "text"},
	})
	if err := client.RPush(ctx, svc.queueKey, raw).Err(); err != nil {
		t.Fatal(err)
	}
	ch := subscribeFor(t, client, "plaita:events:redis_message")
	svc.cycleOnce(ctx)
	var payload string
	// 订阅建立前的 Publish 会静默丢失（pubsub 无留存），且监听 goroutine 到位
	// 的耗时在慢 CI 上超过裸 sleep 能兜的范围——窗口内重发，直到事件到达。
	waitFor(t, "pubsub redis_message event", func() bool {
		if err := client.Publish(ctx, "user:chan", "hello").Err(); err != nil {
			t.Fatal(err)
		}
		select {
		case payload = <-ch:
			return true
		default:
			return false
		}
	})
	var ev map[string]any
	if err := json.Unmarshal([]byte(payload), &ev); err != nil {
		t.Fatal(err)
	}
	data := ev["data"].(map[string]any)
	if data["queue_type"] != "pubsub" || data["message"] != "hello" {
		t.Fatalf("pubsub event shape wrong: %+v", data)
	}
	waitFor(t, "pubsub dispatch acked", func() bool {
		return client.LLen(ctx, svc.queueKey).Val() == 0
	})
}

func TestRedisQueueDialFailurePublishesErrorEvent(t *testing.T) {
	cfg, _, client := newTestSource(t, SourceRedisQueue, nil)
	svc, err := NewRedisQueueSource(cfg, client)
	if err != nil {
		t.Fatal(err)
	}
	// Skip the dial retries in-test (go-redis's pool already burns ~1s per
	// refused Ping with its own internal retries).
	svc.dialMaxRetries = 0
	ctx := context.Background()
	raw := redisQueueTask(t, miniredis.RunT(t), map[string]any{
		"redis_config": map[string]any{"host": "127.0.0.1", "port": float64(1)}, // nothing listens here
	})
	if err := client.RPush(ctx, svc.queueKey, raw).Err(); err != nil {
		t.Fatal(err)
	}
	ch := subscribeFor(t, client, "plaita:events:redis_message")
	svc.cycleOnce(ctx)
	var payload string
	waitFor(t, "redis_error event", func() bool {
		select {
		case payload = <-ch:
			return true
		default:
			return false
		}
	})
	if !strings.Contains(payload, `"trigger_type":"redis_error"`) {
		t.Fatalf("error event shape wrong: %s", payload)
	}
	waitFor(t, "failed-dial entry acked", func() bool {
		return client.LLen(ctx, svc.queueKey).Val() == 0
	})
}
