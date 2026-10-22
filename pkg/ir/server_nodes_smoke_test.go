package ir

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/workflow"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// smokeRegistry 是测试用订阅注册表（引擎挂起边界要求已注册的唤醒源）。
type smokeRegistry struct {
	mu   sync.Mutex
	seq  int
	spec map[string]*workflow.SubscriptionRequest
}

func newSmokeRegistry() *smokeRegistry {
	return &smokeRegistry{spec: map[string]*workflow.SubscriptionRequest{}}
}

func (r *smokeRegistry) Register(_ context.Context, req *workflow.SubscriptionRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seq++
	req.SubscriptionID = "sub-" + strings.Repeat("x", 0) + string(rune('a'+r.seq))
	r.spec[req.SubscriptionID] = req
	return nil
}

func (r *smokeRegistry) Unregister(_ context.Context, subscriptionID, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.spec, subscriptionID)
	return nil
}

// phase 3 冒烟：delay 节点走 ir.Load → Execute 挂起 → Resume 完成 → 下游
// 读恢复后的 $NODE 全形状（service_config/node_subtype 保留 + event_data）。
func TestServerNodesSmokeDelaySuspendResume(t *testing.T) {
	flowJSON := `{
		"flow_id": "smoke_delay",
		"runtime": "python",
		"inputType": {"dataType": "any"},
		"nodes": [
			{"type": "start", "id": "s", "next": "d"},
			{"type": "delay", "id": "d", "delay_seconds": 0.2, "next": "o"},
			{"type": "end", "id": "o", "resultType": "success",
			 "output": {"st": "$NODE.d.status", "tt": "$NODE.d.event_data.trigger_type", "sub": "$NODE.d.node_subtype", "sc": "$NODE.d.service_config.type"}}
		]
	}`
	wf, err := Load([]byte(flowJSON))
	require.NoError(t, err)

	ctx := workflow.WithSuspendOptions(t.Context(), &workflow.SuspendOptions{
		ExecutionID:   "exec-smoke",
		FlowID:        "smoke_delay",
		Subscriptions: newSmokeRegistry(),
	})
	ctx = runtime.WithExecutionID(ctx, "exec-smoke")
	_, execErr := wf.Execute(ctx, map[string]any{})
	susp := workflow.AsSuspend(execErr)
	require.NotNil(t, susp, "delay must suspend, got: %v", execErr)

	// 挂起态形状：pending + service_config + node_subtype。
	var cp map[string]any
	raw, err := json.Marshal(susp.Checkpoint.Context)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &cp))
	nodes, _ := cp["$NODE"].(map[string]any)
	state, _ := nodes["d"].(map[string]any)
	require.NotNil(t, state)
	assert.Equal(t, "pending", state["status"])
	assert.Equal(t, "delay", state["node_subtype"])
	sc, _ := state["service_config"].(map[string]any)
	require.NotNil(t, sc)
	assert.Equal(t, "delay", sc["type"])
	// delay_seconds=0.2 → delay_ms=200（int 截断；引擎侧 JSON 解码为 float64）。
	assert.InEpsilon(t, float64(200), sc["delay_ms"], 0.0001)
	assert.Equal(t, "delay_trigger", sc["event_type"])
	assert.Equal(t, "smoke_delay", sc["flow_id"])
	assert.Equal(t, "exec-smoke", sc["execution_id"])
	assert.NotZero(t, sc["trigger_timestamp"])
	assert.Equal(t, "delay_trigger", state["event_type"])

	// 恢复：service 形状的 delay_completed 载荷（带订阅绑定）。
	payload := map[string]any{
		"node_id":                  "d",
		"flow_id":                  "smoke_delay",
		"trigger_type":             "delay_completed",
		"delay_ms":                 200,
		"success":                  true,
		"__plaita_subscription_id": state["subscription_id"],
	}
	result, err := wf.Resume(t.Context(), susp.Checkpoint, "event", payload)
	require.NoError(t, err)
	out, ok := result.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "completed", out["st"])
	assert.Equal(t, "delay_completed", out["tt"])
	assert.Equal(t, "delay", out["sub"])
	assert.Equal(t, "delay", out["sc"])
}

// 加载期拒绝对齐（pydantic 模型层语义）：缺必填/非法枚举在 ir.Load 拒绝。
func TestServerNodesLoadRejections(t *testing.T) {
	cases := []struct {
		name    string
		node    string
		require string
	}{
		{"delay_missing_seconds", `{"type": "delay", "id": "d"}`, "delay_seconds"},
		{"delay_bad_unit", `{"type": "delay", "id": "d", "delay_seconds": 5, "delay_unit": "MinutesX"}`, "delay_unit"},
		{"approval_missing_approvers", `{"type": "approval", "id": "a", "approval_title": "t", "approval_content": "c"}`, "approvers"},
		{"approval_bad_strategy", `{"type": "approval", "id": "a", "approval_title": "t", "approval_content": "c", "approvers": ["m1"], "approval_strategy": "SOME"}`, "approval_strategy"},
		{"redisq_missing_queue", `{"type": "redis_queue", "id": "r"}`, "queue_name"},
		{"redisq_bad_type", `{"type": "redis_queue", "id": "r", "queue_name": "q", "queue_type": "fifo"}`, "queue_type"},
		{"kafka_missing_bootstrap", `{"type": "kafka_queue", "id": "k", "topic": "t", "group_id": "g"}`, "bootstrap_servers"},
		{"kafka_bad_protocol", `{"type": "kafka_queue", "id": "k", "bootstrap_servers": "b:9092", "topic": "t", "group_id": "g", "security_protocol": "QUIC"}`, "security_protocol"},
		{"httpcb_bad_method", `{"type": "http_callback", "id": "h", "callback_method": "PATCH"}`, "callback_method"},
		{"httpcb_bad_auth", `{"type": "http_callback", "id": "h", "auth_type": "openid"}`, "auth_type"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			flow := `{"flow_id":"f","runtime":"python","inputType":{"dataType":"any"},"nodes":[
				{"type":"start","id":"s","next":"n"},` + tc.node + `,{"type":"end","id":"e","resultType":"success","output":"done"}]}
				`
			// 被测节点夹在 start 与 end 之间；next 指向 e。
			flow = strings.Replace(flow, `"id":"n"}]`, `"id":"n","next":"e"}]`, 1)
			_, err := Load([]byte(flow))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.require)
		})
	}
}
