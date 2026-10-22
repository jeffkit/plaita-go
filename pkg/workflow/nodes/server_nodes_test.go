package nodes

// #80 phase 3 节点层单测：钉 golden 看不见的 Go 侧不变量——SuspendSpec 的
// 订阅契约（approval 24h 缺省/显式 null=无限等待）、时长换算表、$ 解析回退、
// 固定事件契约。挂起/恢复状态形状与拒载语义由 server_nodes_golden_test.go
// （plaita 真跑对拍）覆盖。

import (
	"context"
	"testing"
	"time"

	"github.com/jeffkit/plaita-go/internal/runtime"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func serverTestCtx() context.Context {
	return runtime.WithWorkflowRuntimeContext(context.Background(), runtime.NewContext())
}

// approval 订阅超时契约：缺省 24h（#56 兜底——无人审批必须落可观测终态），
// 显式负值哨兵（部署 JSON 显式 null 折叠）= 无限等待，显式正值原样。
func TestApprovalSubscriptionTimeoutContract(t *testing.T) {
	newApproval := func(timeout *float64) *Approval {
		return &Approval{
			BaseNode:          BaseNode{ID: "a"},
			extendedEventCore: extendedEventCore{SubscriptionTimeout: timeout},
			ApprovalTitle:     "t",
			ApprovalContent:   "c",
			Approvers:         []any{"m1"},
		}
	}

	spec, err := newApproval(nil).SuspendSpec(context.Background(), nil)
	require.NoError(t, err)
	assert.InDelta(t, float64(24*3600), spec.Subscription.TimeoutSeconds, 0.0001,
		"default subscription timeout must be 24h")

	inf := -1.0 // ir translate 把显式 null 折叠为 -1
	spec, err = newApproval(&inf).SuspendSpec(context.Background(), nil)
	require.NoError(t, err)
	assert.Zero(t, spec.Subscription.TimeoutSeconds, "explicit null = unlimited wait")

	hour := 3600.0
	spec, err = newApproval(&hour).SuspendSpec(context.Background(), nil)
	require.NoError(t, err)
	assert.InDelta(t, 3600.0, spec.Subscription.TimeoutSeconds, 0.0001)
}

// 固定事件订阅契约：四类节点的 event_type 由契约覆盖（部署 JSON 里的用户值
// 在 ir translate 层被覆盖），SuspendSpec/CanHandleEvent 都以契约为准。
func TestServerNodesContractEventTypes(t *testing.T) {
	apr := &Approval{BaseNode: BaseNode{ID: "a"}, ApprovalTitle: "t", ApprovalContent: "c"}
	spec, err := apr.SuspendSpec(context.Background(), nil)
	require.NoError(t, err)
	assert.Equal(t, "approval_decision", spec.Subscription.EventType)
	assert.False(t, apr.CanHandleEvent("other", nil))
	assert.True(t, apr.CanHandleEvent("approval_decision", nil))

	rq := &RedisQueue{BaseNode: BaseNode{ID: "r"}}
	spec, err = rq.SuspendSpec(context.Background(), nil)
	require.NoError(t, err)
	assert.Equal(t, "redis_message", spec.Subscription.EventType)

	kq := &KafkaQueue{BaseNode: BaseNode{ID: "k"}}
	spec, err = kq.SuspendSpec(context.Background(), nil)
	require.NoError(t, err)
	assert.Equal(t, "kafka_message", spec.Subscription.EventType)

	hc := &HTTPCallback{BaseNode: BaseNode{ID: "h"}}
	spec, err = hc.SuspendSpec(context.Background(), nil)
	require.NoError(t, err)
	assert.Equal(t, "http_callback", spec.Subscription.EventType)
}

// delay 时长换算表（Python _convert_to_milliseconds：单位乘法 + int 截断）。
func TestDelayUnitConversion(t *testing.T) {
	assert.Equal(t, int64(200), delayToMilliseconds(0.2, "seconds"))
	assert.Equal(t, int64(600), delayToMilliseconds(0.01, "minutes"))
	assert.Equal(t, int64(720), delayToMilliseconds(0.0002, "hours"))
	assert.Equal(t, int64(86400_000), delayToMilliseconds(1, "days"))
	// 未知单位兜底按秒（加载期 Literal 已拦，直构流防御）。
	assert.Equal(t, int64(5000), delayToMilliseconds(5, "fortnights"))
}

// delay $ 表达式解析回退 60 秒（Python _resolve_delay_time），数值串按 float
// 解析、非数值串报 Python ValueError 同文案。
func TestDelaySecondsResolution(t *testing.T) {
	newDelay := func(v any) *Delay { return &Delay{BaseNode: BaseNode{ID: "d"}, DelaySeconds: v} }
	ctx := serverTestCtx()

	// $INPUT 缺键 → 求值 error → 60 秒回退。
	secs, err := newDelay("$INPUT.missing").resolveDelaySeconds(ctx)
	require.NoError(t, err)
	assert.InDelta(t, 60.0, secs, 0.0001)

	// $INPUT 数值 → 原值。
	secs, err = newDelay("$INPUT.secs").resolveDelaySeconds(runtime.WithWorkflowInput(ctx, map[string]any{"secs": 3}))
	require.NoError(t, err)
	assert.InDelta(t, 3.0, secs, 0.0001)

	// 数值串（非 $）→ float 解析。
	secs, err = newDelay("0.5").resolveDelaySeconds(ctx)
	require.NoError(t, err)
	assert.InDelta(t, 0.5, secs, 0.0001)

	// 非数值串 → Python float() 的 ValueError 口径（golden/日志逐字对拍子串）。
	_, err = newDelay("abc").resolveDelaySeconds(ctx)
	require.ErrorContains(t, err, "could not convert string to float: 'abc'")
}

// http_callback 自动路径：缺省/空串（Python 真值判伪）→ 自动生成；
// execution_id 缺省现场铸 uuid hex、flow_id 缺省字面 unknown。
func TestHTTPCallbackAutoPath(t *testing.T) {
	h := &HTTPCallback{BaseNode: BaseNode{ID: "h"}}
	rc := runtime.NewContext()
	path := h.callbackPath(serverTestCtx(), rc)
	auto, ok := path.(string)
	require.True(t, ok)
	assert.Equal(t, "/callback/unknown/h/", auto[:len("/callback/unknown/h/")])
	assert.Len(t, auto[len("/callback/unknown/h/"):], 32, "execution id must be uuid-hex shaped")

	// 显式空串也是 falsy → 自动路径（Python `if self.callback_path:`）。
	empty := ""
	h2 := &HTTPCallback{BaseNode: BaseNode{ID: "h"}, CallbackPath: empty}
	path2 := h2.callbackPath(serverTestCtx(), rc)
	assert.NotEqual(t, "", path2)

	// 显式路径原样返回（$ 解析走 resolveExtendedValue）。
	h3 := &HTTPCallback{BaseNode: BaseNode{ID: "h"}, CallbackPath: "/cb/x"}
	assert.Equal(t, "/cb/x", h3.callbackPath(serverTestCtx(), rc))
}

// kafka bootstrap_servers 串按逗号切分+trim；非串（list/None）原样透传。
func TestKafkaBootstrapSplit(t *testing.T) {
	k := &KafkaQueue{
		BaseNode:         BaseNode{ID: "k"},
		BootstrapServers: "b1:9092, b2:9092",
		Topic:            "t",
		GroupID:          "g",
	}
	res, err := k.Execute(serverTestCtx())
	require.NoError(t, err)
	sc := res.Result.(map[string]any)["service_config"].(map[string]any)
	servers := sc["kafka_config"].(map[string]any)["bootstrap_servers"]
	assert.Equal(t, []any{"b1:9092", "b2:9092"}, servers)

	k2 := &KafkaQueue{BaseNode: BaseNode{ID: "k"}, BootstrapServers: []any{"x:1"}, Topic: "t", GroupID: "g"}
	res2, err := k2.Execute(serverTestCtx())
	require.NoError(t, err)
	servers2 := res2.Result.(map[string]any)["service_config"].(map[string]any)["kafka_config"].(map[string]any)["bootstrap_servers"]
	assert.Equal(t, []any{"x:1"}, servers2)
}

// redis_queue 缺省值形状（pydantic default：host localhost / port 6379 /
// db 0 / list / json / 0s / batch 1）。
func TestRedisQueueDefaults(t *testing.T) {
	r := &RedisQueue{BaseNode: BaseNode{ID: "r"}, QueueName: "tasks"}
	res, err := r.Execute(serverTestCtx())
	require.NoError(t, err)
	sc := res.Result.(map[string]any)["service_config"].(map[string]any)
	assert.Equal(t, map[string]any{"host": "localhost", "port": 6379, "db": 0, "password": nil}, sc["redis_config"])
	assert.Equal(t, map[string]any{"name": "tasks", "type": "list", "message_format": "json"}, sc["queue_config"])
	assert.Equal(t, map[string]any{"timeout_seconds": 0, "batch_size": 1}, sc["listen_config"])
}

// require_auth=false → {"enabled": false}；token 型凭据缺省现场生成（32hex）。
func TestHTTPCallbackAuthConfig(t *testing.T) {
	no := false
	h := &HTTPCallback{BaseNode: BaseNode{ID: "h"}, RequireAuth: &no}
	assert.Equal(t, map[string]any{"enabled": false}, h.authConfig(serverTestCtx()))

	h2 := &HTTPCallback{BaseNode: BaseNode{ID: "h"}, AuthToken: "tok-123"}
	cfg := h2.authConfig(serverTestCtx())
	assert.Equal(t, map[string]any{"enabled": true, "type": "token", "token": "tok-123"}, cfg)

	h3 := &HTTPCallback{BaseNode: BaseNode{ID: "h"}}
	cfg3 := h3.authConfig(serverTestCtx())
	assert.Equal(t, true, cfg3["enabled"])
	assert.Len(t, cfg3["token"], 32)
}

// 五节点的 Execute 都产出 pending 状态 + service_config + node_subtype，
// service_config.type = 节点类型（BaseExtendedNode 合并语义）。
func TestServerNodesPendingShape(t *testing.T) {
	d := &Delay{BaseNode: BaseNode{ID: "d"}, DelaySeconds: 1}
	res, err := d.Execute(serverTestCtx())
	require.NoError(t, err)
	state := res.Result.(map[string]any)
	assert.Equal(t, EventStatusPending, state["status"])
	assert.Equal(t, "delay", state["node_subtype"])
	assert.True(t, state["is_async"] == true)
	assert.Contains(t, state["event_id"], "event_d_")
	require.NotZero(t, state["service_config"])
	assert.Equal(t, "delay", state["service_config"].(map[string]any)["type"])
}

// 取消传播：server 节点不睡眠不阻塞（挂起即返回），Execute 在已取消的 ctx
// 上照常产出挂起状态——等待发生在进程外，节点自身无占死消费者的盲区
// （#48 矩阵口径：挂起驻留型 Execute 即时返回）。
func TestServerNodesExecuteUnderCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(serverTestCtx())
	cancel()
	d := &Delay{BaseNode: BaseNode{ID: "d"}, DelaySeconds: 100}
	done := make(chan error, 1)
	go func() {
		_, err := d.Execute(ctx)
		done <- err
	}()
	select {
	case err := <-done:
		require.NoError(t, err, "suspending nodes return immediately even on a cancelled ctx")
	case <-time.After(time.Second):
		t.Fatal("delay Execute blocked on a cancelled context")
	}
}
