package worker

// #80 phase 3 负控（§5.2 写侧）：server 扩展节点挂起时，suspended 行必须带
// dispatch_pending 凭据落盘并 RPUSH `{prefix}:{subtype}:queue`（service_config
// + execution_id + tenant_id），成功后清凭据。写侧缺失时（pre-fix）队列空、
// 凭据缺——恢复只能靠手工事件，server 节点在生产链路上无人唤醒。

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const serverNodesDelayFlow = `{
  "flow_id": "server_delay_flow",
  "inputType": {"dataType": "any"},
  "nodes": [
    {"type": "start", "id": "start", "next": "wait"},
    {"type": "delay", "id": "wait", "delay_seconds": 30, "next": "end"},
    {"type": "end", "id": "end", "resultType": "success", "output": "resumed"}
  ]
}`

func TestServerNodeSuspendsAndDispatchesServiceTask(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	require.NoError(t, w.flows.PresetFlowDefinition(ctx, "", "server_delay_flow", "latest", []byte(serverNodesDelayFlow)))

	execID := newExecutionID()
	start := baseStartPayload(execID)
	start["flow_id"] = "server_delay_flow"
	enqueueTask(t, rdb, cfg, start, "")

	done := make(chan error, 1)
	go func() {
		_, err := w.ProcessOne(ctx)
		done <- err
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("delay flow did not suspend within 10s")
	}

	// suspended 行 + dispatch_pending 凭据已清（派发成功）。
	state := loadStateRow(t, rdb, cfg, execID)
	assert.Equal(t, StatusSuspended, state.Status, "delay node must park the flow")
	assert.Nil(t, state.DispatchPending, "credential must be cleared after a successful dispatch")

	// `{prefix}:delay:queue` 收到任务：service_config 全文 + execution_id + tenant_id。
	raw, err := rdb.LIndex(ctx, cfg.KeyPrefix+":delay:queue", 0).Result()
	require.NoError(t, err, "delay service queue must carry the dispatched task")
	var body map[string]any
	require.NoError(t, json.Unmarshal([]byte(raw), &body))
	assert.Equal(t, "delay", body["type"])
	assert.Equal(t, "wait", body["node_id"])
	assert.Equal(t, execID, body["execution_id"])
	assert.Equal(t, "default", body["tenant_id"])
	assert.InEpsilon(t, float64(30000), body["delay_ms"], 0.0001)
	assert.NotZero(t, body["trigger_timestamp"])

	// 台账：服务派发入 ledger（与队列同源写入）。
	// （dispatch ledger 只记 start/resume 载体入队，服务队列直推不经过
	// EnqueueWithLedger——与 Python 侧 RPUSH 语义一致，这里不断言 ledger。）
}

// 幂等面：派发成功后凭据已清，重复 start 载体（幂等门短路）不得向服务队列
// 重复派发。RPUSH 失败保留凭据的一侧由既有补派测试覆盖
// （TestRedeliveredStartSuspendedCompensatesDispatch / TestResumeSuspendedContinueCompensatesDispatch
// 直接构造带凭据的 suspended 行）。
func TestServerNodeDuplicateStartDoesNotRedispatch(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	require.NoError(t, w.flows.PresetFlowDefinition(ctx, "", "server_delay_flow", "latest", []byte(serverNodesDelayFlow)))

	execID := newExecutionID()
	start := baseStartPayload(execID)
	start["flow_id"] = "server_delay_flow"
	enqueueTask(t, rdb, cfg, start, "")

	done := make(chan error, 1)
	go func() {
		_, err := w.ProcessOne(ctx)
		done <- err
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("delay flow did not suspend within 10s")
	}

	// 重复 start 载体（幂等门）：不得向服务队列重复派发（凭据已清）。
	queueLenBefore, err := rdb.LLen(ctx, cfg.KeyPrefix+":delay:queue").Result()
	require.NoError(t, err)
	enqueueTask(t, rdb, cfg, start, "")
	done2 := make(chan error, 1)
	go func() {
		_, err := w.ProcessOne(ctx)
		done2 <- err
	}()
	select {
	case err := <-done2:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("duplicate start did not settle within 10s")
	}
	queueLenAfter, err := rdb.LLen(ctx, cfg.KeyPrefix+":delay:queue").Result()
	require.NoError(t, err)
	assert.Equal(t, queueLenBefore, queueLenAfter, "duplicate start must not re-dispatch the service task")
}
