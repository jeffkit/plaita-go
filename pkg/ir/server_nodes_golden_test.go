// server_nodes golden 对拍（#80 phase 3，r79）：testdata/server_nodes_golden.json
// 由 scripts/export_server_nodes_golden.py 从 plaita 真跑导出——delay/approval/
// http_callback 的恢复载荷由**真实 service**（DelayService.handle_task /
// ApprovalService.submit_approval_decision / HttpCallbackService.
// handle_callback_request）发布、InMemoryEventBus 捕获；redis_queue/kafka_queue
// 的 service 依赖真实 broker，载荷按其 service 源码的事件构造分支手建。
//
// 对拍口径（脚本 docstring 详述）：plaita 的分布式 _handle_resume 只处理被
// 恢复节点本身（不续跑后继图），Go 的 Workflow.Resume 恢复+续跑一步到位——
// 两侧共有的可比较面 = 恢复后节点状态。Go 侧 end 节点 output=$NODE.<server>
// 恰为该状态。挂起态对 checkpoint $NODE 对拍。
//
// 规范化（两侧同款）：剔键 subscription_id/subscription_tenant/created_time/
// trigger_timestamp/actual_trigger_timestamp/planned_trigger_timestamp/
// timestamp/recorded_at；串内 32hex→<hex32>、13 位毫秒→<ms13>。Go 侧执行
// 现场注入 32hex execution id（httpcb_auto_path 的自动路径打码对齐依赖它）。
// 恢复载荷的订阅绑定键由本测试注入各自执行现场的 subscription_id（两侧
// RESUME_BINDING_KEY 准入同款）。

package ir

import (
	"context"
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/workflow"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var goldenHex32 = regexp.MustCompile(`[0-9a-f]{32}`)
var goldenMs13 = regexp.MustCompile(`[0-9]{13}`)

var goldenDenylistKeys = map[string]bool{
	"subscription_id":           true,
	"subscription_tenant":       true,
	"created_time":              true,
	"trigger_timestamp":         true,
	"actual_trigger_timestamp":  true,
	"planned_trigger_timestamp": true,
	"timestamp":                 true,
	"recorded_at":               true,
}

func normalizeGolden(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			if goldenDenylistKeys[k] {
				continue
			}
			out[k] = normalizeGolden(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = normalizeGolden(val)
		}
		return out
	case string:
		return goldenMs13.ReplaceAllString(goldenHex32.ReplaceAllString(t, "<hex32>"), "<ms13>")
	default:
		return v
	}
}

type goldenSubscriptionRegistry struct {
	mu  sync.Mutex
	seq int
}

func (r *goldenSubscriptionRegistry) Register(_ context.Context, req *workflow.SubscriptionRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seq++
	req.SubscriptionID = "golden-sub-" + string(rune('a'+r.seq))
	return nil
}

func (r *goldenSubscriptionRegistry) Unregister(_ context.Context, _, _ string) error { return nil }

func TestServerNodesGolden(t *testing.T) {
	data, err := os.ReadFile("testdata/server_nodes_golden.json")
	require.NoError(t, err)

	var cases []struct {
		Name        string `json:"name"`
		Flow        any    `json:"flow"`
		Input       any    `json:"input"`
		SuspendNode string `json:"suspend_node"`
		Resume      map[string]any
		Expected    struct {
			Suspend       map[string]any `json:"suspend"`
			Output        any            `json:"output"`
			Error         string         `json:"error"`
			ErrorContains string         `json:"error_contains"`
		} `json:"expected"`
	}
	require.NoError(t, json.Unmarshal(data, &cases))
	require.NotEmpty(t, cases)

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			flowJSON, err := json.Marshal(tc.Flow)
			require.NoError(t, err)
			wf, loadErr := Load(flowJSON)
			if tc.Expected.Error != "" {
				// 拒绝对拍：pydantic 模型层拒绝的 flow，Go 在 ir.Load 同拒。
				require.Error(t, loadErr, "python raised: %s", tc.Expected.Error)
				if tc.Expected.ErrorContains != "" {
					assert.Contains(t, loadErr.Error(), tc.Expected.ErrorContains)
				}
				return
			}
			require.NoError(t, loadErr)

			// 32hex execution id：httpcb_auto_path 的自动路径打码对齐依赖。
			execID := strings.Repeat("a", 32)
			ctx := workflow.WithSuspendOptions(t.Context(), &workflow.SuspendOptions{
				ExecutionID:   execID,
				FlowID:        tc.Name,
				Subscriptions: &goldenSubscriptionRegistry{},
			})
			ctx = runtime.WithExecutionID(ctx, execID)

			_, execErr := wf.Execute(ctx, tc.Input)
			susp := workflow.AsSuspend(execErr)
			require.NotNil(t, susp, "server node must suspend, got: %v", execErr)

			// 挂起态对拍（checkpoint $NODE 表）。
			var cp struct {
				Nodes map[string]map[string]any `json:"$NODE"`
			}
			raw, err := json.Marshal(susp.Checkpoint.Context)
			require.NoError(t, err)
			require.NoError(t, json.Unmarshal(raw, &cp))
			state, ok := cp.Nodes[tc.SuspendNode]
			require.True(t, ok, "suspended node %q must appear in $NODE", tc.SuspendNode)

			wantSuspend, err := marshalCanonical(tc.Expected.Suspend)
			require.NoError(t, err)
			gotSuspend, err := marshalCanonical(normalizeGolden(state))
			require.NoError(t, err)
			assert.JSONEq(t, wantSuspend, gotSuspend)

			if tc.Resume == nil {
				// 无恢复用例（错误凭据）：断言面 = 挂起态本身（已比）。
				return
			}

			// 恢复：注入本执行现场的订阅绑定（两侧 RESUME_BINDING_KEY 准入同款）。
			payload := make(map[string]any, len(tc.Resume)+1)
			for k, v := range tc.Resume {
				payload[k] = v
			}
			payload["__plaita_subscription_id"] = state["subscription_id"]
			result, err := wf.Resume(ctx, susp.Checkpoint, workflow.ResumeEvent, payload)
			require.NoError(t, err)

			wantOutput, err := marshalCanonical(tc.Expected.Output)
			require.NoError(t, err)
			gotOutput, err := marshalCanonical(normalizeGolden(result))
			require.NoError(t, err)
			assert.JSONEq(t, wantOutput, gotOutput)
		})
	}
}
