// node_parity2 golden 对拍（#80 phase 2，r78）：testdata/node_parity2_golden.json
// 由 scripts/export_node_parity2_golden.py 从 plaita 真跑导出（deployment 形态
// IR → Flow.model_validate → [调度器角色: 按 flow_registry 注入 reference] →
// flow.run），每个用例记录期望输出或错误。这里用同一份 IR 走 ir.Load（reference
// 用例走 ir.LoadWithFlowProvider，registry 即 provider）+ wf.Execute 逐例对拍。
//
// 对拍口径与 collection_golden 相同：输出按 canonical JSON 比较；错误用例钉
// 发生性，带 error_contains 的（两侧逐字共有的确定性文案）加钉子串。
// reject_calculate / reject_ignore 是「两侧都拒载」的对拍：Python 报
// unRecognized node type，Go 在 ir.Load 拒绝（错误里的同款子串已钉）。

package ir

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/jeffkit/plaita-go/pkg/workflow"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNodeParity2Golden(t *testing.T) {
	data, err := os.ReadFile("testdata/node_parity2_golden.json")
	require.NoError(t, err)

	var cases []struct {
		Name         string                     `json:"name"`
		Flow         json.RawMessage            `json:"flow"`
		Input        any                        `json:"input"`
		FlowRegistry map[string]json.RawMessage `json:"flow_registry"`
		Expected     struct {
			Output        *json.RawMessage `json:"output"`
			Error         string           `json:"error"`
			ErrorContains string           `json:"error_contains"`
		} `json:"expected"`
	}
	require.NoError(t, json.Unmarshal(data, &cases))
	require.NotEmpty(t, cases)

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			var loaded *workflow.Workflow
			var loadErr error
			if tc.FlowRegistry != nil {
				loaded, loadErr = LoadWithFlowProvider(tc.Flow, registryProvider(tc.FlowRegistry))
			} else {
				loaded, loadErr = Load(tc.Flow)
			}
			// 拒载对拍：Python 模型层拒绝的 flow（节点类型未注册 /
			// SwitchLegacy 结构错误），Go 必须在 ir.Load 同样拒绝。
			isModelReject := strings.HasPrefix(tc.Expected.Error, "ValidationError") ||
				strings.Contains(tc.Expected.Error, "unRecognized node type")
			if isModelReject {
				require.Error(t, loadErr, "python raised: %s", tc.Expected.Error)
				if tc.Expected.ErrorContains != "" {
					assert.Contains(t, loadErr.Error(), tc.Expected.ErrorContains)
				}
				return
			}
			require.NoError(t, loadErr, "golden flow must load")

			got, execErr := loaded.Execute(t.Context(), tc.Input)
			if tc.Expected.Error != "" {
				require.Error(t, execErr, "python raised: %s", tc.Expected.Error)
				if tc.Expected.ErrorContains != "" {
					assert.Contains(t, execErr.Error(), tc.Expected.ErrorContains)
				}
				return
			}
			require.NoError(t, execErr)

			wantJSON, err := marshalCanonical(tc.Expected.Output)
			require.NoError(t, err)
			gotJSON, err := marshalCanonical(got)
			require.NoError(t, err)
			assert.JSONEq(t, wantJSON, gotJSON)
		})
	}
}

// registryProvider adapts a golden case's flow_registry into the loader's
// FlowProvider — the same role plaita's scheduler plays before flow.run.
func registryProvider(reg map[string]json.RawMessage) FlowProvider {
	return registryFunc(func(flowID, _ string) ([]byte, error) {
		raw, ok := reg[flowID]
		if !ok {
			return nil, fmt.Errorf("flow %q not in registry", flowID)
		}
		return raw, nil
	})
}

type registryFunc func(flowID, flowVersion string) ([]byte, error)

func (f registryFunc) LoadFlowJSON(flowID, flowVersion string) ([]byte, error) {
	return f(flowID, flowVersion)
}
