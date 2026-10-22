package ir

// collection golden 对拍（#80 phase 1）：testdata/collection_golden.json 由
// scripts/export_collection_golden.py 从 plaita 真跑导出（deployment 形态 IR →
// Flow.model_validate → flow.run），每个用例记录期望输出或错误。这里用同一份
// IR 走 ir.Load + wf.Execute，逐例对拍——「Python 真跑 = Go 真跑」的契约钉。
//
// 对拍口径：输出按 canonical JSON 逐字节比（Python int 与 Go 数值类型的
// marshal 形状一致：3 == float64(3) == int64(3) 的 JSON 都是 "3"）；错误用例
// 钉发生性，带 error_contains 的（两侧逐字共有的确定性文案）加钉子串。

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollectionGolden(t *testing.T) {
	data, err := os.ReadFile("testdata/collection_golden.json")
	require.NoError(t, err)

	var cases []struct {
		Name     string          `json:"name"`
		Flow     json.RawMessage `json:"flow"`
		Input    any             `json:"input"`
		Expected struct {
			Output        *json.RawMessage `json:"output"`
			Error         string           `json:"error"`
			ErrorContains string           `json:"error_contains"`
		} `json:"expected"`
	}
	require.NoError(t, json.Unmarshal(data, &cases))
	require.NotEmpty(t, cases)

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			wf, err := Load(tc.Flow)
			require.NoError(t, err, "golden flow must load")

			got, execErr := wf.Execute(t.Context(), tc.Input)
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

// marshalCanonical renders a value as canonical JSON. The exported expected
// values are raw JSON; actual results are Go values — both go through
// encoding/json so numeric shapes converge on the wire format.
func marshalCanonical(v any) (string, error) {
	if raw, ok := v.(*json.RawMessage); ok {
		if raw == nil {
			return "null", nil
		}
		return string(*raw), nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
