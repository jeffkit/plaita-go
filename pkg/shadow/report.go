package shadow

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

// Report is the full reconciliation report.
type Report struct {
	GeneratedAt   string         `json:"generated_at"`
	Source        string         `json:"source"`
	PythonRepo    string         `json:"python_repo"`
	PythonCommand string         `json:"python_command"`
	PythonVersion string         `json:"python_version"`
	GoVersion     string         `json:"go_version"`
	Samples       []SampleResult `json:"samples"`
	Summary       Summary        `json:"summary"`
}

// Summary aggregates pass/fail statistics.
type Summary struct {
	Total    int     `json:"total"`
	Passed   int     `json:"passed"`
	Failed   int     `json:"failed"`
	PassRate float64 `json:"pass_rate"`
}

// NewReport builds a report from sample results.
func NewReport(results []SampleResult, source, pythonRepo, pythonCommand, goVersion string) *Report {
	summary := Summary{Total: len(results)}
	for _, result := range results {
		if result.Pass {
			summary.Passed++
		} else {
			summary.Failed++
		}
	}
	if summary.Total > 0 {
		summary.PassRate = float64(summary.Passed) / float64(summary.Total) * 100
	}
	return &Report{
		GeneratedAt:   time.Now().Format(time.RFC3339),
		Source:        source,
		PythonRepo:    pythonRepo,
		PythonCommand: pythonCommand,
		GoVersion:     goVersion,
		Samples:       results,
		Summary:       summary,
	}
}

// WriteJSON writes the structured report.
func (r *Report) WriteJSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	return enc.Encode(r)
}

// WriteText writes a human-readable summary followed by per-sample verdicts
// and the details of every failing diff.
func (r *Report) WriteText(w io.Writer) {
	fmt.Fprintf(w, "plaita-go shadow reconciliation: %d/%d passed (%.1f%%)\n",
		r.Summary.Passed, r.Summary.Total, r.Summary.PassRate)
	fmt.Fprintf(w, "python: %s @ %s | go: %s\n\n", r.PythonCommand, r.PythonRepo, r.GoVersion)

	for _, sample := range r.Samples {
		status := "PASS"
		if !sample.Pass {
			status = "FAIL"
		}
		fmt.Fprintf(w, "%-4s %s (python %dms, go %dms, trace %d/%d)\n",
			status, sample.Name, sample.PythonDurationMS, sample.GoDurationMS,
			eventCount(sample.Python), eventCount(sample.Go))
		for _, diff := range sample.Diffs {
			fmt.Fprintf(w, "     %s [%s] %s\n", diff.Path, diff.Kind, diff.Detail)
			if diff.Detail == "" {
				fmt.Fprintf(w, "     %s [%s] python=%s go=%s\n",
					diff.Path, diff.Kind, compactJSON(diff.Reference), compactJSON(diff.Actual))
			}
		}
		for _, diff := range sample.ExpectDiffs {
			fmt.Fprintf(w, "     %s [%s] %s (expect=%s actual=%s)\n",
				diff.Path, diff.Kind, diff.Detail,
				compactJSON(diff.Reference), compactJSON(diff.Actual))
		}
	}
}

// WriteMarkdown writes the checked-in report (docs/SHADOW_REPORT.md).
func (r *Report) WriteMarkdown(w io.Writer) {
	fmt.Fprintln(w, "# plaita-go 影子对账报告（M2）")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "同一份部署形态 flow + 同一输入，分别在 plaita Python 引擎与 plaita-go 引擎执行，")
	fmt.Fprintln(w, "比对最终输出与节点轨迹。样本全部为纯计算节点（无 http/redis/agentrun/notify 等副作用）。")
	fmt.Fprintln(w)
	fmt.Fprintf(w, "- 生成时间：%s\n", r.GeneratedAt)
	fmt.Fprintf(w, "- Python 引擎：`%s`（%s）@ `%s`\n", r.PythonCommand, r.PythonVersion, r.PythonRepo)
	fmt.Fprintf(w, "- Go 引擎：%s\n", r.GoVersion)
	fmt.Fprintf(w, "- 样本来源：`%s`\n", r.Source)
	fmt.Fprintln(w)
	fmt.Fprintln(w, "## 结论")
	fmt.Fprintln(w)
	fmt.Fprintf(w, "**%d / %d 样本一致（%.1f%%）**，失败 %d 个。\n",
		r.Summary.Passed, r.Summary.Total, r.Summary.PassRate, r.Summary.Failed)
	fmt.Fprintln(w)
	fmt.Fprintln(w, "| 样本 | 结果 | Python 耗时 | Go 耗时 | 轨迹事件（py/go） | 说明 |")
	fmt.Fprintln(w, "|---|---|---:|---:|---:|---|")
	for _, sample := range r.Samples {
		status := "✅ pass"
		if !sample.Pass {
			status = "❌ fail"
		}
		fmt.Fprintf(w, "| `%s` | %s | %dms | %dms | %d / %d | %s |\n",
			sample.Name, status, sample.PythonDurationMS, sample.GoDurationMS,
			eventCount(sample.Python), eventCount(sample.Go), escapeTable(sample.Description))
	}
	fmt.Fprintln(w)

	failing := make([]SampleResult, 0)
	for _, sample := range r.Samples {
		if !sample.Pass {
			failing = append(failing, sample)
		}
	}
	if len(failing) > 0 {
		fmt.Fprintln(w, "## 差异详情")
		fmt.Fprintln(w)
		for _, sample := range failing {
			fmt.Fprintf(w, "### `%s`\n", sample.Name)
			fmt.Fprintln(w)
			if sample.Python != nil && !sample.Python.OK {
				fmt.Fprintf(w, "- Python 失败：`%s`\n", sample.Python.Error)
			}
			if sample.Go != nil && !sample.Go.OK {
				fmt.Fprintf(w, "- Go 失败：`%s`\n", sample.Go.Error)
			}
			for _, diff := range sample.Diffs {
				fmt.Fprintf(w, "- `%s` [%s] %s（python=%s, go=%s）\n",
					diff.Path, diff.Kind, diff.Detail,
					compactJSON(diff.Reference), compactJSON(diff.Actual))
			}
			for _, diff := range sample.ExpectDiffs {
				fmt.Fprintf(w, "- `%s` [%s] %s（expect=%s, actual=%s）\n",
					diff.Path, diff.Kind, diff.Detail,
					compactJSON(diff.Reference), compactJSON(diff.Actual))
			}
			fmt.Fprintln(w)
		}
	}

	fmt.Fprintln(w, "## 比对口径")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "1. **最终输出**：JSON 值深度比较；`int`/`float` 按数值等价（`1 == 1.0`），容器递归、对象键序无关。")
	fmt.Fprintln(w, "2. **节点轨迹**：两侧都按 `flow_id` 分组，组内按执行顺序比较 `node_id`、节点类型、节点结果与错误有无；")
	fmt.Fprintln(w, "   并行分支的跨 flow 交错顺序不参与比较（组内顺序仍然比较）。")
	fmt.Fprintln(w, "3. **样本期望值**：每个样本带 `expect`，Python 与 Go 的输出都对照它检查——防止两侧同错时误判一致。")
	fmt.Fprintln(w, "4. **声明分歧（不计 fail）**：")
	fmt.Fprintln(w, "   - 键序：Go `map` 序列化与 Python `dict` 的键序不同——比较器按键集合递归比较，天然吸收；")
	fmt.Fprintln(w, "   - int/float 渲染：Python `1` 与 Go `1`/`1.0` 的 JSON 词法不同——比较器用精确有理数比较，天然吸收。")
	fmt.Fprintln(w, "5. 任一引擎执行失败即判 fail（样本均为应成功的纯计算 flow）；失败详情含引擎错误原文。")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "## 已知限制")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "- 嵌入流程（loop/child/parallel 分支）必须在部署 JSON 里显式声明 `flow_id`：轨迹按 `flow_id` 分组，")
	fmt.Fprintln(w, "  缺失时 Go 侧 ir 归一化会合成 id 而 Python 侧为空，两侧分组不可比。")
	fmt.Fprintln(w, "- Go 的节点事件按**每次尝试**触发，Python callback 每个节点只触发一次。含节点重试的 flow（M3 生产样本）")
	fmt.Fprintln(w, "  需要按尝试次数归一再比；当前合成样本不含重试。")
	fmt.Fprintln(w, "- 轨迹只比较节点结束事件（id/类型/结果/错误有无），不比较节点耗时。")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "## 复现")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "```bash")
	fmt.Fprintln(w, "GOPROXY=https://goproxy.cn,direct go run ./cmd/shadow-compare \\")
	fmt.Fprintf(w, "  -python-repo %s \\\n", r.PythonRepo)
	fmt.Fprintln(w, "  -md docs/SHADOW_REPORT.md -json /tmp/shadow-report.json")
	fmt.Fprintln(w, "```")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "单个 flow（M3 生产 flow 只读回放用）：")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "```bash")
	fmt.Fprintln(w, "GOPROXY=https://goproxy.cn,direct go run ./cmd/shadow-compare \\")
	fmt.Fprintln(w, "  -flow path/to/flow.json -input path/to/input.json \\")
	fmt.Fprintf(w, "  -python-repo %s -json /tmp/shadow-one.json\n", r.PythonRepo)
	fmt.Fprintln(w, "```")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "集成测试（无 Python 仓库时自动 skip）：")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "```bash")
	fmt.Fprintln(w, "GOPROXY=https://goproxy.cn,direct go test ./pkg/shadow/ -run TestSamples -v")
	fmt.Fprintln(w, "```")
}

func eventCount(outcome *Outcome) int {
	if outcome == nil {
		return 0
	}
	return len(outcome.Trace)
}

func escapeTable(s string) string {
	return strings.ReplaceAll(s, "|", "\\|")
}
