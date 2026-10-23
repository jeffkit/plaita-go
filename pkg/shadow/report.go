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
	fmt.Fprintln(w, "# plaita-go 影子对账报告（M2/#317）")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "同一份部署形态 flow + 同一输入，分别在 plaita Python 引擎与 plaita-go 引擎执行，")
	fmt.Fprintln(w, "比对最终输出与节点轨迹。四个维度的合成语料：纯计算（01-16，无任何副作用节点）、")
	fmt.Fprintln(w, "错误分类与重试归属（17-22，code/hermetic http + abort 重试预算）、节点结果形状（23-26，")
	fmt.Fprintln(w, "end 三态/数字错误码/缺失变量/null 穿透）、挂起恢复（27-28，event 载体/continue 载体）。")
	fmt.Fprintln(w, "http 用例全部 hermetic（非 http scheme 或 loopback，测试期守卫强制）。")
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
	fmt.Fprintln(w, "| 样本 | 维度 | 结果 | Python 耗时 | Go 耗时 | 轨迹事件（py/go） | 说明 |")
	fmt.Fprintln(w, "|---|---|---|---:|---:|---:|---|")
	for _, sample := range r.Samples {
		status := "✅ pass"
		if !sample.Pass {
			status = "❌ fail"
		}
		dimension := sample.Dimension
		if dimension == "" {
			dimension = "pure"
		}
		fmt.Fprintf(w, "| `%s` | %s | %s | %dms | %dms | %d / %d | %s |\n",
			sample.Name, dimension, status, sample.PythonDurationMS, sample.GoDurationMS,
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
	fmt.Fprintln(w, "   - int/float 渲染：Python `1` 与 Go `1`/`1.0` 的 JSON 词法不同——比较器用精确有理数比较，天然吸收；")
	fmt.Fprintln(w, "   - `load-refusal:go`（样本 23）：go 装载期具名拒 `end resultType:\"nop\"`（fail-closed，从严），")
	fmt.Fprintln(w, "     Python 静默结束 result=null——用例钉 Python 行为并断言 go 拒载，声明不关闭期望检查；")
	fmt.Fprintln(w, "   - `replay-from-start:go`（样本 28）：go worker 的 continue 载体从 flow 起点重入（已完成前序节点重放，")
	fmt.Fprintln(w, "     AGENTS.md §worker 二期，Stage 1 中途入口落地后收敛）——比较前两侧轨迹投影到每节点首现，")
	fmt.Fprintln(w, "     重放伪影的 attempts 不进比对；原始轨迹保留双现证据。")
	fmt.Fprintln(w, "5. **失败形态判据**：常规样本任一引擎失败即 fail；`expectError` 样本（17-22/24/28）两侧必须都终态失败")
	fmt.Fprintln(w, "   且错误文本含 `expectError.match`；`load-refusal:go` 样本 go 必须装载拒绝而 Python 必须跑通。")
	fmt.Fprintln(w, "6. **重试归属**：轨迹事件带每节点尝试数（retries+1）——go 按尝试触发 OnNodeEnd 折叠合并，")
	fmt.Fprintln(w, "   Python 经 runner 重试告警的 logging.Handler 计数；确定性错误烧重试预算即红（E1-E6 钉 #43 口径）。")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "## 已知限制")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "- 嵌入流程（loop/child/parallel 分支）必须在部署 JSON 里显式声明 `flow_id`：轨迹按 `flow_id` 分组，")
	fmt.Fprintln(w, "  缺失时 Go 侧 ir 归一化会合成 id 而 Python 侧为空，两侧分组不可比。")
	fmt.Fprintln(w, "- event-resume 形态（样本 27）的比较面是恢复后节点状态的确定性公共核：event_id（毫秒时间戳）、")
	fmt.Fprintln(w, "  subscription_id/tenant（运行时指派）与挂起态形状extras（event_filter/is_async/subscription_timeout，")
	fmt.Fprintln(w, "  两侧结构性不同）递归擦除；事件节点投影到恢复后末次出现（两侧都在挂起与恢复时各触发一次 node-end）。")
	fmt.Fprintln(w, "- 轨迹只比较节点结束事件（id/类型/结果/错误有无/尝试数），不比较节点耗时。")
	fmt.Fprintln(w, "- Rust 引擎不在本 harness 内：go↔rust 双跑走 `tools/diffcheck.py`，三列消费矩阵见 plaita 仓")
	fmt.Fprintln(w, "  `docs/governance/conformance-matrix.md`（#317 §2/§3）。")
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
