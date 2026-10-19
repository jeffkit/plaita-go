# plaita-go 影子对账报告（M2）

同一份部署形态 flow + 同一输入，分别在 plaita Python 引擎与 plaita-go 引擎执行，
比对最终输出与节点轨迹。样本全部为纯计算节点（无 http/redis/agentrun/notify 等副作用）。

- 生成时间：2026-10-19T23:27:08+08:00
- Python 引擎：`python3`（Python 3.13.3）@ `/Users/kong/projects/infra4agent/plaita`
- Go 引擎：go1.24.3
- 样本来源：`pkg/shadow/testdata/samples`

## 结论

**16 / 16 样本一致（100.0%）**，失败 0 个。

| 样本 | 结果 | Python 耗时 | Go 耗时 | 轨迹事件（py/go） | 说明 |
|---|---|---:|---:|---:|---|
| `01_assignment_concat` | ✅ pass | 431ms | 0ms | 3 / 3 | assignment 对象输出 + $F.concat/$F.add，end 直接引用 $NODE |
| `02_assignment_math_chain` | ✅ pass | 405ms | 0ms | 4 / 4 | 两级 assignment 数值运算链（$F.mul/sub/add/div），下游引用 $NODE 嵌套字段 |
| `03_assignment_template` | ✅ pass | 400ms | 0ms | 3 / 3 | {% ... %} 模板插值（表达式 + 字面花括号），验证两侧门面分发一致 |
| `04_switch_priority` | ✅ pass | 393ms | 1ms | 3 / 3 | switch 按 priority 降序命中中档分支（gte 90 / gte 60 / default） |
| `05_switch_default` | ✅ pass | 383ms | 0ms | 3 / 3 | switch 条件全部不命中时走 default 分支 |
| `06_switch_contains_and` | ✅ pass | 385ms | 0ms | 3 / 3 | switch 条件组 and + contains（列表元素 / 字符串子串） |
| `07_loop_double` | ✅ pass | 392ms | 0ms | 11 / 11 | loop 逐元素执行 inline 子流程，取末次子结果（plaita 语义） |
| `08_loop_with_index` | ✅ pass | 410ms | 0ms | 9 / 9 | loop 子流程同时使用 $INPUT.item 与 $INPUT.index（索引从 0 起） |
| `09_loop_empty` | ✅ pass | 409ms | 0ms | 3 / 3 | 空集合 loop：两侧都返回 None（末次结果为空） |
| `10_parallel_join` | ✅ pass | 396ms | 0ms | 7 / 7 | parallel 两分支并发 + join，分支未声明 input 时继承父 $INPUT |
| `11_parallel_mixed_input` | ✅ pass | 456ms | 0ms | 7 / 7 | parallel 分支输入：一路显式表达式 input，一路继承父 $INPUT |
| `12_child_inline` | ✅ pass | 426ms | 0ms | 5 / 5 | inline child 节点：输入表达式注入 + 子流程 $INPUT.who |
| `13_child_with_loop` | ✅ pass | 424ms | 0ms | 12 / 12 | 嵌套组合：child -> loop -> inline 子流程，结果逐层回传 |
| `14_parse_json_success` | ✅ pass | 415ms | 0ms | 3 / 3 | parse_json 成功路径：末行 JSON 提取 + verdict 白名单 + join_fields 平铺 |
| `15_parse_json_fallback` | ✅ pass | 439ms | 0ms | 3 / 3 | parse_json 失败兜底：default 合并 + parse_ok=false + notes 追加明细 |
| `16_parallel_switch_nested` | ✅ pass | 450ms | 0ms | 8 / 8 | 嵌套组合：parallel 两分支，一路 switch 分类、一路平方计算，join 后汇总 |

## 比对口径

1. **最终输出**：JSON 值深度比较；`int`/`float` 按数值等价（`1 == 1.0`），容器递归、对象键序无关。
2. **节点轨迹**：两侧都按 `flow_id` 分组，组内按执行顺序比较 `node_id`、节点类型、节点结果与错误有无；
   并行分支的跨 flow 交错顺序不参与比较（组内顺序仍然比较）。
3. **样本期望值**：每个样本带 `expect`，Python 与 Go 的输出都对照它检查——防止两侧同错时误判一致。
4. **声明分歧（不计 fail）**：
   - 键序：Go `map` 序列化与 Python `dict` 的键序不同——比较器按键集合递归比较，天然吸收；
   - int/float 渲染：Python `1` 与 Go `1`/`1.0` 的 JSON 词法不同——比较器用精确有理数比较，天然吸收。
5. 任一引擎执行失败即判 fail（样本均为应成功的纯计算 flow）；失败详情含引擎错误原文。

## 已知限制

- 嵌入流程（loop/child/parallel 分支）必须在部署 JSON 里显式声明 `flow_id`：轨迹按 `flow_id` 分组，
  缺失时 Go 侧 ir 归一化会合成 id 而 Python 侧为空，两侧分组不可比。
- Go 的节点事件按**每次尝试**触发，Python callback 每个节点只触发一次。含节点重试的 flow（M3 生产样本）
  需要按尝试次数归一再比；当前合成样本不含重试。
- 轨迹只比较节点结束事件（id/类型/结果/错误有无），不比较节点耗时。

## 复现

```bash
GOPROXY=https://goproxy.cn,direct go run ./cmd/shadow-compare \
  -python-repo /Users/kong/projects/infra4agent/plaita \
  -md docs/SHADOW_REPORT.md -json /tmp/shadow-report.json
```

单个 flow（M3 生产 flow 只读回放用）：

```bash
GOPROXY=https://goproxy.cn,direct go run ./cmd/shadow-compare \
  -flow path/to/flow.json -input path/to/input.json \
  -python-repo /Users/kong/projects/infra4agent/plaita -json /tmp/shadow-one.json
```

集成测试（无 Python 仓库时自动 skip）：

```bash
GOPROXY=https://goproxy.cn,direct go test ./pkg/shadow/ -run TestSamples -v
```
