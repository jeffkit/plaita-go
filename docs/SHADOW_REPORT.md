# plaita-go 影子对账报告（M2/#317）

同一份部署形态 flow + 同一输入，分别在 plaita Python 引擎与 plaita-go 引擎执行，
比对最终输出与节点轨迹。四个维度的合成语料：纯计算（01-16，无任何副作用节点）、
错误分类与重试归属（17-22，code/hermetic http + abort 重试预算）、节点结果形状（23-26，
end 三态/数字错误码/缺失变量/null 穿透）、挂起恢复（27-28，event 载体/continue 载体）。
http 用例全部 hermetic（非 http scheme 或 loopback，测试期守卫强制）。

- 生成时间：2026-10-24T02:36:03+08:00
- Python 引擎：`python3`（Python 3.13.3）@ `/Users/kong/projects/infra4agent/plaita`
- Go 引擎：go1.24.3
- 样本来源：`pkg/shadow/testdata/samples`

## 结论

**28 / 28 样本一致（100.0%）**，失败 0 个。

| 样本 | 维度 | 结果 | Python 耗时 | Go 耗时 | 轨迹事件（py/go） | 说明 |
|---|---|---|---:|---:|---:|---|
| `01_assignment_concat` | pure | ✅ pass | 395ms | 0ms | 3 / 3 | assignment 对象输出 + $F.concat/$F.add，end 直接引用 $NODE |
| `02_assignment_math_chain` | pure | ✅ pass | 393ms | 0ms | 4 / 4 | 两级 assignment 数值运算链（$F.mul/sub/add/div），下游引用 $NODE 嵌套字段 |
| `03_assignment_template` | pure | ✅ pass | 395ms | 0ms | 3 / 3 | {% ... %} 模板插值（表达式 + 字面花括号），验证两侧门面分发一致 |
| `04_switch_priority` | pure | ✅ pass | 391ms | 0ms | 3 / 3 | switch 按 priority 降序命中中档分支（gte 90 / gte 60 / default） |
| `05_switch_default` | pure | ✅ pass | 389ms | 0ms | 3 / 3 | switch 条件全部不命中时走 default 分支 |
| `06_switch_contains_and` | pure | ✅ pass | 398ms | 0ms | 3 / 3 | switch 条件组 and + contains（列表元素 / 字符串子串） |
| `07_loop_double` | pure | ✅ pass | 394ms | 0ms | 11 / 11 | loop 逐元素执行 inline 子流程，取末次子结果（plaita 语义） |
| `08_loop_with_index` | pure | ✅ pass | 396ms | 0ms | 9 / 9 | loop 子流程同时使用 $INPUT.item 与 $INPUT.index（索引从 0 起） |
| `09_loop_empty` | pure | ✅ pass | 388ms | 0ms | 3 / 3 | 空集合 loop：两侧都返回 None（末次结果为空） |
| `10_parallel_join` | pure | ✅ pass | 397ms | 1ms | 7 / 7 | parallel 两分支并发 + join，分支未声明 input 时继承父 $INPUT |
| `11_parallel_mixed_input` | pure | ✅ pass | 441ms | 0ms | 7 / 7 | parallel 分支输入：一路显式表达式 input，一路继承父 $INPUT |
| `12_child_inline` | pure | ✅ pass | 390ms | 0ms | 5 / 5 | inline child 节点：输入表达式注入 + 子流程 $INPUT.who |
| `13_child_with_loop` | pure | ✅ pass | 404ms | 0ms | 12 / 12 | 嵌套组合：child -> loop -> inline 子流程，结果逐层回传 |
| `14_parse_json_success` | pure | ✅ pass | 388ms | 0ms | 3 / 3 | parse_json 成功路径：末行 JSON 提取 + verdict 白名单 + join_fields 平铺 |
| `15_parse_json_fallback` | pure | ✅ pass | 394ms | 0ms | 3 / 3 | parse_json 失败兜底：default 合并 + parse_ok=false + notes 追加明细 |
| `16_parallel_switch_nested` | pure | ✅ pass | 438ms | 0ms | 8 / 8 | 嵌套组合：parallel 两分支，一路 switch 分类、一路平方计算，join 后汇总 |
| `17_error_e1_valueerror_nonretryable` | error-classification | ✅ pass | 425ms | 28ms | 2 / 2 | E1 错误分类×重试归属：ValueError（确定性四类白名单）不烧重试——py 信封 origin_type 判不重试、go pyDeterministicEnvelopeTypes 判 NonRetryable，retryTimes=5 预算全额保留（plaita-go#43 裁决口径） |
| `18_error_e2_keyerror_retryable` | error-classification | ✅ pass | 3489ms | 3098ms | 2 / 2 | E2 错误分类×重试归属：KeyError（四类白名单之外）维持历史可重试缺省——两侧各烧满 retryTimes=2 即 3 次尝试；钉「白名单语义」而非「凡异常皆不重试」 |
| `19_error_e3_exit_noenvelope_retryable` | error-classification | ✅ pass | 3490ms | 3092ms | 2 / 2 | E3 错误分类×重试归属：沙箱进程 os._exit(2) 无信封非零退出=基础设施失败维持可重试——与 E2 分打「退出状态」路径（E2 是信封解码路径），两侧各 3 次尝试 |
| `20_error_e4_end_errorresult_nonretryable` | error-classification | ✅ pass | 396ms | 0ms | 2 / 2 | E4 错误分类×重试归属：end resultType=error 是 flow 的决定不是瞬态故障（plaita-go#43 裁决原文）——py ErrorResultException 逃出重试环、go 构造点即 NonRetryable，retryTimes=3 下 1 次尝试 |
| `21_error_e5_http_unsupported_scheme` | error-classification | ✅ pass | 7402ms | 7006ms | 2 / 2 | E5 http 错误分类：ftp:// 不支持协议——实测两侧都落在 do-request 期 code=1002 瞬态可重试（非 1001 生成期，修正 A1 评审表格引文），retryTimes=3 下各 4 次尝试；hermetic 零网络（scheme 在 dial 前被拒） |
| `22_error_e6_http_loopback_refused` | error-classification | ✅ pass | 3417ms | 3005ms | 2 / 2 | E6 http 错误分类：loopback 连接拒绝=传输瞬态 1002 可重试——显式 127.0.0.1 避代理 env 面（本机 HTTP_PROXY 有把不可达主机变 502 值的前科），retryTimes=2 下各 3 次尝试 |
| `23_shape_s1_end_nop` | result-shape | ✅ pass | 394ms | 0ms | 3 / 0 | S1 结果形状：end resultType=nop（三态里唯一没被 16 例覆盖的分支）——py 支持：静默结束 result=null；实测 go 在装载期 fail-closed 拒绝（silent end not supported）=已声明装载面分歧（go 列 red-declared：拒绝方向更严，不吸收） |
| `24_shape_s2_end_error_numeric_code` | result-shape | ✅ pass | 389ms | 0ms | 2 / 2 | S2 结果形状：end error 携带数字 code=12345——py ErrorResultException code int 保真、go HandlerError.Code int 解码保真，两侧错误文本均携带消息；code 类型漂移会打断消费方 switch（rs EndError.code 是 String 的风险面另单） |
| `25_shape_s3_end_output_missing_var` | result-shape | ✅ pass | 389ms | 0ms | 2 / 2 | S3 结果形状：end output 引用缺失变量 $INPUT.nothere——实测两侧同款静默折叠 null（模板方言「静默空值」族在执行面的钉子；16 例只测过正常插值 03） |
| `26_shape_s4_code_null_penetration` | result-shape | ✅ pass | 422ms | 28ms | 3 / 3 | S4 结果形状：code 返回 {a: null} 后 end output 走 $NODE.c.a.b null 穿透求值——实测两侧同款折叠 null（#63 修的是文法投影层，本例钉求值层；16 例全是存在路径，反向面零覆盖） |
| `27_suspendresume_r1_event_resume` | suspend-resume | ✅ pass | 579ms | 0ms | 4 / 4 | R1 suspend/resume×event 载体：start→event→assignment→end，driver=event-resume。比较口径=恢复后节点状态经公共可见面（status 翻转 completed + event_data 保真，经 assignment 复制进 end 输出）。event_id 含毫秒时间戳且挂起态形状两侧结构性不同（go 恢复态多 event_filter/is_async/subscription_id，py 无），均不进比对面；plaita 分布式 resume 只处理被恢复节点本身、go Workflow.Resume 恢复+续跑一步到位——AGENTS.md「恢复状态形状（Python 实测钉死）」的共有面口径第一次进自动对拍 |
| `28_suspendresume_r2_continue_replay` | suspend-resume | ✅ pass | 429ms | 56ms | 3 / 6 | R2 suspend/resume×continue 载体（声明分叉 replay-from-start:go）：start→assignment A→code 必败(abort retryTimes=1，ValueError 确定性两侧均 1 次尝试)→end，driver=continue-after-error。实测：py 分布式首拍止于失败边界（start+a 后返回），continue 载体自 checkpoint 步进恰好重跑失败节点（失败节点无 checkpoint 条目）——A 只现一次；go 整链终态失败后从 flow 起点重入——A 双现（原始轨迹 6 事件留证，AGENTS.md §worker 二期）⇒ 副作用节点重复执行风险（#317 §3 声明分叉，矩阵 red-declared，收敛指针=Stage 1 中途入口单）。投影后节点序一致；attempts 豁免（重入伪影）。终态两侧同为 ValueError 具名失败 |

## 比对口径

1. **最终输出**：JSON 值深度比较；`int`/`float` 按数值等价（`1 == 1.0`），容器递归、对象键序无关。
2. **节点轨迹**：两侧都按 `flow_id` 分组，组内按执行顺序比较 `node_id`、节点类型、节点结果与错误有无；
   并行分支的跨 flow 交错顺序不参与比较（组内顺序仍然比较）。
3. **样本期望值**：每个样本带 `expect`，Python 与 Go 的输出都对照它检查——防止两侧同错时误判一致。
4. **声明分歧（不计 fail）**：
   - 键序：Go `map` 序列化与 Python `dict` 的键序不同——比较器按键集合递归比较，天然吸收；
   - int/float 渲染：Python `1` 与 Go `1`/`1.0` 的 JSON 词法不同——比较器用精确有理数比较，天然吸收；
   - `load-refusal:go`（样本 23）：go 装载期具名拒 `end resultType:"nop"`（fail-closed，从严），
     Python 静默结束 result=null——用例钉 Python 行为并断言 go 拒载，声明不关闭期望检查；
   - `replay-from-start:go`（样本 28）：go worker 的 continue 载体从 flow 起点重入（已完成前序节点重放，
     AGENTS.md §worker 二期，Stage 1 中途入口落地后收敛）——比较前两侧轨迹投影到每节点首现，
     重放伪影的 attempts 不进比对；原始轨迹保留双现证据。
5. **失败形态判据**：常规样本任一引擎失败即 fail；`expectError` 样本（17-22/24/28）两侧必须都终态失败
   且错误文本含 `expectError.match`；`load-refusal:go` 样本 go 必须装载拒绝而 Python 必须跑通。
6. **重试归属**：轨迹事件带每节点尝试数（retries+1）——go 按尝试触发 OnNodeEnd 折叠合并，
   Python 经 runner 重试告警的 logging.Handler 计数；确定性错误烧重试预算即红（E1-E6 钉 #43 口径）。

## 已知限制

- 嵌入流程（loop/child/parallel 分支）必须在部署 JSON 里显式声明 `flow_id`：轨迹按 `flow_id` 分组，
  缺失时 Go 侧 ir 归一化会合成 id 而 Python 侧为空，两侧分组不可比。
- event-resume 形态（样本 27）的比较面是恢复后节点状态的确定性公共核：event_id（毫秒时间戳）、
  subscription_id/tenant（运行时指派）与挂起态形状extras（event_filter/is_async/subscription_timeout，
  两侧结构性不同）递归擦除；事件节点投影到恢复后末次出现（两侧都在挂起与恢复时各触发一次 node-end）。
- 轨迹只比较节点结束事件（id/类型/结果/错误有无/尝试数），不比较节点耗时。
- Rust 引擎不在本 harness 内：go↔rust 双跑走 `tools/diffcheck.py`，三列消费矩阵见 plaita 仓
  `docs/governance/conformance-matrix.md`（#317 §2/§3）。

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
