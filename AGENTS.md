# AGENTS.md — plaita-go

> plaita 的 Golang 引擎 + 运行时。
> 定位：**引擎 + worker 运行时**，不做全栈替代——console/keeper/派发控制面继续由 Python 侧承担，本仓按 queue/lease 协议接入。
> 负责人：jeffkit | 创建：2026-10-19

## 已拍板的三个决策（2026-10-19）

1. **表达式语言 = plaita 现行**（`$INPUT/$NODE/$PARENT/$GLOBAL/$ENV` + `$F.fn()` + `{% ... %}` 模板插值），不做 JS 方言。前身引擎的表达式语言与 plaita 同族（变量根完全一致），对齐成本低；缺口清单见下文「表达式对齐」。
2. **权威 IR = plaita 部署形态的 camelCase JSON**（`tests/fixture/assigment.json`、`examples/agent/flows/router.json` 即此形态）。@flow Python 源码只是前端写法，编译产物交给本引擎跑。snake_case 的 pydantic model_dump 是 Python 内部模型，不是契约。**禁止**在本仓再引入第三种表示；schema 变更必须同步 ir_validate 语义与往返 golden 测试（吸取 plaita「DSL 三表示往返无不变量 ~20 单」的教训）。
3. **来源已清理**：模块路径 `github.com/jeffkit/plaita-go`，git 历史不带入，腾讯内网依赖全删。

## 仓库结构

```
engine 侧（现 pkg/，内核自前身引擎清理）：
  pkg/workflow/       Workflow/Node 接口/registry/retry/timeout/event 回调 +
                      checkpoint.go（环安全/深度上限快照 + CheckpointStore）/suspend.go（Suspender/Resumer/Resume 入口）
  pkg/workflow/nodes/ 内置节点（assignment/branch/http/loop/parallel/redis/code/child/end/event/hitl/hitl_await…）
  pkg/expression/     plaitaexpr（$ 变量 + $F 函数，goval 求值）
  internal/interpreter/ goja（JS）/ yeagi（Go 源码）解释器，code 节点用
  pkg/model, pkg/parser, pkg/providers/  流程模型、解析、flow 来源（fs/memory）
  pkg/delegate, pkg/discovery/           http 节点的委托与寻址（polaris 已删，仅 domain/noop）
  pkg/worker/       runtime worker（2026-10-19 MVP + 二期 + M6）：Streams 取单/租约 Lua/状态写回/事件/取消/心跳 +
                    orphan watch/resume 载体语义(A47-F2)/dispatch_ledger/DLQ replay+守卫恢复/drain 三态/节点级重试预算 +
                    checkpointstore.go（`{ns}:execution:checkpoint:{id}`）/eventbus.go（订阅/事件桥/超时检查器）
  cmd/plaita-worker/  worker 入口（env 配置，影子键前缀可覆写；-dlq-replay 单向回灌）
  pkg/shadow/       M2 双跑对账 harness（Python sidecar / Go 节点轨迹+值比对 / 16 纯计算样本）
  cmd/shadow-compare/  对账 CLI（样本集或 -flow 单流程），一致率报告 docs/SHADOW_REPORT.md
  pkg/llm/          LLM provider 链（OpenAI/Anthropic 兼容）
  docs/WORKER_PROTOCOL.md  worker 协议权威文档（田野调查 + §9.4 M6 键名/语义，实现与影子对账的验收基准）
规划中：
  worker 三期      task age gate/租户配额/dedup/每步 checkpoint（M6 v1.5 只做挂起边界 checkpoint）
  server/       console BFF 契约只读子集（execution 查询/事件流）
  沙箱           Docker driver 优先 + AGS HTTP API（lease 与 worker 租约同族设计）
```

## IR 契约层（pkg/ir，已落地 2026-10-19）

`ir.Load(data) (*workflow.Workflow, error)`：plaita 部署 JSON → 引擎 Workflow。管线 = 归一化 → 结构校验 → 引擎反序列化。

**权威契约 = plaita codeflow 编译器 `compile_source` 的产物**（顶层 `flow_id/inputType/nodes/runtime`；camel 为主、`source_line` 等 snake 残留双键都收；console 旧导出的 `id`/`required`/`default`/`children` 也归一）。`model_dump(by_alias=True)` 是 snake_case，**不是**发布形态。

**归一化规则**（pkg/ir/normalize.go）：flow_id|flowId|id（flow_id 优先）；无 id 合成（根=`plaita-flow`，嵌入=`<node>.childFlow`/`<node>.branch.<name>`，引擎要求非空 id）；`global_context`→`context.global`；handler `continue_with`→`continue-with`、plaita 的 `code`/`message` 落 metadata 防丢失；timeout 毫秒数→ISO8601；Property 的 `type/title/description/default/children`→引擎拼写；switch 分支 `next` 空时回落 `name`（branch_name_as_target 语义）。

**校验子集**（pkg/ir/validate.go，对应 ir_validate DEFAULT_RULES）：唯一 start + 全图可达（R1）、next/分支引用存在（R3）、嵌入流程可达 end（R2）、`joinBranches ⊆ branches[].name`（R4 语义）。表达式级校验（R5）随表达式对齐工作流补。

**fail-closed 拒载清单**：decision（plaita-nodes）/ map/filter/find/reduce/while / if / case / calculate / mock / reference（需 provider）/ code 非 javascript / `sandbox_backend` / end `nop` / http 的 `allowedHosts`/`deniedHosts`/`blockPrivateNetworks`（SSRF 护栏未移植，靠它保护的 flow 不许静默降级）/ parallel 分支 input 非 dict。**M6 起 event / hitl_await 已转正**（挂起节点；仅有根流程挂起语义）。

**随 ir 对齐落地的引擎语义修正**（plaita 语义为基准）：
- loop：`$NODE.<loop>` = 末次子结果（前身引擎为 nil）
- switch：default 分支 0..1 个（原强制恰好 1）；无匹配且无 default → 执行期显式报错
- 条件算子补齐 plaita 集：`contains`（右∈左：串子串/切片元素/map 键）/`notContains`/`truthy`/`falsy`（Python 真值语义，value 不参与）
- parallel：分支无 `input` 时继承父 `$INPUT`（原为 nil）
- loop 无 `condition` = 恒真（零值 Condition 不再报"invalid expression type"）

**golden 测试**（pkg/ir/ir_test.go + testdata/）：assignment_flow.json 是 console 真实导出；loop_flow/parallel_flow/child_flow 由 **plaita 编译器实编**（`flow_from_source` → `compile_source`），在 Go 引擎执行并断言输出——「Python codeFlow 产 IR → Go 跑」已实证。改契约前先重跑这组。

## worker 二期（2026-10-19，分支 feat/worker-phase2）

六项全部落在 `pkg/worker/`，测试全 miniredis。与协议（docs/WORKER_PROTOCOL.md）的对应与**刻意偏差**：

- **orphan watch**（§3.4/§8.2，orphan.go）：看门狗 renew 失败 → 先发布一次进度心跳（fenced CAS；fence 键比租约多活 7 天，旧世代仍可写）→ 标记 lease_lost、中止当前步 → 观察集只发心跳，不续租、不再 cancel、不写终态。执行结束/fence 换代/行终态即摘除。目的：keeper §8.2 判据在「租约已丢但执行仍在跑」窗口不误杀。
- **resume 载体语义**（A47-F2/§9.2，worker.go handleResume）：决议载体（event/timeout/cancel）租约冲突窗口**无条件不 ack**；终态执行幂等 ack（不覆写终态）；cancel 标志或 `resume_type=cancel` 压过 suspended 短路（租约内终态化 cancelled）；suspended+continue/retry 不取租约、幂等 ack，但若有 `dispatch_pending` 凭据则做一次补偿派发（RPUSH `{prefix}:{subtype}:queue`，成功后清凭据，失败不 ack）。running 上的 event/timeout 载体按 §9.2/#258 归一化重投 continue 载体（先入队后 ack）。**M6 起 event/timeout 在 suspended 上真恢复**（检查点 → 节点 Resume → 继续步进）；陈旧载体/节点 resume 失败 = `ResumeGuardError`/`ResumeError`：执行保持 suspended、消息留 pending。
- **dispatch_ledger**（§6.3，ledger.go）：`{prefix}:dispatch:ledger` 只增 Stream，`payload` JSON + `schema_version`；`EnqueueWithLedger` 是入队+记账唯一入口（target 同源写入消息体与台账，结构上不可能分叉）；`Read`/`Parity` 读侧对坏行/故障不抛。TaskNotForThisWorker 完整交接（worker.go handleForeignTarget）：目标不在册/非 running → 剥 target 广播重投；在册 running → 按 `{ns}:execution:affinityhandover:{id}` 计数（INCR+7d），≤3 enqueue→ack，达限终态化 `AffinityHandoverExhausted` 并留 pending；计数不可用 fail-closed 留 pending；**全程不 DLQ**。
- **DLQ replay + 守卫恢复**（§2.4/§9.2，dlqreplay.go）：`ReplayDeadLetter` 幂等标记 `{prefix}:dlq:replay:{dlq_key}:{id}`（SET NX 7d，force 覆盖）、源信封 schema_version 继承（超版本入队前拒绝）、无 id 的 start 用 uuid5 确定性铸造、墓碑拒复活、审计 `{prefix}:dlq:replay:audit`、DLQ 原条不删。死信守卫：schema 拒收/终态/墓碑放行不克隆；租约在/读失败跳过；running 且节点预算耗尽（noderetry max ≥ max_deliveries 或 noderefetch ≥100）→ 终态化 `node_retry_budget_exhausted`；其余非终态+租约空 → 克隆源 payload（继承信封版本）后再放行死信。
- **drain 三态**（§9.3，drain.go）：`RequestDrain`（D1，幂等，注册表 draining 立即 flush 3 重试 + 有界定时器）→ `ForceStop`（D2，无条件停领取 + 取消在途 run + 打断重试退避）→ 信号只调 RequestDrain（D3，main.go 二次信号强退）。drain 后读到的消息不 dispatch/不 ack 留 PEL；D2 中止的步**不写终态**、留 pending（本 build 无硬中断，保住可续跑/可重投优先）。
- **节点级重试预算**（§9.1，retry.go）：`noderetry:{id}:{node}`（预算=max_deliveries）/`noderefetch:{id}`（100）/`nofail:{id}`（12）三层，INCR+7d；可重试判据 = 有失败节点 + 非超时/取消 + `workflow.IsRetryableError`；放行则指数退避 `min(30s, 1s·2^(n-1))`（0.2s 分片可被 D2 打断）→ **重投载体查盘转换（#91，对齐 `_requeue_body_for_retry`）**：盘上有行 → resume/continue 载体（params 透传、不带 version；墓碑命中 ack 丢弃、读盘失败原样 start），失败边界快照 checkpoint（worker 侧 `SnapshotRuntimeContext`，best-effort）→ enqueue→ack。预算耗尽/兜底上限 → 终态化 `node_retry_budget_exhausted`（code=-520）。成功推进清节点键；整跑 completed 再清连续失败计数。**残余偏差（引擎面）**：Go 引擎尚无中途入口（`Workflow.Resume` 仅限挂起节点），continue 载体推进 running 执行时从 flow 起点重入引擎（已完成前序节点重放）；租约/指纹门/历史续接/checkpoint 生命周期均为 resume 路径语义，引擎补中途入口后收敛。

## M6 checkpoint v1.5（2026-10-19，分支 feat/checkpoint-v15）

挂起恢复最小完备；键名与语义权威见 docs/WORKER_PROTOCOL.md §9.4。

- **快照**（pkg/workflow/checkpoint.go）：`SnapshotValue` 显式栈深物化（照 `state.py` `_snapshot_value`）——容器环 `<recursive: $['a'][0]>` 截断、深度上限 10000（`StateSnapshotError` 带路径）、非字符串 map 键具名拒绝、共享 DAG 不误判环（路径内 seen + ptr/length 身份）。`SnapshotRuntimeContext` 快照 `$INPUT/$NODE/$GLOBAL/$ENV/$ERROR/$CURRENT/$LAST_NODE/$PARENT`；`RestoreRuntimeContext` 重建（含 $PARENT 链）。`CheckpointStore` 接口 + `MemoryCheckpointStore`；Redis 实现在 pkg/worker/checkpointstore.go（`{ns}:execution:checkpoint:{id}`，无 TTL，终态删除）。
- **挂起/恢复**（pkg/workflow/suspend.go）：`Suspender`（IsSuspending）/`Resumer`（Resume(ctx,resumeType,data)）/`SuspendDescriptor`（订阅规格）；`Workflow.suspend` 先注册订阅再写快照，失败拒挂；`*SuspendError` 作为错误穿透嵌套调用（`AsSuspend` 识别）。`Workflow.Resume` 恢复快照 → pending/订阅绑定准入（event/timeout 必须匹配 `subscription_id`，event 另比对 `event_type`）→ 节点多态 resume → 从 next 继续；触发 OnNodeEnd（收口节点 timing）。continue/retry 对挂起节点是 guard 错误。
- **节点**：event（event_type 表达式/event_filter 点路径/subscription_timeout/三种恢复状态）与 hitl_await（hitl_client 复用；发消息拿 session_id 即 pending；hitl_reply/超时唤醒；resume 解嵌套 event_data）。ir supportedNodeTypes 转正 + translateEvent/translateHitlAwait（event_type 非空、timeout>0 在加载期拒绝）。
- **worker**：eventbus.go（订阅 Redis 持久化 + `{ns}:events:*` Pub/Sub 桥 + 超时索引/检查器 + 事件去重）；handleResume 在 suspended 上真恢复（检查点 → Resume → 续步），复用 executeAndFinalize 的取消轮询/租约看门狗/终态分类；恢复运行续接挂起行的 node_timings/timeline（seq 单调）。start 载体命中 suspended 执行 → 幂等 ack（不重放）。
- **修正**：终态 context 的 `$NODE` 改为节点结果表（原误写 `CurrentNodeContext`）；`$CURRENT` 单独保留。
- **限制**：仅根流程可挂起（child/loop/parallel 内挂起显式报错）；事件 Pub/Sub 无回扫器（订阅超时与执行状态双写仍保证至少一次唤醒）。**M6 之后（本轮 worker-tail）**：retry 载体的 start→resume/continue 转换已做（§9.1，见二期「节点级重试预算」条目）——失败边界 checkpoint 由 worker 侧快照，continue 载体对 running 执行真推进（引擎面残余：从 flow 起点重入，见该条）。

## 常用命令

```bash
# 本机 GOPROXY 可能指向内网 goproxy.woa.com（外网不可达），本仓统一走公共代理
GOPROXY=https://goproxy.cn,direct go build ./...
GOPROXY=https://goproxy.cn,direct go test ./...
make test   # 已封装代理
```

## 节点对齐矩阵（现状）

| plaita (Python) | plaita-go | 状态 |
|---|---|---|
| assignment | assignment | ✅ ir 对齐（golden 过） |
| decide（switch） | switch | ✅ ir 对齐 + 条件算子补齐（golden 过） |
| concurrent（parallel） | parallel | ✅ ir 对齐 + 分支输入继承（golden 过） |
| loop（基础 for-in） | loop | ✅ ir 对齐 + 末次结果语义（golden 过）；map/filter/find/reduce/while 拒载 |
| child（inline） | child | ✅ ir 对齐（golden 过）；reference 拒载 |
| http | http | ✅ ir 收录；SSRF 护栏字段拒载；golden 待补（执行面已有 nodes 测试） |
| redis | redis | ✅ ir 收录，字段同形 |
| start/end | start/end | ✅ ir 对齐；end nop 拒载 |
| code（python） | code（javascript） | ⚠️ 仅 javascript；Python 语义不可移植，Go 版 code 契约待定 |
| agentrun（plaita-nodes） | agentrun（pkg/workflow/nodes/agentrun.go + pkg/agentproc） | ✅ spawn 路径落地（2026-10-19）；workspace 沙箱分支拒载 |
| event（挂起） | event（pkg/workflow/nodes/event.go） | ✅ M6 落地（2026-10-19）：订阅/过滤/超时/event·timeout·cancel 恢复；仅根流程挂起 |
| hitl_await（挂起） | hitl_await（pkg/workflow/nodes/hitl_await.go） | ✅ M6 落地（2026-10-19）：发消息拿 session_id 即 pending，hitl_reply 事件/超时唤醒 |
| mock / calculate | — | 拒载（清单见上） |
| —（plaita-nodes 仓：decision/agent/gate/rate_limit/report/hitl 等） | — | 缺：decision 已在拒载清单带提示 |
| trpc / mdb（前身引擎特有） | 已删 | 不移植（公司内网设施） |

### plaita-nodes 22 节点全量盘点（2026-10-19 清点；M6 checkpoint v1.5 后 **22/22**）

| 节点 | Go 状态 | 说明 |
|---|---|---|
| agentrun | ✅ 已落地 | spawn 路径 + recursive-direct；**workspace 沙箱分支待做**（见下） |
| gate / rate_limit / report | ✅ 已落地（2026-10-19） | 语义逐行移植；gate 的 shlex/裁剪/重试、rate_limit 的 flock 计数、report 的 token 白名单全对齐；错误重试分类已落在错误值上，**引擎按类型分流的接线待做** |
| hitl | ✅ 已落地（2026-10-19，阻塞版） | hitl-server 协议（send/轮询/图片降级）对齐；deadline→timeout 不抛错；凭据 env/宿主装配 |
| hitl_await | ✅ 已落地（2026-10-19，M6 挂起版） | execute 发消息拿 session_id 即 pending；hitl_reply 事件/订阅超时唤醒；resume 解 cancel/timeout/replied |
| event | ✅ 已落地（2026-10-19，M6） | is_suspending；event_type 表达式/event_filter 点路径/subscription_timeout；Redis Pub/Sub 唤醒 |
| decision | ✅ 已落地（2026-10-19） | 封闭决策空间/confidence 门控/批量 index 对齐/provider 注册表（llm+jev+jevlike stub）；DecisionError=可重试口径 |
| llm | ✅ 已落地（2026-10-19） | pkg/llm 双协议客户端（OpenAI/Anthropic 兼容）+ 三级端点链（显式>providers.json>LLM_* env）；${ENV} 凭据插值零入库 |
| notify / feishu_webhook / dingtalk_webhook / slack_webhook / wecom_webhook / generic_webhook / email_send | ✅ 已落地（2026-10-19） | payload/加签（钉钉 HMAC）/业务码分类（feishu code/wecom·dingtalk errcode/slack ok）对齐；凭据经 `NotifyCredentialStore` 接口（宿主装配，缺省 fail-closed）；URL 打码按 _redact |
| api_request / parse_json | ✅ 已落地（2026-10-19） | api_request：手动重定向（跨源剥头/303 规则/10 hop）、凭据经 resolver 接口 fail-closed、URL 打码；parse_json：候选提取（末行胜出，绝不用首个 `{`——#43）、verdict 白名单、join_fields |
| capture | ✅ 已落地（2026-10-19） | 本地命令捕获（命令失败非节点失败）；与 gate 同族复用 env 白名单/进程组/裁剪 |
| sql_query | ❌ 未做（22 节点清点外补充：database.py，SQLAlchemy 多驱动） | Go 侧需驱动选型（新依赖）+ 凭据仓，按生产需要立项 |
| git_publish / github_comment / writefile | ❌ 拒载 | 文件系统/远端写操作，需沙箱/宿主工作区约定，**Wave 2-3** |

**已知接线缺口（Wave 2 遗留）**：①引擎 `workflow.executeNode` 对节点错误统一按 retryTimes 重试——notify/api_request 已把「确定性 vs 瞬态」分类落在错误值上，引擎按错误类型分流的接线待做；②webhook/邮件凭据仓（Python 侧 Fernet console 存储）Go 侧为宿主装配接口，worker 主线接。

### 沙箱支持（已拍板：要支持，2026-10-19）

agentrun `workspace=` 分支从「拒载」改为「计划支持」。**对齐生产契约**（plaita-nodes `_execute_sandboxed`）：workspace 名 → `.plaita/sandboxes.json` 注册表（fail-closed 不自动建）→ driver → 沙箱内 agentproc → env 白名单 + envfile(0600) 注入 → lease/ensure/释放（成功 kill / 失败 pause 分流）。**Go 实现取形**：Docker driver 优先（本地Mac/容器宿主，docker API），AGS 经 E2B 兼容 HTTP API 接（不移植 4300 行 driver，调用其服务面）。顺序在 worker 主线之后（沙箱 lease 与 worker 的执行租约同族，一起设计）。**M5 落地后的本轮收尾**：worker 在引擎执行 ctx 注入真实 execution_id（`sandbox.WithExecutionID`，ctx > `PLAITA_EXECUTION_ID` env > `$GLOBAL`），沙箱资源名/工作区身份 `{execution_id}:{ws_key}` = 真实执行 id，重投/续跑载体 re-attach 同一沙箱；此前 `(node_id, input)` 派生 `local-<hash>` 的兜底（`sandbox.DeriveExecutionID`）仅剩无 worker 装配的裸跑场景。

## 表达式对齐（已落地 2026-10-19，golden 45/45 绿）

变量根一致：`INPUT/NODE/PARENT/GLOBAL/ENV`（引擎另有 `ERROR/NULL/EMPTYSTRING`，保留）。

**已对齐**（语义权威 = `plaita/core/expression_parser.py` docstring + golden）：
- **`$F` 函数**：补齐 `eq/ne/gt/gte/lt/lte`（跨类型数值+容器深比较）、`ifelse`（急切）、`notContains`、`getDictValues`、`pop`（list/dict 双形态）；`and/or/not` 改 Python 真值语义（`or` 返回首个真值参数的**值**）；`concat`/`get`/`getDictValue` 变参+默认参可用（适配器支持 variadic，nil 实参→参数零值）。
- **`{% ... %}` 模板插值**：门面分发——`$` 开头=整串表达式（原类型返回）；含 `{% %}`=模板（内层须 `$` 前缀且可解析才插值，否则字面）；其余原样。nil 透传。
- **未知函数 = 报错**（不采 scoped registry 的 `"undefined"` 哨兵）：plaita 默认注册表 NameError + did-you-mean，部署 flow 用默认注册表，fail-closed 同仓规。goval 自带 `no such function` panic→error。
- **`$NODE.raw.<node>`** 免评估旁路（#130，含同名 `raw` 节点冲突规则）；**缺键语义**：缺根=error（KeyError 口径），已存在对象缺字段=nil，索引 miss=error，负索引 `[-1]` 支持。
- **语句变量预解析架构**（plaitaexpr/evaluator.go `preResolveStatementVariables`）：`$F` 语句里的内建根变量先经 plaita 遍历求值、注入合成标识符，goval 只做函数调用——方括号 `X[0]`/`[-1]`（parser 归一为 `.#N`）、raw 修饰符、缺键语义在语句里与纯变量路径完全一致；`$PARENT.` 链整段吸收走 goval 导航。
- **parser**：单引号字面量→双引号转换（goval 无单引号 token）、尾逗号剔除、字面量掩码防误改。

**agentproc 节点（spawn 路径，2026-10-19 落地）**：选型拍板 = 只实现协议 host 侧（pkg/agentproc：spawn profile command → stdin 一行 turn → stdout NDJSON 事件组装），CLI 适配留在 agentproc hub bridge（与 Python 运行时**共用同一份 bridge.py**——影子对账要求两 runtime 适配器逐字节一致，diff 才能归因到引擎）；in-process executor 是 per-SDK 注册表，Go 不做（将来需要时按 spec four-case 规则插到 spawn 前）。注册表 = **同一份 agents.json/providers.json**（~/.plaita，`PLAITA_HOME` 可覆写）：provider 翻译器实现 recursive/claude 两个（与 Python `_ENV_TRANSLATORS` 同键），其他 executor 是锁定型 CLI 不接受 provider；**agents.json 条目必须带 `command`/`args`**（Go 走 spawn，缺了 fail-closed），如 `python3 <hub>/<executor>/bridge.py`。runner 语义 = spec v0.4：error 事件即失败（exit 0 也算）、streaming 时 body=partials 拼接（result.text 不重复追加）、非流式 body=result.text、无 result 无 partial 退 0 = 成功空 body、畸形行忽略、session 首个非空胜出（wire 非法丢弃）、超时 killpg 杀整组。输出 dict 对齐 Python 节点：`{text, cli, model, session_id, usage[, details]}`。`workspace` 沙箱分支已随 M5 Docker driver 落地（见下节）。

**golden 跑批**：`pkg/expression/golden_test.go` + `testdata/expression_golden.json`（45 条，由 `scripts/export_golden.py` 从 `plaita/tests/unit/test_expression_golden.py` 程序化导出，回灌校验后落盘）。**改表达式任何行为前先跑它**；Python 侧语义升级后重新导出：`cd ~/projects/infra4agent/plaita && python3 ~/projects/plaita-go/scripts/export_golden.py`。3 条依赖自定义注册表的用例不可导出（schema 无 registry 字段）；2 条 parse_function API 用例 runner 跳过（门面对应的是 evaluate 路径）。

## 已知问题

- ~~error_node 上游遗留红~~ **已修平（2026-10-19 audit wave，#12/#13 http 契约修复顺带解决；撤代理后 1002=期望）**。
- `test_execute_workflow_with_http_request_info`：live-network 测试（断言真实出网失败），结果取决于出口网络状态——CI 以 -skip 排除。
- `Test_today_eval`/`Test_now_eval`：gomonkey time.Now 补丁抖动（测的是补丁不是代码），CI -skip。
- `go vet`：pkg/providers/memory、filesystem 有 lock 值拷贝告警（上游质量噪音）。

## 约定

- 引擎逻辑活在 flow 图里（jeffkit 红线）；不接受「宿主代码 + flow 皮」。
- 所有降级（错误/超时策略、重试）必须走事件流落盘，禁止静默。
- go.mod 禁止引入内网 module（git.woa.com / git.code.oa.com）。
