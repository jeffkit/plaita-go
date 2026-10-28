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
  internal/interpreter/ goja（JS）解释器，code 节点用（yaegi/Go 后端已删，A62-04）
  pkg/model, pkg/parser, pkg/providers/  流程模型、解析、flow 来源（fs/memory）
  pkg/delegate, pkg/discovery/           http 节点的委托与寻址（polaris 已删，仅 domain/noop）
  pkg/worker/       runtime worker（2026-10-19 MVP + 二期 + M6）：Streams 取单/租约 Lua/状态写回/事件/取消/心跳 +
                    orphan watch/resume 载体语义(A47-F2)/dispatch_ledger/DLQ replay+守卫恢复/drain 三态/节点级重试预算 +
                    checkpointstore.go（`{ns}:execution:checkpoint:{id}`）/eventbus.go（订阅/事件桥/超时检查器）
  cmd/plaita-worker/  worker 入口（env 配置，影子键前缀可覆写；-dlq-replay 单向回灌）
  cmd/plaita/         inline 直跑入口（run/validate；**inline 台阶闸** stage=entry——挂起族具名拒
                      `error_type=suspend_not_executable` + 逐节点点名〔nodes.FlowSuspendingNodes，
                      含 child/reference/collection/parallel 嵌套〕；内联执行预算
                      `PLAITA_EXECUTION_DEADLINE_SECONDS` 缺省 1800s〔与 worker 同 env 同缺省〕、
                      到期 `execution_deadline_exceeded` 具名终态。与 Python Flow.run /
                      RS plaita-cli 同轴（2026-10-28）；托管面仍在 plaita-worker，本入口不触 Redis）
  pkg/shadow/       M2/#317 双跑对账 harness（Python sidecar / Go 节点轨迹+值+尝试数比对 / 28 例四维度：纯计算
                    01-16、错误分类×重试归属 17-22、结果形状 23-26、挂起恢复 27-28；driver 三态 fresh/event-resume/
                    continue-after-error；声明分叉 load-refusal:go、replay-from-start:go；http 用例 hermetic 测试期守卫）
  cmd/shadow-compare/  对账 CLI（样本集或 -flow 单流程），一致率报告 docs/SHADOW_REPORT.md
  pkg/wake/         wake 值班室（2026-10-28 起）：挂起族配套服务的 Go 原生消费者——语义权威
                    Python DelayService（list→ZSET 两段式搬运〔先 ZADD 后 LREM，member 幂等〕/
                    到期轮询/触发确认才出队 at-least-once/发布失败留排程 + attempts hash
                    指数退避〔1s·2^(n-1) cap 30s〕超限转 `{queue}:dead` 死信〔rpush 先于 zrem
                    宁重不丢〕+ service_error 广播）；resume 事件复用 worker.EventPublisher
                    双写（SETEX event:events + ZADD event:types + PUBLISH {ns}:events:{type}，
                    与 Python 服务/Go worker 字节兼容，混编舰队共用一份 Redis）；注册键
                    `{prefix}:registry:delay:{instance}`（TTL 30s/心跳 10s/stopped 墓碑 5s）
                    = 集群档受理点名判据数据源。**已实现 delay / approval / redis_queue**
                    （2026-10-28，共享 listSource 消费骨架=Python WakeConsumeMixin 同构：
                    LRANGE 批读/确认才 LREM/触发失败与停机留排程/坏条目丢弃；
                    approval=记录 `{prefix}:approval:pending:{id}` 7d TTL + SET NX 决策锁
                    〔compare-and-del 释放〕+ any/all/majority 三策略 + 终局删记录，
                    决策入口=导出 API SubmitApprovalDecision，记录形状与 Python 字节
                    兼容〔混编下 Python console 决策可直写〕；redis_queue=任务派发桥
                    + 用户 Redis 三类监听〔list BLPOP/stream XREAD $/pubsub〕，
                    task_complete 键 + 处理后空闲窗〔缺省 10s〕收尾，Python 的
                    订阅存储提前收尾检查未实现〔无 correlation 索引，行为等价仅慢〕；
                    发布失败决策不持久化——持久化会把重试挡在「已经审批过」上，
                    Python 语义逐字）。http_callback=注册记录 + **内置 HTTP ingress**
                    （PLAITA_HTTP_CALLBACK_ADDR 缺省 :8080=节点 full_url 指向；
                    Python 侧 handle_callback_request 只是进程内 API、服务端无路由
                    ——纯 Go 部署的前门由 plaita-wake 提供）：POST 注册路径 →
                    只读记录 → 凭据校验（token/bearer、basic、HMAC-SHA256 签名
                    〔payload=`{unix秒}.{CanonicalJSON}`，复用 worker 规范编码器；
                    X-Plaita-* 头；容差 300s〕先于认领 #113）→ Lua 两阶段认领
                    （GET+SET NX PX 30s；发布成功删记录+占位〔先记录后备位，
                    崩溃不双 resume〕/失败只释占位可重试 #188）→ http_callback
                    resume 事件；Q5 统一失败响应「回调路径未注册」防 path 存在性
                    oracle（细节只进日志）。**余量：kafka_queue（需 broker 客户端）**
  cmd/plaita-wake/  wake 进程入口（`-sources delay,approval,redis_queue`；env 与 worker 同族：PLAITA_KEY_PREFIX/
                    PLAITA_REDIS_URL/PLAITA_DELAY_QUEUE〔空值具名拒，B2R31-03〕；D3 停机：
                    首信号优雅停〔在途触发落地或留排程〕，二信号立即退）
  pkg/workflow/span.go span 模型（plaita#344 对应物，2026-10-28）：NodeSpan
                      {span_id,parent_span_id,frame_path,iteration,node_id}——节点执行
                      实例身份，executeNode 在 retry 循环前生成盖章进 ctx（监听器经
                      SpanFromContext 读；Go 位置签名无 kwarg，ctx 戳即 span= 的对位），
                      一次实例一个 span 重试共享；帧态=runtime.Context 新增
                      FramePath/FrameKey（loop/while/collection/parallel/child/reference
                      六帧入口延长+限定，NewExecutionRuntimeContext 随 FrameKind 同途
                      继承）；resume 收尾腿无 start 可配对维持无 span 回退。worker 侧
                      beginNode/endNode 配 span：timeline 事件带 span 身份
                      （frame_path 渲染列表），node_timings 加 occurrences 逐轮明细
                      （cap 200 截断可观测，聚合语义不变）
  pkg/llm/          LLM provider 链（OpenAI/Anthropic 兼容）
  docs/WORKER_PROTOCOL.md  worker 协议权威文档（田野调查 + §9.4 M6 键名/语义，实现与影子对账的验收基准）
规划中：
  worker 三期      ~~task age gate/租户配额/dedup~~ 已落地（phase3_*：agegate.go/quota.go +
                    metrics.go 具名指标接口）；余：每步 checkpoint（M6 v1.5 只做挂起边界 checkpoint）
  沙箱           Docker driver 优先 + AGS HTTP API（lease 与 worker 租约同族设计）
已落位更正（2026-10-28）：console BFF 只读子集在 `pkg/server/`（execution 三条只读路由，
形状对齐 plaita-console，非「规划中」）。
```

## IR 契约层（pkg/ir，已落地 2026-10-19）

`ir.Load(data) (*workflow.Workflow, error)`：plaita 部署 JSON → 引擎 Workflow。管线 = 归一化 → 结构校验 → 引擎反序列化。

**权威契约 = plaita codeflow 编译器 `compile_source` 的产物**（顶层 `flow_id/inputType/nodes/runtime`；camel 为主、`source_line` 等 snake 残留双键都收；console 旧导出的 `id`/`required`/`default`/`children` 也归一）。`model_dump(by_alias=True)` 是 snake_case，**不是**发布形态。

**归一化规则**（pkg/ir/normalize.go）：flow_id|flowId|id（flow_id 优先）；无 id 合成（根=`plaita-flow`，嵌入=`<node>.childFlow`/`<node>.branch.<name>`，引擎要求非空 id）；`global_context`→`context.global`；handler `continue_with`→`continue-with`、plaita 的 `code`/`message` 落 metadata 防丢失；timeout 毫秒数→ISO8601；Property 的 `type/title/description/default/children`→引擎拼写；switch 分支 `next` 空时回落 `name`（branch_name_as_target 语义）。

**校验子集**（pkg/ir/validate.go，对应 ir_validate DEFAULT_RULES）：唯一 start + 全图可达（R1）、next/分支引用存在（R3）、嵌入流程可达 end（R2）、`joinBranches ⊆ branches[].name`（R4 语义）。表达式级校验（R5）随表达式对齐工作流补。

**fail-closed 拒载清单**：calculate / ignore（**r78 结论：两者都不是 plaita 注册节点类型**——`Flow.model_validate` 对它们报 `unRecognized node type`，calculate.py 模块存在但未注册是遗留物、ignore 只是 Node 基类的 node_type 占位 ClassVar；Go 保持拒载对齐的是运行时行为，错误信息保留同款子串逐字对拍）/ code 非 javascript / end `nop` / http 的 `allowedHosts`/`deniedHosts`/`blockPrivateNetworks`（SSRF 护栏未移植，靠它保护的 flow 不许静默降级）/ parallel 分支 input 非 dict / reference 携带 inline childFlow（plaita 运行时把它丢掉并在执行期炸，见「节点对齐 phase 2」节）。**M6 起 event / hitl_await 已转正**。**#80 phase 1（2026-10-22）起 map / filter / find / reduce / while 已转正**。**#80 phase 2（2026-10-22，r78）起 case / mock / reference 已转正**（见「节点对齐 phase 2」节）。**#80 phase 3（2026-10-22，r79）起 delay / approval / redis_queue / kafka_queue / http_callback 已转正**（见「节点对齐 phase 3」节）。**#84（2026-10-22，r84）起 sql_query 已转正**（见「sql_query 节点」节；node-parity 例外表同步摘条，python-only 差集清零）。

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
- **节点级重试预算**（§9.1，retry.go）：`noderetry:{id}:{node}`（预算=max_deliveries）/`noderefetch:{id}`（100）/`nofail:{id}`（12）三层，INCR+7d；可重试判据 = 有失败节点 + 非超时/取消 + `workflow.IsRetryableError`；放行则指数退避 `min(30s, 1s·2^(n-1))`（0.2s 分片可被 D2 打断）→ **重投载体查盘转换（#91，对齐 `_requeue_body_for_retry`）**：盘上有行 → resume/continue 载体（params 透传、不带 version；墓碑命中 ack 丢弃、读盘失败原样 start），失败边界快照 checkpoint（worker 侧 `SnapshotRuntimeContext`，best-effort）→ enqueue→ack。预算耗尽/兜底上限 → 终态化 `node_retry_budget_exhausted`（code=-520）。成功推进清节点键；整跑 completed 再清连续失败计数。**残余偏差（引擎面·已声明行为分叉类 e，范围按 #32 中途入口落地后实况收窄）**：引擎中途入口已落（`Workflow.ResumeFrom`，#32；`Workflow.Resume` 仍仅限挂起节点），worker continue 路径已接线——有可用 per-step checkpoint（或 legacy continue 快照）时经 `ResumeFrom` 中途续跑、已完成前序节点**不重放**（`pkg/worker/worker.go` `continueRunning`，入口被守卫拒绝时经 `midFlowEntryRefused` 落回起点）；仅当无可用 checkpoint 时回退 flow 起点重入（§9.1 fresh-run parity 回退臂：`continue_fallback_start` 具名时间线 + timeline 留痕）——该臂下对 http 回调/审批/agentrun 等副作用节点即重复执行；Python 自 checkpoint/last_node 步进不重放=权威方向。矩阵格=plaita `docs/governance/conformance-matrix.md` go 列 R2 行 `red-declared`（声明 `replay-from-start:go`，对拍样本 28 每夜红=回退臂分叉仍存证据，翻绿=收敛自动验收）；Go 已解冻为 evolving companion（ADR-002，2026-10-25）且 **恢复生产可选 runtime**（ADR-002 修订 2026-10-27：runtime=go 的发布/受理/派发链为生产面，go_worker 集群模板就位；E1 反向修正，VALID_RUNTIMES 三值为准绳），回退臂保持具名可观测降级现状、对齐 Python 零重放语义为候选跟进项（ADR-002 §3.5，未立项）；租约/指纹门/历史续接/checkpoint 生命周期均为 resume 路径语义照常。

## M6 checkpoint v1.5（2026-10-19，分支 feat/checkpoint-v15）

挂起恢复最小完备；键名与语义权威见 docs/WORKER_PROTOCOL.md §9.4。

- **快照**（pkg/workflow/checkpoint.go）：`SnapshotValue` 显式栈深物化（照 `state.py` `_snapshot_value`）——容器环 `<recursive: $['a'][0]>` 截断、深度上限 10000（`StateSnapshotError` 带路径）、非字符串 map 键具名拒绝、共享 DAG 不误判环（路径内 seen + ptr/length 身份）。`SnapshotRuntimeContext` 快照 `$INPUT/$NODE/$GLOBAL/$ENV/$ERROR/$CURRENT/$LAST_NODE/$PARENT`；`RestoreRuntimeContext` 重建（含 $PARENT 链）。`CheckpointStore` 接口 + `MemoryCheckpointStore`；Redis 实现在 pkg/worker/checkpointstore.go（`{ns}:execution:checkpoint:{id}`，无 TTL，终态删除）。
- **挂起/恢复**（pkg/workflow/suspend.go）：`Suspender`（IsSuspending）/`Resumer`（Resume(ctx,resumeType,data)）/`SuspendDescriptor`（订阅规格）；`Workflow.suspend` 先注册订阅再写快照，失败拒挂；`*SuspendError` 作为错误穿透嵌套调用（`AsSuspend` 识别）。`Workflow.Resume` 恢复快照 → pending/订阅绑定准入（event/timeout 必须匹配 `subscription_id`，event 另比对 `event_type`）→ 节点多态 resume → 从 next 继续；触发 OnNodeEnd（收口节点 timing）。continue/retry 对挂起节点是 guard 错误。
- **嵌套挂起 v1**（NS-M2，#88，rs#16 语义契约）：child/reference 调用链（任意深度）内的挂起穿透到根——`SuspendError` 携带活帧调用栈（`CallStack []CallFrame`，NodeID+父 Workflow+父 rc；`Flow`/`Ctx` 为最内帧）；作用域闸=传播转换点检查**调用方帧 kind**（runtime.Context.FrameKind：Root/ChildCall/ReferenceCall 放行，LoopIteration/ParallelBranch 具名拒 `SuspendRejectedError`——不可重试、不可被 errorHandler/timeoutHandler 吞，越界载体拒绝时注销已注册订阅防孤儿；suspend() 的帧链行走仅是注册前 advisory，reference 的 ambient 父链会遮蔽它）；挂起不进任何 retry/timeout/errorHandler 策略链（executeNode 双拦截臂 + IsRetryableError 兜底臂）；parallel 分支派发前静态闸 `flowContainsSuspending`（后台分支的动态拒无法同步上抛）；`Workflow.ResumeSuspension` 沿栈恢复：决策步 → 内层续跑 → 返回步（$NODE[child]=返回值 + outputType 校验 + triggerNodeEnd + 每帧新鲜预算 flow.Timeout）→ 根完成；再挂起=栈拼接重入。checkpoint 保持扁平单帧（worker 层序列化调用栈路径是后续单）。
- **节点**：event（event_type 表达式/event_filter 点路径/subscription_timeout/三种恢复状态）与 hitl_await（hitl_client 复用；发消息拿 session_id 即 pending；hitl_reply/超时唤醒；resume 解嵌套 event_data）。ir supportedNodeTypes 转正 + translateEvent/translateHitlAwait（event_type 非空、timeout>0 在加载期拒绝）。
- **worker**：eventbus.go（订阅 Redis 持久化 + `{ns}:events:*` Pub/Sub 桥 + 超时索引/检查器 + 事件去重）；handleResume 在 suspended 上真恢复（检查点 → Resume → 续步），复用 executeAndFinalize 的取消轮询/租约看门狗/终态分类；恢复运行续接挂起行的 node_timings/timeline（seq 单调）。start 载体命中 suspended 执行 → 幂等 ack（不重放）。
- **修正**：终态 context 的 `$NODE` 改为节点结果表（原误写 `CurrentNodeContext`）；`$CURRENT` 单独保留。
- **限制**：挂起 v1 只支持 child/reference 调用链（loop/map/filter/find/reduce/while 迭代帧与 parallel 分支帧上的挂起仍具名拒，v2 需迭代状态快照）；事件 Pub/Sub 无回扫器（订阅超时与执行状态双写仍保证至少一次唤醒）。**M6 之后（本轮 worker-tail）**：retry 载体的 start→resume/continue 转换已做（§9.1，见二期「节点级重试预算」条目）——失败边界 checkpoint 由 worker 侧快照，continue 载体对 running 执行真推进（引擎面残余：从 flow 起点重入，见该条）。

## Face 2 安全对齐闸（2026-10-20，分支 fix/security-gates；#34 余量续于 fix/gate-capture-tenant）

A1 r62/r63 情报（A62-02/04/05、issue #34）的落地件。Python 权威语义：`plaita-nodes/_tenant_guard.py`、`agent_run._host_run_denied`、`plaita/node/code.py` 的 A42-06、`plaita/subprocess_env.py`。

- **租户闸全族（#34）**：四条宿主子进程路径对非 default 租户 spawn 前 fail-closed（命令零执行机会、`workflow.NonRetryable` 不烧重试预算；default / 无租户上下文保持历史语义）——① agentrun 宿主直跑 `ErrHostRunTenantDenied`（A62-02）；② gate / ③ capture / ④ code-python sidecar 共用 `pkg/workflow/nodes/tenant_guard.go` 的 `refuseNonDefaultTenant`（`ErrTenantIsolationUnavailable`，消息与日志带 marker `PLAITA_SUBPROCESS_TENANT_ISOLATION_UNAVAILABLE`，同 plaita A42-06 / pn#18）。gate/capture 的 dry-run 在拒绝点之前短路（无命令执行，同 Python）；code-python 的拒绝点落在 `runPythonSubprocessBudget`（唯一 spawn 入口，绕过节点直调也被拦）。租户由 worker 注入 `runtime.WithTenantID`（ctx 值，child/loop/parallel 经 context 链继承）。宿主命令/代码与 worker 同用户、共享宿主文件系统与租户级解密密钥——env 白名单只挡宿主凭据外泄，不构成租户凭据隔离边界；租户级执行需容器（docker，不挂载宿主凭据）。
- **agentrun 宿主直跑（A62-02）**：①`pkg/agentproc/runner.go` 的 `composeEnv` 不再继承全量 `os.Environ()`，改为白名单重建（`HostEnvAllowlist` = PATH/HOME/TMPDIR/LANG/LC_ALL/LC_CTYPE/PYTHONIOENCODING，同 plaita `build_subprocess_env`）+ 部署钩子 `HostEnvExtra`（同 `SUBPROCESS_ENV_EXTRA`；spec env 覆盖）——agent 进程只看到显式声明的 env，worker 的 `PLAITA_CREDENTIALS_KEY`/provider key 不随子进程走。**代理等部署变量必须经 `HostEnvExtra` 显式加**。②`agentrun` 宿主路径的租户闸见上；`workspace` 沙箱分支不受影响（容器边界 + 0600 envfile）。bare engine / 单租户部署（无租户上下文）保持放行，同 plaita 无租户层语义。`PLAITA_ALLOW_HOST_AGENT_RUN` 式 opt-out 未移植（默认拒绝，单机信任部署需另立单）。
- **code 的 Go/yaegi 后端（A62-04）**：`internal/interpreter/yeagi.go` + `GoArgs`/`GO_*` 常量删除，yaegi 依赖移出 go.mod；`code` 节点构造期（`Validate`）与执行期（`Execute`，防直接构造绕过）均以 `ErrInProcessGoUnsupported` 拒绝 `language:"go"`。支持面 = javascript（goja，进程内，保留原语义）+ python（subprocess sidecar）。IR `translateCode` 原本就拒 Go，worker 路径不变。
- **providers.json 明文（A62-05）**：`agentproc.readJSONFile` 对 agents.json / providers.json 做 fail-loud 权限检查——mode 0600 或更严，组/其他任何权限位 → 具名错误（含 `chmod 600 <path>`），无告警继续路径；Windows 跳过 POSIX 位检查（按 NTFS ACL）。明文 key 注入子进程的通道仍在（协议需要 provider env），加密/租户凭据路由另单；check-then-read 的 TOCTOU 不构成安全边界（本地运营者配置，威胁模型是误配泄露而非同机竞态）。
- **宿主写面 + 平台 Redis 租户闸（#36/#37，2026-10-21）**：flow 控制路径的宿主写面五节点（writefile / git_publish / report / rate_limit / github_comment）在 Execute 入口接入共享租户闸（`refuseNonDefaultTenantWriteFace`，经泛化的共享拒绝核心 `refuseNonDefaultTenantReasoned`，与 #34 三门同族：`ErrTenantIsolationUnavailable` + marker + NonRetryable；#34 三门文案保持不变）。位置逐节点定在**首个宿主副作用之前**：github_comment 的闸在 dry-run 短路**之前**（其 dry-run 仍写宿主 artifact 草稿），宿主 GH_TOKEN/GITHUB_TOKEN 读取面被同一道闸覆盖；git_publish 的闸在 dry-run 短路**之后**（dry-run 无任何 git 操作，同 gate/capture 家族语义）；report 的 read action 同闸（读其他租户 report 通道与写同权）。redis 节点（#37）双层闸，在任何 target 解析/拨号之前：非 default 租户必须有 worker 级 target allowlist（env `PLAITA_REDIS_TARGET_ALLOWLIST` 或部署钩子 `SetRedisTargetAllowlist`，逗号/空白分隔 redis URL），未配置一律拒；配置后按 host:port/db 身份匹配（大小写不敏感，userinfo 凭据不参与），target 不在册即拒——注入 client 的 DI 旁路不绕过闸。default/无租户全部保持历史语义（正控各门有测）。**构造期不拒（IR 层面结论）**：租户是执行期 ctx 属性（worker 按 message 注入 `runtime.WithTenantID`），`Validate`/`ir.Load` 无租户语境，且同一份 flow JSON 跨租户复用、节点实例按执行构造——加载期拒绝会错伤其他租户与单租户部署，也会把策略决定提前到无策略输入的位置；故拒绝点固定在各节点 Execute 首个宿主副作用之前，构造期保持租户无关（dry-run 准入等运行时属性同理）。

## 执行级 deadline 与构造期预算 caps（2026-10-21，分支 fix/exec-deadline；#35 = plaita#267 Go 同型）

plaita 侧权威语义：`plaita/core/deadline.py` + `core/strategies.py`（#267，commit `1e57801`）+ r47 retryTimes cap。协议文档 §9.5 是完整口径。

- **worker 级执行预算**（`pkg/worker/config.go` + `executeAndFinalize`）：env `PLAITA_EXECUTION_DEADLINE_SECONDS` 或部署钩子 `Config.ExecutionDeadline`，**默认 1800s**；env 非法/≤0 回退默认并 WARN（绝不静默无限额），显式关闭只能走钩子（负值）。**口径 = 一次引擎调用的占用跨度**（每载体独立计，挂起等待不占预算——对齐 plaita distributed「每步独立计」），不是执行全寿命。到期协作式取消（runCtx cancel；节点边界 fail-fast + go-retry ctx 优先，无 kill -9），终态分类插在 cancel/drain/suspend 之后、引擎错误之前：即使 handler 把 ctx 错误吞成 continue 也照样落账（对齐 Python「ExecutionDeadlineError 不可被 handler 吞」）。终态 = error + `error_type=execution_deadline_exceeded`（code -500）+ details 带 deadline/occupied 秒数 + timeline 事件 + metric `exec_deadline_total`；不重试、不进 DLQ，消费者立即释放（负控断言队尾任务恢复时延）。
- **构造期 caps**（plaita r47/#267 同值，IR 装载期 + 引擎模型双层）：`errorHandler.retryTimes`（节点级+flow 级）≤ 10（`pkg/workflow/budget.go` `MaxRetryTimes`；引擎 `Validate` 顺带修掉了存量 `Min(0)` 对 uint 的 ozzo 阈值崩溃——非零值必炸的潜伏 bug）；节点 `timeout` ≤ 24h（`MaxNodeTimeout`）；flow 级 timeout **不 cap**（显式运营预算，运行时被 worker 预算取严封顶）。HTTP `requestTimeout` 改**整个请求共享总预算**（`nodes/http.go` reqDeadline，跳前检查、耗尽具名 `requestTimeout budget exhausted` → 1002 可重试族，同 plaita `_RequestBudgetExceeded`），delegate 路径同闸。
- **与 r63「构造期不拒」结论不冲突**：租户闸拒的是执行期 ctx 属性；本轮 caps 拒的是 flow JSON 静态形状（retryTimes/timeout 数字），构造期即有完整输入，且 IR 是部署唯一入口。
- **负控（先红后绿）**：`exec_deadline_test.go` 自循环 switch flow——pre-fix 基线 30s 仍独占（红，实测）；post-fix 预算内具名终态 + 队尾任务恢复 + 无 DLQ 污染；钩子关闸行为守恒。`budget_caps_test.go` retryTimes=11/timeout=24h+1ms 拒、=10/=24h 收。`http_budget_test.go` 两慢跳挤爆总预算 + 1ns 预算具名拒绝（pre-fix 双红，实测）。

## 全节点 ctx 取消反弹矩阵（2026-10-21，分支 fix/ctx-rebound；#48 = #35 的成立前提）

#35 的执行预算是**协作式**（到点只 cancelRun，`execution_deadline_exceeded` 分类在 Execute 返回后才可达），其隐含前提是每个节点都会反弹 ctx。#48 修补的两个 ctx 盲区击穿了它：**js code 的 goja vm 收了 ctx 从未引用**（死循环永久占死唯一消费者）、**sleep 裸 `time.Sleep` 且 Duration 无 cap**（`sleep 87600h` 一个配置值占死消费者）——Execute 永不返回 → 具名终态/exec_deadline_total 全无，watchdog 随 runCtx 静默退出（orphan 注册只在失租分支，此路径无）；叠 #47 转换 → 租约过期被别的 worker 取走 → start 载体撞 running 行路由 continue 转换 → **双引擎并发跑同一执行**（fence 只挡状态写，这类盲区节点恰是「写不进状态但副作用照跑」的反例面）。修复：goja 接 `vm.Interrupt`（ctx.Done 触发，`*goja.InterruptedError` 按 ctx.Err() 具名归类——`context.Canceled` 刻意**不**算 `contextTimeoutError`（那只认 DeadlineExceeded），预算取消不会被 continue-with timeout handler 吞掉；节点级 timeout（WithTimeout→DeadlineExceeded）照常走 timeout 策略）；sleep 改 `select{ctx.Done(), timer.C}` + 具名取消。行为守恒由既有三条 flow-timeout 策略测试（abort/continue/continue-with，走 sleep 节点）背书。

**矩阵**（「预算到期后 Execute 最迟多久返回」上界；35 个注册类型全量过目）：

| 节点/层 | 阻塞点 | ctx 反弹路径 | 到期返回上界 |
|---|---|---|---|
| sleep | 计时等待 | `select{ctx.Done(), timer}`（#48 修复；**IR 拒载**「not part of the plaita dialect」，仅裸引擎嵌入可达） | 即时 |
| code(js) | goja vm | `vm.Interrupt`（#48 修复） | goja 下个中断点（ms 级） |
| code(python) | 子进程 | `CommandContext` + Setpgid + Cancel=killpg + WaitDelay | 即时（杀组） |
| gate / capture / git_publish | 子进程 | `exec.CommandContext` | 即时 |
| agentrun(宿主) | 子进程 stdout 读取 | agentproc watchdog（ctx→killGroup）；spawn→write turn 窗口只写一行、管道缓冲可吞，不阻塞 | 即时 |
| http / api_request / notify 族 / generic_webhook / github_comment / llm / decision | HTTP | `NewRequestWithContext`（http 另有共享 reqDeadline 预算，r64） | 连接/读超时族 |
| email_send | SMTP 会话 | 拨号 `DialContext(ctx)`；会话整体 20s deadline（`conn.SetDeadline`，对齐 Python timeout=20——非 ctx 即时但有界） | ≤20s |
| redis | go-redis | 全 API ctx | 连接/读超时族 |
| hitl(阻塞版) | 轮询间隔 | `select{ctx.Done(), time.After}`；轮询 HTTP reqCtx | 即时 |
| event / hitl_await | 挂起驻留 | Execute 立即返回 SuspendError（不占 Execute 跨度） | 即时 |
| loop / child / parallel | 子执行 | ctx 贯传（parallel 分支 CloneContext），每子节点 executeNode 入口 ctx fail-fast | 子节点族上界 |
| assignment / switch / if / parse_json / end / start / reference | 无 | 纯计算（reference 另被 IR 拒载） | 0 |

引擎面兜底：`executeNode` 每次 attempt 入口 `ctx.Err()` fail-fast；可重试错误经 go-retry 循环顶 + 退避 select 的 ctx 优先立即放弃（常数退避 10ms 内）。worker 消费环内部等待（drain force-stop 轮询 20ms、queue/eventbus 重连退避）非 Execute 面，全部 select/`time.After` 有界。**残余豁免**：parallel 后台分支（JoinBranches 空 = fire-and-forget）在 deadline 后仍会跑到其叶子节点的反弹点为止——叶子全 ctx-aware，故仍有界；无孤儿面（节点内不持锁）。

**Python 侧硬杀语义 parity（A2 建议项，结论入档）**：权威 = `plaita/core/runner.py` `_run_with_timeout`——sync 节点在裸 daemon 线程跑、`wait_for(shield(fut))` 到点弃置 future + `cancel_event.set()`，**run_node 必返回**（卡死线程被遗弃、后台继续烧 CPU/做副作用，Python 杀不了线程）；async 节点 wait_for cancel 同为协作式。真硬杀只在进程边界（code sidecar / 宿主命令 killpg，同 Go 侧）。即 Python 的不变量也是「Execute 必返回」而非「执行体必死」；Go 修复后同一不变量成立且更强——goja Interrupt 真正停掉 VM，无遗弃僵尸。plaita 无 sleep 节点（plaita-nodes 无对应物；Go 的 sleep 是前身引擎遗产，IR 已拒载）。

**双实例并发面推演（#47 叠加；A2 声明未实弹部分，本轮补推演留档）**：修复前窗口 = deadline 到点 → Execute 不返回 → watchdog 随 runCtx 退出且无 orphan 注册 → 租约无续约过期 → 其他 worker 取走 → #47 start 载体转换 continue → 第二实例并发跑。修复后 = 到点 → Interrupt/select 即时反弹 → Execute 返回 → deadlineExceeded 具名终态 → 租约释放，窗口闭合。租约真丢路径（renew 失败）的 orphan 注册/心跳语义不变。本轮未实弹双 worker 抢占（miniredis 单实例测试环境）；负控的 follower 恢复 + PEL/DLQ 零污染断言覆盖可观测面（`ctx_rebound_test.go`）。

**负控（先红后绿，#48）**：`internal/interpreter/goja_test.go` cancel/deadline 两子测试 pre-fix 双红（实测）；`pkg/workflow/ctx_rebound_test.go` sleep 87600h + WithCancel pre-fix 红（实测）；`pkg/worker/ctx_rebound_test.go` js 死循环 worker 层——pre-fix 30s 仍独占（红，实测），post-fix 预算内（2s）具名终态 + follower 恢复 + 队列零污染。

## 集合/循环节点对齐（#80 phase 1，2026-10-22，分支 feat/node-parity-1）

权威 = `plaita/node/loop.py` 的 BaseCollectionNode 家族 + While。全部落在 `pkg/workflow/nodes/`（collection.go 共享件 + map/filter/find/reduce/while.go）。phase 2 = case/calculate/mock/reference + manifest 导出；phase 3 = server 节点。

- **语义速查**（Python 逐条对齐）：五节点循环体是独立子流程，`$INPUT` 只含注入键（map/filter/find/loop = `{item,index}`；reduce 无 index，object 形态 `{first,second}`、子流程 `inputType.dataType=="array"` 时位置形态 `[acc,item]`——@flow 编译器固定产 array 形态），另有 `$PARENT`。**map**：结果 list 按集合顺序，空→`[]`；`concurrent`（历史别名 `async`）+ `maxConcurrent`（缺省/0=不设限，Python `or None`）。**filter**：裸 Python 真值判定（`if result:`），返回原子元素子集。**find**：首个真值短路返回**元素**，未找到/空→`None`。**reduce**：initial 缺省→首元素作初值从第二个起；空集合无 initial→`IndexError`（具名 + `NonRetryable`，确定性不烧重试预算）；空集合有 initial→返回 initial。**while**：先判断后执行；条件可见 `$LOOP-ITEM`（上一轮结果，首轮 None）+`$LOOP-INDEX`（不注入 `$LOOP-RESULT`——Loop 专属）；condition 缺省/null=只执行一轮（Python `condition is None → index==0`）；`max_iterations` 缺省 1000（snake/camel 双拼写），触顶仅告警。错误传播：五节点内部零 try/except，子流程错误直接冒泡（loop.go 既有同款）。
- **golden**：`scripts/export_collection_golden.py` 从 plaita 真跑导出（deployment IR → `Flow.model_validate` → `flow.run`），`pkg/ir/testdata/collection_golden.json` 39 例（每节点 5-8 例，含空集合/错误传播/嵌套 loop-in-loop、map→while、map→reduce、短路实证），`collection_golden_test.go` 逐例对拍（canonical JSON；reduce IndexError 钉两侧共有子串 `IndexError: list index out of range`）。负控：golden 全套在基线 c224d1e 精确红（`ir.Load` 拒载实测），post 绿。
- **并发 map（Go 面）**：每元素 goroutine + `runtime.CloneContext` 隔离 + `maxConcurrent` 信号量；childFlow 每元素一份 `childFlowView` 逐字段浅拷（`Workflow.running` 是实例级重入守卫，共享指针并发 Execute 会互斥失败；Context 含 RWMutex 不可整拷，照 `parallel.branchFlowView` 先例）；节点实例共享（执行面只读）。错误取最小失败下标（Python ThreadPoolExecutor.map 按序产出口径）。负控 `map_test.go`：并发峰值探针钉 `peak>1 且 ≤maxConcurrent` + 结果按集合顺序落位 + 缺省不设限峰值==元素数。
- **执行台阶两级对齐（2026-10-25，plaita fb36f085 配套）**：执行语义只有两级 `inline`/`managed`（owner 裁决：managed:queue 子档废除，唤醒源=Python 节点 `wake_service` 接线 + 受理门点名，不进模式词汇）。Go 侧只读对齐（当日尚处 frozen 约束、零引擎行为变化；次笔 ADR-002 解冻后该函数面成为跟随机制的第一个对拍锚）：`pkg/ir/capability.go` 新增 `ExecutionTier(nodeType)`（判据=注册节点 `IsSuspending()`，单源同 Python「实现声明事实」方向）/`WakeServiceNodeTypes()`（=nodeDependencies 五族 keys，与 Python `WAKE_SERVICE_NODE_TYPES` 互为对拍锚）；`capability_test.go` 三格对拍钉（Python 账本 managed 七节点/inline 锚逐项相等、唤醒源集合单源、supported 全覆盖恰一级）。账本=`plaita/docs/governance/architecture.yaml` `gov.capability_matrix`（值=两值台阶字符串）。
- **capability contract 起步（plaita#300 数据源）**：`pkg/ir/capability.go` 导出 `SupportedNodeTypes()`/`UnsupportedNodeTypes()`/`ContractVersion`（"2026-10-22"）/`CapabilityManifest()`——`{engine, ir_contract_version, supported_node_types, unsupported_node_types, features}`，supported/unsupported 一律由白名单自动生成（#300 硬性要求：不许手写第二份）；`capability_test.go` 钉清单与白名单逐项相等、支持类型必有注册工厂、五新类型在 supported 不在 unsupported、JSON 形状与 #300 issue 文本逐键对拍。features = `["checkpoint","hitl_await","suspend"]`。worker registry 写 manifest 归 #300 后续单。
- **已知残差（记录不修）**：① collection 求值为 dict 时取键（Python list(dict) 插入序 vs Go map 无序）——loop.go 既有分歧，author 端不该依赖，golden 规避；② Python Map 忽略 camel `maxConcurrent`（落 extra，真字段 `max_concurrent`），Go 收两者并集（fold `max_concurrent`→`maxConcurrent`）；③ Python While 触顶告警不报错的口径照搬（`max_iterations=0` 零轮且无告警）。

## 节点对齐 phase 2（#80，2026-10-22，r78，分支 feat/node-parity-2）

权威 = `plaita/node/decide.py`（SwitchLegacy）+ `mock.py` + `child.py`（ReferenceFlow）。golden 28 例真跑对拍（`scripts/export_node_parity2_golden.py` → `pkg/ir/testdata/node_parity2_golden.json`，`node_parity2_golden_test.go` 逐例对拍；reference 用例的 registry 即 ir.FlowProvider，复刻 plaita 调度器注入）。phase 3（server 节点）留 r79。

- **case（拍板：IR 归一化层脱糖为 switch）**：Python 的 case = `SwitchLegacy`，**模型校验期就把自己改写成普通 Switch**（每条 case → Branch(field=target, op=eq, value=case.value, next=case.id) + 恒置 isDefault 兜底分支）——「映射 switch + 字段兼容」不是二选一，是 Python 本来的实现方式。Go `translateCase` 做同款脱糖（type 改写 switch、构造 branches、metadata 记 `caseDialect` 留痕），引擎侧 case 与 switch 同一执行面（工厂按 `types.NodeTypeCase` 别名注册，维持「支持类型必有注册工厂」的 manifest 不变量）。字段兼容：`cases[].id` 必填=跳转目标（`cases[].next` 不参与路由——Python `Branch(next=case["id"])` 硬编码 id）；`cases[].value` 必填（null 合法，eq None 比较）；`cases[].name` 可选=label；`default` 缺省/null → 字面 `"default"` 节点 id（`values.get("default","default")` 的合成目标，实测命中 id 为 "default" 的节点）；target 缺省=恒 None（只有 value:null 的 case 可命中，desugar 以引擎 `$NULL` 字面量承载）；target 非表达式串加载期拒绝；缺 id/value 的 case 条目加载期拒绝（Python 模型层 ValueError 前置）；空 cases 合法（只剩兜底分支）。
- **mock**：数据固定 pin 占位（console 把被固定节点替换成本类型跳过真实执行），**未废弃、照实装**。语义：`value` 原样返回**不求值**（连 `"$INPUT.x"` 表达式串都字面成为节点结果——pin 语义即字面固定）；`output` 字段被忽略（runner 不对 mock 求 output）。
- **reference（拍板：provider 解析=加载期注入；inline childFlow 拒载）**：plaita 的调度器在运行前按 flowId/flowVersion 把 child_flow 注入 reference 节点。Go 对应物 = **加载期注入**：`ir.FlowProvider`（部署形态 JSON 接口，刻意不复用 `providers.WorkflowProvider`——解析必须重入 ir 管线，不能绕过静态校验）+ `ir.LoadWithFlowProvider`；注入的子流程走 scopeChild 归一化+校验，嵌套 reference 递归解析（深度上限 32，环在限深处具名拒绝；菱形引用不误伤——深度计数随子树闭合归还）；解析来源落 metadata `resolvedFrom`。**inline childFlow 在 reference 上拒绝**：r78 实测 plaita 运行时把它丢掉（`ReferenceFlow.setup_child_flow` 与基类 FlowNode 同名遮蔽，child_flow 恒 None）并在执行期炸 "child_flow for reference flow node is required"——加载期具名拒绝并指路（child 节点内联、或 flowId+provider），不给「Go 能跑、plaita 跑不了」的跨运行时假象。无 provider 的 `ir.Load` 遇 flowId 引用具名拒绝（worker 的 flowsource 走 `ir.Load`，provider 接线归 #300 后续单）。
- **reference/child 子流程语义（r78 实测钉死，golden + 单测双钉）**：①input 缺省/求值为 None → 子流程**继承调用方 $INPUT**（child 与 reference 同款，`child.go` 同步修——原为 nil）；②子流程**继承 $GLOBAL/$ENV 与执行身份**（$FLOW_ID/$EXECUTION_ID），但 **$PARENT.<x> 求值为 null**——「引用逻辑不能使用父逻辑上下文的数据」是实测口径（plain-run 路径不给子流程播种父快照，child 节点同款）；Go 的 reference 用 ambient-carrier 种子上下文实现（只携带 global/env/FlowDeadline/身份，不带 input/node，`referenceChildContext`）。**已知残差（记录不修）**：`$PARENT.GLOBAL.*`/`$PARENT.ENV.*` 在 Go 侧可解析到继承值（Python 疑似 null，未逐字验证）；child/loop/collection 子流程的 $PARENT 仍指向真实父执行（数据可见），与 Python 的 null 语义存在**既有分歧**——本轮不动（engine-wide，涉及 child/parallel/collection 的既有测试），留单。
- **`$PARENT.<bare-path>` 求值修正**（plaitaexpr evaluator，r78）：原实现把 `$PARENT.x` 重写为父上下文里的 `$x` 查寻，根缺省=KeyError 错误；Python 的 $PARENT 是快照**对象**，字段缺省=None（`_get_attr`）。修正：bare 路径（无 $ 根前缀）按「已存在对象缺字段→nil」解析，`$PARENT.$INPUT.x` 等 $ 根形式保持重写（其自身已有 nil-on-field-miss）。
- **calculate/ignore（拍板：保持拒载，issue 留痕）**：两者都**不是 plaita 注册节点类型**——registry.get 均为 None，`Flow.model_validate` 报 `unRecognized node type`（calculate.py 模块存在但 Calculate 类未注册=遗留物；ignore 只是 Node 基类的 `node_type: ClassVar = "ignore"` 占位）。给 Go 实装 calculate 反而会**放行 Python 拒绝的 flow**（反 parity）；knownUnsupported 提示保留 `unRecognized node type:` 同款子串逐字对拍（golden `reject_*` 用例钉）。
- **capability contract 收口（#300 五键定稿）**：manifest 五键 `{engine, ir_contract_version, supported_node_types, unsupported_node_types, features}` 不变；supported += case/mock/reference（白名单自动生成纪律不变），unsupported = calculate/ignore/sleep/trpc/mdb/readmdb/writemdb（calculate/ignore 的提示含逐字对拍子串）；`ContractVersion` bump 到 **"2026-10-22.2"**（同日 phase 1 为初始 "2026-10-22"——同版本字符串对应不同清单会让消费者缓存失真，必须 bump）。`ignore` 确认结论：**不是部署 IR 节点**（无注册、无前端产出、plaita 自身拒载），永远留在 unsupported。worker registry 写 manifest 归 #300 后续单。
- **负控（先红后绿，stash 基线实测）**：capability_test（phase 2 类型必须在 supported）基线红（Should be true，实测）；child input 继承单测基线红（got nil, want map[z:42]，实测）；golden 22 个行为用例基线全红（ir.Load 拒载，neg-red 逐例实测）；mock/reference 单测与 golden provider 路径在基线为编译级红（新 API/新符号，新接受面固有）。

## 节点对齐 phase 3（#80，2026-10-22，r79，分支 feat/node-parity-3）

权威 = `plaita/server/nodes/*_node.py`（BaseExtendedNode(EventNode) 家族）+ 对应 service 层。golden 30 例真跑对拍（`scripts/export_server_nodes_golden.py` → `pkg/ir/testdata/server_nodes_golden.json`，`server_nodes_golden_test.go` 逐例对拍），其中 delay/approval/http_callback 的恢复载荷由**真实 service** 驱动发布（DelayService.handle_task / ApprovalService.submit_approval_decision 的 any/all/majority 真策略 / HttpCallbackService.handle_callback_request 的真认证+两阶段认领），redis_queue/kafka_queue 依赖真实 broker，载荷按 service 源码的事件构造分支手建。本轮同修 #81（map childFlowView 漏 Timeout）。

- **架构口径（五节点同构）**：挂起型节点——Execute 产出 `{event_type, event_filter, is_async, event_id, status:"pending", service_config, node_subtype}`（Python 实测形状），引擎注册订阅后挂起；**等待/监听/决策在进程外**：worker 挂起时把 service_config 派发到 `{prefix}:{subtype}:queue`，外部 service（plaita Python ServiceManager 同族/控制面）消费后发布恢复事件，worker eventbus 唤醒。节点自身不连 Redis/Kafka、不监听端口、不睡眠（无占死消费者的盲区，#48 矩阵口径：挂起驻留型 Execute 即时返回）。
- **恢复状态形状（Python 实测钉死）**：`_handle_resume` → on_event 从持久化节点状态重建——**service_config/node_subtype/event_id/subscription_id 全量保留**，仅 status 翻转 + `event_data`（绑定键 `__plaita_subscription_id` 准入后剥离，两侧同款）；Go 的 `extendedResume` 从 `$NODE` 结果表回填同款。**对拍口径**：plaita 的分布式 resume 只处理被恢复节点本身（返回 lazy output 即止，不续跑后继图），Go 的 `Workflow.Resume` 恢复+续跑一步到位——两侧共有面 = 恢复后节点状态，golden 用 end 输出 `$NODE.<server>` 取同物。
- **加载期对齐（translate_server.go）**：pydantic 模型层语义前置到 ir.Load——必填字段（delay_seconds/approval_title+content+approvers/queue_name/bootstrap_servers+topic+group_id）、Literal 枚举（大小写归一后校验：delay_unit/approval_type+strategy/queue_type/message_format/security_protocol/sasl_mechanism/auto_offset_reset/callback_method/auth_type）、pydantic 缺省值填充（未写字段补 default，console 编译器只透传 @flow 源码写的字段）、固定事件契约覆盖（approval→approval_decision、redis_queue→redis_message、kafka_queue→kafka_message、http_callback→http_callback，用户 event_type 不参与；**delay 例外**：event_type 可配，缺省 delay_trigger）、EventNode 家族 camel 别名折叠（eventType/eventFilter/subscriptionTimeout）、approval 显式 `subscription_timeout:null` → -1 哨兵（无限等待）vs 缺省 24h（gt=0 校验）。
- **节点语义要点**：delay 时长 = 数值+单位枚举换算 ms（`$` 表达式解析失败/非数值**静默回退 60 秒**；非 $ 数值串 float 解析，Python ValueError 文案逐字对拍）；approval 审批人列表逐元素 `$` 解析、list 结果摊平；kafka bootstrap_servers 串按逗号切分+trim（list/None 原样透传）；http_callback 的 callback_path/auth_token 按 Python 真值判伪（nil/**空串**都走自动生成——自动路径 `/callback/{flow_id}/{node_id}/{execution_id}`、凭据 uuid hex）、full_url 硬编码 `http://localhost:8080` 前缀照搬、Q5 统一失败响应属 service 层（Go 侧无 callback service，`httpcb_wrong_token` 用例钉「凭据错→无事件→保持挂起」的节点面）。
- **worker 首派（§5.2 写侧补齐）**：persistSuspended 把挂起节点 service_config 写成 `dispatch_pending` 凭据（与 suspended 行同次落盘，#233）→ RPUSH `{prefix}:{subtype}:queue`（体 = service_config+execution_id+tenant_id，空租户归一 default）→ 成功清凭据（再落一次盘）；RPUSH 失败不 ack——凭据在，补派路径（start 幂等门 / suspended+continue 短路，二期既有）负责重派；派发 at-least-once（清凭据落盘失败→下一条载体重派→队列可重复），同 Python 消费端幂等假设。event/hitl_await 无 service_config 不派发。负控先红后绿（stash 写侧实测：队列空 redis:nil 红 → post 绿）。
- **capability contract（#300 形状演进）**：supported += 五类（白名单自动生成不变）；manifest 增第六键 `node_dependencies`（部署依赖标注：五节点的恢复都依赖外部 service 消费队列+发布事件，无消费者则停留 suspended——订阅超时兜底除外；additive 键，只读 #300 五键的 consumer 不受影响）；ContractVersion bump **"2026-10-22.3"**。
- **已知残差**：①delay/approval/redis_queue/kafka_queue/http_callback 的 service 层——**delay 已由 `pkg/wake` 补齐（2026-10-28，见结构树）**，approval/redis_queue/kafka_queue/http_callback 仍无 Go 消费者（审批记录/回调 ingress/队列桥是部署侧设施——纯 Go 部署用 plaita-wake，混编可用 Python 服务进程；capability node_dependencies 如实标注）；②`$NODE.<id>.event_data` 里 service 发布的易变时间戳在 golden 规范化层剔键（两侧同款 denylist+32hex/13位ms 打码）；③Python plain-run 分布式 resume 不续跑后继图（worker 下一跳载体驱动），Go 一步到位——runtime 编排差异，节点状态面无分歧。

## flow 定义指纹对齐（#60，2026-10-22，r81，分支 fix/hash-alignment）

`FlowDefinitionHash` 与 Python `compute_flow_definition_hash` 不等价，混编舰队（同 Redis 同 queue 双引擎）resume 硬门把未改定义判成 `flow_hash_mismatch`。权威 = `plaita/server/flow_worker.py` 的 `canonical_flow_definition_json`（`json.dumps(sort_keys=True, ensure_ascii=False, separators=(",",":"), default=str)` over `json.loads` 后的存储定义）。

- **实现（pkg/worker/canonicaljson.go）**：弃 `json.Marshal`（escapeHTML 改写 `<>&`、无条件转义 U+2028/U+2029、`\u0008`/`\u000c` 非 `\b`/`\f` 短形式、转义 DEL、json.Number 字面量直通），自写 CPython 编码器规则移植——①字符串 = `py_encode_basestring` ESCAPE_DCT 全集（`\b\f\t\n\r\"\\` 短形式 + 其余 <0x20 小写 `\u00xx`，其余一切码点原样 UTF-8）；②数字 = json.loads 口径：整数面保留精确十进制（`-0`→`0`，大整数精确），浮点面 `float.__repr__`（最短往返数字 + decpt∈(-4,16] 定点/否则科学计数 ≥2 位指数 + 整数值 `.0` 尾 + `1e400`→`Infinity`/`1e-400`→`0.0`，ParseFloat ErrRange 取 ±Inf/0 值）；③输入 = 严格单文档（trailing data 拒绝，同 json.loads）；④键序 = UTF-8 字节序 == 码点序（sort_keys 等价）。**新钉差异面（r120 十族之外）**：`\b`/`\f` 短转义、整数 `-0`、DEL 不转义、`1e400`→`Infinity`——全部随实现收敛。**已记录残差**：NaN/Infinity 字面量 Python parse_constant 宽容接受、Go 严格拒（解码层接受面分歧，含 NaN 定义在 Go 本就不可运行）；lone surrogate Python 在 encode 期炸、Go 换 U+FFFD（Python 侧 hash 本就不可达）。
- **golden（活体导出）**：`scripts/export_flow_hash_golden.py`（plaita 仓根运行）→ `pkg/worker/testdata/flow_hash_golden.json` 55 例 = A1 r120 十族（`a1_*`，issue 四锚点 hash 导出期自校验）+ 数字/转义/Unicode/结构扩展矩阵 + **仓内语料回放**（ir/testdata 全部 8 文件 + examples）+ goerr 拒收钉（NaN/Infinity/trailing）。`flow_hash_golden_test.go` 逐例断言**规范文本逐字节相等** + hash 相等 + 矩阵规模守卫（a1≥10、extended≥20）。离线差分 fuzz harness（`flow_hash_fuzz_test.go`，`PLAITA_FLOW_HASH_FUZZ` env 门）两轮种子 × 50 万例（随机 double 全域/数字拼写/字符串/嵌套结构）**全部逐字节一致**。
- **算法标记改名**：`FlowHashAlgoV2` = `flow-definition-json-sortkeys-v2-go`（Go 戳记）。语义与 Python 逐字节等价后共享标记本可成立，但「同名不同实现」正是 `upgrade_suspected` 恒 false 的根因（r120 增量②）；改名后：旧 Go 行（旧标记+错 hash）→ `upgrade_suspected=true` 可解释分支（运维放行 = 载体 `allow_flow_hash_change=true`）；Python 行（v2 标记+同 hash）→ hash 相等直接过门，下次落盘自然 refresh 标记。**ir_contract_version 不参与判定**（manifest 演进漂移、Python 无同形字段、算法身份已由标记承载——结论入 `docs/WORKER_PROTOCOL.md` §7 与 config.go 注释）。resume 硬门判据本身未动（issue 边界）。
- **混编端到端负控（先红后绿，stash 基线实测）**：`flow_hash_e2e_test.go` 三件——`TestMixedFleetResumeWithPythonHash`（Python 指纹写入共享 state 行 → Go resume 照跑；pre-fix 红，签名与 issue 实测一致：stored≠current、同标签、upgrade_suspected:false、`flow_hash_mismatch`）+ `TestMixedFleetResumeStillRejectsRealChange`（真变更仍拒，门未放宽）+ `TestMixedFleetLegacyGoRowExplainsUpgrade`（旧 Go 行升级嫌疑置真）。e2e 流定义内嵌分歧内容（`&`/`<>` 字符串 + 指数字面量），ir.Load 可载。

## FlowSource console 读穿透（#299 §2，2026-10-22，分支 fix/issue299-readthrough）

跨 Redis 部署（console 的 durable publish outbox 已在 plaita 2f1be94 落地）的最终一致窗口里，engine storage 缺 `{ns}:flow:{fid}:{ver}` 键。配置 `PLAITA_CONSOLE_FLOW_SOURCE_URL` 后，FlowSource 在 Redis miss 时回源 console 管理面 `GET /api/flows/{flow_id}/versions/{version}`（`console_flowsource.go`），失败才回到 `ErrFlowNotFound`。

- **鉴权/租户**：`X-Admin-API-Key`（= console `PLAITA_CONSOLE_ADMIN_API_KEY`，env `PLAITA_CONSOLE_FLOW_SOURCE_API_KEY`）+ `X-Tenant-ID`（API-key 是平台身份、console 按头选租户；空/default 租户显式拼 default）。不走 HMAC 契约端点——`/api/flowVersion/semver/detail` 的租户归属由密钥决定、无租户头，不适合 worker 按消息租户回源。
- **载荷口径**：拉到的定义经 `engineFlowPayload` 注入 `flow_id`/`version`（= outbox `engine_payload` / `sync_flow_to_engine` 的落盘工件形态），canonical 字节喂 `FlowDefinitionHash` 与 `ir.Load`——指纹与 outbox 登记的 `definition_hash` 同口径。空 version 的 `latest` 哨兵在 console 侧解析为**最新 published 版本**（`GET /api/flows/{id}` versions 清单，draft 跳过）。
- **错误分类（对齐 #304 + errors.go 二分）**：console 404 / 无 published / 版本未发布 / 定义损坏 / ir.Load 拒载 → `ErrFlowNotFound`（确定性：终态错误 + 负缓存 `PLAITA_CONSOLE_FLOW_SOURCE_NEGATIVE_TTL_SECONDS` 默认 30s）；网络/超时/5xx/其余 4xx → `ErrFlowLoad`（瞬态：**绝不负缓存**、不 ack 等重投，经 MaxDeliveries→DLQ 收敛——不可达不得伪造成不存在，也不炸消费循环）。每请求超时 `PLAITA_CONSOLE_FLOW_SOURCE_TIMEOUT_SECONDS` 默认 5s；瞬态重试 `PLAITA_CONSOLE_FLOW_SOURCE_ATTEMPTS` 默认 2（间隔 250ms）。
- **回写**：`PLAITA_CONSOLE_FLOW_SOURCE_WRITEBACK=1` 才把拉到的定义按 Preset 形态回写引擎 Redis（fleet 自愈）；**默认关**——保持对 #303「删除后目标侧不复活」purge 语义只读。
- **负控**：`console_flowsource_test.go` 八件（miss→拉取成功+回写自洽/404 具名+负缓存/不可达瞬态非 NotFound/瞬态重试后成功/既有命中零回归〔console 零调用〕/latest 解析+租户头透传/draft 拒载/payload 注入与数字字面量保真）。pre 基线编译级红（新符号无接受面）。

## CI Python↔Go 节点清单差分（#80 收尾，2026-10-22，r83，分支 fix/issue80-73-residuals）

防单侧新增漂移的活体门（#80 需求 4）：`make node-parity`（ci.yml 同步含此步）先用 `scripts/extract_python_node_types.py` 从 **plaita NodeRegistry 现场提取**权威节点类型清单（core 内建 + `plaita.nodes` entry points 插件——plaita 自带 server 五类 + plaita-nodes 发行版 22 类；须 `pip install` 两个 checkout，entry points 来自 dist-info，sys.path 替代不了），再由 `pkg/ir/node_parity_test.go` 把它与 `ir.SupportedNodeTypes()` 对拍。

- **判据**：双向差集必须与 `node_parity_test.go` 内的显式例外表完全一致，任何单侧新增/删除不改表即红；**过期例外条目同样红**（差集已不含它=表格漂移，通常是 Go 补齐了该类型，必须摘条）。
- **当前例外（#84 落地后，2026-10-22 实测差集 = 恰好这两条，全为 go-only）**：`code`（Python 刻意不进默认 registry，register_code_node 显式 opt-in；Go 为受限 JS 语言白名单 fail-closed）与 `redis`（Python 的 RedisNode 未自动注册；Go 为历史 33 类内建常驻）。python-only 例外已清零——`sql_query` 原列为「移交 #84」，#84 落地当日摘条，双向差集归零（`make node-parity` 绿，实测）。
- **skip 语义**：`PLAITA_NODE_TYPES_JSON` 未设时测试 skip——普通 `go test ./...` 无 plaita checkout 不该红；只有 `make node-parity` / CI 的 parity 步骤设置该 env。提取器遇「entry points 不可见 / 插件加载失败」硬失败（exit 2），**绝不输出缩水清单**（残缺权威=假绿）。
- **CI 侧权威 checkout 按 `ref: main` 活体跟踪**（非 SHA pin）——门的用途就是任一侧单方面新增立刻红，pin 死权威源等于废掉它；`uses:` 动作的 SHA-pin 审计规则不覆盖数据输入。

## sql_query 节点（#84，2026-10-22，r84，分支 feat/issue84-sql-query）

权威 = plaita-nodes `src/plaita_nodes/database.py`（SqlQueryNode）+ plaita#205 上限语义，逐行对拍。实现 = `pkg/workflow/nodes/sql_query.go`（+ sql_dsn / sql_params / sql_errors / sql_credentials / sql_drivers 五件）。plaita-go A 线（#80 移交族）至此全清，node-parity 双向差集归零。

- **驱动选型（拍板）**：`database/sql` + 三驱动全纯 Go、**无 cgo**（worker 免 C 工具链、可交叉编译）——postgres=`github.com/jackc/pgx/v5`（v5.7.6，stdlib 注册名 `pgx`）/ mysql=`github.com/go-sql-driver/mysql`（v1.9.3）/ sqlite=`modernc.org/sqlite`（v1.38.0）。版本刻意钉在 **go 1.24 线**：pgx v5.8+/modernc v1.39+ 要求 go 1.25/1.26，会把全仓工具链顶穿（go.mod 保持 `go 1.24.0`）。Python 侧 sqlalchemy 是 optional extra（懒加载）；Go 无可选导入，驱动编译期内建，scheme→驱动映射只活在 `sqlSchemeFamilies` 一处。SQLAlchemy URL 拼写（含 `postgresql+psycopg2://` 加号方言）原样接受。
- **凭据面**：`get_credential` 的 Go 对应物 = `SetSQLCredentialResolver` 进程钩子 + `PLAITA_CREDENTIALS_FILE` 明文 JSON 文件钩子（`{"名": {payload}}`，0600 fail-loud 权限检查同 A62-05 providers.json 纪律；**Python 的 Fernet console store 本仓不读**——与 webhook 凭据仓同为宿主装配接口口径）。两钩子皆缺 → 具名指路错误，绝不空 DSN 起拨。payload 两形（url / host-port-user-password-database 全集，driver 缺省 postgresql+psycopg2）与 Python 同。文件每执行现读（console 重导出免重启）。
- **上限（plaita#205 fail-closed）**：max_rows/max_bytes 缺省 1000 / 4 MiB；正整数校验先于任何凭据工作，0/负/非整 pydantic-lax 同款拒（字符串整数收、分数拒）且在引擎 Validate（构建期）即拒。SELECT 分批读取**先判限再入列**，超限抛具名 `*SQLResultLimitError`（max_rows/max_bytes/observed 三对字段可断言），NonRetryable（= Python DeterministicError，重试只会重复执行 SQL）。Python 的自适应 fetchmany 批是为 DBAPI 物化上限设的；Go `rows.Next()` 流式逐行，字节预算逐行先判，超限行外零物化——同保证，不同机制。
- **参数化**：flow 保持 sqlalchemy `text()` 方言（`:name` + params dict），节点内改写为驱动占位（postgres `$n` 按首现编号、同名复号；mysql/sqlite `?` 逐出现展开）。扫描器跳过串字面量/引号标识符/行注释/块注释/`$$…$$` dollar-quoted；`::` cast 不吞、`\:` 转义为字面冒号；缺值 `:name` 具名错误先于执行。
- **错误分类（刻意收窄，redis #73 先例）**：transport 族（net 错误/拨号拒/ErrBadConn/语句 deadline）→ `RetryableError`；pg SQLSTATE 白名单（08 连接类/53 资源类/40 事务回滚类/57P01-03 停机类）与 mysql errno 白名单（1040/1205/1213/2002/2003/2006/2013）→ 可重试；**服务器拒绝面（语法/权限/未知）一律 NonRetryable**——Python 引擎按异常形状会把 RuntimeError 全族按 retryTimes 放大（语法错误也重跑 SQL），Go 侧按「同语句+同状态必再败」收窄，已知分歧入档。`context.Canceled` 不可重试（是取消不是失败）。
- **运行时对应物**：取消 = ctx 贯传（A2R7-07 看门狗被 QueryContext/ExecContext 的驱动级取消吸收；引擎节点预算落地为 ctx deadline）；`_engine_cache` = 每 DSN 连接池的 8 上限 LRU（redis client 同款；database/sql 对陈旧池连接透明重拨，covers pool_pre_ping=True）；dry-run 短路在一切之前（不碰凭据面，params 敏感值不看）。
- **租户闸**：非 default 租户在凭据 resolver 之前拒（`refuseNonDefaultTenantReasoned`，ErrTenantIsolationUnavailable 族）——v1 凭据面无租户旁文件，租户级数据库须独立 worker 承载（code 节点 docker 后端指引的反向）。dry-run 先于闸（无任何副作用，同 capture 家族）。
- **形状**：SELECT → `{"rows": [...], "rowcount": len(rows)}`；写操作（INSERT/UPDATE/…按前导关键字分类走 ExecContext）→ `{"rowcount": affected}`。**已知残差（记录不修）**：①单元格 JSON 编码 Go 与 Python 的 dict 序/转义细节不同——上限字节预算是记账口径非跨语言逐字节契约；②time 列 Go 走 RFC3339（Python default=str 走自身格式）；③「读分类但零结果集」语句（如无 RETURNING 的可写 CTE）Go 报 rowcount 0，Python 报驱动 rowcount（常为 -1）；④语句分类按前导关键字（DBAPI returns_rows 的替代），WITH 恒走 rows 路径。
- **测试**：四条负控（基本查询/参数化/行上限具名/连接失败分类）走**公开 API**（ir.Load + wf.Execute + `PLAITA_CREDENTIALS_FILE` 钩子），基线（stash ir 白名单三件）逐条实测红、签名同上一行 `node type "sql_query" is not supported by this engine`，post 绿；细粒度单测（上限字段/脱敏红线/分类表/改写器/凭据文件钩子/租户闸/池 LRU）在 nodes 包（新 API 编译级红先例形态）。`make node-parity` 摘条后绿（实测）。shadow 对账不适用（shadow 样本纯计算纪律本就把 sql_query 列禁入）。

## 常用命令

```bash
# 本机 GOPROXY 可能指向内网 goproxy.woa.com（外网不可达），本仓统一走公共代理
GOPROXY=https://goproxy.cn,direct go build ./...
GOPROXY=https://goproxy.cn,direct go test ./...
make ci     # = ci.yml 同形：gofmt 检查 + vet + build + test（含 skip）+ node-parity + workflow 审计
make node-parity   # #80：plaita NodeRegistry 权威清单 ↔ ir.SupportedNodeTypes() 差分（例外表见 pkg/ir/node_parity_test.go）
make test   # 只跑测试；skip 清单与 CI 共用 scripts/ci-skip.txt
make test-all   # 不带 skip（当前只多跑 Test_now_today_eval，见「已知问题」）
make lint   # gofmt 检查 + vet（golangci-lint 配置存在但未接线）
```

## CI/门禁（2026-10-21，对齐 plaita B59/A60 同型）

- **单一真源**：跳过清单只在 `scripts/ci-skip.txt`（Makefile 与 ci.yml 都读它，改一处两边同时变）。此前 `make test` 不带 skip，出厂即红在 CI 恰好跳过的那条子测试上——本地红/CI 绿无法区分「自己弄坏了」还是「基线红」。
- **workflow 形状自审**：`scripts/ci-workflow-audit.sh` 由 ci.yml 第一步与 `make audit` 调用，机器校验 ①每个 `uses:` 是 40 位 SHA 且带 `# vX.Y.Z` 注释（dependabot 靠该注释跟进）②无浮动工具链（`go-version-file: go.mod`，不用 `stable`）③workflow 级 `permissions:` ④job `timeout-minutes:` ⑤checkout `persist-credentials: false`。回归时精确红。
- **concurrency 只对 PR 取消**：`cancel-in-progress` 绑 `github.event_name == 'pull_request'`。main 上并发取消会产生 `cancelled` 结论，而 landing gate 把 `cancelled` 判红——历史上已多次「main cancelled → 压分支」的假红。
- **本地 Makefile 不再吞 rc**：原 `lint: vet` + `gofmt -l .` 只打印未格式化文件而 exit 0（同一棵树 CI 红、本地绿）；现 `make fmt-check` 列表非空即 rc=1。Makefile 同时去掉了 `GOFLAGS=-mod=mod`——它让 `make build` 静默重写 go.mod/go.sum 并成功，而 CI（默认只读）同树失败。
- **`.code.yml` 已删**：内网 DevCloud 描述符的残留（artifact `go-loki` + `git.woa.com/thor-family/loki/go-loki`），与本仓 remote/module 无任何对应；`.golangci.yml` 的 `local-prefixes` 同步改回本模块。`pre-commit` 改为 local hooks（原配置走 `git://`——明文未认证、GitHub 已停用，`pre-commit install` 直接失败）。
- **dependabot**：`.github/dependabot.yml` 月度分组更新 gomod + github-actions（后者同时改写 SHA 与版本注释）。

## 节点对齐矩阵（现状）

| plaita (Python) | plaita-go | 状态 |
|---|---|---|
| assignment | assignment | ✅ ir 对齐（golden 过） |
| decide（switch） | switch | ✅ ir 对齐 + 条件算子补齐（golden 过） |
| concurrent（parallel） | parallel | ✅ ir 对齐 + 分支输入继承（golden 过） |
| loop（基础 for-in） | loop | ✅ ir 对齐 + 末次结果语义（golden 过）；map/filter/find/reduce/while 已随 #80 phase 1 转正（见「集合/循环节点对齐」节） |
| child（inline） | child | ✅ ir 对齐（golden 过）；reference 拒载 |
| http | http | ✅ ir 收录；SSRF 护栏字段拒载；golden 待补（执行面已有 nodes 测试） |
| redis | redis | ✅ ir 收录，字段同形 |
| start/end | start/end | ✅ ir 对齐；end nop 拒载 |
| code（python） | code（javascript + **python sidecar**） | ✅ 2026-10-20：python 经 subprocess sidecar（镜像 plaita `_RUNNER_TEMPLATE` 字节级契约，run(input)/stdin JSON/stdout 信封）；sandbox_backend 缺省/subprocess 接受，restricted/docker/unsafe fail-closed（生产 588 节点 100% subprocess 覆盖）。javascript 走 goja；**Go/yaegi 后端已删**，`language:"go"` 构造期+执行期均以 `ErrInProcessGoUnsupported` 拒载（A62-04） |
| agentrun（plaita-nodes） | agentrun（pkg/workflow/nodes/agentrun.go + pkg/agentproc） | ✅ spawn 路径落地（2026-10-19）；workspace 沙箱分支落地（Docker driver/envfile/lease）；**宿主直跑非 default 租户 fail-closed + 子进程 env 白名单**（2026-10-20，A62-02） |
| event（挂起） | event（pkg/workflow/nodes/event.go） | ✅ M6 落地（2026-10-19）：订阅/过滤/超时/event·timeout·cancel 恢复；仅根流程挂起 |
| hitl_await（挂起） | hitl_await（pkg/workflow/nodes/hitl_await.go） | ✅ M6 落地（2026-10-19）：发消息拿 session_id 即 pending，hitl_reply 事件/超时唤醒 |
| if（Bool） | if（pkg/workflow/nodes/if.go） | ✅ 2026-10-20 落地：condition Python 真值/两分支/缺 else 显式报错；ir 边集补 if 假分支 |
| case（SwitchLegacy） | switch（IR 脱糖，工厂别名注册） | ✅ #80 phase 2（r78）：等值匹配方言按 Python 模型校验期脱糖语义实装（golden 过） |
| mock | mock | ✅ #80 phase 2（r78）：value 原样透传（不求值）、output 忽略（golden 过） |
| reference | reference | ✅ #80 phase 2（r78）：加载期 provider 注入（ir.FlowProvider）+ $PARENT 隔离/$GLOBAL 继承/input 继承；inline childFlow 拒载（plaita 运行时同炸，golden 过） |
| delay | delay | ✅ #80 phase 3（r79）：挂起型+service_config 派发；时长换算/$回退 60s golden 钉；等待在外部 delay service（部署依赖见 capability） |
| approval | approval | ✅ #80 phase 3（r79）：挂起型；24h 订阅超时缺省/显式 null 无限等待；any/all/majority 在外部 approval service（真服务 golden 驱动） |
| redis_queue | redis_queue | ✅ #80 phase 3（r79）：监听型挂起节点（与动作型 redis 两回事）；连接/队列/监听三组配置 golden 钉；消费在外部 service |
| kafka_queue | kafka_queue | ✅ #80 phase 3（r79）：监听型挂起节点；SASL/消费者组配置；消费在外部 service（broker 部署依赖） |
| http_callback | http_callback | ✅ #80 phase 3（r79）：挂起型；自动路径/token·basic·signature 认证生成 golden 钉；ingress 在外部部署面 |
| calculate / ignore | — | 拒载（r78 结论：plaita 运行时未注册，unRecognized node type；清单见上） |
| —（plaita-nodes 仓余量） | agent/gate/rate_limit/report/notify 族等 | ✅ 已对齐（见上）|
| trpc / mdb（前身引擎特有） | 已删 | 不移植（公司内网设施） |

### plaita-nodes 22 节点全量盘点（2026-10-19 清点；M6 checkpoint v1.5 后 **22/22**）

| 节点 | Go 状态 | 说明 |
|---|---|---|
| agentrun | ✅ 已落地 | spawn 路径 + recursive-direct；**workspace 沙箱分支待做**（见下） |
| gate / rate_limit / report | ✅ 已落地（2026-10-19） | 语义逐行移植；gate 的 shlex/裁剪/重试、rate_limit 的 flock 计数、report 的 token 白名单全对齐；错误重试分类已落在错误值上，**引擎按类型分流的接线待做** |
| hitl | ✅ 已落地（2026-10-19，阻塞版） | hitl-server 协议（send/轮询/图片降级）对齐；deadline→timeout 不抛错；凭据 env/宿主装配 |
| hitl_await | ✅ 已落地（2026-10-19，M6 挂起版） | execute 发消息拿 session_id 即 pending；hitl_reply 事件/订阅超时唤醒；resume 解 cancel/timeout/replied |
| event | ✅ 已落地（2026-10-19，M6） | is_suspending；event_type 表达式/event_filter 点路径/subscription_timeout；Redis Pub/Sub 唤醒 |
| llm | ✅ 已落地（2026-10-19） | pkg/llm 双协议客户端（OpenAI/Anthropic 兼容）+ 三级端点链（显式>providers.json>LLM_* env）；${ENV} 凭据插值零入库 |
| notify / feishu_webhook / dingtalk_webhook / slack_webhook / wecom_webhook / generic_webhook / email_send | ✅ 已落地（2026-10-19） | payload/加签（钉钉 HMAC）/业务码分类（feishu code/wecom·dingtalk errcode/slack ok）对齐；凭据经 `NotifyCredentialStore` 接口（宿主装配，缺省 fail-closed）；URL 打码按 _redact |
| api_request / parse_json | ✅ 已落地（2026-10-19） | api_request：手动重定向（跨源剥头/303 规则/10 hop）、凭据经 resolver 接口 fail-closed、URL 打码；parse_json：候选提取（末行胜出，绝不用首个 `{`——#43）、verdict 白名单、join_fields |
| capture | ✅ 已落地（2026-10-19） | 本地命令捕获（命令失败非节点失败）；与 gate 同族复用 env 白名单/进程组/裁剪 |
| sql_query | ✅ 已落地（2026-10-22，#84，r84） | database/sql + 纯 Go 驱动矩阵（pgx/mysql/modernc sqlite，无 cgo）；凭据面 resolver 钩子 + `PLAITA_CREDENTIALS_FILE` 明文 JSON；上限 fail-closed（plaita#205）；详见「sql_query 节点」节 |
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
- **Unicode parity 三修（2026-10-21，#40/#41/#42，#21 残留）**：`$F.index` str 分支按**字符**计数（strings.Index 字节偏移经 `RuneCountInString` 前缀换算；list 分支不动）；`capitalize`/`title` 首字符位改 titlecase 映射（`pyTitleCaser = cases.Title`，仅喂单字符规避其分词差异——「upper≠titlecase」全集实测 135 码点（ß、Digraph、格鲁吉亚 Mtavruli 全段、希腊 iota 下标族、拉丁/亚美尼亚连字），x/text Title 与 Python 全集零偏差，免手写映射表；`upper()` 不动）；`getListItem` dict 的 str 键查找提到 `pyIndexArg` 之前（原 KeyError 分支为不可达死代码），dict+int 维持 KeyError、list/str IndexError 守恒。oracle 用例落 plaitaexpr 单测验收行（#21 批同型，golden JSON 保持纯程序化导出）。
- **错误分类与序列语义三修（2026-10-21，#43/#44/#45，表达式/引擎对齐第二批）**：
  - **#43 确定性错误不重试**：python sidecar 信封 `ok:false` 的 **ValueError/TypeError/NameError/SyntaxError** 四类在 `decodePythonRunnerOutput` 分类、经 `workflow.NonRetryable` 包裹（错误文本不变；`{"ok":false}` 缺型与 RuntimeError/KeyError 等其余类型保持历史可重试缺省——同 plaita 未知错误姿态）；end `FlowResultError` 构造点同包裹。Python 3.13 实测口径：subprocess 后端把信封包成 RuntimeError（`is_retryable_error`→**可重试**），in-process 后端 ValueError 直传（→**不可重试**）——两档位本就分裂，Go 侧按信封保留的 `type` 字段分类 = 取 in-process 同口径；end 侧 Python 在 `except FlowResultError` 内转 `ErrorResultException` 直接穿出 run_node 重试环、从不重试，无歧义。负控实弹计数（OnNodeStart，`retry_deterministic_test.go`）：sidecar ValueError retryTimes=2 pre-fix 3 次→post-fix 1 次；end-error retryTimes=3 pre-fix 4 次→post 1 次；瞬态 1002 重试行为守恒。
  - **#44 `$F.sort` 混排 TypeError**：排序比较改走共享 `Compare` 原语，首个不可比对具名 `$F.sort: '<' not supported between instances of 'str' and 'int'`（Python sorted 逐字文案形状）；dict 缺键投影回 **None**（`x.get(k)` 语义，原回退元素本身——缺键混排同样 TypeError）；desc/纯数值/纯字符串/int+float 行为守恒。已知残差：Python 原生 list 元素可排序（`sorted([[2],[1]])` 有序），Compare 域（数字/字符串）外报 TypeError——以具名报错替换静默错序（旧实现 lessObj 全 false 原样返回），与 `$F.gt/lte` 的既有 Compare 域一致。
  - **#45 `$F.mul` 超上限 ValueError**：`repeatCount` 钳零（静默空串）改具名错误，文案对齐 Python `_size_refusal`（`$F.mul(<len> * <n>) result too large: <投影> chars|elements (limit 1e8). Refusing to build an unbounded result.`，两方向归一 seq 在前、str 计字符/list 计元素）；判据=**投影规模** `len(seq)×n > lenLimit`（Python 守卫同形投影口径；恰好达限行为不变、负/零倍=空序列、数值乘法不受影响）；`lenLimit=1e8` 本身不动（#45 边界：下调到 Python ~100k 软上限另立单）。

**agentproc 节点（spawn 路径，2026-10-19 落地）**：选型拍板 = 只实现协议 host 侧（pkg/agentproc：spawn profile command → stdin 一行 turn → stdout NDJSON 事件组装），CLI 适配留在 agentproc hub bridge（与 Python 运行时**共用同一份 bridge.py**——影子对账要求两 runtime 适配器逐字节一致，diff 才能归因到引擎）；in-process executor 是 per-SDK 注册表，Go 不做（将来需要时按 spec four-case 规则插到 spawn 前）。注册表 = **同一份 agents.json/providers.json**（~/.plaita，`PLAITA_HOME` 可覆写）：provider 翻译器实现 recursive/claude 两个（与 Python `_ENV_TRANSLATORS` 同键），其他 executor 是锁定型 CLI 不接受 provider；**agents.json 条目必须带 `command`/`args`**（Go 走 spawn，缺了 fail-closed），如 `python3 <hub>/<executor>/bridge.py`。runner 语义 = spec v0.4：error 事件即失败（exit 0 也算）、streaming 时 body=partials 拼接（result.text 不重复追加）、非流式 body=result.text、无 result 无 partial 退 0 = 成功空 body、畸形行忽略、session 首个非空胜出（wire 非法丢弃）、超时 killpg 杀整组。输出 dict 对齐 Python 节点：`{text, cli, model, session_id, usage[, details]}`。`workspace` 沙箱分支已随 M5 Docker driver 落地（见下节）。

**golden 跑批**：`pkg/expression/golden_test.go` + `testdata/expression_golden.json`（45 条，由 `scripts/export_golden.py` 从 `plaita/tests/unit/test_expression_golden.py` 程序化导出，回灌校验后落盘）。**改表达式任何行为前先跑它**；Python 侧语义升级后重新导出：`cd ~/projects/infra4agent/plaita && python3 ~/projects/plaita-go/scripts/export_golden.py`。3 条依赖自定义注册表的用例不可导出（schema 无 registry 字段）；2 条 parse_function API 用例 runner 跳过（门面对应的是 evaluate 路径）。

## 已知问题

- ~~error_node 上游遗留红~~ / ~~`TestWorkflow_Execute` 两子例出网红~~ **已 hermetic 化（2026-10-23，B4 r14）**：`test_execute_workflow_with_error_node` / `test_execute_workflow_with_http_request_info` 原打 `http://invalid-url` / `http://tencent.com`，红的真因不是出口浮动而是**席位 Mac 的 `HTTP_PROXY`**——代理对不可解析主机回 **502**，而 http 节点把 5xx 当值（#12）不写 `$ERROR.http` → `[]`（GH runner 无代理、DNS 直连失败走 1002，故 25/25 绿；AGENTS 旧条「出口可达仍红」正是代理形态）。现两子例改打「刚关闭的 loopback httptest 端口」（ECONNREFUSED → 1002 带 request 快照；loopback 永不经代理、与 NO_PROXY 无关），断言原文不动，`http_request_info` 已移出 ci-skip。**写 http 节点测试一律 httptest，禁打公网主机。**
- ~~`cmd/plaita-worker` CI 随机 `exit status 130`（整包 0.2s 死、无 `--- FAIL` 行）~~ **已修（2026-10-23，B4 r14）**：main 近 25 run 的 4 红**全是它**（同轮 `pkg/workflow` 皆 ok）。根因=测试用 `syscall.Kill(getpid, SIGTERM)` 打真实进程信号，而 `runWorker` 从不 `signal.Stop`、二次信号走孤儿 goroutine `os.Exit(130)`；`os/signal` 分发是异步 goroutine，`signal.Reset` **不等在途信号排空**（只有 `Stop` 会 `signalWaitUntilIdle`），CI 多包争 CPU 时上一子测试的 SIGTERM 可晚到下一子测试 `Notify` 之后 → 误入 drain 并留下 130 goroutine → 吃掉下一枪。修法：`runWorker` `defer signal.Stop`，拆 `runWorkerUntil(..., sigCh)` 注入信号源（依赖 `drainableRunner` 三方法接口），二次信号改**返回** 130（main 仍 `os.Exit`），函数返回后零残留 goroutine；测试零进程信号，新增 Case D 用阻塞 Run 的脚本 runner 确定性钉 130（负控：旧形态下该例直接打死二进制）。**cmd 层测试禁用 `syscall.Kill` / `signal.Reset`。**
- `Test_now_today_eval`：gomonkey `time.Now` 补丁在同一测试二进制内多次重放不稳（darwin/arm64 实测 `-count=3` 第 3 次起红），CI skip。`$F.now/$F.today` 的 CI 覆盖因此只剩 `Test_strftime_directives`（直接测 strftime 层）。`Test_today_eval`/`Test_now_eval` 已不存在，skip 清单与测试注释同步剔除。
- `go vet ./...`：2026-10-21 复核 rc=0 无输出（旧条目里的 providers lock 值拷贝告警已不在）。

## 约定

- 引擎逻辑活在 flow 图里（jeffkit 红线）；不接受「宿主代码 + flow 皮」。
- 所有降级（错误/超时策略、重试）必须走事件流落盘，禁止静默。
- go.mod 禁止引入内网 module（git.woa.com / git.code.oa.com）。
