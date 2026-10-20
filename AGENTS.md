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

**fail-closed 拒载清单**：map/filter/find/reduce/while / if / case / calculate / mock / reference（需 provider）/ code 非 javascript / end `nop` / http 的 `allowedHosts`/`deniedHosts`/`blockPrivateNetworks`（SSRF 护栏未移植，靠它保护的 flow 不许静默降级）/ parallel 分支 input 非 dict。**M6 起 event / hitl_await 已转正**（挂起节点；仅有根流程挂起语义）。

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

## Face 2 安全对齐闸（2026-10-20，分支 fix/security-gates；#34 余量续于 fix/gate-capture-tenant）

A1 r62/r63 情报（A62-02/04/05、issue #34）的落地件。Python 权威语义：`plaita-nodes/_tenant_guard.py`、`agent_run._host_run_denied`、`plaita/node/code.py` 的 A42-06、`plaita/subprocess_env.py`。

- **租户闸全族（#34）**：四条宿主子进程路径对非 default 租户 spawn 前 fail-closed（命令零执行机会、`workflow.NonRetryable` 不烧重试预算；default / 无租户上下文保持历史语义）——① agentrun 宿主直跑 `ErrHostRunTenantDenied`（A62-02）；② gate / ③ capture / ④ code-python sidecar 共用 `pkg/workflow/nodes/tenant_guard.go` 的 `refuseNonDefaultTenant`（`ErrTenantIsolationUnavailable`，消息与日志带 marker `PLAITA_SUBPROCESS_TENANT_ISOLATION_UNAVAILABLE`，同 plaita A42-06 / pn#18）。gate/capture 的 dry-run 在拒绝点之前短路（无命令执行，同 Python）；code-python 的拒绝点落在 `runPythonSubprocessBudget`（唯一 spawn 入口，绕过节点直调也被拦）。租户由 worker 注入 `runtime.WithTenantID`（ctx 值，child/loop/parallel 经 context 链继承）。宿主命令/代码与 worker 同用户、共享宿主文件系统与租户级解密密钥——env 白名单只挡宿主凭据外泄，不构成租户凭据隔离边界；租户级执行需容器（docker，不挂载宿主凭据）。
- **agentrun 宿主直跑（A62-02）**：①`pkg/agentproc/runner.go` 的 `composeEnv` 不再继承全量 `os.Environ()`，改为白名单重建（`HostEnvAllowlist` = PATH/HOME/TMPDIR/LANG/LC_ALL/LC_CTYPE/PYTHONIOENCODING，同 plaita `build_subprocess_env`）+ 部署钩子 `HostEnvExtra`（同 `SUBPROCESS_ENV_EXTRA`；spec env 覆盖）——agent 进程只看到显式声明的 env，worker 的 `PLAITA_CREDENTIALS_KEY`/provider key 不随子进程走。**代理等部署变量必须经 `HostEnvExtra` 显式加**。②`agentrun` 宿主路径的租户闸见上；`workspace` 沙箱分支不受影响（容器边界 + 0600 envfile）。bare engine / 单租户部署（无租户上下文）保持放行，同 plaita 无租户层语义。`PLAITA_ALLOW_HOST_AGENT_RUN` 式 opt-out 未移植（默认拒绝，单机信任部署需另立单）。
- **code 的 Go/yaegi 后端（A62-04）**：`internal/interpreter/yeagi.go` + `GoArgs`/`GO_*` 常量删除，yaegi 依赖移出 go.mod；`code` 节点构造期（`Validate`）与执行期（`Execute`，防直接构造绕过）均以 `ErrInProcessGoUnsupported` 拒绝 `language:"go"`。支持面 = javascript（goja，进程内，保留原语义）+ python（subprocess sidecar）。IR `translateCode` 原本就拒 Go，worker 路径不变。
- **providers.json 明文（A62-05）**：`agentproc.readJSONFile` 对 agents.json / providers.json 做 fail-loud 权限检查——mode 0600 或更严，组/其他任何权限位 → 具名错误（含 `chmod 600 <path>`），无告警继续路径；Windows 跳过 POSIX 位检查（按 NTFS ACL）。明文 key 注入子进程的通道仍在（协议需要 provider env），加密/租户凭据路由另单；check-then-read 的 TOCTOU 不构成安全边界（本地运营者配置，威胁模型是误配泄露而非同机竞态）。

## 常用命令

```bash
# 本机 GOPROXY 可能指向内网 goproxy.woa.com（外网不可达），本仓统一走公共代理
GOPROXY=https://goproxy.cn,direct go build ./...
GOPROXY=https://goproxy.cn,direct go test ./...
make ci     # = ci.yml 同形：gofmt 检查 + vet + build + test（含 skip）+ workflow 审计
make test   # 只跑测试；skip 清单与 CI 共用 scripts/ci-skip.txt
make test-all   # 不带 skip（无出口网络时 http_request_info 会红，见「已知问题」）
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
| loop（基础 for-in） | loop | ✅ ir 对齐 + 末次结果语义（golden 过）；map/filter/find/reduce/while 拒载 |
| child（inline） | child | ✅ ir 对齐（golden 过）；reference 拒载 |
| http | http | ✅ ir 收录；SSRF 护栏字段拒载；golden 待补（执行面已有 nodes 测试） |
| redis | redis | ✅ ir 收录，字段同形 |
| start/end | start/end | ✅ ir 对齐；end nop 拒载 |
| code（python） | code（javascript + **python sidecar**） | ✅ 2026-10-20：python 经 subprocess sidecar（镜像 plaita `_RUNNER_TEMPLATE` 字节级契约，run(input)/stdin JSON/stdout 信封）；sandbox_backend 缺省/subprocess 接受，restricted/docker/unsafe fail-closed（生产 588 节点 100% subprocess 覆盖）。javascript 走 goja；**Go/yaegi 后端已删**，`language:"go"` 构造期+执行期均以 `ErrInProcessGoUnsupported` 拒载（A62-04） |
| agentrun（plaita-nodes） | agentrun（pkg/workflow/nodes/agentrun.go + pkg/agentproc） | ✅ spawn 路径落地（2026-10-19）；workspace 沙箱分支落地（Docker driver/envfile/lease）；**宿主直跑非 default 租户 fail-closed + 子进程 env 白名单**（2026-10-20，A62-02） |
| event（挂起） | event（pkg/workflow/nodes/event.go） | ✅ M6 落地（2026-10-19）：订阅/过滤/超时/event·timeout·cancel 恢复；仅根流程挂起 |
| hitl_await（挂起） | hitl_await（pkg/workflow/nodes/hitl_await.go） | ✅ M6 落地（2026-10-19）：发消息拿 session_id 即 pending，hitl_reply 事件/超时唤醒 |
| if（Bool） | if（pkg/workflow/nodes/if.go） | ✅ 2026-10-20 落地：condition Python 真值/两分支/缺 else 显式报错；ir 边集补 if 假分支 |
| mock / calculate | — | 拒载（清单见上）；decision/llm/hitl 族已转正 |
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
- `TestWorkflow_Execute/test_execute_workflow_with_http_request_info`（当前唯一红）：`$ERROR.http.Info.Request.Method` 取到 `[]`，end 按 string 校验报错。用例打真实出网（`http://tencent.com`），出口不可达时确定性红。**CI 与 `make test` 经 `scripts/ci-skip.txt` 跳过，`make test-all` 复现**。2026-10-21 复核：同表的 `test_execute_workflow_with_error_node` 已绿（旧条目把它记成了这条红）。
- `Test_now_today_eval`：gomonkey `time.Now` 补丁在同一测试二进制内多次重放不稳（darwin/arm64 实测 `-count=3` 第 3 次起红），CI skip。`$F.now/$F.today` 的 CI 覆盖因此只剩 `Test_strftime_directives`（直接测 strftime 层）。`Test_today_eval`/`Test_now_eval` 已不存在，skip 清单与测试注释同步剔除。
- `go vet ./...`：2026-10-21 复核 rc=0 无输出（旧条目里的 providers lock 值拷贝告警已不在）。

## 约定

- 引擎逻辑活在 flow 图里（jeffkit 红线）；不接受「宿主代码 + flow 皮」。
- 所有降级（错误/超时策略、重试）必须走事件流落盘，禁止静默。
- go.mod 禁止引入内网 module（git.woa.com / git.code.oa.com）。
