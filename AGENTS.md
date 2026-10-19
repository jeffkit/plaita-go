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
  pkg/workflow/       Workflow/Node 接口/registry/retry/timeout/event 回调
  pkg/workflow/nodes/ 内置节点（assignment/branch/http/loop/parallel/redis/code/child/end）
  pkg/expression/     plaitaexpr（$ 变量 + $F 函数，goval 求值）
  internal/interpreter/ goja（JS）/ yeagi（Go 源码）解释器，code 节点用
  pkg/model, pkg/parser, pkg/providers/  流程模型、解析、flow 来源（fs/memory）
  pkg/delegate, pkg/discovery/           http 节点的委托与寻址（polaris 已删，仅 domain/noop）
规划中：
  nodes/agentproc/  agentproc stdin/stdout NDJSON 客户端 → agent 节点
  runtime/      worker、队列接入、租约、事件持久化（对齐 plaita server/ 语义）
  server/       console BFF 契约只读子集（execution 查询/事件流）
```

## IR 契约层（pkg/ir，已落地 2026-10-19）

`ir.Load(data) (*workflow.Workflow, error)`：plaita 部署 JSON → 引擎 Workflow。管线 = 归一化 → 结构校验 → 引擎反序列化。

**权威契约 = plaita codeflow 编译器 `compile_source` 的产物**（顶层 `flow_id/inputType/nodes/runtime`；camel 为主、`source_line` 等 snake 残留双键都收；console 旧导出的 `id`/`required`/`default`/`children` 也归一）。`model_dump(by_alias=True)` 是 snake_case，**不是**发布形态。

**归一化规则**（pkg/ir/normalize.go）：flow_id|flowId|id（flow_id 优先）；无 id 合成（根=`plaita-flow`，嵌入=`<node>.childFlow`/`<node>.branch.<name>`，引擎要求非空 id）；`global_context`→`context.global`；handler `continue_with`→`continue-with`、plaita 的 `code`/`message` 落 metadata 防丢失；timeout 毫秒数→ISO8601；Property 的 `type/title/description/default/children`→引擎拼写；switch 分支 `next` 空时回落 `name`（branch_name_as_target 语义）。

**校验子集**（pkg/ir/validate.go，对应 ir_validate DEFAULT_RULES）：唯一 start + 全图可达（R1）、next/分支引用存在（R3）、嵌入流程可达 end（R2）、`joinBranches ⊆ branches[].name`（R4 语义）。表达式级校验（R5）随表达式对齐工作流补。

**fail-closed 拒载清单**：decision（plaita-nodes）/ map/filter/find/reduce/while / if / case / calculate / event / mock / reference（需 provider）/ code 非 javascript / `sandbox_backend` / end `nop` / http 的 `allowedHosts`/`deniedHosts`/`blockPrivateNetworks`（SSRF 护栏未移植，靠它保护的 flow 不许静默降级）/ parallel 分支 input 非 dict。

**随 ir 对齐落地的引擎语义修正**（plaita 语义为基准）：
- loop：`$NODE.<loop>` = 末次子结果（前身引擎为 nil）
- switch：default 分支 0..1 个（原强制恰好 1）；无匹配且无 default → 执行期显式报错
- 条件算子补齐 plaita 集：`contains`（右∈左：串子串/切片元素/map 键）/`notContains`/`truthy`/`falsy`（Python 真值语义，value 不参与）
- parallel：分支无 `input` 时继承父 `$INPUT`（原为 nil）
- loop 无 `condition` = 恒真（零值 Condition 不再报"invalid expression type"）

**golden 测试**（pkg/ir/ir_test.go + testdata/）：assignment_flow.json 是 console 真实导出；loop_flow/parallel_flow/child_flow 由 **plaita 编译器实编**（`flow_from_source` → `compile_source`），在 Go 引擎执行并断言输出——「Python codeFlow 产 IR → Go 跑」已实证。改契约前先重跑这组。

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
| mock / event / calculate | — | 拒载（清单见上） |
| —（plaita-nodes 仓：decision/agent/gate/rate_limit/report/hitl/hitl_await 等） | — | 缺：decision 已在拒载清单带提示 |
| trpc / mdb（前身引擎特有） | 已删 | 不移植（公司内网设施） |

### plaita-nodes 22 节点全量盘点（2026-10-19 清点；Wave 2.5/2.6 后 **20/22**，余 hitl_await+event 依赖挂起恢复地基）

| 节点 | Go 状态 | 说明 |
|---|---|---|
| agentrun | ✅ 已落地 | spawn 路径 + recursive-direct；**workspace 沙箱分支待做**（见下） |
| gate / rate_limit / report | ✅ 已落地（2026-10-19） | 语义逐行移植；gate 的 shlex/裁剪/重试、rate_limit 的 flock 计数、report 的 token 白名单全对齐；错误重试分类已落在错误值上，**引擎按类型分流的接线待做** |
| hitl | ✅ 已落地（2026-10-19，阻塞版） | hitl-server 协议（send/轮询/图片降级）对齐；deadline→timeout 不抛错；凭据 env/宿主装配；hitl_await（挂起版）仍 ❌ 待挂起恢复地基 |
| decision | ✅ 已落地（2026-10-19） | 封闭决策空间/confidence 门控/批量 index 对齐/provider 注册表（llm+jev+jevlike stub）；DecisionError=可重试口径 |
| llm | ✅ 已落地（2026-10-19） | pkg/llm 双协议客户端（OpenAI/Anthropic 兼容）+ 三级端点链（显式>providers.json>LLM_* env）；${ENV} 凭据插值零入库 |
| notify / feishu_webhook / dingtalk_webhook / slack_webhook / wecom_webhook / generic_webhook / email_send | ✅ 已落地（2026-10-19） | payload/加签（钉钉 HMAC）/业务码分类（feishu code/wecom·dingtalk errcode/slack ok）对齐；凭据经 `NotifyCredentialStore` 接口（宿主装配，缺省 fail-closed）；URL 打码按 _redact |
| api_request / parse_json | ✅ 已落地（2026-10-19） | api_request：手动重定向（跨源剥头/303 规则/10 hop）、凭据经 resolver 接口 fail-closed、URL 打码；parse_json：候选提取（末行胜出，绝不用首个 `{`——#43）、verdict 白名单、join_fields |
| capture | ✅ 已落地（2026-10-19） | 本地命令捕获（命令失败非节点失败）；与 gate 同族复用 env 白名单/进程组/裁剪 |
| sql_query | ❌ 未做（22 节点清点外补充：database.py，SQLAlchemy 多驱动） | Go 侧需驱动选型（新依赖）+ 凭据仓，按生产需要立项 |
| git_publish / github_comment / writefile | ❌ 拒载 | 文件系统/远端写操作，需沙箱/宿主工作区约定，**Wave 2-3** |

**已知接线缺口（Wave 2 遗留）**：①引擎 `workflow.executeNode` 对节点错误统一按 retryTimes 重试——notify/api_request 已把「确定性 vs 瞬态」分类落在错误值上，引擎按错误类型分流的接线待做；②webhook/邮件凭据仓（Python 侧 Fernet console 存储）Go 侧为宿主装配接口，worker 主线接。

### 沙箱支持（已拍板：要支持，2026-10-19）

agentrun `workspace=` 分支从「拒载」改为「计划支持」。**对齐生产契约**（plaita-nodes `_execute_sandboxed`）：workspace 名 → `.plaita/sandboxes.json` 注册表（fail-closed 不自动建）→ driver → 沙箱内 agentproc → env 白名单 + envfile(0600) 注入 → lease/ensure/释放（成功 kill / 失败 pause 分流）。**Go 实现取形**：Docker driver 优先（本地Mac/容器宿主，docker API），AGS 经 E2B 兼容 HTTP API 接（不移植 4300 行 driver，调用其服务面）。顺序在 worker 主线之后（沙箱 lease 与 worker 的执行租约同族，一起设计）。

## 表达式对齐（已落地 2026-10-19，golden 45/45 绿）

变量根一致：`INPUT/NODE/PARENT/GLOBAL/ENV`（引擎另有 `ERROR/NULL/EMPTYSTRING`，保留）。

**已对齐**（语义权威 = `plaita/core/expression_parser.py` docstring + golden）：
- **`$F` 函数**：补齐 `eq/ne/gt/gte/lt/lte`（跨类型数值+容器深比较）、`ifelse`（急切）、`notContains`、`getDictValues`、`pop`（list/dict 双形态）；`and/or/not` 改 Python 真值语义（`or` 返回首个真值参数的**值**）；`concat`/`get`/`getDictValue` 变参+默认参可用（适配器支持 variadic，nil 实参→参数零值）。
- **`{% ... %}` 模板插值**：门面分发——`$` 开头=整串表达式（原类型返回）；含 `{% %}`=模板（内层须 `$` 前缀且可解析才插值，否则字面）；其余原样。nil 透传。
- **未知函数 = 报错**（不采 scoped registry 的 `"undefined"` 哨兵）：plaita 默认注册表 NameError + did-you-mean，部署 flow 用默认注册表，fail-closed 同仓规。goval 自带 `no such function` panic→error。
- **`$NODE.raw.<node>`** 免评估旁路（#130，含同名 `raw` 节点冲突规则）；**缺键语义**：缺根=error（KeyError 口径），已存在对象缺字段=nil，索引 miss=error，负索引 `[-1]` 支持。
- **语句变量预解析架构**（plaitaexpr/evaluator.go `preResolveStatementVariables`）：`$F` 语句里的内建根变量先经 plaita 遍历求值、注入合成标识符，goval 只做函数调用——方括号 `X[0]`/`[-1]`（parser 归一为 `.#N`）、raw 修饰符、缺键语义在语句里与纯变量路径完全一致；`$PARENT.` 链整段吸收走 goval 导航。
- **parser**：单引号字面量→双引号转换（goval 无单引号 token）、尾逗号剔除、字面量掩码防误改。

**agentproc 节点（spawn 路径，2026-10-19 落地）**：选型拍板 = 只实现协议 host 侧（pkg/agentproc：spawn profile command → stdin 一行 turn → stdout NDJSON 事件组装），CLI 适配留在 agentproc hub bridge（与 Python 运行时**共用同一份 bridge.py**——影子对账要求两 runtime 适配器逐字节一致，diff 才能归因到引擎）；in-process executor 是 per-SDK 注册表，Go 不做（将来需要时按 spec four-case 规则插到 spawn 前）。注册表 = **同一份 agents.json/providers.json**（~/.plaita，`PLAITA_HOME` 可覆写）：provider 翻译器实现 recursive/claude 两个（与 Python `_ENV_TRANSLATORS` 同键），其他 executor 是锁定型 CLI 不接受 provider；**agents.json 条目必须带 `command`/`args`**（Go 走 spawn，缺了 fail-closed），如 `python3 <hub>/<executor>/bridge.py`。runner 语义 = spec v0.4：error 事件即失败（exit 0 也算）、streaming 时 body=partials 拼接（result.text 不重复追加）、非流式 body=result.text、无 result 无 partial 退 0 = 成功空 body、畸形行忽略、session 首个非空胜出（wire 非法丢弃）、超时 killpg 杀整组。输出 dict 对齐 Python 节点：`{text, cli, model, session_id, usage[, details]}`。`workspace` 沙箱分支拒载（AGS driver 未移植）。

**golden 跑批**：`pkg/expression/golden_test.go` + `testdata/expression_golden.json`（45 条，由 `scripts/export_golden.py` 从 `plaita/tests/unit/test_expression_golden.py` 程序化导出，回灌校验后落盘）。**改表达式任何行为前先跑它**；Python 侧语义升级后重新导出：`cd ~/projects/infra4agent/plaita && python3 ~/projects/plaita-go/scripts/export_golden.py`。3 条依赖自定义注册表的用例不可导出（schema 无 registry 字段）；2 条 parse_function API 用例 runner 跳过（门面对应的是 evaluate 路径）。

## 已知问题（上游带入）

- `TestWorkflow_Execute/test_execute_workflow_with_error_node` 红（got 1003 want 1002）——**原仓同样红**，非清理引入，修复前先在原仓确权。
- `go vet`：pkg/providers/memory、filesystem 有 lock 值拷贝告警（上游质量噪音）。

## 约定

- 引擎逻辑活在 flow 图里（jeffkit 红线）；不接受「宿主代码 + flow 皮」。
- 所有降级（错误/超时策略、重试）必须走事件流落盘，禁止静默。
- go.mod 禁止引入内网 module（git.woa.com / git.code.oa.com）。
