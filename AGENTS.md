# AGENTS.md — plaita-go

> plaita 的 Golang 引擎 + 运行时。
> 定位：**引擎 + worker 运行时**，不做全栈替代——console/keeper/派发控制面继续由 Python 侧承担，本仓按 queue/lease 协议接入。
> 负责人：jeffkit | 创建：2026-10-19

## 已拍板的三个决策（2026-10-19）

1. **表达式语言 = plaita 现行**（`$INPUT/$NODE/$PARENT/$GLOBAL/$ENV` + `$F.fn()` + `{% ... %}` 模板插值），不做 JS 方言。plaitaexpr 与 plaita 是同族语言（变量根完全一致），对齐成本低；缺口清单见下文「表达式对齐」。
2. **权威 IR = plaita 部署形态的 camelCase JSON**（`tests/fixture/assigment.json`、`examples/agent/flows/router.json` 即此形态）。@flow Python 源码只是前端写法，编译产物交给本引擎跑。snake_case 的 pydantic model_dump 是 Python 内部模型，不是契约。**禁止**在本仓再引入第三种表示；schema 变更必须同步 ir_validate 语义与往返 golden 测试（吸取 plaita「DSL 三表示往返无不变量 ~20 单」的教训）。
3. **来源已清理**：模块路径 `github.com/jeffkit/plaita-go`，git 历史不带入，腾讯内网依赖全删。

## 仓库结构

```
engine 侧（现 pkg/，内核自 前身引擎）：
  pkg/workflow/       Workflow/Node 接口/registry/retry/timeout/event 回调
  pkg/workflow/nodes/ 内置节点（assignment/branch/http/loop/parallel/redis/code/child/end）
  pkg/expression/     plaitaexpr（$ 变量 + $F 函数，goval 求值）
  internal/interpreter/ goja（JS）/ yeagi（Go 源码）解释器，code 节点用
  pkg/model, pkg/parser, pkg/providers/  流程模型、解析、flow 来源（fs/memory）
  pkg/delegate, pkg/discovery/           http 节点的委托与寻址（polaris 已删，仅 domain/noop）
规划中：
  engine/ir/    plaita 部署 JSON 的 schema + 解析 + 校验（契约层，优先级最高）
  nodes/agentproc/  agentproc stdin/stdout NDJSON 客户端 → agent 节点
  runtime/      worker、队列接入、租约、事件持久化（对齐 plaita server/ 语义）
  server/       console BFF 契约只读子集（execution 查询/事件流）
```

## 常用命令

```bash
# 本机 GOPROXY 可能指向内网 goproxy.woa.com（外网不可达），本仓统一走公共代理
GOPROXY=https://goproxy.cn,direct go build ./...
GOPROXY=https://goproxy.cn,direct go test ./...
make test   # 已封装代理
```

## 节点对齐矩阵（现状）

| plaita (Python) | plaita-go（前身引擎 内核） | 状态 |
|---|---|---|
| assignment | assignment | 有，语义待对齐（输出包裹 `$F.concat` 差异待核） |
| decide | branch（`switch` JSON type） | 有，语义待对齐 |
| concurrent | parallel | 有 |
| loop | loop | 有 |
| http | http | 有（已改 net/http） |
| redis | redis | 有 |
| code | code（goja JS + yeagi Go） | 有，plaita 侧是 Python 语义，**需定义 Go 版 code 节点契约** |
| child | child | 有 |
| start/end/start→end | start/end | 有 |
| mock / event_node / calculate / basic | — | 缺 |
| —（plaita-nodes 仓） | — | 缺：agent（agentproc）、gate、rate_limit、report、hitl、hitl_await 等 22 节点 |
| trpc / mdb（前身引擎 特有） | 已删 | 不移植（公司内网设施） |

## 表达式对齐（缺口清单）

变量根一致：`INPUT/NODE/PARENT/GLOBAL/ENV`（plaitaexpr 另有 `ERROR/NULL/EMPTYSTRING`，保留）。

plaita 有而 plaitaexpr 缺的 `$F` 函数：`eq, gt, gte, lt, lte, ne, ifelse, notContains, strip, getDictValues, pop`。
引擎级差异：`{% ... %}` 模板插值（plaitaexpr 无）；未知函数回落 `"undefined"` 哨兵（goval 是报错，需改）；缺根 key 报错语义对齐 plaita KeyError 口径；`$NODE.raw.<node>` 免评估旁路。
验收方法：从 `plaita/tests/unit/test_expression_golden.py` 导出 golden 用例为 JSON，Go 侧跑同一份。

## 已知问题（上游带入）

- `TestWorkflow_Execute/test_execute_workflow_with_error_node` 红（got 1003 want 1002）——**原仓同样红**，非清理引入，修复前先在原仓确权。
- `go vet`：pkg/providers/memory、filesystem 有 lock 值拷贝告警（上游质量噪音）。

## 约定

- 引擎逻辑活在 flow 图里（jeffkit 红线）；不接受「宿主代码 + flow 皮」。
- 所有降级（错误/超时策略、重试）必须走事件流落盘，禁止静默。
- go.mod 禁止引入内网 module（git.woa.com / git.code.oa.com）。
