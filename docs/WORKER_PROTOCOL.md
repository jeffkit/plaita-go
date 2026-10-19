# WORKER_PROTOCOL — plaita Python worker ↔ Redis/console 交互协议田野调查

> 用途：plaita-go worker 实现与影子对账的**验收基准**。每条结论都带 Python 侧 `文件:行号` 引用（路径相对 `/Users/kong/projects/infra4agent/plaita/`，console 侧相对 `plaita-console/`，keeper 侧相对 issue-keeper 仓）。
> 调查基线：plaita main @ 2026-10-19；`flow_worker.py` 7455 行、`task_queue.py` 1204 行。
> 「未查实」标记处表示本轮只读调查未取得代码证据，实现前必须回源核对，禁止照抄本文件的占位描述。

---

## 目录

1. [部署与配置面](#1-部署与配置面)
2. [队列协议](#2-队列协议)
3. [租约协议](#3-租约协议)
4. [取消协议](#4-取消协议)
5. [执行与结果写回](#5-执行与结果写回)
6. [事件持久化](#6-事件持久化)
7. [flow IR 来源解析](#7-flow-ir-来源解析)
8. [心跳与僵尸判定口径](#8-心跳与僵尸判定口径)
9. [退避 / drain / 优雅退出](#9-退避--drain--优雅退出)
10. [Go 移植注意（对照 plaita-go 现状）](#10-go-移植注意)

---

## 1. 部署与配置面

### 1.1 进程形态

- CLI 入口 `plaita/server/flow_worker.py:7210`（`main()`），装配 `RedisFlowWorker`（`flow_worker.py:4400`，类声明 `class RedisFlowWorker(RegistryMixin, ControlMixin, FlowWorker)`）。
- `run()` 主入口 `flow_worker.py:6226`：`set_current_tenant(DEFAULT_TENANT_ID, source=TENANT_SOURCE_EXPLICIT)`（:6241，#239 单租户部署迁移：服务入口显式声明 default 租户，写路径对隐式 default fail-closed——见 `plaita/tenant_context.py:153-162` `guard_tenant_write`），然后建队列、起看门狗/取消监听/注册/控制监听/metrics，最后进入消费循环。
- 并发：`concurrency=1` 时单线程原地串行；`>1` 时起 N 个 `flow-consume-{i}` daemon 线程共享同一 worker 实例与同一 `RedisStreamTaskQueue`（`flow_worker.py:6276-6296`；连接安全依赖 redis-py 连接池，:6274-6275 注释）。

### 1.2 CLI 参数 / env 全表（`flow_worker.py:7218-7310`）

| 参数 / env | 默认 | 语义（行号） |
|---|---|---|
| `--redis-url` / `PLAITA_REDIS_URL` / `REDIS_URL` | `redis://localhost:6379/0` | Redis 连接（:7221-7223）。解析唯一路径 `factory._parse_redis_url`（`server/factory.py:1737`），支持 `redis/rediss/unix`（:64），畸形 URL 抛 `RedisUrlError` 绝不静默回落（:1552-1559） |
| `--queue-name` / `PLAITA_QUEUE_NAME` / `QUEUE_NAME` | `plaita:flow:queue` | 任务 Stream 键（:7224-7226）；EventFilter 侧同默认（`server/event_filter.py:40`） |
| `--consumer-group` / `PLAITA_CONSUMER_GROUP` | `plaita-workers` | 消费组（`server/task_queue.py:26` DEFAULT_CONSUMER_GROUP） |
| `--consumer-name` / `PLAITA_CONSUMER_NAME` | instance_id，否则 `worker-<pid>` | `_resolve_consumer_name`（`flow_worker.py:4593-4598`） |
| `--claim-min-idle-ms` / `PLAITA_CLAIM_MIN_IDLE_MS` | 60000 | pending 消息 XCLAIM 回收阈值（`task_queue.py:28`） |
| `--lease-ttl-seconds` / `PLAITA_LEASE_TTL_SECONDS` | 120 | 执行租约 TTL（`server/execution_lease.py:25`） |
| `--max-deliveries` / `PLAITA_MAX_DELIVERIES` | 5 | 最大投递/处理尝试次数（`task_queue.py:29`；注意它是**跨三个机制的上界**，:395-410 注释） |
| `--dlq-key` / `PLAITA_DLQ_KEY` | `<queue>:dlq` | 死信 Stream（`task_queue.py:30,166-167`） |
| `PLAITA_DLQ_MAX_LEN` | 1000 | DLQ XTRIM 保留条数（`task_queue.py:43,48-56`） |
| `--read-block-ms` | 1000（下限 100，:4462） | XREADGROUP BLOCK 上限；消费循环内实际再切成 ≤1s（:6402） |
| `--concurrency` / `PLAITA_WORKER_CONCURRENCY` | 1 | 并发消费线程数（:7281-7285） |
| `--enable-registry` / `--no-registry` | 启用 | 服务注册（:7272-7275） |
| `--registry-ttl` | 30s | 注册键 TTL（`server/registry.py:124` DEFAULT_TTL） |
| `--heartbeat-interval` | 10s | 注册表心跳间隔（`registry.py:126`） |
| `--cache-size` / `--cache-ttl` | 100 / 300s | flow 定义 TTLCache（`flow_worker.py:1362-1365`） |
| `PLAITA_WORKER_DRAIN_TIMEOUT` | 30.0s | drain 有界等待上界（`flow_worker.py:735,752-763`） |
| `PLAITA_METRICS_PORT` / `PLAITA_METRICS_HOST` | 0=不启 / 0.0.0.0 | `/metrics` 抓取端（:396-409） |
| `PLAITA_ALERT_WEBHOOK` | 无 | 死信告警 webhook（:412-413） |
| `PLAITA_WORKER_MIN_FREE_DISK_GIB` | 0=关 | 领取前盘预检守线 GiB（:518-532）；`PLAITA_WORKER_DISK_GUARD_PATH` 检查路径（:535-537）；空转重探间隔 15s（:739） |
| `PLAITA_WORKER_DENY_REPOS` | 空 | 按仓名拒跑名单，逗号分隔（:507-515） |
| `PLAITA_TASK_MAX_AGE_SECONDS` | 30 天（2592000） | 消息最大年龄闸，≤0 关闭（:282-298） |
| `PLAITA_TENANT_QUOTA_DEFER_MAX` | 64 | 超配额 start 延迟重投上限（:1438,5914-5921） |
| `PLAITA_TENANT_MAX_RUNNING` / `PLAITA_TENANT_RUNNING_TTL_SECONDS` | 不限 / 86400 | 租户 start 配额（`server/tenant_quota.py:48-51`） |
| `PLAITA_CAPABILITY_STALE_SECONDS`（实为 `PLAITA_CAPACITY_STALE_SECONDS`） | 90s | 容量快照新鲜期（`server/capacity.py:70`） |

**回滚开关**（`=1` 生效，`flow_worker.py:130-181`）：`PLAITA_DISABLE_CANCEL_CHECKPOINT`（步界取消检查）、`PLAITA_DISABLE_CANCEL_INTERRUPT`（步内取消监听线程）、`PLAITA_DISABLE_FENCING`（世代 CAS）、`PLAITA_DISABLE_LEASE_WATCHDOG`（续租线程）、`PLAITA_DISABLE_HOLDER_LIVENESS`（租约冲突存活核算）、`PLAITA_DISABLE_AFFINITY`（路径亲和闸）、`PLAITA_DISABLE_DISPATCH_TARGET`（显式派发目标闸）、`PLAITA_DISABLE_NODE_RETRY`（节点级重试）、`PLAITA_DISABLE_TENANT_QUOTA`、`PLAITA_DISABLE_EVENT_RECONCILE`（`event_reconcile.py:18`）。另有 `PLAITA_EXECUTION_EVENTS_PUBLISH=0`（SSE 实时发布关，`server/execution_timeline.py:70-75`）、`PLAITA_SANDBOX_LIFECYCLE=0`（沙箱回收关，`flow_worker.py:2275`）。

### 1.3 装配链（`main()` → worker）

1. `apply_writefile_jail("flow-worker")`（:7318）。
2. 可选 Langfuse 回调（`PLAITA_WORKER_LANGFUSE=1`，:7320-7330）。
3. 外部节点模块：`PLAITA_NODE_PATH`（冒号分隔追加 sys.path）+ `PLAITA_NODE_MODULES`（逗号分隔，逐个 import 后调 `register_all()`/`register()`；失败 `SystemExit`，:7332-7349）。
4. CodeNode 注册：`PLAITA_CODE_BACKEND`（默认 `subprocess`，:570-573）；后端不在白名单 / Docker 不可用 → `CodeNodeRegistrationError` 具名拒绝停止启动（fail-closed，:610-701；显式降级需同时设 `PLAITA_ALLOW_SUBPROCESS_FALLBACK=1` 且 `PLAITA_SANDBOX_ALLOWED_BACKENDS` 含 subprocess）。
5. 存储：`create_storage_component(type, "execution"/"flow", tenant_routing=True)`（:7377-7392）→ `TenantRoutingExecutionStorage` / `TenantRoutingFlowStorage`（`server/factory.py:1905-1926`；execution 侧内层是 `FencedExecutionStorage(RedisExecutionStorage)`，`server/tenant_context.py:80-97`）。事件总线 `create_event_bus("redis")`（:7394-7401，`factory.py:1950-1973`）。
6. `RedisFlowWorker.__init__`（:4412-4591）：建队列参数、租户路由租约 `TenantRoutingExecutionLease(redis_client)`（:4454）、注册表 metadata（`queue_name` + 掩码后的 `redis_url`/host/port/db + cache 参数，:4552-4567；掩码规则 `flow_worker.py:416-449`）、控制监听、容量预上报、Redis 日志 handler（`server/log_handler.py` setup_redis_logging，:4584-4591）。
7. 信号：SIGINT/SIGTERM → `request_drain()`（只置位不退出，:7192-7204,7436-7440）。

---

## 2. 队列协议

### 2.1 键与组

| 项 | 值 | 行号 |
|---|---|---|
| 任务 Stream | `queue_name`（默认 `plaita:flow:queue`），**平台机制键，不按租户分区**（多租户共用单 Stream，租户在消息体内） | `task_queue.py:1`、`plaita/tenant_context.py:19` |
| 消费组 | `plaita-workers`；`XGROUP CREATE ... id=0 mkstream=True`，`BUSYGROUP` 幂等；瞬时故障 warning 后下轮重试，非瞬时（NOAUTH 等）上抛 | `task_queue.py:26,458-479` |
| consumer 名 | instance_id 或 `worker-<pid>`（多线程**共享同名 consumer**） | `flow_worker.py:4593-4598,6276-6296` |
| DLQ | `<queue>:dlq`，纯记录流（无组/PEL）；每次死信后 `XTRIM maxlen=dlq_max_len approximate=False` | `task_queue.py:30,166-167,792-823` |
| 派发台账 | `plaita:dispatch:ledger`（env `PLAITA_DISPATCH_LEDGER_KEY`），只增 Stream，`XLEN(台账)==XLEN(队列)` 自检 | `server/dispatch_ledger.py:46-47,385-414` |

### 2.2 信封（Stream 字段）与 payload schema

`enqueue_task`（`task_queue.py:139-163`）每条消息两个 Stream 字段：

- `payload`：任务体 JSON（`ensure_ascii=False`）；
- `schema_version`：字符串化的信封版本。当前写版本 `TASK_SCHEMA_VERSION=1`（:35）；**缺失 = v1**（`LEGACY_TASK_SCHEMA_VERSION=1`，:36-39,107-124）；解析失败（非法值）也回退 v1 + WARNING（:117-124）。语义：字段只增不改语义不 bump；改了同名字段含义/必需性必须 bump，让旧消费者**拒收进 DLQ** 而不是猜着跑（:32-39 注释）。
- 消费者侧拒收：`schema_version > max_schema_version(=本 build 写版本)` → `_reject_unsupported_schema` → 守卫后死信（首投 `task_queue.py:546-552`、回收路径 `:1187-1190`，**两处必须同判**，:666-690）。
- **重投/交接/克隆副本必须继承源消息版本**（A44-02）：`_requeue_envelope_kwargs`（`flow_worker.py:230-244`）。

### 2.3 任务体（payload JSON）字段表

按生产方（console）与 worker 消费侧（`flow_worker.py:5974-6101` `_dispatch_task`）逐字段核实：

| 字段 | 类型 | 出现载体 | 行号 |
|---|---|---|---|
| `type` | `"start"` \| `"resume"` | 全部 | console `backend/api/executions.py:1254-1262`（start）、:1356-1364（cancel resume）、worker `flow_worker.py:6072-6099`（分发；未知 type → `ValueError` poison ack） |
| `flow_id` | str | 全部 | :6073-6090 |
| `execution_id` | str（uuid4 hex） | 全部（console 提交时预铸；worker 侧旧消息缺省时 `start_flow` 就地铸造） | `executions.py:1252`、`flow_worker.py:2898-2901` |
| `version` | str?（可 None=latest） | start | `executions.py:1257`、`flow_worker.py:6076` |
| `params` | dict? | start（resume 转换载体也带） | `executions.py:1258`、`flow_worker.py:6096`（#122：仅 checkpoint 为空时生效） |
| `tenant_id` | str | 全部（缺省按 default fail-open 执行，但**删除/清理类操作缺字段必跳过**） | `task_queue.py:170-194`、`flow_worker.py:6069` |
| `timestamp` | ISO 串（生产方入队时刻，**本机墙钟无时区**） | console 写；worker 用作 `enqueued_at`（排队时长）与消息年龄闸 | `executions.py:1261`、`flow_worker.py:301-310,6082` |
| `dedup_key` | str? | start 可选（幂等键） | `executions.py:1266-1267`、`flow_worker.py:6078` |
| `resume_type` | `"continue"` \| `"retry"` \| `"cancel"` \| `"timeout"` \| `"event"` | resume | `core/errors.py:69-83`（ResumeType 枚举）、`flow_worker.py:6090` |
| `data` | dict? | resume（event/timeout 携带 `{event_id,event_type,event_data,subscription_id}` 或 `{subscription_id,event_type,node_id}`；cancel 为 null） | `server/event_filter.py:227-240,338-353`、`executions.py:1362` |
| `target_instance_id` | str? | 按容量路由定向派发时（派发侧写） | `server/capacity.py:106`、`flow_worker.py:6062-6065` |
| `quota_defers` / `quota_bypass` | int / bool | worker 延迟重投副本自加 | `flow_worker.py:5943-5950` |
| （派发台账同步写）`decision`/`target_instance_id` 等 | — | 台账条目与消息体由同一份输入派生 | `dispatch_ledger.py:417-479` |

EventFilter / console / delay 服务等生产方写 resume 载体时**不带** `schema_version` 参数 → 落当前 build 版本（`enqueue_task` 缺省语义，`task_queue.py:154-155`）。

### 2.4 读取循环（`RedisStreamTaskQueue.read`，`task_queue.py:488-561`）

每轮 `_read_once`：

1. `ensure_group()`（幂等建组）；
2. `_reclaim_one()`（:1080-1204）——**先回收后读新**：
   - `XPENDINGRANGE` 从 `_reclaim_cursor` 起扫 `reclaim_window=8` 条（:392-394,1106-1112；#196 游标轮转防窗口占死；尾部回绕 :1201-1203）；
   - `idle < claim_min_idle_ms` 跳过（:1137）；
   - **XCLAIM 前活跃租约闸** `reclaim_lease_guard`（#47）：先 XRANGE 无副作用读消息体，租约活跃 → 跳过（不 XCLAIM 不计数），探测故障保守跳过（:1052-1078,1141-1145）；
   - `XCLAIM`（min_idle_time=claim_min_idle_ms，单条）→ `delivery_count = XCLAIM 前的 times_delivered`（**不是 +1**；净值恰为「max_deliveries 次处理后死信」，:1157-1169 注释）；
   - 解析失败 → `_dead_letter_invalid`（DLQ+ack，:1170-1182）；超版本 → 同首投拒收（:1187-1190）；`delivery_count >= max_deliveries` → 守卫后死信（:1191-1198）；
3. `XREADGROUP ... COUNT 1 BLOCK block_ms STREAMS key >`（:524-530）；BLOCK 到期的 `RedisTimeoutError` 返回 None（:531-535）；首投 `delivery_count` 恒 1（:553-561）；
4. 畸形 payload（非 JSON/非对象）在解析点就地 DLQ+ack（`InvalidTaskPayload`，:90-104,536-545,882-927；DLQ 信封 payload 截断 500 字符 + `raw_fields` + `truncated`，:83,905-927）。

**ack**（`task_queue.py:563-582`）：`XACK` 成功后 best-effort `XDEL` 该条目（本仓每 Stream 只挂一个组；XDEL 失败不影响 ack 语义）。残留兜底 `sweep_acked_residue`（:593-657）：启动一次 + 每 300s（`flow_worker.py:711,6300-6324`），只删「id ≤ 组 last-delivered-id 且不在 PEL」的条目。

**DLQ 信封 schema**（`dead_letter`，`task_queue.py:764-787`）：

```python
{
  "reason": str, "source_stream": str, "source_id": <原 message_id>,
  "delivery_count": int, "dead_lettered_at": float(epoch),
  "schema_version": int, "payload": <任务体 dict 或原始串>,
  # 畸形消息另带: "raw_fields": {...截断500字符}, "truncated": bool
}
```
死信顺序：`XADD → XTRIM → 计数/日志/on_dead_letter 钩子（best-effort）→ XACK`（:788-870；ack 失败具名告警后上抛 :855-869）。人工回灌唯一入口 `dlq_replay`（幂等键 `plaita:dlq:replay:<key>:<id>` SET NX 7d、审计流 `plaita:dlq:replay:audit`，`server/dlq_replay.py:1-58`）。

**瞬时故障族**（`task_queue.py:16-22,496-510`）：`ConnectionError + TimeoutError + socket.timeout` 一律 warning + 退避 1s（`RECONNECT_BACKOFF_SECONDS`，:59）返回 None 继续轮询；消费循环外层再兜 `(RedisError, socket.timeout)`（`flow_worker.py:6614-6627`，绝不让 Redis 故障炸出主循环）。

---

## 3. 租约协议

### 3.1 键命名（租户路由）

| 键 | 格式 | 行号 |
|---|---|---|
| 租约键 | `{ns}:execution:lease:{execution_id}`；ns = `plaita`（default/空）或 `plaita:{tenant_id}` | `execution_lease.py:26`（DEFAULT_KEY_PREFIX）、`server/tenant_context.py:140-159`、`storage/redis.py:96-98` |
| fence 键（当前世代） | `{ns}:execution:fence:{id}`，值=世代号字符串，TTL 7 天 | `execution_lease.py:30,167-172`、`storage/redis.py:43-54` |
| 世代计数器 | `{ns}:execution:fencegen:{id}`，**永不清除、无 TTL**（ABA 防护单调锚；删除执行刻意不删它） | `execution_lease.py:9-13,173-180`、`storage/redis.py:57-69` |

`RedisExecutionLease.__init__` 前缀推导：`key_prefix` 以 `lease:` 结尾时 fence 前缀 = 去掉 `lease:` 换 `fence:`；fencegen 同理（`execution_lease.py:167-180`）。

### 3.2 三个 Lua 脚本（逐字）

**fenced acquire**（`execution_lease.py:68-80`；KEYS[1]=lease key, KEYS[2]=fence key, KEYS[3]=fencegen key；ARGV[1]=holder, ARGV[2]=ttl 秒, ARGV[3]=fence TTL 秒；返回世代号（>0）或 0=已被持有）：

```lua
if redis.call('get', KEYS[1]) then
  return 0
end
local floor = tonumber(redis.call('get', KEYS[2])) or 0
local gen = redis.call('incr', KEYS[3])
if gen <= floor then
  gen = redis.call('incrby', KEYS[3], floor - gen + 1)
end
redis.call('set', KEYS[2], tostring(gen), 'EX', ARGV[3])
redis.call('set', KEYS[1], ARGV[1] .. ':' .. tostring(gen), 'EX', ARGV[2])
return gen
```

（GET 判空+SET 在同一脚本内原子 ⇒ 等价 SET NX；租约值变为 `{holder}:{gen}`。floor 逻辑处理升级瞬间「fence 键有值而计数器缺失」。注释 :50-66。）

**release**（:33-39；对**整个 value 串** compare-and-del）：

```lua
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
else
  return 0
end
```

**renew**（:42-48）：

```lua
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('expire', KEYS[1], ARGV[2])
else
  return 0
end
```

### 3.3 holder 格式与反解

- `new_holder_token(prefix, instance_id)`（`execution_lease.py:252-266`）：新格式 `{prefix}:{instance_id}:{uuid4hex[:16]}`（嵌入注册表实例 id 供存活核算）；缺省 `{prefix}:{uuid4hex[:16]}`。末段**定长 16 位 hex**（:92-99，#155：与旧格式+世代号的区分依据）。worker 用 prefix：`start`（`flow_worker.py:3065-3067`）、`resume`（:3478-3480）、`dispatch`（:4297-4299）。
- `holder_instance_id(lease_value)`（:102-126）：3/4 段且首段 ∈ (`start`,`resume`,`worker`) 才可反解；3 段且末段为**非 16 位**纯数字 = 旧格式+世代 → None。
- TTL：默认 120s（:25）；fence 键 TTL 7 天 = `FENCE_KEY_TTL_SECONDS = 7*86400`（:30）。普通档 `try_acquire` = `SET key holder NX EX ttl`（:191-194）。`get_holder` 只读探查（:231-233）。
- **`PLAITA_DISABLE_FENCING=1` 或 lease 无 fenced 方法** → 退普通 SET NX、fence_token=None（`flow_worker.py:1979-1999`）。

### 3.4 worker 侧使用时序（关键：acquire 在落 running 行之前）

- **start**：`_cancel_requested` 检查 → `new_holder_token("start")` → `_acquire_lease`（失败抛 `ExecutionLeaseError`）→ `set_current_fence_token` → 落 running 行 → `run_distributed` → finally 释放（`flow_worker.py:3039-3131`）。acquire 必须在落行**前**（:3051-3063 注释列了三个后果）。
- **resume**：前置短路（租约前）→ 取定义/指纹 → acquire → fence token → **租约内重读状态并重跑前置判定**（TOCTOU，plaita#241，:3493-3513）→ run_distributed → finally 无条件 release（:3871-3877）。
- **看门狗续租**（波次②）：`_lease_watchdog_loop` 每 `max(1, ttl/3)`（默认 40s）对登记在册执行 `renew`（`flow_worker.py:4907-4922,5004-5080`）。renew 失败（Lua compare 不符）→ 先发一次进度心跳 → 标记 `_lease_lost` + 摘出登记 + 调 `execution.cancel()` 中止在途步 + 转入 `_lease_orphan_watch`（只发心跳不续租，:5047-5079）；Redis 瞬断（renew 抛异常）不判死（:5032-5036）。
- **步界兜底**：`_renew_lease_if_held`（:2039-2047）每步推进前 renew，失败/已判失租抛 `ExecutionLeaseError`；终态/步进落盘前 `_raise_if_lease_lost`（:2049 区段多处调用，:3949,3957,3993,4048,4128）。
- **失租的写侧防线**：`FencedExecutionStorage.save_execution_state` 单段 Lua CAS（fence 键值 == ContextVar 世代才写；不符抛 `ExecutionLeaseError`），并同步 ZADD 索引（`storage/fenced.py:56-69,142-201`）；fenced delete 同构（:85-104,211-283，附带写平台根墓碑）。未设世代（start 无租约期间/回滚模式）退普通写（:142-153）。
- **释放 ≠ 执行已结束**：start 路径 finally 只有「正常跑完」或「执行已停止推进（`cancel_requested` 为真）」才真正 release；否则转入 `_register_lease_orphan` 继续观测（:3105-3131,5082-5128）。resume 路径 finally 无条件 release（:3877）。

### 3.5 租约冲突时的消息处置（`ExecutionLeaseError` 分支，`flow_worker.py:6456-6516`）

- ack 前先过 `_lease_conflict_ack_safe`（:5312-5408）：
  1. `resume_type ∈ {event, timeout, cancel}` 的**决议载体无条件不 ack**（A47-F2，:5346-5360）——它们是挂起执行的唯一唤醒来源；
  2. 从租约值反解 holder instance id → 查注册键 `plaita:registry:flow_worker:{instance}`（:5374-5384）；不在册（持有者已死、TTL 尾巴）→ 不 ack；
  3. 在册但注册 JSON `status != "running"`（draining/stopping/starting）→ 不 ack（:5392-5399,5410-5434）；
  4. 其余（活持有者）→ ack 释放，避免对端 60s 一轮 XCLAIM 烧 delivery；若是 resume 载体打显式「本次 resume 未生效」WARNING（:6491-6507）。
- 不 ack 的消息留 PEL，等租约过期（≤TTL）后 reclaim 接管。

---

## 4. 取消协议

### 4.1 控制面写入（console，`backend/api/executions.py`）

- **取消标志键**：`{ns}:execution:cancel:{id}`，值=取消时刻 ISO 串，TTL 7 天（`executions.py:322-334`；worker 侧同规则 `flow_worker.py:1848-1852`）。意图与状态分离（选型 B）：对 running 执行**只写标志键**，不直写 status=cancelled（:1380-1389）。
- **取消消息**：同时入队一条 `resume_type=cancel` 载体（:1354-1365）——worker 全灭后的死人开关，留 pending 待 XCLAIM 后由 resume 入口检查点终态化。
- **suspended 执行**：直接写状态行 cancelled（带终态 TTL）+ cancel 消息（:1367-1379）。
- 终态幂等：已终态不取消（:1344-1352）。

### 4.2 worker 侧轮询与节点边界语义

三个检查层次（全部**软中断**，默认让在途节点跑完；code 沙箱类由引擎 cancel_event 即时 killpg，:3986-3991 注释）：

1. **入口检查点**：`start_flow` 落行前（:3034-3049）与 `resume_flow` 租约内（:3568-3581）`_cancel_requested` → 终态化 `cancelled`（`_persist_state_or_raise(id, state, "cancelled_at_start_entry"/"cancelled_at_resume_entry")`），返回 `{"status": "cancelled", "cancelled_at_start/resume": True}`。
2. **步界检查点**：`_process_execution_result` 每步推进前（:3985-4000）——标志键命中即 `cancelled`，context=取消点前一步 checkpoint。
3. **步内中断（波次③）**：`_start_cancel_watcher` 起守护线程 `plaita-cancel-watcher`，默认每 1s（`_cancel_poll_interval`，:1898-1901）对全部登记执行轮询标志键（:1936-1975）；命中即 `execution.cancel()` 置位双 Event → 引擎在**下一个节点入口**抛 `FlowCancelledException`，协作节点当场收手（:1876-1887 注释）。回滚开关 `PLAITA_DISABLE_CANCEL_INTERRUPT=1` 回到纯步界。
4. **异常归一**：任何异常链含 `FlowCancelledException`（`_chain_has_cancellation`，:901-919，深度 ≤6）**或**标志键命中 → 终态化 `cancelled`，绝不写 error（:3155-3172,3789-3803,4116-4138）。取消优先于一切其他判定（:909-911）。
5. **suspended + 取消**：挂起幂等短路被取消意图压过（:3385-3399，台账第三十四轮 ④）——有标志时 normal path 进租约终态化。

### 4.3 写回

取消终态统一走 `_persist_state_or_raise(..., "cancelled*")`：`state.status="cancelled"`、`state.end_time=now ISO`、context=取消点 checkpoint；释放租约（finally）；终态释放租户名额（:2139-2155）；沙箱 pause 保现场（:2264，`keep = status != "completed"`）。**取消标志键本身不被 worker 删除**（自然 7 天过期；未查实有任何 worker 侧清理路径）。

---

## 5. 执行与结果写回

### 5.1 状态字段全集（`ExecutionState`，`storage/base.py:41-136`）

| 字段 | 类型/语义 |
|---|---|
| `execution_id` | str，必填 |
| `flow_id` / `flow_name` / `flow_version` | str? |
| `flow_hash` / `flow_hash_algo` | 定义指纹 sha256 + 算法标记（v2=`flow-definition-json-sortkeys-v2`，`flow_worker.py:724-727`） |
| `engine_version` | 创建者版本（观测用，resume 不覆写） |
| `tenant_id` | str? |
| `context` | dict，checkpoint（`$INPUT/$NODE/$LAST_NODE/$FLOW_ID/$ENV…`） |
| `status` | `running` / `suspended` / `completed` / `error` / `cancelled`（终态集合 `TERMINAL_EXECUTION_STATUSES = {"completed","error","cancelled"}`，`storage/redis.py:199`） |
| `start_time` / `last_update_time` / `end_time` | ISO 串（本机墙钟无时区） |
| `error` | dict?（结构见 5.4） |
| `invoker` | `"worker"`（start 落行时） |
| `node_timings` | dict?（node_id → `{started_at, ended_at, started_ms, ended_ms, duration_ms, total_duration_ms, attempts, failed}`；在跑节点 `ended_at=""`，`server/node_timings.py:115-147`） |
| `timeline` | dict?（`{events:[{seq,type,at,at_ms,...}], dropped, max_events, enqueued_at, queue_wait_ms}`，type ∈ enqueued\|run_start\|node_start\|node_end\|run_end；`server/execution_timeline.py:299-311`） |
| `usage` | dict?（终态聚合 `{prompt_tokens, completion_tokens, total_tokens, llm_calls, models[], source, precision}`，`flow_worker.py:2348-2383`） |
| `dispatch_pending` | dict?（挂起派发凭据 `{pending:true, service_type, service_config, node_id, recorded_at}`，:4208-4229） |

### 5.2 状态机与各写回点（全部经 `_persist_state_or_raise`，`flow_worker.py:2082-2155`）

```
（start）running ──┬─ completed（is_end）          :3946-3955
                   ├─ suspended（is_suspend）      :3956-3984
                   ├─ cancelled（步界/入口/异常链）:3985-4000,3039-3049,3568-3581
                   ├─ error（节点失败终态化）      :4149-4167,3842-3870
                   └─ running（step_persist 每步） :4043-4053（PERSIST_EVERY_N_STEPS=1）
（G1）error ──retry──▶ running（先落盘再清预算）   :3700-3710
```

- 落盘收口动作（锁内，:2119-2127）：合并 node_timings → 收口 timeline（终态补 `run_end(final=True)`、失败尝试补 `run_end(final=False)`，:2310-2346）→ 聚合 usage（仅终态、不覆写）→ 释放沙箱 → `save_execution_state`。**False → `StatePersistError`（不 ack 等重投）**；不可序列化 → 存储层抛 `StateSerializationError`（永久失败，转净化/最小 error 终态，:4057-4110）。
- **Redis 写形**：`{ns}:execution:{id}` = state JSON（`model_dump` 后 `json.dumps`），普通路径 pipeline `SET(+终态 EX 30天，`PLAITA_EXECUTION_STATE_TTL_DAYS` 可调) + ZADD {ns}:execution:index score=start_time`（`storage/redis.py:416-461,196-219`）；fenced 路径单段 Lua CAS（3.4 节）。
- **读侧契约**：键不存在 → None；瞬断/损坏 → 抛 `ExecutionStateLoadError`（不吞成 None，`storage/redis.py:232-244,463-485`）。flow 定义读侧同理 `FlowDefinitionLoadError`（:247-258,734-805）。
- **挂起服务派发**：suspended 落盘带 `dispatch_pending` 凭据后 `RPUSH plaita:{subtype}:queue`（任务 = service_config + `execution_id` + `tenant_id`），成功后清凭据（再落一次盘）；失败抛 `ServiceDispatchError` 不 ack（:3956-3984,4352-4398）。补派入口：suspended 幂等短路 / start dedup 命中（:4257-4350）。

### 5.3 结果返回形状（worker → 队列层/调用方 dict）

`start_flow`/`resume_flow` 返回 dict，消费循环只看**异常**决定 ack 与否；dict 形状举例：成功透传 `run_distributed` 的 result；终态短路 `{"execution_id","status","already_terminal":true,"result","error"}`（:3365-3371）；挂起短路 `{"status":"suspended","already_suspended":true,"dispatch_compensated"?}`（:3416-3423）；取消 `{"status":"cancelled", ...}`；幂等命中 `{"deduplicated":true,...}`；租约内复核拒绝 `rejected_after_lease:true`（:3503-3513）。

### 5.4 错误归一化结构（plaita#93，`flow_worker.py:986-1109`）

终态 `state.error` 统一为 dict：

```python
{
  "message": str,                    # 人读消息（存量字段，保留）
  "code": int,                       # 引擎错误码；节点级异常优先（NodeExecutionError/NodeTimeoutError），兜底 -500（_FLOW_ERROR_CODE，:1001）
  "error_type": str,                 # 枚举 value 或异常类名兜底；worker 级判据自定名（见下），绝不编造节点业务码
  "response"?: dict,                 # HTTP 节点脱敏截断摘要（链上第一份）
  "details"?: {...},                 # exc.details.redacted() 紧凑摘要（code/message/request{method,url,headers}/response{status,statusText,headers}，:1030-1060）
  "node_retries"?: int,              # 重试次数>0 时
  "failed_node"?: str,               # 失败节点 id（_node_id_from_chain）
}
```

固定 `error_type` 值（worker 级，非异常驱动路径）：`"flow_hash_mismatch"`（:3635，附 `stored_flow_hash/current_flow_hash/stored_flow_hash_algo/current_flow_hash_algo/upgrade_suspected/hint`）、`"node_retry_budget_exhausted"`（:4802，code=`DEFAULT_NODE_ABORT_CODE`）、`"AffinityHandoverExhausted"`（:5583，附 `affinity_handovers/affinity_reason`，注意此路径用的是 `"type"` 键）。序列化失败终态：`{"context_sanitized":true,"rule":...,"exception":...}`（:4071-4078）或最小 `{"message":...}`（:4099-4105）。

---

## 6. 事件持久化

### 6.1 事件 schema（`plaita/event/core.py:21-56`）

```python
Event: {event_id: uuid4, event_type: str, data: dict, timestamp: float(epoch, default=time.time),
        source: str = "", correlation_id: str?, tenant_id: str?}   # tenant_id 服务端绑定（bind_trusted_tenant，:41-56），绝不读 data
EventSubscription: {subscription_id, event_type(非空强制), filter_condition?, correlation_id?,
        flow_id?, node_id?, created_at, timeout?, tenant_id?, processed_events:set}
```

匹配语义：`matches_event`（:113-160）按 event_type 精确、correlation_id、flow/node（对 context `$FLOW_ID/$LAST_NODE`）、filter_condition（点路径嵌套遍历）。通配匹配 `EventBus.matches_event_type`（fnmatch，:296-324）。

### 6.2 产生点与去向

- **业务事件（挂起/恢复链）**：节点（EventNode 等）挂起时 `register_subscription` 写订阅（挂起点在 `core/strategies._subscribe_event`，本文件注释 `plaita/tenant_context.py:23-25` 交叉引用），事件由任一进程 `event_bus.publish` 发布。**Redis 后端双写**：`SETEX plaita:event:events:{event_id}`（ex=ttl）+ `ZADD plaita:event:types:{event_type} {event_id: timestamp}`（ex=ttl）+ `PUBLISH plaita:events:{event_type}`（`event/redis.py:705-719,1640-1645`）。默认 TTL 未查实具体值（构造参数 `ttl: Optional[int]`，:655；`event_reconcile.py:9` 注释称事件存储 TTL 7 天）。
- **消费方 = EventFilter**（独立进程）：Pub/Sub 推送（`_listen_for_events` 订阅 `plaita:events:{type}` / `plaita:events:*`，`event/redis.py:1873-1919`）→ `handle_event`（`server/event_filter.py:106-281`）：correlation_id=execution_id → 按事件受信租户加载状态（租户不一致丢弃，:142-149）→ 匹配订阅 → 终态 GC（:201-216）→ 去重键 `plaita:event_filter:dedup:{event_id}:{subscription_id}` SET NX EX 3600（:222-225）→ 入队 `resume_type=event` 载体（:227-262）→ `mark_event_processed`。入队失败**释放去重键**并上抛（:95-104,253-262）。
- **订阅超时**：`SubscriptionTimeoutChecker`（`event/timeout.py:53-140`，随 EventFilter 起，10s 轮询）→ `plaita:event_filter:timeout:{sub_id}` 去重（`event_filter.py:333-336`）→ 入队 `resume_type=timeout`。
- **兜底回扫 EventReconciler**（`server/event_reconcile.py:57-351`）：默认 300s 周期、1h 回填窗、每轮 200 条；复合游标 `plaita:event_reconcile:cursor`（`"<ts>|<event_id>"`，TTL 7 天，单调）；失败轮整轮作废不推进游标。
- **worker 侧执行事件时间线（区别于业务事件）**：`ExecutionTimelineCallback` 监听 `on_node_start/on_node_end` + run 级 `start_run/finish_run/finish_attempt` → ① 落盘进 `ExecutionState.timeline`（合并不覆盖，cap 500 条丢最旧计 dropped，`execution_timeline.py:65-70,345-386`）；② 实时 `PUBLISH plaita:execution:events:{execution_id}`（载荷 `{execution_id, **event}`，:389-394；worker 唯一发布者，`flow_worker.py:2199-2222`）。
- **节点耗时**：`NodeTimingCallback` → `ExecutionState.node_timings`（在跑节点 `ended_at=""` 是 keeper 活性判据的协议约定，`node_timings.py:115-133`）。

### 6.3 派发台账（dispatch_ledger）

- 条目 schema（`dispatch_ledger.py:103-163`）：`{ledger_schema_version:1, decided_at, decided_by, issue_id, queue, decision: targeted|broadcast, reason, target_instance_id?, enqueued, message_id?, candidate_count, allowed[], rejected[], stale[], tenant_id?, tenant_generation?, actor?}`。
- `enqueue_with_ledger`（:417-479）是「入队+记台账」唯一入口：target 写进消息体顶层 `target_instance_id` 与台账同源派生（结构上不可能分叉）；顺序 = 先 `enqueue_task` 成功才 `record_dispatch`（失败不阻塞派发，`ledger_recorded=false` 透出）。写形：`XADD plaita:dispatch:ledger {payload: JSON, schema_version: "1"}`（:180-194）。
- **对 worker 的意义**：消费侧只看消息体 `target_instance_id` 与本机 instance_id 比对（`capacity.target_mismatch`，`flow_worker.py:6062-6065`）；不一致抛 `TaskNotForThisWorker` 走交接。台账读侧对 Redis 故障返回空/None（:197-223）。

---

## 7. flow IR 来源解析

**结论：flow IR 不在消息 payload 里、不在 registry 里，来自 Redis 的 FlowStorage；权威库是 console 的 SQLite/PG，经 engine_sync 发布时同步进 Redis。**

完整链路：

1. **权威库**：console `flow_store`（SQLite/PG）保存 definition JSON（`backend/services/engine_sync.py:5` 模块 docstring）。
2. **发布同步**：`engine_sync.sync_flow_to_engine`（`engine_sync.py:53-74`）→ `RedisFlowStorage(client=redis, namespace=tenant_namespace(tenant)).save_flow(defn)`；defn 强制注入 `flow_id` 与 `version` 两键。启动时 `sync_all_published_to_engine` 回填全部 published 版本（:99-146）。
3. **Redis 形态**（`storage/redis.py:699-732`）：`{ns}:flow:{flow_id}:{version}` = 定义 JSON（`SET`，无 TTL）；`{ns}:flow_list`（SET of flow_id）；`{ns}:flow_versions:{flow_id}`（SET of version）。**未发布/未同步的 flow，worker 永远看不到**——跨机部署需在 worker 所在 Redis 侧跑回填脚本（`engine_sync.py:16-21` 边界声明）。
4. **worker 读取**：消息 `flow_id`(+`version`?) → `FlowWorker.get_flow_definition(flow_id, version)`（`flow_worker.py:2385-2441`）：
   - 缓存键 `{tenant}:{flow_id}:{version|latest}`，TTLCache(100, 300s)（:1362-1365）；
   - 未命中 → `flow_storage.get_flow`：version 缺省时优先 `latest` 版本，否则按数字版本排序取最大（`storage/redis.py:764-782`）；
   - `Flow.model_validate(dict)` 解析（:2428）——**IR 的 Go 侧对齐面 = 这份 camelCase 部署 JSON**（plaita-go AGENTS.md 决策 2 已拍板同口径）；
   - 同窗口缓存指纹 `compute_flow_definition_hash`（对 `model_validate` **之前**的原始定义 JSON `sort_keys + ensure_ascii=False + default=str + separators=(",",":")` 做 sha256，:810-835；算法标记 v2 :724）。
5. **resume**：`state.flow_version` 决定版本（:3458,3521-3529）；`flow_hash` 不匹配 → 终态化 error（5.4）；`data.allow_flow_hash_change=true` 显式放行（:3592-3617）。
6. **start 载体重投/预铸 id**：查盘有 checkpoint → 转 resume 语义（:2979-3013,2758-2814）；execution_id 已属另一 flow → `ExecutionIdConflictError` poison ack（:2795-2806）。

---

## 8. 心跳与僵尸判定口径

### 8.1 worker 侧心跳（三个层次）

| 心跳 | 间隔 | 写入 | 行号 |
|---|---|---|---|
| 注册表心跳 | 10s（TTL 30s） | `SETEX plaita:registry:flow_worker:{instance_id}` = ServiceInfo JSON `{instance_id, service_type, host, status(starting/running/draining/stopping/stopped), start_time, metadata{queue_name, redis_url(掩码), host/port/db, cache_*, worker_capacity}, active_tasks, last_heartbeat}` | `registry.py:122-126,187-251,351-387`；metadata `flow_worker.py:4552-4567` |
| 容量直推 | 在跑数变化即刷 + 盘闸迁移即刷 | `metadata.worker_capacity` = `{schema_version:1, free_disk_gib(null=探不到), min_free_disk_gib, disk_blocked, active_tasks, concurrency, sampled_at}` | `flow_worker.py:6630-6692`、`capacity.py:57,170-199` |
| 执行进度心跳 | 复用看门狗周期（TTL/3≈40s） | 长**节点执行期间**把 `node_timings` 当前快照（含在跑节点 `ended_at=""`）+ `last_update_time` 合并落盘（持每执行写锁 + fence CAS）；失败只告警 | `flow_worker.py:4924-5002` |

### 8.2 keeper/keeper 侧三判据（`issue_keeper/console_exec.py:234-335`）

对 running 执行 `worker_alive(detail)`（detail = 状态行 JSON + node_timings）：

1. **在跑节点**：`node_timings` 有 `started_at` 且无 `ended_at` 的节点 → 活（但**必须新鲜**：`last_update_time` 停更超 3600s（`_NODE_LIVENESS_STALE_SECS`，:231）则在跑节点不作活证据；started 超 86400s 同样不计，:276-286）。
2. **租约/心跳字段**：`lease_expires_at`/`lease_until`（或 `lease.expires_at` 等）未来时间 = 活；`worker_heartbeat`/`heartbeat` 年龄 ≤ lease_ttl(120s) = 活（:288-303；字段形态注释自称「今天未实测、属推测」）。
3. **停滞判死**：节点史最新 `ended_at` 停滞超 `node_stale_secs`（默认 1800s）→ 死（:305-313）。
4. 汇总 `zombie()`：alive=True 永不判死；alive=False 直接判死；None（无判据）才回退 `last_update_time`（缺省 `start_time`）年龄 > threshold（默认 2h，:317-335）。

**worker 的协议义务**：只要执行对象仍在推进（即使已失租/已 release），必须持续发进度心跳，否则健康 run 会在 1800s 后被误杀——这正是 `_lease_orphan_watch` 与 `_publish_orphan_progress` 存在的理由（`flow_worker.py:5047-5080,5190-5241`，含生产实证注释）。

### 8.3 相关判定键汇总

| 键 | 值 | 用途 | 行号 |
|---|---|---|---|
| `{ns}:execution:lease:{id}` | `{holder}` 或 `{holder}:{gen}` | 双跑仲裁 + keeper 判据②数据源（经 lease_expires_at 反推，worker 不直接写该字段——未查实有写入方） | `execution_lease.py:154-157` |
| `{ns}:execution:fence:{id}` / `fencegen:{id}` | 世代号 / 单调计数器 | CAS 写；fencegen 永不清 | `storage/redis.py:43-69` |
| `plaita:registry:flow_worker:{instance}` | ServiceInfo JSON，TTL 30s | 存活核算（`_lease_conflict_ack_safe`）、派发门、ready 探针 | `flow_worker.py:5374-5408`、`registry.py:122` |
| `{ns}:execution:cancel:{id}` | ISO 串，TTL 7d | 取消意图 | `executions.py:326-334` |
| `{ns}:execution:noderetry:{id}[:{node}]`、`noderefetch`、`nofail`、`g1wakeups`、`affinityhandover` | 裸整数，TTL 7d | 重试/交接预算；**删除执行时一并清**（防同 id 新执行继承旧预算） | `flow_worker.py:1379-1432,1601-1633`、`storage/redis.py:77-88,515-567` |
| `{ns}:start-dedup:{key}` | execution_id，TTL 7d | start 幂等 | `flow_worker.py:2492-2580` |
| `plaita:exec_tombstone:{id}` | `{tenant, deleted_at, reason}`，TTL 默认 30d，平台根 | 删除后拒绝重建 | `storage/redis.py:106-149`、`fenced.py:245-255` |
| `plaita:tenant_status`（HASH）、`plaita:tenant_quota`（HASH）、`plaita:tenant_running:{tenant}`（ZSET） | — | 租户闸/配额（平台键不分区） | `tenant_status.py:23`、`tenant_quota.py:46-80` |
| `plaita:event_filter:dedup:*` / `:timeout:*` | "1"，TTL 3600 | 事件/超时去重 | `event_filter.py:222-225,333-336` |
| `plaita:event_reconcile:cursor` | `<ts>\|<event_id>`，TTL 7d | 回扫游标 | `event_reconcile.py:60,292-324` |

---

## 9. 退避 / drain / 优雅退出

### 9.1 退避（节点级可重试失败）

- 判据 `_is_retryable_node_failure`（`flow_worker.py:859-898`）：异常链（`__cause__` 上溯 ≤5 层）含 `NodeExecutionError` 且不含超时/取消、深层非确定性错误（`core.runner.is_retryable_error` 单一来源）→ 可重试；超时/取消/协议类 → 不重试。
- 预算（三层，全部 Redis 计数键，**不用 delivery_count**——:1687-1713 注释解释了原因）：
  1. 单节点：`{ns}:execution:noderetry:{id}:{node}`，预算=队列 `max_deliveries`（默认 5），INCR+7d 滑动 EX（:1601-1713）；
  2. 兜底总上限：`{ns}:execution:noderefetch:{id}`，100 次（:1413-1414,1440-1500）；
  3. 确定性连续失败：`{ns}:execution:nofail:{id}`，12 次连续（任一节点成功推进即 DEL 清零；G1 唤醒不清，:1400-1401,1502-1553）。
- 放行 → `NodeExecutionRetryableError` → run() 捕获 → `_requeue_retryable_failure`（:5829-5912）：**指数退避** `min(30s, 1s * 2^(attempt-1))`（:190-215），分片 0.2s 睡以便强停打断（:5809-5827）→ start 载体查盘转换 resume（`_requeue_body_for_retry`，:313-389；墓碑命中 → ack 丢弃）→ **先入队、后 ack**。失败退化留 pending。
- 成功推进清零：`_reset_node_retry_counter(id, result.id)` + `_reset_deterministic_failure_counter` + `_reset_node_retry_total`（:4017-4038）。
- G1 retry 唤醒（error 态人工/自动救援）：唤醒预算 `{ns}:execution:g1wakeups:{id}` ≤2 次；唤醒 = error→running 落盘 + 清全部节点预算 + INCR 唤醒计数（:3700-3710,3324-3360）；无 checkpoint 的 retry 拒绝（ResumeProtocolError，:3683-3699）。

### 9.2 队列级重投与死信路径总表

| 异常 | 处置 | 行号 |
|---|---|---|
| `ValueError`（含 `FlowHashMismatchError`、`NodeFailureTerminalizedError`、`ExecutionIdConflictError`） | poison ack（畸形/已终态化，重投无意义） | :6565-6570 |
| `NodeExecutionRetryableError` | 退避后显式重投 + ack | :6517-6549 |
| `TaskNotForThisWorker` | 交接：计数键（`{ns}:execution:affinityhandover:{id}` 或 body sha1）INCR+7d；≤3 次 enqueue→ack（目标不可用剥 `target_instance_id` 广播重投）；计数不可用 fail-closed 留 pending；达上限终态化 error（已终态则 ack 消费） | :5672-5807,6431-6446 |
| `TenantQuotaDeferred` | 延迟：副本加 `quota_defers` 重投尾部 + ack；>64 次带 `quota_bypass` 放行 | :5923-5972,6447-6455 |
| `ResumeProtocolError` | ack 前状态感知（#258）：event/timeout 载体已错过挂起点且执行仍 running → 归一化 `continue` 重投（enqueue→ack）；否则原样 ack | :6103-6224,6550-6564 |
| `ExecutionLeaseError` | 3.5 节守卫判定 ack / 留 pending | :6456-6516 |
| 其他 Exception | 不 ack 留 pending；`delivery_count >= max_deliveries` 时经 `dead_letter_if_allowed`（守卫）死信 | :6571-6609 |
| 死信守卫 `_dead_letter_guard`（队列回调） | 状态缺失/终态 → 放行；读失败 → 跳过；非终态+租约在 → 跳过；墓碑 → 放行不克隆；schema_rejected → 放行不克隆；running 且预算/兜底计数达限 → 终态化 error 后放行；其余非终态+租约空 → **重入队恢复副本（继承源信封版本）后放行原消息死信** | :4633-4835 |

### 9.3 drain 与优雅退出（S18 语义，D1/D2/D3）

- **D1 draining**：`request_drain(reason)`（:6847-6888）——置 `_drain_event`、注册表 status=draining **立即落盘**（`flush_registry_status` 3 次短退避重试，`registry.py:619-640`）、起 `PLAITA_WORKER_DRAIN_TIMEOUT`（默认 30s）定时器。消费循环条件检查在任务边界（:6390）；read 后 post-read gate（:6405-6417）：drain 后才返回的消息不 dispatch/ack，留 PEL。
- **D2 强停**：定时器到 → `_force_stop_after_drain`（:6890-6919）——**无条件**置 `_running=False` + `_force_stop_event`（可中断退避当场放弃 enqueue/ack）+ best-effort cancel 在途执行（复用取消监听登记表，:6921-6959）。硬中断 B18-01 未实现。
- **D3 信号**：SIGINT/SIGTERM handler 只调 `request_drain`（:7192-7204）；XREADGROUP 分片 ≤1s 保证停机延迟 ≈1s（:6398-6402）。
- **`stop(wait=)`**（:6970-7052）：顺序 = draining → 有界等 in-flight（预算=drain timeout 剩余量）→ 停注册重试 → 停看门狗 → 停取消监听 → 停 metrics/告警 → 停控制监听 → 注册表 stopping 立即落盘 → `_mark_registry_closed`（迟到写入全 no-op）→ `unregister_service`（写 stopped 墓碑 SETEX 5s 再 DEL，`registry.py:257-349`）→ 关日志 handler。
- 未收尾步的消息留 pending，由其他 worker XCLAIM 从**步界检查点**续跑（该步重放，业务节点须幂等——文档级契约，:735-737,6857 注释）。

### 9.4 checkpoint v1.5（plaita-go M6 实现，2026-10-19）

本节记录 Go worker 的挂起恢复落地形态；语义基准是 `core/strategies.py` 的 `_execute_current_node` / `_handle_resume` 与 `core/state.py`。

**键名**（全部在 `{ns}=cfg.Namespace(tenant)` 下；影子运行只需 `PLAITA_KEY_PREFIX=plaita-go-shadow`）：

| 键 | 值 | 用途 |
|---|---|---|
| `{ns}:execution:checkpoint:{execution_id}` | `Checkpoint` JSON（`schema_version` / `execution_id` / `tenant_id` / `flow_id` / `suspended_node` / `next_node` / `resume_type` / `context` / `created_at`） | 挂起边界快照；无 TTL（人工审批可等任意久），终态写回后删除 |
| `{ns}:event:subscriptions:{subscription_id}` | 订阅 JSON（event_type/filter/correlation_id/flow_id/node_id/tenant_id/timeout_at） | 挂起节点的唤醒订阅，TTL = `EventTTLSeconds`（默认 7d） |
| `{ns}:event:subscriptions:type:{event_type}` | SET(subscription_id) | 事件分发按类型索引 |
| `{ns}:event:subscriptions:timeouts` | ZSET(sub key → timeout_at 秒) | 超时检查器索引 |
| `{ns}:event_filter:dedup:{event_id}:{sub_id}` | `"1"`，TTL 1h | 事件去重（对齐 `event_filter.py:222-225`） |
| `{ns}:event_filter:timeout:{sub_id}` | `"1"`，TTL 1h | 超时去重（对齐 `event_filter.py:333-336`） |

**语义**：

- **挂起**：节点实现 `workflow.Suspender`（`IsSuspending()`）；`Workflow.Execute` 在其返回 `status=pending` 后注册订阅、写快照、返回 `*SuspendError`。worker 写 `status=suspended` 行（无 `end_time`，context=快照），ack 载体并释放租约。订阅注册失败拒绝挂起（plaita「不得悬挂在无唤醒源的执行上」）。
- **快照**：`$INPUT/$NODE/$GLOBAL/$ENV/$ERROR/$CURRENT/$LAST_NODE/$PARENT` 深度物化；容器环以 `<recursive: $['a'][0]>` 截断，深度 > 10000 抛 `StateSnapshotError`；非字符串 map 键具名拒绝。
- **恢复**：`event`/`timeout` 载体在 `status=suspended` 上执行真恢复（`Workflow.Resume` → 节点 `Resume(resume_type, data)` → 从 `next` 继续）；`cancel` 仍走终态化 `cancelled`；`suspended + continue/retry` 保持 B2 的幂等短路。事件/超时载体必须绑定挂起时写入 `$NODE.<node>.subscription_id`（`event` 另比对 `event_type`），不匹配 → `ResumeGuardError`：**执行保持 suspended、消息留 pending**，绝不终态化（plaita#221）。
- **唤醒**：Redis `PUBLISH {ns}:events:{event_type}`（`EventPublisher` 双写）→ worker 事件桥（`PSUBSCRIBE`）→ 匹配租户/execution/filter → `resume_type=event` 载体；订阅超时索引 → `resume_type=timeout` 载体。核心字段对齐 `event_filter.py:227-262` 的 `data={event_id,event_type,event_data,subscription_id}`。
- **start 载体重投**：目标执行已 `suspended` 时幂等 ack（不重放流程）；真正推进只能靠决议载体。**偏差**：Python §7.6 的「start 查盘转 resume」未做（相位 3，需要每步 checkpoint）。
- **节点结果写回**：`$NODE` 现在存**节点结果表**（`runtime.ExecuteNodeResults`）；此前 worker 的终态 context 误写 `CurrentNodeContext`，本轮修正，`$CURRENT` 单独保留。恢复运行会把挂起行的 `node_timings`/`timeline` 续接到终态行（seq 单调）。
- **作用域**：仅根流程可挂起；child/loop/parallel 子流程内的挂起节点返回显式引擎错误（快照无法表达嵌套恢复位置）。

---

## 10. Go 移植注意

### 10.1 plaita-go 现状（@ 2026-10-19，M6 checkpoint v1.5 收口）

- **MVP（一期）已落地**：`pkg/worker`（Stream 消费 + reclaim + DLQ 信封/schema 拒收/残留 sweep、三段租约 Lua + fence CAS 写、ExecutionState 字段级写回 + timeline/node_timings、取消标志轮询、注册表心跳 + 容量块、flow 来源读路径 + v2 指纹）、`cmd/plaita-worker`（env 配置 + 影子前缀）。
- **二期（M4）已落地**：orphan watch（失租后仅进度心跳）、resume 载体语义（A47-F2：决议载体冲突窗口不 ack / 终态幂等 ack / suspended+continue 短路 + dispatch_pending 补偿 / event/timeout 归一化 continue）、dispatch_ledger（§6.3 schema + 入队唯一入口 + 读/对账）+ TaskNotForThisWorker 有界交接、DLQ replay（幂等标记 + 审计 + 确定性 id + schema 继承 + 墓碑）+ 死信守卫恢复克隆/预算终态化、drain 三态（D1/D2/D3）+ 信号优雅退出、节点级三层重试预算 + 退避重投。
- **M6 checkpoint v1.5 已落地**（见 §9.4）：挂起边界快照（环安全/深度上限）、event/hitl_await 挂起节点、event/timeout 真恢复、Redis Pub/Sub 唤醒 + 订阅超时检查器、start 载体命中 suspended 幂等 ack。
- **刻意偏差**：节点重投载体保持 start（同 execution_id 整条重放），不转 resume/continue（错误执行没有 checkpoint，需每步 checkpoint）；D2 中止的步不写终态、留 pending（无硬中断 B18-01）；仅根流程可挂起；事件 Pub/Sub 无回扫器（订阅超时/状态双写保证至少一次唤醒）。以上已在 AGENTS.md 对应章节逐条声明。

### 10.2 缺口清单（按本协议文档逐章）

状态列 = plaita-go 实现进度（2026-10-19 B2 收口）。

| 协议面 | 缺口 | 建议优先级 | Go 状态 |
|---|---|---|---|
| §2 队列 | XREADGROUP 消费组 + reclaim（游标轮转 + 租约闸）+ DLQ（信封 schema/裁剪/守卫）+ schema_version 拒收 + 残留 sweep | P0（影子对账的最小面） | ✅ 一期；守卫恢复克隆/预算终态化 = 二期 |
| §3 租约 | 三个 Lua 逐字移植 + holder token 格式（16 位 hex 末段是反解协议的一部分）+ fencegen 语义 + fenced CAS 存储 + 看门狗 | P0 | ✅ 一期；orphan watch = 二期 |
| §5 写回 | ExecutionState JSON 字段级兼容（Python `model_dump` 的 camel/snake 混合——`node_timings`/`timeline` 键名以本文 §5.1 为准，落地前用真实 Redis 快照对拍）；终态 TTL；索引 ZSET；StatePersistError/StateSerializationError 二分 | P0 | ✅；挂起边界 checkpoint + 恢复（M6 §9.4），每步持久化未做（三期） |
| §4 取消 | 取消标志键轮询（步界 + 步内两个层次）+ cancel resume 载体 + 终态写回 | P1（步界版先够对账） | ✅ 步内轮询 + 入口/租约内检查点 |
| §7 flow 来源 | RedisFlowStorage 读路径（`{ns}:flow:{id}:{ver}` + flow_list/flow_versions SET + latest/数字版本序）；指纹算法（sort_keys JSON sha256 + algo v2 标记）——指纹是 resume 硬门，Go 必须同口径 | P0（同 Redis 即可对跑） | ✅ 读/指纹；resume 指纹硬门（flow_hash_mismatch + allow_flow_hash_change 放行）已落 |
| §8 心跳 | 注册表键 + ServiceInfo JSON（status 生命周期、worker_capacity 块）+ 进度心跳（node_timings 在跑节点 `ended_at=""` 约定） | P1（keeper 误杀风险，随影子上线即需要） | ✅ 三层（含失租 orphan watch） |
| §6 事件 | Go worker 第一阶段可只读不写业务事件；`timeline`/`node_timings` 落盘必须有（keeper 判据）；SSE 发布可后置 | P2 | ✅ timeline/耗时落盘 + execution events PUBLISH；业务事件 PUBLISH/订阅/唤醒（M6 §9.4） |
| §9 退避/drain | 三层重试预算键 + 显式重投（先入队后 ack）+ resume 载体转换 + 信号处理 | P1 | ✅ 三层预算 + 退避重投 + drain 三态；重投载体仍为 start（无每步 checkpoint，三期转换） |
| §6.3 派发台账 | 台账 Stream + 入队唯一入口 + TaskNotForThisWorker 有界交接 | P1 | ✅ 二期 |
| 控制面 | stop/status Pub/Sub 命令（`plaita:control:{instance}`）、`/metrics` | P3 | ❌ 未做 |


### 10.3 建议 Go 包切分

```
runtime/queue/       StreamTask、RedisStreamTaskQueue（read/reclaim/ack/dead_letter/sweep）、信封 schema_version
runtime/lease/       RedisExecutionLease（三段 Lua 原文移植）、holder token、TenantRouting 前缀规则
runtime/state/       ExecutionState 模型 + Redis 读写 + Fenced CAS + 墓碑/索引/计数键
runtime/worker/      消费循环（异常→处置分支表 §9.2）、start/resume 处理器、看门狗、取消监听、drain/stop
runtime/registry/    ServiceRegistry（注册/心跳/注销墓碑/closed 世代闸）+ capacity 块
runtime/control/     stop/status 命令订阅
runtime/obs/         node_timings、timeline、SSE publish、/metrics
```

### 10.4 影子对账要点（从协议直接导出的可断言不变量）

1. **同一 Redis、同 queue 键混跑**：Go worker 必须尊重 `schema_version` 拒收语义（v1 现状下不会触发，但实现不得丢该分支）。
2. **ack/dlq/重投后** `XLEN(queue)`、`XPENDING`、`XLEN(DLQ)` 与 Python worker 基线一致（`queue.stats()` 形状，`task_queue.py:938-974`，可作为对账探针输出格式）。
3. **状态行 JSON diff**：同一 flow/params 在双引擎各跑一次，`{ns}:execution:{id}` 反序列化后 diff（忽略 `engine_version`/`invoker`；对齐 `node_timings`/`timeline` 的**键形**而非值）。
4. **租约互斥**：Go/Python 同时 resume 同一执行，恰有一方推进（另一方收到等价 `ExecutionLeaseError` 行为：不 ack、留 pending）。
5. **fencing**：旧世代写必须被拒（`ExecutionLeaseError` + 不 ack）。

---

## 附：未查实清单

| # | 事项 | 现有证据边界 |
|---|---|---|
| U1 | 事件存储 TTL 具体值 | `RedisEventStorage.__init__` 的 `ttl: Optional[int]`（`event/redis.py:655`）；`event_reconcile.py:9` 注释称 7 天，未找到生产构造点取值（console/部署脚本未读） |
| U2 | keeper 判据②的 `lease_expires_at`/`worker_heartbeat` 字段由谁写入 | `console_exec.py:288-289` 注释自称「字段形态今天未实测、属推测」；未查实任何 worker/console 写入点。Go 侧不要实现该字段，以判据①③+last_update_time 兜底为准 |
| U3 | `plaita:dlq:replay` 的 console/CLI 暴露面 | `dlq_replay.py` 模块本身已核实，其调用入口（console API 路由）未逐点核对 |
| U4 | resume_type=cancel 在 `resume_flow` 内的专属分支 | cancel 属终态短路放行集合外的决议载体（`_DECISION_RESUME_TYPES`，`flow_worker.py:165-167`）；其推进语义（on_cancel 回调→END）在 `core/strategies.py`，本轮未读该文件，只核实了入口终态短路与 console 写形 |
| U5 | `services/`（`plaita/server/services/`）目录内是否有 worker 相关 daemon | 未读（delay/approval 服务等）；对 worker 协议的影响限于 `plaita:{subtype}:queue` 消费侧 |
| U6 | `PLAITA_HOME` 在 worker 进程内的使用 | 本轮在 flow_worker.py 未发现引用（agentproc/沙箱节点侧使用）；worker 装配面无该 env。如 Go 侧需要 agents.json/providers.json 共享，按 plaita-go AGENTS.md 的 `~/.plaita` 约定，**不是**本协议的一部分 |
| U7 | 多消费线程下 `_dispatch_task` 租户 ContextVar 的 goroutine 传播 | Python 用 ContextVar（线程安全）；Go 需显式传参替代，无对应代码可直接照抄 |
