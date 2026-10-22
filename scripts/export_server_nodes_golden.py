#!/usr/bin/env python3
"""Export #80 phase-3 server-node golden cases from REAL plaita runs (r79).

用法（与 export_node_parity2_golden.py 同框架）：
    cd ~/projects/infra4agent/plaita && python3 ~/projects/plaita-go/scripts/export_server_nodes_golden.py

覆盖五类 server 扩展节点（plaita/server/nodes/）：delay / approval /
redis_queue / kafka_queue / http_callback。每例记录三段：
  - flow/input：deployment 形态 IR；
  - resume：恢复事件载荷（delay/approval/http_callback 由**真实 service** 的
    handle_task/submit_approval_decision/handle_callback_request 发布、经
    InMemoryEventBus 捕获；redis_queue/kafka_queue 的 service 依赖真实
    broker，载荷按其 service 源码的事件构造分支手建（形状逐字段同源））；
  - expected.suspend：挂起态（规范化后——易变键剔除 + 32hex/13位毫秒打码）；
  - expected.output：恢复后的**节点状态**（r2["result"]）。

对拍口径：plaita 的分布式 _handle_resume 只处理被恢复节点本身（返回
lazy output 即止，不续跑后继图——续跑由 worker 的下一跳载体驱动）；Go 引擎
的 Workflow.Resume 是「恢复+续跑」一步到位。两侧共有的可比较面 = 恢复后
节点状态，Go 测试用 end 节点 output=$NODE.<server_node> 取同物。

规范化（Go 测试侧 server_nodes_golden_test.go 实现同款）：
  - 剔键：subscription_id/subscription_tenant/created_time/trigger_timestamp/
    actual_trigger_timestamp/planned_trigger_timestamp/timestamp/recorded_at；
  - 串内打码：32 位十六进制（uuid hex/execution_id）→ <hex32>，13 位毫秒
    时间戳（event_id/approval_id 尾缀）→ <ms13>。

订阅绑定：恢复载荷由 Go 测试注入各自执行现场的 subscription_id
（RESUME_BINDING_KEY=__plaita_subscription_id，两侧准入校验同款），导出载荷
不含该键。
"""

from __future__ import annotations

import asyncio
import base64
import json
import re
import sys
from typing import Any, Dict, List, Optional

from plaita import ExecutionMode
from plaita.core.executor import FlowExecution
from plaita.event import InMemoryEventBus
from plaita.node import get_default_registry
from plaita.server.nodes import (
    ApprovalNode,
    DelayNode,
    HttpCallbackNode,
    KafkaQueueNode,
    RedisQueueNode,
)
from plaita.server.services import ApprovalService, DelayService, HttpCallbackService

CASES: List[Dict[str, Any]] = []

# 与 Go 侧同款的规范化规则（见模块 docstring）。
DENYLIST_KEYS = {
    "subscription_id",
    "subscription_tenant",
    "created_time",
    "trigger_timestamp",
    "actual_trigger_timestamp",
    "planned_trigger_timestamp",
    "timestamp",
    "recorded_at",
}
_HEX32 = re.compile(r"[0-9a-f]{32}")
_MS13 = re.compile(r"\d{13}")


def normalize(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: normalize(v) for k, v in value.items() if k not in DENYLIST_KEYS}
    if isinstance(value, list):
        return [normalize(v) for v in value]
    if isinstance(value, str):
        value = _HEX32.sub("<hex32>", value)
        value = _MS13.sub("<ms13>", value)
        return value
    return value


def _start(nid: str, nxt: str) -> Dict[str, Any]:
    return {"type": "start", "id": nid, "next": nxt}


def _end_whole_state(nid: str, server_node: str) -> Dict[str, Any]:
    """end 输出 = 恢复后节点状态全量（与 Python r2['result'] 同物，见 docstring）。"""
    return {
        "type": "end",
        "id": nid,
        "resultType": "success",
        "output": f"$NODE.{server_node}",
    }


def _case(name: str, nodes: List[Dict[str, Any]], input: Dict[str, Any],
          suspend_node: str, driver) -> None:
    CASES.append({
        "name": name,
        "flow": {
            "flow_id": name,
            "runtime": "python",
            "inputType": {"dataType": "any"},
            "nodes": nodes,
        },
        "input": input,
        "suspend_node": suspend_node,
        "driver": driver,
    })


def _load_reject(name: str, node: Dict[str, Any], error_contains: str) -> None:
    """加载期拒绝对拍：pydantic 模型层拒绝的 flow，Go 的 ir.Load 同拒。"""
    CASES.append({
        "name": name,
        "flow": {
            "flow_id": name,
            "runtime": "python",
            "inputType": {"dataType": "any"},
            "nodes": [_start("s", "n"), {**node, "id": node.get("id", "n"), "next": "e"},
                      _end_whole_state("e", "n")],
        },
        "input": {},
        "suspend_node": "n",
        "expect_load_error_contains": error_contains,
    })


# --- delay ----------------------------------------------------------------
# 服务事件形状 = delay_service.py handle_task 的 event_data 构造块。
def delay_event_data(service_config: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "node_id": service_config.get("node_id"),
        "execution_id": service_config.get("execution_id"),
        "flow_id": service_config.get("flow_id"),
        "tenant_id": service_config.get("tenant_id") or "default",
        "trigger_type": "delay_completed",
        "delay_ms": service_config.get("delay_ms"),
        "actual_trigger_timestamp": 1792660000000,
        "planned_trigger_timestamp": service_config.get("trigger_timestamp"),
        "success": True,
    }


def _delay_node(**fields: Any) -> Dict[str, Any]:
    return {"type": "delay", "id": "d", **fields}


def build_delay_flow(name: str, node_fields: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [_start("s", "d"), _delay_node(**{**node_fields, "next": "e"}),
            _end_whole_state("e", "d")]


_case("delay_normal_seconds",
      build_delay_flow("delay_normal_seconds", {"delay_seconds": 0.2}),
      {}, "d",
      {"kind": "delay_service"})  # 真实 DelayService.handle_task（200ms 等待）

_case("delay_unit_minutes",
      build_delay_flow("delay_unit_minutes", {"delay_seconds": 0.01, "delay_unit": "minutes"}),
      {}, "d",
      {"kind": "delay_service"})  # 0.01 分钟 = 600ms

_case("delay_custom_event_type",
      build_delay_flow("delay_custom_event_type",
                       {"delay_seconds": 0.2, "event_type": "my_trigger"}),
      {}, "d",
      {"kind": "delay_service"})

_case("delay_expr_fallback_60s",
      build_delay_flow("delay_expr_fallback_60s", {"delay_seconds": "$INPUT.missing_secs"}),
      {}, "d",
      # 解析失败回退 60 秒——真实服务会睡 60s，载荷按 handle_task 同形状手建。
      {"kind": "manual_delay"})

_case("delay_negative_ms",
      build_delay_flow("delay_negative_ms", {"delay_seconds": -5}),
      {}, "d",
      # 负延迟：节点层不拦（Python validate_service_config 是服务侧语义），
      # service_config.delay_ms = -5000 钉形状；载荷手建。
      {"kind": "manual_delay"})

_case("delay_expr_from_input",
      build_delay_flow("delay_expr_from_input", {"delay_seconds": "$INPUT.secs", "delay_unit": "hours"}),
      {"secs": 0.0002}, "d",
      {"kind": "delay_service"})  # 0.0002 小时 = 720ms

# 加载期拒绝对拍（pydantic 模型层）。
_load_reject("reject_delay_missing_seconds", {"type": "delay"}, "delay_seconds")
_load_reject("reject_delay_bad_unit",
             {"type": "delay", "delay_seconds": 5, "delay_unit": "MinutesX"}, "delay_unit")

# --- approval ---------------------------------------------------------------
_case("approval_any_approve",
      [_start("s", "a"),
       {"type": "approval", "id": "a", "next": "e",
        "approval_title": "资源申请", "approval_content": "CPU 4 核",
        "approvers": ["manager1", "manager2"], "approval_strategy": "any"},
       _end_whole_state("e", "a")],
      {}, "a",
      {"kind": "approval_service", "decisions": [("manager1", "approve")]})

_case("approval_all_needs_all",
      [_start("s", "a"),
       {"type": "approval", "id": "a", "next": "e",
        "approval_title": "上线审批", "approval_content": "全量发布",
        "approvers": ["m1", "m2"], "approval_strategy": "all"},
       _end_whole_state("e", "a")],
      {}, "a",
      # all 策略：第一个 approve 不完案（无事件），第二个才发布恢复事件。
      {"kind": "approval_service", "decisions": [("m1", "approve"), ("m2", "approve")]})

_case("approval_any_reject",
      [_start("s", "a"),
       {"type": "approval", "id": "a", "next": "e",
        "approval_title": "采购审批", "approval_content": "超预算采购",
        "approvers": ["m1", "m2"], "approval_strategy": "any"},
       _end_whole_state("e", "a")],
      {}, "a",
      {"kind": "approval_service", "decisions": [("m1", "reject")]})

_case("approval_majority",
      [_start("s", "a"),
       {"type": "approval", "id": "a", "next": "e",
        "approval_title": "评审审批", "approval_content": "架构评审",
        "approvers": ["m1", "m2", "m3"], "approval_strategy": "majority"},
       _end_whole_state("e", "a")],
      {}, "a",
      {"kind": "approval_service", "decisions": [("m1", "approve"), ("m2", "approve")]})

_case("approval_expr_approvers",
      [_start("s", "a"),
       {"type": "approval", "id": "a", "next": "e",
        "approval_title": "按需审批", "approval_content": "owner 审批",
        "approvers": ["$INPUT.owner"], "approval_strategy": "any"},
       _end_whole_state("e", "a")],
      {"owner": "boss1"}, "a",
      {"kind": "approval_service", "decisions": [("boss1", "approve")]})

_case("approval_filter_and_camel",
      [_start("s", "a"),
       {"type": "approval", "id": "a", "next": "e",
        "approval_title": "过滤审批", "approval_content": "带过滤器",
        "approvers": ["m1"], "approval_strategy": "any",
        "eventFilter": {"trigger_type": "approval_completed"},
        "subscriptionTimeout": 3600},
       _end_whole_state("e", "a")],
      {}, "a",
      # camelCase 别名（EventNode 家族继承）+ 事件过滤器钉进挂起态。
      {"kind": "approval_service", "decisions": [("m1", "approve")]})

_case("approval_explicit_null_timeout",
      [_start("s", "a"),
       {"type": "approval", "id": "a", "next": "e",
        "approval_title": "无限等待", "approval_content": "显式 null 超时",
        "approvers": ["m1"], "subscription_timeout": None},
       _end_whole_state("e", "a")],
      {}, "a",
      {"kind": "approval_service", "decisions": [("m1", "approve")]})

_load_reject("reject_approval_bad_strategy",
             {"type": "approval", "approval_title": "t", "approval_content": "c",
              "approvers": ["m1"], "approval_strategy": "SOME"}, "approval_strategy")

# --- redis_queue --------------------------------------------------------------
def redis_message_event(service_config: Dict[str, Any], message: Any) -> Dict[str, Any]:
    """redis_queue_service.py 的消息事件形状（list 分支，L365-376）。"""
    return {
        "node_id": service_config.get("node_id"),
        "execution_id": service_config.get("execution_id"),
        "flow_id": service_config.get("flow_id"),
        "tenant_id": "default",
        "trigger_type": "redis_message",
        "queue_type": service_config.get("queue_config", {}).get("type", "list"),
        "queue_name": service_config.get("queue_config", {}).get("name"),
        "message": message,
        "raw_message": message if isinstance(message, str) else json.dumps(message),
        "timestamp": 1792660000000,
        "success": True,
    }


_case("redisq_default_fields",
      [_start("s", "r"),
       {"type": "redis_queue", "id": "r", "queue_name": "tasks", "next": "e"},
       _end_whole_state("e", "r")],
      {}, "r",
      {"kind": "manual", "event": "REDISQ_DEFAULT"})

_case("redisq_stream_expr_name",
      [_start("s", "r"),
       {"type": "redis_queue", "id": "r", "next": "e",
        "queue_name": "$INPUT.stream", "queue_type": "stream",
        "redis_host": "$INPUT.host", "redis_port": 6380, "redis_db": 2,
        "message_format": "text", "timeout_seconds": 30, "batch_size": 5},
       _end_whole_state("e", "r")],
      {"stream": "events", "host": "redis.internal"}, "r",
      {"kind": "manual", "event": "REDISQ_TEXT"})

_case("redisq_pubsub",
      [_start("s", "r"),
       {"type": "redis_queue", "id": "r", "next": "e",
        "queue_name": "alerts", "queue_type": "pubsub"},
       _end_whole_state("e", "r")],
      {}, "r",
      {"kind": "manual", "event": "REDISQ_PUBSUB"})

_load_reject("reject_redisq_bad_type",
             {"type": "redis_queue", "queue_name": "q", "queue_type": "fifo"}, "queue_type")

# --- kafka_queue --------------------------------------------------------------
def kafka_message_event(service_config: Dict[str, Any], message: Any) -> Dict[str, Any]:
    """kafka_queue_service.py 的消息事件形状（L660-671）。"""
    topic = service_config.get("topic_config", {}).get("topic")
    return {
        "node_id": service_config.get("node_id"),
        "execution_id": service_config.get("execution_id"),
        "flow_id": service_config.get("flow_id"),
        "tenant_id": "default",
        "trigger_type": "kafka_message",
        "topic": topic,
        "message": message,
        "partition": 0,
        "offset": 42,
        "timestamp": 1792660000000,
        "success": True,
    }


_case("kafka_bootstrap_split",
      [_start("s", "k"),
       {"type": "kafka_queue", "id": "k", "next": "e",
        "bootstrap_servers": "b1:9092,b2:9092", "topic": "orders", "group_id": "g1"},
       _end_whole_state("e", "k")],
      {}, "k",
      {"kind": "manual", "event": "KAFKA_JSON"})

_case("kafka_sasl_list_servers",
      [_start("s", "k"),
       {"type": "kafka_queue", "id": "k", "next": "e",
        "bootstrap_servers": ["b1:9092", "b2:9092"], "topic": "audit",
        "group_id": "g2", "security_protocol": "SASL_SSL",
        "sasl_mechanism": "SCRAM-SHA-256", "sasl_username": "usr", "sasl_password": "pwd",
        "auto_offset_reset": "earliest", "enable_auto_commit": False, "max_poll_records": 10,
        "timeout_ms": 5000},
       _end_whole_state("e", "k")],
      {}, "k",
      {"kind": "manual", "event": "KAFKA_TEXT"})

_case("kafka_expr_topic",
      [_start("s", "k"),
       {"type": "kafka_queue", "id": "k", "next": "e",
        "bootstrap_servers": "$INPUT.broker", "topic": "$INPUT.topic", "group_id": "g3"},
       _end_whole_state("e", "k")],
      {"broker": "kafka.internal:9092", "topic": "telemetry"}, "k",
      {"kind": "manual", "event": "KAFKA_JSON"})

_load_reject("reject_kafka_bad_protocol",
             {"type": "kafka_queue", "bootstrap_servers": "b:9092", "topic": "t",
              "group_id": "g", "security_protocol": "QUIC"}, "security_protocol")

# --- http_callback ------------------------------------------------------------
_case("httpcb_token_auth",
      [_start("s", "h"),
       {"type": "http_callback", "id": "h", "next": "e",
        "callback_path": "/cb/explicit", "auth_type": "token", "auth_token": "tok-123"},
       _end_whole_state("e", "h")],
      {}, "h",
      {"kind": "http_callback_service",
       "request_data": {"approved": True},
       "headers": {"Authorization": "Bearer tok-123"}})

_case("httpcb_no_auth",
      [_start("s", "h"),
       {"type": "http_callback", "id": "h", "next": "e",
        "callback_path": "/cb/open", "require_auth": False},
       _end_whole_state("e", "h")],
      {}, "h",
      {"kind": "http_callback_service",
       "request_data": {"result": 42},
       "headers": {}})

_case("httpcb_auto_path",
      [_start("s", "h"),
       {"type": "http_callback", "id": "h", "next": "e",
        "callback_method": "put"},
       _end_whole_state("e", "h")],
      {}, "h",
      # 自动路径 /callback/{flow_id}/{node_id}/{execution_id}——execution_id 是
      # 32hex，规范化层打码为 <hex32>（Go 测试侧注入等长的 32hex execution id）。
      # require_auth 缺省 true：节点自动生成 token，凭据从挂起态读回。
      {"kind": "http_callback_service",
       "request_data": {"ok": True},
       "headers": "TOKEN_FROM_STATE"})

_case("httpcb_basic_auth",
      [_start("s", "h"),
       {"type": "http_callback", "id": "h", "next": "e",
        "callback_path": "/cb/basic", "auth_type": "basic", "auth_token": "cbuser"},
       _end_whole_state("e", "h")],
      {}, "h",
      # basic 的 password 由节点现场生成——驱动器从挂起态 service_config 读回
      # 后拼 Authorization 头。
      {"kind": "http_callback_service",
       "request_data": {"go": True},
       "headers": "BASIC_FROM_STATE"})

_case("httpcb_wrong_token_stays_suspended",
      [_start("s", "h"),
       {"type": "http_callback", "id": "h", "next": "e",
        "callback_path": "/cb/locked", "auth_type": "token", "auth_token": "right"},
       _end_whole_state("e", "h")],
      {}, "h",
      # 错误凭据：Q5 统一失败响应、不发布事件——流程保持挂起（Go 侧同断言：
      # 挂起态不变、无恢复）。
      {"kind": "http_callback_service",
       "request_data": {"sneaky": True},
       "headers": {"Authorization": "Bearer wrong"},
       "expect_error": True})

_load_reject("reject_httpcb_bad_method",
             {"type": "http_callback", "callback_method": "PATCH"}, "callback_method")


# --- 驱动器 ------------------------------------------------------------------
# 服务发布走 asyncio.create_task 分发处理器——驱动与捕获必须在同一个事件循环
# 里（asyncio.run 的驱动函数内注册 wildcard 处理器、驱动服务、再泵一轮循环）。

async def _drive_in_loop(case: Dict[str, Any], suspend_state: Dict[str, Any],
                         bus: InMemoryEventBus) -> Optional[Dict[str, Any]]:
    captured: List[Dict[str, Any]] = []

    async def _on_event(event) -> None:
        captured.append(event.data)

    await bus.register_handler(event_type="*", handler=_on_event)

    service_config = suspend_state.get("service_config", {})
    kind = case["driver"]["kind"]

    if kind == "delay_service":
        svc = DelayService(event_bus=bus)
        await svc.handle_task(dict(service_config))

    elif kind == "approval_service":
        svc = ApprovalService(event_bus=bus)
        await svc.handle_task(dict(service_config))
        approval_id = service_config["approval_id"]
        for approver, decision in case["driver"]["decisions"]:
            result = await svc.submit_approval_decision(approval_id, approver, decision)
            if result.get("status") != "success":
                raise RuntimeError(f"decision rejected: {result}")

    elif kind == "http_callback_service":
        svc = HttpCallbackService(event_bus=bus)
        await svc.handle_task(dict(service_config))
        path = service_config["callback_config"]["path"]
        headers = case["driver"]["headers"]
        if headers == "BASIC_FROM_STATE":
            username = service_config["auth_config"].get("username")
            password = service_config["auth_config"]["password"]
            headers = {"Authorization": "Basic " + base64.b64encode(
                f"{username}:{password}".encode()).decode()}
        elif headers == "TOKEN_FROM_STATE":
            # 自动生成 token（32hex，规范化层打码）——从挂起态 service_config
            # 读回后拼 Bearer 头。
            headers = {"Authorization": "Bearer " + service_config["auth_config"]["token"]}
        response = await svc.handle_callback_request(
            path, case["driver"]["request_data"], headers)
        if case["driver"].get("expect_error"):
            if response.get("status") != "error":
                raise RuntimeError(f"wrong-token callback unexpectedly succeeded: {response}")
            case["service_response"] = response  # 留档不参与 Go 对拍
            await asyncio.sleep(0.2)
            return None
        if response.get("status") != "success":
            raise RuntimeError(f"callback rejected: {response}")

    elif kind == "manual_delay":
        captured.append(delay_event_data(service_config))

    elif kind == "manual":
        event_kind = case["driver"]["event"]
        if event_kind == "REDISQ_DEFAULT":
            captured.append(redis_message_event(service_config, {"job": "build", "id": 7}))
        elif event_kind == "REDISQ_TEXT":
            captured.append(redis_message_event(service_config, "plain-text-payload"))
        elif event_kind == "REDISQ_PUBSUB":
            event = redis_message_event(service_config, {"level": "warn"})
            event["channel_name"] = service_config["queue_config"]["name"]
            captured.append(event)
        elif event_kind == "KAFKA_JSON":
            captured.append(kafka_message_event(service_config, {"order": "A1", "qty": 3}))
        elif event_kind == "KAFKA_TEXT":
            captured.append(kafka_message_event(service_config, "raw-kafka-line"))
        else:
            raise RuntimeError(f"unknown manual event {event_kind}")

    else:
        raise RuntimeError(f"unknown driver {kind}")

    # 泵一轮循环：服务发布经 create_task 分发到 wildcard 处理器。
    for _ in range(10):
        await asyncio.sleep(0.05)
        if captured:
            break
    if not captured and kind not in ("http_callback_service",):
        raise RuntimeError(f"{case['name']}: service published no event")
    return captured[-1] if captured else None


def run_all() -> None:
    registry = get_default_registry()
    for cls in (DelayNode, ApprovalNode, RedisQueueNode, KafkaQueueNode, HttpCallbackNode):
        registry.register(cls)

    out = []
    for case in CASES:
        name = case["name"]
        data: Dict[str, Any] = {
            "name": name,
            "flow": case["flow"],
            "input": case.get("input", {}),
            "suspend_node": case["suspend_node"],
        }
        if "expect_load_error_contains" in case:
            # 拒绝对拍：Python 模型层报错，Go 侧在 ir.Load 拒绝（error_contains
            # 取两侧共有的稳定子串=字段名）。
            try:
                _flow_cls().model_validate(case["flow"])
                raise RuntimeError(f"{name}: expected model rejection but load succeeded")
            except Exception as e:  # noqa: BLE001
                data["expected"] = {
                    "error": f"{type(e).__name__}: {e}",
                    "error_contains": case["expect_load_error_contains"],
                }
                print(f"[rej]  {name}")
                out.append(data)
                continue

        bus = InMemoryEventBus()
        flow = _flow_cls().model_validate(case["flow"])
        r1 = FlowExecution.run(flow, params=case.get("input", {}),
                               mode=ExecutionMode.DISTRIBUTED, event_bus=bus)
        if not r1.get("is_suspend"):
            raise RuntimeError(f"{name}: flow did not suspend: {r1.get('result')}")
        suspend_state = r1["result"]
        data["expected"] = {"suspend": normalize(suspend_state)}

        event_data = asyncio.run(_drive_in_loop(case, suspend_state, bus))
        if event_data is None:
            # 无恢复（错误凭据用例）：断言面 = 挂起态本身。
            data["expected"]["output"] = None
            print(f"[sus]  {name}")
        else:
            payload = dict(event_data)
            payload["__plaita_subscription_id"] = suspend_state["subscription_id"]
            data["resume"] = normalize_resume(payload)
            r2 = FlowExecution.run(flow, params={}, context=r1["context"],
                                   mode=ExecutionMode.DISTRIBUTED, resume_type="event",
                                   resume_data=payload, event_bus=bus)
            data["expected"]["output"] = normalize(r2["result"])
            print(f"[ok]   {name}: status={data['expected']['output'].get('status')}")
        out.append(data)

    path = sys.argv[1] if len(sys.argv) > 1 else "server_nodes_golden.json"
    with open(path, "w") as f:
        json.dump(out, f, indent=1, sort_keys=True, ensure_ascii=True)
        f.write("\n")
    print(f"--- {len(out)} cases exported -> {path}")


def normalize_resume(payload: Dict[str, Any]) -> Dict[str, Any]:
    """恢复载荷去掉真实绑定键后落盘（Go 测试注入自己执行现场的 subscription_id）。"""
    clean = {k: v for k, v in payload.items() if k != "__plaita_subscription_id"}
    return json.loads(json.dumps(clean, default=str))


def _flow_cls():
    from plaita import Flow
    return Flow


if __name__ == "__main__":
    run_all()
