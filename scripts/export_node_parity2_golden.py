#!/usr/bin/env python3
"""Export #80 phase-2 node golden cases from REAL plaita runs (r78).

用法（与 export_collection_golden.py 同框架）：
    cd ~/projects/infra4agent/plaita && python3 ~/projects/plaita-go/scripts/export_node_parity2_golden.py

覆盖：case（SwitchLegacy 等值匹配方言）、mock（value 原样透传）、reference
（调度器注入语义——导出器在 Flow.model_validate 之后、flow.run 之前按
flow_registry 把 child_flow 注入 reference 节点，即 plaita 调度器的角色）、
calculate/ignore（运行时未注册的拒载对拍：Python unRecognized node type）。

Go 侧 pkg/ir/node_parity2_golden_test.go 消费同一份 JSON：reference 用例经
ir.LoadWithFlowProvider（registry 即 provider）加载注入，其余 ir.Load。

r78 实测的两个关键 Python 语义（导出用例把它们钉进 golden）：
  - reference 子流程继承 $GLOBAL/$ENV，但 $PARENT.<x> 求值为 null（plaita
    plain-run 路径不给子流程播种父快照——child.py 文档语义「不能使用父逻辑
    上下文的数据」的实测口径，child 节点同款）；
  - reference/child 的 input 缺省（求值为 None）→ 子流程继承调用方 $INPUT。
"""

from __future__ import annotations

import json
import sys
from typing import Any, Dict, List, Optional

from plaita import Flow
from plaita.node.child import ReferenceFlow

CASES: List[Dict[str, Any]] = []


def _start(nid: str, nxt: str) -> Dict[str, Any]:
    return {"type": "start", "id": nid, "next": nxt}


def _end(nid: str, output: Any, nxt: Optional[str] = None) -> Dict[str, Any]:
    d = {"type": "end", "id": nid, "resultType": "success", "output": output}
    if nxt:
        d["next"] = nxt
    return d


def _end_err(nid: str, code: str = "500", message: str = "boom") -> Dict[str, Any]:
    return {"type": "end", "id": nid, "resultType": "error", "error": {"code": code, "message": message}}


def _assign(nid: str, output: Any, nxt: str) -> Dict[str, Any]:
    return {"type": "assignment", "id": nid, "output": output, "next": nxt}


def _root(name: str, nodes: List[Dict[str, Any]], input_type: Optional[Dict[str, Any]] = None) -> None:
    CASES.append({
        "name": name,
        "flow": {
            "flow_id": name,
            "runtime": "python",
            "inputType": input_type or {"dataType": "any"},
            "nodes": nodes,
        },
        "input": {},
    })


def _registry(reg: Dict[str, Dict[str, Any]]) -> None:
    """最后一个用例挂上 reference 注册表（调度器解析 flow_id 的数据源）。"""
    CASES[-1]["flow_registry"] = reg


# --- case（decide.py SwitchLegacy：等值匹配方言，模型校验期脱糖为 Switch） ----

def _case_flow(name: str, cases: List[Dict[str, Any]], extra: Dict[str, Any], inp: Dict[str, Any],
               targets: Optional[List[str]] = None) -> None:
    """targets = 实际被 cases/default 引用的分支目标（部署面全图可达约束，
    未被引用的节点不许存在于图里）。"""
    targets = targets or ["hit", "other", "fb"]
    nodes = [_start("s", "c"),
             {"type": "case", "id": "c", "target": "$INPUT.x", "cases": cases, **extra}]
    for tid in targets:
        nodes.append(_assign(tid, tid[0].upper(), "o"))
    nodes.append(_end("o", "$NODE.c"))
    _root(name, nodes)
    CASES[-1]["input"] = inp


_case_flow("case_hit", [{"id": "hit", "value": "a"}, {"id": "other", "value": "b"}], {"default": "fb"}, {"x": "a"})
# 先声明先命中（同值双 case，稳定顺序）。
_case_flow("case_first_wins", [{"id": "hit", "value": 1}, {"id": "other", "value": 1}], {"default": "fb"}, {"x": 1})
# default 兜底。
_case_flow("case_default", [{"id": "hit", "value": "a"}], {"default": "fb"}, {"x": "zzz"}, targets=["hit", "fb"])
# default 键缺省 → 字面 "default" 节点 id（Python values.get("default","default")）。
_case_flow("case_default_implicit_node", [{"id": "hit", "value": "a"}], {}, {"x": "zzz"},
           targets=["hit", "default"])
# value 是表达式：求值后比较。
_case_flow("case_expr_value", [{"id": "hit", "value": "$INPUT.expect"}], {"default": "fb"}, {"x": 5, "expect": 5},
           targets=["hit", "fb"])
# 数值等值跨 int/float（Python 5 == 5.0）。
_case_flow("case_numeric_eq", [{"id": "hit", "value": 5.0}], {"default": "fb"}, {"x": 5}, targets=["hit", "fb"])
# target 求值为 None、value 为 null → eq None 命中（None is None）。
_case_flow("case_none_eq", [{"id": "hit", "value": None}], {"default": "fb"}, {}, targets=["hit", "fb"])
# 空 cases → 只有兜底分支（fixture 只留 default 可达节点——部署面结构校验
# 两侧都要求全图可达，无分支目标的节点不许存在）。
_case_flow("case_empty_cases", [], {"default": "fb"}, {"x": 1}, targets=["fb"])
# 负控：cases[] 缺 id / 缺 value → 模型层 ValueError（部署面拒载对拍）。
_root("case_missing_id", [
    _start("s", "c"),
    {"type": "case", "id": "c", "target": "$INPUT.x", "cases": [{"value": 1}], "default": "fb"},
    _assign("fb", "F", "o"),
    _end("o", "$NODE.c"),
])
_root("case_missing_value", [
    _start("s", "c"),
    {"type": "case", "id": "c", "target": "$INPUT.x", "cases": [{"id": "hit"}], "default": "fb"},
    _assign("fb", "F", "o"),
    _end("o", "$NODE.c"),
])
# case 嵌在子流程里（脱糖对嵌入流程同样生效）。
_root("case_in_child", [
    _start("s", "ch"),
    {"type": "child", "id": "ch", "input": "$INPUT", "next": "o",
     "childFlow": {"runtime": "python", "inputType": {"dataType": "any"}, "nodes": [
         _start("c_s", "c_c"),
         {"type": "case", "id": "c_c", "target": "$INPUT.k",
          "cases": [{"id": "c_hit", "value": "on"}], "default": "c_fb"},
         _assign("c_hit", "switched-on", "c_o"),
         _assign("c_fb", "fell-back", "c_o"),
         _end("c_o", "$NODE.c_c"),
     ]}},
    _end("o", "$NODE.ch"),
])
CASES[-1]["input"] = {"k": "on"}

# --- mock（plaita/node/mock.py：数据固定 pin 占位，value 原样返回） ----------

_root("mock_raw_string", [
    _start("s", "m"),
    {"type": "mock", "id": "m", "value": "$INPUT.x", "next": "o"},
    _end("o", "$NODE.m"),
])
CASES[-1]["input"] = {"x": "hello"}
# value 是表达式串也不求值——pin 语义就是字面固定。
_root("mock_dict", [
    _start("s", "m"),
    {"type": "mock", "id": "m", "value": {"k": [1, 2], "s": "t"}, "next": "o"},
    _end("o", "$NODE.m"),
])
_root("mock_none", [
    _start("s", "m"),
    {"type": "mock", "id": "m", "next": "o"},
    _end("o", "$NODE.m"),
])
_root("mock_list", [
    _start("s", "m"),
    {"type": "mock", "id": "m", "value": [1, "two", None], "next": "o"},
    _end("o", "$NODE.m"),
])
# output 字段被忽略：runner 不对 mock 求 output，value 即全部（r78 实测）。
_root("mock_output_ignored", [
    _start("s", "m"),
    {"type": "mock", "id": "m", "value": "pinned", "output": "$INPUT.x", "next": "o"},
    _end("o", "$NODE.m"),
])
CASES[-1]["input"] = {"x": "eval-output"}
# mock 结果供下游 $NODE 引用。
_root("mock_downstream", [
    _start("s", "m"),
    {"type": "mock", "id": "m", "value": {"code": 7}, "next": "a"},
    _assign("a", "$NODE.m.code", "o"),
    _end("o", "$NODE.a"),
])
# mock 在子流程里。
_root("mock_in_child", [
    _start("s", "ch"),
    {"type": "child", "id": "ch", "input": "$INPUT", "next": "o",
     "childFlow": {"runtime": "python", "inputType": {"dataType": "any"}, "nodes": [
         _start("c_s", "c_m"),
         {"type": "mock", "id": "c_m", "value": "child-pin", "next": "c_o"},
         _end("c_o", "$NODE.c_m"),
     ]}},
    _end("o", "$NODE.ch"),
])

# --- reference（child.py ReferenceFlow：调度器按 flow_id 注入 child_flow） ----

SUB = {"runtime": "python", "inputType": {"dataType": "any"}, "flow_id": "sub", "nodes": [
    _start("sub_s", "sub_e"),
    _end("sub_e", {"got": "$INPUT", "parent_x": "$PARENT.x", "parent_node": "$PARENT.n"}),
]}
SUB_GLOBAL = {"runtime": "python", "inputType": {"dataType": "any"}, "flow_id": "sub_global", "nodes": [
    _start("a", "b"),
    _end("b", {"g": "$GLOBAL.token", "inp_z": "$INPUT.z"}),
]}
SUB_REQUIRED = {"runtime": "python", "inputType": {"dataType": "object", "properties": {
    "need": {"dataType": "string", "is_required": True}}}, "flow_id": "sub_req", "nodes": [
    _start("a", "b"),
    _end("b", "$INPUT.need"),
]}
SUB_ERR = {"runtime": "python", "inputType": {"dataType": "any"}, "flow_id": "sub_err", "nodes": [
    _start("a", "b"),
    _end_err("b", "501", "sub-boom"),
]}
SUBSUB = {"runtime": "python", "inputType": {"dataType": "any"}, "flow_id": "subsub", "nodes": [
    _start("a", "b"),
    _end("b", "deep-ok"),
]}
SUB_NESTED_REF = {"runtime": "python", "inputType": {"dataType": "any"}, "flow_id": "sub_nested", "nodes": [
    _start("a", "r2"),
    {"type": "reference", "id": "r2", "input": "$INPUT", "flow_id": "subsub", "next": "b"},
    _end("b", "$NODE.r2"),
]}

# input dict 逐元素求值后作为子流程 $INPUT；子流程读 $PARENT → null（隔离实测）。
_root("ref_basic", [
    _start("s", "r"),
    {"type": "reference", "id": "r", "input": {"prefix": "$INPUT.name"}, "flow_id": "sub", "next": "o"},
    _end("o", "$NODE.r"),
])
CASES[-1]["input"] = {"name": "kong"}
_registry({"sub": SUB})
# $PARENT.<x>/$PARENT.<node> 在 reference 子流程里求值为 null（隔离钉子）。
_root("ref_parent_isolation", [
    _start("s", "r"),
    {"type": "reference", "id": "r", "input": {}, "flow_id": "sub", "next": "o"},
    _end("o", "$NODE.r"),
])
CASES[-1]["input"] = {"x": "top-x"}
_registry({"sub": SUB})
# input 缺省（None）→ 子流程继承调用方 $INPUT（child/reference 同款实测）。
_root("ref_no_input_inherits_caller", [
    _start("s", "r"),
    {"type": "reference", "id": "r", "flow_id": "sub", "next": "o"},
    _end("o", "$NODE.r"),
])
CASES[-1]["input"] = {"z": 42}
_registry({"sub": SUB})
# $GLOBAL 继承（子流程无自己的 global_context 时回落父级）。
_root("ref_global_inherit", [
    _start("s", "r"),
    {"type": "reference", "id": "r", "input": "$INPUT", "flow_id": "sub_global", "next": "o"},
    _end("o", "$NODE.r"),
])
CASES[-1]["input"] = {"z": 9}
CASES[-1]["flow"]["global_context"] = {"token": "top-token"}
_registry({"sub_global": SUB_GLOBAL})
# input 不满足子流程 inputType 必填 → AssertionError: input not match required。
_root("ref_input_mismatch", [
    _start("s", "r"),
    {"type": "reference", "id": "r", "input": "$INPUT", "flow_id": "sub_req", "next": "o"},
    _end("o", "$NODE.r"),
])
CASES[-1]["input"] = {"wrong": 1}
_registry({"sub_req": SUB_REQUIRED})
# 子流程错误直接冒泡（ErrorResultException）。
_root("ref_error_propagation", [
    _start("s", "r"),
    {"type": "reference", "id": "r", "input": "$INPUT", "flow_id": "sub_err", "next": "o"},
    _end("o", "$NODE.r"),
])
_registry({"sub_err": SUB_ERR})
# 嵌套 reference：ref → sub → ref → subsub（调度器递归注入）。
_root("ref_nested", [
    _start("s", "r"),
    {"type": "reference", "id": "r", "input": "$INPUT", "flow_id": "sub_nested", "next": "o"},
    _end("o", "$NODE.r"),
])
_registry({"sub_nested": SUB_NESTED_REF, "subsub": SUBSUB})
# reference 结果供下游 $NODE 引用。
_root("ref_result_downstream", [
    _start("s", "r"),
    {"type": "reference", "id": "r", "input": {"v": 3}, "flow_id": "subsub", "next": "a"},
    _assign("a", "$NODE.r", "o"),
    _end("o", "$NODE.a"),
])
_registry({"subsub": SUBSUB})

# --- calculate / ignore（plaita 运行时未注册的拒载对拍） ----------------------
# 两者都不是注册节点类型：ir_validate 结构校验放行，Flow.model_validate 以
# 「unRecognized node type」拒绝。Go 侧 ir.Load 同样拒载（capability 清单里的
# unsupported 成员）——对拍口径：两侧都拒。

_root("reject_calculate", [
    _start("s", "n1"),
    {"type": "calculate", "id": "n1", "expression": {"function_name": "add", "params": {"left": 10, "right": 32}}, "next": "o"},
    _end("o", "$NODE.n1"),
])
_root("reject_ignore", [
    _start("s", "n1"),
    {"type": "ignore", "id": "n1", "next": "o"},
    _end("o", "$NODE.n1"),
])


def _inject(flow_obj: Any, registry: Dict[str, Any], depth: int = 0) -> None:
    """调度器角色：把 flow_id 指向的子流程注入 reference 节点（递归）。"""
    if depth > 16:
        raise RuntimeError("reference injection too deep")
    for node in flow_obj.nodes:
        if isinstance(node, ReferenceFlow) and node.child_flow is None:
            fid = node.flow_id
            if fid not in registry:
                raise RuntimeError(f"flow_registry has no {fid!r}")
            sub = Flow.model_validate(registry[fid])
            node.child_flow = sub
            _inject(sub, registry, depth + 1)


def run_all() -> None:
    out = []
    for entry in CASES:
        name = entry["name"]
        data: Dict[str, Any] = {
            "name": name,
            "flow": entry["flow"],
            "input": entry.get("input", {}),
        }
        if "flow_registry" in entry:
            data["flow_registry"] = entry["flow_registry"]
        try:
            flow = Flow.model_validate(entry["flow"])
            if "flow_registry" in entry:
                _inject(flow, entry["flow_registry"])
            result = flow.run(entry.get("input", {}))
            data["expected"] = {"output": json.loads(json.dumps(result))}
            print(f"[ok]   {name}: {json.dumps(data['expected']['output'], sort_keys=True)}")
        except Exception as e:  # noqa: BLE001 — golden 需要记录全部错误形态
            data["expected"] = {"error": f"{type(e).__name__}: {e}"}
            # unRecognized node type 是两侧共有的逐字拒载口径（Go 的
            # knownUnsupported 提示刻意保留同一子串）——钉子串对拍。
            for t in ("calculate", "ignore"):
                if f"unRecognized node type: {t}" in data["expected"]["error"]:
                    data["expected"]["error_contains"] = f"unRecognized node type: {t}"
            print(f"[err]  {name}: {type(e).__name__}: {str(e)[:160]}")
        out.append(data)
    with open(sys.argv[1] if len(sys.argv) > 1 else "node_parity2_golden.json", "w") as f:
        json.dump(out, f, indent=1, sort_keys=True, ensure_ascii=True)
        f.write("\n")
    print(f"--- {len(out)} cases exported")


if __name__ == "__main__":
    run_all()
