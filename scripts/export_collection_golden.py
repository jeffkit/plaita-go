#!/usr/bin/env python3
"""Export collection-node golden cases from REAL plaita runs (#80 phase 1).

用法（与 export_golden.py 同框架）：
    cd ~/projects/infra4agent/plaita && python3 ~/projects/plaita-go/scripts/export_collection_golden.py

每个用例：deployment 形态 IR dict（codeflow 编译器形状：集合节点 camel
childFlow、while 的 child_flow/max_iterations）→ Flow.model_validate →
flow.run(input) → 记录期望输出/错误。Go 侧 pkg/ir/collection_golden_test.go
消费同一份 JSON：ir.Load + wf.Execute 逐例对拍。
"""

from __future__ import annotations

import json
import sys
from typing import Any, Dict, List, Optional

from plaita import Flow

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


def _if(nid: str, cond: Dict[str, Any], nxt: str, else_nxt: str) -> Dict[str, Any]:
    return {"type": "if", "id": nid, "condition": cond, "next": nxt, "else_next": else_nxt}


def _child(nodes: List[Dict[str, Any]], input_type: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    d: Dict[str, Any] = {"runtime": "python", "nodes": nodes}
    d["inputType"] = input_type or {"dataType": "object"}
    return d


def _root(name: str, nodes: List[Dict[str, Any]], input_type: Optional[Dict[str, Any]] = None) -> None:
    CASES.append({
        "name": name,
        "flow": {
            "flow_id": name,
            "runtime": "python",
            "inputType": input_type or {"dataType": "object"},
            "nodes": nodes,
        },
    })


def _if_err_on(field: str, value: Any, ok_output: Any) -> Dict[str, Any]:
    """child 子流程：命中 field==value 即 error end，否则输出 ok_output。"""
    return _child([
        _start("c_s", "c_if"),
        _if("c_if", {"field": field, "operator": "eq", "value": value}, "c_err", "c_ok"),
        _end_err("c_err"),
        _end("c_ok", ok_output),
    ])


# --- 下一层公共子流程 -------------------------------------------------------

DOUBLE_CHILD = _child([
    _start("c_s", "c_e"),
    _end("c_e", "$F.mul($INPUT.item, 2)"),
])

IDENTITY_CHILD = _child([
    _start("c_s", "c_e"),
    _end("c_e", "$INPUT.item"),
])

REDUCE_SUM_ARRAY_CHILD = _child([
    _start("c_s", "c_e"),
    _end("c_e", "$F.add($INPUT[0], $INPUT[1])"),
], input_type={"dataType": "array"})

REDUCE_SUM_OBJECT_CHILD = _child([
    _start("c_s", "c_e"),
    _end("c_e", "$F.add($INPUT.first, $INPUT.second)"),
])

# --- loop -------------------------------------------------------------------

_root("loop_basic", [
    _start("s", "lp"),
    {"type": "loop", "id": "lp", "collection": "$INPUT.items",
     "childFlow": DOUBLE_CHILD, "next": "o"},
    _end("o", "$NODE.lp"),
])
CASES[-1]["input"] = {"items": [1, 2, 3]}

_root("loop_empty_null", [
    _start("s", "lp"),
    {"type": "loop", "id": "lp", "collection": "$INPUT.items",
     "childFlow": DOUBLE_CHILD, "next": "o"},
    _end("o", "$NODE.lp"),
])
CASES[-1]["input"] = {"items": None}

_root("loop_empty_literal", [
    _start("s", "lp"),
    {"type": "loop", "id": "lp", "collection": [],
     "childFlow": DOUBLE_CHILD, "next": "o"},
    _end("o", "$NODE.lp"),
])
CASES[-1]["input"] = {}

_root("loop_condition_break", [
    _start("s", "lp"),
    {"type": "loop", "id": "lp", "collection": "$INPUT.items",
     "condition": {"field": "$LOOP-RESULT", "operator": "eq", "value": 2},
     "childFlow": DOUBLE_CHILD, "next": "o"},
    _end("o", "$NODE.lp"),
])
CASES[-1]["input"] = {"items": [1, 2, 3]}

_root("loop_string_collection", [
    _start("s", "lp"),
    {"type": "loop", "id": "lp", "collection": "[1, 2, 3]",
     "childFlow": _child([_start("c_s", "c_e"), _end("c_e", "$INPUT.item")]),
     "next": "o"},
    _end("o", "$NODE.lp"),
])
CASES[-1]["input"] = {}

_root("loop_nested", [
    _start("s", "lp"),
    {"type": "loop", "id": "lp", "collection": "$INPUT.items",
     "childFlow": _child([
         _start("c_s", "inner"),
         {"type": "loop", "id": "inner", "collection": "$INPUT.item",
          "childFlow": DOUBLE_CHILD, "next": "c_e"},
         _end("c_e", "$NODE.inner"),
     ]),
     "next": "o"},
    _end("o", "$NODE.lp"),
])
CASES[-1]["input"] = {"items": [[1, 2], [3]]}

_root("loop_error_propagation", [
    _start("s", "lp"),
    {"type": "loop", "id": "lp", "collection": "$INPUT.items",
     "childFlow": _if_err_on("$INPUT.item", 2, "$INPUT.item"), "next": "o"},
    _end("o", "$NODE.lp"),
])
CASES[-1]["input"] = {"items": [1, 2, 3]}

# --- map --------------------------------------------------------------------

_root("map_basic", [
    _start("s", "m"),
    {"type": "map", "id": "m", "collection": "$INPUT.items",
     "childFlow": DOUBLE_CHILD, "next": "o"},
    _end("o", "$NODE.m"),
])
CASES[-1]["input"] = {"items": [1, 2, 3]}

_root("map_empty", [
    _start("s", "m"),
    {"type": "map", "id": "m", "collection": "$INPUT.items",
     "childFlow": DOUBLE_CHILD, "next": "o"},
    _end("o", "$NODE.m"),
])
CASES[-1]["input"] = {"items": None}

_root("map_objects", [
    _start("s", "m"),
    {"type": "map", "id": "m", "collection": "$INPUT.items",
     "childFlow": _child([_start("c_s", "c_e"), _end("c_e", "$INPUT.item.n")]),
     "next": "o"},
    _end("o", "$NODE.m"),
])
CASES[-1]["input"] = {"items": [{"n": 1}, {"n": 2}, {"n": 3}]}

_root("map_concurrent", [
    _start("s", "m"),
    {"type": "map", "id": "m", "collection": "$INPUT.items",
     "concurrent": True, "maxConcurrent": 2,
     "childFlow": DOUBLE_CHILD, "next": "o"},
    _end("o", "$NODE.m"),
])
CASES[-1]["input"] = {"items": [1, 2, 3]}
# plaita 实测：camel maxConcurrent 落 extra 被忽略（Python 字段是
# max_concurrent），本例并发不设限；Go 侧 honor maxConcurrent=2。两侧输出
# 语义一致（结果按集合顺序），并发峰值由 Go 本地探针测试钉住。

_root("map_nested_reduce", [
    _start("s", "m"),
    {"type": "map", "id": "m", "collection": "$INPUT.items",
     "childFlow": _child([
         _start("c_s", "rd"),
         {"type": "reduce", "id": "rd", "collection": "$INPUT.item",
          "childFlow": REDUCE_SUM_ARRAY_CHILD, "next": "c_e"},
         _end("c_e", "$NODE.rd"),
     ]),
     "next": "o"},
    _end("o", "$NODE.m"),
])
CASES[-1]["input"] = {"items": [[1, 2, 3], [10, 20]]}

_root("map_error_propagation", [
    _start("s", "m"),
    {"type": "map", "id": "m", "collection": "$INPUT.items",
     "childFlow": _if_err_on("$INPUT.item", 2, "$F.mul($INPUT.item, 2)"), "next": "o"},
    _end("o", "$NODE.m"),
])
CASES[-1]["input"] = {"items": [1, 2, 3]}

# --- filter -----------------------------------------------------------------

_root("filter_truthiness_strings", [
    _start("s", "f"),
    {"type": "filter", "id": "f", "collection": "$INPUT.items",
     "childFlow": IDENTITY_CHILD, "next": "o"},
    _end("o", "$NODE.f"),
])
CASES[-1]["input"] = {"items": ["apple", "", "fig", "banana"]}

_root("filter_empty", [
    _start("s", "f"),
    {"type": "filter", "id": "f", "collection": "$INPUT.items",
     "childFlow": IDENTITY_CHILD, "next": "o"},
    _end("o", "$NODE.f"),
])
CASES[-1]["input"] = {"items": None}

_root("filter_lists_truthiness", [
    _start("s", "f"),
    {"type": "filter", "id": "f", "collection": "$INPUT.items",
     "childFlow": IDENTITY_CHILD, "next": "o"},
    _end("o", "$NODE.f"),
])
CASES[-1]["input"] = {"items": [[1], [], [2, 3], []]}

_root("filter_dicts_truthiness", [
    _start("s", "f"),
    {"type": "filter", "id": "f", "collection": "$INPUT.items",
     "childFlow": IDENTITY_CHILD, "next": "o"},
    _end("o", "$NODE.f"),
])
CASES[-1]["input"] = {"items": [{}, {"a": 1}, {"b": 0}]}

_root("filter_error_propagation", [
    _start("s", "f"),
    {"type": "filter", "id": "f", "collection": "$INPUT.items",
     "childFlow": _if_err_on("$INPUT.index", 1, "$INPUT.item"), "next": "o"},
    _end("o", "$NODE.f"),
])
CASES[-1]["input"] = {"items": [1, 2, 3]}

# --- find -------------------------------------------------------------------

_root("find_dicts", [
    _start("s", "f"),
    {"type": "find", "id": "f", "collection": "$INPUT.items",
     "childFlow": _child([
         _start("c_s", "c_if"),
         _if("c_if", {"field": "$INPUT.item.tag", "operator": "eq", "value": "b"}, "c_t", "c_f"),
         _end("c_t", "$INPUT.item"),
         _end("c_f", ""),
     ]),
     "next": "o"},
    _end("o", "$NODE.f"),
])
CASES[-1]["input"] = {"items": [{"tag": "a"}, {"tag": "b"}, {"tag": "b"}]}

_root("find_not_found", [
    _start("s", "f"),
    {"type": "find", "id": "f", "collection": "$INPUT.items",
     "childFlow": _child([_start("c_s", "c_e"), _end("c_e", "")]),
     "next": "o"},
    _end("o", "$NODE.f"),
])
CASES[-1]["input"] = {"items": [1, 2, 3]}

_root("find_empty", [
    _start("s", "f"),
    {"type": "find", "id": "f", "collection": "$INPUT.items",
     "childFlow": IDENTITY_CHILD, "next": "o"},
    _end("o", "$NODE.f"),
])
CASES[-1]["input"] = {"items": None}

_root("find_shortcircuit", [
    _start("s", "f"),
    {"type": "find", "id": "f", "collection": "$INPUT.items",
     "childFlow": _if_err_on("$INPUT.item", "boom", "$INPUT.item"), "next": "o"},
    _end("o", "$NODE.f"),
])
CASES[-1]["input"] = {"items": [0, 2, "boom", 4]}
# item=0 falsy 继续；item=2 truthy 命中返回——"boom" 不执行（短路实证）。

_root("find_returns_element_not_index", [
    _start("s", "f"),
    {"type": "find", "id": "f", "collection": "$INPUT.items",
     "childFlow": _child([_start("c_s", "c_e"), _end("c_e", "$INPUT.index")]),
     "next": "o"},
    _end("o", "$NODE.f"),
])
CASES[-1]["input"] = {"items": ["a", "b"]}
# index=0 falsy、index=1 truthy → 返回的是元素 "b"（不是 1）。

# --- reduce -----------------------------------------------------------------

_root("reduce_object_sum_no_initial", [
    _start("s", "r"),
    {"type": "reduce", "id": "r", "collection": "$INPUT.items",
     "childFlow": REDUCE_SUM_OBJECT_CHILD, "next": "o"},
    _end("o", "$NODE.r"),
])
CASES[-1]["input"] = {"items": [1, 2, 3]}

_root("reduce_array_sum_no_initial", [
    _start("s", "r"),
    {"type": "reduce", "id": "r", "collection": "$INPUT.items",
     "childFlow": REDUCE_SUM_ARRAY_CHILD, "next": "o"},
    _end("o", "$NODE.r"),
])
CASES[-1]["input"] = {"items": [1, 2, 3]}

_root("reduce_initial_literal", [
    _start("s", "r"),
    {"type": "reduce", "id": "r", "collection": "$INPUT.items", "initial": 100,
     "childFlow": REDUCE_SUM_OBJECT_CHILD, "next": "o"},
    _end("o", "$NODE.r"),
])
CASES[-1]["input"] = {"items": [1, 2, 3]}

_root("reduce_initial_expression", [
    _start("s", "r"),
    {"type": "reduce", "id": "r", "collection": "$INPUT.items", "initial": "$INPUT.seed",
     "childFlow": REDUCE_SUM_OBJECT_CHILD, "next": "o"},
    _end("o", "$NODE.r"),
])
CASES[-1]["input"] = {"seed": 5, "items": [1, 2]}

_root("reduce_empty_no_initial", [
    _start("s", "r"),
    {"type": "reduce", "id": "r", "collection": "$INPUT.items",
     "childFlow": REDUCE_SUM_OBJECT_CHILD, "next": "o"},
    _end("o", "$NODE.r"),
])
CASES[-1]["input"] = {"items": None}

_root("reduce_empty_with_initial", [
    _start("s", "r"),
    {"type": "reduce", "id": "r", "collection": "$INPUT.items", "initial": 100,
     "childFlow": REDUCE_SUM_OBJECT_CHILD, "next": "o"},
    _end("o", "$NODE.r"),
])
CASES[-1]["input"] = {"items": None}

_root("reduce_single_no_initial", [
    _start("s", "r"),
    {"type": "reduce", "id": "r", "collection": "$INPUT.items",
     "childFlow": REDUCE_SUM_OBJECT_CHILD, "next": "o"},
    _end("o", "$NODE.r"),
])
CASES[-1]["input"] = {"items": [7]}

_root("reduce_string_concat", [
    _start("s", "r"),
    {"type": "reduce", "id": "r", "collection": "$INPUT.items", "initial": "",
     "childFlow": _child([
         _start("c_s", "c_e"),
         _end("c_e", "$F.concat($INPUT.first, $INPUT.second)"),
     ]),
     "next": "o"},
    _end("o", "$NODE.r"),
])
CASES[-1]["input"] = {"items": ["a", "b", "c"]}

# --- while ------------------------------------------------------------------

WHILE_INDEX_CHILD = _child([_start("c_s", "c_e"), _end("c_e", "$INPUT.index")])

_root("while_basic", [
    _start("s", "w"),
    {"type": "while", "id": "w",
     "condition": {"field": "$LOOP-INDEX", "operator": "lt", "value": 3},
     "child_flow": WHILE_INDEX_CHILD, "next": "o"},
    _end("o", "$NODE.w"),
])
CASES[-1]["input"] = {}

_root("while_item_chain", [
    _start("s", "w"),
    {"type": "while", "id": "w",
     "condition": {"relation": "or", "conditions": [
         {"field": "$LOOP-ITEM", "operator": "eq", "value": None},
         {"field": "$LOOP-ITEM", "operator": "lt", "value": 3},
     ]},
     "child_flow": _child([
         _start("c_s", "c_if"),
         # ne null：None → 假（出 0）；0 → 真（0+1=1）。不能用 truthy——
         # Python 0 falsy，链会卡在 0 打满默认上限。
         _if("c_if", {"field": "$INPUT.item", "operator": "ne", "value": None}, "c_t", "c_f"),
         _end("c_t", "$F.add($INPUT.item, 1)"),
         _end("c_f", 0),
     ]),
     "next": "o"},
    _end("o", "$NODE.w"),
])
CASES[-1]["input"] = {}
# item: None → 0 → 1 → 2 → 3（$LOOP-ITEM=3 时 or 两支皆假，停）。结果 3。

_root("while_max_iterations", [
    _start("s", "w"),
    {"type": "while", "id": "w",
     "condition": {"field": "$INPUT.on", "operator": "eq", "value": True},
     "max_iterations": 5,
     "child_flow": WHILE_INDEX_CHILD, "next": "o"},
    _end("o", "$NODE.w"),
])
CASES[-1]["input"] = {"on": True}
# 恒真条件 + 上限 5：跑满 5 轮（0..4），仅告警不报错，结果 4。

_root("while_default_limit", [
    _start("s", "w"),
    {"type": "while", "id": "w",
     "condition": {"field": "$INPUT.on", "operator": "eq", "value": True},
     "child_flow": WHILE_INDEX_CHILD, "next": "o"},
    _end("o", "$NODE.w"),
])
CASES[-1]["input"] = {"on": True}
# 缺省 max_iterations=1000：钉默认上限（跑满 1000 轮，结果 999）。

_root("while_zero_rounds", [
    _start("s", "w"),
    {"type": "while", "id": "w",
     "condition": {"field": "$LOOP-INDEX", "operator": "gt", "value": 0},
     "child_flow": WHILE_INDEX_CHILD, "next": "o"},
    _end("o", "$NODE.w"),
])
CASES[-1]["input"] = {}
# 首轮 0>0 即假：零轮，结果 None。

_root("while_no_condition", [
    _start("s", "w"),
    {"type": "while", "id": "w",
     "child_flow": WHILE_INDEX_CHILD, "next": "o"},
    _end("o", "$NODE.w"),
])
CASES[-1]["input"] = {}
# condition 缺省（None）：只执行一轮，结果 0。

_root("while_error_propagation", [
    _start("s", "w"),
    {"type": "while", "id": "w",
     "condition": {"field": "$LOOP-INDEX", "operator": "lt", "value": 10},
     "child_flow": _if_err_on("$INPUT.index", 1, "$INPUT.index"),
     "next": "o"},
    _end("o", "$NODE.w"),
])
CASES[-1]["input"] = {}

_root("while_nested_in_map", [
    _start("s", "m"),
    {"type": "map", "id": "m", "collection": "$INPUT.items",
     "childFlow": _child([
         _start("c_s", "w"),
         {"type": "while", "id": "w",
          "condition": {"field": "$LOOP-INDEX", "operator": "lt", "value": "$INPUT.item"},
          "child_flow": WHILE_INDEX_CHILD, "next": "c_e"},
         _end("c_e", "$NODE.w"),
     ]),
     "next": "o"},
    _end("o", "$NODE.m"),
])
CASES[-1]["input"] = {"items": [3, 1, 2]}
# map→while 嵌套；条件 value 是表达式（$INPUT.item 取自 map 子流程输入）。


def run_all() -> None:
    out = []
    failures = 0
    for entry in CASES:
        name = entry["name"]
        flow = Flow.model_validate(entry["flow"])
        data: Dict[str, Any] = {
            "name": name,
            "flow": entry["flow"],
            "input": entry.get("input", {}),
        }
        try:
            result = flow.run(entry.get("input", {}))
            # JSON 覆写（嵌套容器）保证可序列化；不可序列化直接炸出来
            data["expected"] = {"output": json.loads(json.dumps(result))}
            print(f"[ok]   {name}: {json.dumps(data['expected']['output'], sort_keys=True)}")
        except Exception as e:  # noqa: BLE001 — golden 需要记录全部错误形态
            data["expected"] = {"error": f"{type(e).__name__}: {e}"}
            # IndexError 是两个运行时共同逐字的确定性错误（Go reduce 直接产出
            # 同款文案），可以钉消息子串；end-error 的外层包装两侧不同，只钉
            # 发生性。
            if "IndexError: list index out of range" in data["expected"]["error"]:
                data["expected"]["error_contains"] = "IndexError: list index out of range"
            print(f"[err]  {name}: {type(e).__name__}: {e}")
        out.append(data)
    with open(sys.argv[1] if len(sys.argv) > 1 else "collection_golden.json", "w") as f:
        json.dump(out, f, indent=1, sort_keys=True, ensure_ascii=True)
        f.write("\n")
    print(f"--- {len(out)} cases exported ({failures} hard failures)")


if __name__ == "__main__":
    run_all()
