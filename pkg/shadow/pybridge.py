#!/usr/bin/env python3
"""Python sidecar for the plaita-go shadow reconciliation harness (M2).

Reads one JSON request from stdin::

    {"flow": {...deployment-form flow JSON...}, "input": {...}}

Executes it with the reference Python engine (plaita) and writes one JSON
document to stdout::

    {"ok": true, "result": ..., "trace": [{"flow_id", "node_id", "type",
     "result", "error"}], "error": "", "error_type": ""}

The node trace comes from FlowCallback.on_node_end. Child executions inherit
the callback manager (CallbackManager.child()), so loop bodies, inline child
flows and parallel branches are traced too — the Go side captures the same
events through the workflow.OnNodeEnd listener.

This file is embedded into the Go binary, written to a temp file and run with
``python3`` from the plaita repository root. It never mutates the Python repo.
"""

from __future__ import annotations

import json
import sys
import warnings

warnings.filterwarnings("ignore")


def _jsonable(value):
    """Best-effort conversion into the JSON value domain."""
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return {"__unserializable__": type(value).__name__, "repr": repr(value)}


class _Tracer:
    """Minimal FlowCallback-compatible recorder (duck typed on purpose)."""

    def __init__(self):
        self.events = []

    def _flow_id(self, flow):
        return str(getattr(flow, "flow_id", None) or "")

    def on_node_end(self, flow, node, result=None, error=None, exception=None, **kwargs):
        self.events.append({
            "flow_id": self._flow_id(flow),
            "node_id": str(getattr(node, "id", "")),
            "type": str(getattr(node, "node_type", "")),
            "result": _jsonable(result),
            "error": "" if error is None else str(error),
        })


def main() -> int:
    request = json.load(sys.stdin)
    response = {
        "ok": False,
        "result": None,
        "trace": [],
        "error": "",
        "error_type": "",
    }
    tracer = _Tracer()
    try:
        from plaita.core.executor import FlowExecution
        from plaita.core.flow import Flow

        flow = Flow.model_validate(request["flow"])
        execution = FlowExecution(callback_handlers=[tracer])
        result = execution.execute(flow, request.get("input") or {})
        response["ok"] = True
        response["result"] = _jsonable(result)
    except BaseException as exc:  # noqa: BLE001 - report every failure to the harness
        response["error"] = f"{type(exc).__name__}: {exc}"
        response["error_type"] = type(exc).__name__
    finally:
        response["trace"] = tracer.events

    json.dump(response, sys.stdout, ensure_ascii=False, allow_nan=False)
    sys.stdout.write("\n")
    return 0 if response["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
