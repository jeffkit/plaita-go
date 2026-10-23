#!/usr/bin/env python3
"""Python sidecar for the plaita-go shadow reconciliation harness (M2/#317).

Reads one JSON request from stdin::

    {"flow": {...deployment-form flow JSON...}, "input": {...},
     "driver": "fresh" | "event-resume" | "continue-after-error",
     "driverOptions": {...}}

Executes it with the reference Python engine (plaita) and writes one JSON
document to stdout::

    {"ok": true, "result": ..., "trace": [{"flow_id", "node_id", "type",
     "result", "error", "attempts"}], "error": "", "error_type": ""}

The node trace comes from FlowCallback.on_node_end (one event per node
execution). Child executions inherit the callback manager
(CallbackManager.child()), so loop bodies, inline child flows and parallel
branches are traced too — the Go side captures the same events through the
workflow.OnNodeEnd listener.

Since #317 the trace events also carry the per-node attempt count (retries +
1), captured by a logging.Handler on the plaita.core.runner retry loop ("node
%s attempt %d/%d failed ...; retrying" is its only observable outlet — zero
engine changes). The Go engine emits one OnNodeEnd per attempt, so both sides
normalize to attempts-per-node before the trace comparison.

Drivers (#317 execution-semantics cases; default "fresh" = single execute):

- event-resume: distributed mode, one step at a time — run to the suspending
  event node, resolve it with the resume carrier built from driverOptions
  (eventType/eventData) plus the subscription recorded in the checkpoint,
  then continue to the end node. Mirrors the go ResumeSuspension drive.
- continue-after-error: distributed mode — run the flow; the first pass stops
  at the failure boundary (or fails outright), then the continue carrier steps
  from the checkpoint (a failed node has no checkpoint entry, so stepping from
  last_node_id's successor re-runs exactly the failing node — plaita ResumeType
  docstring G1). Mirrors the go worker's full re-entry from the flow start
  (the declared replay divergence, replay-from-start:go).

The CodeNode is opted in explicitly with the subprocess backend (it left the
default registry in 0.4.0; the subprocess posture mirrors the worker and the
go engine, which only implements the subprocess backend). This file is
embedded into the Go binary, written to a temp file and run with ``python3``
from the plaita repository root. It never mutates the Python repo.
"""

from __future__ import annotations

import json
import logging
import sys
import warnings

warnings.filterwarnings("ignore")

_RETRY_WARNING_PREFIX = "node %s attempt %d/%d failed"


def _jsonable(value):
    """Best-effort conversion into the JSON value domain."""
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return {"__unserializable__": type(value).__name__, "repr": repr(value)}


class _AttemptCounter(logging.Handler):
    """Counts the runner's retry warnings per node (attempts = warnings + 1)."""

    def __init__(self):
        super().__init__(level=logging.WARNING)
        self.counts: dict[str, int] = {}

    def emit(self, record: logging.LogRecord) -> None:
        if not isinstance(record.msg, str) or not record.msg.startswith(_RETRY_WARNING_PREFIX):
            return
        try:
            node = str(record.args[0])
            attempt = int(record.args[1])
        except Exception:  # noqa: BLE001 - log shape drift must not kill the harness
            return
        self.counts[node] = max(self.counts.get(node, 0), attempt)


class _Tracer:
    """Minimal FlowCallback-compatible recorder (duck typed on purpose)."""

    def __init__(self, counter: _AttemptCounter):
        self.events = []
        self._counter = counter

    def _flow_id(self, flow):
        return str(getattr(flow, "flow_id", None) or "")

    def on_node_end(self, flow, node, result=None, error=None, exception=None, **kwargs):
        node_id = str(getattr(node, "id", ""))
        self.events.append({
            "flow_id": self._flow_id(flow),
            "node_id": node_id,
            "type": str(getattr(node, "node_type", "")),
            "result": _jsonable(result),
            "error": "" if error is None else str(error),
            "attempts": self._counter.counts.get(node_id, 0) + 1,
        })


def _checkpoint_of(execution):
    """The execution's checkpoint dict (the persisted resume position)."""
    ctx = execution.context
    for name in ("to_checkpoint_dict", "to_dict"):
        method = getattr(ctx, name, None)
        if callable(method):
            return method()
    return ctx


def _run_fresh(flow, input_value, tracer):
    from plaita.core.executor import FlowExecution

    execution = FlowExecution(callback_handlers=[tracer])
    result = execution.execute(flow, input_value)
    return result


def _run_event_resume(flow, input_value, tracer, options):
    from plaita.core.executor import FlowExecution

    execution = FlowExecution(callback_handlers=[tracer], mode="distributed")
    parked = execution.run_distributed(flow, input_value)
    if not parked.get("is_suspend"):
        return parked.get("result")
    context = parked.get("context") or {}
    node_id = parked.get("id")
    node_state = (context.get("$NODE") or {}).get(node_id) or {}
    resume_data = {
        "subscription_id": node_state.get("subscription_id"),
        "event_type": options.get("eventType"),
        "event_data": options.get("eventData") or {},
    }
    resolved = execution.run_distributed(
        flow, saved_context=context, resume_type="event", resume_data=resume_data)
    # The distributed model advances one step per continue carrier (the worker
    # redelivers carriers in a loop); keep stepping until the flow ends or
    # parks again — the go ResumeSuspension runs to completion in one call,
    # which is the documented driver asymmetry (plaita-go AGENTS.md §worker
    # 二期: distributed resume processes the resumed node itself).
    final = resolved
    for _ in range(64):
        if final.get("is_end") or final.get("is_suspend"):
            break
        final = execution.run_distributed(
            flow, saved_context=final.get("context") or {}, resume_type="continue")
    return final.get("result")


def _run_continue_after_error(flow, input_value, tracer):
    from plaita.core.executor import FlowExecution

    execution = FlowExecution(callback_handlers=[tracer], mode="distributed")
    try:
        execution.run_distributed(flow, input_value)
    except BaseException:  # noqa: BLE001 - terminal failure is the resume setup
        pass
    checkpoint = _checkpoint_of(execution)
    result = execution.run_distributed(
        flow, saved_context=checkpoint, resume_type="continue")
    return result


def main() -> int:
    request = json.load(sys.stdin)
    response = {
        "ok": False,
        "result": None,
        "trace": [],
        "error": "",
        "error_type": "",
    }
    counter = _AttemptCounter()
    logging.getLogger("plaita.core.runner").addHandler(counter)
    tracer = _Tracer(counter)
    driver = request.get("driver") or "fresh"
    try:
        from plaita.core.flow import Flow
        from plaita.node import register_code_node

        # The CodeNode left the default registry in 0.4.0; the harness opts in
        # with the subprocess backend (same posture as the worker and the go
        # engine, which only implements the subprocess backend).
        register_code_node(default_backend="subprocess")

        flow = Flow.model_validate(request["flow"])
        input_value = request.get("input") or {}
        if driver == "event-resume":
            result = _run_event_resume(flow, input_value, tracer, request.get("driverOptions") or {})
        elif driver == "continue-after-error":
            result = _run_continue_after_error(flow, input_value, tracer)
        else:
            result = _run_fresh(flow, input_value, tracer)
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
