#!/usr/bin/env python3
"""Export plaita expression golden tests to JSON for the Go port.

Reads ``tests/unit/test_expression_golden.py`` from the (read-only) Python
plaita repo, extracts every ``evaluate`` / ``parse_function`` assertion
programmatically via ``ast`` (no hand transcription), verifies each extracted
case against the live reference implementation, and writes one JSON case file
for github.com/jeffkit/plaita-go/pkg/expression.

Usage (run from the plaita Python repo root so ``import plaita`` resolves):
    cd /Users/kong/projects/infra4agent/plaita
    python3 /Users/kong/projects/plaita-go/scripts/export_golden.py

Extraction rules:
    assertEqual(evaluate(expr, ctx), expected)   -> expected value
    assertIsNone(evaluate(expr, ctx))            -> null
    assertIs(evaluate(expr, ctx), True)          -> true
    with assertRaises(Exc): evaluate(expr, ctx)  -> {"$error": "Exc"}
    assertEqual(parse_function(expr, ctx, "$"), expected) -> note marks the API

``expr`` / ``context`` / ``expected`` are literals in the test source; simple
local assignments (``ctx = {...}``) are resolved.  Adjacent comments and the
method docstring are carried into ``note``.  Cases that depend on a non-default
function registry (custom / scoped ``ExpressionRegistry``, ``registry=`` kwarg,
or ``ExpressionEvaluator(registry=...)``) are not expressible in the JSON
schema and are reported as skipped on stdout.

Verification runs every exported case through ``plaita.io.evaluate`` /
``parse_function`` and compares strictly (type + value, bool/int separated).
Any mismatch aborts the export with exit code 2 and writes nothing.
"""

from __future__ import annotations

import ast
import json
import os
import sys
from datetime import date, datetime
from decimal import Decimal

DEFAULT_TEST_FILE = "tests/unit/test_expression_golden.py"
DEFAULT_OUT = "/Users/kong/projects/plaita-go/pkg/expression/testdata/expression_golden.json"

PARSE_FUNCTION_NOTE = (
    "API: parse_function (the $F.* call-parse entry; returns the input "
    "unchanged when the expression is not a $F.* call)"
)


# ---------------------------------------------------------------------------
# AST helpers
# ---------------------------------------------------------------------------

def resolve_literal(node, env):
    """Best-effort literal value of *node* with one-level Name resolution."""
    if isinstance(node, ast.Name):
        if node.id in env:
            return env[node.id], True
        return None, False
    try:
        return ast.literal_eval(node), True
    except (ValueError, TypeError):
        return None, False


def comments_before(src_lines, start_line, prev_end):
    """Full-line comments between *prev_end* and *start_line* (1-based, excl/excl)."""
    parts = []
    for ln in range(prev_end + 1, start_line):
        text = src_lines[ln - 1].strip()
        if text.startswith("#"):
            parts.append(text.lstrip("#").strip())
    return parts


def is_api_call(node):
    """Is *node* a call to module-level evaluate/parse_function or evaluator.evaluate?"""
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    if isinstance(func, ast.Name):
        return func.id in ("evaluate", "parse_function")
    if isinstance(func, ast.Attribute):
        return func.attr in ("evaluate", "parse_function")
    return False


def classify_api_call(call, evaluators):
    """Return (api, registry_dependent, reason) for an evaluate/parse_function call."""
    func = call.func
    if isinstance(func, ast.Name):
        return func.id, False, None
    # Attribute call: <evaluator>.evaluate(...) — registry dependence is decided
    # by how the evaluator was constructed (ExpressionEvaluator() with no args
    # is the default registry and behaves like the module-level function).
    ctor = evaluators.get(func.value.id, "custom") if isinstance(func.value, ast.Name) else "custom"
    if ctor == "default":
        return func.attr, False, None
    return func.attr, True, (
        "evaluator built as ExpressionEvaluator(registry=...) with a custom/"
        "scoped ExpressionRegistry — a non-default function registry is not "
        "expressible in the JSON schema"
    )


def api_args(call, env):
    """Resolve (expr, context, registry_kw, ok) from an evaluate/parse_function call."""
    ok = True
    expr = None
    ctx = None
    if not call.args:
        return None, None, False, False
    expr, ok1 = resolve_literal(call.args[0], env)
    if len(call.args) > 1:
        ctx, ok2 = resolve_literal(call.args[1], env)
    else:
        ctx, ok2 = None, True
    registry_kw = any(kw.arg == "registry" for kw in call.keywords)
    return expr, ctx, registry_kw, ok1 and ok2


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------

def extract_assertion(stmt, env, evaluators):
    """Return (api_call_node, expected, mode) or None when *stmt* is not a case.

    mode is "value" | "is_none" | "is" | "raises".
    """
    if isinstance(stmt, ast.With):
        # with self.assertRaises(Exc) as cm: evaluate(...)
        if not stmt.items:
            return None
        ctx_expr = stmt.items[0].context_expr
        if not (isinstance(ctx_expr, ast.Call) and ctx_expr.func.attr == "assertRaises"):
            return None
        if not ctx_expr.args:
            return None
        exc_name = ctx_expr.args[0].id if isinstance(ctx_expr.args[0], ast.Name) else None
        for sub in stmt.body:
            if isinstance(sub, ast.Expr) and isinstance(sub.value, ast.Call) and is_api_call(sub.value):
                return sub.value, {"$error": exc_name}, "raises"
        return None

    if not (isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call)):
        return None
    call = stmt.value
    if not (isinstance(call.func, ast.Attribute) and isinstance(call.func.value, ast.Name)
            and call.func.value.id == "self"):
        return None
    attr = call.func.attr

    if attr == "assertEqual":
        api_call, expected_node = None, None
        for arg in call.args:
            if is_api_call(arg) and api_call is None:
                api_call = arg
            else:
                expected_node = arg
        if api_call is None or expected_node is None:
            return None
        expected, ok = resolve_literal(expected_node, env)
        if not ok:
            return None
        return api_call, expected, "value"

    if attr == "assertIsNone" and call.args and is_api_call(call.args[0]):
        return call.args[0], None, "is_none"

    if attr == "assertIs" and len(call.args) == 2 and is_api_call(call.args[0]):
        expected, ok = resolve_literal(call.args[1], env)
        if not ok:
            return None
        return call.args[0], expected, "is"

    return None


def extract_method(cls_name, fn, src_lines):
    """Extract golden cases and skip-records from one test method."""
    cases = []
    skipped = []
    env = {}
    evaluators = {}
    docstring = ast.get_docstring(fn)
    prev_end = fn.lineno

    for stmt in fn.body:
        comments = comments_before(src_lines, stmt.lineno, prev_end)
        prev_end = getattr(stmt, "end_lineno", stmt.lineno)

        if isinstance(stmt, (ast.Import, ast.ImportFrom)):
            continue

        if isinstance(stmt, ast.Assign):
            if len(stmt.targets) == 1 and isinstance(stmt.targets[0], ast.Name):
                name = stmt.targets[0].id
                value = stmt.value
                if isinstance(value, ast.Call) and isinstance(value.func, ast.Name) \
                        and value.func.id == "ExpressionEvaluator":
                    evaluators[name] = "default" if not value.args and not value.keywords else "custom"
                    continue
                literal, ok = resolve_literal(value, {})
                if ok:
                    env[name] = literal
            continue

        found = extract_assertion(stmt, env, evaluators)
        if found is None:
            continue
        api_call, expected, mode = found

        api, registry_dependent, reason = classify_api_call(api_call, evaluators)
        expr, ctx, registry_kw, ok = api_args(api_call, env)
        if registry_kw:
            skipped.append({
                "test": f"{cls_name}.{fn.name}",
                "reason": "explicit registry= argument (scoped ExpressionRegistry) — "
                          "a non-default function registry is not expressible in the JSON schema",
                "comments": comments,
            })
            continue
        if registry_dependent:
            skipped.append({"test": f"{cls_name}.{fn.name}", "reason": reason, "comments": comments})
            continue
        if not ok:
            skipped.append({
                "test": f"{cls_name}.{fn.name}",
                "reason": "expr/context could not be resolved to literals by the extractor",
                "comments": comments,
            })
            continue

        note_parts = list(comments)
        if mode == "raises":
            note_parts.append("expect native %s (test also asserts the message names the function)" % expected["$error"])
        if mode == "is":
            note_parts.append("assertIs (identity)")
        if api == "parse_function":
            note_parts.append(PARSE_FUNCTION_NOTE)
        if not isinstance(expr, str):
            note_parts.append("non-string passthrough: expr is the raw value, not an expression string")

        cases.append({
            "api": api,
            "expr": expr,
            "context": ctx if ctx is not None else {},
            "expected": expected,
            "docstring": docstring,
            "note_parts": note_parts,
        })
    return cases, skipped


def extract_cases(test_file):
    with open(test_file, "r", encoding="utf-8") as f:
        source = f.read()
    src_lines = source.splitlines()
    tree = ast.parse(source)

    cases = []
    skipped = []
    for cls in tree.body:
        if not isinstance(cls, ast.ClassDef):
            continue
        for fn in cls.body:
            if not isinstance(fn, ast.FunctionDef) or not fn.name.startswith("test_"):
                continue
            method_cases, method_skipped = extract_method(cls.name, fn, src_lines)
            multi = len(method_cases) > 1
            for i, case in enumerate(method_cases):
                note_parts = list(case["note_parts"])
                if i == 0 and case["docstring"]:
                    note_parts.insert(0, case["docstring"])
                note = "; ".join(" ".join(p.split()) for p in note_parts if p).strip()
                out = {
                    "id": "%s.%s%s" % (cls.name, fn.name, "#%d" % (i + 1) if multi else ""),
                    "expr": case["expr"],
                    "context": case["context"],
                    "expected": _jsonable(case["expected"]),
                }
                if note:
                    out["note"] = note
                cases.append((out, case["api"]))
            for rec in method_skipped:
                skipped.append(rec)
    return cases, skipped


# ---------------------------------------------------------------------------
# Normalisation (defensive; the current golden set is all JSON literals)
# ---------------------------------------------------------------------------

def _jsonable(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (set, frozenset, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    return value


# ---------------------------------------------------------------------------
# Verification against the live reference implementation
# ---------------------------------------------------------------------------

def values_equal(actual, expected):
    if isinstance(actual, bool) != isinstance(expected, bool):
        return False
    if type(actual) is not type(expected):
        return False
    return actual == expected


def verify(cases):
    """Re-run every case through plaita; return a list of failure strings."""
    from plaita.io import evaluate, parse_function

    failures = []
    for case, api in cases:
        cid = case["id"]
        expected = case["expected"]
        expr = case["expr"]
        context = case["context"]
        if isinstance(expected, dict) and "$error" in expected:
            try:
                if api == "parse_function":
                    parse_function(expr, context)
                else:
                    evaluate(expr, context)
            except Exception as exc:  # noqa: BLE001
                if type(exc).__name__ != expected["$error"]:
                    failures.append("%s: expected %s, got %s (%s)"
                                    % (cid, expected["$error"], type(exc).__name__, exc))
            else:
                failures.append("%s: expected %s, but no exception was raised"
                                % (cid, expected["$error"]))
            continue
        try:
            actual = parse_function(expr, context) if api == "parse_function" else evaluate(expr, context)
        except Exception as exc:  # noqa: BLE001
            failures.append("%s: raised unexpectedly: %s: %s" % (cid, type(exc).__name__, exc))
            continue
        if not values_equal(actual, expected):
            failures.append("%s: expected %r, got %r" % (cid, expected, actual))
    return failures


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    repo_root = os.getcwd()
    test_file = sys.argv[1] if len(sys.argv) > 1 else os.path.join(repo_root, DEFAULT_TEST_FILE)
    out_path = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_OUT

    cases, skipped = extract_cases(test_file)

    failures = verify(cases)
    if failures:
        print("VERIFICATION FAILED (%d):" % len(failures), file=sys.stderr)
        for line in failures:
            print("  - " + line, file=sys.stderr)
        print("No output written.", file=sys.stderr)
        return 2

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump([case for case, _ in cases], f, indent=2, ensure_ascii=False)
        f.write("\n")

    by_api = {}
    for _, api in cases:
        by_api[api] = by_api.get(api, 0) + 1
    print("exported %d cases -> %s" % (len(cases), out_path))
    print("  by api: " + ", ".join("%s=%d" % kv for kv in sorted(by_api.items())))
    if skipped:
        print("skipped %d case(s) (not expressible in the JSON schema):" % len(skipped))
        for rec in skipped:
            extra = (" | comment: " + " ".join(rec["comments"])) if rec.get("comments") else ""
            print("  - %s: %s%s" % (rec["test"], rec["reason"], extra))
    return 0


if __name__ == "__main__":
    sys.exit(main())
