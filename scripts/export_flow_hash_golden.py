#!/usr/bin/env python3
"""Export the cross-engine flow-definition-hash golden fixture (#60).

For every case the script json.loads the RAW stored-definition text, runs it
through the authoritative plaita implementation
(``plaita.server.flow_worker.compute_flow_definition_json`` /
``compute_flow_definition_hash``), verifies the result, and writes one JSON
fixture for github.com/jeffkit/plaita-go/pkg/worker. The Go test
(flow_hash_golden_test.go) asserts byte-equality of the canonical text and
equality of the sha256 — pinning FlowDefinitionHash to Python, not to a
hand-transcribed expectation.

Usage (run from the plaita Python repo root so ``import plaita`` resolves)::

    cd ~/projects/infra4agent/plaita
    python3 ~/projects/infra4agent/plaita-go/scripts/export_flow_hash_golden.py

The output path defaults to this repository's
``pkg/worker/testdata/flow_hash_golden.json``, resolved from the script's own
location — never from the caller's CWD. A second argument overrides it.

Case families:
    a1_*        the ten families of the A1 r120 three-way comparison matrix
                (html_chars / num_1e2 / u2028 diverged pre-#60; 1.0, bigint,
                -0.0, unicode, control chars, plain, key_order already agreed).
    num_*       float/int literal normalization through Python float.__repr__
                and json.loads int parsing (exponent thresholds, subnormals,
                overflow → Infinity, underflow → 0.0, "-0" → 0, big ints).
    esc_*       CPython ESCAPE_DCT string escaping (\\b/\\f short forms,
                lowercase \\u00xx for other controls, quote/backslash).
    u_*         non-ASCII pass-through: U+2028/U+2029 raw, DEL unescaped,
                code-point key ordering (CJK / accented keys).
    struct_*    nested containers, empty containers, duplicate keys (last
                wins), a realistic multi-node definition.
    err_*       inputs Python json.loads also REJECTS (trailing data):
                exported as expect_error=true so the Go side stays
                fail-closed instead of hashing what Python never would.
    goerr_*     Python json.loads is LENIENT (parse_constant accepts
                NaN/Infinity) but Go's RFC-8259 decoder refuses them —
                exported with go_rejects=true plus the Python-side
                canonical/hash for the record; the Go test pins the
                rejection. Decode-layer acceptance residual (a
                NaN-bearing definition is unrunnable on Go regardless of
                the fingerprint; not one of #60's divergence families).
    e2e_*       the exact definition JSON used by the mixed-fleet end-to-end
                resume test (TestMixedFleetResumeWithPythonHash), so the e2e
                writes a hash produced by real Python into the state row.

Verification aborts the export (exit 2, nothing written) if any raw fails to
parse when it should not, if an err_* case unexpectedly parses, or if any
previously-recorded sample hash from the #60 issue text no longer matches.
"""

from __future__ import annotations

import glob
import json
import os
import re
import sys

from plaita.server.flow_worker import (
    canonical_flow_definition_json,
    compute_flow_definition_hash,
)

# Hand-verified sample hashes quoted in plaita-go#60 (issue body, "实测" table).
# The exporter refuses to write anything if these drift — that would mean the
# Python authority moved and every pinned expectation needs re-review.
ISSUE_ANCHOR_HASHES = {
    "a1_html_chars": "62b1e171117b67ce1a9bf000e74c536bdae37c067d3ce083cfd3dd36e7fcfb9e",
    "num_1e_minus3_issue_sample": "350364ceed555c8745bd706d362cf463c79dc85446408ba4b8dda806a776adcb",
    "a1_plain": "4f3655758adea147d93843bc2d06bc69b0c16b9b36c5494116a3b1bd01c0afb5",
    "a1_num_bigint": "1055d1c6eaaa5faddcd97a9de86be8b77df3f491df4387c1d4f709360aecca83",
}


def cases() -> list[dict]:
    out = []

    def add(
        name: str,
        raw: str,
        *,
        expect_error: bool = False,
        go_rejects: bool = False,
        export_when_py_rejects: bool = False,
    ) -> None:
        out.append(
            {
                "name": name,
                "raw": raw,
                "expect_error": expect_error,
                "go_rejects": go_rejects,
                "export_when_py_rejects": export_when_py_rejects,
            }
        )

    # ---- A1 r120 ten-family matrix --------------------------------------
    add("a1_plain", '{"flow_id":"demo","nodes":[{"type":"http","url":"https://x/y"}]}')
    add(
        "a1_html_chars",
        '{"flow_id":"demo","nodes":[{"cond":"$F.and(a,b)","type":"http","url":"https://x/y?a=1&b=2"}]}',
    )
    add("a1_num_1e2", '{"flow_id":"demo","retry":1e2}')
    add("a1_u2028", '{"flow_id":"demo","u":"xy\u2028z"}')
    add("a1_u2028_escaped_input", '{"flow_id":"demo","u":"xy\\u2028z"}')
    add("a1_num_1_0", '{"flow_id":"demo","ratio":1.0}')
    add("a1_num_bigint", '{"flow_id":"demo","n":12345678901234567890}')
    add("a1_num_negzero_float", '{"flow_id":"demo","n":-0.0}')
    add("a1_unicode_emoji", '{"flow_id":"demo","name":"流程🚀üñï","note":"日本語テキスト"}')
    add("a1_control_chars", '{"flow_id":"demo","desc":"a\\u0001b\\u001fc"}')
    add(
        "a1_key_order",
        '{"nodes":[{"id":"n1","type":"assignment"},{"id":"start","type":"start"}],'
        '"zz_last":1,"aa_first":2,"flow_id":"demo","inputType":"free"}',
    )

    # ---- number normalization through float.__repr__ ---------------------
    add("num_1e_minus3_issue_sample", '{"flow_id":"demo","retry":1e-3}')
    add("num_1e16_exp_form", '{"flow_id":"demo","big":1e16}')
    add("num_1e15_fixed_boundary", '{"flow_id":"demo","big":1e15}')
    add("num_1e-7_two_digit_exp", '{"flow_id":"demo","tiny":1e-7}')
    add("num_1e-4_fixed_boundary", '{"flow_id":"demo","tiny":1e-4}')
    add("num_1_00_trailing_zeros", '{"flow_id":"demo","ratio":1.00}')
    add("num_neg_zero_int", '{"flow_id":"demo","n":-0}')
    add("num_1E2_capital", '{"flow_id":"demo","n":1E2}')
    add("num_1e21", '{"flow_id":"demo","n":1e21}')
    add("num_1e100_three_digit_exp", '{"flow_id":"demo","n":1e100}')
    add("num_5e-324_subnormal", '{"flow_id":"demo","n":5e-324}')
    add("num_max_double", '{"flow_id":"demo","n":1.7976931348623157e308}')
    add("num_overflow_1e400", '{"flow_id":"demo","n":1e400}')
    add("num_neg_overflow_-1e400", '{"flow_id":"demo","n":-1e400}')
    add("num_underflow_1e-400", '{"flow_id":"demo","n":1e-400}')
    add("num_0p1", '{"flow_id":"demo","n":0.1}')
    add("num_0p3", '{"flow_id":"demo","n":0.3}')
    add("num_decpt16_boundary", '{"flow_id":"demo","n":9999999999999998.0}')
    add("num_fractional", '{"flow_id":"demo","n":12.5}')
    add("int_huge_30digits", '{"flow_id":"demo","n":123456789012345678901234567890}')

    # ---- CPython ESCAPE_DCT string escaping ------------------------------
    add("esc_bf_shortforms", '{"flow_id":"demo","s":"x\\u0008y\\u000cz"}')
    add(
        "esc_mixed_controls",
        '{"flow_id":"demo","s":"a\\tb\\nc\\rd\\u0001e\\u001ff\\"g\\\\h"}',
    )

    # ---- non-ASCII pass-through and key ordering -------------------------
    add("u_2029", '{"flow_id":"demo","u":"p\\u2029q"}')
    add("u_del_7f", '{"flow_id":"demo","u":"del\\u007fchar"}')
    add(
        "u_cjk_keys_sorted",
        '{"中文字段":1,"a":2,"z":3,"é":4,"Ω":5,"flow_id":"demo"}',
    )
    add("u_rtl_combining", '{"flow_id":"demo","s":"مرحبا é🇺🇳"}')

    # ---- structural shapes ------------------------------------------------
    add(
        "struct_nested_deep",
        '{"flow_id":"demo","nodes":[{"config":{"list":[1,-2.5,[true,null],"x"],'
        '"map":{"b":{},"a":[]},"deep":{"deeper":{"deepest":{"k":"v"}}}},"id":"n1"}]}',
    )
    add("struct_empty_containers", '{"flow_id":"demo","m":{},"l":[],"nodes":[]}')
    add("struct_mixed_array", '{"flow_id":"demo","l":[1,"1",true,null,1.5,[1],{"a":1}]}')
    add("dup_keys_last_wins", '{"flow_id":"demo","x":1,"x":2}')
    add(
        "struct_big_definition",
        '{"flow_id":"webhook_flow","inputType":"free","nodes":['
        '{"id":"start","type":"start","next":"fetch"},'
        '{"id":"fetch","type":"http","method":"GET","url":"https://api.example.com/v1/items?limit=10&offset=0&page=1","next":"branch"},'
        '{"id":"branch","type":"switch","field":"$NODE.fetch.status","branches":[{"name":"ok","op":"eq","value":200,"next":"notify"},{"name":"default","isDefault":true,"next":"notify"}]},'
        '{"id":"notify","type":"feishu_webhook","url":"https://open.feishu.cn/open-apis/bot/v2/hook/xxx-yyy&zzz","next":"end"},'
        '{"id":"end","type":"end","resultType":"success","output":"$NODE.fetch.data"}],'
        '"timeout":"PT30M","errorHandler":{"retryTimes":3,"retryInterval":1000}}',
    )

    # ---- decode-rejection parity ------------------------------------------
    # err_trailing_data: Python json.loads rejects it too (both sides refuse).
    # goerr_*: Python is lenient (parse_constant), Go's strict RFC-8259
    # decoder refuses the document — recorded residual, Go test pins the
    # rejection (see the goerr_* note in this file's docstring).
    add("err_trailing_data", '{"flow_id":"demo"} trailing', expect_error=True)
    add("goerr_nan_literal", '{"flow_id":"demo","n":NaN}', go_rejects=True)
    add("goerr_infinity_literal", '{"flow_id":"demo","n":Infinity}', go_rejects=True)

    # ---- A1 r132 card G-3: cross-engine load/execute expansion (green) ----
    # Corpus mirrors the plaita-rs tools/diffcheck load/* cases (card R-3);
    # probes from A1 r130-r131 (/tmp/a1r130/flows). neg-infinity completes
    # card G-2 (T8).
    #
    # LAYER NOTE: branches-str / next-int are NOT here — the hash layer
    # legitimately accepts any valid JSON document; Go refuses those two at
    # the ir.Load layer (typed unmarshal / reachability gate), pinned by
    # TestLoadRejects in pkg/ir/ir_test.go. diffcheck (card R-3) pins the
    # same two facts on the go-vs-rust axis.
    add(
        "goerr_neg_infinity_literal",
        '{"flow_id":"probe","nodes":[{"id":"s","type":"start","next":"e",'
        '"output":{"a":-Infinity}},{"id":"e","type":"end"}]}',
        go_rejects=True,
    )
    add(
        "struct_depth_100_nested",
        '{"flow_id":"probe","nodes":[{"id":"s","type":"start","next":"e","output":'
        + "[" * 100 + "]" * 100 + '},{"id":"e","type":"end"}]}',
    )
    # depth 10000: Python's json.loads itself dies with RecursionError at this
    # depth (its ~9998 boundary is environment-sensitive — A1 owner-call O3),
    # so no canonical/hash exists to record. export_when_py_rejects still
    # writes the case as a Go-side rejection pin. The 150-deep variant stays
    # OUT of the golden on purpose: Go accepts / Rust rejects at 128 — a
    # permanent cross-engine divergence (plaita-rs AGENTS.md), never a case.
    add(
        "err_depth_10000",
        '{"flow_id":"probe","nodes":[{"id":"s","type":"start","next":"e","output":'
        + "[" * 10000 + "]" * 10000 + '},{"id":"e","type":"end"}]}',
        expect_error=True,
        go_rejects=True,
        export_when_py_rejects=True,
    )

    # ---- e2e mixed-fleet definition (must stay loadable by ir.Load) -------
    # The definition deliberately carries #60 divergence content: '&' and
    # '<>' in a string plus an exponent-form numeric literal — exactly the
    # shapes where pre-fix Go and Python disagreed on the fingerprint, so the
    # mixed-fleet e2e is genuinely red against the pre-fix algorithm.
    e2e_raw = """{
      "flow_id": "mixed_fleet_flow",
      "version": "1",
      "description": "wait & see <prod> webhook?a=1&b=2",
      "retry_hint": 1e3,
      "nodes": [
        {"id": "start", "type": "start", "next": "wait"},
        {"id": "wait", "type": "event", "event_type": "approval", "next": "end"},
        {"id": "end", "type": "end", "resultType": "success", "output": "$NODE.wait.status"}
      ]
    }"""
    add("e2e_mixed_fleet_flow", e2e_raw)

    # ---- corpus replay: every flow JSON already in this repository --------
    # plaita-go#60 acceptance: replay the repo's own testdata/examples
    # documents through both engines so real data shapes are pinned too, not
    # just hand-picked samples.
    repo_root = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir))
    corpus_globs = [
        os.path.join(repo_root, "pkg", "ir", "testdata", "*.json"),
        os.path.join(repo_root, "examples", "**", "*.json"),
    ]
    seen = set()
    for pattern in corpus_globs:
        for path in sorted(glob.glob(pattern, recursive=True)):
            if os.path.abspath(path) in seen:
                continue
            seen.add(os.path.abspath(path))
            rel = os.path.relpath(path, repo_root)
            name = "corpus_" + re.sub(r"[^A-Za-z0-9]+", "_", rel)[:80]
            with open(path, "r", encoding="utf-8") as fh:
                add(name, fh.read())

    return out


def main() -> int:
    here = os.path.dirname(os.path.abspath(__file__))
    default_out = os.path.join(here, os.pardir, "pkg", "worker", "testdata", "flow_hash_golden.json")
    out_path = os.path.abspath(sys.argv[1] if len(sys.argv) > 1 else default_out)

    exported = []
    for case in cases():
        raw = case["raw"]
        try:
            doc = json.loads(raw)
        except Exception:
            if case["expect_error"]:
                if case["export_when_py_rejects"]:
                    # Python's own loader refuses the document (no
                    # canonical/hash exists) — still record the case as a
                    # Go-side rejection pin.
                    exported.append(
                        {
                            "name": case["name"],
                            "raw": raw,
                            "python_canonical": "",
                            "python_hash": "",
                            "go_rejects": True,
                        }
                    )
                continue
            print(f"FAIL: {case['name']}: raw does not parse: {raw!r}", file=sys.stderr)
            return 2
        if case["expect_error"]:
            print(
                f"FAIL: {case['name']}: expected json.loads rejection but it parsed: {raw!r}",
                file=sys.stderr,
            )
            return 2
        canonical = canonical_flow_definition_json(doc)
        digest = compute_flow_definition_hash(doc)
        anchor = ISSUE_ANCHOR_HASHES.get(case["name"])
        if anchor is not None and digest != anchor:
            print(
                f"FAIL: {case['name']}: hash {digest} no longer matches the "
                f"#60 issue anchor {anchor} — Python authority moved, re-review",
                file=sys.stderr,
            )
            return 2
        exported.append(
            {
                "name": case["name"],
                "raw": raw,
                "python_canonical": canonical,
                "python_hash": digest,
                "go_rejects": bool(case["go_rejects"]),
            }
        )

    if len(exported) < 40:
        print(f"FAIL: only {len(exported)} cases exported, matrix shrunk", file=sys.stderr)
        return 2

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    payload = {
        "source": "plaita.server.flow_worker.compute_flow_definition_hash (live, see scripts/export_flow_hash_golden.py)",
        "algo_python": "flow-definition-json-sortkeys-v2",
        "algo_go": "flow-definition-json-sortkeys-v2-go",
        "cases": exported,
    }
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=True, indent=2)
        fh.write("\n")
    print(f"exported {len(exported)} cases -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
