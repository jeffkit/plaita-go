
def run(input):
    import json, time, pathlib
    base = pathlib.Path.home() / ".issue-keeper/pipeline/controller"
    ts = time.strftime("%Y-%m-%d %H:%M")
    stamp = time.strftime("%Y%m%d-%H%M")
    text = str(input.get("text") or "")
    findings = []
    try:
        findings = json.loads(input.get("report") or "{}").get("findings") or []
    except Exception:
        pass

    one = " ".join(text.strip().splitlines()[0:1])[:160]
    line = (f"{ts} B-flow 轮次=keeper-watch（值守） "
            f"落地=简报发 #3 + handoffs/B-handoff-{stamp}.md "
            f"findings={len(findings)} 摘要={one}\n")
    with open(base / "rounds.log", "a", encoding="utf-8") as fh:
        fh.write(line)

    hd = (f"# B 班滚动交接（keeper-watch 值守轮 {ts} 自动生成）\n\n"
          f"**findings**：{json.dumps(findings, ensure_ascii=False)}\n\n"
          f"**本轮总结**：\n\n{text}\n\n"
          f"---\n（本文件由 keeper-watch flow 生成；下一轮开机读最新 B-handoff-*.md）\n")
    out = base / "handoffs" / f"B-handoff-{stamp}.md"
    out.write_text(hd, encoding="utf-8")
    return {"handoff": str(out), "rounds_line": line.strip()}
