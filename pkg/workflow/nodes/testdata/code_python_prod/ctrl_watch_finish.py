
def run(input):
    import fcntl, json, os, tempfile, time

    duty = os.path.expanduser(input.get("duty_dir") or "~/.issue-keeper/duty")
    os.makedirs(duty, exist_ok=True)
    now = time.time()
    ts = time.strftime("%Y-%m-%dT%H:%M:%S+08:00")
    findings = input.get("findings") or []
    status = "attention" if findings else "ok"
    rd = now
    try:
        doc = json.load(open(os.path.join(duty, "state-controller.json")))
        rd = max([r.get("round", 0) for r in (doc.get("rounds") or [])] or [0]) + 1
    except Exception:
        rd = 1
    round_doc = {
        "schema_version": "duty/state@0", "role": "controller", "generation": 2,
        "round": rd, "started_at": ts, "finished_at": ts, "status": status,
        "findings": findings,
        "narrative": str(input.get("report") or "")[:300]
                     + (" | 升级已发" if input.get("posted") else ""),
    }

    def atomic(path, d):
        fd, tmp = tempfile.mkstemp(dir=str(os.path.dirname(path)), prefix=".tmp-")
        with os.fdopen(fd, "w") as f:
            f.write(json.dumps(d, indent=1, ensure_ascii=False))
        os.replace(tmp, path)

    spath = os.path.join(duty, "state-controller.json")
    with open(spath + ".lock", "w") as lf:
        fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
        try:
            doc = {"schema_version": "duty/state@0", "role": "controller", "rounds": []}
            try:
                doc = json.load(open(spath))
            except Exception:
                pass
            meta = doc.get("meta") or {}
            rounds = doc.get("rounds") or []
            rounds.append(round_doc)
            doc.update({"rounds": rounds[-20:], "meta": meta, "updated_at": ts})
            atomic(spath, doc)
        finally:
            fcntl.flock(lf.fileno(), fcntl.LOCK_UN)

    with open(os.path.join(duty, "rounds.log"), "a") as fh:
        fh.write("%s ctrl-watch 轮次=%s 状态=%s findings=%s 升级=%s 摘要=%s\n"
                 % (ts, rd, status, len(findings),
                    "已发" if input.get("posted") else "无",
                    str(input.get("report") or "")[:120]))

    return {"round": rd, "status": status, "posted": bool(input.get("posted"))}
