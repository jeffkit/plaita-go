import os, subprocess


def run(input):
    wt = input["wt"]
    env = dict(os.environ)
    env["GIT_EDITOR"] = "true"
    env["GIT_SEQUENCE_EDITOR"] = "true"

    def git(*a):
        return subprocess.run(["git", "-C", wt] + list(a),
                              capture_output=True, text=True, env=env)

    def rebasing():
        gd = git("rev-parse", "--git-dir").stdout.strip()
        gd = gd if os.path.isabs(gd) else os.path.join(wt, gd)
        return (os.path.isdir(os.path.join(gd, "rebase-merge"))
                or os.path.isdir(os.path.join(gd, "rebase-apply")))

    def unmerged():
        return git("diff", "--name-only", "--diff-filter=U").stdout.strip()

    def done():
        return git("merge-base", "--is-ancestor", "origin/main",
                   "HEAD").returncode == 0

    def scene():
        NL = "\n"
        return NL.join(["git status --short",
                        git("status", "--short").stdout.strip(), "",
                        "unmerged files", unmerged(), "",
                        "conflict diff", git("diff").stdout[:6000], "",
                        "origin/main..HEAD",
                        git("log", "--oneline", "origin/main..HEAD")
                        .stdout.strip()[-2000:]])

    # agent 解完可能已 continue，也可能只 add 未 continue：替它把 rebase 推完
    why = ""
    for _ in range(50):
        if not rebasing() or unmerged():
            break
        c = git("rebase", "--continue")
        if c.returncode != 0:
            why = ((c.stderr or "") + (c.stdout or ""))[-400:]
            break
    if not rebasing() and not done():
        # agent 可能 abort 后重试：现场没在 rebase，就自己重来一次定论
        rr = git("rebase", "origin/main")
        if rr.returncode != 0:
            why = why or ((rr.stderr or "") + (rr.stdout or ""))[-400:]
    if rebasing() or unmerged() or not done():
        s = scene()
        if rebasing():
            git("rebase", "--abort")
        return {"ok": False, "scene": s,
                "why": why or "unresolved rebase conflict"}
    git("push", "origin", "--delete", input["branch"])
    return {"ok": True, "scene": "", "why": ""}
