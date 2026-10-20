#!/usr/bin/env bash
# Gate on the gate: the workflow hardening recorded in the round-62 ledger
# (same family as plaita B59-06 / A60-02) must not silently regress.
#
#   * every `uses:` is pinned to a 40-char commit SHA with a `# vX.Y.Z` comment
#     (a moved upstream tag/branch cannot change what runs; the comment is what
#     lets dependabot keep the pin current);
#   * no floating toolchain (`go-version: stable|latest`);
#   * every workflow declares `permissions:` (nothing runs with default scope);
#   * every job is bounded (`timeout-minutes:`);
#   * checkout does not leave its token in `.git/config`.
#
# Exit: 0 clean, 1 findings. Deps: bash + grep + sed only.
set -uo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"

shopt -s nullglob
files=(.github/workflows/*.yml .github/workflows/*.yaml)
if [ ${#files[@]} -eq 0 ]; then
  echo "ci-workflow-audit: no workflows under .github/workflows/" >&2
  exit 1
fi

findings=0
report() {
  printf '%s: %s\n' "$1" "$2" >&2
  findings=$((findings + 1))
}

for f in "${files[@]}"; do
  # 1. Every `uses:` pinned. `./path` local actions are exempt.
  while IFS= read -r hit; do
    lineno=${hit%%:*}
    text=${hit#*:}
    ref=$(printf '%s\n' "$text" \
      | sed -E 's/^[[:space:]]*-?[[:space:]]*uses:[[:space:]]*//; s/[[:space:]]+#.*$//')
    case "$ref" in
      ./*) continue ;;
    esac
    if ! printf '%s' "$ref" | grep -qE '@[0-9a-f]{40}$'; then
      report "$f:$lineno" "uses: not pinned to a 40-char SHA -> $ref"
      continue
    fi
    printf '%s\n' "$text" | grep -qE '#[[:space:]]*v[0-9]' \
      || report "$f:$lineno" "pinned SHA without '# vX.Y.Z' comment -> $ref"
  done < <(grep -nE '^[[:space:]]*(-[[:space:]]*)?uses:' "$f")

  # 2. Toolchain must come from the module, not from a floating label.
  #    Full-line comments are blanked first: the header documents the old shape.
  if sed -E 's/^[[:space:]]*#.*$//' "$f" | grep -nE "go-version:[[:space:]]*['\"]?(stable|latest)"; then
    report "$f" "floating go-version (stable/latest); use go-version-file: go.mod"
  fi

  # 3. Least privilege declared explicitly.
  grep -qE '^permissions:' "$f" || report "$f" "no top-level permissions: block"

  # 4. Bounded jobs.
  grep -qE '^[[:space:]]+timeout-minutes:[[:space:]]*[0-9]+' "$f" \
    || report "$f" "no job timeout-minutes:"

  # 5. Checkout token must not persist into later steps.
  if grep -qE '^[[:space:]]*(-[[:space:]]*)?uses:[[:space:]]*actions/checkout@' "$f"; then
    grep -qE '^[[:space:]]+persist-credentials:[[:space:]]*false' "$f" \
      || report "$f" "actions/checkout leaves its token in .git/config (persist-credentials: false)"
  fi
done

if [ "$findings" -ne 0 ]; then
  echo "ci-workflow-audit: $findings finding(s)" >&2
  exit 1
fi
echo "ci-workflow-audit: ${#files[@]} workflow file(s) clean"
