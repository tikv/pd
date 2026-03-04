#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REFRESH_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SKILLS_ROOT="$(cd "$REFRESH_ROOT/.." && pwd)"
FIX_REF_DIR="${FIX_REF_DIR:-$SKILLS_ROOT/pd-flaky-fix/references}"

OUT_CORPUS="$FIX_REF_DIR/flaky-pr-corpus.jsonl"
OUT_PLAYBOOK="$FIX_REF_DIR/flaky-fix-playbook.md"
SUMMARY_FILE="$REFRESH_ROOT/references/last-refresh-summary.md"

PD_REPO="${PD_REPO:-$(pwd)}"
if ! git -C "$PD_REPO" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "PD_REPO is not a git repository: $PD_REPO" >&2
  exit 1
fi

mkdir -p "$FIX_REF_DIR" "$REFRESH_ROOT/references"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pd-flaky-corpus.XXXXXX")"
trap 'rm -rf "$TMP_DIR"' EXIT

GH_ENHANCE=0
if command -v gh >/dev/null 2>&1 && gh auth status -h github.com >/dev/null 2>&1; then
  GH_ENHANCE=1
fi
export GH_ENHANCE

python3 - "$PD_REPO" "$OUT_CORPUS" "$OUT_PLAYBOOK" "$SUMMARY_FILE" "$TMP_DIR" <<'PY'
import collections
import datetime
import json
import os
import re
import subprocess
import sys
from pathlib import Path

repo = sys.argv[1]
out_corpus = Path(sys.argv[2])
out_playbook = Path(sys.argv[3])
out_summary = Path(sys.argv[4])
tmp_dir = Path(sys.argv[5])
gh_enhance = os.environ.get("GH_ENHANCE", "0") == "1"

keywords_order = ["flaky", "unstable", "timeout", "deadlock", "data race", "go leak", "panic"]
log_pattern = "flaky|unstable|timeout|deadlock|data race|go leak|panic"


def run(cmd):
    return subprocess.check_output(cmd, text=True, errors="replace")


def classify(title_lower, files, kws):
    pats = []
    joined_files = "\n".join(files).lower()
    if "data race" in title_lower or "data race" in kws:
        pats.append("race_elimination")
    if "panic" in title_lower:
        pats.append("panic_guard")
    if "timeout" in title_lower:
        pats.append("timeout_budget")
    if "deadlock" in title_lower:
        pats.append("deadlock_avoidance")
    if "go leak" in title_lower or " leak" in title_lower:
        pats.append("leak_cleanup")
    if "flaky" in title_lower or "unstable" in title_lower:
        pats.append("flaky_stabilization")
    if any(x in title_lower for x in ["leader", "leadership", "watch", "redirect", "notleader"]):
        pats.append("leadership_watch_sync")
    if "test" in title_lower or "tests/" in joined_files or "_test.go" in joined_files:
        pats.append("test_harness_alignment")
    if not pats:
        pats.append("other_stability_fix")
    return sorted(set(pats))


def extract_test_names(text):
    names = set(re.findall(r"Test[A-Za-z0-9_/]+", text))
    return sorted(names)


log_cmd = [
    "git", "-C", repo, "log",
    "--pretty=format:%H\t%ad\t%s",
    "--date=short",
    "--extended-regexp",
    "--regexp-ignore-case",
    f"--grep={log_pattern}",
]

lines = run(log_cmd).splitlines()
rows = []
seen_pr = set()

for line in lines:
    parts = line.split("\t", 2)
    if len(parts) != 3:
        continue
    commit, date_s, title = parts
    m = re.search(r"#(\d+)", title)
    if not m:
        continue
    pr = int(m.group(1))
    if pr in seen_pr:
        continue
    seen_pr.add(pr)

    title_lower = title.lower()
    kws = [k for k in keywords_order if k in title_lower]
    files_raw = run(["git", "-C", repo, "show", "--pretty=format:", "--name-only", commit]).splitlines()
    files = [f.strip() for f in files_raw if f.strip()]
    is_test_related = (
        "test" in title_lower
        or any("tests/" in f.lower() or f.lower().endswith("_test.go") for f in files)
    )

    row = {
        "pr_number": pr,
        "merge_commit": commit,
        "title": title,
        "merged_at": date_s,
        "url": f"https://github.com/tikv/pd/pull/{pr}",
        "keywords": kws,
        "is_test_related": is_test_related,
        "files_changed": files,
        "test_names": extract_test_names(title),
        "fix_patterns": classify(title_lower, files, kws),
        "evidence_snippet": title,
        "source": "local_git",
    }
    rows.append(row)

if gh_enhance:
    for row in rows[:160]:
        pr = row["pr_number"]
        try:
            p = subprocess.run(
                ["gh", "pr", "view", str(pr), "--repo", "tikv/pd", "--json", "title,body,url,mergedAt"],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            if p.returncode != 0:
                continue
            obj = json.loads(p.stdout)
            title = obj.get("title") or row["title"]
            body = obj.get("body") or ""
            merged_at = (obj.get("mergedAt") or "")[:10] or row["merged_at"]
            row["title"] = title
            row["merged_at"] = merged_at
            row["url"] = obj.get("url") or row["url"]
            row["evidence_snippet"] = (body.strip().splitlines() or [title])[0][:280]
            row["keywords"] = [k for k in keywords_order if k in (title + "\n" + body).lower()]
            row["test_names"] = extract_test_names(title + "\n" + body)
            row["fix_patterns"] = classify((title + "\n" + body).lower(), row["files_changed"], row["keywords"])
            row["source"] = "api_enriched"
        except Exception:
            continue

if len(rows) < 100:
    raise SystemExit(f"corpus rows below threshold: {len(rows)} < 100")

# Build playbook templates.
template_defs = [
    ("race_elimination", "Data Race Elimination", ["race_elimination"]),
    ("panic_guard", "Panic Guarding and Nil Safety", ["panic_guard"]),
    ("timeout_budget", "Timeout and Retry Budget Tuning", ["timeout_budget"]),
    ("deadlock_avoidance", "Deadlock Avoidance", ["deadlock_avoidance"]),
    ("flaky_stabilization", "Flaky and Unstable Test Stabilization", ["flaky_stabilization"]),
    ("leak_cleanup", "Goroutine/Leak Cleanup", ["leak_cleanup"]),
    ("leadership_watch_sync", "Leadership/Watch Synchronization", ["leadership_watch_sync"]),
    ("test_harness_alignment", "Test Harness and Fixture Alignment", ["test_harness_alignment"]),
]

by_pattern = collections.defaultdict(list)
for row in rows:
    for p in row["fix_patterns"]:
        by_pattern[p].append(row)

pattern_count = collections.Counter()
keyword_count = collections.Counter()
source_count = collections.Counter()
test_related_count = 0
for r in rows:
    pattern_count.update(r["fix_patterns"])
    keyword_count.update(r["keywords"])
    source_count[r["source"]] += 1
    if r["is_test_related"]:
        test_related_count += 1

playbook_lines = []
playbook_lines.append("# PD Flaky Fix Playbook")
playbook_lines.append("")
playbook_lines.append(f"Generated from {len(rows)} merged flaky-adjacent PRs.")
playbook_lines.append("")
playbook_lines.append("Use these templates as hypothesis starters. Always validate against current issue evidence.")
playbook_lines.append("")

for idx, (key, title, required_keys) in enumerate(template_defs, start=1):
    pool = []
    for req in required_keys:
        pool.extend(by_pattern.get(req, []))
    # Keep unique by pr number in original order.
    seen = set()
    reps = []
    for row in pool:
        if row["pr_number"] in seen:
            continue
        seen.add(row["pr_number"])
        reps.append(row)
        if len(reps) >= 5:
            break
    if not reps:
        reps = rows[:3]

    playbook_lines.append(f"## Pattern {idx}: {title}")
    playbook_lines.append("")
    playbook_lines.append("**Trigger signals**")
    playbook_lines.append("- Failure signature and code shape match this class.")
    playbook_lines.append("- Nearby stable tests use stricter synchronization or cleanup patterns.")
    playbook_lines.append("")
    playbook_lines.append("**Minimal fix strategy**")
    if key == "race_elimination":
        playbook_lines.append("- Remove shared mutable access races with scoped locks/atomics or isolated test fixtures.")
    elif key == "panic_guard":
        playbook_lines.append("- Add nil/state guards before dereference and preserve existing control flow.")
    elif key == "timeout_budget":
        playbook_lines.append("- Replace brittle hard timeout assumptions with condition-aware waits and bounded retries.")
    elif key == "deadlock_avoidance":
        playbook_lines.append("- Break circular waits by adjusting lock/order or test orchestration points.")
    elif key == "flaky_stabilization":
        playbook_lines.append("- Remove non-determinism by making readiness/ownership checks explicit.")
    elif key == "leak_cleanup":
        playbook_lines.append("- Ensure goroutine/resource teardown happens on every exit path.")
    elif key == "leadership_watch_sync":
        playbook_lines.append("- Wait on concrete leader/watch state transitions rather than elapsed time.")
    else:
        playbook_lines.append("- Align setup/teardown and assertion semantics with stable tests in same suite.")

    playbook_lines.append("")
    playbook_lines.append("**TDD guidance**")
    playbook_lines.append("- Add a focused failing test/assertion first.")
    playbook_lines.append("- Keep implementation change minimal and rerun focused test to green.")
    playbook_lines.append("")
    playbook_lines.append("**Verification commands (example)**")
    playbook_lines.append("```bash")
    playbook_lines.append("make gotest GOTEST_ARGS='./<pkg> -run <TestName> -count=1 -v'")
    playbook_lines.append("```")
    playbook_lines.append("")
    playbook_lines.append("**Representative PRs**")
    for r in reps:
        playbook_lines.append(f"- #{r['pr_number']}: {r['title']} ({r['url']})")
    playbook_lines.append("")

summary_lines = []
summary_lines.append("# Last Refresh Summary")
summary_lines.append("")
summary_lines.append(f"- Generated at: {datetime.datetime.utcnow().isoformat()}Z")
summary_lines.append(f"- Total rows: {len(rows)}")
summary_lines.append(f"- Test-related rows: {test_related_count}")
summary_lines.append(f"- Source local_git: {source_count.get('local_git', 0)}")
summary_lines.append(f"- Source api_enriched: {source_count.get('api_enriched', 0)}")
summary_lines.append("")
summary_lines.append("## Keyword Counts")
for k, v in keyword_count.most_common():
    summary_lines.append(f"- {k}: {v}")
summary_lines.append("")
summary_lines.append("## Pattern Counts")
for k, v in pattern_count.most_common():
    summary_lines.append(f"- {k}: {v}")

corpus_tmp = tmp_dir / "flaky-pr-corpus.jsonl"
playbook_tmp = tmp_dir / "flaky-fix-playbook.md"
summary_tmp = tmp_dir / "last-refresh-summary.md"

with corpus_tmp.open("w", encoding="utf-8") as f:
    for row in rows:
        f.write(json.dumps(row, ensure_ascii=True) + "\n")

playbook_tmp.write_text("\n".join(playbook_lines) + "\n", encoding="utf-8")
summary_tmp.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

out_corpus.parent.mkdir(parents=True, exist_ok=True)
out_playbook.parent.mkdir(parents=True, exist_ok=True)
out_summary.parent.mkdir(parents=True, exist_ok=True)

os.replace(corpus_tmp, out_corpus)
os.replace(playbook_tmp, out_playbook)
os.replace(summary_tmp, out_summary)

print(f"rows={len(rows)}")
print(f"test_related_rows={test_related_count}")
print(f"patterns={len(template_defs)}")
print(f"source_local_git={source_count.get('local_git', 0)}")
print(f"source_api_enriched={source_count.get('api_enriched', 0)}")
PY
