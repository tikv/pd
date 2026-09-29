#!/usr/bin/env python3
"""Legacy helpers for PD CI collection and raw log download."""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
import os
import re
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path
from typing import Any


UTC = dt.timezone.utc
PROGRESS_ENV = "PD_CI_FLAKY_PROGRESS"
FIXED_ACTION_EVENTS = {"pull_request", "push"}
MASTER_BRANCH = "master"


@dataclasses.dataclass
class FailureRecord:
    record_id: str
    source: str
    ci_name: str
    ci_url: str
    log_url: str | None
    occurred_at: str
    pr_number: int | None
    commit_sha: str | None
    run_id: str | None
    job_id: int | None
    status: str


@dataclasses.dataclass
class RunSummary:
    scanned_window_start: str
    scanned_window_end: str
    prow_records: int = 0
    actions_records: int = 0
    log_spool_dir: str = ""
    skipped_unknown: list[str] = dataclasses.field(default_factory=list)
    command_failed_after_retries: list[str] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class DownloadedLog:
    record: FailureRecord
    path: Path


def now_utc() -> dt.datetime:
    return dt.datetime.now(tz=UTC)


def progress_enabled() -> bool:
    value = os.getenv(PROGRESS_ENV, "1").strip().lower()
    return value not in {"0", "false", "off", "no"}


def log_progress(message: str) -> None:
    if not progress_enabled():
        return
    ts = now_utc().isoformat(timespec="seconds")
    print(f"[progress] {ts} {message}", file=sys.stderr, flush=True)


def parse_iso8601(ts: str | None) -> dt.datetime:
    if not ts:
        return dt.datetime.fromtimestamp(0, tz=UTC)
    normalized = ts.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    return dt.datetime.fromisoformat(normalized).astimezone(UTC)


def run_cmd(
    cmd: list[str],
    summary: RunSummary,
    retries: int,
    timeout: int = 600,
) -> tuple[bool, str, str]:
    last_stdout = ""
    last_stderr = ""
    for attempt in range(1, retries + 1):
        try:
            proc = subprocess.run(
                cmd,
                check=False,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        except Exception as exc:  # noqa: BLE001
            proc = None
            last_stderr = str(exc)
        else:
            last_stdout = proc.stdout or ""
            last_stderr = proc.stderr or ""
            if proc.returncode == 0:
                return True, last_stdout, last_stderr
            last_stderr = f"exit={proc.returncode}; {last_stderr.strip()}"

        if attempt < retries:
            time.sleep(2 ** (attempt - 1))

    snippet = " ".join(cmd[:4])
    summary.command_failed_after_retries.append(f"{snippet}: {last_stderr[:240]}")
    return False, last_stdout, last_stderr


def run_gh_json(args: list[str], summary: RunSummary, retries: int) -> Any | None:
    ok, out, _ = run_cmd(["gh", *args], summary=summary, retries=retries)
    if not ok:
        return None
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        summary.skipped_unknown.append(f"json_decode_failed: gh {' '.join(args[:4])}")
        return None


def run_curl_text(url: str, summary: RunSummary, retries: int) -> str | None:
    ok, out, _ = run_cmd(["curl", "-L", "-s", url], summary=summary, retries=retries)
    if not ok:
        return None
    return out


def extract_json_array_after(text: str, marker: str) -> str | None:
    idx = text.find(marker)
    if idx < 0:
        return None
    start = text.find("[", idx)
    if start < 0:
        return None

    in_string = False
    escaped = False
    depth = 0
    for i in range(start, len(text)):
        ch = text[i]
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
            continue
        if ch == "[":
            depth += 1
            continue
        if ch == "]":
            depth -= 1
            if depth == 0:
                return text[start : i + 1]
    return None


def extract_older_runs_url(page_html: str) -> str | None:
    match = re.search(r'href="([^"]+buildId=[^"]+)"[^>]*>&lt;- Older Runs', page_html)
    if not match:
        return None
    return urllib.parse.urljoin("https://prow.tidb.net", match.group(1))


def spyglass_to_build_log_url(spyglass_link: str) -> str | None:
    if not spyglass_link:
        return None
    trimmed = spyglass_link.split("#", 1)[0]
    prefix = "/view/gs/"
    if prefix not in trimmed:
        return None
    rel = trimmed.split(prefix, 1)[1].strip("/")
    if not rel:
        return None
    return f"https://storage.googleapis.com/{rel}/build-log.txt"


def build_refs_match_repo(build: dict[str, Any], target_repo: str) -> bool:
    if "/" not in target_repo:
        return True
    target_org, target_name = target_repo.split("/", 1)
    target_org = target_org.lower().strip()
    target_name = target_name.lower().strip()

    refs = build.get("Refs", {}) or {}
    org = str(refs.get("org") or "").lower().strip()
    repo = str(refs.get("repo") or "").lower().strip()
    if org and repo:
        return org == target_org and repo == target_name

    pulls = refs.get("pulls", []) or []
    for pull in pulls:
        pull_org = str(pull.get("org") or pull.get("author") or "").lower().strip()
        pull_repo = str(pull.get("repo") or "").lower().strip()
        if pull_org and pull_repo and pull_org == target_org and pull_repo == target_name:
            return True

    spyglass_link = str(build.get("SpyglassLink") or "").lower()
    return f"/pull/{target_org}_{target_name}/" in spyglass_link


def looks_test_related(name: str) -> bool:
    lowered = name.lower()
    return bool(re.search(r"(test|tests|unit|integration|tso|goleak|pull-unit-test|pd test|chunks)", lowered))


def is_release_branch_job(job_name: str) -> bool:
    lowered = (job_name or "").lower()
    return "/release-" in lowered or lowered.startswith("release-")


def build_targets_master(build: dict[str, Any]) -> bool:
    refs = build.get("Refs", {}) or {}
    pulls = refs.get("pulls", []) or []
    if not pulls:
        return True
    base_ref = str(refs.get("base_ref") or refs.get("baseRef") or "").strip()
    return base_ref == MASTER_BRANCH


def actions_run_targets_master(
    repo: str,
    run: dict[str, Any],
    summary: RunSummary,
    retries: int,
) -> bool:
    event = str(run.get("event") or "").lower()
    head_branch = str(run.get("headBranch") or "").strip()
    if event == "push":
        return head_branch == MASTER_BRANCH
    if event != "pull_request":
        return False
    if not head_branch:
        return False

    prs = run_gh_json(
        [
            "pr",
            "list",
            "--repo",
            repo,
            "--head",
            head_branch,
            "--state",
            "all",
            "--json",
            "baseRefName,headRefName,headRefOid",
        ],
        summary=summary,
        retries=retries,
    )
    if not prs:
        return False

    head_sha = str(run.get("headSha") or "").strip()
    for pr in prs:
        if str(pr.get("baseRefName") or "").strip() != MASTER_BRANCH:
            continue
        if head_sha and str(pr.get("headRefOid") or "").strip() == head_sha:
            return True

    for pr in prs:
        if str(pr.get("baseRefName") or "").strip() != MASTER_BRANCH:
            continue
        if str(pr.get("headRefName") or "").strip() == head_branch:
            return True
    return False


def collect_prow_jobs(repo: str, summary: RunSummary, retries: int) -> list[dict[str, Any]]:
    configured_url = f"https://prow.tidb.net/configured-jobs/{repo}"
    html = run_curl_text(configured_url, summary=summary, retries=retries)
    if not html:
        return []

    blob = extract_json_array_after(html, "let includedRepos =")
    if not blob:
        summary.skipped_unknown.append("unknown-log-source: cannot parse configured-jobs page")
        return []

    try:
        included = json.loads(blob)
    except json.JSONDecodeError:
        summary.skipped_unknown.append("json_decode_failed: configured-jobs")
        return []

    jobs = included[0].get("jobs", []) if included else []
    filtered_jobs = []
    skipped_release_jobs = 0
    for job in jobs:
        job_name = job.get("name", "")
        if not (looks_test_related(job_name) and job.get("jobHistoryLink")):
            continue
        if is_release_branch_job(job_name):
            skipped_release_jobs += 1
            continue
        filtered_jobs.append(job)

    if skipped_release_jobs:
        log_progress(f"skip release-branch prow jobs: {skipped_release_jobs}")
    return filtered_jobs


def collect_prow_failures(
    repo: str,
    since: dt.datetime,
    max_pages: int,
    summary: RunSummary,
    retries: int,
) -> tuple[list[FailureRecord], dict[str, set[str]]]:
    jobs = collect_prow_jobs(repo=repo, summary=summary, retries=retries)
    log_progress(f"prow jobs discovered: {len(jobs)}")
    failures: list[FailureRecord] = []
    outcomes_by_ci_sha: dict[str, set[str]] = {}

    for job in jobs:
        job_name = job.get("name", "")
        log_progress(f"prow job start: {job_name}")
        next_url = urllib.parse.urljoin("https://prow.tidb.net", job.get("jobHistoryLink", ""))
        pages = 0
        while next_url and pages < max_pages:
            html = run_curl_text(next_url, summary=summary, retries=retries)
            if not html:
                break
            if "failed to get job history" in html:
                summary.skipped_unknown.append(f"unknown-log-source: {job_name} history unavailable")
                break

            blob = extract_json_array_after(html, "var allBuilds =")
            if not blob:
                break
            try:
                builds = json.loads(blob)
            except json.JSONDecodeError:
                summary.skipped_unknown.append(f"json_decode_failed: allBuilds {job_name}")
                break

            if not builds:
                break

            all_older = True
            for build in builds:
                started = parse_iso8601(build.get("Started"))
                if started >= since:
                    all_older = False
                else:
                    continue

                if not build_refs_match_repo(build, repo):
                    continue
                if not build_targets_master(build):
                    continue

                result = str(build.get("Result", "")).upper()
                refs = build.get("Refs", {}) or {}
                pulls = refs.get("pulls", []) or []
                pr_number = pulls[0].get("number") if pulls else None
                commit_sha = pulls[0].get("sha") if pulls else refs.get("base_sha")
                ci_sha_key = f"{job_name}::{commit_sha or 'none'}"
                outcomes_by_ci_sha.setdefault(ci_sha_key, set()).add(result)

                if result not in {"FAILURE", "ERROR"}:
                    continue
                spyglass_link = build.get("SpyglassLink", "")
                ci_url = urllib.parse.urljoin("https://prow.tidb.net", spyglass_link)
                log_url = spyglass_to_build_log_url(spyglass_link)
                failures.append(
                    FailureRecord(
                        record_id=f"prow-{job_name}-{build.get('ID', '')}",
                        source="prow",
                        ci_name=job_name,
                        ci_url=ci_url,
                        log_url=log_url,
                        occurred_at=started.isoformat(),
                        pr_number=pr_number,
                        commit_sha=commit_sha,
                        run_id=str(build.get("ID", "")),
                        job_id=None,
                        status=result,
                    )
                )

            pages += 1
            if pages % 5 == 0:
                log_progress(f"prow job progress: {job_name} pages={pages} failures={len(failures)}")
            if all_older:
                break
            next_url = extract_older_runs_url(html)
        log_progress(f"prow job done: {job_name} pages={pages}")

    return failures, outcomes_by_ci_sha


def collect_actions_failures(
    repo: str,
    since: dt.datetime,
    max_runs: int,
    summary: RunSummary,
    retries: int,
) -> list[FailureRecord]:
    log_progress("querying github actions run list")
    run_list = run_gh_json(
        [
            "run",
            "list",
            "--repo",
            repo,
            "--status",
            "failure",
            "--limit",
            str(max_runs),
            "--json",
            "databaseId,name,url,event,createdAt,headSha,headBranch,displayTitle",
        ],
        summary=summary,
        retries=retries,
    )
    if run_list is None:
        return []
    log_progress(f"github actions runs fetched: {len(run_list)}")

    failures: list[FailureRecord] = []
    for run in run_list:
        created_at = parse_iso8601(run.get("createdAt"))
        if created_at < since:
            continue
        event = str(run.get("event") or "").lower()
        if event not in FIXED_ACTION_EVENTS:
            continue
        if not actions_run_targets_master(repo=repo, run=run, summary=summary, retries=retries):
            continue

        run_name = run.get("name", "")
        run_id = str(run.get("databaseId", ""))
        log_progress(f"inspecting run {run_id}: {run_name}")

        run_view = run_gh_json(
            ["run", "view", run_id, "--repo", repo, "--json", "jobs"],
            summary=summary,
            retries=retries,
        )
        if not run_view:
            continue

        for job in run_view.get("jobs", []) or []:
            conclusion = str(job.get("conclusion") or "").lower()
            if conclusion not in {"failure", "timed_out"}:
                continue

            job_name = job.get("name", "")
            if not (looks_test_related(job_name) or "chunks" in job_name.lower()):
                continue

            job_id = job.get("databaseId")
            failures.append(
                FailureRecord(
                    record_id=f"actions-{run_id}-{job_id}",
                    source="actions",
                    ci_name=f"{run_name} / {job_name}",
                    ci_url=job.get("url") or run.get("url") or "",
                    log_url=None,
                    occurred_at=created_at.isoformat(),
                    pr_number=None,
                    commit_sha=run.get("headSha"),
                    run_id=run_id,
                    job_id=job_id,
                    status=conclusion.upper(),
                )
            )
        if failures and len(failures) % 10 == 0:
            log_progress(f"github actions failure records collected: {len(failures)}")

    return failures


def fetch_log_text(
    record: FailureRecord,
    repo: str,
    summary: RunSummary,
    retries: int,
) -> str | None:
    if record.source == "prow":
        if not record.log_url:
            summary.skipped_unknown.append(f"unknown-log-source: {record.ci_url}")
            return None
        return run_curl_text(record.log_url, summary=summary, retries=retries)

    if record.source == "actions":
        if record.job_id is None:
            summary.skipped_unknown.append(f"unknown-log-source: {record.ci_url}")
            return None
        ok, out, _ = run_cmd(
            ["gh", "api", f"repos/{repo}/actions/jobs/{record.job_id}/logs"],
            summary=summary,
            retries=retries,
            timeout=900,
        )
        if not ok:
            return None
        return out

    summary.skipped_unknown.append(f"unknown-log-source: {record.ci_url}")
    return None


def sanitize_record_id(record_id: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", record_id).strip("._")
    if not cleaned:
        return "record"
    return cleaned[:180]


def write_log_file_atomic(path: Path, content: str) -> None:
    tmp_path = path.with_suffix(path.suffix + ".part")
    with open(tmp_path, "w", encoding="utf-8", errors="replace") as f:
        f.write(content)
    os.replace(tmp_path, path)


def fetch_and_spool_log(
    record: FailureRecord,
    repo: str,
    summary: RunSummary,
    retries: int,
    spool_dir: Path,
) -> tuple[DownloadedLog | None, str | None]:
    log_text = fetch_log_text(record=record, repo=repo, summary=summary, retries=retries)
    if not log_text:
        return None, "empty_or_unavailable"

    path = spool_dir / f"{sanitize_record_id(record.record_id)}.log"
    try:
        write_log_file_atomic(path, log_text)
    except Exception as exc:  # noqa: BLE001
        return None, str(exc)

    return DownloadedLog(record=record, path=path), None


def ensure_gh_auth(summary: RunSummary, retries: int) -> bool:
    log_progress("checking gh auth status")
    ok, _, _ = run_cmd(["gh", "auth", "status"], summary=summary, retries=retries)
    return ok
