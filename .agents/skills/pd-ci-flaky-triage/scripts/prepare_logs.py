#!/usr/bin/env python3
"""Collect recent failures and fetch raw logs into source-specific artifacts."""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import importlib.util
import json
import os
import re
import sys
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
LEGACY_PATH = SCRIPT_DIR / "triage_pd_ci_flaky.py"
LEGACY_SPEC = importlib.util.spec_from_file_location("pd_ci_flaky_legacy", LEGACY_PATH)
LEGACY = importlib.util.module_from_spec(LEGACY_SPEC)
assert LEGACY_SPEC and LEGACY_SPEC.loader
sys.modules[LEGACY_SPEC.name] = LEGACY
LEGACY_SPEC.loader.exec_module(LEGACY)

UTC = dt.timezone.utc
DEFAULT_RUN_ROOT = Path("/tmp/pd-ci-flaky")


def write_json(path: str | Path, payload: object) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)


def build_window_payload(start: dt.datetime, end: dt.datetime) -> dict[str, str]:
    return {
        "start": start.astimezone(UTC).isoformat(),
        "end": end.astimezone(UTC).isoformat(),
    }


def parse_start_from(value: str) -> dt.datetime:
    raw = value.strip()
    if not raw:
        raise ValueError("start-from cannot be empty")
    if raw.endswith("Z") or re.search(r"[+-]\d{2}:\d{2}$", raw):
        return LEGACY.parse_iso8601(raw)
    parsed = dt.datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def resolve_window(*, end: dt.datetime, days: int, start_from: str) -> tuple[dt.datetime, dt.datetime]:
    if start_from:
        start = parse_start_from(start_from)
        return start, start + dt.timedelta(days=days)
    return end - dt.timedelta(days=days), end


def build_default_run_dir(
    *,
    base_dir: str | Path = DEFAULT_RUN_ROOT,
    now: dt.datetime | None = None,
    pid: int | None = None,
) -> Path:
    timestamp = (now or dt.datetime.now(tz=UTC)).strftime("%Y%m%dT%H%M%SZ")
    return Path(base_dir) / f"run-{timestamp}-{pid or os.getpid()}"


def resolve_run_dir(
    run_dir: str,
    *,
    now: dt.datetime | None = None,
    pid: int | None = None,
) -> Path:
    target = Path(run_dir) if run_dir else build_default_run_dir(now=now, pid=pid)
    target.mkdir(parents=True, exist_ok=True)
    return target


def resolve_output_paths(args: argparse.Namespace, run_dir: Path) -> dict[str, Path]:
    return {
        "prow_failures": Path(args.prow_failures_json) if args.prow_failures_json else run_dir / "prow_failures.json",
        "actions_failures": Path(args.actions_failures_json) if args.actions_failures_json else run_dir / "actions_failures.json",
        "prow_logs": Path(args.prow_logs_json) if args.prow_logs_json else run_dir / "prow_logs.json",
        "actions_logs": Path(args.actions_logs_json) if args.actions_logs_json else run_dir / "actions_logs.json",
    }


def resolve_log_spool_root(log_spool_dir: str, run_dir: Path) -> Path:
    target = Path(log_spool_dir) if log_spool_dir else run_dir / "raw-logs"
    target.mkdir(parents=True, exist_ok=True)
    return target


def split_actions_ci_name(ci_name: str) -> tuple[str, str]:
    if " / " in ci_name:
        workflow_name, job_name = ci_name.split(" / ", 1)
        return workflow_name, job_name
    return ci_name, ci_name


def serialize_prow_failure(record: Any) -> dict[str, Any]:
    return {
        "source": "prow",
        "source_item_id": record.record_id,
        "job_name": record.ci_name,
        "ci_name": record.ci_name,
        "ci_url": record.ci_url,
        "log_url": record.log_url,
        "occurred_at": record.occurred_at,
        "pr_number": record.pr_number,
        "commit_sha": record.commit_sha,
        "build_id": record.run_id,
        "status": record.status,
    }


def serialize_actions_failure(record: Any) -> dict[str, Any]:
    workflow_name, job_name = split_actions_ci_name(record.ci_name)
    return {
        "source": "actions",
        "source_item_id": record.record_id,
        "workflow_name": workflow_name,
        "job_name": job_name,
        "ci_name": record.ci_name,
        "ci_url": record.ci_url,
        "occurred_at": record.occurred_at,
        "commit_sha": record.commit_sha,
        "run_id": record.run_id,
        "job_id": record.job_id,
        "status": record.status,
    }


def deserialize_failure_item(item: dict[str, Any]) -> Any:
    source = item["source"]
    if source == "prow":
        return LEGACY.FailureRecord(
            record_id=item["source_item_id"],
            source="prow",
            ci_name=item["ci_name"],
            ci_url=item["ci_url"],
            log_url=item.get("log_url"),
            occurred_at=item["occurred_at"],
            pr_number=item.get("pr_number"),
            commit_sha=item.get("commit_sha"),
            run_id=str(item.get("build_id") or ""),
            job_id=None,
            status=item["status"],
        )
    if source == "actions":
        return LEGACY.FailureRecord(
            record_id=item["source_item_id"],
            source="actions",
            ci_name=item["ci_name"],
            ci_url=item["ci_url"],
            log_url=None,
            occurred_at=item["occurred_at"],
            pr_number=None,
            commit_sha=item.get("commit_sha"),
            run_id=str(item.get("run_id") or ""),
            job_id=item.get("job_id"),
            status=item["status"],
        )
    raise ValueError(f"unsupported failure source: {source}")


def _prepare_log_root(source: str, base_dir: str | Path | None) -> Path:
    root = Path(base_dir) if base_dir else build_default_run_dir()
    target = root / source
    target.mkdir(parents=True, exist_ok=True)
    return target


def _serialize_log_item(source_item: dict[str, Any], downloaded: Any) -> dict[str, Any]:
    item = {
        "source": source_item["source"],
        "source_item_id": source_item["source_item_id"],
        "ci_name": source_item["ci_name"],
        "ci_url": source_item["ci_url"],
        "log_ref": str(downloaded.path),
        "occurred_at": source_item["occurred_at"],
        "commit_sha": source_item.get("commit_sha"),
        "status": source_item["status"],
    }
    if source_item["source"] == "prow":
        item["job_name"] = source_item["job_name"]
        item["pr_number"] = source_item.get("pr_number")
        item["build_id"] = source_item.get("build_id")
        item["log_url"] = source_item.get("log_url")
    else:
        item["workflow_name"] = source_item.get("workflow_name")
        item["job_name"] = source_item.get("job_name")
        item["run_id"] = source_item.get("run_id")
        item["job_id"] = source_item.get("job_id")
    return item


def fetch_logs_for_failures(
    *,
    failures_payload: dict[str, Any],
    repo: str,
    retries: int,
    download_workers: int = 8,
    log_spool_dir: str | None = None,
) -> dict[str, Any]:
    summary = LEGACY.RunSummary(
        scanned_window_start=failures_payload["window"]["start"],
        scanned_window_end=failures_payload["window"]["end"],
    )
    source = failures_payload["source"]
    records = [deserialize_failure_item(item) for item in failures_payload.get("failures", [])]
    source_items = {item["source_item_id"]: item for item in failures_payload.get("failures", [])}
    spool_dir = _prepare_log_root(source, log_spool_dir)
    logs: list[dict[str, Any]] = []
    skipped: list[dict[str, str]] = []

    def fetch(record: Any) -> tuple[str, Any | None, str | None]:
        downloaded, err = LEGACY.fetch_and_spool_log(
            record=record,
            repo=repo,
            summary=summary,
            retries=retries,
            spool_dir=spool_dir,
        )
        return record.record_id, downloaded, err

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, download_workers)) as executor:
        future_map = {executor.submit(fetch, record): record for record in records}
        for future in concurrent.futures.as_completed(future_map):
            record_id, downloaded, err = future.result()
            if err or downloaded is None:
                skipped.append({"source_item_id": record_id, "reason": err or "empty_or_unavailable"})
                continue
            logs.append(_serialize_log_item(source_items[record_id], downloaded))

    logs.sort(key=lambda item: item["source_item_id"])
    skipped.sort(key=lambda item: item["source_item_id"])
    return {
        "source": source,
        "repo": repo,
        "window": failures_payload["window"],
        "log_root": str(spool_dir),
        "counts": {
            "inputs": len(records),
            "logs": len(logs),
            "skipped": len(skipped),
        },
        "logs": logs,
        "skipped": skipped,
        "summary": {
            "skipped_unknown": list(summary.skipped_unknown),
            "command_failed_after_retries": list(summary.command_failed_after_retries),
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default="tikv/pd")
    parser.add_argument("--days", type=int, default=1)
    parser.add_argument(
        "--start-from",
        default="",
        help="start time for a fixed window; when set, --days controls the window length from this time",
    )
    parser.add_argument("--max-prow-pages", type=int, default=30)
    parser.add_argument("--max-action-runs", type=int, default=500)
    parser.add_argument("--retry-count", type=int, default=3)
    parser.add_argument("--download-workers", type=int, default=8)
    parser.add_argument(
        "--run-dir",
        default="",
        help="directory for this run's JSON artifacts; defaults to a fresh /tmp/pd-ci-flaky/run-<timestamp>-<pid> directory",
    )
    parser.add_argument("--log-spool-dir", default="")
    parser.add_argument("--prow-failures-json", default="")
    parser.add_argument("--actions-failures-json", default="")
    parser.add_argument("--prow-logs-json", default="")
    parser.add_argument("--actions-logs-json", default="")
    return parser.parse_args()


def _build_prow_failures_payload(
    *,
    repo: str,
    start: dt.datetime,
    end: dt.datetime,
    max_pages: int,
    retries: int,
) -> dict:
    summary = LEGACY.RunSummary(
        scanned_window_start=start.isoformat(),
        scanned_window_end=end.isoformat(),
    )
    failures, outcomes = LEGACY.collect_prow_failures(
        repo=repo,
        since=start,
        max_pages=max_pages,
        summary=summary,
        retries=retries,
    )
    return {
        "source": "prow",
        "repo": repo,
        "window": build_window_payload(start, end),
        "counts": {
            "failures": len(failures),
            "skipped_unknown": len(summary.skipped_unknown),
            "command_failed_after_retries": len(summary.command_failed_after_retries),
        },
        "failures": [serialize_prow_failure(record) for record in failures],
        "skipped_unknown": list(summary.skipped_unknown),
        "command_failed_after_retries": list(summary.command_failed_after_retries),
        "outcomes_by_ci_sha": {key: sorted(values) for key, values in outcomes.items()},
    }


def _build_actions_failures_payload(
    *,
    repo: str,
    start: dt.datetime,
    end: dt.datetime,
    max_runs: int,
    retries: int,
) -> dict:
    summary = LEGACY.RunSummary(
        scanned_window_start=start.isoformat(),
        scanned_window_end=end.isoformat(),
    )
    failures = LEGACY.collect_actions_failures(
        repo=repo,
        since=start,
        max_runs=max_runs,
        summary=summary,
        retries=retries,
    )
    return {
        "source": "actions",
        "repo": repo,
        "window": build_window_payload(start, end),
        "counts": {
            "failures": len(failures),
            "skipped_unknown": len(summary.skipped_unknown),
            "command_failed_after_retries": len(summary.command_failed_after_retries),
        },
        "failures": [serialize_actions_failure(record) for record in failures],
        "skipped_unknown": list(summary.skipped_unknown),
        "command_failed_after_retries": list(summary.command_failed_after_retries),
    }


def main() -> int:
    args = parse_args()
    end = LEGACY.now_utc()
    start, end = resolve_window(end=end, days=args.days, start_from=args.start_from)
    run_dir = resolve_run_dir(args.run_dir)
    artifact_paths = resolve_output_paths(args, run_dir)
    log_spool_root = resolve_log_spool_root(args.log_spool_dir, run_dir)
    auth_summary = LEGACY.RunSummary(
        scanned_window_start=start.isoformat(),
        scanned_window_end=end.isoformat(),
    )
    if not LEGACY.ensure_gh_auth(summary=auth_summary, retries=args.retry_count):
        return 1

    prow_failures_payload = _build_prow_failures_payload(
        repo=args.repo,
        start=start,
        end=end,
        max_pages=args.max_prow_pages,
        retries=args.retry_count,
    )
    actions_failures_payload = _build_actions_failures_payload(
        repo=args.repo,
        start=start,
        end=end,
        max_runs=args.max_action_runs,
        retries=args.retry_count,
    )
    prow_failures_payload["run_dir"] = str(run_dir)
    actions_failures_payload["run_dir"] = str(run_dir)
    write_json(artifact_paths["prow_failures"], prow_failures_payload)
    write_json(artifact_paths["actions_failures"], actions_failures_payload)

    common_fetch_kwargs = {
        "repo": args.repo,
        "retries": args.retry_count,
        "download_workers": args.download_workers,
        "log_spool_dir": str(log_spool_root),
    }
    prow_logs_payload = fetch_logs_for_failures(
        failures_payload=prow_failures_payload,
        **common_fetch_kwargs,
    )
    actions_logs_payload = fetch_logs_for_failures(
        failures_payload=actions_failures_payload,
        **common_fetch_kwargs,
    )
    prow_logs_payload["run_dir"] = str(run_dir)
    actions_logs_payload["run_dir"] = str(run_dir)
    write_json(artifact_paths["prow_logs"], prow_logs_payload)
    write_json(artifact_paths["actions_logs"], actions_logs_payload)
    print(f"RUN_DIR={run_dir}")
    print(
        "wrote "
        f"{prow_failures_payload['counts']['failures']} prow failures to {artifact_paths['prow_failures']}, "
        f"{actions_failures_payload['counts']['failures']} actions failures to {artifact_paths['actions_failures']}, "
        f"{prow_logs_payload['counts']['logs']} prow logs to {artifact_paths['prow_logs']}, and "
        f"{actions_logs_payload['counts']['logs']} actions logs to {artifact_paths['actions_logs']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
