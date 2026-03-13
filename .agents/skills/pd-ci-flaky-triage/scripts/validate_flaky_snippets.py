#!/usr/bin/env python3
"""Validate generated flaky snippet drafts and emit trace artifacts."""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import json
import re
import sys
from pathlib import Path

ACTION_HEADER_RE = re.compile(r"^### \[(create|comment|reopen\+comment)\s*#?([^\]]+)\]", re.MULTILINE)
WHICH_JOBS_RE = re.compile(
    r"### Which jobs are failing\s*\n```(?:[a-zA-Z0-9_-]+)?\n(?P<body>.*?)\n```",
    re.DOTALL,
)
CI_LINK_SECTION_RE = re.compile(
    r"### (?:CI link|New CI link)\s*\n(?P<body>.*?)(?:\n### |\Z)",
    re.DOTALL,
)
URL_RE = re.compile(r"https?://[^\s)]+")
SUMMARY_TEMPLATE_RE = re.compile(r"^type=[^;]+;.*signatures=", re.IGNORECASE)
TEST_CLUE_RE = re.compile(r"\bTest[A-Za-z0-9_./=\-]+")
PACKAGE_CLUE_RE = re.compile(r"^FAIL\s+github\.com/[^\s]+", re.MULTILINE)

ANCHOR_PATTERNS: dict[str, re.Pattern[str]] = {
    "FAIL_LINE": re.compile(r"--- FAIL:", re.IGNORECASE),
    "PANIC": re.compile(r"panic:|\[panic\]", re.IGNORECASE),
    "DATA_RACE": re.compile(r"WARNING:\s*DATA RACE", re.IGNORECASE),
    "DEADLOCK": re.compile(r"POTENTIAL DEADLOCK", re.IGNORECASE),
    "GOLEAK": re.compile(r"goleak", re.IGNORECASE),
    "PACKAGE_FAIL": re.compile(r"^FAIL\s+github\.com/[^\s]+", re.MULTILINE),
    "CONDITION": re.compile(r"Condition never satisfied", re.IGNORECASE),
    "ERROR_LINE": re.compile(r"Error:", re.IGNORECASE),
}

LINE_BUDGETS: dict[str, tuple[int, int]] = {
    "timeout": (4, 12),
    "assertion": (5, 20),
    "panic": (2, 40),
    "data_race": (12, 120),
    "deadlock": (12, 140),
    "goleak": (8, 220),
    "unknown": (3, 20),
}


@dataclasses.dataclass
class SnippetValidation:
    action_id: str
    action_type: str
    primary_link: str
    snippet_lines: list[str]
    anchors: list[str]
    validation_passed: bool
    error: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate generated `Which jobs are failing` snippets and emit trace artifacts",
    )
    parser.add_argument("--input", required=True, help="Draft markdown path to validate")
    parser.add_argument("--trace-out", default="/tmp/pd_ci_flaky_trace.json")
    parser.add_argument("--error-report-out", default="/tmp/pd_ci_flaky_validation_errors.json")
    return parser.parse_args()


def normalize_action_type(raw: str) -> str:
    lowered = raw.lower()
    if lowered == "reopen+comment":
        return "reopen_and_comment"
    return lowered


def detect_failure_type(snippet_text: str) -> str:
    text = snippet_text.lower()
    if "warning: data race" in text:
        return "data_race"
    if "potential deadlock" in text:
        return "deadlock"
    if "panic: test timed out" in text:
        return "timeout"
    if "goleak" in text:
        return "goleak"
    if "condition never satisfied" in text or ("error:" in text and "test:" in text):
        return "assertion"
    if "panic:" in text or "[panic]" in text:
        return "panic"
    return "unknown"


def collect_anchors(snippet_text: str) -> list[str]:
    anchors = []
    for name, pattern in ANCHOR_PATTERNS.items():
        if pattern.search(snippet_text):
            anchors.append(name)
    return anchors


def extract_primary_link(section: str) -> str:
    ci_match = CI_LINK_SECTION_RE.search(section)
    if ci_match:
        urls = URL_RE.findall(ci_match.group("body"))
        if urls:
            return urls[0]
    urls = URL_RE.findall(section)
    return urls[0] if urls else ""


def extract_action_sections(text: str) -> list[tuple[str, str, str]]:
    matches = list(ACTION_HEADER_RE.finditer(text))
    if not matches:
        return [("single-1", "unknown", text)]

    sections: list[tuple[str, str, str]] = []
    for index, match in enumerate(matches):
        start = match.start()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        action_type = normalize_action_type(match.group(1))
        suffix = match.group(2).strip()
        action_id = f"{action_type}#{suffix}" if suffix else f"{action_type}#{index + 1}"
        sections.append((action_id, action_type, text[start:end]))
    return sections


def validate_section(action_id: str, action_type: str, section: str) -> SnippetValidation:
    snippet_match = WHICH_JOBS_RE.search(section)
    primary_link = extract_primary_link(section)
    errors: list[str] = []
    snippet_lines: list[str] = []
    anchors: list[str] = []

    if not snippet_match:
        errors.append("missing `### Which jobs are failing` fenced code block")
    else:
        raw_lines = snippet_match.group("body").splitlines()
        snippet_lines = [line.rstrip() for line in raw_lines if line.strip()]
        snippet_text = "\n".join(snippet_lines)

        if not snippet_lines:
            errors.append("empty snippet body")
        if any(SUMMARY_TEMPLATE_RE.search(line) for line in snippet_lines):
            errors.append("summary-template snippet is forbidden")
        if snippet_text and not (TEST_CLUE_RE.search(snippet_text) or PACKAGE_CLUE_RE.search(snippet_text)):
            errors.append("snippet missing test/package clue")

        anchors = collect_anchors(snippet_text)
        if not anchors:
            errors.append("snippet missing required error anchor")

        failure_type = detect_failure_type(snippet_text)
        minimum, maximum = LINE_BUDGETS[failure_type]
        if len(snippet_lines) < minimum or len(snippet_lines) > maximum:
            errors.append(
                f"line budget violated for {failure_type}: got={len(snippet_lines)} expected={minimum}-{maximum}"
            )

    return SnippetValidation(
        action_id=action_id,
        action_type=action_type,
        primary_link=primary_link,
        snippet_lines=snippet_lines,
        anchors=anchors,
        validation_passed=not errors,
        error="; ".join(errors),
    )


def validate_report_text(text: str) -> list[SnippetValidation]:
    sections = extract_action_sections(text)
    return [validate_section(action_id, action_type, section) for action_id, action_type, section in sections]


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)


def build_trace_payload(input_path: str, validations: list[SnippetValidation]) -> dict[str, object]:
    return {
        "generated_at_utc": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
        "input": input_path,
        "counts": {
            "total_actions": len(validations),
            "passed": sum(1 for item in validations if item.validation_passed),
            "failed": sum(1 for item in validations if not item.validation_passed),
        },
        "entries": [dataclasses.asdict(item) for item in validations],
    }


def build_error_payload(input_path: str, validations: list[SnippetValidation]) -> dict[str, object]:
    failures = [dataclasses.asdict(item) for item in validations if not item.validation_passed]
    return {
        "generated_at_utc": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
        "input": input_path,
        "failed_count": len(failures),
        "failures": failures,
    }


def main() -> int:
    args = parse_args()
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"input file not found: {input_path}", file=sys.stderr)
        return 2

    text = input_path.read_text(encoding="utf-8")
    validations = validate_report_text(text)
    trace_payload = build_trace_payload(str(input_path), validations)
    write_json(Path(args.trace_out), trace_payload)

    failures = [entry for entry in validations if not entry.validation_passed]
    if failures:
        error_payload = build_error_payload(str(input_path), validations)
        write_json(Path(args.error_report_out), error_payload)
        print(
            f"validation failed: {len(failures)}/{len(validations)} actions invalid; "
            f"trace={args.trace_out} errors={args.error_report_out}",
            file=sys.stderr,
        )
        for failure in failures:
            print(
                f"- {failure.action_id} ({failure.action_type}): {failure.error or 'unknown error'}",
                file=sys.stderr,
            )
        return 1

    print(f"validation passed: {len(validations)} actions; trace={args.trace_out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
