#!/usr/bin/env python3
"""Unit tests for PD CI collection and log download helpers."""

from __future__ import annotations

import importlib.util
import pathlib
import sys
import tempfile
import unittest
from unittest import mock


SCRIPT_PATH = pathlib.Path(__file__).resolve().parents[1] / "triage_pd_ci_flaky.py"
SPEC = importlib.util.spec_from_file_location("triage_pd_ci_flaky", SCRIPT_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def make_summary() -> object:
    return MODULE.RunSummary(
        scanned_window_start="2026-03-30T00:00:00Z",
        scanned_window_end="2026-03-31T00:00:00Z",
    )


def make_record(source: str = "prow") -> object:
    return MODULE.FailureRecord(
        record_id="sample-1",
        source=source,
        ci_name="pull-unit-test-next-gen-1" if source == "prow" else "PD Test / chunks (1, unit)",
        ci_url="https://example.invalid/ci/1",
        log_url="https://example.invalid/log.txt" if source == "prow" else None,
        occurred_at="2026-03-30T00:00:00Z",
        pr_number=123 if source == "prow" else None,
        commit_sha="abc123",
        run_id="1001",
        job_id=None if source == "prow" else 2002,
        status="FAILURE",
    )


class TriageCollectionHelperTests(unittest.TestCase):
    def test_spyglass_to_build_log_url_converts_gcs_view(self) -> None:
        url = MODULE.spyglass_to_build_log_url(
            "https://prow.tidb.net/view/gs/pingcap-jenkins/logs/job-name/123456#1",
        )
        self.assertEqual(
            "https://storage.googleapis.com/pingcap-jenkins/logs/job-name/123456/build-log.txt",
            url,
        )

    def test_build_refs_match_repo_filters_non_target_repo(self) -> None:
        build = {"Refs": {"org": "pingcap", "repo": "ticdc", "pulls": [{"number": 4263}]}}
        self.assertFalse(MODULE.build_refs_match_repo(build, "tikv/pd"))

        build = {"Refs": {"org": "tikv", "repo": "pd", "pulls": [{"number": 10254}]}}
        self.assertTrue(MODULE.build_refs_match_repo(build, "tikv/pd"))

    def test_looks_test_related_identifies_test_jobs(self) -> None:
        self.assertTrue(MODULE.looks_test_related("pull-unit-test-next-gen-1"))
        self.assertTrue(MODULE.looks_test_related("PD Test / chunks (9, Integration)"))
        self.assertFalse(MODULE.looks_test_related("build-dashboard"))

    def test_is_release_branch_job(self) -> None:
        self.assertTrue(MODULE.is_release_branch_job("pull-unit-test/release-9.0"))
        self.assertTrue(MODULE.is_release_branch_job("release-9.0-unit-test"))
        self.assertFalse(MODULE.is_release_branch_job("pull-unit-test-next-gen-1"))

    def test_build_targets_master_filters_non_master_prs(self) -> None:
        self.assertTrue(
            MODULE.build_targets_master(
                {
                    "Refs": {
                        "base_ref": "master",
                        "pulls": [{"number": 123}],
                    }
                },
            ),
        )
        self.assertFalse(
            MODULE.build_targets_master(
                {
                    "Refs": {
                        "base_ref": "release-8.5",
                        "pulls": [{"number": 123}],
                    }
                },
            ),
        )

    def test_sanitize_record_id_replaces_unsafe_chars(self) -> None:
        sanitized = MODULE.sanitize_record_id("prow/pull unit test?=1")
        self.assertEqual("prow_pull_unit_test_1", sanitized)

    def test_fetch_and_spool_log_writes_file(self) -> None:
        record = make_record("prow")
        summary = make_summary()
        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.object(MODULE, "fetch_log_text", return_value="hello log\n"):
                downloaded, err = MODULE.fetch_and_spool_log(
                    record=record,
                    repo="tikv/pd",
                    summary=summary,
                    retries=1,
                    spool_dir=pathlib.Path(tmpdir),
                )
            self.assertIsNone(err)
            self.assertIsNotNone(downloaded)
            assert downloaded is not None
            self.assertEqual(record.record_id, downloaded.record.record_id)
            self.assertEqual("hello log\n", downloaded.path.read_text(encoding="utf-8"))

    def test_collect_actions_failures_filters_event_and_job_type(self) -> None:
        run_list = [
            {
                "databaseId": "1001",
                "name": "PD Test",
                "url": "https://example.invalid/runs/1001",
                "event": "pull_request",
                "createdAt": "2026-03-30T08:00:00Z",
                "headBranch": "feature-master",
                "headSha": "abc123",
            },
            {
                "databaseId": "1002",
                "name": "Nightly",
                "url": "https://example.invalid/runs/1002",
                "event": "schedule",
                "createdAt": "2026-03-30T08:00:00Z",
                "headBranch": "master",
                "headSha": "def456",
            },
            {
                "databaseId": "1003",
                "name": "PD Test",
                "url": "https://example.invalid/runs/1003",
                "event": "pull_request",
                "createdAt": "2026-03-30T08:05:00Z",
                "headBranch": "feature-release",
                "headSha": "ghi789",
            },
            {
                "databaseId": "1004",
                "name": "PD Test",
                "url": "https://example.invalid/runs/1004",
                "event": "push",
                "createdAt": "2026-03-30T08:10:00Z",
                "headBranch": "master",
                "headSha": "master123",
            },
            {
                "databaseId": "1005",
                "name": "PD Test",
                "url": "https://example.invalid/runs/1005",
                "event": "push",
                "createdAt": "2026-03-30T08:12:00Z",
                "headBranch": "release-8.5",
                "headSha": "release123",
            },
        ]
        run_views = {
            "1001": {
                "jobs": [
                    {
                        "databaseId": 3001,
                        "name": "chunks (9, Integration)",
                        "conclusion": "failure",
                        "url": "https://example.invalid/jobs/3001",
                    },
                    {
                        "databaseId": 3002,
                        "name": "build-dashboard",
                        "conclusion": "failure",
                        "url": "https://example.invalid/jobs/3002",
                    },
                ]
            },
            "1004": {
                "jobs": [
                    {
                        "databaseId": 3004,
                        "name": "chunks (10, Integration)",
                        "conclusion": "failure",
                        "url": "https://example.invalid/jobs/3004",
                    }
                ]
            },
        }
        pr_lists = {
            "feature-master": [
                {
                    "baseRefName": "master",
                    "headRefName": "feature-master",
                    "headRefOid": "abc123",
                }
            ],
            "feature-release": [
                {
                    "baseRefName": "release-8.5",
                    "headRefName": "feature-release",
                    "headRefOid": "ghi789",
                }
            ],
        }

        def fake_run_gh_json(args: list[str], summary: object, retries: int) -> object:
            if args[:2] == ["run", "list"]:
                return run_list
            if args[:2] == ["run", "view"]:
                return run_views[args[2]]
            if args[:2] == ["pr", "list"]:
                head_index = args.index("--head") + 1
                return pr_lists[args[head_index]]
            raise AssertionError(f"unexpected gh args: {args}")

        with mock.patch.object(MODULE, "run_gh_json", side_effect=fake_run_gh_json):
            failures = MODULE.collect_actions_failures(
                repo="tikv/pd",
                since=MODULE.parse_iso8601("2026-03-30T00:00:00Z"),
                max_runs=20,
                summary=make_summary(),
                retries=1,
            )

        self.assertEqual(2, len(failures))
        self.assertEqual("actions", failures[0].source)
        self.assertEqual("PD Test / chunks (9, Integration)", failures[0].ci_name)
        self.assertEqual(3001, failures[0].job_id)
        self.assertEqual("PD Test / chunks (10, Integration)", failures[1].ci_name)
        self.assertEqual(3004, failures[1].job_id)

    def test_collect_prow_failures_filters_repo_and_tracks_outcomes(self) -> None:
        jobs = [
            {
                "name": "pull-unit-test-next-gen-1",
                "jobHistoryLink": "/job-history/pull-unit-test-next-gen-1",
            }
        ]
        history_html = """
        <html>
        <script>
        var allBuilds = [
          {
            "Started": "2026-03-30T08:00:00Z",
            "Result": "FAILURE",
            "ID": "1001",
            "SpyglassLink": "https://prow.tidb.net/view/gs/pingcap-jenkins/logs/job-name/1001",
            "Refs": {
              "org": "tikv",
              "repo": "pd",
              "base_ref": "master",
              "pulls": [{"number": 123, "sha": "abc123"}]
            }
          },
          {
            "Started": "2026-03-30T08:05:00Z",
            "Result": "SUCCESS",
            "ID": "1002",
            "SpyglassLink": "https://prow.tidb.net/view/gs/pingcap-jenkins/logs/job-name/1002",
            "Refs": {
              "org": "tikv",
              "repo": "pd",
              "base_ref": "master",
              "pulls": [{"number": 123, "sha": "abc123"}]
            }
          },
          {
            "Started": "2026-03-30T08:07:00Z",
            "Result": "FAILURE",
            "ID": "1004",
            "SpyglassLink": "https://prow.tidb.net/view/gs/pingcap-jenkins/logs/job-name/1004",
            "Refs": {
              "org": "tikv",
              "repo": "pd",
              "base_ref": "release-8.5",
              "pulls": [{"number": 124, "sha": "rel124"}]
            }
          },
          {
            "Started": "2026-03-30T08:10:00Z",
            "Result": "FAILURE",
            "ID": "1003",
            "SpyglassLink": "https://prow.tidb.net/view/gs/pingcap-jenkins/logs/job-name/1003",
            "Refs": {
              "org": "pingcap",
              "repo": "ticdc",
              "pulls": [{"number": 999, "sha": "zzz999"}]
            }
          }
        ];
        </script>
        </html>
        """

        with mock.patch.object(MODULE, "collect_prow_jobs", return_value=jobs):
            with mock.patch.object(MODULE, "run_curl_text", return_value=history_html):
                failures, outcomes = MODULE.collect_prow_failures(
                    repo="tikv/pd",
                    since=MODULE.parse_iso8601("2026-03-30T00:00:00Z"),
                    max_pages=1,
                    summary=make_summary(),
                    retries=1,
                )

        self.assertEqual(1, len(failures))
        self.assertEqual("prow", failures[0].source)
        self.assertEqual("pull-unit-test-next-gen-1", failures[0].ci_name)
        self.assertEqual(123, failures[0].pr_number)
        self.assertEqual(
            {"FAILURE", "SUCCESS"},
            outcomes["pull-unit-test-next-gen-1::abc123"],
        )


if __name__ == "__main__":
    unittest.main()
