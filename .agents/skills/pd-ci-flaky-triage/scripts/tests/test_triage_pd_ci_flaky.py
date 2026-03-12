#!/usr/bin/env python3
"""Unit tests for pd CI flaky triage parser heuristics."""

from __future__ import annotations

import argparse
import importlib.util
import io
import pathlib
import re
import sys
import unittest
from contextlib import redirect_stderr
from unittest import mock


SCRIPT_PATH = pathlib.Path(__file__).resolve().parents[1] / "triage_pd_ci_flaky.py"
SPEC = importlib.util.spec_from_file_location("triage_pd_ci_flaky", SCRIPT_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)

SOURCE_TEXT = SCRIPT_PATH.read_text(encoding="utf-8")


def make_record(record_id: str = "sample") -> object:
    return MODULE.FailureRecord(
        record_id=record_id,
        source="prow",
        ci_name="pull-unit-test-next-gen-3",
        ci_url="https://example.invalid/ci",
        log_url="https://example.invalid/log",
        occurred_at="2026-03-03T00:00:00Z",
        pr_number=1,
        commit_sha="abc",
        run_id="1",
        job_id=None,
        status="FAILURE",
    )


class TriageParserTests(unittest.TestCase):
    def test_parse_signatures_ignores_goleak_download_line(self) -> None:
        log_text = "go: downloading go.uber.org/goleak v1.3.0\n"
        signatures = MODULE.parse_signatures(log_text)
        self.assertNotIn("GOLEAK", signatures)
        self.assertEqual(["UNKNOWN_FAILURE"], signatures)

    def test_parse_test_names_prioritizes_explicit_markers_over_stack_noise(self) -> None:
        log_text = """
panic: test timed out after 5m0s
    running tests:
        TestConfigTTLAfterTransferLeader (5m0s)
server_test.go:86 server_test.TestUpdateAdvertiseUrls { err = cluster.RunInitialServers() }
"""
        tests = MODULE.parse_test_names(log_text)
        self.assertIn("TestConfigTTLAfterTransferLeader", tests)
        self.assertNotIn("TestUpdateAdvertiseUrls", tests)

    def test_parse_test_names_falls_back_to_stack_when_explicit_markers_missing(self) -> None:
        log_text = """
server_test.go:86 server_test.TestUpdateAdvertiseUrls { err = cluster.RunInitialServers() }
"""
        tests = MODULE.parse_test_names(log_text)
        self.assertEqual(["TestUpdateAdvertiseUrls"], tests)

    def test_extract_evidence_collects_anchor_lines(self) -> None:
        log_text = """
[2026-03-03T00:00:00Z] INFO bootstrap done
--- FAIL: TestFoo (10.00s)
panic: test timed out after 5m0s
Condition never satisfied
"""
        evidence = MODULE.extract_evidence(log_text)
        self.assertGreaterEqual(len(evidence), 2)
        self.assertTrue(any("--- FAIL:" in line for line in evidence))
        self.assertTrue(any("panic:" in line.lower() for line in evidence))

    def test_score_issue_match_blocks_generic_unknown_flaky_match(self) -> None:
        issue = {
            "title": "some test is flaky",
            "body": "flaky in CI",
            "updatedAt": "2026-03-01T00:00:00Z",
        }
        score = MODULE.score_issue_match(
            issue,
            test_name=None,
            signatures=["UNKNOWN_FAILURE"],
            package_name=None,
        )
        self.assertEqual(0, score)

    def test_score_issue_match_requires_test_token_when_test_name_is_known(self) -> None:
        issue = {
            "title": "Data race in scheduler",
            "body": "panic and data race observed at scheduler.go:42",
            "updatedAt": "2026-03-01T00:00:00Z",
        }
        score = MODULE.score_issue_match(
            issue,
            test_name="TestConfigTTLAfterTransferLeader",
            signatures=["DATA_RACE"],
            package_name=None,
        )
        self.assertEqual(0, score)

    def test_normalize_extracted_tests_prefers_specific_and_collapses_param_subtests(self) -> None:
        tests = [
            "TestQPS/concurrency=1000,reserveN=10,limit=400000",
            "TestQPS",
            "TestTSOKeyspaceGroupManagerSuite",
            "TestTSOKeyspaceGroupManagerSuite/TestWatchFailed",
        ]
        normalized = MODULE.normalize_extracted_tests(tests)
        self.assertEqual(["TestQPS", "TestTSOKeyspaceGroupManagerSuite/TestWatchFailed"], normalized)

    def test_build_refs_match_repo_filters_non_target_repo(self) -> None:
        build = {"Refs": {"org": "pingcap", "repo": "ticdc", "pulls": [{"number": 4263}]}}
        self.assertFalse(MODULE.build_refs_match_repo(build, "tikv/pd"))
        build = {"Refs": {"org": "tikv", "repo": "pd", "pulls": [{"number": 10254}]}}
        self.assertTrue(MODULE.build_refs_match_repo(build, "tikv/pd"))

    def test_parse_failures_from_log_extracts_test_and_confidence(self) -> None:
        log_text = """
panic: test timed out after 5m0s
    running tests:
        TestConfigTTLAfterTransferLeader (5m0s)
"""
        parsed = MODULE.parse_failures_from_log(
            record=make_record("sample-1"),
            log_text=log_text,
            agent_max_log_bytes=1024 * 1024,
        )
        self.assertTrue(parsed)
        self.assertEqual("TestConfigTTLAfterTransferLeader", parsed[0].primary_test)
        self.assertGreater(parsed[0].confidence, 0.65)

    def test_parse_failures_from_log_uses_package_key_for_goleak(self) -> None:
        log_text = """
goleak: Errors on successful test run: found unexpected goroutines:
FAIL\tgithub.com/tikv/pd/client\t8.624s
"""
        parsed = MODULE.parse_failures_from_log(
            record=make_record("sample-2"),
            log_text=log_text,
            agent_max_log_bytes=1024 * 1024,
        )
        self.assertTrue(parsed)
        self.assertEqual("github.com/tikv/pd/client", parsed[0].primary_package)
        self.assertEqual("package::githubcomtikvpdclient", parsed[0].key)
        self.assertIsNone(parsed[0].test_name)

    def test_to_action_payload_schema_excludes_generated_body_fields(self) -> None:
        summary = MODULE.RunSummary(
            scanned_window_start="2026-03-04T00:00:00+00:00",
            scanned_window_end="2026-03-05T00:00:00+00:00",
            prow_records=3,
            actions_records=2,
            parsed_failures=4,
            flaky_true=2,
        )
        create = MODULE.CreateAction(
            key="testfoo",
            test_name="TestFoo",
            package_name=None,
            title="TestFoo is flaky",
            labels=["type/ci"],
            links=["https://example.invalid/ci/1"],
            ci_names=["pull-unit-test-next-gen-3"],
            signatures=["PANIC"],
            evidence_summary="type=panic; test=TestFoo",
        )
        comment = MODULE.CommentAction(
            key="testbar",
            test_name="TestBar",
            package_name=None,
            issue_number=101,
            issue_url="https://github.com/tikv/pd/issues/101",
            new_links=["https://example.invalid/ci/2"],
            ci_names=["PD Test"],
            signatures=["DATA_RACE"],
            evidence_summary="type=data_race; test=TestBar",
        )
        payload = MODULE.to_action_payload(
            summary=summary,
            create_actions=[create],
            comment_actions=[comment],
            reopen_actions=[comment],
            unknown_actions=[],
        )

        self.assertEqual(
            {"window", "counts", "create", "comment", "reopen_and_comment", "unknown"},
            set(payload.keys()),
        )
        self.assertEqual(1, payload["counts"]["create"])
        self.assertEqual(1, payload["counts"]["comment"])
        self.assertEqual(1, payload["counts"]["reopen_and_comment"])
        self.assertEqual(0, payload["counts"]["unknown"])
        self.assertNotIn("body", payload["create"][0])
        self.assertNotIn("comment_body", payload["comment"][0])
        self.assertIn("debug_only_evidence_summary", payload["create"][0])
        self.assertIn("debug_only_evidence_summary", payload["comment"][0])
        self.assertIn("debug_only_evidence_summary", payload["reopen_and_comment"][0])
        self.assertNotIn("evidence_summary", payload["create"][0])
        self.assertNotIn("evidence_summary", payload["comment"][0])
        self.assertNotIn("evidence_summary", payload["reopen_and_comment"][0])
        self.assertEqual([], payload["unknown"])

    def test_unknown_failures_are_emitted_separately_and_not_as_issue_actions(self) -> None:
        record = make_record("unknown-1")
        record = MODULE.FailureRecord(
            record_id=record.record_id,
            source=record.source,
            ci_name="pull-unit-test-next-gen-3",
            ci_url="https://example.invalid/ci/unknown-1",
            log_url=record.log_url,
            occurred_at=record.occurred_at,
            pr_number=101,
            commit_sha=record.commit_sha,
            run_id=record.run_id,
            job_id=record.job_id,
            status=record.status,
        )
        parsed = MODULE.ParsedFailure(
            record_id=record.record_id,
            key="signature::unknown_failure",
            test_name=None,
            signatures=["UNKNOWN_FAILURE"],
            evidence_lines=["--- FAIL: TestUnknown (1.00s)"],
            tests=[],
            primary_test=None,
            failure_type="unknown",
            evidence_summary="type=unknown; test=N/A; signatures=UNKNOWN_FAILURE",
            confidence=0.35,
            primary_package=None,
            failed_packages=[],
        )
        decision = MODULE.FlakyDecision(
            key=parsed.key,
            test_name=None,
            is_flaky=True,
            reason="existing_flaky_issue",
            distinct_pr_count=2,
            distinct_sha_count=2,
            has_existing_issue=True,
            existing_issue_number=1234,
            confidence=0.35,
            action_reason="matched_existing_issue",
        )
        grouped = {parsed.key: [parsed]}
        records_by_id = {record.record_id: record}
        signatures_by_key = {parsed.key: ["UNKNOWN_FAILURE"]}
        issue_matches = {
            parsed.key: {
                "number": 1234,
                "url": "https://github.com/tikv/pd/issues/1234",
            }
        }
        args = argparse.Namespace(
            issue_labels="type/ci",
            repo="tikv/pd",
            reopen_closed=True,
            retry_count=1,
        )
        summary = MODULE.RunSummary(
            scanned_window_start="2026-03-04T00:00:00+00:00",
            scanned_window_end="2026-03-05T00:00:00+00:00",
        )

        create_actions, comment_actions, reopen_actions = MODULE.build_issue_actions(
            args=args,
            grouped=grouped,
            records_by_id=records_by_id,
            decisions=[decision],
            issue_matches=issue_matches,
            signatures_by_key=signatures_by_key,
            summary=summary,
        )
        self.assertEqual([], create_actions)
        self.assertEqual([], comment_actions)
        self.assertEqual([], reopen_actions)

        unknown_actions = MODULE.build_unknown_actions(
            grouped=grouped,
            records_by_id=records_by_id,
            decisions=[decision],
            issue_matches=issue_matches,
            signatures_by_key=signatures_by_key,
        )
        self.assertEqual(1, len(unknown_actions))
        self.assertEqual("https://example.invalid/ci/unknown-1", unknown_actions[0].links[0])
        self.assertEqual(1234, unknown_actions[0].existing_issue_number)

        payload = MODULE.to_action_payload(
            summary=summary,
            create_actions=create_actions,
            comment_actions=comment_actions,
            reopen_actions=reopen_actions,
            unknown_actions=unknown_actions,
        )
        self.assertEqual(1, payload["counts"]["unknown"])
        self.assertEqual([], payload["create"])
        self.assertEqual([], payload["comment"])
        self.assertEqual([], payload["reopen_and_comment"])
        self.assertEqual("signature::unknown_failure", payload["unknown"][0]["key"])
        self.assertEqual("existing_flaky_issue", payload["unknown"][0]["decision_reason"])

    def test_script_has_no_gh_write_issue_ops(self) -> None:
        self.assertIsNone(re.search(r"['\"]issue['\"]\s*,\s*['\"]create['\"]", SOURCE_TEXT))
        self.assertIsNone(re.search(r"['\"]issue['\"]\s*,\s*['\"]comment['\"]", SOURCE_TEXT))
        self.assertIsNone(re.search(r"['\"]issue['\"]\s*,\s*['\"]reopen['\"]", SOURCE_TEXT))

    def test_parse_args_defaults_use_only_supported_surface(self) -> None:
        with mock.patch.object(sys, "argv", ["triage_pd_ci_flaky.py"]):
            args = MODULE.parse_args()

        self.assertEqual("tikv/pd", args.repo)
        self.assertEqual(7, args.days)
        self.assertFalse(hasattr(args, "scope"))
        self.assertFalse(hasattr(args, "ci_scope"))
        self.assertFalse(hasattr(args, "flaky_policy"))
        self.assertFalse(hasattr(args, "reopen_closed"))
        self.assertFalse(hasattr(args, "pipeline_mode"))

    def test_parse_args_rejects_removed_flags(self) -> None:
        removed_flags = [
            ["--mode", "auto"],
            ["--scope", "pr+push"],
            ["--ci-scope", "test-all"],
            ["--flaky-policy", "evidence-first"],
            ["--reopen-closed", "true"],
            ["--pipeline-mode", "parallel"],
        ]

        for argv_suffix in removed_flags:
            with self.subTest(flag=argv_suffix[0]):
                with mock.patch.object(sys, "argv", ["triage_pd_ci_flaky.py", *argv_suffix]):
                    with redirect_stderr(io.StringIO()):
                        with self.assertRaises(SystemExit):
                            MODULE.parse_args()

    def test_build_issue_actions_reopens_closed_issue_without_cli_toggle(self) -> None:
        record = MODULE.FailureRecord(
            record_id="closed-1",
            source="actions",
            ci_name="PD Test / unit",
            ci_url="https://example.invalid/ci/closed-1",
            log_url=None,
            occurred_at="2026-03-04T00:00:00Z",
            pr_number=102,
            commit_sha="def",
            run_id="22",
            job_id=33,
            status="FAILURE",
        )
        parsed = MODULE.ParsedFailure(
            record_id=record.record_id,
            key="test::testfoo",
            test_name="TestFoo",
            signatures=["PANIC"],
            evidence_lines=["panic: boom"],
            tests=["TestFoo"],
            primary_test="TestFoo",
            failure_type="panic",
            evidence_summary="type=panic; test=TestFoo",
            confidence=0.91,
            primary_package=None,
            failed_packages=[],
        )
        decision = MODULE.FlakyDecision(
            key=parsed.key,
            test_name="TestFoo",
            is_flaky=True,
            reason="existing_flaky_issue",
            distinct_pr_count=2,
            distinct_sha_count=2,
            has_existing_issue=True,
            existing_issue_number=88,
            confidence=0.91,
            action_reason="matched_existing_issue",
        )
        args = argparse.Namespace(
            issue_labels="type/ci",
            repo="tikv/pd",
            retry_count=1,
        )
        summary = MODULE.RunSummary(
            scanned_window_start="2026-03-04T00:00:00+00:00",
            scanned_window_end="2026-03-05T00:00:00+00:00",
        )

        with mock.patch.object(
            MODULE,
            "run_gh_json",
            return_value={"body": "", "comments": []},
        ):
            create_actions, comment_actions, reopen_actions = MODULE.build_issue_actions(
                args=args,
                grouped={parsed.key: [parsed]},
                records_by_id={record.record_id: record},
                decisions=[decision],
                issue_matches={
                    parsed.key: {
                        "number": 88,
                        "url": "https://github.com/tikv/pd/issues/88",
                        "state": "closed",
                    }
                },
                signatures_by_key={parsed.key: ["PANIC"]},
                summary=summary,
            )

        self.assertEqual([], create_actions)
        self.assertEqual([], comment_actions)
        self.assertEqual(1, len(reopen_actions))
        self.assertEqual(88, reopen_actions[0].issue_number)

    def test_decide_flaky_does_not_treat_closed_issue_as_sufficient_evidence(self) -> None:
        record = MODULE.FailureRecord(
            record_id="closed-evidence-1",
            source="prow",
            ci_name="pull-unit-test-next-gen-3",
            ci_url="https://example.invalid/ci/closed-evidence-1",
            log_url="https://example.invalid/log/closed-evidence-1",
            occurred_at="2026-03-04T00:00:00Z",
            pr_number=102,
            commit_sha="sha-1",
            run_id="22",
            job_id=None,
            status="FAILURE",
        )
        parsed = MODULE.ParsedFailure(
            record_id=record.record_id,
            key="test::testfoo",
            test_name="TestFoo",
            signatures=["PANIC"],
            evidence_lines=["panic: boom"],
            tests=["TestFoo"],
            primary_test="TestFoo",
            failure_type="panic",
            evidence_summary="type=panic; test=TestFoo",
            confidence=0.91,
            primary_package=None,
            failed_packages=[],
        )

        decision = MODULE.decide_flaky(
            key=parsed.key,
            test_name=parsed.test_name,
            entries=[parsed],
            records_by_id={record.record_id: record},
            open_issue=None,
            closed_issue={
                "number": 88,
                "url": "https://github.com/tikv/pd/issues/88",
                "state": "closed",
            },
            outcomes_by_ci_sha={},
        )

        self.assertFalse(decision.is_flaky)
        self.assertEqual("likely_pr_regression_or_insufficient_evidence", decision.reason)
        self.assertIsNone(decision.existing_issue_number)

    def test_build_issue_actions_reopens_closed_issue_after_independent_flaky_decision(self) -> None:
        record = MODULE.FailureRecord(
            record_id="closed-reopen-1",
            source="actions",
            ci_name="PD Test / unit",
            ci_url="https://example.invalid/ci/closed-reopen-1",
            log_url=None,
            occurred_at="2026-03-04T00:00:00Z",
            pr_number=102,
            commit_sha="def",
            run_id="22",
            job_id=33,
            status="FAILURE",
        )
        parsed = MODULE.ParsedFailure(
            record_id=record.record_id,
            key="test::testfoo",
            test_name="TestFoo",
            signatures=["PANIC"],
            evidence_lines=["panic: boom"],
            tests=["TestFoo"],
            primary_test="TestFoo",
            failure_type="panic",
            evidence_summary="type=panic; test=TestFoo",
            confidence=0.91,
            primary_package=None,
            failed_packages=[],
        )
        decision = MODULE.FlakyDecision(
            key=parsed.key,
            test_name="TestFoo",
            is_flaky=True,
            reason="reproduced_across_prs",
            distinct_pr_count=2,
            distinct_sha_count=2,
            has_existing_issue=False,
            existing_issue_number=None,
            confidence=0.91,
            action_reason="cross_pr_repro",
        )
        args = argparse.Namespace(
            issue_labels="type/ci",
            repo="tikv/pd",
            retry_count=1,
        )
        summary = MODULE.RunSummary(
            scanned_window_start="2026-03-04T00:00:00+00:00",
            scanned_window_end="2026-03-05T00:00:00+00:00",
        )

        with mock.patch.object(
            MODULE,
            "run_gh_json",
            return_value={"body": "", "comments": []},
        ):
            create_actions, comment_actions, reopen_actions = MODULE.build_issue_actions(
                args=args,
                grouped={parsed.key: [parsed]},
                records_by_id={record.record_id: record},
                decisions=[decision],
                issue_matches={
                    parsed.key: {
                        "number": 88,
                        "url": "https://github.com/tikv/pd/issues/88",
                        "state": "closed",
                    }
                },
                signatures_by_key={parsed.key: ["PANIC"]},
                summary=summary,
            )

        self.assertEqual([], create_actions)
        self.assertEqual([], comment_actions)
        self.assertEqual(1, len(reopen_actions))
        self.assertEqual(88, reopen_actions[0].issue_number)


if __name__ == "__main__":
    unittest.main()
