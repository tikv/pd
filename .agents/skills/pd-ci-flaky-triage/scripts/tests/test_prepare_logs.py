#!/usr/bin/env python3
"""Unit tests for prepare_logs window selection."""

from __future__ import annotations

import argparse
import importlib.util
import pathlib
import sys
import tempfile
import unittest


SCRIPT_PATH = pathlib.Path(__file__).resolve().parents[1] / "prepare_logs.py"
SPEC = importlib.util.spec_from_file_location("prepare_logs", SCRIPT_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class PrepareLogsTests(unittest.TestCase):
    def test_parse_start_from_accepts_date_only(self) -> None:
        parsed = MODULE.parse_start_from("2026-03-28")
        self.assertEqual("2026-03-28T00:00:00+00:00", parsed.isoformat())

    def test_parse_start_from_normalizes_offset_timestamp(self) -> None:
        parsed = MODULE.parse_start_from("2026-03-28T08:30:00+08:00")
        self.assertEqual("2026-03-28T00:30:00+00:00", parsed.isoformat())

    def test_resolve_window_prefers_start_from_over_days(self) -> None:
        end = MODULE.dt.datetime(2026, 3, 30, 12, 0, tzinfo=MODULE.UTC)
        start, resolved_end = MODULE.resolve_window(
            end=end,
            days=1,
            start_from="2026-03-20",
        )
        self.assertEqual("2026-03-20T00:00:00+00:00", start.isoformat())
        self.assertEqual("2026-03-21T00:00:00+00:00", resolved_end.isoformat())

    def test_resolve_run_dir_defaults_to_unique_tmp_subdir(self) -> None:
        resolved = MODULE.resolve_run_dir(
            "",
            now=MODULE.dt.datetime(2026, 3, 30, 12, 0, tzinfo=MODULE.UTC),
            pid=4321,
        )
        self.assertEqual(
            MODULE.DEFAULT_RUN_ROOT / "run-20260330T120000Z-4321",
            resolved,
        )

    def test_resolve_output_paths_default_to_run_dir(self) -> None:
        run_dir = pathlib.Path("/tmp/pd-ci-flaky/run-20260330T120000Z-4321")
        args = argparse.Namespace(
            prow_failures_json="",
            actions_failures_json="",
            prow_logs_json="",
            actions_logs_json="",
        )
        resolved = MODULE.resolve_output_paths(args, run_dir)
        self.assertEqual(run_dir / "prow_failures.json", resolved["prow_failures"])
        self.assertEqual(run_dir / "actions_failures.json", resolved["actions_failures"])
        self.assertEqual(run_dir / "prow_logs.json", resolved["prow_logs"])
        self.assertEqual(run_dir / "actions_logs.json", resolved["actions_logs"])

    def test_resolve_log_spool_root_defaults_under_run_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            run_dir = pathlib.Path(tempdir) / "run-1"
            resolved = MODULE.resolve_log_spool_root("", run_dir)
            self.assertEqual(run_dir / "raw-logs", resolved)
            self.assertTrue(resolved.is_dir())


if __name__ == "__main__":
    unittest.main()
