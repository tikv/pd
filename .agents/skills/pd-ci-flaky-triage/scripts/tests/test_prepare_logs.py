#!/usr/bin/env python3
"""Unit tests for prepare_logs window selection."""

from __future__ import annotations

import importlib.util
import pathlib
import sys
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


if __name__ == "__main__":
    unittest.main()
