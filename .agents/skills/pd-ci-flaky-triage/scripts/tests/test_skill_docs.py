#!/usr/bin/env python3
"""Checks for skill doc contracts that downstream steps rely on."""

from __future__ import annotations

import pathlib
import unittest


SKILL_PATH = pathlib.Path(__file__).resolve().parents[2] / "SKILL.md"


class SkillDocTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.contents = SKILL_PATH.read_text(encoding="utf-8")

    def test_run_dir_scopes_all_json_handoffs(self) -> None:
        self.assertIn("$RUN_DIR/failure_items.json", self.contents)
        self.assertIn("$RUN_DIR/env_filtered.json", self.contents)
        self.assertIn("$RUN_DIR/flaky_tests.json", self.contents)
        self.assertIn("$RUN_DIR/prow_logs.json", self.contents)
        self.assertIn("$RUN_DIR/actions_logs.json", self.contents)

    def test_step_three_defines_source_review_skeleton(self) -> None:
        self.assertIn("$RUN_DIR/prow_source_review.json", self.contents)
        self.assertIn("$RUN_DIR/actions_source_review.json", self.contents)
        self.assertIn('"source": "prow"', self.contents)
        self.assertIn('"failure_items": []', self.contents)
        self.assertIn('"env_filtered": []', self.contents)

    def test_final_output_mentions_collection_gaps(self) -> None:
        self.assertIn("log fetch failures", self.contents)
        self.assertIn("command failures after retries", self.contents)


if __name__ == "__main__":
    unittest.main()
