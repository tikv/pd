#!/usr/bin/env python3
"""Unit tests for flaky snippet validator."""

from __future__ import annotations

import importlib.util
import pathlib
import sys
import unittest


SCRIPT_PATH = pathlib.Path(__file__).resolve().parents[1] / "validate_flaky_snippets.py"
SPEC = importlib.util.spec_from_file_location("validate_flaky_snippets", SCRIPT_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class ValidateFlakySnippetsTests(unittest.TestCase):
    def test_rejects_summary_template_snippet(self) -> None:
        text = """
### [create #1] TestFoo is flaky
### Which jobs are failing
```text
type=unknown; test=TestFoo; signatures=UNKNOWN_FAILURE
```
### CI link
- https://example.invalid/ci/1
"""
        result = MODULE.validate_report_text(text)
        self.assertEqual(1, len(result))
        self.assertFalse(result[0].validation_passed)
        self.assertIn("summary-template", result[0].error)

    def test_rejects_snippet_without_target_or_anchor(self) -> None:
        text = """
### [comment #2] issue #123 - TestFoo
### Which jobs are failing
```text
download start
setup complete
all retries done
```
### New CI link
- https://example.invalid/ci/2
"""
        result = MODULE.validate_report_text(text)
        self.assertEqual(1, len(result))
        self.assertFalse(result[0].validation_passed)
        self.assertIn("test/package", result[0].error)
        self.assertIn("required error anchor", result[0].error)

    def test_accepts_goleak_package_snippet(self) -> None:
        text = """
### [reopen+comment #3] issue #456 - GOLEAK detected in github.com/tikv/pd/client package tests
### Which jobs are failing
```text
goleak: Errors on successful test run: found unexpected goroutines:
Goroutine 120 in state chan receive, with net.(*Resolver).lookupIPAddr.func2 on top of the stack:
goroutine 120 [chan receive]:
net.(*Resolver).lookupIPAddr.func2(...)
    /usr/local/go/src/net/lookup.go:339
created by net.(*Resolver).lookupIPAddr in goroutine 149
    /usr/local/go/src/net/lookup.go:354 +0x929

Goroutine 117 in state IO wait, with internal/poll.runtime_pollWait on top of the stack:
goroutine 117 [IO wait]:
internal/poll.runtime_pollWait(0x7e4a81a99a00, 0x72)
    /usr/local/go/src/runtime/netpoll.go:351 +0x85
internal/poll.(*pollDesc).wait(0xc000352e20, 0x72, 0x0)
    /usr/local/go/src/internal/poll/fd_poll_runtime.go:84 +0xb1
internal/poll.(*pollDesc).waitRead(...)
    /usr/local/go/src/internal/poll/fd_poll_runtime.go:89
internal/poll.(*FD).Read(0xc000352e00, {0xc000302000, 0x4d0, 0x4d0})
    /usr/local/go/src/internal/poll/fd_unix.go:165 +0x453
net.(*netFD).Read(0xc000352e00, {0xc000302000, 0x4d0, 0x4d0})
    /usr/local/go/src/net/fd_posix.go:68 +0x4b
net.(*conn).Read(0xc000116520, {0xc000302000, 0x4d0, 0x4d0})
    /usr/local/go/src/net/net.go:196 +0xad
FAIL github.com/tikv/pd/client 8.624s
```
### New CI link
- https://example.invalid/ci/3
"""
        result = MODULE.validate_report_text(text)
        self.assertEqual(1, len(result))
        self.assertTrue(result[0].validation_passed)
        self.assertEqual("reopen_and_comment", result[0].action_type)
        self.assertEqual("https://example.invalid/ci/3", result[0].primary_link)
        self.assertIn("GOLEAK", result[0].anchors)
        self.assertIn("PACKAGE_FAIL", result[0].anchors)

    def test_accepts_panic_package_snippet(self) -> None:
        text = """
### [create #4] There is panic in pd-ctl/tests/scheduler
### Which jobs are failing
```text
[FATAL] [log.go:94] [panic] [recover="\\"invalid memory address or nil pointer dereference\\""] [stack="github.com/tikv/pd/pkg/utils/logutil.LogPanic
    /home/runner/work/pd/pd/pkg/utils/logutil/log.go:94
runtime.gopanic
    /opt/hostedtoolcache/go/1.23.11/x64/src/runtime/panic.go:791
github.com/tikv/pd/pkg/schedule/schedulers.(*Controller).ReloadSchedulerConfig
    /home/runner/work/pd/pd/pkg/schedule/schedulers/scheduler_controller.go:303"]
FAIL github.com/tikv/pd/tools/pd-ctl/tests/scheduler 74.950s
```
### CI link
- https://example.invalid/ci/5
"""
        result = MODULE.validate_report_text(text)
        self.assertEqual(1, len(result))
        self.assertTrue(result[0].validation_passed)
        self.assertIn("PANIC", result[0].anchors)
        self.assertIn("PACKAGE_FAIL", result[0].anchors)

    def test_accepts_deadlock_full_report(self) -> None:
        text = """
### [comment #5] issue #789 - TestTSOKeyspaceGroupManagerSuite/TestTSOKeyspaceGroupSplitElection
### Which jobs are failing
```text
testutil.go:68:
    Error Trace: /home/runner/work/pd/pd/pkg/utils/testutil/testutil.go:68
                 /home/runner/work/pd/pd/tests/tso_cluster.go:173
                 /home/runner/work/pd/pd/tests/integrations/mcs/tso/keyspace_group_manager_test.go:502
    Error:      Condition never satisfied
    Test:       TestTSOKeyspaceGroupManagerSuite/TestTSOKeyspaceGroupSplitElection
POTENTIAL DEADLOCK:
Previous place where the lock was grabbed
goroutine 796 lock 0xc002910000
../../../../pkg/tso/keyspace_group_manager.go:1238 tso.(*KeyspaceGroupManager).finishSplitKeyspaceGroup { kgm.Lock() } <<<<<
Have been trying to lock it again for more than 30s
goroutine 434767 lock 0xc002910000
../../../../pkg/tso/keyspace_group_manager.go:273 tso.(*state).getKeyspaceGroupMetaWithCheck { s.RLock() } <<<<<
Here is what goroutine 796 doing now
goroutine 796 [select]:
github.com/tikv/pd/pkg/tso.(*KeyspaceGroupManager).finishSplitKeyspaceGroup(...)
    /home/runner/work/pd/pd/pkg/tso/keyspace_group_manager.go:1250
Other goroutines holding locks:
goroutine 434784 lock 0xc0013532b0
../../../../pkg/keyspace/tso_keyspace_group.go:720 keyspace.(*GroupManager).FinishSplitKeyspaceByID { m.Lock() } <<<<<
```
### New CI link
- https://example.invalid/ci/6
"""
        result = MODULE.validate_report_text(text)
        self.assertEqual(1, len(result))
        self.assertTrue(result[0].validation_passed)
        self.assertIn("DEADLOCK", result[0].anchors)
        self.assertIn("CONDITION", result[0].anchors)

    def test_trace_payload_contains_required_fields(self) -> None:
        text = """
### [create #1] TestBar is flaky
### Which jobs are failing
```text
panic: test timed out after 5m0s
running tests:
  TestBar (5m0s)
```
### CI link
- https://example.invalid/ci/4
"""
        validations = MODULE.validate_report_text(text)
        payload = MODULE.build_trace_payload("/tmp/in.md", validations)
        self.assertIn("entries", payload)
        self.assertEqual(1, payload["counts"]["total_actions"])
        entry = payload["entries"][0]
        self.assertEqual(
            {
                "action_id",
                "action_type",
                "primary_link",
                "snippet_lines",
                "anchors",
                "validation_passed",
                "error",
            },
            set(entry.keys()),
        )


if __name__ == "__main__":
    unittest.main()
