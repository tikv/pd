# Copyright 2026 TiKV Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import random
import re
import sys
import tempfile
import unittest
from unittest import mock
import xml.etree.ElementTree as ET


spec = importlib.util.spec_from_file_location("ci_shard", Path(__file__).with_name("ci-shard.py"))
ci = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = ci
spec.loader.exec_module(ci)


class ShardTests(unittest.TestCase):
    def test_runner_names_and_scopes(self):
        root = Path(__file__).resolve().parent.parent
        config = json.loads((root / "scripts/ci-test-groups.json").read_text())
        workflow = (root / ".github/workflows/pd-tests.yaml").read_text()
        names = re.findall(r"worker_id: (\d+)\s+name: '([^']+)'", workflow)
        self.assertEqual([(int(i), name) for i, name in names], [(r["id"], r["name"]) for r in config["layouts"]["standard"]])
        packages = [
            ci.Package(".", "pkg/mcs/tso/server", 1, 1),
            ci.Package(".", "tests/server/tso", 1, 1),
            ci.Package("tools", "pd-ut", 1, 1),
            ci.Package("client", ".", 1, 1),
            ci.Package("tests/integrations", "client", 1, 1),
            ci.Package("tests/integrations", "tso", 1, 1),
            ci.Package("tests/integrations", "mcs/scheduling", 1, 1),
            ci.Package("tests/integrations", "mcs/tso", 1, 1),
        ]
        plan = ci.plan_layout(packages, config, "standard", 4)
        expected = [range(3), range(3, 5), [5], [6], [6], [7], [8], [9]]
        for package, allowed in zip(packages, expected):
            owners = [i for i, shard in enumerate(plan) if package in shard]
            self.assertEqual(len(owners), 1)
            self.assertIn(owners[0], allowed)
        nextgen = ci.plan_layout(packages, config, "nextgen", 4)
        self.assertEqual(nextgen[0], packages[:1])
        self.assertCountEqual(nextgen[1], packages[1:3])
        self.assertCountEqual(nextgen[2], packages[3:])
        with self.assertRaises(ValueError):
            ci.plan_layout([ci.Package("tests/integrations", "unknown", 1, 1)], config, "standard", 4)
        config["layouts"]["standard"][0]["pools"].append("tools")
        with self.assertRaises(ValueError):
            ci.plan_layout([ci.Package("tools", str(i), 10, 1) for i in range(10)], config, "standard", 4)

    def test_all_packages_once_and_deterministic(self):
        packages = [ci.Package(module, f"pkg/{i}", i * 3 + 1, i + 1) for module in ci.MODULES for i in range(30)]
        for count in (1, 3, 10, 150):
            plan = ci.plan_shards(packages, count, 4)
            self.assertCountEqual([p for shard in plan for p in shard], packages)
            shuffled = packages.copy()
            random.Random(1).shuffle(shuffled)
            self.assertEqual(plan, ci.plan_shards(shuffled, count, 4))

    def test_balances_unequal_packages(self):
        packages = [ci.Package(".", str(i), size, min(size, 20)) for i, size in enumerate([400, 300, 200, 100, 50, 40, 30, 20, 10, 10])]
        costs = [ci.estimate(shard, 4) for shard in ci.plan_shards(packages, 3, 4)]
        self.assertLess(max(costs) - min(costs), 20)

    def test_invalid_plan(self):
        for count, parallel in ((0, 4), (1, 0)):
            with self.assertRaises(ValueError):
                ci.plan_shards([], count, parallel)
        package = ci.Package(".", "pkg/a", 1, 1)
        with self.assertRaises(ValueError):
            ci.plan_shards([package, package], 2, 4)

    @mock.patch.object(ci.subprocess, "check_output")
    def test_discovers_new_packages_and_module_root(self, output):
        output.side_effect = [
            "github.com/tikv/pd/pkg/new\ngithub.com/tikv/pd/cmd/server\n",
            "github.com/tikv/pd/client\n",
            "github.com/tikv/pd/tools/pd-ut\n",
            "github.com/tikv/pd/tests/integrations/realcluster\ngithub.com/tikv/pd/tests/integrations/client\n",
        ]
        packages = ci.discover_packages(Path("/repo"), {"packages": {}})
        self.assertEqual([(p.module, p.path) for p in packages], [
            (".", "pkg/new"), ("client", "."), ("tools", "pd-ut"), ("tests/integrations", "client"),
        ])

    def test_merge_results_preserves_failure_and_coverage(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            results = root / "results"
            results.mkdir()
            (results / "1.xml").write_text('<testsuites><testsuite name="a"><testcase name="bad"><failure>failed</failure></testcase></testsuite></testsuites>')
            (results / "2.xml").write_text('<testsuites><testsuite name="b"><testcase name="good"/></testsuite></testsuites>')
            for index in (1, 2):
                (results / f"{index}.cov").write_text(f"mode: atomic\nfile{index}:1.1,2.1 1 1\n")
            ci.merge_results(results, root)
            self.assertEqual(len(list(ET.parse(root / "junitfile").iter("testsuite"))), 2)
            self.assertEqual(len(list(ET.parse(root / "junitfile").iter("failure"))), 1)
            coverage = (root / "covprofile").read_text()
            self.assertEqual(coverage.count("mode: atomic"), 1)
            self.assertIn("file1:", coverage)
            self.assertIn("file2:", coverage)
            (results / "2.cov").write_text("mode: count\n")
            with self.assertRaises(ValueError):
                ci.merge_results(results, root)

    def test_failed_module_stops_execution_and_keeps_report(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            packages = [ci.Package(".", "a", 2, 1), ci.Package("tools", "b", 1, 1)]

            def fail(command, **_):
                report = Path(command[command.index("--junitfile") + 1])
                report.write_text('<testsuites><testsuite name="failed"><testcase name="bad"><failure/></testcase></testsuite></testsuites>')
                return mock.Mock(returncode=7)

            with mock.patch.object(ci.subprocess, "run", side_effect=fail) as run:
                self.assertEqual(ci.run_shard(root, packages, root / "durations.json", 4), 7)
                self.assertEqual(run.call_count, 1)
            self.assertEqual(len(list(ET.parse(root / "junitfile").iter("failure"))), 1)

    def test_preserves_native_client_and_tools_execution(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            packages = [ci.Package("client", ".", 2, 1), ci.Package("tools", "pd-ut", 1, 1)]
            with mock.patch.object(ci, "run_native_tests", return_value=(0, [])) as run:
                self.assertEqual(ci.run_shard(root, packages, root / "durations.json", 4), 0)
            for call, module in zip(run.call_args_list, ("client", "tools")):
                command = call.args[0]
                self.assertEqual(command[:2], ["go", "test"])
                self.assertIn("-count=1", command)
                self.assertEqual(command[-1], "./...")
                self.assertIn("-coverpkg=../...", command)
                self.assertEqual(command[command.index("-tags") + 1], "deadlock")
                self.assertEqual(call.args[1], root / module)
                self.assertEqual(call.args[2]["CGO_ENABLED"], "1")

    def test_native_timings_keep_failure_logs_and_test_lifecycle(self):
        events = [
            {"Action": "output", "Package": "p", "Test": "TestPass", "Output": "quiet pass log\n"},
            {"Action": "pass", "Package": "p", "Test": "TestPass/child", "Elapsed": 1},
            {"Action": "pass", "Package": "p", "Test": "TestPass", "Elapsed": 2},
            {"Action": "output", "Package": "p", "Test": "TestFail/child", "Output": "failure detail\n"},
            {"Action": "fail", "Package": "p", "Test": "TestFail/child", "Elapsed": 3},
            {"Action": "fail", "Package": "p", "Test": "TestFail", "Elapsed": 4},
            {"Action": "skip", "Package": "p", "Test": "TestSkip", "Elapsed": 10},
            {"Action": "pass", "Package": "cached", "Test": "TestCached", "Elapsed": 100},
            {"Action": "output", "Package": "cached", "Output": "ok cached (cached)\n"},
            {"Action": "output", "Package": "p", "Test": "TestIncomplete", "Output": "interrupted test detail\n"},
        ]
        process = mock.MagicMock()
        process.__enter__.return_value = process
        process.stdout = io.StringIO("".join(json.dumps(event) + "\n" for event in events))
        process.wait.return_value = 1
        output = io.StringIO()
        with mock.patch.object(ci.subprocess, "Popen", return_value=process) as popen, contextlib.redirect_stdout(output):
            code, timings = ci.run_native_tests(["go", "test", "-race", "./..."], Path("/repo/client"), {"CGO_ENABLED": "1"})
        self.assertEqual(code, 1)
        self.assertEqual(timings, [(2, "p", "TestPass")])
        self.assertIn("failure detail", output.getvalue())
        self.assertIn("interrupted test detail", output.getvalue())
        self.assertNotIn("quiet pass log", output.getvalue())
        self.assertEqual(popen.call_args.args[0], ["go", "test", "-json", "-race", "./..."])

    def test_slowest_three_combine_modules_without_counting_subtests(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "0.xml").write_text(
                '<testsuites><testsuite failures="1" errors="1">'
                '<testcase classname="root" name="TestSuite" time="8"/>'
                '<testcase classname="root" name="TestSuite/child" time="7"/>'
                '<testcase classname="root" name="TestSkip" time="100"><skipped/></testcase>'
                '<testcase classname="root" name="TestFail" time="200"><failure/></testcase>'
                '<testcase classname="root" name="TestError" time="300"><error/></testcase>'
                '<testcase classname="root" name="TestIncomplete"/>'
                '</testsuite></testsuites>'
            )
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                ci.print_slowest(root, [(10, "client", "TestSlow"), (9, "tools", "TestMedium"), (1, "client", "TestFast")])
            lines = output.getvalue().splitlines()
            self.assertEqual([line.strip() for line in lines[-3:]], [
                "1. 10.000s  client.TestSlow", "2. 9.000s  tools.TestMedium", "3. 8.000s  root.TestSuite",
            ])

    def test_no_successful_tests_still_prints_summary(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "0.xml").write_text('<testsuite><testcase classname="p" name="TestFail" time="100"><failure/></testcase></testsuite>')
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                ci.print_slowest(root, [])
            self.assertIn("No successful tests recorded.", output.getvalue())
            self.assertNotIn("TestFail", output.getvalue())

    def test_update_uses_medians_and_ignores_failures(self):
        with tempfile.TemporaryDirectory() as temp:
            reports = []
            for index, seconds in enumerate((3, 9)):
                report = Path(temp) / f"{index}.xml"
                report.write_text(f'<testsuites><testsuite><testcase classname="p" name="t" time="{seconds}"/><testcase classname="p" name="failed" time="100"><failure/></testcase></testsuite></testsuites>')
                reports.append(report)
            profile = ci.update_profile({"tests": {}, "packages": {}}, reports)
            self.assertEqual(profile["packages"], {"p": {"seconds": 6, "longest": 6}})
            self.assertEqual(profile["tests"], {"p.t": 6})

    def test_failed_samples_preserve_previous_successful_history(self):
        with tempfile.TemporaryDirectory() as temp:
            report = Path(temp) / "report.xml"
            report.write_text(
                '<testsuites><testsuite failures="1" errors="1">'
                '<testcase classname="p" name="TestOld" time="0.01"><failure/></testcase>'
                '<testcase classname="p" name="TestOld" time="1000"><error/></testcase>'
                '<testcase classname="p" name="TestOld" time="2000"><skipped/></testcase>'
                '<testcase classname="p" name="TestOld"/>'
                '<testcase classname="p" name="TestOld/child" time="3000"/>'
                '<testcase classname="p" name="TestPass" time="4"/>'
                '</testsuite></testsuites>'
            )
            profile = {"tests": {"p.TestOld": 12}, "packages": {"p": {"seconds": 12, "longest": 12}}}
            ci.update_profile(profile, [report])
            self.assertEqual(profile["tests"], {"p.TestOld": 12, "p.TestPass": 4})
            self.assertEqual(profile["packages"], {"p": {"seconds": 16, "longest": 12}})

    def test_rejects_invalid_successful_test_durations(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            report = root / "0.xml"
            for duration in ("-1", "nan", "inf"):
                with self.subTest(duration=duration):
                    report.write_text(f'<testsuite><testcase classname="p" name="TestPass" time="{duration}"/></testsuite>')
                    with self.assertRaises(ValueError):
                        ci.update_profile({"tests": {}, "packages": {}}, [report])
                    with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(ValueError):
                        ci.print_slowest(root, [])

    def test_rejects_invalid_history(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "durations.json"
            path.write_text(json.dumps({"packages": {"p": {"seconds": -1, "longest": 1}}}))
            with self.assertRaises(ValueError):
                ci.read_profile(path)


if __name__ == "__main__":
    unittest.main()
