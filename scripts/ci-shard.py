#!/usr/bin/env python3
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

"""Assign every test package to one runner using recorded CI durations."""

import argparse
import collections
import json
import math
import os
from pathlib import Path
import statistics
import subprocess
import tempfile
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parent.parent
MODULE_PREFIX = "github.com/tikv/pd"
MODULES = (".", "client", "tools", "tests/integrations")
# A scheduling preference for keeping modules together; common dependency
# compilation is already part of the runner's preparation step.
MODULE_PENALTY_SECONDS = 30


# The pinned Prow image provides Python 3.6; keep the planner dependency-free.
Package = collections.namedtuple("Package", "module path seconds longest")


def read_profile(filename):
    profile = json.loads(Path(filename).read_text())
    for package, timing in profile["packages"].items():
        for field in ("seconds", "longest"):
            value = timing[field]
            if not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                raise ValueError(f"invalid {field} for {package}: {value}")
    return profile


def discover_packages(root, profile):
    timings = profile["packages"]
    fallback = statistics.median(p["seconds"] for p in timings.values()) if timings else 1
    packages = []
    for module in MODULES:
        tags = "deadlock,nextgen" if os.getenv("NEXT_GEN") == "1" and module not in ("client", "tools") else "deadlock"
        prefix = MODULE_PREFIX if module == "." else f"{MODULE_PREFIX}/{module}"
        output = subprocess.check_output(
            ["go", "list", "-tags", tags, "-f", "{{if or .TestGoFiles .XTestGoFiles}}{{.ImportPath}}{{end}}", "./..."],
            cwd=root / module, universal_newlines=True,
        )
        for full_path in sorted(set(output.split())):
            if full_path != prefix and not full_path.startswith(prefix + "/"):
                raise ValueError(f"unexpected package {full_path} in module {module}")
            relative = "." if full_path == prefix else full_path[len(prefix) + 1:]
            if relative.split("/")[0] in ("bin", "cmd", "realcluster"):
                continue
            timing = timings.get(full_path, {"seconds": fallback, "longest": fallback})
            packages.append(Package(module, relative, timing["seconds"], timing["longest"]))
    return packages


def estimate(packages, parallel):
    modules = collections.defaultdict(list)
    for package in packages:
        modules[package.module].append(package)
    return sum(
        max(sum(p.seconds for p in group) / parallel, max(p.longest for p in group))
        + MODULE_PENALTY_SECONDS
        for group in modules.values()
    )


def plan_shards(packages, count, parallel, eligible=None):
    if count < 1 or parallel < 1:
        raise ValueError("shard count and parallelism must be positive")
    identities = [(p.module, p.path) for p in packages]
    if len(set(identities)) != len(identities):
        raise ValueError("duplicate test package")
    shards = [[] for _ in range(count)]
    for package in sorted(packages, key=lambda p: (-max(p.seconds / parallel, p.longest), -p.seconds, p.module, p.path)):
        candidates = range(count) if eligible is None else eligible[(package.module, package.path)]
        if not candidates:
            raise ValueError(f"no eligible runner for {package.module}: {package.path}")
        index = min(candidates, key=lambda i: (estimate(shards[i] + [package], parallel), len(shards[i]), i))
        shards[index].append(package)
    return shards


def under(path, prefix):
    return not prefix or path == prefix or path.startswith(prefix + "/")


def plan_layout(packages, config, layout_name, parallel):
    layout = config["layouts"][layout_name]
    if [runner["id"] for runner in layout] != list(range(1, len(layout) + 1)):
        raise ValueError("runner IDs must be consecutive and one-based")
    eligible = {}
    for package in packages:
        pools = [name for name, rules in config["pools"].items() if any(
            package.module == rule["module"] and under(package.path, rule["prefix"])
            and not any(under(package.path, excluded) for excluded in rule.get("exclude", []))
            for rule in rules
        )]
        if len(pools) != 1:
            raise ValueError(f"package must match exactly one test scope: {package.module}: {package.path} ({pools})")
        eligible[(package.module, package.path)] = [i for i, runner in enumerate(layout) if pools[0] in runner["pools"]]
    shards = plan_shards(packages, len(layout), parallel, eligible)
    for module in ("client", "tools"):
        owners = [i for i, shard in enumerate(shards) if any(p.module == module for p in shard)]
        if len(owners) > 1:
            raise ValueError(f"native go test module {module} must stay on one runner")
    return shards


def merge_results(directory, root):
    suites = ET.Element("testsuites")
    coverage = []
    mode = None
    for filename in sorted(directory.glob("*.xml")):
        report = ET.parse(filename).getroot()
        suites.extend([report] if report.tag == "testsuite" else list(report))
    ET.ElementTree(suites).write(root / "junitfile", encoding="utf-8", xml_declaration=True)
    for filename in sorted(directory.glob("*.cov")):
        lines = filename.read_text().splitlines()
        if not lines:
            continue
        if mode is not None and mode != lines[0]:
            raise ValueError(f"inconsistent coverage mode in {filename}")
        mode = lines[0]
        coverage.extend(lines[1:])
    (root / "covprofile").write_text("\n".join([mode or "mode: atomic", *coverage]) + "\n")


def run_native_tests(command, directory, environment):
    timings = []
    cached = set()
    output = collections.defaultdict(list)
    command = [*command[:2], "-json", *command[2:]]
    with subprocess.Popen(command, cwd=directory, env=environment, stdout=subprocess.PIPE, universal_newlines=True) as process:
        for line in process.stdout:
            try:
                event = json.loads(line)
            except ValueError:
                print(line, end="", flush=True)
                continue
            package, test = event.get("Package", ""), event.get("Test", "")
            key = (package, test.split("/", 1)[0])
            message = event.get("Output", "")
            if message:
                if test:
                    output[key].append(message)
                else:
                    if "(cached)" in message:
                        cached.add(package)
                    print(message, end="", flush=True)
            if test and "/" not in test and event.get("Action") in ("pass", "fail", "skip"):
                if event["Action"] == "fail":
                    print("".join(output[key]), end="", flush=True)
                output.pop(key, None)
                if event["Action"] != "skip":
                    timings.append((event.get("Elapsed", 0), package, test))
        returncode = process.wait()
    if returncode:
        for messages in output.values():
            print("".join(messages), end="", flush=True)
    return returncode, [timing for timing in timings if timing[1] not in cached]


def print_slowest(directory, native_timings):
    timings = list(native_timings)
    for filename in sorted(directory.glob("*.xml")):
        for case in ET.parse(filename).iter("testcase"):
            if case.find("skipped") is None and "/" not in case.get("name", ""):
                timings.append((float(case.get("time", "0")), case.get("classname", ""), case.get("name", "")))
    print("\nSlowest completed tests on this runner (actual elapsed; top-level tests/suites):", flush=True)
    for index, (seconds, package, test) in enumerate(sorted(timings, key=lambda item: (-item[0], item[1], item[2]))[:3], 1):
        print(f"  {index}. {seconds:.3f}s  {package}.{test}", flush=True)
    if not timings:
        print("  No completed tests recorded.", flush=True)


def run_shard(root, packages, durations, parallel):
    by_module = collections.defaultdict(list)
    for package in packages:
        by_module[package.module].append(package)
    with tempfile.TemporaryDirectory(prefix="pd-ci-results-") as temp:
        results = Path(temp)
        native_timings = []
        try:
            for index, (module, group) in enumerate(sorted(by_module.items(), key=lambda item: -estimate(item[1], parallel))):
                environment = os.environ.copy()
                if module in ("client", "tools"):
                    # Preserve the existing package-level test lifecycle in these modules.
                    command = [
                        "go", "test", "-tags", "deadlock", "-race", "-cover",
                        "-covermode=atomic", f"-coverprofile={results / f'{index}.cov'}",
                        "-coverpkg=../...", "./...",
                    ]
                    directory = root / module
                    environment["CGO_ENABLED"] = "1"
                else:
                    command = [
                        str(root / "bin/pd-ut"), "run", "--module", module,
                        "--packages", ",".join(sorted(p.path for p in group)),
                        "--test-durations", str(durations.resolve()), "--parallel", str(parallel),
                        "--race", "--coverprofile", str(results / f"{index}.cov"),
                        "--junitfile", str(results / f"{index}.xml"),
                    ]
                    directory = root
                print(f"Running {len(group)} packages from {module}", flush=True)
                if module in ("client", "tools"):
                    returncode, timings = run_native_tests(command, directory, environment)
                    native_timings.extend(timings)
                else:
                    returncode = subprocess.run(command, cwd=directory, env=environment, check=False).returncode
                if returncode:
                    return returncode
            return 0
        finally:
            print_slowest(results, native_timings)
            merge_results(results, root)


def update_profile(profile, reports):
    samples = collections.defaultdict(list)
    for filename in reports:
        for case in ET.parse(filename).iter("testcase"):
            if case.find("failure") is not None or case.find("skipped") is not None:
                continue
            seconds = float(case.get("time", "0"))
            if not math.isfinite(seconds) or seconds < 0:
                raise ValueError(f"invalid test duration in {filename}")
            samples[(case.attrib["classname"], case.attrib["name"])].append(seconds)
    for (package, test), values in samples.items():
        profile["tests"][f"{package}.{test}"] = round(statistics.median(values), 3)
    for package in {package for package, _ in samples}:
        values = [seconds for key, seconds in profile["tests"].items() if key.startswith(package + ".")]
        profile["packages"][package] = {"seconds": round(sum(values), 3), "longest": max(values)}
    return profile


def available_parallelism():
    available = os.cpu_count() or 1
    if os.getenv("GOMAXPROCS", "").isdigit():
        available = min(available, max(1, int(os.environ["GOMAXPROCS"])))
    try:
        quota, period = Path("/sys/fs/cgroup/cpu.max").read_text().split()
        if quota != "max":
            available = min(available, max(1, math.ceil(int(quota) / int(period))))
    except (OSError, ValueError):
        try:
            quota = int(Path("/sys/fs/cgroup/cpu/cpu.cfs_quota_us").read_text())
            period = int(Path("/sys/fs/cgroup/cpu/cpu.cfs_period_us").read_text())
            if quota > 0:
                available = min(available, max(1, math.ceil(quota / period)))
        except (OSError, ValueError):
            pass
    return available


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("plan", "run", "update"))
    parser.add_argument("--count", type=int)
    parser.add_argument("--layout", default="standard", choices=("standard", "nextgen"))
    parser.add_argument("--index", type=int, default=1, help="one-based runner index")
    parser.add_argument("--parallel", type=int, default=available_parallelism(), help="test processes on this runner; package assignments use the profile's fixed parallelism")
    parser.add_argument("--durations", type=Path, default=ROOT / "scripts/ci-test-durations.json")
    parser.add_argument("reports", nargs="*", type=Path)
    args = parser.parse_args()
    profile = read_profile(args.durations)
    if args.action == "update":
        if not args.reports:
            parser.error("update requires at least one JUnit report")
        updated = update_profile(profile, args.reports)
        args.durations.write_text(json.dumps(updated, indent=2, sort_keys=True) + "\n")
        return 0
    config = json.loads((ROOT / "scripts/ci-test-groups.json").read_text())
    layout = config["layouts"][args.layout]
    count = len(layout)
    if args.count is not None and args.count != count:
        parser.error(f"layout {args.layout} requires {count} runners")
    if not 1 <= args.index <= count:
        parser.error("runner index must be between 1 and shard count")
    if args.parallel < 1:
        parser.error("parallelism must be positive")
    planning_parallel = profile.get("parallel", 4)
    if not isinstance(planning_parallel, int) or planning_parallel < 1:
        parser.error("profile parallelism must be a positive integer")
    shards = plan_layout(discover_packages(ROOT, profile), config, args.layout, planning_parallel)
    for index, shard in enumerate(shards, 1):
        print(f"Shard {index}/{count} ({layout[index - 1]['name']}): {len(shard)} packages, estimated cost {estimate(shard, planning_parallel):.1f}s", flush=True)
        if args.action == "plan":
            for package in sorted(shard, key=lambda p: (p.module, p.path)):
                print(f"  {package.module}: {package.path}")
    if args.action == "run":
        print(f"Selected runner {args.index}/{count}: {layout[args.index - 1]['name']}", flush=True)
        return run_shard(ROOT, shards[args.index - 1], args.durations, args.parallel)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
