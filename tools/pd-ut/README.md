# pd-ut

pd-ut is a tool to run unit tests for PD.

## Build

1. [Go](https://golang.org/) Version 1.27.3 or later
2. In the root directory of the [PD project](https://github.com/tikv/pd), use the `make pd-ut` command to compile and generate `bin/pd-ut`

## Usage

This section describes how to use the pd-ut tool.

### brief run all tests
```shell
make ut 
```


### run by pd-ut

- You should `make failpoint-enable` before running the tests.
- And after running the tests, you should `make failpoint-disable` and `make clean-test` to disable the failpoint and clean the environment.

#### Flags description

```shell
// run all tests
pd-ut

// show usage
pd-ut -h

// list all packages
pd-ut list

// list test cases of a single package
pd-ut list $package

// list test cases that match a pattern
pd-ut list $package 'r:$regex'

// run all tests
pd-ut run

// run test all cases of a single package
pd-ut run $package

// run test cases of a single package
pd-ut run $package $test

// run test cases that match a pattern
pd-ut run $package 'r:$regex'

// build all test package
pd-ut build

// build a test package
pd-ut build xxx

// write the junitfile
pd-ut run --junitfile xxx

// test with race flag
pd-ut run --race

// test with coverprofile
pd-ut run --coverprofile xxx
go tool cover --func=xxx
```

### Balance CI runners

`make ci-test-job JOB_INDEX=1 JOB_COUNT=10` assigns each test package to one
runner across the root, client, tools, and integration modules. All runners must
use the same commit, layout, `JOB_COUNT`, and `NEXT_GEN` setting. Assignment uses
the timing profile's fixed parallelism so different CPU quotas cannot produce
different package ownership. `pd-ut` execution parallelism follows each runner's CPU
quota and can be overridden with `--parallel`.
The existing runner names and their allowed test scopes are recorded in
`scripts/ci-test-groups.json`. Unit tests are balanced across runners 1–3,
`tests/` packages across runners 4–5, and runners 6–10 retain their tools,
client, TSO, and microservice scopes. Each package must match exactly one scope;
a new integration category needs an explicit scope entry before it can run.

The planner uses recorded test durations, the longest test in each package,
and a small preference for keeping modules together to reduce repeated build
setup. New packages are discovered with `go list` and receive a median package
estimate. Package assignments are deterministic. Within root and integration
modules, `pd-ut` starts tests with the longest recorded durations first. The
client and tools modules retain their existing `go test` execution and
package-level setup and cleanup. Duration estimates exclude the common
installation and compilation steps, so CI completion times still need to be
checked after CI runs with this change. The planner uses Python 3.6 or later
and requires no third-party packages.

CI test jobs install only the failpoint tool needed to prepare the tests.
`make install-tools` still installs the complete toolchain for static checks
and generation. GitHub Actions shares Go module caches within each workflow,
keyed by the runner image, Go version, module dependency files, and tool pins.
Build caches are separate for each workflow and runner. A new commit restores
the previous build snapshot for the same workload and dependencies, then saves
its updated cache.
Native Go tests use `-count=1` so restored build caches do not skip test execution.

```shell
# Inspect every runner's packages without compiling or running tests.
python3 scripts/ci-shard.py plan --count 10 --parallel 4

# Refresh the committed estimates from downloaded test-results artifacts.
python3 scripts/ci-shard.py update artifacts/*/junitfile

# Run an exact package set; similar package names are not included.
bin/pd-ut run --module tools --packages pd-ut,pd-ut/alloc --race
```

The planner merges module coverage and available JUnit reports into the existing
`covprofile` and `junitfile` outputs. Test failures still return a nonzero exit
code and their JUnit reports are retained. As before, native client and tools
tests produce coverage without JUnit test cases. The committed timing snapshot
comes from the CI run identified in `scripts/ci-test-durations.json`; refresh it when
new reports show sustained runner imbalance. The `nextgen` layout preserves
the current three Prow runner scopes: unit tests; `tests/` plus tools; and
integration tests plus client. Each runner runs
its complete package set together instead of executing several numbered jobs
in sequence. Use `NEXT_GEN=1 JOB_LAYOUT=nextgen JOB_COUNT=3` with indices 1–3.
The CI configuration must opt into that layout; existing callers keep the
standard ten-runner layout.
The client and tools modules retain their existing `deadlock` test tags in
NextGen CI; the root and integration modules also use `nextgen`.

Each runner prints its three slowest successful top-level tests or suites using
actual elapsed times from that run. A failed job still prints the summary for
tests that passed. Failed, errored, skipped, and incomplete tests are excluded
from both the ranking and historical duration updates; a failed sample never
replaces a previously successful timing. Root and
integration timings reuse the existing JUnit reports. Client and tools tests
use `go test -json` with the same package-level test execution; successful test
logs are suppressed and failure logs are retained. Subtests are excluded from
the ranking so their time is not counted again with their parent suite.
Cached native test results are also excluded; their recorded duration comes
from a previous run. The test cache remains enabled as before.
