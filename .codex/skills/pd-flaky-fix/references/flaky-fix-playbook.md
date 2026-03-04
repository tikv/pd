# PD Flaky Fix Playbook

Generated from 218 merged flaky-adjacent PRs.

Use these templates as hypothesis starters. Always validate against current issue evidence.

## Pattern 1: Data Race Elimination

**Trigger signals**
- Failure signature and code shape match this class.
- Nearby stable tests use stricter synchronization or cleanup patterns.

**Minimal fix strategy**
- Remove shared mutable access races with scoped locks/atomics or isolated test fixtures.

**TDD guidance**
- Add a focused failing test/assertion first.
- Keep implementation change minimal and rerun focused test to green.

**Verification commands (example)**
```bash
make gotest GOTEST_ARGS='./<pkg> -run <TestName> -count=1 -v'
```

**Representative PRs**
- #10184: tests: fix data race in TestOnlineProgress (https://github.com/tikv/pd/pull/10184)
- #10176: fix(resourcemanager): call getServiceLimit to prevent data race (https://github.com/tikv/pd/pull/10176)
- #9979: operator: fix data race avoid panic (https://github.com/tikv/pd/pull/9979)
- #9899: *: fix data race of cluster (https://github.com/tikv/pd/pull/9899)
- #9843: test: fix data race for `balance-range-scheduler` (https://github.com/tikv/pd/pull/9843)

## Pattern 2: Panic Guarding and Nil Safety

**Trigger signals**
- Failure signature and code shape match this class.
- Nearby stable tests use stricter synchronization or cleanup patterns.

**Minimal fix strategy**
- Add nil/state guards before dereference and preserve existing control flow.

**TDD guidance**
- Add a focused failing test/assertion first.
- Keep implementation change minimal and rerun focused test to green.

**Verification commands (example)**
```bash
make gotest GOTEST_ARGS='./<pkg> -run <TestName> -count=1 -v'
```

**Representative PRs**
- #10073: resource control: avoid panic when reservation is nil (https://github.com/tikv/pd/pull/10073)
- #9979: operator: fix data race avoid panic (https://github.com/tikv/pd/pull/9979)
- #9781: mcs: fix panic of store not found (https://github.com/tikv/pd/pull/9781)
- #9634: testutil: fix LastHeartbeat assignment and streamline PutMetaStore call (https://github.com/tikv/pd/pull/9634)
- #8965: cluster: fix panic when minResolvedTS is not initialized (https://github.com/tikv/pd/pull/8965)

## Pattern 3: Timeout and Retry Budget Tuning

**Trigger signals**
- Failure signature and code shape match this class.
- Nearby stable tests use stricter synchronization or cleanup patterns.

**Minimal fix strategy**
- Replace brittle hard timeout assumptions with condition-aware waits and bounded retries.

**TDD guidance**
- Add a focused failing test/assertion first.
- Keep implementation change minimal and rerun focused test to green.

**Verification commands (example)**
```bash
make gotest GOTEST_ARGS='./<pkg> -run <TestName> -count=1 -v'
```

**Representative PRs**
- #9986: tso: improve the high availability of etcd client for etcd save timestamp (https://github.com/tikv/pd/pull/9986)
- #8539: mcs: fix potential data race in scheduling server (https://github.com/tikv/pd/pull/8539)
- #8336: statistics: fix data race in `IsRegionHot` (https://github.com/tikv/pd/pull/8336)
- #6295: dr-autosync: add recover timeout (https://github.com/tikv/pd/pull/6295)
- #7271: ci: run `make check` with longer timeout (https://github.com/tikv/pd/pull/7271)

## Pattern 4: Deadlock Avoidance

**Trigger signals**
- Failure signature and code shape match this class.
- Nearby stable tests use stricter synchronization or cleanup patterns.

**Minimal fix strategy**
- Break circular waits by adjusting lock/order or test orchestration points.

**TDD guidance**
- Add a focused failing test/assertion first.
- Keep implementation change minimal and rerun focused test to green.

**Verification commands (example)**
```bash
make gotest GOTEST_ARGS='./<pkg> -run <TestName> -count=1 -v'
```

**Representative PRs**
- #10037: *: upgrade deadlock to v0.3.6 (https://github.com/tikv/pd/pull/10037)
- #5758: *: fix `AtomicCheckAndPutRegion` deadlock (https://github.com/tikv/pd/pull/5758)
- #4863: *: use build tag to manage deadlock check (https://github.com/tikv/pd/pull/4863)
- #2625: cooridinator: fix the issue caused a deadlock when deleting scheduler  (https://github.com/tikv/pd/pull/2625)
- #2380: go-tools: add go-deadlock dep (#2380) (https://github.com/tikv/pd/pull/2380)

## Pattern 5: Flaky and Unstable Test Stabilization

**Trigger signals**
- Failure signature and code shape match this class.
- Nearby stable tests use stricter synchronization or cleanup patterns.

**Minimal fix strategy**
- Remove non-determinism by making readiness/ownership checks explicit.

**TDD guidance**
- Add a focused failing test/assertion first.
- Keep implementation change minimal and rerun focused test to green.

**Verification commands (example)**
```bash
make gotest GOTEST_ARGS='./<pkg> -run <TestName> -count=1 -v'
```

**Representative PRs**
- #10203: test: fix flaky test TestForwardTestSuite in next-gen (https://github.com/tikv/pd/pull/10203)
- #10134: tests: fix some unstable tests (https://github.com/tikv/pd/pull/10134)
- #9678: test: add test log to flaky test `TestSchedulerDiagnostic` (https://github.com/tikv/pd/pull/9678)
- #9634: testutil: fix LastHeartbeat assignment and streamline PutMetaStore call (https://github.com/tikv/pd/pull/9634)
- #9465: *: fix flaky test `TestPreparingProgress` and `TestRemovingProgress` (https://github.com/tikv/pd/pull/9465)

## Pattern 6: Goroutine/Leak Cleanup

**Trigger signals**
- Failure signature and code shape match this class.
- Nearby stable tests use stricter synchronization or cleanup patterns.

**Minimal fix strategy**
- Ensure goroutine/resource teardown happens on every exit path.

**TDD guidance**
- Add a focused failing test/assertion first.
- Keep implementation change minimal and rerun focused test to green.

**Verification commands (example)**
```bash
make gotest GOTEST_ARGS='./<pkg> -run <TestName> -count=1 -v'
```

**Representative PRs**
- #10203: test: fix flaky test TestForwardTestSuite in next-gen (https://github.com/tikv/pd/pull/10203)
- #7749: http/client, test: remove IgnoreTopFunction of go leak and repair http SDK leak (https://github.com/tikv/pd/pull/7749)
- #6585: TSO Proxy: improve tso proxy reliability (https://github.com/tikv/pd/pull/6585)

## Pattern 7: Leadership/Watch Synchronization

**Trigger signals**
- Failure signature and code shape match this class.
- Nearby stable tests use stricter synchronization or cleanup patterns.

**Minimal fix strategy**
- Wait on concrete leader/watch state transitions rather than elapsed time.

**TDD guidance**
- Add a focused failing test/assertion first.
- Keep implementation change minimal and rerun focused test to green.

**Verification commands (example)**
```bash
make gotest GOTEST_ARGS='./<pkg> -run <TestName> -count=1 -v'
```

**Representative PRs**
- #9241: test: fix flaky test `TestTSONotLeader` (https://github.com/tikv/pd/pull/9241)
- #8682: *: fix `TestStoreWatch` panic (https://github.com/tikv/pd/pull/8682)
- #8605: *: fix data race of `TestWatchResourceGroup` (https://github.com/tikv/pd/pull/8605)
- #8407: middleware: fix waitForLeader in redirector panic (https://github.com/tikv/pd/pull/8407)
- #7911: client: avoid panic when leader gRPC conn is nil (https://github.com/tikv/pd/pull/7911)

## Pattern 8: Test Harness and Fixture Alignment

**Trigger signals**
- Failure signature and code shape match this class.
- Nearby stable tests use stricter synchronization or cleanup patterns.

**Minimal fix strategy**
- Align setup/teardown and assertion semantics with stable tests in same suite.

**TDD guidance**
- Add a focused failing test/assertion first.
- Keep implementation change minimal and rerun focused test to green.

**Verification commands (example)**
```bash
make gotest GOTEST_ARGS='./<pkg> -run <TestName> -count=1 -v'
```

**Representative PRs**
- #10203: test: fix flaky test TestForwardTestSuite in next-gen (https://github.com/tikv/pd/pull/10203)
- #10184: tests: fix data race in TestOnlineProgress (https://github.com/tikv/pd/pull/10184)
- #10176: fix(resourcemanager): call getServiceLimit to prevent data race (https://github.com/tikv/pd/pull/10176)
- #10134: tests: fix some unstable tests (https://github.com/tikv/pd/pull/10134)
- #10073: resource control: avoid panic when reservation is nil (https://github.com/tikv/pd/pull/10073)

