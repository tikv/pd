# Fix Flaky Test Playbook

This checked-in playbook is the maintained summary artifact for the skill; the raw corpus is intentionally kept out of the repository in this iteration.

Use these patterns as hypothesis starters. Always validate against current issue evidence before changing code.

## Shared Guidance

- Compare the failing test against nearby stable tests before choosing a fix shape.
- Add or strengthen a focused failing test or assertion before changing implementation.
- A representative PR can appear under more than one pattern when the same fix addresses multiple failure modes; that overlap is intentional.
- Prefer narrow verification:

```bash
make gotest GOTEST_ARGS='./<pkg> -run <TestName> -count=1 -v'
```

- Representative PR titles below are copied verbatim from upstream for traceability, including original spelling in the source title.

## Pattern 1: Data Race Elimination

- Fit signals: race detector output or unsynchronized shared mutable state across goroutines.
- Minimal fix shape: remove shared mutable access races with scoped locks, atomics, or isolated test fixtures.

**Representative PRs**
- #10184: tests: fix data race in TestOnlineProgress (https://github.com/tikv/pd/pull/10184)
- #10176: fix(resourcemanager): call getServiceLimit to prevent data race (https://github.com/tikv/pd/pull/10176)
- #9899: *: fix data race of cluster (https://github.com/tikv/pd/pull/9899)
- #9843: test: fix data race for `balance-range-scheduler` (https://github.com/tikv/pd/pull/9843)
- #8539: mcs: fix potential data race in scheduling server (https://github.com/tikv/pd/pull/8539)
- #8336: statistics: fix data race in `IsRegionHot` (https://github.com/tikv/pd/pull/8336)

## Pattern 2: Panic Guarding and Nil Safety

- Fit signals: panic stacks show nil dereference, stale state access, or missing lifecycle guards.
- Minimal fix shape: add nil or state guards before dereference while preserving existing control flow.

**Representative PRs**
- #10073: resource control: avoid panic when reservation is nil (https://github.com/tikv/pd/pull/10073)
- #9781: mcs: fix panic of store not found (https://github.com/tikv/pd/pull/9781)
- #9634: testutil: fix LastHeartbeat assignment and streamline PutMetaStore call (https://github.com/tikv/pd/pull/9634)
- #8965: cluster: fix panic when minResolvedTS is not initialized (https://github.com/tikv/pd/pull/8965)

## Pattern 3: Timeout and Retry Budget Tuning

- Fit signals: failures depend on elapsed-time assumptions, hard sleeps, or brittle retry windows.
- Minimal fix shape: replace fixed timeouts with condition-aware waits and bounded retries.

**Representative PRs**
- #9986: tso: improve the high availability of etcd client for etcd save timestamp (https://github.com/tikv/pd/pull/9986)
- #6295: dr-autosync: add recover timeout (https://github.com/tikv/pd/pull/6295)
- #7271: ci: run `make check` with longer timeout (https://github.com/tikv/pd/pull/7271)

## Pattern 4: Deadlock Avoidance

- Fit signals: blocked goroutines, lock-order inversions, or orchestration steps that wait on each other.
- Minimal fix shape: break circular waits by adjusting lock order or test orchestration points.

**Representative PRs**
- #10037: *: upgrade deadlock to v0.3.6 (https://github.com/tikv/pd/pull/10037)
- #5758: *: fix `AtomicCheckAndPutRegion` deadlock (https://github.com/tikv/pd/pull/5758)
- #4863: *: use build tag to manage deadlock check (https://github.com/tikv/pd/pull/4863)
- #2625: cooridinator: fix the issue caused a deadlock when deleting scheduler  (https://github.com/tikv/pd/pull/2625)
- #2380: go-tools: add go-deadlock dep (#2380) (https://github.com/tikv/pd/pull/2380)

## Pattern 5: Flaky and Unstable Test Stabilization

- Fit signals: failures depend on non-deterministic readiness, ownership, ordering, or background progress.
- Minimal fix shape: remove non-determinism by making readiness and ownership checks explicit.

**Representative PRs**
- #10203: test: fix flaky test TestForwardTestSuite in next-gen (https://github.com/tikv/pd/pull/10203)
- #10134: tests: fix some unstable tests (https://github.com/tikv/pd/pull/10134)
- #9678: test: add test log to flaky test `TestSchedulerDiagnostic` (https://github.com/tikv/pd/pull/9678)
- #9634: testutil: fix LastHeartbeat assignment and streamline PutMetaStore call (https://github.com/tikv/pd/pull/9634)
- #9465: *: fix flaky test `TestPreparingProgress` and `TestRemovingProgress` (https://github.com/tikv/pd/pull/9465)

## Pattern 6: Goroutine/Leak Cleanup

- Fit signals: goleak failures, hanging tests, or teardown paths that miss cancellation and close.
- Minimal fix shape: ensure goroutine and resource teardown happens on every exit path.

**Representative PRs**
- #10203: test: fix flaky test TestForwardTestSuite in next-gen (https://github.com/tikv/pd/pull/10203)
- #7749: http/client, test: remove IgnoreTopFunction of go leak and repair http SDK leak (https://github.com/tikv/pd/pull/7749)
- #6585: TSO Proxy: improve tso proxy reliability (https://github.com/tikv/pd/pull/6585)

## Pattern 7: Leadership/Watch Synchronization

- Fit signals: test behavior races leader election, watch propagation, or redirect readiness.
- Minimal fix shape: wait on concrete leader or watch state transitions instead of elapsed time.

**Representative PRs**
- #9241: test: fix flaky test `TestTSONotLeader` (https://github.com/tikv/pd/pull/9241)
- #8682: *: fix `TestStoreWatch` panic (https://github.com/tikv/pd/pull/8682)
- #8407: middleware: fix waitForLeader in redirector panic (https://github.com/tikv/pd/pull/8407)
- #7911: client: avoid panic when leader gRPC conn is nil (https://github.com/tikv/pd/pull/7911)

## Pattern 8: Test Harness and Fixture Alignment

- Fit signals: the failing test diverges from stable suite setup, teardown, or assertion patterns.
- Minimal fix shape: align setup, teardown, and assertion semantics with stable tests in the same suite.

**Representative PRs**
- #10203: test: fix flaky test TestForwardTestSuite in next-gen (https://github.com/tikv/pd/pull/10203)
- #10184: tests: fix data race in TestOnlineProgress (https://github.com/tikv/pd/pull/10184)
- #10176: fix(resourcemanager): call getServiceLimit to prevent data race (https://github.com/tikv/pd/pull/10176)
- #10134: tests: fix some unstable tests (https://github.com/tikv/pd/pull/10134)
- #10073: resource control: avoid panic when reservation is nil (https://github.com/tikv/pd/pull/10073)
