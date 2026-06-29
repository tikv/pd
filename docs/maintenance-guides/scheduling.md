# Scheduling

This guide covers scheduling in `pkg/schedule/`.

## Purpose And Scope

Scheduling owns:

- coordinator lifecycle
- schedulers and checker controllers
- placement rule evaluation at the scheduler boundary
- operator creation, tracking, dispatch, and TTL recording
- store and region filters
- scatter and split helpers
- scheduler plugins
- scheduling diagnostics

It consumes cluster metadata from `server/cluster` and `pkg/core`. It should not
own persistence details outside scheduler config and placement metadata.
Detailed placement-rule, region-label, and affinity-policy maintenance lives in
[placement-policy](./placement-policy.md).

## Architectural Views

### Coordinator view

`Coordinator` wires checkers, schedulers, operator controller, scatterer,
splitter, heartbeat streams, plugin interface, and diagnostics.

### Decision view

Checkers patrol regions for correctness and rule compliance. Schedulers produce
balancing or policy-driven operators. Filters decide candidate eligibility.
Operators encode concrete actions delivered to TiKV.

### Dispatch view

Operators are pushed through heartbeat streams or actively driven by
`drivePushOperator`. Operator status and TTL records explain whether an action
was created, running, finished, timed out, or canceled.

## Process Lifecycle And Startup Sequencing

Important anchors:

1. `schedule.NewCoordinator`
2. `Coordinator.RunUntilStop`
3. `Coordinator.PatrolRegions`
4. `Coordinator.drivePushOperator`
5. `schedulers.Controller`
6. `checker.Controller`
7. `operator.Controller`

In classic mode, `RaftCluster.checkSchedulingService` starts scheduling jobs
inside PD. In microservice mode, scheduling can be independent and PD may use
fallback behavior depending on config and service discovery.

Maintenance rules:

- Do not create operators before the cluster has enough prepared region state.
- Do not bypass filters unless the caller has a narrowly documented reason.
- Operator lifecycle changes must preserve status tracking and metrics.

## Data Model And Metadata Contracts

Hot contracts:

- scheduler names and args
- placement rules and rule groups
- store filters and candidate comparison
- operator kind, step, status, and influence
- pending processed regions
- scatter and split group semantics
- scheduler config persisted through storage

Maintenance rule:

- Scheduler decisions are only as correct as the cluster snapshot and filters
  they use. Changes that alter `StoreInfo` or `RegionInfo` interpretation
  require scheduling review.

## Observability And Operational Signals

Open these first:

- `pkg/schedule/metrics.go`
- `pkg/schedule/checker/metrics.go`
- `pkg/schedule/operator/metrics.go`
- `pkg/schedule/schedulers/metrics.go`
- `pkg/schedule/filter/metrics.go`
- `pkg/schedule/scatter/metrics.go`

Signals to preserve:

- scheduler run counters and durations
- checker patrol duration and pending region state
- operator create, finish, timeout, cancel, and replace metrics
- filter reason counters
- scatter/split metrics
- diagnostic records

## Change Management Guidance

- New schedulers should define config, tests, metrics, and operator behavior.
- New filters should include tests for positive and negative candidate cases.
- Placement rule changes should be reviewed with
  [placement-policy](./placement-policy.md) and tested with fit and
  rule-manager tests.
- Operator changes need tests for status transitions and step semantics.
- Microservice scheduling changes need review with `pkg/mcs/scheduling`.

## Must-Read File Order

1. `pkg/schedule/coordinator.go`
2. `pkg/schedule/prepare_checker.go`
3. `pkg/schedule/checker/checker_controller.go`
4. `pkg/schedule/schedulers/scheduler_controller.go`
5. `pkg/schedule/schedulers/scheduler.go`
6. `pkg/schedule/operator/operator_controller.go`
7. `pkg/schedule/operator/operator.go`
8. `pkg/schedule/operator/step.go`
9. `pkg/schedule/filter/filters.go`
10. `pkg/schedule/placement/rule_manager.go`
11. `pkg/schedule/placement/fit.go`
12. `pkg/schedule/hbstream/heartbeat_streams.go`

## Review Checklist

- Does the scheduler or checker run only when the required cluster state is
  prepared?
- Are candidate stores filtered for health, state, labels, placement, and
  special eviction flags?
- Does the operator remain idempotent across heartbeat retries?
- Are operator status transitions and TTL records still correct?
- Does the change alter balancing fairness, hot-region behavior, or slow-store
  behavior?
- Are placement rule changes compatible with existing rule persistence?
- Are microservice and classic scheduling paths both considered?
