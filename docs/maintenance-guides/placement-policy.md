# Placement Policy

This guide covers placement rules, region labels, and affinity policy in
`pkg/schedule/placement/`, `pkg/schedule/labeler/`, and
`pkg/schedule/affinity/`.

## Purpose And Scope

Placement policy owns:

- placement rule lifecycle and validation
- rule groups, rule lists, and fit calculation
- label constraints, location labels, isolation scoring, witness rules, and
  orphan peer detection
- region label rules and key-range matching
- affinity groups, key-range to group mapping, and affinity availability
- policy inputs used by checkers, schedulers, scatter, split, and API handlers

It does not own operator execution. Operators consume placement decisions and
belong to scheduling.

## Core Concepts

- Placement rules declare desired replica roles, counts, labels, key ranges, and
  constraints.
- `RuleManager` owns rule loading, validation, adjustment, default-rule
  migration, and region fitting.
- `RegionLabeler` maps key ranges to labels that can affect scheduling and API
  behavior.
- `affinity.Manager` overlays affinity group policy and has stricter lock-order
  requirements than normal rule fitting.
- Policy metadata is persisted and user-visible, so validation and deterministic
  fit behavior are compatibility concerns.

## Architectural Views

### Placement rule view

`RuleManager` loads rules and groups from storage, creates default rules when
needed, validates rule content, builds rule lists, and fits regions to rules.
`RegionFit` describes which peers satisfy which rules and which peers are
orphans.

### Region label view

`RegionLabeler` loads label rules, indexes key ranges through `rangelist`, and
periodically removes expired labels. Label rules are persisted as region rules
and are visible through APIs.

### Affinity view

`affinity.Manager` owns affinity groups, region-to-group cache, key ranges,
group availability, and synchronization with region labels. It uses separate
locks for metadata and in-memory state; lock ordering is a correctness
constraint.

## Process Lifecycle And Startup Sequencing

Important anchors:

1. `placement.NewRuleManager`
2. `RuleManager.Initialize`
3. `RuleManager.AdjustRule`
4. `placement.fitRegion`
5. `labeler.NewRegionLabeler`
6. `RegionLabeler.SetLabelRule`
7. `RegionLabeler.BuildRangeListLocked`
8. `affinity.NewManager`
9. `affinity.Manager.initialize`
10. `affinity.Manager.startAvailabilityCheckLoop`

Startup order in cluster leadership:

1. Initialize placement rules if enabled.
2. Create region labeler.
3. Create affinity manager with the region labeler.
4. Start scheduling and checker logic that consumes these policies.

Maintenance rules:

- Placement rules must be initialized before placement-aware region statistics
  and checkers depend on them.
- Region label range lists must be rebuilt after rule mutations.
- Affinity `metaMutex` must not be taken while holding `RWMutex`.

## Data Model And Metadata Contracts

Hot contracts:

- placement rule group ID, rule ID, role, count, key range, label constraints,
  location labels, isolation level, and witness flag
- default placement rules generated from legacy replication config
- hex-encoded rule keys and key type validation
- `RegionFit`, `RuleFit`, orphan peers, isolation score, and witness score
- region label ID, index, labels, TTL, start time, rule type, and key ranges
- affinity group ID, leader store, voter stores, availability, affinity version,
  and region label mapping
- store conditions that degrade or expire affinity groups

Maintenance rule:

- Policy metadata is persisted and user visible. Validate input strictly, keep
  deterministic fit behavior, and preserve upgrade behavior from legacy config.

## Observability And Operational Signals

Open these first:

- `pkg/schedule/placement`
- `pkg/schedule/labeler/metrics.go`
- `pkg/schedule/affinity/metrics.go`
- `server/api/rule.go`
- `server/api/region_label.go`
- `server/apiv2/handlers/affinity.go`

Signals to preserve:

- rule load and validation logs
- region labeler creation and GC metrics
- affinity group count and affinity region count metrics
- affinity availability logs
- placement status metrics from statistics collection

## Change Management Guidance

- Placement rule changes need tests for fit determinism, role matching,
  isolation score, witness behavior, and orphan handling.
- Region label changes need tests for TTL, key-range parsing, range-list split
  keys, and patch behavior.
- Affinity changes need tests for lock ordering, key-range sync, availability
  transitions, degraded/expired state, and store-condition interpretation.
- API changes need compatibility review because rule and label structs are
  exported through HTTP.

## Must-Read File Order

1. `pkg/schedule/placement/rule_manager.go`
2. `pkg/schedule/placement/rule.go`
3. `pkg/schedule/placement/rule_list.go`
4. `pkg/schedule/placement/fit.go`
5. `pkg/schedule/placement/label_constraint.go`
6. `pkg/schedule/labeler/labeler.go`
7. `pkg/schedule/labeler/rules.go`
8. `pkg/schedule/rangelist/range_list.go`
9. `pkg/schedule/affinity/manager.go`
10. `pkg/schedule/affinity/policy.go`
11. `pkg/schedule/affinity/group.go`
12. `pkg/schedule/checker/rule_checker.go`

## Glossary

- Placement rule:
  declarative replica placement requirement.
- Rule group:
  namespace and ordering container for placement rules.
- `RegionFit`:
  fit result describing which peers satisfy rules and which are orphan peers.
- Orphan peer:
  peer that does not satisfy any placement rule.
- Region label:
  key-range label persisted as label-rule metadata.
- Affinity group:
  policy object that binds regions and stores for availability intent.
- Range list:
  indexed key-range structure used by region label matching.

## Review Checklist

- Are rule and label inputs validated before persistence?
- Is fit behavior deterministic across peer ordering and store ordering?
- Does the change preserve default-rule migration from legacy config?
- Are key ranges encoded and compared in the correct key mode?
- Are expired region labels removed without breaking range-list state?
- Does affinity preserve lock ordering and metadata/cache consistency?
- Do checkers and schedulers observe the intended policy state?
