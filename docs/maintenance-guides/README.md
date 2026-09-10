# PD Maintenance Guides

This directory is a maintainership-oriented index for the PD repository. It is
not a user manual. It is meant to help maintainers, reviewers, and contributors
answer four questions quickly:

1. Which subsystem owns this behavior?
2. Which files are the real entry points?
3. Which invariants are easy to break?
4. Which tests, metrics, and docs should move with the change?

## Maintainer Contract

These files are part of the repository's maintenance surface.

- Developers and reviewers should read the relevant guide here before making or
  reviewing non-trivial changes.
- These guides are assistant documents for implementation and review, not
  optional afterthoughts.
- If a change modifies ownership boundaries, startup order, data contracts,
  invariants, operational signals, or the recommended reading map for a covered
  subsystem, update the matching guide in the same change.
- A code change that invalidates these guides but does not update them should be
  treated as an incomplete maintenance change.

## Standard Section Contract

Each subsystem guide is expected to cover these domains:

1. Purpose and scope
2. Core concepts
3. Architectural views
4. Process lifecycle and startup sequencing
5. Data model and metadata contracts
6. Observability and operational signals
7. Change management guidance
8. Reading map and companion docs when they add subsystem-specific context
9. Glossary
10. Must-read file order
11. Change-impact matrix or review checklist

The depth varies by subsystem size. Small or focused guides can keep sections
short. Reading-map material can be covered by `Must-Read File Order` and
[repo-overview](./repo-overview.md), but `Core Concepts` and `Glossary` should
remain present because they are the main retrieval anchors for agents and new
reviewers.

## How To Use This Set

- Start with [Repository Overview](./repo-overview.md) to understand layer
  boundaries.
- Open the guide for the subsystem you are touching.
- Use the "Core Concepts", "Glossary", "Must-Read File Order", and any
  "Change-Impact Matrix" or "Review Checklist" sections first.
- Treat every guide as a map, not a complete specification. The source of truth
  is still the code.

## Agent Retrieval Entry Points

Agents should treat this directory as a retrieval map:

1. Read this file to classify the subsystem and find the matching guide.
2. Read [repo-overview](./repo-overview.md) for cross-component ownership,
   common terms, and change-impact routing.
3. Read the subsystem guide's `Core Concepts` and `Glossary` before searching
   code so terms like leader, primary, fallback, region, keyspace, and rule are
   interpreted in the right PD context.
4. Use the subsystem guide's `Must-Read File Order` as the first code-search
   seed, then expand with `rg` from the concrete symbols listed there.

## System Map

- Process bootstrap, embedded etcd, PD leader election, service wiring:
  [server](./server.md)
- Static config, persisted options, dynamic config side effects:
  [config](./config.md)
- PD member, leader election, service primary election, etcd utilities:
  [member-election](./member-election.md)
- gRPC, HTTP APIs, request forwarding, and client compatibility:
  [api-and-client](./api-and-client.md)
- Cluster metadata, store/region cache, heartbeats, and background cluster jobs:
  [cluster](./cluster.md)
- Scheduler coordinator, checkers, schedulers, operators, placement rules:
  [scheduling](./scheduling.md)
- Region/store statistics, hot cache, store load and hot peer signals:
  [statistics](./statistics.md)
- Placement rules, region labels, affinity groups and policy fitting:
  [placement-policy](./placement-policy.md)
- Metadata storage backends and endpoint contracts:
  [storage](./storage.md)
- Timestamp oracle, TSO allocation, and TSO primary behavior:
  [tso](./tso.md)
- Keyspace metadata, keyspace groups, microservice discovery and split services:
  [keyspace-and-microservices](./keyspace-and-microservices.md)
- Resource groups, RU token buckets, service limits, resource manager service:
  [resource-manager](./resource-manager.md)

## Guide Index

### Repository

- [Repository Overview](./repo-overview.md)

### Core PD

- [server](./server.md)
- [config](./config.md)
- [member-election](./member-election.md)
- [api-and-client](./api-and-client.md)
- [cluster](./cluster.md)
- [scheduling](./scheduling.md)
- [statistics](./statistics.md)
- [placement-policy](./placement-policy.md)
- [storage](./storage.md)
- [tso](./tso.md)
- [keyspace-and-microservices](./keyspace-and-microservices.md)
- [resource-manager](./resource-manager.md)

## Cross-Cutting Review Checklist

- Check whether the change touches a request hot path:
  TSO, region heartbeat, store heartbeat, scheduler operator dispatch, or API
  forwarding.
- Verify leader/primary ownership:
  PD leader, embedded etcd leader, TSO primary, scheduling primary, resource
  manager primary, and microservice fallback are separate concepts.
- Check metadata contracts:
  cluster ID, store ID, region epoch, placement rules, keyspace state, keyspace
  group assignment, service primary keys, and persisted config.
- Check storage routing:
  region metadata can use the dedicated local region storage, while most other
  metadata remains etcd-backed.
- Check runtime config behavior:
  persisted option updates must have the matching in-memory side effect.
- Check statistics and policy inputs:
  placement rules, region labels, affinity groups, store loads, and hot cache
  state can change scheduler behavior without touching scheduler code.
- Check failure semantics:
  leader change, context cancellation, stream close, follower forwarding,
  timeout, stale region, and retry behavior.
- Check observability:
  metrics, logs, API responses, gRPC errors, and health/readiness endpoints
  should move with behavior changes.
- Move tests with the behavior:
  use failpoint-aware make targets and prefer package-level targeted tests for
  focused changes.

## Current Scope

This first guide set covers the main PD maintenance boundaries:

- `server/`
- `server/config/`
- `server/api/`
- `server/apiv2/`
- `server/cluster/`
- `pkg/member/`
- `pkg/utils/etcdutil/`
- `pkg/core/`
- `pkg/schedule/`
- `pkg/statistics/`
- `pkg/storage/`
- `pkg/tso/`
- `pkg/keyspace/`
- `pkg/mcs/`
- `pkg/mcs/resourcemanager/`
- `client/`

The overview mentions additional packages for cross-component reasoning. They
do not all have dedicated subsystem guides yet.
