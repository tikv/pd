# Repository Overview

This file is the top-level maintenance map for the PD repository. It is the
first file maintainers and reviewers should read before working on a
cross-component change.

## Purpose And Scope

- Describe PD at the maintenance boundary level, not at the user feature level.
- Explain how major subsystems fit together and where ownership boundaries are.
- Highlight repository-wide invariants, sequencing rules, and failure modes.
- Serve as an index into the deeper subsystem guides in this directory.

## System Context And External Dependencies

PD is the Placement Driver for TiKV clusters. It normally runs with:

- TiKV:
  reports store and region heartbeats, receives scheduling operators, and asks
  PD for split IDs and timestamps.
- TiDB:
  requests TSO, uses PD client service discovery, reads cluster metadata, and
  manages GC safe points and resource groups.
- etcd:
  embedded by PD for membership, leader election, cluster metadata, dynamic
  config, and service discovery.
- TiFlash and other TiDB components:
  report store metadata and depend on placement and scheduling decisions.
- PD microservices:
  optional split services for TSO, scheduling, resource manager, router, and
  meta storage.

Operationally significant dependencies include:

- filesystem state under the configured PD data directory
- local LevelDB region metadata storage when enabled
- TLS material and auth configuration
- Prometheus metric scraping and HTTP/gRPC health probes
- failpoint instrumentation in tests

## Core Components And Boundaries

### Process bootstrap and service wiring

`server/` owns embedded etcd startup, PD member initialization, gRPC/HTTP
registration, leader election, service loops, callbacks, and shutdown.

### Configuration and runtime options

`server/config/` owns static config, persisted options, dynamic config access,
and config API compatibility. Runtime side effects belong to the consuming
subsystem but must stay aligned with persisted config.

### Member and election

`pkg/member/`, `pkg/election/`, and `pkg/utils/etcdutil/` own PD member
identity, PD leader election, service primary election, etcd client helpers,
health checks, and watch loops.

### API and client compatibility

`server/grpc_service.go`, `server/api/`, `server/apiv2/`, and `client/` own the
external request boundary. Changes here can affect TiKV, TiDB, operators, and
PD clients even when the implementation change is internal.

### Cluster metadata and heartbeats

`server/cluster/` owns `RaftCluster`, store and region heartbeat handling,
background jobs, cluster bootstrap state, and coordination with scheduling,
replication mode, GC, and statistics. `pkg/core/` owns the in-memory store and
region data model used by schedulers and APIs.

### Scheduling

`pkg/schedule/` owns the coordinator, checkers, schedulers, placement rules,
operators, filters, scatter/split helpers, scheduler plugins, and the heartbeat
stream used to deliver operators to TiKV.

### Statistics and placement policy

`pkg/statistics/` owns region/store status, store load, hot region, hot peer,
and bucket statistics used by scheduling decisions. `pkg/schedule/placement/`,
`pkg/schedule/labeler/`, and `pkg/schedule/affinity/` own persisted policy,
region labels, rule fitting, and affinity availability.

### Metadata storage

`pkg/storage/` owns the storage interfaces and backends. Most metadata is
etcd-backed through endpoint-specific interfaces. Region metadata can be routed
to a specialized local LevelDB storage by `coreStorage`.

### Timestamp service

`pkg/tso/` owns the timestamp oracle and allocator used by the embedded PD TSO
path. `pkg/mcs/tso/` owns the independent TSO microservice path.

### Keyspace and microservices

`pkg/keyspace/` owns keyspace metadata, keyspace states, keyspace groups, and
meta-service group assignment. `pkg/mcs/` owns split service servers,
registration, discovery, primary election, forwarding, and fallback behavior.

### Resource manager

`pkg/mcs/resourcemanager/` owns resource groups, RU token buckets, service
limits, controller config, resource group metadata watches, metering, and the
independent resource manager service.

## Architectural Views

### Layered view

1. Process bootstrap, config, and embedded etcd
2. gRPC/HTTP request edge and client compatibility surface
3. Cluster metadata cache and storage contracts
4. Statistics and placement policy inputs
5. Scheduling decision engine and operator dispatch
6. TSO, keyspace, resource group, and service-discovery control planes
7. Metrics, health, diagnostics, and maintenance APIs

### Runtime ownership view

- Main startup and stop orchestration: `server.Server`
- PD leader ownership: `pkg/member` through `server.leaderLoop`
- Config ownership: `server/config.Config` and `server/config.PersistOptions`
- Cluster state ownership: `server/cluster.RaftCluster`
- Scheduler ownership: `pkg/schedule.Coordinator`
- Store/region cache ownership: `pkg/core.BasicCluster`
- Statistics ownership: `pkg/statistics`
- Placement policy ownership: `pkg/schedule/placement`,
  `pkg/schedule/labeler`, and `pkg/schedule/affinity`
- Timestamp ownership: `pkg/tso.Allocator`
- Keyspace ownership: `pkg/keyspace.Manager` and `pkg/keyspace.GroupManager`
- Resource group ownership: `pkg/mcs/resourcemanager/server.Manager`
- Microservice ownership: `pkg/mcs/*/server`

### Deployment view

- A classic PD process embeds etcd and can provide PD, TSO, scheduling, and
  resource-manager behavior in one process.
- In microservice mode, TSO, scheduling, router, resource manager, and meta
  storage can run as independent services discovered through etcd.
- Fallback paths let PD provide some services when the corresponding
  microservice is absent or disabled by configuration.

## Process Lifecycle And Startup Sequencing

The high-level classic PD startup order is:

1. Build `server.Server` from configuration and service builders.
2. Start embedded etcd and wait for readiness.
3. Create etcd clients and initialize the PD member.
4. Initialize cluster ID, member metadata, ID allocator, encryption manager,
   storage, TSO allocator, `BasicCluster`, `RaftCluster`, keyspace managers,
   metering, GC state manager, heartbeat streams, and hot-region storage.
5. Mark the server running and start service loops.
6. Campaign for PD leader.
7. As leader, keep the leader lease, reload persisted config, initialize TSO,
   run leader callbacks, and start or resume cluster background jobs.

Shutdown runs the inverse ownership path: stop server loops, close keyspace and
TSO state, close clients, member, heartbeat streams, storage, rate limiters, and
registered callbacks.

Maintenance rule:

- Startup and shutdown ordering are correctness concerns. Do not treat them as
  operational details only.

## Data Model And Metadata Contracts

Repository-wide metadata and state contracts include:

- cluster identity:
  cluster ID, bootstrap timestamp, PD member ID, advertised URLs
- store metadata:
  `metapb.Store`, labels, state, address fields, deployment info, slow-store
  state, and store limit settings
- region metadata:
  `metapb.Region`, epoch, peers, leader, range, buckets, approximate size, flow
  statistics, and source freshness
- scheduling metadata:
  placement rules, operators, scheduler config, checker state, pending regions,
  and region label rules
- timestamp metadata:
  TSO physical/logical window, save interval, leader/primary lease, and
  keyspace group ownership
- keyspace metadata:
  keyspace name, ID, state, config, keyspace group assignment, and region-bound
  labels
- service discovery metadata:
  registered service entries, primary keys, expected primary flags, and
  microservice fallback state

Cross-component rule:

- Any change to a persisted, network-exposed, or dynamically watched metadata
  contract should trigger review of the matching guide and usually a doc update
  in this directory.

## Hot Request Paths

### TSO

1. Clients call `PD.Tso` or the TSO microservice.
2. The request is served by `pkg/tso.Allocator` if the local member is serving.
3. The allocator updates and persists timestamp windows through
   `timestampOracle`.
4. In microservice or forwarding mode, requests may be redirected or proxied to
   the current primary.

Review rule:

- TSO changes must preserve monotonicity, lease semantics, logical overflow
  behavior, and leader/primary failure handling.

### Region heartbeat

1. TiKV streams region heartbeats to PD or scheduling service.
2. The service boundary validates leadership and forwards if needed.
3. `server/cluster` converts the heartbeat to `core.RegionInfo`.
4. `RaftCluster` validates epoch/range freshness, updates cache, persists
   metadata when required, collects statistics, and syncs followers.
5. Schedulers and checkers create operators that are sent back through
   heartbeat streams.

Review rule:

- Region heartbeat changes must preserve stale-region rejection, overlap
  handling, async persistence semantics, and operator response behavior.

### Store heartbeat

1. TiKV reports store state and statistics.
2. PD updates `core.StoreInfo`, store limits, slow-store status, node state, and
   scheduling inputs.
3. Background jobs and schedulers consume the updated store view.

Review rule:

- Store heartbeat changes can affect scheduling fairness, slow-node handling,
  health checks, and operator target selection.

## Background Jobs And Determinism

Important background jobs include:

- PD leader election and leader watch
- etcd leader watch and server metrics collection
- encryption key manager loop
- raft cluster service checks
- metrics collection, node state checks, store stats updates
- region sync to followers
- replication mode management
- min resolved TS persistence
- store config sync
- GC state manager and GC tuner
- hot region and storage size collection

Important rule:

- Background work should not change request ordering, leader ownership, or
  metadata freshness guarantees expected by foreground logic.

## Observability And Operational Signals

Important observability surfaces include:

- Prometheus metrics in `server/metrics.go`, `server/cluster/metrics.go`,
  `pkg/schedule/metrics.go`, `pkg/core/metrics.go`, `pkg/storage/kv/metrics.go`,
  `pkg/tso/metrics.go`, and `pkg/keyspace/metrics.go`
- HTTP API status, health, readiness, config, region, store, scheduler, and
  maintenance endpoints
- gRPC response headers and error fields
- structured logs with member, region, store, scheduler, and service fields
- pprof and diagnostic APIs

Maintenance rule:

- If behavior changes the operator-facing explanation of a failure or delay,
  update metrics, logs, and API responses together.

## Failure Modes And Triage Playbook

Common repository-wide failure modes:

- PD leader and etcd leader diverge longer than expected
- TSO allocator resets or serves from the wrong ownership state
- region metadata becomes stale, overlaps are mishandled, or follower sync lags
- runtime config is persisted but not applied in memory
- statistics drift changes scheduler behavior unexpectedly
- placement rules, region labels, or affinity metadata reject valid placements
- microservice fallback flips unexpectedly
- resource manager token state or metadata watcher state diverges from storage
- API forwarding loops or returns incompatible errors
- scheduler operator creation ignores a store/region invariant
- storage backend switch causes region metadata warm-up drift

Triage guidance:

1. Identify the first boundary where the wrong behavior becomes visible:
   client, API, cluster cache, scheduler, storage, TSO, or microservice.
2. Check leader/primary ownership before reasoning about state mutation.
3. Check whether the failing path is classic embedded PD or microservice mode.
4. Check metrics and logs before editing code.
5. Re-check whether the current maintenance guide is stale relative to the code.

## Change Management Guidance

- Treat this file and the matching subsystem guide as required context for any
  non-trivial change.
- If a change modifies startup order, ownership boundaries, metadata contracts,
  path-specific behavior, invariants, observability, or reading maps, update the
  relevant guide in the same change.
- If a fix touches an API boundary, explicitly evaluate TiKV, TiDB, PD client,
  and microservice compatibility.
- If the guide is missing facts needed for a safe review, improve the guide as
  part of the maintenance work.

## Change-Impact Matrix

- Startup, shutdown, or leader election changes:
  read [server](./server.md), [member-election](./member-election.md),
  [storage](./storage.md), [tso](./tso.md), and
  [keyspace-and-microservices](./keyspace-and-microservices.md).
- Region or store heartbeat changes:
  read [api-and-client](./api-and-client.md), [cluster](./cluster.md), and
  [statistics](./statistics.md), then [scheduling](./scheduling.md) if the
  change affects operator creation or heartbeat responses.
- Scheduler, checker, operator, or placement changes:
  read [scheduling](./scheduling.md), [placement-policy](./placement-policy.md),
  [statistics](./statistics.md), [cluster](./cluster.md), and
  [storage](./storage.md).
- Persisted config or metadata changes:
  read [config](./config.md), [storage](./storage.md), [server](./server.md),
  and the owning subsystem guide.
- TSO changes:
  read [tso](./tso.md), [server](./server.md), [api-and-client](./api-and-client.md),
  and [keyspace-and-microservices](./keyspace-and-microservices.md).
- Keyspace or microservice changes:
  read [keyspace-and-microservices](./keyspace-and-microservices.md),
  [storage](./storage.md), [tso](./tso.md), and [api-and-client](./api-and-client.md).
- Resource group or RU changes:
  read [resource-manager](./resource-manager.md), [config](./config.md),
  [keyspace-and-microservices](./keyspace-and-microservices.md), and
  [api-and-client](./api-and-client.md).

## Must-Read File Order

Use this faster file order when the goal is review or change-impact analysis:

1. `server/server.go`
2. `server/grpc_service.go`
3. `server/config/config.go`
4. `server/config/persist_options.go`
5. `pkg/member/member.go`
6. `server/cluster/cluster.go`
7. `server/cluster/cluster_worker.go`
8. `pkg/core/region.go`
9. `pkg/core/store.go`
10. `pkg/statistics/hot_cache.go`
11. `pkg/schedule/placement/rule_manager.go`
12. `pkg/schedule/coordinator.go`
13. `pkg/schedule/operator/operator_controller.go`
14. `pkg/storage/storage.go`
15. `pkg/tso/allocator.go`
16. `pkg/keyspace/keyspace.go`
17. `pkg/keyspace/tso_keyspace_group.go`
18. `pkg/mcs/resourcemanager/server/manager.go`
19. `pkg/mcs/discovery/register.go`
20. `client/client.go`

## Reading Map And Companion Docs

Suggested reading order for new maintainers:

1. `README.md`
2. `docs/development.md`
3. `server/server.go`
4. `server/grpc_service.go`
5. `server/config/config.go`
6. `pkg/member/member.go`
7. `server/cluster/cluster.go`
8. `pkg/core/basic_cluster.go`
9. `pkg/core/region.go`
10. `pkg/core/store.go`
11. `pkg/statistics/region_collection.go`
12. `pkg/statistics/store_collection.go`
13. `pkg/schedule/placement/rule_manager.go`
14. `pkg/schedule/coordinator.go`
15. `pkg/schedule/checker/checker_controller.go`
16. `pkg/schedule/schedulers/scheduler_controller.go`
17. `pkg/schedule/operator/operator_controller.go`
18. `pkg/storage/storage.go`
19. `pkg/tso/allocator.go`
20. `pkg/keyspace/keyspace.go`
21. the matching deeper guide in `docs/maintenance-guides/`

Useful companion docs in this repo:

- `README.md`
- `CONTRIBUTING.md`
- `docs/development.md`
- `docs/development-workflow.md`

## Glossary

- PD leader:
  the PD member that owns cluster mutations and serves leader-only behavior.
- etcd leader:
  the embedded etcd raft leader. PD tries to keep it aligned with the PD leader.
- TSO:
  timestamp oracle used by TiDB transactions.
- Keyspace:
  logical metadata namespace used by serverless and multi-tenant paths.
- Keyspace group:
  a grouping of keyspaces assigned to TSO service ownership.
- Region:
  TiKV key-range shard and scheduling unit.
- Store:
  TiKV node or storage service identity reported to PD.
- Operator:
  scheduler-produced action plan delivered to TiKV through heartbeat response.
- Placement rule:
  declarative replica placement contract.
- MCS:
  microservice mode components under `pkg/mcs/`.
