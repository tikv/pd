# Cluster

This guide covers cluster metadata ownership in `server/cluster/` and the
in-memory store/region model in `pkg/core/`.

## Purpose And Scope

`RaftCluster` owns:

- cluster bootstrap state
- store and region heartbeat processing
- store and region metadata persistence
- background cluster jobs
- region synchronization to followers
- replication mode and GC integration
- scheduling coordinator wiring
- slow-store and service fallback checks

`pkg/core` owns the in-memory data structures and query interfaces used by
schedulers, APIs, and statistics.

## Core Concepts

- `RaftCluster` is leader-owned cluster runtime state, not a raft
  implementation.
- `BasicCluster` is the in-memory store/region snapshot model consumed by
  schedulers, APIs, and statistics.
- Heartbeat processing separates validation, cache update, statistics, storage
  persistence, and follower sync.
- Region and store objects are snapshot-like. Prefer clone/update helpers over
  mutating shared structures.
- Service-independent mode changes whether classic PD or a split scheduling
  service owns scheduling behavior.

## Architectural Views

### Metadata cache view

`BasicCluster` combines `StoresInfo` and `RegionsInfo`. `StoreInfo` and
`RegionInfo` are mostly immutable snapshots with clone/update helpers. The
cluster updates these snapshots from storage, heartbeats, and follower sync.

### Heartbeat view

Store heartbeats update store stats and state. Region heartbeats update region
metadata, detect stale or overlapping regions, update trees, persist when
needed, trigger statistics collection, and sync follower PD members.

### Background job view

When the PD member becomes leader, `RaftCluster.Start` initializes the cluster
and launches service checks, metrics collection, node state checks, region
syncing, replication mode, min resolved TS, store config sync, store stats,
GC-related jobs, progress GC, and storage size collection.

## Process Lifecycle And Startup Sequencing

Important anchors:

1. `cluster.NewRaftCluster`
2. `RaftCluster.InitCluster`
3. `RaftCluster.Start`
4. `RaftCluster.LoadClusterInfo`
5. `RaftCluster.HandleStoreHeartbeat`
6. `RaftCluster.HandleRegionHeartbeat`
7. `RaftCluster.processRegionHeartbeat`
8. `RaftCluster.Stop`

Maintenance rules:

- `Start` is leader-owned behavior. Do not make follower-only code depend on
  leader-only initialization.
- Background jobs must stop through `RaftCluster.Stop`.
- Region heartbeat processing deliberately separates cache update, async stats,
  async persistence, and follower sync.

## Data Model And Metadata Contracts

Hot contracts:

- `metapb.Cluster`
- `metapb.Store`
- `metapb.Region`
- region epoch, key range, peers, leader, buckets, source freshness, and
  approximate statistics
- store labels, state, address fields, slow-store status, store limits, and min
  resolved TS
- `core.RegionInfo` clone and inheritance rules
- `core.StoreInfo` clone and store stats rules
- region tree and sub-tree reference behavior

Maintenance rule:

- A region heartbeat can be fresher for some fields and stale for others. Always
  inspect `PreCheckPutRegion`, `Inherit`, `regionGuide`, and overlap handling
  before changing region cache behavior.

## Observability And Operational Signals

Open these first:

- `server/cluster/metrics.go`
- `pkg/core/metrics.go`
- `pkg/statistics/metrics.go`
- `pkg/schedule/metrics.go`

Signals to preserve:

- region cache update and KV update counters
- cluster start duration
- heartbeat processing logs and errors
- store status and slow-store metrics
- region statistics and hot-region metrics
- min resolved TS and storage size signals

## Change Management Guidance

- Region metadata changes usually need scheduler and API review.
- Store metadata changes usually need scheduler filter and placement review.
- Persistence changes need `pkg/storage` review.
- Background job changes need startup/shutdown and leader-transfer tests.
- Follower sync changes need tests for leader/follower region storage behavior.

## Must-Read File Order

1. `server/cluster/cluster.go`
2. `server/cluster/cluster_worker.go`
3. `server/cluster/scheduling_controller.go`
4. `pkg/core/basic_cluster.go`
5. `pkg/core/region.go`
6. `pkg/core/region_tree.go`
7. `pkg/core/store.go`
8. `pkg/core/store_stats.go`
9. `pkg/statistics/region.go`
10. `pkg/statistics/store.go`
11. `pkg/storage/region_storage.go`

## Glossary

- `RaftCluster`:
  PD's leader-owned cluster metadata and background-job coordinator.
- `BasicCluster`:
  in-memory store and region collection used by schedulers and APIs.
- Region heartbeat:
  TiKV report that updates region metadata, freshness, statistics, and operator
  responses.
- Store heartbeat:
  TiKV report that updates store stats, state, slow-store signals, and limits.
- Region epoch:
  version metadata used to reject stale region state across splits and merges.
- Follower sync:
  propagation of region changes from the PD leader to follower PD members.
- Service independent:
  state indicating a split microservice owns behavior instead of classic PD.

## Review Checklist

- Does the change preserve region epoch and key-range validation?
- Are overlapping regions removed from cache and storage consistently?
- Does async persistence failure remain non-fatal where intended?
- Does follower sync still receive all required region changes?
- Are store and region clones used instead of mutating shared snapshots?
- Does the scheduler see consistent store/region state after the change?
- Are background jobs started and stopped under the right leader state?
