# Statistics

This guide covers cluster statistics in `pkg/statistics/`.

## Purpose And Scope

Statistics owns:

- region status classification
- store status and store load observation
- hot region and hot peer caches
- bucket-level hot statistics
- CPU and store load dimensions
- metrics derived from store, region, and scheduling config state
- data used by hot-region, balance, slow-store, and checker logic

Statistics consumes `pkg/core` snapshots. It should not mutate the authoritative
cluster cache.

## Architectural Views

### Region statistics view

`RegionStatistics` records region health classes such as missing peers, extra
peers, down peers, pending peers, offline peers, learners, empty regions,
oversized regions, undersized regions, and witness leaders. It uses placement
rules when placement is enabled.

### Store statistics view

Store statistics aggregate node state, storage capacity, region and leader
counts, label distribution, slow-store state, scores, store limits, and rolling
load dimensions from store heartbeat input.

### Hot cache view

`HotCache` maintains read and write `HotPeerCache` instances and processes
updates asynchronously through queues. Schedulers query hot peer stats,
thresholds, and hotness state from these caches.

### Collector view

Collectors translate TiKV and TiFlash store/peer load signals into comparable
dimensions. TiFlash handling differs from TiKV because TiFlash has no leader and
may derive write peer load differently.

## Process Lifecycle And Startup Sequencing

Important anchors:

1. `statistics.NewRegionStatistics`
2. `statistics.RegionStatistics.Observe`
3. `statistics.StoreStatisticsMap`
4. `statistics.NewHotCache`
5. `statistics.HotCache.CheckReadAsync`
6. `statistics.HotCache.CheckWriteAsync`
7. `server/cluster.processRegionHeartbeat`
8. `server/cluster.runMetricsCollectionJob`
9. `server/cluster.runUpdateStoreStats`

Maintenance rules:

- Statistics should be updated from cloned or immutable cluster snapshots.
- Hot cache queue behavior is part of scheduler correctness and latency.
- Region stat refresh must be triggered when special status changes even if
  region metadata does not otherwise need cache persistence.

## Data Model And Metadata Contracts

Hot contracts:

- `RegionStatisticType` bit flags
- region status maps and region ID index
- store load dimensions in `pkg/statistics/utils`
- read/write hot peer thresholds and hot degree
- rolling store stats and instant store stats
- TiKV versus TiFlash collector behavior
- bucket statistics under `pkg/statistics/buckets`
- metrics label names for cluster status and hot cache status

Maintenance rule:

- Scheduler decisions often depend on statistical meaning, not only raw values.
  Changing a dimension, threshold, interval, or collector can change placement
  and balance behavior.

## Observability And Operational Signals

Open these first:

- `pkg/statistics/metrics.go`
- `pkg/statistics/store.go`
- `pkg/statistics/hot_cache.go`
- `pkg/statistics/hot_peer_cache.go`
- `pkg/statistics/buckets/metric.go`
- `pkg/schedule/schedulers/hot_region.go`

Signals to preserve:

- region status gauges
- store status gauges
- store load rates and instant rates
- hot cache status gauges
- hot peer metrics
- bucket hot statistics
- scheduler config metrics emitted with store statistics

## Change Management Guidance

- New statistics dimensions need scheduler review before being used for
  decisions.
- Hot cache changes need tests for read and write paths, queue saturation,
  expiration, and threshold calculation.
- Region status changes need API and checker review.
- Store status changes need filter and slow-store scheduler review.
- Metric changes should avoid high-cardinality labels.

## Must-Read File Order

1. `pkg/statistics/region_collection.go`
2. `pkg/statistics/store_collection.go`
3. `pkg/statistics/hot_cache.go`
4. `pkg/statistics/hot_peer_cache.go`
5. `pkg/statistics/hot_peer.go`
6. `pkg/statistics/store_load.go`
7. `pkg/statistics/collector.go`
8. `pkg/statistics/buckets/hot_bucket_cache.go`
9. `pkg/statistics/utils/kind.go`
10. `server/cluster/cluster.go`
11. `pkg/schedule/schedulers/hot_region.go`

## Review Checklist

- Does the change alter scheduler-visible meaning of load or hotness?
- Are read and write paths handled symmetrically where required?
- Are TiKV and TiFlash semantics both considered?
- Does async hot cache work avoid blocking heartbeat hot paths?
- Are stale or missing region/store snapshots handled safely?
- Are metric labels bounded and consistent?
- Do tests cover threshold, expiration, and special status transitions?
