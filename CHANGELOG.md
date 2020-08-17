# PD Change Log

## v3.0.0-beta.1

+ Unify the log format for easy collection and analysis by tools
+ Simulator
    - Support different heartbeat intervals in different stores [#1418](https://github.com/tikv/pd/pull/1418)
    - Add a case about importing data [#1263](https://github.com/tikv/pd/pull/1263)
+ Make hotspot scheduling configurable [#1412](https://github.com/tikv/pd/pull/1412)
+ Add the store address as the dimension monitoring item to replace the previous Store ID [#1429](https://github.com/tikv/pd/pull/1429)
+ Optimize the `GetStores` overhead to speed up the Region inspection cycle [#1410](https://github.com/tikv/pd/pull/1410)
+ Add an interface to delete the Tombstone Store [#1472](https://github.com/tikv/pd/pull/1472)

## v3.0.0-beta
- Add `RegionStorage` to store Region metadata separately [#1237](https://github.com/tikv/pd/pull/1237)
- Add shuffle hot Region scheduler [#1361](https://github.com/tikv/pd/pull/1361)
- Add scheduling parameter related metrics [#1406](https://github.com/tikv/pd/pull/1406)
- Add cluster label related metrics [#1402](https://github.com/tikv/pd/pull/1402)
- Add the importing data simulator [#1263](https://github.com/tikv/pd/pull/1263)
- Fix the `Watch` issue about leader election [#1396](https://github.com/tikv/pd/pull/1396)

## v2.1.2
- Fix the Region information update issue about Region merge [#1377](https://github.com/tikv/pd/pull/1377)

## v2.1.1
- Fix the issue that some configuration items cannot be set to `0` in the configuration file [#1334](https://github.com/tikv/pd/pull/1334)
- Check the undefined configuration when starting PD [#1362](https://github.com/tikv/pd/pull/1362)
- Avoid transferring the leader to a newly created peer, to optimize the possible delay [#1339](https://github.com/tikv/pd/pull/1339)
- Fix the issue that `RaftCluster` cannot stop caused by deadlock [#1370](https://github.com/tikv/pd/pull/1370)

## v2.1.0
+ Optimize availability
    - Introduce the version control mechanism and support rolling update of the cluster compatibly
    - [Enable `Raft PreVote`](https://github.com/tikv/pd/blob/5c7b18cf3af91098f07cf46df0b59fbf8c7c5462/conf/config.toml#L22) among PD nodes to avoid leader reelection when network recovers after network isolation
    - Enable `raft learner` by default to lower the risk of unavailable data caused by machine failure during scheduling
    - TSO allocation is no longer affected by the system clock going backwards
    - Support the `Region merge` feature to reduce the overhead brought by metadata

+ Optimize the scheduler
    - Optimize the processing of Down Store to speed up making up replicas
    - Optimize the hotspot scheduler to improve its adaptability when traffic statistics information jitters
    - Optimize the start of Coordinator to reduce the unnecessary scheduling caused by restarting PD
    - Optimize the issue that Balance Scheduler schedules small Regions frequently
    - Optimize Region merge to consider the number of rows within the Region
    - [Add more commands to control the scheduling policy](https://pingcap.com/docs/tools/pd-control/#config-show--set-option-value)
    - Improve [PD simulator](https://github.com/tikv/pd/tree/release-2.1/tools/pd-simulator) to simulate the scheduling scenarios

+ API and operation tools
    - Add the [`GetPrevRegion` interface](https://github.com/pingcap/kvproto/blob/8e3f33ac49297d7c93b61a955531191084a2f685/proto/pdpb.proto#L40) to support the `TiDB reverse scan` feature
    - Add the [`BatchSplitRegion` interface](https://github.com/pingcap/kvproto/blob/8e3f33ac49297d7c93b61a955531191084a2f685/proto/pdpb.proto#L54) to speed up TiKV Region splitting
    - Add the [`GCSafePoint` interface](https://github.com/pingcap/kvproto/blob/8e3f33ac49297d7c93b61a955531191084a2f685/proto/pdpb.proto#L64-L66) to support distributed GC in TiDB
    - Add the [`GetAllStores` interface](https://github.com/pingcap/kvproto/blob/8e3f33ac49297d7c93b61a955531191084a2f685/proto/pdpb.proto#L32), to support distributed GC in TiDB
    - pd-ctl supports:
        - [using statistics for Region split](https://pingcap.com/docs/tools/pd-control/#operator-show--add--remove)
        - [calling `jq` to format the JSON output](https://pingcap.com/docs/tools/pd-control/#jq-formatted-json-output-usage)
        - [checking the Region information of the specified store](https://pingcap.com/docs/tools/pd-control/#region-store-store-id)
        - [checking topN Region list sorted by versions](https://pingcap.com/docs/tools/pd-control/#region-topconfver-limit)
        - [checking topN Region list sorted by size](https://pingcap.com/docs/tools/pd-control/#region-topsize-limit)
        - [more precise TSO encoding](https://pingcap.com/docs/tools/pd-control/#tso)
    - [pd-recover](https://pingcap.com/docs/tools/pd-recover) doesn't need to provide the `max-replica` parameter

+ Metrics
    - Add related metrics for `Filter`
    - Add metrics about etcd Raft state machine

+ Performance
    - Optimize the performance of Region heartbeat to reduce the memory overhead brought by heartbeats
    - Optimize the Region tree performance
    - Optimize the performance of computing hotspot statistics

## v2.1.0-rc.5
- Fix the issues related to `pd-ctl` reading the Region key [#1298](https://github.com/tikv/pd/pull/1298) [#1299](https://github.com/tikv/pd/pull/1299) [#1308](https://github.com/tikv/pd/pull/1308)
- Fix the issue that the `regions/check` API returns the wrong result [#1311](https://github.com/tikv/pd/pull/1311)
- Fix the issue that PD cannot restart join after a PD join failure [#1279](https://github.com/tikv/pd/pull/1279)
- Fix the issue that `watch leader` might lose events in some cases [#1317](https://github.com/tikv/pd/pull/1317)

## v2.1.0-rc.4
- Fix the issue that the tombstone TiKV is not removed from Grafana [#1261](https://github.com/tikv/pd/pull/1261)
- Fix the data race issue when grpc-go configures the status [#1265](https://github.com/tikv/pd/pull/1265)
- Fix the issue that the PD server gets stuck caused by etcd startup failure [#1267](https://github.com/tikv/pd/pull/1267)
- Fix the issue that data race might occur during leader switching [#1273](https://github.com/tikv/pd/pull/1273)
- Fix the issue that extra warning logs might be output when TiKV becomes tombstone [#1280](https://github.com/tikv/pd/pull/1273)

## v2.1.0-rc.3
### New feature
- Add the API to get the Region list by size in reverse order [#1254](https://github.com/tikv/pd/pull/1254)
### Improvement
- Return more detailed information in the Region API [#1252](https://github.com/tikv/pd/pull/1252)
### Bug fix
- Fix the issue that `adjacent-region-scheduler` might lead to a crash after PD switches the leader [#1250](https://github.com/tikv/pd/pull/1250)

## v2.1.0-rc2
### Features
* Support the `GetAllStores` interface
* Add the statistics of scheduling estimation in Simulator
### Improvements
* Optimize the handling process of down stores to make up replicas as soon as possible
* Optimize the start of Coordinator to reduce the unnecessary scheduling caused by restarting PD
* Optimize the memory usage to reduce the overhead caused by heartbeats
* Optimize error handling and improve the log information
* Support querying the Region information of a specific store in pd-ctl
* Support querying the topN Region information based on version
* Support more accurate TSO decoding in pd-ctl
### Bug fix
* Fix the issue that pd-ctl uses the `hot store` command to exit wrongly

## v2.1.0-rc1
### Features
* Introduce the version control mechanism and support rolling update of the cluster with compatibility
* Enable the `region merge` feature
* Support the `GetPrevRegion` interface
* Support splitting Regions in batch
* Support storing the GC safepoint
### Improvements
* Optimize the issue that TSO allocation is affected by the system clock going backwards
* Optimize the performance of handling Region hearbeats
* Optimize the Region tree performance
* Optimize the performance of computing hotspot statistics
* Optimize returning the error code of API interface
* Add options of controlling scheduling strategies
* Prohibit using special characters in `label`
* Improve the scheduling simulator
* Support splitting Regions using statistics in pd-ctl
* Support formatting JSON output by calling `jq` in pd-ctl
* Add metrics about etcd Raft state machine
### Bug fixes
* Fix the issue that the namespace is not reloaded after switching Leader
* Fix the issue that namespace scheduling exceeds the schedule limit
* Fix the issue that hotspot scheduling exceeds the schedule limit
* Fix the issue that wrong logs are output when the PD client closes
* Fix the wrong statistics of Region heartbeat latency

## v2.1.0-beta
### Improvements
* Enable Raft PreVote between PD nodes to avoid leader reelection when network recovers after network isolation
* Optimize the issue that Balance Scheduler schedules small Regions frequently
* Optimize the hotspot scheduler to improve its adaptability in traffic statistics information jitters
* Skip the Regions with a large number of rows when scheduling `region merge`
* Enable `raft learner` by default to lower the risk of unavailable data caused by machine failure during scheduling
* Remove `max-replica` from `pd-recover`
* Add `Filter` metrics
### Bug Fixes
* Fix the issue that Region information is not updated after tikv-ctl unsafe recovery
* Fix the issue that TiKV disk space is used up caused by replica migration in some scenarios
### Compatibility notes
* Do not support rolling back to v2.0.x or earlier due to update of the new version storage engine
* Enable `raft learner` by default in the new version of PD. If the cluster is upgraded from 1.x to 2.1, the machine should be stopped before upgrade or a rolling update should be first applied to TiKV and then PD

## v2.0.4
### Improvement
* Improve the behavior of the unset scheduling argument `max-pending-peer-count` by changing it to no limit for the maximum number of `PendingPeer`s

## v2.0.3
### Bug Fixes
* Fix the issue about scheduling of the obsolete Regions
* Fix the panic issue when collecting the hot-cache metrics in specific conditions

## v2.0.2
### Improvements
* Make the balance leader scheduler filter the disconnected nodes
* Make the tick interval of patrol Regions configurable
* Modify the timeout of the transfer leader operator to 10s
### Bug Fixes
* Fix the issue that the label scheduler does not schedule when the cluster Regions are in an unhealthy state
* Fix the improper scheduling issue of `evict leader scheduler`

## v2.0.1
### New Feature
* Add the `Scatter Range` scheduler to balance Regions with the specified key range
### Improvements
* Optimize the scheduling of Merge Region to prevent the newly split Region from being merged
* Add Learner related metrics
### Bug Fixes
* Fix the issue that the scheduler is mistakenly deleted after restart
* Fix the error that occurs when parsing the configuration file
* Fix the issue that the etcd leader and the PD leader are not synchronized
* Fix the issue that Learner still appears after it is closed
* Fix the issue that Regions fail to load because the packet size is too large

## v2.0.0-GA
### New Feature
* Support using pd-ctl to scatter specified Regions for manually adjusting hotspot Regions in some cases
### Improvements
* Improve configuration check rules to prevent unreasonable scheduling configuration
* Optimize the scheduling strategy when a TiKV node has insufficient space so as to prevent the disk from being fully occupied
* Optimize hot-region scheduler execution efficiency and add more metrics
* Optimize Region health check logic to avoid generating redundant schedule operators

## v2.0.0-rc.5
### New Feature
* Support adding the learner node
### Improvements
* Optimize the Balance Region Scheduler to reduce scheduling overhead
* Adjust the default value of `schedule-limit` configuration
* Fix the compatibility issue when adding a new scheduler
### Bug Fix
* Fix the issue of allocating IDs frequently

## v2.0.0-rc.4
### New Feature
* Support splitting Region manually to handle the hot spot in a single Region
### Improvement
* Optimize metrics
### Bug Fix
* Fix the issue that the label property is not displayed when `pdctl` runs `config show all`

## v2.0.0-rc3
### New Feature
* Support Region Merge, to merge empty Regions or small Regions after deleting data
### Improvements
* Ignore the nodes that have a lot of pending peers during adding replicas, to improve the speed of restoring replicas or making nodes offline
* Optimize the scheduling speed of leader balance in scenarios of unbalanced resources within different labels
* Add more statistics about abnormal Regions
### Bug Fix
* Fix the frequent scheduling issue caused by a large number of empty Regions
