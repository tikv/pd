# PD Change Log

## Unreleased

+ Fix the issue about the limit of the hot region [#1552](https://github.com/pingcap/pd/pull/1552)
+ Add a option about grpc gateway [#1596](https://github.com/pingcap/pd/pull/1596)
+ Add the missing schedule config items [#1601](https://github.com/pingcap/pd/pull/1601)

## v3.0.0

+ Support re-creating a cluster from a single node 
+ Migrate Region metadata from etcd to the go-leveldb storage engine to solve the storage bottleneck in etcd for large-scale clusters 
+ API 
  - Add the `remove-tombstone` API to clear Tombstone stores
  - Add the `ScanRegions` API to batch query Region information
  - Add the `GetOperator` API to query running operators
  - Optimize the performance of the `GetStores` API
+ Configurations
  - Optimize configuration check logic to avoid configuration item errors
  - Add `enable-one-way-merge` to control the direction of Region merge 
  - Add `hot-region-schedule-limit` to control the scheduling rate for hot Regions 
  - Add `hot-region-cache-hits-threshold` to identify hotspot when hitting multiple thresholds consecutively 
  - Add the `store-balance-rate` configuration item to control the maximum numbers of balance Region operators allowed per minute
+ Scheduler Optimizations
  - Add the store limit mechanism for separately controlling the speed of operators for each store
  - Support the `waitingOperator` queue to optimize the resource race among different schedulers
  - Support scheduling rate limit to actively send scheduling operations to TiKV. This improves the scheduling rate by limiting the number of concurrent scheduling tasks on a single node.
  - Optimize the `Region Scatter` scheduling to be not restrained by the limit mechanism
  - Add the `shuffle-hot-region` scheduler to facilitate TiKV stability test in scenarios of poor hotspot scheduling
+ Simulator
  - Add simulator for data import scenarios
  - Support setting different heartbeats intervals for the Store 
+ Others
  - Upgrade etcd to solve the issues of inconsistent log output formats, Leader selection failure in prevote, and lease deadlocking. 
  - Develop a unified log format specification with restructured log system to facilitate collection and analysis by tools
  - Add monitoring metrics including scheduling parameters, cluster label information, time consumed by PD to process TSO requests, Store ID and address information, etc.

## v3.0.0-rc.3

+ Add light peer without considering the influence [1563](https://github.com/pingcap/pd/pull/1563)
+ Add initialized flag in cluster status [1581](https://github.com/pingcap/pd/pull/1581)
+ Add option to only merge from left into right [1583](https://github.com/pingcap/pd/pull/1583)
+ Improve config check [1585](https://github.com/pingcap/pd/pull/1585)
+ Fix store maybe always overloaded [1590](https://github.com/pingcap/pd/pull/1590)
+ Adjust store balance rate meaning [1591](https://github.com/pingcap/pd/pull/1591)

## v3.0.0-rc.2

+ Enable the Region storage by default to store the Region metadata [1524](https://github.com/pingcap/pd/pull/1524)
+ Fix the issue that hot Region scheduling is preempted by another scheduler [1522](https://github.com/pingcap/pd/pull/1522)
+ Fix the issue that the priority for the leader does not take effect [1533](https://github.com/pingcap/pd/pull/1533)
+ Add the gRPC interface for ScanRegions [1535](https://github.com/pingcap/pd/pull/1535)
+ Push operators actively [1536](https://github.com/pingcap/pd/pull/1536)
+ Add the store limit mechanism for separately controlling the speed of operators for each store [1474](https://github.com/pingcap/pd/pull/1474)
+ Fix the issue of inconsistent Config status [1476](https://github.com/pingcap/pd/pull/1476)

## v3.0.0-rc.1

+ Upgrade ETCD [1452](https://github.com/pingcap/pd/pull/1452)
    - Unify the log format of etcd and PD server
    - Fix the issue of failing to elect Leader by PreVote
    - Support fast dropping the “propose” and “read” requests that are to fail to avoid blocking the subsequent requests
    - Fix the deadlock issue of Lease
+ Fix the issue that a hot store makes incorrect statistics of keys [1487](https://github.com/pingcap/pd/pull/1487)
+ Support forcibly rebuilding a PD cluster from a single PD node [1485](https://github.com/pingcap/pd/pull/1485)
+ Fix the issue that regionScatterer might generate an invalid OperatorStep [1482](https://github.com/pingcap/pd/pull/1482)
+ Fix the too short timeout issue of the MergeRegion operator [1495](https://github.com/pingcap/pd/pull/1495)
+ Support giving high priority to hot region scheduling [1492](https://github.com/pingcap/pd/pull/1492)
+ Add the metrics for recording the time of handling TSO requests on the PD server side [1502](https://github.com/pingcap/pd/pull/1502)
+ Add the corresponding Store ID and Address to the metrics related to the store [1506](https://github.com/pingcap/pd/pull/1506)
+ Support the GetOperator service [1477](https://github.com/pingcap/pd/pull/1477)
+ Fix the issue that the error cannot be sent in the Heartbeat stream because the store cannot be found [1521](https://github.com/pingcap/pd/pull/1521)

## v3.0.0-beta.1

+ Unify the log format for easy collection and analysis by tools
+ Simulator
    - Support different heartbeat intervals in different stores [#1418](https://github.com/pingcap/pd/pull/1418)
    - Add a case about importing data [#1263](https://github.com/pingcap/pd/pull/1263)
+ Make hotspot scheduling configurable [#1412](https://github.com/pingcap/pd/pull/1412)
+ Add the store address as the dimension monitoring item to replace the previous Store ID [#1429](https://github.com/pingcap/pd/pull/1429)
+ Optimize the `GetStores` overhead to speed up the Region inspection cycle [#1410](https://github.com/pingcap/pd/pull/1410)
+ Add an interface to delete the Tombstone Store [#1472](https://github.com/pingcap/pd/pull/1472)

## v3.0.0-beta
- Add `RegionStorage` to store Region metadata separately [#1237](https://github.com/pingcap/pd/pull/1237)
- Add shuffle hot Region scheduler [#1361](https://github.com/pingcap/pd/pull/1361)
- Add scheduling parameter related metrics [#1406](https://github.com/pingcap/pd/pull/1406)
- Add cluster label related metrics [#1402](https://github.com/pingcap/pd/pull/1402)
- Add the importing data simulator [#1263](https://github.com/pingcap/pd/pull/1263)
- Fix the `Watch` issue about leader election [#1396](https://github.com/pingcap/pd/pull/1396)

## v2.1.2
- Fix the Region information update issue about Region merge [#1377](https://github.com/pingcap/pd/pull/1377)

## v2.1.1
- Fix the issue that some configuration items cannot be set to `0` in the configuration file [#1334](https://github.com/pingcap/pd/pull/1334)
- Check the undefined configuration when starting PD [#1362](https://github.com/pingcap/pd/pull/1362)
- Avoid transferring the leader to a newly created peer, to optimize the possible delay [#1339](https://github.com/pingcap/pd/pull/1339)
- Fix the issue that `RaftCluster` cannot stop caused by deadlock [#1370](https://github.com/pingcap/pd/pull/1370)

## v2.1.0
+ Optimize availability
    - Introduce the version control mechanism and support rolling update of the cluster compatibly
    - [Enable `Raft PreVote`](https://github.com/pingcap/pd/blob/5c7b18cf3af91098f07cf46df0b59fbf8c7c5462/conf/config.toml#L22) among PD nodes to avoid leader reelection when network recovers after network isolation
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
    - Improve [PD simulator](https://github.com/pingcap/pd/tree/release-2.1/tools/pd-simulator) to simulate the scheduling scenarios

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
- Fix the issues related to `pd-ctl` reading the Region key [#1298](https://github.com/pingcap/pd/pull/1298) [#1299](https://github.com/pingcap/pd/pull/1299) [#1308](https://github.com/pingcap/pd/pull/1308)
- Fix the issue that the `regions/check` API returns the wrong result [#1311](https://github.com/pingcap/pd/pull/1311)
- Fix the issue that PD cannot restart join after a PD join failure [#1279](https://github.com/pingcap/pd/pull/1279)
- Fix the issue that `watch leader` might lose events in some cases [#1317](https://github.com/pingcap/pd/pull/1317)

## v2.1.0-rc.4
- Fix the issue that the tombstone TiKV is not removed from Grafana [#1261](https://github.com/pingcap/pd/pull/1261)
- Fix the data race issue when grpc-go configures the status [#1265](https://github.com/pingcap/pd/pull/1265)
- Fix the issue that the PD server gets stuck caused by etcd startup failure [#1267](https://github.com/pingcap/pd/pull/1267)
- Fix the issue that data race might occur during leader switching [#1273](https://github.com/pingcap/pd/pull/1273)
- Fix the issue that extra warning logs might be output when TiKV becomes tombstone [#1280](https://github.com/pingcap/pd/pull/1273)

## v2.1.0-rc.3
### New feature
- Add the API to get the Region list by size in reverse order [#1254](https://github.com/pingcap/pd/pull/1254)
### Improvement
- Return more detailed information in the Region API [#1252](https://github.com/pingcap/pd/pull/1252)
### Bug fix
- Fix the issue that `adjacent-region-scheduler` might lead to a crash after PD switches the leader [#1250](https://github.com/pingcap/pd/pull/1250)

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
