# PD Change Log

## v2.0.9
### Bug Fixes
* Fix the issue that the PD server gets stuck caused by etcd startup failure [#1267](https://github.com/pingcap/pd/pull/1267)
* Fix the issues related to `pd-ctl` reading the Region key [#1298](https://github.com/pingcap/pd/pull/1298) [#1299](https://github.com/pingcap/pd/pull/1299) [#1308](https://github.com/pingcap/pd/pull/1308)
* Fix the issue that the `regions/check` API returns the wrong result [#1311](https://github.com/pingcap/pd/pull/1311)
* Fix the issue that PD cannot restart join after a PD join failure [#1279](https://github.com/pingcap/pd/pull/1279)

## V2.0.5
### Bug Fixes
* Fix the issue that replicas migration uses up TiKV disks space in some scenarios
* Fix the crash issue caused by `AdjacentRegionScheduler`

## V2.0.4
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
