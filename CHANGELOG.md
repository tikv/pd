# PD Change Log
## v2.0.0-rc.5
### New Features
* Support adding the learner node
### Improves
* Optimize the Balance Region Scheduler to reduce scheduling overhead
* Adjust the default value of `schedule-limit` configuration
* Fix the compatibility issue when adding a new scheduler
### Bug Fixes
* Fix the issue of allocating IDs frequently

## v2.0.0-rc.4
### New Features
* Support splitting Region manually to handle the hot spot in a single Region
### Improves
* Optimize metrics
### Bug Fixes
* Fix the issue that the label property is not displayed when `pdctl` runs `config show all`

## v2.0.0-rc3
### New Features
* Support Region Merge, to merge empty Regions or small Regions after deleting data
### Improves
* Ignore the nodes that have a lot of pending peers during adding replicas, to improve the speed of restoring replicas or making nodes offline
* Optimize the scheduling speed of leader balance in scenarios of unbalanced resources within different labels
* Add more statistics about abnormal Regions
### Bug Fixes
* Fix the frequent scheduling issue caused by a large number of empty Regions