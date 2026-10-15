# Sustained hot Region Top 10

The PD Grafana dashboard has instant tables under **Statistics - hot read** and
**Statistics - hot write**. They display distinct Regions for `read-peer`,
`write-leader`, and `write-peer` respectively. Each list contains at most ten
Regions, ranked by the effective first priority of that scheduling type. Version
fallback is shared with the scheduling solver; changing priorities takes effect
at the next hotspot metrics collection. The dimension is shown alongside the
value: byte/s, key/s, query/s, or the scheduler's native CPU usage unit.

The source is the existing hot-peer cache snapshot, already filtered by hot
degree, cold status, and anti-count. Loads use the existing denoised values.
Collection does not add hot-degree observations or introduce another history
window. Thus "sustained" follows the existing hot-cache configuration and
heartbeat cadence, rather than a new fixed five-minute rule. A Region can qualify
as hot in any tracked dimension; its displayed rank uses only the first priority.
Regions awaiting scheduling or constrained by placement can still appear.

For peer lists, replicas of a Region are represented by the maximum peer load,
with the corresponding Store ID. Loads are not summed across replicas. Equal
loads prefer the smaller Region ID and then the smaller Store ID. Write-leader
lists include only leader peers. Removed, missing, or unhealthy Stores are
excluded. Cache expiry and the existing periodic collection govern freshness.

The new metric is `pd_hotspot_top_region_load{type,dim,region,store}`. Existing
metrics and APIs are unchanged. Both monolithic PD and the scheduling service
use the coordinator collection path; API-service mode also supports resolving
the scheduler through its configuration handler. Pausing scheduling or reaching
an operator limit does not stop metrics collection. Empty snapshots, scheduler
removal, and metric resets remove obsolete entries.

Selection reuses the snapshot collected for existing hotspot metrics. It scans
only hot peers and keeps a fixed array of ten candidates per list, with a fast
cutoff check and at most ten comparisons/moves per insertion. Additional
selection memory is constant, with no Region-sized map or per-candidate
allocation. Scrapes only read immutable snapshots, containing at most thirty
samples in total; they do not traverse the hot cache. Snapshot replacement
releases old labels rather than retaining them in a GaugeVec.

The exporter has at most thirty active samples, but TSDB historical cardinality
still grows when Regions or representative Stores change. Use the instant tables
for the current list; a range query can contain more than ten historical Regions.
The Store selector filters the cluster-wide list, rather than recomputing a
separate Top 10 for that Store. No continuous-hot-key lineage is inferred across
Region splits or merges.
