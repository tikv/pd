# Splitting affinity Regions before peer moves

Affinity merges can produce Regions much larger than an ordinary TiKV Region.
When an affinity target changes, moving such a Region as one snapshot makes the
move slow and consumes store resources for a long time. The affinity checker
splits these Regions before adding voters, then moves the children through its
existing move operator.

## Execution

The affinity checker prepares each Region when executing its group's target.
This covers administrative target changes and convergence to a stored or
automatically learned target. Table affinity, partition affinity and custom
groups use the same preparation.

Replica recovery still runs ahead of the affinity checker. Preparation applies
to convergence toward an affinity target; it does not delay repair of missing
or misplaced replicas by the rule checker.

| Observed Region state | Checker action |
| --- | --- |
| Only the leader needs to change | Create the existing move operator without splitting. |
| A target voter store has no peer, and reported size is unknown or nonpositive | Wait for usable heartbeat statistics. |
| A target voter store has no peer, and size or keys exceed ordinary split thresholds | Create an `affinity-split-region` operator with policy `SCAN`. |
| Size and keys are within thresholds, but group ranges lack complete heartbeat coverage | Wait for the missing Regions. |
| Size and keys are within thresholds, and the group has complete heartbeat coverage | Create the existing affinity move operator. |
| Some Regions have reached the target, while others have not | Keep the children separate. |
| Every Region is healthy, covers the group ranges and matches the current target | Resume the existing merge checks. |

The thresholds are the observed TiKV `region-split-size` and `region-split-keys`,
obtained through the existing store config provider. A value equal to a threshold
can move. Either size or keys exceeding its threshold requests a split. No new
configuration or persistent metadata is introduced.

`SCAN` asks TiKV to choose a split point from its data and normally half-splits a
Region. PD does not estimate a key from byte sizes. Children still above a
threshold are split again on later patrols. For example, a reported 1 GiB Region
with a 256 MiB split size can be half-split twice into four reported 256 MiB
Regions before moving them. This does not reduce the total bytes to migrate.

The existing placement checks, learner conflict check and target leader checks
run before preparation. The split operator has both `OpAffinity` and `OpSplit`,
so it consumes the affinity schedule limit. The operator controller prevents
another operator from starting on the same Region while the split is running.
Each child goes through the existing placement and operator checks before its
move. Existing learners are retained by the move builder.

## Coverage and merging

Finishing a split operator means that its original Region's boundary changed.
It does not mean that every child has reported its heartbeat. Before adding a
voter, the checker reads all group ranges from one RegionTree snapshot and
requires complete coverage, a leader and a positive reported size for each
Region. Unbounded range endpoints are checked explicitly. Separate ranges of a
group do not require coverage of the gaps outside the group.

Ranges are copied under the affinity manager's metadata lock. The manager rejects
a state whose target, ranges or group instance changed before that copy.

Both affinity merging and ordinary automatic merging require all Regions of an
active group to be healthy and match its target. Checking only the two merge
candidates could merge children that have already moved while their siblings
are still moving. The ordinary merge gate also covers the random merge
scheduler, which shares `AllowMerge`. With `affinity-schedule-limit=0`, ordinary
merging retains its existing behavior. Manual administrative merge operators
continue to follow the administrative path.

The coverage check reads a snapshot of the Regions of the group, then readiness
validation stops on the first unready Region. Each check costs O(R) time and
memory for a group with R Regions; there is no additional progress cache to
synchronize or persist. A round that checks every child can therefore cost
O(R²) in total.

## Recovery and retries

The stored group target and ranges describe the desired state. The RegionTree
describes the observed state. A restarted checker can therefore repeat the same
decision without remembering the original parent ID, split keys or a migration
phase. Regions loaded only from storage have no heartbeat leader or usable size
and cannot authorize a move. MCS uses the same checker and manager snapshot;
splitting adds no target write or cross-service protocol.

If TiKV cannot find a legal split key, the split operator follows its existing
timeout and retry behavior. An unchanged, oversized Region requests another
split on a later patrol; it is never deliberately moved as a fallback. A target
change or deletion affects the next check. Already admitted operators keep their
existing execution semantics; this feature does not add immediate cancellation
of operators on metadata changes.

TiKV can report a positive but stale size after a merge. Before allowing a peer
move below the thresholds, the checker reuses its one-minute recent-merge
cooldown. It also waits one minute after checker startup, when that cache has
been lost. A reported oversized Region may be split during this interval.

Size and key counts remain approximate heartbeat statistics, expressed by PD in
integer MiB for size. The cooldown mitigates known merge staleness; it does not
prove that statistics were refreshed. Split children can inherit estimates, and
an estimate can lag subsequent writes. This feature bounds the estimates used
to admit peer moves, not the exact bytes in a future snapshot. A strict snapshot
size guarantee would require additional TiKV statistics or protocol support.
