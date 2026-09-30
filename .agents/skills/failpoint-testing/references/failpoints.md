# PD Failpoint Reference

Complete reference for all PD failpoints found in the codebase. Use this guide to understand what failpoints are available for testing PD internal state transitions.

## Table of Contents

- [Leader Election Failpoints](#leader-election-failpoints)
- [Server Core Failpoints](#server-core-failpoints)
- [Cluster & Member Management Failpoints](#cluster--member-management-failpoints)
- [Replication Failpoints](#replication-failpoints)
- [TSO & Timestamp Failpoints](#tso--timestamp-failpoints)
- [Region & Scheduling Failpoints](#region--scheduling-failpoints)
- [Keyspace Failpoints](#keyspace-failpoints)
- [Storage & Etcd Failpoints](#storage--etcd-failpoints)
- [Performance & Rate Limiting Failpoints](#performance--rate-limiting-failpoints)
- [Debug & Testing Failpoints](#debug--testing-failpoints)

## Leader Election Failpoints

| Failpoint | Full Path | Injection Point | Purpose |
|---|---|---|---|
| `skipGrantLeader` | `github.com/tikv/pd/pkg/election/skipGrantLeader` | `pkg/election/leadership.go:186` | Block Campaign() by comparing injected name against member.Name (not member_id) |
| `exitCampaignLeader` | `github.com/tikv/pd/server/exitCampaignLeader` | `server/server.go:1820` | Force named member (by numeric member_id) to exit campaign loop |
| `leaderLoopCheckAgain` | `github.com/tikv/pd/server/leaderLoopCheckAgain` | `server/server.go:1668` | Force leaderLoop to re-check without re-campaigning (by numeric member_id) |
| `timeoutWaitPDLeader` | `github.com/tikv/pd/server/timeoutWaitPDLeader` | `server/server.go` | Speed up no-leader timeout |
| `skipCampaignLeaderCheck` | `github.com/tikv/pd/server/skipCampaignLeaderCheck` | `server/server.go` | Skip campaign leader check |

## Server Core Failpoints

| Failpoint | Full Path | Injection Point | Purpose |
|---|---|---|---|
| `raftclusterIsBusy` | `github.com/tikv/pd/server/raftclusterIsBusy` | `server/server.go` | Freeze stopRaftCluster() |
| `changeAvailabilityCheckInterval` | `github.com/tikv/pd/server/changeAvailabilityCheckInterval` | `server/cluster.go` | Change availability check interval |
| `changeCoordinatorTicker` | `github.com/tikv/pd/server/changeCoordinatorTicker` | `server/cluster.go` | Change coordinator ticker interval |
| `delayStartServerLoop` | `github.com/tikv/pd/server/delayStartServerLoop` | `server/server.go` | Delay starting server loop |
| `fastTick` | `github.com/tikv/pd/server/fastTick` | `server/server.go` | Speed up tick interval |
| `speedUpMemberLoop` | `github.com/tikv/pd/server/speedUpMemberLoop` | `server/member/` | Speed up member loop |
| `fastUpdateMember` | `github.com/tikv/pd/server/fastUpdateMember` | `server/member/` | Speed up member update |
| `fastUpdatePhysicalInterval` | `github.com/tikv/pd/server/fastUpdatePhysicalInterval` | `server/` | Speed up physical interval update |
| `pauseFinishSplitBeforeTxn` | `github.com/tikv/pd/server/pauseFinishSplitBeforeTxn` | `server/txn.go` | Pause before finishing split during transaction |
| `etcdIsLearner` | `github.com/tikv/pd/server/etcdIsLearner` | `server/` | Make PD act as etcd learner |
| `memberNil` | `github.com/tikv/pd/server/memberNil` | `server/member/` | Simulate nil member |
| `doNotBuryStore` | `github.com/tikv/pd/server/doNotBuryStore` | `server/cluster.go` | Prevent burying dead stores |
| `enableDegradedModeAndTraceLog` | `github.com/tikv/pd/server/enableDegradedModeAndTraceLog` | `server/` | Enable degraded mode with trace logging |

## Cluster & Member Management Failpoints

| Failpoint | Full Path | Injection Point | Purpose |
|---|---|---|---|
| `addMemberFailed` | `github.com/tikv/pd/server/addMemberFailed` | `server/cluster.go` | Simulate add member failure |
| `removeMemberFailed` | `github.com/tikv/pd/server/removeMemberFailed` | `server/cluster.go` | Simulate remove member failure |
| `acquireFailed` | `github.com/tikv/pd/server/acquireFailed` | `server/` | Simulate resource acquisition failure |
| `delayBeforeCheckingElectionMember` | `github.com/tikv/pd/server/delayBeforeCheckingElectionMember` | `server/` | Delay before checking election member |
| `decEpoch` | `github.com/tikv/pd/server/decEpoch` | `server/` | Decrement epoch for testing |
| `externalAllocNode` | `github.com/tikv/pd/server/externalAllocNode` | `server/` | External node allocation |
| `allocIDNonBatch` | `github.com/tikv/pd/server/allocIDNonBatch` | `server/` | Allocate ID in non-batch mode |
| `handleAllocIDNonBatch` | `github.com/tikv/pd/server/handleAllocIDNonBatch` | `server/` | Handle non-batch ID allocation |
| `acceleratedAllocNodes` | `github.com/tikv/pd/server/acceleratedAllocNodes` | `server/` | Accelerate node allocation |

## Replication Failpoints

| Failpoint | Full Path | Injection Point | Purpose |
|---|---|---|---|
| `getReplicationConfigFailed` | `github.com/tikv/pd/server/getReplicationConfigFailed` | `server/replication/` | Simulate getting replication config failure |
| `assignToSpecificKeyspaceGroup` | `github.com/tikv/pd/server/assignToSpecificKeyspaceGroup` | `server/replication/` | Assign to specific keyspace group |
| `splitResponses` | `github.com/tikv/pd/server/splitResponses` | `server/replication/` | Split responses |
| `raftClusterReturn` | `github.com/tikv/pd/server/raftClusterReturn` | `server/replication/` | Make raft cluster return early |

## TSO & Timestamp Failpoints

| Failpoint | Full Path | Injection Point | Purpose |
|---|---|---|---|
| `tsoProxyFailToSendToClient` | `github.com/tikv/pd/server/tsoProxyFailToSendToClient` | `server/tso/` | Simulate TSO proxy send to client failure |
| `failedToSaveTimestamp` | `github.com/tikv/pd/server/failedToSaveTimestamp` | `server/tso/` | Simulate timestamp save failure |
| `slowTxn` | `github.com/tikv/pd/server/slowTxn` | `server/tso/` | Slow down transaction |
| `transientRecoveryGap` | `github.com/tikv/pd/server/transientRecoveryGap` | `server/tso/` | Create transient recovery gap |

## Region & Scheduling Failpoints

| Failpoint | Full Path | Injection Point | Purpose |
|---|---|---|---|
| `scatterFail` | `github.com/tikv/pd/server/scatterFail` | `server/schedule/` | Simulate scatter failure |
| `scatterHbStreamsDrain` | `github.com/tikv/pd/server/scatterHbStreamsDrain` | `server/schedule/` | Drain scatter heartbeat streams |
| `skipSplitRegion` | `github.com/tikv/pd/server/skipSplitRegion` | `server/schedule/` | Skip region split |
| `skipPatrolRegions` | `github.com/tikv/pd/server/skipPatrolRegions` | `server/schedule/` | Skip region patrol |
| `skipCheckSuspectRanges` | `github.com/tikv/pd/server/skipCheckSuspectRanges` | `server/schedule/` | Skip checking suspect ranges |
| `highFrequencyClusterJobs` | `github.com/tikv/pd/server/highFrequencyClusterJobs` | `server/schedule/` | Run cluster jobs at high frequency |
| `regionCount` | `github.com/tikv/pd/server/regionCount` | `server/schedule/` | Control region count for testing |
| `customTimeout` | `github.com/tikv/pd/server/customTimeout` | `server/schedule/` | Set custom timeout for operations |
| `fastGroupSplitPatroller` | `github.com/tikv/pd/server/fastGroupSplitPatroller` | `server/schedule/` | Speed up group split patroller |
| `fastDeletedGroupCleaner` | `github.com/tikv/pd/server/fastDeletedGroupCleaner` | `server/schedule/` | Speed up deleted group cleaner |
| `fastCleanupTicker` | `github.com/tikv/pd/server/fastCleanupTicker` | `server/schedule/` | Speed up cleanup ticker |

## Keyspace Failpoints

| Failpoint | Full Path | Injection Point | Purpose |
|---|---|---|---|
| `skipKeyspaceRegionCheck` | `github.com/tikv/pd/server/skipKeyspaceRegionCheck` | `server/keyspace/` | Skip keyspace region check |
| `SkipKeyspaceWatch` | `github.com/tikv/pd/server/SkipKeyspaceWatch` | `server/keyspace/` | Skip keyspace watch |
| `keyspaceIteratorLoadingBatchSize` | `github.com/tikv/pd/server/keyspaceIteratorLoadingBatchSize` | `server/keyspace/` | Control keyspace iterator loading batch size |
| `onGetAllKeyspacesGCStatesFinish` | `github.com/tikv/pd/server/onGetAllKeyspacesGCStatesFinish` | `server/keyspace/` | Callback when getting all keyspace GC states finishes |
| `gcExpiredTime` | `github.com/tikv/pd/server/gcExpiredTime` | `server/keyspace/` | Control GC expiration time |

## Storage & Etcd Failpoints

| Failpoint | Full Path | Injection Point | Purpose |
|---|---|---|---|
| `etcdDataCorruptionAlarm` | `github.com/tikv/pd/server/etcdDataCorruptionAlarm` | `server/etcdutil/` | Trigger etcd data corruption alarm |
| `etcdLinearizableReadError` | `github.com/tikv/pd/server/etcdLinearizableReadError` | `server/etcdutil/` | Simulate etcd linearizable read error |
| `etcdSerializableReadError` | `github.com/tikv/pd/server/etcdSerializableReadError` | `server/etcdutil/` | Simulate etcd serializable read error |
| `etcdSaveFailed` | `github.com/tikv/pd/server/etcdSaveFailed` | `server/etcdutil/` | Simulate etcd save failure |
| `SlowEtcdKVGet` | `github.com/tikv/pd/server/SlowEtcdKVGet` | `server/etcdutil/` | Slow down etcd KV get |
| `SlowEtcdMemberList` | `github.com/tikv/pd/server/SlowEtcdMemberList` | `server/etcdutil/` | Slow down etcd member list |
| `levelDBStorageFastFlush` | `github.com/tikv/pd/server/levelDBStorageFastFlush` | `server/storage/` | Speed up LevelDB storage flush |
| `persistFail` | `github.com/tikv/pd/server/persistFail` | `server/storage/` | Simulate persistence failure |
| `fastPersist` | `github.com/tikv/pd/server/fastPersist` | `server/storage/` | Speed up persistence |
| `syncMetError` | `github.com/tikv/pd/server/syncMetError` | `server/storage/` | Simulate sync metadata error |
| `meetEtcdError` | `github.com/tikv/pd/server/meetEtcdError` | `server/etcdutil/` | Simulate etcd error |
| `rebaseErr` | `github.com/tikv/pd/server/rebaseErr` | `server/storage/` | Simulate rebase error |

## Performance & Rate Limiting Failpoints

| Failpoint | Full Path | Injection Point | Purpose |
|---|---|---|---|
| `rateLimit` | `github.com/tikv/pd/server/rateLimit` | `server/` | Trigger rate limiting |
| `slowRequest` | `github.com/tikv/pd/server/slowRequest` | `server/` | Slow down requests |
| `systemTimeSlow` | `github.com/tikv/pd/server/systemTimeSlow` | `server/` | Slow down system time |
| `delayProcess` | `github.com/tikv/pd/server/delayProcess` | `server/` | Delay processing |
| `delaySyncTimestamp` | `github.com/tikv/pd/server/delaySyncTimestamp` | `server/` | Delay timestamp synchronization |
| `concurrentBucketHeartbeat` | `github.com/tikv/pd/server/concurrentBucketHeartbeat` | `server/` | Concurrent bucket heartbeat |
| `concurrentRegionHeartbeat` | `github.com/tikv/pd/server/concurrentRegionHeartbeat` | `server/` | Concurrent region heartbeat |
| `concurrentRemoveOperator` | `github.com/tikv/pd/server/concurrentRemoveOperator` | `server/` | Concurrent operator removal |
| `syncRegionChannelFull` | `github.com/tikv/pd/server/syncRegionChannelFull` | `server/` | Simulate full sync region channel |
| `loadRegionSlow` | `github.com/tikv/pd/server/loadRegionSlow` | `server/` | Slow down region loading |
| `loadTemporaryFail` | `github.com/tikv/pd/server/loadTemporaryFail` | `server/` | Temporary load failure |
| `queryRegionMetError` | `github.com/tikv/pd/server/queryRegionMetError` | `server/` | Query region met with error |
| `slowLoadRegion` | `github.com/tikv/pd/server/slowLoadRegion` | `server/` | Slow load region |

## Debug & Testing Failpoints

| Failpoint | Full Path | Injection Point | Purpose |
|---|---|---|---|
| `assertShouldCache` | `github.com/tikv/pd/server/assertShouldCache` | Various | Assert that value should be cached |
| `assertShouldNotCache` | `github.com/tikv/pd/server/assertShouldNotCache` | Various | Assert that value should not be cached |
| `mockPending` | `github.com/tikv/pd/server/mockPending` | Various | Mock pending state |
| `mockRaftKV2` | `github.com/tikv/pd/server/mockRaftKV2` | Tests | Mock Raft KV v2 |
| `mockWriteError` | `github.com/tikv/pd/server/mockWriteError` | Tests | Mock write error |
| `mockFetchStoreConfigFromTiKV` | `github.com/tikv/pd/server/mockFetchStoreConfigFromTiKV` | Tests | Mock fetching store config from TiKV |
| `mockNextGenBuildFlag` | `github.com/tikv/pd/server/mockNextGenBuildFlag` | Tests | Mock next generation build flag |
| `breakPatrol` | `github.com/tikv/pd/server/breakPatrol` | Tests | Break patrol loop |
| `regionLabelExpireSub1Minute` | `github.com/tikv/pd/server/regionLabelExpireSub1Minute` | Tests | Make region labels expire in < 1 minute |
| `disableClientStreaming` | `github.com/tikv/pd/server/disableClientStreaming` | Tests | Disable client streaming |
| `canNotCreateForwardStream` | `github.com/tikv/pd/server/canNotCreateForwardStream` | Tests | Cannot create forward stream |
| `grpcClientClosed` | `github.com/tikv/pd/server/grpcClientClosed` | Tests | gRPC client closed |
| `enableAddServerNameHeader` | `github.com/tikv/pd/server/enableAddServerNameHeader` | Tests | Enable adding server name header |
| `addRequestInfoMiddleware` | `github.com/tikv/pd/server/addRequestInfoMiddleware` | Tests | Add request info middleware |
| `persistServiceMiddlewareFail` | `github.com/tikv/pd/server/persistServiceMiddlewareFail` | Tests | Persist service middleware fail |
| `ReadMemStats` | `github.com/tikv/pd/server/ReadMemStats` | Tests | Read memory stats |
| `GetMemTotalError` | `github.com/tikv/pd/server/GetMemTotalError` | Tests | Get total memory error |
| `fallBackSync` | `github.com/tikv/pd/server/fallBackSync` | Tests | Fall back to sync |
| `fallBackUpdate` | `github.com/tikv/pd/server/fallBackUpdate` | Tests | Fall back to update |
| `noFastExitSync` | `github.com/tikv/pd/server/noFastExitSync` | Tests | No fast exit sync |
| `delayWatcher` | `github.com/tikv/pd/server/delayWatcher` | Tests | Delay watcher |
| `skipDashboardLoop` | `github.com/tikv/pd/server/skipDashboardLoop` | Tests | Skip dashboard loop |
| `skipStoreConfigSync` | `github.com/tikv/pd/server/skipStoreConfigSync` | Tests | Skip store config sync |

## HTTP API Details

### Enable Failpoint
```bash
curl -X PUT "http://127.0.0.1:2379/pd/api/v1/fail/<FULL_PATH>" -d '<ACTION>'
```

### Disable Failpoint
```bash
curl -X PUT "http://127.0.0.1:2379/pd/api/v1/fail/<FULL_PATH>" -d ''
```

## Action String Reference

| Action | Payload | Behavior | Use Case |
|---|---|---|---|
| Pause at failpoint | `pause` | Freezes goroutine at failpoint; unblocks on Disable | Temporary blocking you'll undo |
| Inject empty string | `return("")` | Injects empty string; effect depends on failpoint implementation | Often blocks all for skipGrantLeader (len(name)==0) |
| Inject name string | `return("pd-0")` | Injects specific string; effect depends on failpoint implementation | For skipGrantLeader: blocks node matching member.Name |
| Inject numeric ID | `return("12345")` | Injects numeric member_id string; effect depends on failpoint | For exitCampaignLeader/leaderLoopCheckAgain: targets member by ID |
| Disable | `(empty)` | Removes failpoint (clears any pause/return behavior) | Always cleanup after testing |

> **Note**: `return(...)` never blocks by itself. It only supplies a value to the failpoint; whether that causes blocking, exiting a loop, or no-op depends on the particular failpoint. Use `pause` when you need to block execution at the failpoint.

## Finding Failpoints in Code

```bash
# Search for all failpoints in PD
grep -r "failpoint.Inject" pkg/ server/

# Find specific failpoint usage
grep -r "Inject(\"skipGrantLeader\"" .

# Count total failpoints
grep -rh 'failpoint.Inject' --include="*.go" pkg/ server/ | sed -n 's/.*Inject("\([^"]*\)".*/\1/p' | sort -u | wc -l
```

## Key Failpoint Injection Points

### skipGrantLeader (pkg/election/leadership.go:186)
```go
// NOTE: This is simplified pseudo-code. See pkg/election/leadership.go for actual implementation.
// The real code unmarshals leaderData before comparing member.Name.
failpoint.Inject("skipGrantLeader", func(val failpoint.Value) {
    name, ok := val.(string)
    // When len(name) == 0 (empty string or unset): returns error (blocks ALL nodes)
    if len(name) == 0 {
        failpoint.Return(errors.Errorf("failed to grant lease"))
    }
    // When name matches member.Name: returns error (blocks specific node)
    var member pdpb.Member
    member.Unmarshal([]byte(leaderData))
    if ok && member.Name == name {
        failpoint.Return(errors.Errorf("failed to grant lease"))
    }
    // Otherwise: no-op (allows other nodes to campaign)
})
```

### exitCampaignLeader (server/server.go:1820)
```go
// NOTE: This is simplified pseudo-code. See server/server.go for actual implementation.
failpoint.Inject("exitCampaignLeader", func(val failpoint.Value) {
    // Forces a specific member to exit one campaign tick
    // val: member_id string (numeric, e.g., "12345")
    memberIDStr, ok := val.(string)
    if !ok || len(memberIDStr) == 0 {
        // Invalid or empty payload; ignore injection
        return
    }
    // In the real implementation, memberIDStr is parsed as a uint64:
    // memberID, err := strconv.ParseUint(memberIDStr, 10, 64)
    // if err != nil {
    //     return
    // }
    // Check if we're the named member (by ID) and exit the campaign loop
})
```

### leaderLoopCheckAgain (server/server.go:1668)
```go
// NOTE: This is simplified pseudo-code. See server/server.go for actual implementation.
failpoint.Inject("leaderLoopCheckAgain", func(val failpoint.Value) {
    // Forces leaderLoop to re-check leadership without campaigning
    // val: member_id string (numeric, e.g., "12345")
    // Used with exitCampaignLeader to prevent re-campaign
    memberIDStr, ok := val.(string)
    if !ok || len(memberIDStr) == 0 {
        // Invalid or empty payload; ignore injection
        return
    }
    // In the real implementation, memberIDStr is parsed as a uint64:
    // memberID, err := strconv.ParseUint(memberIDStr, 10, 64)
    // if err != nil {
    //     return
    // }
    // Check if we're the named member (by ID) and set checkAgain = true
})
```

## Best Practices

1. **Always cleanup**: Enabled failpoints persist in running binaries. Always disable failpoints after testing.
2. **Use pause for blocking**: When you need to block execution at a failpoint, use `pause` instead of `return(...)`.
3. **Test with specific payloads**: Many failpoints expect specific payload formats (numeric IDs, strings, etc.).
4. **Pair failpoints appropriately**: Some failpoints need to be paired (e.g., `exitCampaignLeader` + `leaderLoopCheckAgain`).
5. **Check implementation**: Before using a failpoint, check its implementation to understand expected payload format and behavior.
