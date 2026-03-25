# PD Failpoint Reference

Complete reference for PD failpoints used in testing.

## All Known PD Failpoints

| Failpoint | Full Path | Injection Point | Purpose |
|---|---|---|---|
| `skipGrantLeader` | `github.com/tikv/pd/pkg/election/skipGrantLeader` | `pkg/election/leadership.go:186` | Block Campaign() for all or named node |
| `exitCampaignLeader` | `github.com/tikv/pd/server/exitCampaignLeader` | `server/server.go:1820` | Force named member to exit campaign loop |
| `leaderLoopCheckAgain` | `github.com/tikv/pd/server/leaderLoopCheckAgain` | `server/server.go:1668` | Force leaderLoop to re-check without re-campaigning |
| `raftclusterIsBusy` | `github.com/tikv/pd/server/raftclusterIsBusy` | `server/server.go` | Freeze stopRaftCluster() |
| `timeoutWaitPDLeader` | `github.com/tikv/pd/pd/server/timeoutWaitPDLeader` | `pd/server/` | Speed up no-leader timeout |

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
| Block all | `pause` | Freezes goroutine inside call; unblocks on Disable | Temporary blocking you'll undo |
| Block clean | `return("")` | Exits cleanly; goroutine continues its loop | Permanent blocking |
| Block named | `return("pd-0")` | Blocks specific node by name | Target single node |
| Block by ID | `return("3474...")` | Blocks specific member by ID | Target by member_id |
| Disable | `(empty)` | Removes failpoint | Always cleanup after testing |

## Finding Failpoints in Code

```bash
# Search for all failpoints in PD
grep -r "failpoint.Inject" pkg/ server/

# Find specific failpoint usage
grep -r "Inject(\"skipGrantLeader\"" .
```

## Code Injection Points

### skipGrantLeader (pkg/election/leadership.go:186)
```go
failpoint.Inject("skipGrantLeader", func(val failpoint.Value) {
    // Blocks Campaign() - val can be "" (all), "node-name", or member_id
    if val == nil {
        return
    }
    if len(val.(string)) > 0 {
        // Block specific node
    }
    // Block all
})
```

### exitCampaignLeader (server/server.go:1820)
```go
failpoint.Inject("exitCampaignLeader", func(val failpoint.Value) {
    // Forces member to exit one campaign tick
    // val: member_id string
    if val == nil {
        return
    }
    memberID := val.(string)
    // Check if we're the named member and exit
})
```

### leaderLoopCheckAgain (server/server.go:1668)
```go
failpoint.Inject("leaderLoopCheckAgain", func(val failpoint.Value) {
    // Forces leaderLoop to re-check leadership without campaigning
    // val: member_id string
    // Used with exitCampaignLeader to prevent re-campaign
})
```
