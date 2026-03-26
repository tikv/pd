---
name: failpoint-testing
description: Use when testing PD internal state transitions like leader election, campaign loops, or PD_leader_service_stuck alert. Use instead of mocks for PD-specific behaviors that require failpoint instrumentation via HTTP API control at runtime.
---

Test PD internal goroutines using Go failpoints. Build PD with failpoint support, control failpoints at runtime via HTTP API, trigger leader-election paths, and verify metrics.

## Prerequisites

```bash
make install-tools
```

Tools install to `.tools/bin/` but may not be in PATH.

## Build

```bash
PATH=$(pwd)/.tools/bin:$PATH make pd-server-failpoint
```

**Checkpoint:** Verify `bin/pd-server` exists (128MB with failpoint instrumentation).

## Start Cluster

```bash
# Clean up any existing cluster
pkill -9 -f "pd-server|tiup.*playground" 2>/dev/null || true
rm -rf ~/.tiup/data/alert-test

# Start 3-node PD cluster with failpoint-enabled binary
tiup playground --pd 3 --pd.binpath $(pwd)/bin/pd-server \
  --kv 0 --db 0 --tiflash 0 --without-monitor \
  --tag alert-test > /tmp/tiup-playground.log 2>&1 &

# Wait for cluster stabilization
sleep 25
```

**Checkpoint:** Verify leader election:
```bash
for port in 2379 2382 2384; do
  echo "=== Port $port ==="
  curl -s "http://127.0.0.1:$port/metrics" | \
    grep -E "^etcd_server_is_leader|^service_member_role"
done
```

Leader shows: `etcd_server_is_leader 1` AND `service_member_role{service="PD"} 1`

## Get Leader Details

```bash
curl -s "http://127.0.0.1:2379/pd/api/v1/leader" | python3 -c \
  "import json,sys; d=json.load(sys.stdin); print(f\"Member ID: {d['member_id']}\nName: {d['name']}\")"
```

## Known PD Failpoints

| Failpoint | Full Path | Purpose |
|---|---|---|
| `skipGrantLeader` | `github.com/tikv/pd/pkg/election/skipGrantLeader` | Block Campaign() |
| `exitCampaignLeader` | `github.com/tikv/pd/server/exitCampaignLeader` | Exit campaign loop |
| `leaderLoopCheckAgain` | `github.com/tikv/pd/server/leaderLoopCheckAgain` | Re-check without campaign |

**Complete catalog:** See [references/failpoints.md](references/failpoints.md)

## HTTP API

**Enable:**
```bash
curl -X PUT "http://127.0.0.1:2379/pd/api/v1/fail/<FULL_PATH>" -d '<ACTION>'
```

**Disable:**
```bash
curl -X PUT "http://127.0.0.1:2379/pd/api/v1/fail/<FULL_PATH>" -d ''
```

## Action Strings

| Action | Payload | Use When |
|---|---|---|
| Block all | `pause` | Indefinite blocking (unblocks on Disable) |
| Block clean | `return("")` | Permanent blocking |
| Block named | `return("pd-0")` | Block specific node |
| Disable | (empty) | Remove failpoint |

## Critical Patterns

**Block ALL nodes from campaigning:**
```bash
for port in 2379 2382 2384; do
  curl -X PUT "http://127.0.0.1:$port/pd/api/v1/fail/github.com/tikv/pd/pkg/election/skipGrantLeader" -d 'pause'
done
```

**Step down PD leader (requires three failpoints to match TestPDLeaderLostWhileEtcdLeaderIntact):**
```bash
MEMBER_ID="3474484975246189105"

# Exit campaign loop (leader steps down)
curl -X PUT "http://127.0.0.1:2379/pd/api/v1/fail/github.com/tikv/pd/server/exitCampaignLeader" \
  -d "return(\"$MEMBER_ID\")"

# Prevent re-campaign (keeps leader from campaigning again)
curl -X PUT "http://127.0.0.1:2379/pd/api/v1/fail/github.com/tikv/pd/server/leaderLoopCheckAgain" \
  -d "return(\"$MEMBER_ID\")"

# Speed up no-leader timeout (optional, accelerates test)
curl -X PUT "http://127.0.0.1:2379/pd/api/v1/fail/github.com/tikv/pd/server/timeoutWaitPDLeader" \
  -d "return(\"1s\")"
```

**Checkpoint:** Verify stuck state:
```bash
curl -s "http://127.0.0.1:2379/metrics" | \
  grep -E "^etcd_server_is_leader|^service_member_role"
```

Expected: `etcd_server_is_leader 1` AND `service_member_role{service="PD"} 0`

## Common Mistakes

| Thought | Reality |
|---|---|
| "PATH doesn't matter" | Tools install to `.tools/bin/`, not PATH |
| "Cluster ready immediately" | Needs 20-30 seconds to stabilize |
| "Just `exitCampaignLeader`" | Leader re-campaigns without `leaderLoopCheckAgain` (or global `skipGrantLeader`) |
| "Cleanup errors = broken" | `[ErrRedirectNoLeader]` after step-down is expected |
| "I'll clean up later" | Enabled failpoints persist in running binaries |

## Red Flags - STOP and Fix

- Building without `PATH=$(pwd)/.tools/bin:$PATH` — **tools first, then build**
- Checking metrics before `sleep 25` — **cluster not ready, results invalid**
- Using `exitCampaignLeader` alone — **leader will re-campaign immediately**
- Skipping cleanup — **pollutes subsequent test runs**
- Changing code while failpoints enabled — **violates failpoint discipline**

## Gotchas

**`pause` vs `return("")`**
- `pause` freezes goroutine inside call; unblocks on Disable
- `return("")` exits cleanly; goroutine continues its loop
- Use `return("")` for permanent blocking

**`exitCampaignLeader` alone does NOT step down leader**
Always pair with `leaderLoopCheckAgain` on the same member ID to prevent re-campaigning.

**Exception: When using global `skipGrantLeader` block**
If `skipGrantLeader` with `pause` is applied to ALL nodes, `leaderLoopCheckAgain` is technically optional (since no node can campaign). However, keeping the pair is still recommended for consistency and clarity.

**Cleanup errors are expected**
`[ErrRedirectNoLeader]` after step-down is normal — leader is gone, commands still succeed.

## Cleanup

```bash
# Disable all failpoints used in this skill
for port in 2379 2382 2384; do
  # pkg/election failpoints
  curl -X PUT "http://127.0.0.1:$port/pd/api/v1/fail/github.com/tikv/pd/pkg/election/skipGrantLeader" -d ''
  # server failpoints
  curl -X PUT "http://127.0.0.1:$port/pd/api/v1/fail/github.com/tikv/pd/server/exitCampaignLeader" -d ''
  curl -X PUT "http://127.0.0.1:$port/pd/api/v1/fail/github.com/tikv/pd/server/leaderLoopCheckAgain" -d ''
  curl -X PUT "http://127.0.0.1:$port/pd/api/v1/fail/github.com/tikv/pd/server/timeoutWaitPDLeader" -d ''
done

# Stop cluster
pkill -9 -f "tiup.*playground"
rm -rf ~/.tiup/data/alert-test
```

## End-to-End Examples

**Testing PD_leader_service_stuck Alert:**
For a complete example of testing the `PD_leader_service_stuck` alert, including Prometheus setup, triggering the stuck state with failpoints, and verifying the alert fires, see [examples/pd-leader-stuck-alert.md](examples/pd-leader-stuck-alert.md).

Use this example when you need to:
- Test alert firing conditions with Prometheus
- Understand how failpoints create sustained stuck states
- Validate alert rules and timing

**Quick Test Script:**
For a simpler test script that creates the stuck state without Prometheus, see [scripts/test-pd-leader-stuck.sh](scripts/test-pd-leader-stuck.sh).

## Finding Failpoints

```bash
# Search for all failpoints
grep -r "failpoint.Inject" pkg/ server/

# Find specific failpoint
grep -r "Inject(\"skipGrantLeader\"" .
```
