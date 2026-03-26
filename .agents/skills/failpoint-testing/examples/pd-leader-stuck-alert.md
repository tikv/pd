# End-to-End Example: Testing PD_leader_service_stuck Alert

This example demonstrates the complete workflow for testing the `PD_leader_service_stuck` alert using failpoints.

## When to use this example

Use this example when you need to:
- Test the `PD_leader_service_stuck` alert behavior
- Verify alert firing conditions with Prometheus
- Understand how failpoints create sustained stuck states
- Validate alert rules and timing

## Alert Overview

**Alert Expression:**
```promql
(service_member_role{job="pd",service="PD"} == 0)
and on(instance,job) (etcd_server_is_leader{job="pd"} == 1)
for: 1m
```

This alert fires when the embedded etcd leader node's PD service layer is not serving as PD leader for more than 1 minute.

## Prerequisites

- PD server with failpoint support
- Prometheus ≥ 2.x
- tiup for cluster orchestration
- 3-node PD cluster

## Step 1: Build PD with Failpoint Support

```bash
cd /path/to/pd
make pd-server-failpoint
```

## Step 2: Start a 3-Node PD Cluster

```bash
# Clean up any existing cluster
pkill -9 -f "pd-server|tiup.*playground"
rm -rf ~/.tiup/data/alert-test

# Start 3-node cluster with failpoint-enabled binary
tiup playground --pd 3 --pd.binpath $(pwd)/bin/pd-server \
  --kv 0 --db 0 --tiflash 0 --without-monitor \
  --tag alert-test > /tmp/tiup-playground.log 2>&1 &

# Wait for cluster to stabilize (20-30 seconds)
sleep 25
```

**Verify cluster and identify leader:**
```bash
for port in 2379 2382 2384; do
  echo "=== Port $port ==="
  curl -s "http://127.0.0.1:$port/metrics" | \
    grep -E "^etcd_server_is_leader|^service_member_role"
done
```

Expected output (pd-1 at port 2382 is leader):
```
=== Port 2379 ===
etcd_server_is_leader 0

=== Port 2382 ===
etcd_server_is_leader 1
service_member_role{service="PD"} 1

=== Port 2384 ===
etcd_server_is_leader 0
```

## Step 3: Configure Prometheus

Create `/tmp/prometheus.yml`:
```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  - /path/to/pd/metrics/alertmanager/pd.rules.yml

scrape_configs:
  - job_name: pd
    static_configs:
      - targets: ["127.0.0.1:2379", "127.0.0.1:2382", "127.0.0.1:2384"]
    relabel_configs:
      - source_labels: [__address__]
        target_label: instance
        regex: '127.0.0.1:2379'
        replacement: 'pd-0'
      - source_labels: [__address__]
        target_label: instance
        regex: '127.0.0.1:2382'
        replacement: 'pd-1'
      - source_labels: [__address__]
        target_label: instance
        regex: '127.0.0.1:2384'
        replacement: 'pd-2'
```

Start Prometheus:
```bash
prometheus --config.file=/tmp/prometheus.yml \
  --storage.tsdb.path=/tmp/prometheus-data \
  > /tmp/prometheus.log 2>&1 &

# Wait for data collection
sleep 15
```

## Step 4: Trigger the Alert

Use the failpoints to create the stuck state:

```bash
#!/usr/bin/env bash
set -euo pipefail

# PD leader (adjust if different node is leader)
PD_LEADER="http://127.0.0.1:2382"
FP_SKIP="github.com/tikv/pd/pkg/election/skipGrantLeader"
FP_EXIT="github.com/tikv/pd/server/exitCampaignLeader"

# Get leader's member_id
MEMBER_ID=$(curl -fsS "$PD_LEADER/pd/api/v1/leader" | \
  python3 -c "import json,sys; print(json.load(sys.stdin)['member_id'])")

echo "=== Leader member_id: $MEMBER_ID ==="
curl -fsS "$PD_LEADER/metrics" | grep -E "^etcd_server_is_leader|^service_member_role"

# Block ALL nodes from campaigning (pause = indefinite block)
echo ""
echo "Setting skipGrantLeader (pause) on all nodes..."
for port in 2379 2382 2384; do
  curl -fsS -X PUT "http://127.0.0.1:$port/pd/api/v1/fail/$FP_SKIP" -d 'pause'
done
echo "Done"

# Force leader to step down
echo ""
echo "Triggering exitCampaignLeader for member $MEMBER_ID..."
curl -fsS -X PUT "$PD_LEADER/pd/api/v1/fail/$FP_EXIT" \
  -d "return(\"$MEMBER_ID\")"
echo "Done"

# Verify stuck state
sleep 2
echo ""
echo "=== Stuck state achieved ==="
curl -fsS "$PD_LEADER/metrics" | grep -E "^etcd_server_is_leader|^service_member_role"

# Hold stuck state for >1m to trigger alert
echo ""
echo "Holding stuck state for 70s (alert fires after 60s)..."
sleep 70

echo ""
echo "=== Final state ==="
curl -fsS "$PD_LEADER/metrics" | grep -E "^etcd_server_is_leader|^service_member_role"
```

## Step 5: Verify Alert Firing

**Check via Prometheus UI:**
```bash
# On macOS
open http://localhost:9090/alerts

# On Linux
xdg-open http://localhost:9090/alerts
```

Look for `PD_leader_service_stuck` with state "firing".

**Check via Prometheus API:**
```bash
curl -s http://127.0.0.1:9090/api/v1/alerts | python3 -c "
import json, sys
data = json.load(sys.stdin)
alerts = [a for a in data['data']['alerts']
  if a['labels']['alertname'] == 'PD_leader_service_stuck']
for a in alerts:
  print(f\"Instance: {a['labels']['instance']}\")
  print(f\"State: {a['state']}\")
  print(f\"Value: {a['annotations']['value']}\")
"
```

Expected output:
```
Instance: pd-1
State: firing
Value: 0
```

## Observed Behavior Timeline

```
t=0s    service_member_role=1  etcd_server_is_leader=1  (normal leader)
t+2s    service_member_role=0  etcd_server_is_leader=1  ← stuck condition
t+60s   ALERT FIRES (for: 1m satisfied)
```

### Alert State Progression

| Time | Alert State |
|---|---|
| t=0 | Inactive (condition not yet in first eval) |
| t+15s | **Pending** (condition true, accumulating `for` duration) |
| t+60s | **Firing** (condition true for ≥ 1 minute across 4 × 15 s evals) |

## Step 6: Clean Up

```bash
# Disable all failpoints
for port in 2379 2382 2384; do
  # pkg/election failpoints
  curl -fsS -X PUT "http://127.0.0.1:$port/pd/api/v1/fail/github.com/tikv/pd/pkg/election/skipGrantLeader" -d ''
  # server failpoints
  curl -fsS -X PUT "http://127.0.0.1:$port/pd/api/v1/fail/github.com/tikv/pd/server/exitCampaignLeader" -d ''
  curl -fsS -X PUT "http://127.0.0.1:$port/pd/api/v1/fail/github.com/tikv/pd/server/leaderLoopCheckAgain" -d ''
  curl -fsS -X PUT "http://127.0.0.1:$port/pd/api/v1/fail/github.com/tikv/pd/server/timeoutWaitPDLeader" -d ''
done

# Stop cluster and Prometheus
pkill -9 -f "pd-server|tiup.*playground|prometheus"
rm -rf ~/.tiup/data/alert-test /tmp/prometheus-data
```

## Key Insights

1. **Metric behavior**: `service_member_role` is only exported by nodes that have been PD leader at least once. When the leader steps down, it's set to `0` via a deferred call and persists until process restart.

2. **Failpoint pairing**: Using `skipGrantLeader` with `pause` blocks ALL nodes indefinitely, creating a sustained stuck state. Without this, the cluster might recover before the alert fires.

3. **Timing matters**: The alert requires the condition to be true for 1 minute (`for: 1m`). Prometheus evaluates every 15s by default, so it takes 4 evaluation cycles to transition from Pending to Firing.

4. **Etcd independence**: The embedded etcd raft layer (independent of PD application logic) may trigger its own leader election after ~40-70s due to heartbeat timeout, but in real scenarios the issue can persist much longer.

## References

- **PR:** https://github.com/tikv/pd/pull/10321
- **Issue:** https://github.com/tikv/pd/issues/10320
- **Alert rule:** `metrics/alertmanager/pd.rules.yml:156`
- **Metric sources:**
  - `service_member_role` → `server/server.go:1797` (Set(1)), `server/server.go:1803` (Set(0) deferred)
  - Failpoints: `pkg/election/leadership.go:186` (`skipGrantLeader`), `server/server.go:1820` (`exitCampaignLeader`)
