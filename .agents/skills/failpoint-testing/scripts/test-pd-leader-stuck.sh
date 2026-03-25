#!/usr/bin/env bash
set -euo pipefail

PD_LEADER="http://127.0.0.1:2379"
FP_SKIP="github.com/tikv/pd/pkg/election/skipGrantLeader"
FP_EXIT="github.com/tikv/pd/server/exitCampaignLeader"

# Get leader's member_id
MEMBER_ID=$(curl -s "$PD_LEADER/pd/api/v1/leader" | \
  python3 -c "import json,sys; print(json.load(sys.stdin)['member_id'])")

echo "=== Leader member_id: $MEMBER_ID ==="
curl -s "$PD_LEADER/metrics" | grep -E "^etcd_server_is_leader|^service_member_role"

# Block ALL nodes from campaigning (pause = indefinite block)
echo ""
echo "Setting skipGrantLeader (pause) on all nodes..."
for port in 2379 2382 2384; do
  curl -s -X PUT "http://127.0.0.1:$port/pd/api/v1/fail/$FP_SKIP" -d 'pause'
done
echo "Done"

# Force leader to step down
echo ""
echo "Triggering exitCampaignLeader for member $MEMBER_ID..."
curl -s -X PUT "$PD_LEADER/pd/api/v1/fail/$FP_EXIT" \
  -d "return(\"$MEMBER_ID\")"
echo "Done"

# Verify stuck state
sleep 2
echo ""
echo "=== Stuck state achieved ==="
curl -s "$PD_LEADER/metrics" | grep -E "^etcd_server_is_leader|^service_member_role"

# Should see:
# etcd_server_is_leader 1
# service_member_role{service="PD"} 0

# Cleanup
for port in 2379 2382 2384; do
  curl -s -X PUT "http://127.0.0.1:$port/pd/api/v1/fail/$FP_SKIP" -d ''
done
