// Copyright 2026 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cluster_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

func TestLogStoreFailureRepairUsesMatchingStoreCapability(t *testing.T) {
	for _, tc := range []struct {
		name          string
		failedStore   uint64
		targetStore   uint64
		witnessTarget bool
	}{
		{name: "full", failedStore: 2, targetStore: 4},
		{name: "witness", failedStore: 3, targetStore: 5, witnessTarget: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			pdCluster, err := tests.NewTestCluster(t.Context(), 1, func(cfg *config.Config, _ string) {
				cfg.Replication.EnablePlacementRules = true
				cfg.Schedule.PatrolRegionInterval = typeutil.NewDuration(time.Hour)
			})
			re.NoError(err)
			defer pdCluster.Destroy()
			re.NoError(pdCluster.RunInitialServers())
			pdCluster.WaitLeader()
			leader := pdCluster.GetLeaderServer()
			client, conn := testutil.MustNewGrpcClient(re, leader.GetAddr())
			defer conn.Close()
			bootstrapCluster(re, leader.GetClusterID(), client)

			for id := uint64(1); id <= 5; id++ {
				capability, storeClass := core.FullStoreCapability, "full"
				if id == 3 || id == 5 {
					capability, storeClass = core.LogOnlyStoreCapability, "log"
				}
				store := newMetaStore(id, fmt.Sprintf("mock://tikv-%d:%d", id, id), "v8.0.0", metapb.StoreState_Up, getTestDeployPath(id))
				store.LogOnlyProtocolVersion = core.LogOnlyProtocolVersion
				store.Labels = []*metapb.StoreLabel{
					{Key: core.StoreCapabilityLabel, Value: capability},
					{Key: "store-class", Value: storeClass},
				}
				resp, err := putStore(client, leader.GetClusterID(), store)
				re.NoError(err)
				re.Equal(pdpb.ErrorType_OK, resp.GetHeader().GetError().GetType())
				heartbeat, err := client.StoreHeartbeat(t.Context(), &pdpb.StoreHeartbeatRequest{
					Header: testutil.NewRequestHeader(leader.GetClusterID()),
					Stats:  &pdpb.StoreStats{StoreId: id, Capacity: 1 << 40, Available: 1 << 40},
				})
				re.NoError(err)
				re.Equal(pdpb.ErrorType_OK, heartbeat.GetHeader().GetError().GetType())
			}

			rc := leader.GetRaftCluster()
			leader.GetPersistOptions().SetEnableWitness(true)
			re.NoError(rc.GetRuleManager().SetRule(&placement.Rule{
				GroupID: placement.DefaultGroupID, ID: placement.DefaultRuleID,
				Role: placement.Voter, Count: 2,
				LabelConstraints: []placement.LabelConstraint{{Key: "store-class", Op: placement.In, Values: []string{"full"}}},
			}))
			re.NoError(rc.GetRuleManager().SetRule(&placement.Rule{
				GroupID: placement.DefaultGroupID, ID: "log",
				Role: placement.Voter, Count: 1, IsWitness: true,
				LabelConstraints: []placement.LabelConstraint{{Key: "store-class", Op: placement.In, Values: []string{"log"}}},
			}))

			peers := []*metapb.Peer{
				{Id: 101, StoreId: 1},
				{Id: 102, StoreId: 2},
				{Id: 103, StoreId: 3, IsWitness: true},
			}
			region := core.NewRegionInfo(&metapb.Region{
				Id: 10, StartKey: []byte("a"), EndKey: []byte("b"),
				RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1}, Peers: peers,
			}, peers[0],
				core.SetApproximateSize(10),
				core.WithRecoveryStatus(&pdpb.RegionRecoveryStatus{
					State: pdpb.RegionRecoveryState_NORMAL, DataServiceReady: true,
					SizeSource:                    pdpb.RegionSizeSource_SIZE_CURRENT,
					RequiredReaderProtocolVersion: core.LogOnlyProtocolVersion,
				}))
			re.NoError(pdCluster.HandleRegionHeartbeat(region))

			// Report a down peer and advance that Store's last heartbeat beyond the
			// replacement threshold without waiting for a real timer.
			store := rc.GetStore(tc.failedStore)
			re.NotNil(store)
			rc.GetBasicCluster().PutStore(store.Clone(core.SetLastHeartbeatTS(time.Now().Add(-31 * time.Minute))))
			region = region.Clone(core.WithDownPeers([]*pdpb.PeerStats{{
				Peer: peers[tc.failedStore-1], DownSeconds: 31 * 60,
			}}))
			re.NoError(pdCluster.HandleRegionHeartbeat(region))

			cached := rc.GetRegion(region.GetID())
			re.NotNil(cached)
			re.Len(cached.GetDownPeers(), 1)
			re.NotNil(cached.GetLeader())
			re.GreaterOrEqual(rc.GetStore(tc.failedStore).DownTime(), 30*time.Minute)
			re.Len(rc.GetRuleManager().FitRegion(rc, cached).RuleFits, 2)
			ops := rc.GetCoordinator().GetCheckerController().CheckRegion(cached)
			re.Len(ops, 1)
			op := ops[0]
			re.NotNil(op)
			var added []operator.AddLearner
			for i := range op.Len() {
				switch step := op.Step(i).(type) {
				case operator.AddLearner:
					added = append(added, step)
				case operator.BecomeNonWitness:
					re.Fail("repair must not turn a LogOnly witness into Full")
				}
			}
			re.Len(added, 1, "unexpected repair operator: %s", op)
			add := added[0]
			re.Equal(tc.targetStore, add.ToStore)
			re.Equal(tc.witnessTarget, add.IsWitness)
			target := rc.GetStore(add.ToStore)
			re.NotNil(target)
			re.True(target.SupportsPeerCapability(add.IsWitness))
			controller := rc.GetOperatorController()
			re.True(controller.AddOperator(op))
			re.Same(op, controller.GetOperator(region.GetID()))
		})
	}
}
