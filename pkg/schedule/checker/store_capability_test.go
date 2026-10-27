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

package checker

import (
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
)

func (suite *ruleCheckerTestSuite) prepareHeterogeneousRegion() *core.RegionInfo {
	for id := uint64(1); id <= 5; id++ {
		capability, pool := core.FullStoreCapability, "full"
		if id == 3 || id == 5 {
			capability, pool = core.LogOnlyStoreCapability, "log"
		}
		suite.cluster.AddLabelsStore(id, 1, map[string]string{
			core.StoreCapabilityLabel: capability, "store-class": pool,
		})
		suite.cluster.PutStore(suite.cluster.GetStore(id).Clone(core.SetStoreLogOnlyProtocolVersion(core.LogOnlyProtocolVersion)))
	}
	suite.cluster.AddLeaderRegion(1, 1, 2, 3)
	region := suite.cluster.GetRegion(1)
	region = region.Clone(core.WithWitnesses([]*metapb.Peer{region.GetStorePeer(3)}))
	suite.cluster.PutRegion(region)
	suite.Require().NoError(suite.ruleManager.SetRule(&placement.Rule{
		GroupID: placement.DefaultGroupID, ID: placement.DefaultRuleID,
		Role: placement.Voter, Count: 2,
		LabelConstraints: []placement.LabelConstraint{{Key: "store-class", Op: placement.In, Values: []string{"full"}}},
	}))
	suite.Require().NoError(suite.ruleManager.SetRule(&placement.Rule{
		GroupID: placement.DefaultGroupID, ID: "log", Role: placement.Voter, Count: 1, IsWitness: true,
		LabelConstraints: []placement.LabelConstraint{{Key: "store-class", Op: placement.In, Values: []string{"log"}}},
	}))
	return region
}

func (suite *ruleCheckerTestSuite) TestLogStoreFullFailureDoesNotConvertWitness() {
	region := suite.prepareHeterogeneousRegion()
	suite.cluster.SetStoreDown(2)
	suite.cluster.SetStoreLastHeartbeatInterval(2, 11*time.Minute)
	region = region.Clone(core.WithDownPeers([]*pdpb.PeerStats{{Peer: region.GetStorePeer(2), DownSeconds: 600}}))
	suite.Nil(suite.rc.Check(region))

	// Once replacement is due, add a genuine Full on the spare Full Store.
	suite.cluster.SetStoreLastHeartbeatInterval(2, 31*time.Minute)
	op := suite.rc.Check(region)
	suite.Require().NotNil(op)
	add, ok := op.Step(0).(operator.AddLearner)
	suite.Require().True(ok)
	suite.Equal(uint64(4), add.ToStore)
	suite.False(add.IsWitness)
	for i := range op.Len() {
		_, converts := op.Step(i).(operator.BecomeNonWitness)
		suite.False(converts)
	}
}

func (suite *ruleCheckerTestSuite) TestLogStoreWitnessFailureAddsNewWitness() {
	region := suite.prepareHeterogeneousRegion()
	suite.cluster.SetStoreDown(3)
	suite.cluster.SetStoreLastHeartbeatInterval(3, 31*time.Minute)
	region = region.Clone(core.WithDownPeers([]*pdpb.PeerStats{{Peer: region.GetStorePeer(3), DownSeconds: 1900}}))
	op := suite.rc.Check(region)
	suite.Require().NotNil(op)
	add, ok := op.Step(0).(operator.AddLearner)
	suite.Require().True(ok)
	suite.Equal(uint64(5), add.ToStore)
	suite.True(add.IsWitness)
}

func (suite *ruleCheckerTestSuite) TestLogStoreMissingFullCapacityDoesNotAddWitness() {
	region := suite.prepareHeterogeneousRegion()
	suite.cluster.SetStoreDown(2)
	suite.cluster.SetStoreLastHeartbeatInterval(2, 31*time.Minute)
	suite.cluster.SetStoreDown(4)
	region = region.Clone(core.WithDownPeers([]*pdpb.PeerStats{{Peer: region.GetStorePeer(2), DownSeconds: 1900}}))
	suite.Nil(suite.rc.Check(region))
	// Disabling old witness scheduling must not turn the existing W into Full.
	suite.cluster.SetEnableWitness(false)
	suite.Nil(suite.rc.Check(region))
}

func (suite *ruleCheckerTestSuite) TestRecoveringWitnessLeaderStillRepairsFailedFull() {
	region := suite.prepareHeterogeneousRegion()
	suite.cluster.SetStoreDown(2)
	suite.cluster.SetStoreLastHeartbeatInterval(2, 31*time.Minute)
	region = region.Clone(core.WithLeader(region.GetStorePeer(3)),
		core.WithRecoveryStatus(&pdpb.RegionRecoveryStatus{State: pdpb.RegionRecoveryState_RECOVERING_LEADER, SizeSource: pdpb.RegionSizeSource_SIZE_HISTORICAL}),
		core.WithDownPeers([]*pdpb.PeerStats{{Peer: region.GetStorePeer(2), DownSeconds: 1900}}))
	suite.cluster.PutRegion(region)
	op := suite.rc.Check(region)
	suite.Require().NotNil(op)
	add, ok := op.Step(0).(operator.AddLearner)
	suite.Require().True(ok)
	suite.Equal(uint64(4), add.ToStore)
	suite.False(add.IsWitness)
	for i := range op.Len() {
		_, transfer := op.Step(i).(operator.TransferLeader)
		suite.False(transfer)
	}
}

func (suite *ruleCheckerTestSuite) TestFullOnlyRepairRequiresReportedReaderProtocol(t *testing.T) {
	for id := uint64(1); id <= 4; id++ {
		suite.cluster.AddLabelsStore(id, 1, map[string]string{})
		suite.cluster.PutStore(suite.cluster.GetStore(id).Clone(core.SetStoreLogOnlyProtocolVersion(2)))
	}
	suite.cluster.AddLeaderRegion(1, 1, 2)
	region := suite.cluster.GetRegion(1).Clone(core.WithRecoveryStatus(&pdpb.RegionRecoveryStatus{RequiredReaderProtocolVersion: 2, DataServiceReady: true, SizeSource: pdpb.RegionSizeSource_SIZE_CURRENT}))
	suite.cluster.PutRegion(region)
	for _, id := range []uint64{3, 4} {
		suite.cluster.PutStore(suite.cluster.GetStore(id).Clone(core.SetStoreLogOnlyProtocolVersion(0)))
	}
	suite.Nil(suite.rc.Check(region))
	suite.cluster.PutStore(suite.cluster.GetStore(4).Clone(core.SetStoreLogOnlyProtocolVersion(2)))
	op := suite.rc.Check(region)
	suite.Require().NotNil(op)
	add, ok := op.Step(0).(operator.AddLearner)
	suite.Require().True(ok)
	suite.Equal(uint64(4), add.ToStore)
	suite.False(add.IsWitness)
}
