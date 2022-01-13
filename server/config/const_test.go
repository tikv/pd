// Copyright 2022 TiKV Project Authors.
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

package config

import (
	. "github.com/pingcap/check"
)

var _ = Suite(&testConstSuite{})

type testConstSuite struct{}

func (s *testConstSuite) TestServiceLabelConstOnceOnly(c *C) {
	labelMap := make(map[string]interface{})
	c.Assert(addKey(labelMap, GetOperators), Equals, true)
	c.Assert(addKey(labelMap, SetOperators), Equals, true)
	c.Assert(addKey(labelMap, GetRegionOperator), Equals, true)
	c.Assert(addKey(labelMap, DeleteRegionOperator), Equals, true)
	c.Assert(addKey(labelMap, SetChecker), Equals, true)
	c.Assert(addKey(labelMap, GetChecker), Equals, true)
	c.Assert(addKey(labelMap, GetSchedulers), Equals, true)
	c.Assert(addKey(labelMap, AddScheduler), Equals, true)
	c.Assert(addKey(labelMap, DeleteScheduler), Equals, true)
	c.Assert(addKey(labelMap, PauseOrResumeScheduler), Equals, true)
	c.Assert(addKey(labelMap, GetSchedulerConfig), Equals, true)
	c.Assert(addKey(labelMap, GetCluster), Equals, true)
	c.Assert(addKey(labelMap, GetClusterStatus), Equals, true)
	c.Assert(addKey(labelMap, GetConfig), Equals, true)
	c.Assert(addKey(labelMap, SetConfig), Equals, true)
	c.Assert(addKey(labelMap, GetDefaultConfig), Equals, true)
	c.Assert(addKey(labelMap, GetScheduleConfig), Equals, true)
	c.Assert(addKey(labelMap, SetScheduleConfig), Equals, true)
	c.Assert(addKey(labelMap, GetPDServerConfig), Equals, true)
	c.Assert(addKey(labelMap, GetReplicationConfig), Equals, true)
	c.Assert(addKey(labelMap, SetReplicationConfig), Equals, true)
	c.Assert(addKey(labelMap, GetLabelProperty), Equals, true)
	c.Assert(addKey(labelMap, SetLabelProperty), Equals, true)
	c.Assert(addKey(labelMap, GetClusterVersion), Equals, true)
	c.Assert(addKey(labelMap, SetClusterVersion), Equals, true)
	c.Assert(addKey(labelMap, GetReplicationMode), Equals, true)
	c.Assert(addKey(labelMap, SetReplicationMode), Equals, true)
	c.Assert(addKey(labelMap, GetAllRules), Equals, true)
	c.Assert(addKey(labelMap, SetAllRules), Equals, true)
	c.Assert(addKey(labelMap, SetBatchRules), Equals, true)
	c.Assert(addKey(labelMap, GetRuleByGroup), Equals, true)
	c.Assert(addKey(labelMap, GetRuleByByRegion), Equals, true)
	c.Assert(addKey(labelMap, GetRuleByKey), Equals, true)
	c.Assert(addKey(labelMap, GetRuleByGroupAndID), Equals, true)
	c.Assert(addKey(labelMap, SetRule), Equals, true)
	c.Assert(addKey(labelMap, DeleteRuleByGroup), Equals, true)
	c.Assert(addKey(labelMap, GetAllRegionLabelRule), Equals, true)
	c.Assert(addKey(labelMap, GetRegionLabelRulesByIDs), Equals, true)
	c.Assert(addKey(labelMap, GetRegionLabelRuleByID), Equals, true)
	c.Assert(addKey(labelMap, DeleteRegionLabelRule), Equals, true)
	c.Assert(addKey(labelMap, SetRegionLabelRule), Equals, true)
	c.Assert(addKey(labelMap, PatchRegionLabelRules), Equals, true)
	c.Assert(addKey(labelMap, GetRegionLabelByKey), Equals, true)
	c.Assert(addKey(labelMap, GetAllRegionLabels), Equals, true)
	c.Assert(addKey(labelMap, GetRuleGroup), Equals, true)
	c.Assert(addKey(labelMap, SetRuleGroup), Equals, true)
	c.Assert(addKey(labelMap, DeleteRuleGroup), Equals, true)
	c.Assert(addKey(labelMap, GetAllRuleGroups), Equals, true)
	c.Assert(addKey(labelMap, GetAllPlacementRules), Equals, true)
	c.Assert(addKey(labelMap, SetAllPlacementRules), Equals, true)
	c.Assert(addKey(labelMap, GetPlacementRuleByGroup), Equals, true)
	c.Assert(addKey(labelMap, SetPlacementRuleByGroup), Equals, true)
	c.Assert(addKey(labelMap, DeletePlacementRuleByGroup), Equals, true)
	c.Assert(addKey(labelMap, GetStore), Equals, true)
	c.Assert(addKey(labelMap, DeleteStore), Equals, true)
	c.Assert(addKey(labelMap, SetStoreState), Equals, true)
	c.Assert(addKey(labelMap, SetStoreLabel), Equals, true)
	c.Assert(addKey(labelMap, SetStoreWeight), Equals, true)
	c.Assert(addKey(labelMap, SetStoreLimit), Equals, true)
	c.Assert(addKey(labelMap, GetAllStores), Equals, true)
	c.Assert(addKey(labelMap, RemoveTombstone), Equals, true)
	c.Assert(addKey(labelMap, GetAllStoresLimit), Equals, true)
	c.Assert(addKey(labelMap, SetAllStoresLimit), Equals, true)
	c.Assert(addKey(labelMap, SetStoreSceneLimit), Equals, true)
	c.Assert(addKey(labelMap, GetStoreSceneLimit), Equals, true)
	c.Assert(addKey(labelMap, GetLabels), Equals, true)
	c.Assert(addKey(labelMap, GetStoresByLabel), Equals, true)
	c.Assert(addKey(labelMap, GetHotspotWriteRegion), Equals, true)
	c.Assert(addKey(labelMap, GetHotspotReadRegion), Equals, true)
	c.Assert(addKey(labelMap, GetHotspotStores), Equals, true)
	c.Assert(addKey(labelMap, GetHistoryHotspotRegion), Equals, true)
	c.Assert(addKey(labelMap, GetRegionByID), Equals, true)
	c.Assert(addKey(labelMap, GetRegionByKey), Equals, true)
	c.Assert(addKey(labelMap, GetAllRegions), Equals, true)
	c.Assert(addKey(labelMap, ScanRegions), Equals, true)
	c.Assert(addKey(labelMap, CountRegions), Equals, true)
	c.Assert(addKey(labelMap, GetRegionsByStore), Equals, true)
	c.Assert(addKey(labelMap, GetTopWriteFlowRegions), Equals, true)
	c.Assert(addKey(labelMap, GetTopReadRegions), Equals, true)
	c.Assert(addKey(labelMap, GetTopConfverRegions), Equals, true)
	c.Assert(addKey(labelMap, GetTopVersionRegions), Equals, true)
	c.Assert(addKey(labelMap, GetTopSizeRegions), Equals, true)
	c.Assert(addKey(labelMap, GetMissPeerRegions), Equals, true)
	c.Assert(addKey(labelMap, GetExtraPeerRegions), Equals, true)
	c.Assert(addKey(labelMap, GetPendingPeerRegions), Equals, true)
	c.Assert(addKey(labelMap, GetDownPeerRegions), Equals, true)
	c.Assert(addKey(labelMap, GetLearnerPeerRegions), Equals, true)
	c.Assert(addKey(labelMap, GetEmptyRegion), Equals, true)
	c.Assert(addKey(labelMap, GetOfflinePeer), Equals, true)
	c.Assert(addKey(labelMap, GetSizeHistogram), Equals, true)
	c.Assert(addKey(labelMap, GetKeysHistogram), Equals, true)
	c.Assert(addKey(labelMap, GetRegionSiblings), Equals, true)
	c.Assert(addKey(labelMap, AccelerateRegionsSchedule), Equals, true)
	c.Assert(addKey(labelMap, ScatterRegions), Equals, true)
	c.Assert(addKey(labelMap, SplitRegions), Equals, true)
	c.Assert(addKey(labelMap, GetRangeHoles), Equals, true)
	c.Assert(addKey(labelMap, CheckRegionsReplicated), Equals, true)
	c.Assert(addKey(labelMap, GetVersion), Equals, true)
	c.Assert(addKey(labelMap, GetPDStatus), Equals, true)
	c.Assert(addKey(labelMap, GetMembers), Equals, true)
	c.Assert(addKey(labelMap, DeleteMemberByName), Equals, true)
	c.Assert(addKey(labelMap, DeleteMemberByID), Equals, true)
	c.Assert(addKey(labelMap, SetMemberByName), Equals, true)
	c.Assert(addKey(labelMap, GetLeader), Equals, true)
	c.Assert(addKey(labelMap, ResignLeader), Equals, true)
	c.Assert(addKey(labelMap, TransferLeader), Equals, true)
	c.Assert(addKey(labelMap, GetRegionStatus), Equals, true)
	c.Assert(addKey(labelMap, GetTrend), Equals, true)
	c.Assert(addKey(labelMap, DeleteRegionCache), Equals, true)
	c.Assert(addKey(labelMap, ResetTS), Equals, true)
	c.Assert(addKey(labelMap, SavePersistFile), Equals, true)
	c.Assert(addKey(labelMap, SetWaitAsyncTime), Equals, true)
	c.Assert(addKey(labelMap, SetLogLevel), Equals, true)
	c.Assert(addKey(labelMap, GetReplicationModeStatus), Equals, true)
	c.Assert(addKey(labelMap, SetPlugin), Equals, true)
	c.Assert(addKey(labelMap, DeletePlugin), Equals, true)
	c.Assert(addKey(labelMap, GetHealthStatus), Equals, true)
	c.Assert(addKey(labelMap, GetDiagnoseInfo), Equals, true)
	c.Assert(addKey(labelMap, Ping), Equals, true)
	c.Assert(addKey(labelMap, QueryMetric), Equals, true)
	c.Assert(addKey(labelMap, TransferLocalTSOAllocator), Equals, true)
	c.Assert(addKey(labelMap, DebugPProfProfile), Equals, true)
	c.Assert(addKey(labelMap, DebugPProfTrace), Equals, true)
	c.Assert(addKey(labelMap, DebugPProfSymbol), Equals, true)
	c.Assert(addKey(labelMap, DebugPProfHeap), Equals, true)
	c.Assert(addKey(labelMap, DebugPProfMutex), Equals, true)
	c.Assert(addKey(labelMap, DebugPProfAllocs), Equals, true)
	c.Assert(addKey(labelMap, DebugPProfBlock), Equals, true)
	c.Assert(addKey(labelMap, DebugPProfGoroutine), Equals, true)
	c.Assert(addKey(labelMap, DebugPProfThreadCreate), Equals, true)
	c.Assert(addKey(labelMap, DebugPProfZip), Equals, true)
	c.Assert(addKey(labelMap, GetGCSafePoint), Equals, true)
	c.Assert(addKey(labelMap, DeleteGCSafePoint), Equals, true)
	c.Assert(addKey(labelMap, RemoveFailedStoresUnsafely), Equals, true)
	c.Assert(addKey(labelMap, GetOngoingFailedStoresRemoval), Equals, true)
	c.Assert(addKey(labelMap, GetHistoryFailedStoresRemoval), Equals, true)
}

func addKey(keyMap map[string]interface{}, key string) bool {
	if isKeyRepetitive(keyMap, key) {
		return false
	}
	keyMap[key] = true
	return true
}

func isKeyRepetitive(keyMap map[string]interface{}, key string) bool {
	_, ok := keyMap[key]
	return ok
}
