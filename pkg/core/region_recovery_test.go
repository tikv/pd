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

package core

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/schedulingpb"
)

func TestRegionRecoveryStatusControlsScheduling(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  *pdpb.RegionRecoveryStatus
		blocked bool
	}{
		{"legacy", nil, false},
		{"ready", &pdpb.RegionRecoveryStatus{State: pdpb.RegionRecoveryState_NORMAL, DataServiceReady: true, SizeSource: pdpb.RegionSizeSource_SIZE_CURRENT}, false},
		{"recovering", &pdpb.RegionRecoveryStatus{State: pdpb.RegionRecoveryState_RECOVERING_LEADER, SizeSource: pdpb.RegionSizeSource_SIZE_HISTORICAL}, true},
		{"not-ready", &pdpb.RegionRecoveryStatus{SizeSource: pdpb.RegionSizeSource_SIZE_CURRENT}, true},
		{"historical-size", &pdpb.RegionRecoveryStatus{DataServiceReady: true, SizeSource: pdpb.RegionSizeSource_SIZE_HISTORICAL}, true},
		{"future-state", &pdpb.RegionRecoveryStatus{State: pdpb.RegionRecoveryState(99), DataServiceReady: true, SizeSource: pdpb.RegionSizeSource_SIZE_CURRENT}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			region := NewRegionInfo(&metapb.Region{Id: 1}, nil, WithRecoveryStatus(tc.status))
			require.Equal(t, tc.blocked, region.IsRecoverySchedulingBlocked())
			require.True(t, proto.Equal(tc.status, region.Clone().GetRecoveryStatus()))
		})
	}
}

func TestRegionRecoveryStatusSurvivesBothHeartbeatTypes(t *testing.T) {
	status := &pdpb.RegionRecoveryStatus{State: pdpb.RegionRecoveryState_RECOVERING_LEADER, SizeSource: pdpb.RegionSizeSource_SIZE_HISTORICAL, TargetPeerId: 2, SessionId: []byte("session")}
	for _, heartbeat := range []RegionHeartbeatRequest{
		&pdpb.RegionHeartbeatRequest{Region: &metapb.Region{Id: 1}, RecoveryStatus: status},
		&schedulingpb.RegionHeartbeatRequest{Region: &metapb.Region{Id: 1}, RecoveryStatus: status},
	} {
		region := RegionFromHeartbeat(heartbeat, 0)
		require.True(t, region.IsRecoveringLeader())
		require.True(t, proto.Equal(status, region.GetRecoveryStatus()))
	}
	region := NewRegionInfo(&metapb.Region{Id: 1}, nil)
	legacy := NewStoreInfo(&metapb.Store{Id: 1})
	require.Nil(t, NormalizeRegionRecoveryStatus(region, legacy).GetRecoveryStatus())
	modern := NewStoreInfo(&metapb.Store{Id: 1, LogOnlyProtocolVersion: LogOnlyProtocolVersion})
	require.True(t, NormalizeRegionRecoveryStatus(region, modern).IsRecoverySchedulingBlocked())
}

func TestRegionRecoveryStatusChangesRefreshCacheAndRejectStaleTerm(t *testing.T) {
	peer := &metapb.Peer{Id: 1, StoreId: 1, IsWitness: true}
	meta := &metapb.Region{Id: 1, RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1}, Peers: []*metapb.Peer{peer}}
	origin := NewRegionInfo(meta, peer)
	current := origin.Clone(WithRecoveryStatus(&pdpb.RegionRecoveryStatus{State: pdpb.RegionRecoveryState_RECOVERING_LEADER}))
	current.term = 10
	_, saveCache, sync, _ := GenerateRegionGuideFunc(false)(ContextTODO(), current, origin)
	require.True(t, saveCache)
	require.True(t, sync)
	regions := NewRegionsInfo()
	regions.SetRegion(current)
	stale := origin.Clone()
	stale.term = 9
	_, _, err := regions.PreCheckPutRegion(stale)
	require.Error(t, err)
	require.True(t, regions.GetRegion(1).IsRecoveringLeader())
}

func TestReaderProtocolFloorSurvivesMissingAndLowerReports(t *testing.T) {
	origin := NewRegionInfo(&metapb.Region{Id: 1}, nil, WithRecoveryStatus(&pdpb.RegionRecoveryStatus{RequiredReaderProtocolVersion: 2, DataServiceReady: true, SizeSource: pdpb.RegionSizeSource_SIZE_CURRENT, SessionId: []byte("old")}))
	for _, reported := range []*pdpb.RegionRecoveryStatus{nil, {RequiredReaderProtocolVersion: 1, State: pdpb.RegionRecoveryState_RECOVERING_LEADER, SizeSource: pdpb.RegionSizeSource_SIZE_HISTORICAL, SessionId: []byte("new")}} {
		reportedFloor := reported.GetRequiredReaderProtocolVersion()
		current := origin.Clone(WithRecoveryStatus(reported))
		current.Inherit(origin, false)
		require.Equal(t, uint32(2), current.GetRequiredReaderProtocolVersion())
		require.Equal(t, reported.GetState(), current.GetRecoveryStatus().GetState())
		require.Equal(t, reported.GetSizeSource(), current.GetRecoveryStatus().GetSizeSource())
		require.Equal(t, reported.GetDataServiceReady(), current.GetRecoveryStatus().GetDataServiceReady())
		require.Equal(t, reported.GetSessionId(), current.GetRecoveryStatus().GetSessionId())
		require.Equal(t, reportedFloor, reported.GetRequiredReaderProtocolVersion(), "inherit must not mutate the input message")
	}
	higher := origin.Clone(WithRecoveryStatus(&pdpb.RegionRecoveryStatus{RequiredReaderProtocolVersion: 3}))
	higher.InheritReaderProtocolVersion(origin)
	require.Equal(t, uint32(3), higher.GetRequiredReaderProtocolVersion())
	// Raising only the durable floor is still a cache/sync change.
	legacy := origin.Clone(WithRecoveryStatus(nil))
	_, cache, sync, _ := GenerateRegionGuideFunc(false)(ContextTODO(), origin, legacy)
	require.True(t, cache)
	require.True(t, sync)
}

func TestConcurrentHeartbeatCannotOverwriteNewReaderProtocolFloor(t *testing.T) {
	peer := &metapb.Peer{Id: 1, StoreId: 1}
	legacy := NewRegionInfo(&metapb.Region{Id: 1, RegionEpoch: &metapb.RegionEpoch{}, Peers: []*metapb.Peer{peer}}, peer)
	regions := NewRegionsInfo()
	regions.SetRegion(legacy)
	pending := legacy.Clone()
	pending.Inherit(legacy, false)
	modern := legacy.Clone(WithRecoveryStatus(&pdpb.RegionRecoveryStatus{RequiredReaderProtocolVersion: 2}))
	_, err := regions.CheckAndPutRootTree(ContextTODO(), modern)
	require.NoError(t, err)
	_, err = regions.CheckAndPutRootTree(ContextTODO(), pending)
	require.ErrorContains(t, err, "reader protocol requirement changed")
	_, err = regions.AtomicCheckAndPutRegion(ContextTODO(), pending)
	require.ErrorContains(t, err, "reader protocol requirement changed")
	require.Equal(t, uint32(2), regions.GetRegion(1).GetRequiredReaderProtocolVersion())
	// A retried report inherits the new requirement before publication.
	pending.Inherit(regions.GetRegion(1), false)
	_, err = regions.CheckAndPutRootTree(ContextTODO(), pending)
	require.NoError(t, err)
}

func TestMetadataReloadRetainsReaderFloorWithoutDeletingValidRegion(t *testing.T) {
	peer := &metapb.Peer{Id: 1, StoreId: 1}
	meta := &metapb.Region{Id: 1, RegionEpoch: &metapb.RegionEpoch{}, Peers: []*metapb.Peer{peer}}
	regions := NewRegionsInfo()
	modern := NewRegionInfo(meta, peer, WithRecoveryStatus(&pdpb.RegionRecoveryStatus{RequiredReaderProtocolVersion: 2}))
	regions.SetRegion(modern)
	loaded := NewRegionInfo(meta, nil, SetSource(Storage))
	require.Empty(t, regions.CheckAndPutRegion(loaded), "valid metadata must not enter the storage loader deletion list")
	require.Equal(t, uint32(2), regions.GetRegion(1).GetRequiredReaderProtocolVersion())
	require.True(t, regions.GetRegion(1).IsRecoverySchedulingBlocked())
	require.Nil(t, loaded.GetRecoveryStatus(), "cache publication must not mutate the caller's input")
}
