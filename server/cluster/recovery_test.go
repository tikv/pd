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

package cluster

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/pkg/storage"
)

func TestRecoveryStateBlocksBothSplitAllocationEntrypoints(t *testing.T) {
	ctx := t.Context()
	_, opt, err := newTestScheduleConfig()
	require.NoError(t, err)
	c := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	peer := &metapb.Peer{Id: 1, StoreId: 1}
	region := core.NewRegionInfo(&metapb.Region{Id: 1, RegionEpoch: &metapb.RegionEpoch{}, Peers: []*metapb.Peer{peer}}, peer,
		core.WithRecoveryStatus(&pdpb.RegionRecoveryStatus{State: pdpb.RegionRecoveryState_NORMAL, DataServiceReady: false, SizeSource: pdpb.RegionSizeSource_SIZE_CURRENT}))
	c.PutRegion(region)
	_, err = c.HandleAskSplit(&pdpb.AskSplitRequest{Region: region.GetMeta()})
	require.ErrorContains(t, err, "paused during recovery")
	_, err = c.HandleAskBatchSplit(&pdpb.AskBatchSplitRequest{Region: region.GetMeta(), SplitCount: 1})
	require.ErrorContains(t, err, "paused during recovery")
}

func TestStoreProtocolVersionSurvivesRegistrationAndLabels(t *testing.T) {
	ctx := t.Context()
	_, opt, err := newTestScheduleConfig()
	require.NoError(t, err)
	c := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	meta := newTestStores(1, "6.6.0")[0].GetMeta()
	require.NoError(t, c.PutMetaStore(meta))
	meta = proto.Clone(meta).(*metapb.Store)
	meta.LogOnlyProtocolVersion = core.LogOnlyProtocolVersion
	meta.TxnProtocolVersionRange = &metapb.TxnProtocolVersionRange{Min: 1, Max: 2}
	require.NoError(t, c.PutMetaStore(meta))
	require.NoError(t, c.UpdateStoreLabels(1, []*metapb.StoreLabel{{Key: "zone", Value: "z1"}}, false))
	require.Equal(t, core.LogOnlyProtocolVersion, c.GetStore(1).GetMeta().GetLogOnlyProtocolVersion())
	require.True(t, proto.Equal(meta.GetTxnProtocolVersionRange(), c.GetStore(1).GetMeta().GetTxnProtocolVersionRange()))
}

func TestDeclaredProtocolCannotDowngradeAfterLeavingWitnessGroup(t *testing.T) {
	ctx := t.Context()
	_, opt, err := newTestScheduleConfig()
	require.NoError(t, err)
	c := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend())
	for _, store := range newTestStores(3, "6.6.0") {
		meta := store.GetMeta()
		capability := core.FullStoreCapability
		if meta.Id == 3 {
			capability = core.LogOnlyStoreCapability
		}
		meta.Labels = []*metapb.StoreLabel{{Key: core.StoreCapabilityLabel, Value: capability}}
		meta.LogOnlyProtocolVersion = core.LogOnlyProtocolVersion
		require.NoError(t, c.PutMetaStore(meta))
	}
	peers := []*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}, {Id: 3, StoreId: 3, IsWitness: true}}
	region := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: peers}, peers[0])
	c.PutRegion(region)
	downgraded := proto.Clone(c.GetStore(1).GetMeta()).(*metapb.Store)
	downgraded.LogOnlyProtocolVersion = 0
	require.ErrorContains(t, c.PutMetaStore(downgraded), "cannot downgrade a declared LogOnly protocol")
	c.PutRegion(region.Clone(core.WithRemoveStorePeer(3)))
	// Returning to 3F cannot prove that old readers understand persistent DFS copies.
	require.ErrorContains(t, c.PutMetaStore(downgraded), "cannot downgrade a declared LogOnly protocol")
}
