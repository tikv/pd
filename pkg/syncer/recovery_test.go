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

package syncer

import (
	"context"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockserver"
	"github.com/tikv/pd/pkg/storage"
)

func TestSparseRecoveryStatusSurvivesRegionSyncAndPreservesLegacyAbsence(t *testing.T) {
	peer := &metapb.Peer{Id: 1, StoreId: 1}
	legacy := core.NewRegionInfo(&metapb.Region{Id: 1, EndKey: []byte("m"), RegionEpoch: &metapb.RegionEpoch{}, Peers: []*metapb.Peer{peer}}, peer)
	status := &pdpb.RegionRecoveryStatus{State: pdpb.RegionRecoveryState_RECOVERING_LEADER, SizeSource: pdpb.RegionSizeSource_SIZE_HISTORICAL, TargetPeerId: 2, SessionId: []byte("recovery")}
	recovering := core.NewRegionInfo(&metapb.Region{Id: 2, StartKey: []byte("m"), RegionEpoch: &metapb.RegionEpoch{}, Peers: []*metapb.Peer{peer}}, peer, core.WithRecoveryStatus(status))
	response := buildSyncRegionResponse(0, []*core.RegionInfo{legacy, recovering})
	require.Len(t, response.GetRegionRecoveryInfos(), 1)
	require.Equal(t, uint64(2), response.GetRegionRecoveryInfos()[0].GetRegionId())
	// Marshal round-trip proves nil legacy slots were not encoded as empty status.
	wire, err := proto.Marshal(response)
	require.NoError(t, err)
	response = &pdpb.SyncRegionResponse{}
	require.NoError(t, proto.Unmarshal(wire, response))
	backend := storage.NewStorageWithMemoryBackend()
	basic := core.NewBasicCluster()
	server := mockserver.NewMockServer(context.Background(), nil, nil, backend, basic)
	syncer := NewRegionSyncer(server)
	handled, _ := syncer.handleRegionSyncResponse(context.Background(), response, basic, backend, false)
	require.True(t, handled)
	require.Nil(t, basic.GetRegion(1).GetRecoveryStatus())
	require.True(t, proto.Equal(status, basic.GetRegion(2).GetRecoveryStatus()))
	require.True(t, basic.GetRegion(2).IsRecoverySchedulingBlocked())
}
