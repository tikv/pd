// Copyright 2016 TiKV Project Authors.
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
	"context"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/pkg/storage"
)

func mockRegionPeer(cluster *RaftCluster, voters []uint64) []*metapb.Peer {
	rst := make([]*metapb.Peer, len(voters))
	for i, v := range voters {
		id, _ := cluster.AllocID()
		rst[i] = &metapb.Peer{
			Id:      id,
			StoreId: v,
			Role:    metapb.PeerRole_Voter,
		}
	}
	return rst
}

func TestReportSplit(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	right := &metapb.Region{Id: 1, StartKey: []byte("a"), EndKey: []byte("c"), Peers: mockRegionPeer(cluster, []uint64{1, 2, 3}),
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1}}
	region := core.NewRegionInfo(right, right.Peers[0])
	cluster.putRegion(region)

	// split failed, split region keys must be continuous.
	left := &metapb.Region{Id: 2, StartKey: []byte("a"), EndKey: []byte("b"), Peers: mockRegionPeer(cluster, []uint64{1, 2, 3}),
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 2}}
	_, err = cluster.HandleReportSplit(&pdpb.ReportSplitRequest{Left: right, Right: left})
	re.Error(err)

	// split success with continuous region keys.
	right = &metapb.Region{Id: 1, StartKey: []byte("b"), EndKey: []byte("c"), Peers: mockRegionPeer(cluster, []uint64{1, 2, 3}),
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 2}}
	_, err = cluster.HandleReportSplit(&pdpb.ReportSplitRequest{Left: left, Right: right})
	re.NoError(err)
	// no range hole
	storeID := region.GetLeader().GetStoreId()
	re.Equal(storeID, cluster.GetRegionByKey([]byte("b")).GetLeader().GetStoreId())
	re.Equal(storeID, cluster.GetRegionByKey([]byte("a")).GetLeader().GetStoreId())
	re.Equal(uint64(1), cluster.GetRegionByKey([]byte("b")).GetID())
	re.Equal(uint64(2), cluster.GetRegionByKey([]byte("a")).GetID())

	testdata := []struct {
		regionID uint64
		startKey []byte
		endKey   []byte
	}{
		{
			regionID: 1,
			startKey: []byte("b"),
			endKey:   []byte("c"),
		}, {
			regionID: 2,
			startKey: []byte("a"),
			endKey:   []byte("b"),
		},
	}

	for _, data := range testdata {
		r := metapb.Region{}
		ok, err := cluster.storage.LoadRegion(data.regionID, &r)
		re.Nil(err)
		re.True(ok)
		re.Equal(data.startKey, r.GetStartKey())
		re.Equal(data.endKey, r.GetEndKey())
	}
}

func TestReportBatchSplit(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, opt, err := newTestScheduleConfig()
	re.NoError(err)
	cluster := newTestRaftCluster(ctx, mockid.NewIDAllocator(), opt, storage.NewStorageWithMemoryBackend(), core.NewBasicCluster())
	regions := []*metapb.Region{
		{Id: 1, StartKey: []byte(""), EndKey: []byte("a"), Peers: mockRegionPeer(cluster, []uint64{1, 2, 3})},
		{Id: 2, StartKey: []byte("a"), EndKey: []byte("b"), Peers: mockRegionPeer(cluster, []uint64{1, 2, 3})},
		{Id: 3, StartKey: []byte("b"), EndKey: []byte("c"), Peers: mockRegionPeer(cluster, []uint64{1, 2, 3})},
		{Id: 3, StartKey: []byte("c"), EndKey: []byte(""), Peers: mockRegionPeer(cluster, []uint64{1, 2, 3})},
	}
	_, err = cluster.HandleBatchReportSplit(&pdpb.ReportBatchSplitRequest{Regions: regions})
	re.NoError(err)
}
