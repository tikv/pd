// Copyright 2017 TiKV Project Authors.
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

package api

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
)

func TestTrend(t *testing.T) {
	re := require.New(t)
	svr, cleanup := mustNewServer(re)
	defer cleanup()
	server.MustWaitLeader(re, []*server.Server{svr})

	mustBootstrapCluster(re, svr)
	for i := 1; i <= 3; i++ {
		mustPutStore(re, svr, uint64(i), metapb.StoreState_Up, metapb.NodeState_Serving, nil)
	}

	// Create 3 regions, all peers on store1 and store2, and the leaders are all on store1.
	region4 := newRegionInfo(4, "", "a", 2, 2, []uint64{1, 2}, nil, nil, 1)
	region5 := newRegionInfo(5, "a", "b", 2, 2, []uint64{1, 2}, nil, []uint64{2}, 1)
	region6 := newRegionInfo(6, "b", "", 2, 2, []uint64{1, 2}, nil, nil, 1)
	mustRegionHeartbeat(re, svr, region4)
	mustRegionHeartbeat(re, svr, region5)
	mustRegionHeartbeat(re, svr, region6)

	// Create 3 operators that transfers leader, moves follower, moves leader.
	re.NoError(svr.GetHandler().AddTransferLeaderOperator(4, 2))
	re.NoError(svr.GetHandler().AddTransferPeerOperator(5, 2, 3))
	time.Sleep(time.Second)
	re.NoError(svr.GetHandler().AddTransferPeerOperator(6, 1, 3))
	// Complete the operators.
	mustRegionHeartbeat(re, svr, region4.Clone(core.WithLeader(region4.GetStorePeer(2))))

	op, err := svr.GetHandler().GetOperator(5)
	re.NoError(err)
	re.NotNil(op)
	re.True(op.Step(0).(operator.AddLearner).IsWitness)

	newPeerID := op.Step(0).(operator.AddLearner).PeerID
	region5 = region5.Clone(core.WithAddPeer(&metapb.Peer{Id: newPeerID, StoreId: 3, Role: metapb.PeerRole_Learner}), core.WithIncConfVer())
	mustRegionHeartbeat(re, svr, region5)
	region5 = region5.Clone(core.WithRole(newPeerID, metapb.PeerRole_Voter), core.WithRemoveStorePeer(2), core.WithIncConfVer())
	mustRegionHeartbeat(re, svr, region5)

	op, err = svr.GetHandler().GetOperator(6)
	re.NoError(err)
	re.NotNil(op)
	newPeerID = op.Step(0).(operator.AddLearner).PeerID
	region6 = region6.Clone(core.WithAddPeer(&metapb.Peer{Id: newPeerID, StoreId: 3, Role: metapb.PeerRole_Learner}), core.WithIncConfVer())
	mustRegionHeartbeat(re, svr, region6)
	region6 = region6.Clone(core.WithRole(newPeerID, metapb.PeerRole_Voter), core.WithLeader(region6.GetStorePeer(2)), core.WithRemoveStorePeer(1), core.WithIncConfVer())
	mustRegionHeartbeat(re, svr, region6)
	time.Sleep(50 * time.Millisecond)

	var trend Trend
	err = tu.ReadGetJSON(re, testDialClient, fmt.Sprintf("%s%s/api/v1/trend", svr.GetAddr(), apiPrefix), &trend)
	re.NoError(err)

	// Check store states.
	expectLeaderCount := map[uint64]int{1: 1, 2: 2, 3: 0}
	expectRegionCount := map[uint64]int{1: 2, 2: 2, 3: 2}
	re.Len(trend.Stores, 3)
	for _, store := range trend.Stores {
		re.Equal(expectLeaderCount[store.ID], store.LeaderCount)
		re.Equal(expectRegionCount[store.ID], store.RegionCount)
	}

	// Check history.
	expectHistory := map[trendHistoryEntry]int{
		{From: 1, To: 2, Kind: "leader"}: 2,
		{From: 1, To: 3, Kind: "region"}: 1,
		{From: 2, To: 3, Kind: "region"}: 1,
	}
	re.Len(trend.History.Entries, 3)
	for _, history := range trend.History.Entries {
		re.Equal(expectHistory[trendHistoryEntry{From: history.From, To: history.To, Kind: history.Kind}], history.Count)
	}
}

func newRegionInfo(id uint64, startKey, endKey string, confVer, ver uint64, voters []uint64, learners []uint64, witnesses []uint64, leaderStore uint64) *core.RegionInfo {
	var (
		peers  = make([]*metapb.Peer, 0, len(voters)+len(learners))
		leader *metapb.Peer
	)
	for _, id := range voters {
		witness := false
		for _, wid := range witnesses {
			if id == wid {
				witness = true
				break
			}
		}
		p := &metapb.Peer{Id: 10 + id, StoreId: id, IsWitness: witness}
		if id == leaderStore {
			leader = p
		}
		peers = append(peers, p)
	}
	for _, id := range learners {
		witness := false
		for _, wid := range witnesses {
			if id == wid {
				witness = true
				break
			}
		}
		p := &metapb.Peer{Id: 10 + id, StoreId: id, Role: metapb.PeerRole_Learner, IsWitness: witness}
		peers = append(peers, p)
	}
	return core.NewRegionInfo(
		&metapb.Region{
			Id:          id,
			StartKey:    []byte(startKey),
			EndKey:      []byte(endKey),
			RegionEpoch: &metapb.RegionEpoch{ConfVer: confVer, Version: ver},
			Peers:       peers,
		},
		leader,
	)
}
