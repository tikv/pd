// Copyright 2024 TiKV Project Authors.
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

package response

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/replication_modepb"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestPeer(t *testing.T) {
	re := require.New(t)
	peers := []*metapb.Peer{
		{Id: 1, StoreId: 10, Role: metapb.PeerRole_Voter},
		{Id: 2, StoreId: 20, Role: metapb.PeerRole_Learner},
		{Id: 3, StoreId: 30, Role: metapb.PeerRole_IncomingVoter},
		{Id: 4, StoreId: 40, Role: metapb.PeerRole_DemotingVoter},
	}
	// float64 is the default numeric type for JSON
	expected := []map[string]any{
		{"id": float64(1), "store_id": float64(10), "role_name": "Voter"},
		{"id": float64(2), "store_id": float64(20), "role": float64(1), "role_name": "Learner", "is_learner": true},
		{"id": float64(3), "store_id": float64(30), "role": float64(2), "role_name": "IncomingVoter"},
		{"id": float64(4), "store_id": float64(40), "role": float64(3), "role_name": "DemotingVoter"},
	}

	data, err := json.Marshal(fromPeerSlice(peers))
	re.NoError(err)
	var ret []map[string]any
	re.NoError(json.Unmarshal(data, &ret))
	re.Equal(expected, ret)
}

func TestPeerStats(t *testing.T) {
	re := require.New(t)
	peers := []*pdpb.PeerStats{
		{Peer: &metapb.Peer{Id: 1, StoreId: 10, Role: metapb.PeerRole_Voter}, DownSeconds: 0},
		{Peer: &metapb.Peer{Id: 2, StoreId: 20, Role: metapb.PeerRole_Learner}, DownSeconds: 1},
		{Peer: &metapb.Peer{Id: 3, StoreId: 30, Role: metapb.PeerRole_IncomingVoter}, DownSeconds: 2},
		{Peer: &metapb.Peer{Id: 4, StoreId: 40, Role: metapb.PeerRole_DemotingVoter}, DownSeconds: 3},
	}
	// float64 is the default numeric type for JSON
	expected := []map[string]any{
		{"peer": map[string]any{"id": float64(1), "store_id": float64(10), "role_name": "Voter"}},
		{"peer": map[string]any{"id": float64(2), "store_id": float64(20), "role": float64(1), "role_name": "Learner", "is_learner": true}, "down_seconds": float64(1)},
		{"peer": map[string]any{"id": float64(3), "store_id": float64(30), "role": float64(2), "role_name": "IncomingVoter"}, "down_seconds": float64(2)},
		{"peer": map[string]any{"id": float64(4), "store_id": float64(40), "role": float64(3), "role_name": "DemotingVoter"}, "down_seconds": float64(3)},
	}

	data, err := json.Marshal(fromPeerStatsSlice(peers))
	re.NoError(err)
	var ret []map[string]any
	re.NoError(json.Unmarshal(data, &ret))
	re.Equal(expected, ret)
}

func TestEasyJSONCompatibility(t *testing.T) {
	re := require.New(t)

	peer := MetaPeer{
		Peer: &metapb.Peer{
			Id:        1,
			StoreId:   10,
			Role:      metapb.PeerRole_Voter,
			IsWitness: false,
		},
		RoleName:  metapb.PeerRole_Voter.String(),
		IsLearner: false,
	}
	// Serialize with easyjson
	easyData, err := peer.MarshalJSON()
	re.NoError(err)
	// Serialize with standard json
	stdData, err := json.Marshal(peer)
	re.NoError(err)
	// Deserialize with both methods and compare
	var easyMetaPeer, stdMetaPeer MetaPeer
	re.NoError(easyMetaPeer.UnmarshalJSON(easyData))
	re.NoError(json.Unmarshal(stdData, &stdMetaPeer))
	re.Equal(stdMetaPeer, easyMetaPeer)

	stats := PDPeerStats{
		PeerStats: &pdpb.PeerStats{
			Peer:        &metapb.Peer{Id: 2, StoreId: 20, Role: metapb.PeerRole_Learner},
			DownSeconds: 100,
		},
		Peer: fromPeer(&metapb.Peer{Id: 2, StoreId: 20, Role: metapb.PeerRole_Learner}),
	}
	// Serialize with easyjson
	easyData, err = stats.MarshalJSON()
	re.NoError(err)
	// Serialize with standard json
	stdData, err = json.Marshal(stats)
	re.NoError(err)
	// Deserialize with both methods and compare
	var easyPDPeerStats, stdPDPeerStats PDPeerStats
	re.NoError(easyPDPeerStats.UnmarshalJSON(easyData))
	re.NoError(json.Unmarshal(stdData, &stdPDPeerStats))
	re.Equal(stdPDPeerStats, easyPDPeerStats)

	status := ReplicationStatus{
		State:   replication_modepb.RegionReplicationState_INTEGRITY_OVER_LABEL.String(),
		StateID: 1,
	}
	// Serialize with easyjson
	easyData, err = status.MarshalJSON()
	re.NoError(err)
	// Serialize with standard json
	stdData, err = json.Marshal(status)
	re.NoError(err)
	// Deserialize with both methods and compare
	var easyReplicationStatus, stdReplicationStatus ReplicationStatus
	re.NoError(easyReplicationStatus.UnmarshalJSON(easyData))
	re.NoError(json.Unmarshal(stdData, &stdReplicationStatus))
	re.Equal(stdReplicationStatus, easyReplicationStatus)

	region := RegionInfo{
		ID:       100,
		StartKey: "7480000000000000FF5F698000000000FF0000010380000000FF0000000F04",
		EndKey:   "7480000000000000FF5F698000000000FF0000010380000000FF0000001104",
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 2,
		},
		Peers: []MetaPeer{
			{
				Peer:      &metapb.Peer{Id: 101, StoreId: 1, Role: metapb.PeerRole_Voter},
				RoleName:  metapb.PeerRole_Voter.String(),
				IsLearner: false,
			},
			{
				Peer:      &metapb.Peer{Id: 102, StoreId: 2, Role: metapb.PeerRole_Learner},
				RoleName:  metapb.PeerRole_Learner.String(),
				IsLearner: true,
			},
		},
		Leader: MetaPeer{
			Peer:      &metapb.Peer{Id: 101, StoreId: 1, Role: metapb.PeerRole_Voter},
			RoleName:  metapb.PeerRole_Voter.String(),
			IsLearner: false,
		},
		WrittenBytes:              1000,
		ReadBytes:                 2000,
		WrittenKeys:               100,
		ReadKeys:                  200,
		ApproximateSize:           500,
		ApproximateKeys:           1000,
		ApproximateKvSize:         450,
		ApproximateColumnarKvSize: 50,
		ReplicationStatus: &ReplicationStatus{
			State:   replication_modepb.RegionReplicationState_INTEGRITY_OVER_LABEL.String(),
			StateID: 1,
		},
	}
	// Serialize with easyjson
	easyData, err = region.MarshalJSON()
	re.NoError(err)
	// Serialize with standard json
	stdData, err = json.Marshal(region)
	re.NoError(err)
	// Deserialize with both methods and compare
	var easyRegionInfo, stdRegionInfo RegionInfo
	re.NoError(easyRegionInfo.UnmarshalJSON(easyData))
	re.NoError(json.Unmarshal(stdData, &stdRegionInfo))
	re.Equal(stdRegionInfo, easyRegionInfo)

	regions := RegionsInfo{
		Count:   2,
		Regions: []RegionInfo{region, region},
	}
	// Serialize with easyjson
	easyData, err = regions.MarshalJSON()
	re.NoError(err)
	// Serialize with standard json
	stdData, err = json.Marshal(regions)
	re.NoError(err)
	// Deserialize with both methods and compare
	var easyRegionsInfo, stdRegionsInfo RegionsInfo
	re.NoError(easyRegionsInfo.UnmarshalJSON(easyData))
	re.NoError(json.Unmarshal(stdData, &stdRegionsInfo))
	re.Equal(stdRegionsInfo, easyRegionsInfo)
}
