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

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

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
