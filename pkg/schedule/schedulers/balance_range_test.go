// Copyright 2025 TiKV Project Authors.
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

package schedulers

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/core"
)

func TestGetPeers(t *testing.T) {
	re := require.New(t)
	learner := &metapb.Peer{StoreId: 1, Id: 1, Role: metapb.PeerRole_Learner}
	leader := &metapb.Peer{StoreId: 2, Id: 2}
	follower1 := &metapb.Peer{StoreId: 3, Id: 3}
	follower2 := &metapb.Peer{StoreId: 4, Id: 4}
	region := core.NewRegionInfo(&metapb.Region{Id: 100, Peers: []*metapb.Peer{
		leader, follower1, follower2, learner,
	}}, leader, core.WithLearners([]*metapb.Peer{learner}))
	for _, v := range []struct {
		role  string
		peers []*metapb.Peer
	}{
		{
			role:  "leader",
			peers: []*metapb.Peer{leader},
		},
		{
			role:  "follower",
			peers: []*metapb.Peer{follower1, follower2},
		},
		{
			role:  "learner",
			peers: []*metapb.Peer{learner},
		},
	} {
		role := newRole(v.role)
		re.Equal(v.peers, role.getPeers(region))
	}
}
