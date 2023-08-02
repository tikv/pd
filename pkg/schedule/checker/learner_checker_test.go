// Copyright 2020 TiKV Project Authors.
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
	"context"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/versioninfo"
)

func TestPromoteLearner(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	cluster.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	lc := NewLearnerChecker(cluster)
	for id := uint64(1); id <= 10; id++ {
		cluster.PutStoreWithLabels(id)
	}

	region := core.NewRegionInfo(
		&metapb.Region{
			Id: 1,
			Peers: []*metapb.Peer{
				{Id: 101, StoreId: 1},
				{Id: 102, StoreId: 2},
				{Id: 103, StoreId: 3, Role: metapb.PeerRole_Learner},
			},
		}, &metapb.Peer{Id: 101, StoreId: 1})
	op := lc.Check(region)
	re.NotNil(op)
	re.Equal("promote-learner", op.Desc())
	re.IsType(operator.PromoteLearner{}, op.Step(0))
	re.Equal(uint64(3), op.Step(0).(operator.PromoteLearner).ToStore)

	region = region.Clone(core.WithPendingPeers([]*metapb.Peer{region.GetPeer(103)}))
	op = lc.Check(region)
	re.Nil(op)
}
