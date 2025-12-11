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

package filter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/affinity"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

func TestAffinityFilterSuite(t *testing.T) {
	suite.Run(t, new(affinityFilterSuite))
}

type affinityFilterSuite struct {
	suite.Suite
	cluster *mockcluster.Cluster
	cancel  context.CancelFunc
	filter  RegionFilter
}

func (s *affinityFilterSuite) SetupSuite() {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	opt := mockconfig.NewTestOptions()
	cfg := opt.GetScheduleConfig().Clone()
	cfg.EnableAffinityScheduling = true
	opt.SetScheduleConfig(cfg)

	s.cluster = mockcluster.NewCluster(ctx, opt)
	for i := 1; i <= 3; i++ {
		s.cluster.AddRegionStore(uint64(i), 1)
	}
	s.filter = NewAffinityFilter(s.cluster)
}

func (s *affinityFilterSuite) TearDownSuite() {
	s.cancel()
}

func (s *affinityFilterSuite) TestSelect() {
	re := s.Require()

	groupID := "g1"
	createAffinityGroup(re, s.cluster, groupID, "a", "z")
	defer func() {
		err := s.cluster.GetAffinityManager().DeleteAffinityGroups([]string{groupID}, true)
		re.NoError(err)
	}()

	affinityRegion := newRegionWithPeers(1, "b", "y", []uint64{1, 2, 3})
	re.Equal(statusRegionAffinity, s.filter.Select(affinityRegion))

	nonAffinityRegion := newRegionWithPeers(2, "za", "zz", []uint64{1, 2, 3})
	re.Equal(statusOK, s.filter.Select(nonAffinityRegion))
	re.True(AllowAutoSplit(s.cluster, nonAffinityRegion, pdpb.SplitReason_SIZE))
	re.True(AllowAutoSplit(s.cluster, nonAffinityRegion, pdpb.SplitReason_LOAD))
}

func (s *affinityFilterSuite) TestAllowAutoSplit() {
	re := s.Require()

	groupID := "g1"
	createAffinityGroup(re, s.cluster, groupID, "a", "z")
	defer func() {
		err := s.cluster.GetAffinityManager().DeleteAffinityGroups([]string{groupID}, true)
		re.NoError(err)
	}()

	oldSize := s.cluster.GetCheckerConfig().GetMaxAffinityMergeRegionSize()
	s.cluster.SetMaxAffinityMergeRegionSize(10)
	defer s.cluster.SetMaxAffinityMergeRegionSize(int(oldSize))

	// Case 1: small affinity region will not be split but admin split will always be allowed
	smallRegion := newRegionWithPeers(1, "b", "y", []uint64{1, 2, 3},
		core.SetApproximateSize(50),
		core.SetApproximateKeys(int64(config.RegionSizeToKeysRatio)),
	)
	re.False(AllowAutoSplit(s.cluster, smallRegion, pdpb.SplitReason_SIZE))
	re.False(AllowAutoSplit(s.cluster, smallRegion, pdpb.SplitReason_LOAD))
	re.True(AllowAutoSplit(s.cluster, smallRegion, pdpb.SplitReason_ADMIN))

	// Case 2: large affinity region will be split
	maxSize := (int64(s.cluster.GetCheckerConfig().GetMaxAffinityMergeRegionSize()) + affinityRegionSizeBufferMB) * affinityRegionSizeMultiplier
	largeRegion := smallRegion.Clone(
		core.SetApproximateSize(maxSize+1),
		core.SetApproximateKeys((maxSize+1)*int64(config.RegionSizeToKeysRatio)),
	)
	re.True(AllowAutoSplit(s.cluster, largeRegion, pdpb.SplitReason_SIZE))
	re.True(AllowAutoSplit(s.cluster, largeRegion, pdpb.SplitReason_LOAD))
	re.True(AllowAutoSplit(s.cluster, largeRegion, pdpb.SplitReason_ADMIN))

	// Case 3: small affinity region will be split when affinity is disabled
	cfg := s.cluster.GetScheduleConfig().Clone()
	cfg.EnableAffinityScheduling = false
	s.cluster.SetScheduleConfig(cfg)
	re.True(AllowAutoSplit(s.cluster, smallRegion, pdpb.SplitReason_SIZE))
	re.True(AllowAutoSplit(s.cluster, smallRegion, pdpb.SplitReason_LOAD))
	re.True(AllowAutoSplit(s.cluster, smallRegion, pdpb.SplitReason_ADMIN))
	cfg.EnableAffinityScheduling = true
	s.cluster.SetScheduleConfig(cfg)
	re.False(AllowAutoSplit(s.cluster, smallRegion, pdpb.SplitReason_SIZE))
	re.False(AllowAutoSplit(s.cluster, smallRegion, pdpb.SplitReason_LOAD))
	re.True(AllowAutoSplit(s.cluster, smallRegion, pdpb.SplitReason_ADMIN))

	// Case 4: small affinity region will be split when affinity group expired
	manager := s.cluster.GetAffinityManager()
	manager.ExpireAffinityGroup(groupID)
	re.True(AllowAutoSplit(s.cluster, smallRegion, pdpb.SplitReason_SIZE))
	re.True(AllowAutoSplit(s.cluster, smallRegion, pdpb.SplitReason_LOAD))
	re.True(AllowAutoSplit(s.cluster, smallRegion, pdpb.SplitReason_ADMIN))
}

func createAffinityGroup(re *require.Assertions, cluster *mockcluster.Cluster, groupID string, startKey, endKey string) {
	manager := cluster.GetAffinityManager()
	re.NoError(manager.CreateAffinityGroups([]affinity.GroupKeyRanges{{
		GroupID: groupID,
		KeyRanges: []keyutil.KeyRange{{
			StartKey: []byte(startKey),
			EndKey:   []byte(endKey),
		}},
	}}))
	_, err := manager.UpdateAffinityGroupPeers(groupID, 1, []uint64{1, 2, 3})
	re.NoError(err)
}

func newRegionWithPeers(id uint64, startKey, endKey string, peers []uint64, opts ...core.RegionCreateOption) *core.RegionInfo {
	metaPeers := make([]*metapb.Peer, len(peers))
	for i, storeID := range peers {
		metaPeers[i] = &metapb.Peer{Id: id*10 + uint64(i), StoreId: storeID}
	}
	return core.NewRegionInfo(&metapb.Region{
		Id:       id,
		StartKey: []byte(startKey),
		EndKey:   []byte(endKey),
		Peers:    metaPeers,
	}, metaPeers[0], opts...)
}
