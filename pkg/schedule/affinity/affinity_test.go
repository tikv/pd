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

package affinity

import (
	"fmt"
	"maps"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func (m *Manager) getGroupForTest(re *require.Assertions, id string) *runtimeGroupInfo {
	m.RLock()
	defer m.RUnlock()
	group, ok := m.groups[id]
	re.True(ok)
	// Return a cloned runtimeGroupInfo to ensure lock safety.
	newGroup := *group
	newGroup.Regions = maps.Clone(group.Regions)
	return &newGroup
}

func (m *Manager) createGroupForTest(re *require.Assertions, id string, rangeCount int) []keyutil.KeyRange {
	gkr := GroupKeyRanges{
		KeyRanges: make([]keyutil.KeyRange, rangeCount),
		GroupID:   id,
	}
	for i := range rangeCount {
		gkr.KeyRanges[i] = keyutil.KeyRange{
			StartKey: []byte(fmt.Sprintf("test-%s-%04d", id, i)),
			EndKey:   []byte(fmt.Sprintf("test-%s-%04d", id, i+1)),
		}
	}
	re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{gkr}))
	return gkr.KeyRanges
}

func (m *Manager) testCacheStale(re *require.Assertions, region *core.RegionInfo) {
	cache, group := m.getCache(region)
	if cache != nil && group != nil {
		re.NotEqual(cache.affinityVer, group.affinityVer)
	}
}

// generateRegionForTest generates a test Region from the given information,
// where voterStoreIDs[0] is used as the leaderStoreID.
func generateRegionForTest(id uint64, voterStoreIDs []uint64, keyRange keyutil.KeyRange) *core.RegionInfo {
	peers := make([]*metapb.Peer, len(voterStoreIDs))
	for i, storeID := range voterStoreIDs {
		peers[i] = &metapb.Peer{
			Id:      id*10 + uint64(i),
			StoreId: storeID,
		}
	}
	meta := &metapb.Region{
		Id:       id,
		StartKey: keyRange.StartKey,
		EndKey:   keyRange.EndKey,
		Peers:    peers,
	}
	return core.NewRegionInfo(meta, peers[0])
}

var nonOverlappingRange = keyutil.KeyRange{
	StartKey: []byte("non-overlapping"),
	EndKey:   []byte("noop"),
}
