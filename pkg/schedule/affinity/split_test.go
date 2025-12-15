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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

func TestAllowSplit(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	for i := uint64(1); i <= 3; i++ {
		storeInfo := core.NewStoreInfo(&metapb.Store{Id: i, Address: fmt.Sprintf("test%d", i)})
		storeInfo = storeInfo.Clone(core.SetLastHeartbeatTS(time.Now()))
		storeInfos.PutStore(storeInfo)
	}

	// Enable affinity scheduling
	conf := mockconfig.NewTestOptions()
	cfg := conf.GetScheduleConfig().Clone()
	cfg.AffinityScheduleLimit = 4 // Enable affinity scheduling
	conf.SetScheduleConfig(cfg)

	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)

	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	groupID := "g1"
	keyRange := keyutil.KeyRange{StartKey: []byte("a"), EndKey: []byte("z")}
	err = manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: groupID, KeyRanges: []keyutil.KeyRange{keyRange}}})
	re.NoError(err)
	_, err = manager.UpdateAffinityGroupPeers(groupID, 1, []uint64{1, 2, 3})
	re.NoError(err)
	defer func() {
		err := manager.DeleteAffinityGroups([]string{groupID}, true)
		re.NoError(err)
	}()

	conf.SetMaxAffinityMergeRegionSize(10)

	// Case 1: small affinity region will not be split but admin split will always be allowed
	smallRegion := generateRegionForTest(1, []uint64{1, 2, 3}, keyutil.KeyRange{StartKey: []byte("b"), EndKey: []byte("y")})
	smallRegion = smallRegion.Clone(
		core.SetApproximateSize(5),
		core.SetApproximateKeys(int64(5*config.RegionSizeToKeysRatio)),
	)
	re.False(manager.AllowSplit(smallRegion, pdpb.SplitReason_SIZE))
	re.False(manager.AllowSplit(smallRegion, pdpb.SplitReason_LOAD))
	re.True(manager.AllowSplit(smallRegion, pdpb.SplitReason_ADMIN))

	// Case 2: large affinity region will be split
	maxSize := (int64(conf.GetMaxAffinityMergeRegionSize()) + affinityRegionSizeBufferMB) * affinityRegionSizeMultiplier
	largeRegion := smallRegion.Clone(
		core.SetApproximateSize(maxSize+1),
		core.SetApproximateKeys((maxSize+1)*int64(config.RegionSizeToKeysRatio)),
	)
	re.True(manager.AllowSplit(largeRegion, pdpb.SplitReason_SIZE))
	re.True(manager.AllowSplit(largeRegion, pdpb.SplitReason_LOAD))
	re.True(manager.AllowSplit(largeRegion, pdpb.SplitReason_ADMIN))

	// Case 3: small affinity region will be split when affinity is disabled
	oldLimit := conf.GetAffinityScheduleLimit()
	conf.SetAffinityScheduleLimit(0)
	re.True(manager.AllowSplit(smallRegion, pdpb.SplitReason_SIZE))
	re.True(manager.AllowSplit(smallRegion, pdpb.SplitReason_LOAD))
	re.True(manager.AllowSplit(smallRegion, pdpb.SplitReason_ADMIN))
	conf.SetAffinityScheduleLimit(8)
	re.False(manager.AllowSplit(smallRegion, pdpb.SplitReason_SIZE))
	re.False(manager.AllowSplit(smallRegion, pdpb.SplitReason_LOAD))
	re.True(manager.AllowSplit(smallRegion, pdpb.SplitReason_ADMIN))
	conf.SetAffinityScheduleLimit(oldLimit)

	// Case 4: small affinity region will be split when affinity group expired
	manager.ExpireAffinityGroup(groupID)
	re.True(manager.AllowSplit(smallRegion, pdpb.SplitReason_SIZE))
	re.True(manager.AllowSplit(smallRegion, pdpb.SplitReason_LOAD))
	re.True(manager.AllowSplit(smallRegion, pdpb.SplitReason_ADMIN))
}
